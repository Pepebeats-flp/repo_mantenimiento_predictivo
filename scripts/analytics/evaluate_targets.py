#!/usr/bin/env python3
"""Evaluate 6 prediction targets on fleet maintenance data.

Targets:
  1. Tiempo al próximo correctivo (regression, days)
  2. Sistema del próximo evento (multiclass, 9 categories)
  3. ¿Necesita repuestos? (binary)
  4. Duración de reparación (regression, hours)
  5. Pico de correctivos próximo mes (binary, >=10 in month)
  6. Carga por sistema+terminal próxima semana (regression, count)

Usage: python3 scripts/analytics/evaluate_targets.py
"""
from __future__ import annotations

import sys
import time
import warnings
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.metrics import (
    accuracy_score,
    classification_report,
    f1_score,
    mean_absolute_error,
    mean_squared_error,
    r2_score,
    roc_auc_score,
)
from sklearn.model_selection import TimeSeriesSplit
from sklearn.preprocessing import LabelEncoder
from xgboost import XGBClassifier, XGBRegressor

warnings.filterwarnings("ignore")

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(PROJECT_ROOT))

TRAIN_CUTOFF = pd.Timestamp("2025-07-01")
RANDOM_STATE = 42

# ═══════════════════════════════════════════════════════════════════════════════
# Shared feature engineering
# ═══════════════════════════════════════════════════════════════════════════════

def load_data() -> pd.DataFrame:
    df = pd.read_parquet(PROJECT_ROOT / "data" / "processed" / "base.parquet")
    df["fecha_evento"] = pd.to_datetime(df["fecha_evento"])
    return df


def build_bus_history(df: pd.DataFrame) -> pd.DataFrame:
    """For each corrective event, compute features from that bus's history BEFORE the event.

    Returns DataFrame with event-level features + target columns.
    """
    df = df.sort_values(["placa_patente", "fecha_evento"]).copy()
    corr = df[df["tipo_servicio"] == "CORRECTIVO"].copy()

    results = []
    for bus, bus_df in corr.groupby("placa_patente"):
        bus_df = bus_df.sort_values("fecha_evento")
        dates = bus_df["fecha_evento"].values
        sistemas = bus_df["causa_sistema_reconstruida"].values
        duraciones = bus_df["duracion_ot_horas"].values
        repuestos = bus_df["tiene_repuestos"].values
        kms = bus_df["km_ejecucion"].values
        terminales = bus_df["taller_planta_grouped"].values

        for i in range(len(bus_df)):
            current_date = dates[i]
            past_mask = dates < current_date

            n_past = past_mask.sum()
            if n_past == 0:
                # First event for this bus - minimal features
                features = {
                    "n_correctivos_7d": 0,
                    "n_correctivos_14d": 0,
                    "n_correctivos_30d": 0,
                    "n_correctivos_90d": 0,
                    "n_correctivos_total": 0,
                    "dias_desde_ultimo": 999,
                    "dias_entre_ultimos_2": 999,
                    "dias_promedio_entre": 999,
                    "avg_duracion_past": 0,
                    "prop_repuestos_past": 0,
                    "km_ultimo": kms[i] if pd.notna(kms[i]) else 0,
                    "n_sistemas_distintos": 0,
                    "ultimo_sistema": "NUNCA",
                    "ultimo_terminal": terminales[i] if pd.notna(terminales[i]) else "MISSING",
                    "tendencia_aceleracion": 0.0,
                    "n_eventos_totales_past": 0,
                    "dias_desde_primer_evento": 0,
                }
            else:
                past_dates = dates[past_mask]
                past_durations = duraciones[past_mask]
                past_repuestos = repuestos[past_mask]
                past_sistemas = sistemas[past_mask]

                # Time-based counts
                days_ago = (current_date - past_dates).astype("timedelta64[D]").astype(float)
                n_7d = (days_ago <= 7).sum()
                n_14d = (days_ago <= 14).sum()
                n_30d = (days_ago <= 30).sum()

                # Inter-event times
                if len(past_dates) >= 1:
                    dias_desde_ultimo = (current_date - past_dates[-1]).astype("timedelta64[D]").astype(float)
                else:
                    dias_desde_ultimo = 999

                if len(past_dates) >= 2:
                    inter_event_times = np.diff(
                        past_dates.astype("datetime64[ns]").astype(np.int64)
                    ) / 1e9 / 86400
                    dias_promedio = inter_event_times.mean()
                    dias_entre_ultimos_2 = (past_dates[-1] - past_dates[-2]).astype(
                        "timedelta64[D]"
                    ).astype(float)
                else:
                    dias_promedio = 999
                    dias_entre_ultimos_2 = 999

                # Duration and parts history
                valid_dur = past_durations[pd.notna(past_durations)]
                avg_dur = valid_dur.mean() if len(valid_dur) > 0 else 0
                prop_rep = past_repuestos.mean() if pd.notna(past_repuestos).any() else 0

                # System diversity
                n_sistemas = len(set(s for s in past_sistemas if pd.notna(s)))

                # Trend: recent rate vs older rate (acceleration)
                half = max(1, n_past // 2)
                older = past_dates[:half]
                recent = past_dates[half:]
                if len(older) >= 2 and len(recent) >= 2:
                    old_span = (older[-1] - older[0]).astype("timedelta64[D]").astype(float)
                    rec_span = (recent[-1] - recent[0]).astype("timedelta64[D]").astype(float)
                    old_rate = len(older) / max(old_span, 1)
                    rec_rate = len(recent) / max(rec_span, 1)
                    tendencia = rec_rate / max(old_rate, 0.001) - 1.0
                else:
                    tendencia = 0.0

                # First event date
                dias_desde_primero = (
                    current_date - past_dates[0]
                ).astype("timedelta64[D]").astype(float)

                features = {
                    "n_correctivos_7d": n_7d,
                    "n_correctivos_14d": n_14d,
                    "n_correctivos_30d": n_30d,
                    "n_correctivos_90d": (days_ago <= 90).sum(),
                    "n_correctivos_total": n_past,
                    "dias_desde_ultimo": dias_desde_ultimo,
                    "dias_entre_ultimos_2": dias_entre_ultimos_2,
                    "dias_promedio_entre": dias_promedio,
                    "avg_duracion_past": avg_dur,
                    "prop_repuestos_past": prop_rep,
                    "km_ultimo": kms[i] if pd.notna(kms[i]) else (kms[past_mask][-1] if past_mask.any() and pd.notna(kms[past_mask]).any() else 0),
                    "n_sistemas_distintos": n_sistemas,
                    "ultimo_sistema": str(past_sistemas[-1]) if pd.notna(past_sistemas[-1]) else "MISSING",
                    "ultimo_terminal": str(terminales[i]) if pd.notna(terminales[i]) else "MISSING",
                    "tendencia_aceleracion": tendencia,
                    "n_eventos_totales_past": n_past,
                    "dias_desde_primer_evento": dias_desde_primero,
                }

            # Target 1: days to next corrective (NaN for last event of each bus)
            if i < len(dates) - 1:
                next_date = dates[i + 1]
                days_to_next = (next_date - current_date).astype("timedelta64[D]").astype(float)
            else:
                days_to_next = np.nan

            # Target 2: next system
            next_system = str(sistemas[i + 1]) if i < len(dates) - 1 else np.nan

            # Target 3 & 4 are event attributes (not next)
            features["target_days_to_next"] = days_to_next
            features["target_next_system"] = next_system
            features["target_tiene_repuestos"] = repuestos[i] if pd.notna(repuestos[i]) else 0
            features["target_duracion"] = duraciones[i] if pd.notna(duraciones[i]) else np.nan

            # Context
            features["placa_patente"] = bus
            features["fecha_evento"] = current_date
            features["causa_sistema_actual"] = str(sistemas[i]) if pd.notna(sistemas[i]) else "MISSING"
            features["taller_planta"] = str(terminales[i]) if pd.notna(terminales[i]) else "MISSING"
            features["km_actual"] = kms[i] if pd.notna(kms[i]) else 0

            results.append(features)

    return pd.DataFrame(results)


def prepare_event_features(feat_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.Series]:
    """Prepare feature matrix for event-level models."""
    X = feat_df.copy()

    # One-hot encode categoricals
    for col in ["ultimo_sistema", "ultimo_terminal", "causa_sistema_actual", "taller_planta"]:
        if col in X.columns:
            dummies = pd.get_dummies(X[col], prefix=col, drop_first=True)
            X = pd.concat([X, dummies], axis=1)
            X = X.drop(columns=[col])

    # Drop non-feature columns
    drop_cols = [
        "placa_patente", "fecha_evento",
        "target_days_to_next", "target_next_system",
        "target_tiene_repuestos", "target_duracion",
    ]
    feature_cols = [c for c in X.columns if c not in drop_cols]

    return X[feature_cols], X["fecha_evento"]


# ═══════════════════════════════════════════════════════════════════════════════
# Target-specific functions
# ═══════════════════════════════════════════════════════════════════════════════

def evaluate_target_1(feat_df: pd.DataFrame, full_df: pd.DataFrame):
    """Target 1: Days to next corrective (regression)."""
    print("\n" + "=" * 70)
    print("TARGET 1: Tiempo al próximo correctivo (regression)")
    print("=" * 70)

    mask = feat_df["target_days_to_next"].notna() & (feat_df["target_days_to_next"] > 0)
    data = feat_df[mask].copy()

    X, dates = prepare_event_features(data)
    y = data["target_days_to_next"].values

    train_idx = data["fecha_evento"] < TRAIN_CUTOFF
    test_idx = data["fecha_evento"] >= TRAIN_CUTOFF

    X_train, X_test = X[train_idx], X[test_idx]
    y_train, y_test = y[train_idx], y[test_idx]

    print(f"Train: {len(y_train):,}  |  Test: {len(y_test):,}")
    print(f"y mean={y_train.mean():.1f}d, median={np.median(y_train):.1f}d, p90={np.percentile(y_train, 90):.1f}d")

    model = XGBRegressor(n_estimators=200, max_depth=6, learning_rate=0.05,
                         subsample=0.8, colsample_bytree=0.8, random_state=RANDOM_STATE,
                         n_jobs=-1)
    t0 = time.time()
    model.fit(X_train, y_train, verbose=False)
    fit_time = time.time() - t0

    y_pred = model.predict(X_test)

    # Metrics
    r2 = r2_score(y_test, y_pred)
    mae = mean_absolute_error(y_test, y_pred)
    rmse = np.sqrt(mean_squared_error(y_test, y_pred))

    # Naive baseline: always predict mean of train
    naive_pred = np.full_like(y_test, y_train.mean())
    naive_mae = mean_absolute_error(y_test, naive_pred)
    naive_r2 = r2_score(y_test, naive_pred)

    print(f"R²:  {r2:.4f}  (baseline naive: {naive_r2:.4f})")
    print(f"MAE: {mae:.1f}d (baseline naive: {naive_mae:.1f}d)")
    print(f"RMSE: {rmse:.1f}d")
    print(f"Fit: {fit_time:.1f}s")

    # Feature importance top 10
    imp = sorted(zip(X.columns, model.feature_importances_), key=lambda x: -x[1])[:10]
    print("Top features:", ", ".join(f"{n}({v:.3f})" for n, v in imp))

    return {"target": "dias_proximo_correctivo", "type": "regression",
            "r2": r2, "mae": mae, "rmse": rmse,
            "naive_mae": naive_mae, "naive_r2": naive_r2,
            "n_train": len(y_train), "n_test": len(y_test)}


def evaluate_target_2(feat_df: pd.DataFrame, full_df: pd.DataFrame):
    """Target 2: Next system classification."""
    print("\n" + "=" * 70)
    print("TARGET 2: Sistema del próximo correctivo (multiclass)")
    print("=" * 70)

    mask = feat_df["target_next_system"].notna() & (feat_df["target_next_system"] != "nan")
    data = feat_df[mask].copy()

    # Encode target
    le = LabelEncoder()
    y_all = le.fit_transform(data["target_next_system"].values)

    X, dates = prepare_event_features(data)

    train_idx = data["fecha_evento"] < TRAIN_CUTOFF
    test_idx = data["fecha_evento"] >= TRAIN_CUTOFF

    X_train, X_test = X[train_idx], X[test_idx]
    y_train, y_test = y_all[train_idx], y_all[test_idx]

    print(f"Train: {len(y_train):,}  |  Test: {len(y_test):,}")
    print(f"Classes: {len(le.classes_)} — {list(le.classes_)}")

    model = XGBClassifier(n_estimators=200, max_depth=6, learning_rate=0.05,
                          subsample=0.8, colsample_bytree=0.8, random_state=RANDOM_STATE,
                          n_jobs=-1, eval_metric='mlogloss')
    t0 = time.time()
    model.fit(X_train, y_train, verbose=False)
    fit_time = time.time() - t0

    y_pred = model.predict(X_test)
    y_proba = model.predict_proba(X_test)

    acc = accuracy_score(y_test, y_pred)
    f1_macro = f1_score(y_test, y_pred, average="macro")

    # Naive baseline: most frequent class
    most_freq = np.bincount(y_train).argmax()
    naive_acc = (y_test == most_freq).mean()

    print(f"Accuracy: {acc:.4f}  (naive most-frequent: {naive_acc:.4f})")
    print(f"F1 (macro): {f1_macro:.4f}")

    # Per-class F1
    cls_report = classification_report(y_test, y_pred, target_names=le.classes_,
                                       zero_division=0, output_dict=True)
    print("\nPer-class F1:")
    for cls in le.classes_:
        f1_c = cls_report[cls]["f1-score"]
        sup = cls_report[cls]["support"]
        print(f"  {cls:<20s}: F1={f1_c:.3f} (n={sup})")

    print(f"\nFit: {fit_time:.1f}s")

    imp = sorted(zip(X.columns, model.feature_importances_), key=lambda x: -x[1])[:10]
    print("Top features:", ", ".join(f"{n}({v:.3f})" for n, v in imp))

    return {"target": "sistema_proximo_correctivo", "type": "multiclass",
            "accuracy": acc, "f1_macro": f1_macro,
            "naive_acc": naive_acc, "n_classes": len(le.classes_),
            "n_train": len(y_train), "n_test": len(y_test)}


def evaluate_target_3(feat_df: pd.DataFrame, full_df: pd.DataFrame):
    """Target 3: Needs spare parts (binary classification)."""
    print("\n" + "=" * 70)
    print("TARGET 3: ¿El correctivo necesita repuestos? (binary)")
    print("=" * 70)

    data = feat_df.copy()
    data["target_tiene_repuestos"] = data["target_tiene_repuestos"].fillna(0).astype(int)

    X, dates = prepare_event_features(data)
    y = data["target_tiene_repuestos"].values

    train_idx = data["fecha_evento"] < TRAIN_CUTOFF
    test_idx = data["fecha_evento"] >= TRAIN_CUTOFF

    X_train, X_test = X[train_idx], X[test_idx]
    y_train, y_test = y[train_idx], y[test_idx]

    pos_rate = y_train.mean()
    print(f"Train: {len(y_train):,}  |  Test: {len(y_test):,}")
    print(f"Positive rate: {pos_rate:.1%}")

    model = XGBClassifier(n_estimators=200, max_depth=6, learning_rate=0.05,
                          subsample=0.8, colsample_bytree=0.8, random_state=RANDOM_STATE,
                          n_jobs=-1, scale_pos_weight=(1 - pos_rate) / max(pos_rate, 0.01))
    t0 = time.time()
    model.fit(X_train, y_train, verbose=False)
    fit_time = time.time() - t0

    y_pred = model.predict(X_test)
    y_proba = model.predict_proba(X_test)[:, 1]

    acc = accuracy_score(y_test, y_pred)
    f1 = f1_score(y_test, y_pred)
    auc = roc_auc_score(y_test, y_proba)

    naive_acc = max(y_train.mean(), 1 - y_train.mean())

    print(f"Accuracy:  {acc:.4f}  (naive majority: {naive_acc:.4f})")
    print(f"F1:        {f1:.4f}")
    print(f"ROC-AUC:   {auc:.4f}")
    print(f"Fit: {fit_time:.1f}s")

    imp = sorted(zip(X.columns, model.feature_importances_), key=lambda x: -x[1])[:10]
    print("Top features:", ", ".join(f"{n}({v:.3f})" for n, v in imp))

    return {"target": "necesita_repuestos", "type": "binary",
            "accuracy": acc, "f1": f1, "roc_auc": auc,
            "naive_acc": naive_acc, "positive_rate": pos_rate,
            "n_train": len(y_train), "n_test": len(y_test)}


def evaluate_target_4(feat_df: pd.DataFrame, full_df: pd.DataFrame):
    """Target 4: Repair duration (regression)."""
    print("\n" + "=" * 70)
    print("TARGET 4: Duración de reparación (regression)")
    print("=" * 70)

    mask = feat_df["target_duracion"].notna() & (feat_df["target_duracion"] > 0)
    data = feat_df[mask].copy()

    # Log-transform the target (it's very skewed: median 1.1h, mean 11.4h)
    y_raw = data["target_duracion"].values
    y_log = np.log1p(y_raw)

    X, dates = prepare_event_features(data)

    train_idx = data["fecha_evento"] < TRAIN_CUTOFF
    test_idx = data["fecha_evento"] >= TRAIN_CUTOFF

    X_train, X_test = X[train_idx], X[test_idx]
    y_train_log, y_test_raw = y_log[train_idx], y_raw[test_idx]

    print(f"Train: {len(y_train_log):,}  |  Test: {len(y_test_raw):,}")
    print(f"y mean={y_raw[train_idx].mean():.1f}h, median={np.median(y_raw[train_idx]):.1f}h, "
          f"p90={np.percentile(y_raw[train_idx], 90):.1f}h")
    print(f"(training on log1p-transformed target)")

    model = XGBRegressor(n_estimators=200, max_depth=6, learning_rate=0.05,
                         subsample=0.8, colsample_bytree=0.8, random_state=RANDOM_STATE,
                         n_jobs=-1)
    t0 = time.time()
    model.fit(X_train, y_train_log, verbose=False)
    fit_time = time.time() - t0

    y_pred_log = model.predict(X_test)
    y_pred = np.expm1(y_pred_log)

    r2 = r2_score(y_test_raw, y_pred)
    mae = mean_absolute_error(y_test_raw, y_pred)
    rmse = np.sqrt(mean_squared_error(y_test_raw, y_pred))

    naive_pred = np.full_like(y_test_raw, y_raw[train_idx].mean())
    naive_mae = mean_absolute_error(y_test_raw, naive_pred)
    naive_r2 = r2_score(y_test_raw, naive_pred)

    print(f"R²:  {r2:.4f}  (baseline naive: {naive_r2:.4f})")
    print(f"MAE: {mae:.1f}h (baseline naive: {naive_mae:.1f}h)")
    print(f"RMSE: {rmse:.1f}h")
    print(f"Fit: {fit_time:.1f}s")

    imp = sorted(zip(X.columns, model.feature_importances_), key=lambda x: -x[1])[:10]
    print("Top features:", ", ".join(f"{n}({v:.3f})" for n, v in imp))

    return {"target": "duracion_reparacion", "type": "regression",
            "r2": r2, "mae": mae, "rmse": rmse,
            "naive_mae": naive_mae, "naive_r2": naive_r2,
            "n_train": len(y_train_log), "n_test": len(y_test_raw)}


def evaluate_target_5(feat_df: pd.DataFrame, full_df: pd.DataFrame):
    """Target 5: Spike of >=10 correctivos in next month (binary, bus-month level)."""
    print("\n" + "=" * 70)
    print("TARGET 5: Pico de correctivos próximo mes (binary, >=10 en mes)")
    print("=" * 70)

    df = full_df.copy()
    corr = df[df["tipo_servicio"] == "CORRECTIVO"].copy()
    corr["ym"] = corr["fecha_evento"].dt.to_period("M")

    # Count per bus per month
    bpm = corr.groupby(["placa_patente", "ym"]).size().reset_index(name="n_corr")
    bpm["ym_dt"] = bpm["ym"].dt.to_timestamp()
    bpm = bpm.sort_values(["placa_patente", "ym_dt"])

    # Build features per bus-month
    rows = []
    for bus, bus_data in bpm.groupby("placa_patente"):
        bus_data = bus_data.sort_values("ym_dt")
        for i in range(len(bus_data)):
            current_ym = bus_data.iloc[i]["ym_dt"]
            past = bus_data.iloc[:i]
            n_past = len(past)

            if n_past == 0:
                feats = {
                    "n_corr_1m_ago": 0, "n_corr_2m_ago": 0, "n_corr_3m_ago": 0,
                    "avg_corr_3m": 0, "avg_corr_6m": 0,
                    "trend_3m_vs_6m": 0, "max_corr_ever": 0,
                    "months_with_data": 0,
                }
            else:
                avg_3m = past.tail(3)["n_corr"].mean() if len(past) >= 1 else 0
                avg_6m = past.tail(6)["n_corr"].mean() if len(past) >= 1 else 0

                feats = {
                    "n_corr_1m_ago": past.iloc[-1]["n_corr"] if len(past) >= 1 else 0,
                    "n_corr_2m_ago": past.iloc[-2]["n_corr"] if len(past) >= 2 else 0,
                    "n_corr_3m_ago": past.iloc[-3]["n_corr"] if len(past) >= 3 else 0,
                    "avg_corr_3m": avg_3m,
                    "avg_corr_6m": avg_6m,
                    "trend_3m_vs_6m": avg_3m / max(avg_6m, 0.01) - 1.0 if avg_6m > 0 else 0,
                    "max_corr_ever": past["n_corr"].max(),
                    "months_with_data": n_past,
                }

            feats["placa_patente"] = bus
            feats["ym_dt"] = current_ym
            feats["target"] = int(bus_data.iloc[i]["n_corr"] >= 10)
            rows.append(feats)

    data5 = pd.DataFrame(rows)

    X_cols = [c for c in data5.columns if c not in ("placa_patente", "ym_dt", "target")]
    X = data5[X_cols].fillna(0)
    y = data5["target"].values

    train_idx = data5["ym_dt"] < TRAIN_CUTOFF
    test_idx = data5["ym_dt"] >= TRAIN_CUTOFF

    X_train, X_test = X[train_idx], X[test_idx]
    y_train, y_test = y[train_idx], y[test_idx]

    pos_rate = y_train.mean()
    print(f"Train: {len(y_train):,} bus-months  |  Test: {len(y_test):,}")
    print(f"Positive rate (>=10 corr/mes): {pos_rate:.1%} ({y_train.sum():.0f} months)")

    model = XGBClassifier(n_estimators=200, max_depth=5, learning_rate=0.05,
                          subsample=0.8, colsample_bytree=0.8, random_state=RANDOM_STATE,
                          n_jobs=-1, scale_pos_weight=(1 - pos_rate) / max(pos_rate, 0.01))
    t0 = time.time()
    model.fit(X_train, y_train, verbose=False)
    fit_time = time.time() - t0

    y_pred = model.predict(X_test)
    y_proba = model.predict_proba(X_test)[:, 1]

    acc = accuracy_score(y_test, y_pred)
    f1 = f1_score(y_test, y_pred)
    auc = roc_auc_score(y_test, y_proba)

    naive_acc = max(pos_rate, 1 - pos_rate)

    print(f"Accuracy:  {acc:.4f}  (naive majority: {naive_acc:.4f})")
    print(f"F1:        {f1:.4f}")
    print(f"ROC-AUC:   {auc:.4f}")
    print(f"Fit: {fit_time:.1f}s")

    imp = sorted(zip(X_cols, model.feature_importances_), key=lambda x: -x[1])[:10]
    print("Top features:", ", ".join(f"{n}({v:.3f})" for n, v in imp))

    return {"target": "pico_correctivos_mes", "type": "binary",
            "accuracy": acc, "f1": f1, "roc_auc": auc,
            "naive_acc": naive_acc, "positive_rate": pos_rate,
            "n_train": len(y_train), "n_test": len(y_test)}


def evaluate_target_6(feat_df: pd.DataFrame, full_df: pd.DataFrame):
    """Target 6: Correctivo count per system+terminal next week (regression)."""
    print("\n" + "=" * 70)
    print("TARGET 6: Carga correctivos por sistema+terminal próxima semana (regression)")
    print("=" * 70)

    df = full_df.copy()
    corr = df[df["tipo_servicio"] == "CORRECTIVO"].copy()
    corr["fecha"] = corr["fecha_evento"].dt.date.astype("datetime64[ns]")
    corr["week"] = corr["fecha_evento"].dt.isocalendar().week.astype(int)
    corr["year"] = corr["fecha_evento"].dt.isocalendar().year.astype(int)
    corr["year_week"] = corr["year"].astype(str) + "-W" + corr["week"].astype(str).str.zfill(2)
    # Map to Monday date
    corr["week_dt"] = pd.to_datetime(corr["year_week"] + "-1", format="%Y-W%W-%w")

    grupo = corr.groupby(["causa_sistema_reconstruida", "taller_planta_grouped", "week_dt"]).size()
    grupo = grupo.reset_index(name="n_correctivos")
    grupo = grupo.sort_values(["causa_sistema_reconstruida", "taller_planta_grouped", "week_dt"])

    rows = []
    for (sistema, terminal), grp in grupo.groupby(["causa_sistema_reconstruida", "taller_planta_grouped"]):
        grp = grp.sort_values("week_dt")
        counts = grp["n_correctivos"].values
        weeks = grp["week_dt"].values

        for i in range(8, len(grp)):  # need at least 8 weeks of history
            feats = {
                "n_1w_ago": counts[i - 1],
                "n_2w_ago": counts[i - 2],
                "n_3w_ago": counts[i - 3],
                "n_4w_ago": counts[i - 4],
                "avg_4w": counts[i - 4 : i].mean(),
                "avg_8w": counts[max(0, i - 8) : i].mean(),
                "max_4w": counts[i - 4 : i].max(),
                "trend": counts[i - 4 : i].mean() / max(counts[max(0, i - 8) : i - 4].mean(), 0.01) - 1,
            }
            feats["sistema"] = sistema
            feats["terminal"] = terminal
            feats["week_dt"] = weeks[i]
            feats["target"] = counts[i]
            rows.append(feats)

    data6 = pd.DataFrame(rows)

    # One-hot encode
    data6 = pd.get_dummies(data6, columns=["sistema", "terminal"], drop_first=True)

    feat_cols = [c for c in data6.columns if c not in ("week_dt", "target")]
    X = data6[feat_cols].fillna(0)
    y = data6["target"].values

    train_idx = data6["week_dt"] < TRAIN_CUTOFF
    test_idx = data6["week_dt"] >= TRAIN_CUTOFF

    X_train, X_test = X[train_idx], X[test_idx]
    y_train, y_test = y[train_idx], y[test_idx]

    print(f"Train: {len(y_train):,} week-cells  |  Test: {len(y_test):,}")
    print(f"y mean={y_train.mean():.1f}, median={np.median(y_train):.1f}, max={y_train.max():.0f}")

    model = XGBRegressor(n_estimators=200, max_depth=5, learning_rate=0.05,
                         subsample=0.8, colsample_bytree=0.8, random_state=RANDOM_STATE,
                         n_jobs=-1)
    t0 = time.time()
    model.fit(X_train, y_train, verbose=False)
    fit_time = time.time() - t0

    y_pred = model.predict(X_test)
    y_pred = np.maximum(y_pred, 0)

    r2 = r2_score(y_test, y_pred)
    mae = mean_absolute_error(y_test, y_pred)
    rmse = np.sqrt(mean_squared_error(y_test, y_pred))

    naive_pred = np.full_like(y_test, y_train.mean())
    naive_mae = mean_absolute_error(y_test, naive_pred)
    naive_r2 = r2_score(y_test, naive_pred)

    print(f"R²:  {r2:.4f}  (baseline naive: {naive_r2:.4f})")
    print(f"MAE: {mae:.1f} events/week  (baseline naive: {naive_mae:.1f})")
    print(f"RMSE: {rmse:.1f}")
    print(f"Fit: {fit_time:.1f}s")

    imp = sorted(zip(feat_cols, model.feature_importances_), key=lambda x: -x[1])[:10]
    print("Top features:", ", ".join(f"{n}({v:.3f})" for n, v in imp))

    return {"target": "carga_sistema_terminal", "type": "regression",
            "r2": r2, "mae": mae, "rmse": rmse,
            "naive_mae": naive_mae, "naive_r2": naive_r2,
            "n_train": len(y_train), "n_test": len(y_test)}


# ═══════════════════════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    print("Cargando datos...")
    t0 = time.time()
    full_df = load_data()
    print(f"Datos cargados: {len(full_df):,} filas en {time.time() - t0:.1f}s")

    print("\nConstruyendo features por bus (event-level history)...")
    t0 = time.time()
    feat_df = build_bus_history(full_df)
    print(f"Features construidas: {len(feat_df):,} eventos en {time.time() - t0:.1f}s")

    results = []
    results.append(evaluate_target_1(feat_df, full_df))
    results.append(evaluate_target_2(feat_df, full_df))
    results.append(evaluate_target_3(feat_df, full_df))
    results.append(evaluate_target_4(feat_df, full_df))
    results.append(evaluate_target_5(feat_df, full_df))
    results.append(evaluate_target_6(feat_df, full_df))

    # Summary
    print("\n" + "=" * 70)
    print("RESUMEN")
    print("=" * 70)
    print(f"{'Target':<45s} {'Tipo':<12s} {'Métrica clave':<25s} {'Baseline':<12s} {'Train/Test':<15s}")
    print("-" * 110)

    for r in results:
        if r["type"] == "regression":
            metric = f"R²={r['r2']:.3f}, MAE={r['mae']:.1f}"
            baseline = f"R²={r['naive_r2']:.3f}"
        elif r["type"] == "binary":
            metric = f"ROC-AUC={r['roc_auc']:.3f}, F1={r['f1']:.3f}"
            baseline = f"Acc={r['naive_acc']:.3f}"
        else:  # multiclass
            metric = f"Acc={r['accuracy']:.3f}, F1m={r['f1_macro']:.3f}"
            baseline = f"Acc={r['naive_acc']:.3f}"

        size = f"{r['n_train']:,}/{r['n_test']:,}"
        print(f"{r['target']:<45s} {r['type']:<12s} {metric:<25s} {baseline:<12s} {size:<15s}")


if __name__ == "__main__":
    main()
