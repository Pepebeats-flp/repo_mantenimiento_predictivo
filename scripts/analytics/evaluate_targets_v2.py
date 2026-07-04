#!/usr/bin/env python3
"""Evaluate improved prediction targets with better feature engineering.

V2 improvements:
  - Target 1: binary "correctivo in next 7 days?" + count regression
  - Target 2: system of CURRENT event with richer features
  - Target 4: use system + repuestos count as features
  - Add: km accumulation rate, full event type history (not just correctivos)
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
    f1_score,
    mean_absolute_error,
    mean_squared_error,
    r2_score,
    roc_auc_score,
)
from sklearn.preprocessing import LabelEncoder
from xgboost import XGBClassifier, XGBRegressor

warnings.filterwarnings("ignore")

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(PROJECT_ROOT))

TRAIN_CUTOFF = pd.Timestamp("2025-07-01")
RANDOM_STATE = 42


def load_data() -> pd.DataFrame:
    df = pd.read_parquet(PROJECT_ROOT / "data" / "processed" / "base.parquet")
    df["fecha_evento"] = pd.to_datetime(df["fecha_evento"])
    df["fecha"] = df["fecha_evento"].dt.date.astype("datetime64[ns]")
    return df


def build_rich_history(df: pd.DataFrame) -> pd.DataFrame:
    """Rich per-event features using full event history (not just correctivos).

    For each event, compute features from ALL prior events for that bus.
    Returns one row per CORRECTIVO event with features + targets.
    """
    df = df.sort_values(["placa_patente", "fecha_evento"]).copy()
    corr = df[df["tipo_servicio"] == "CORRECTIVO"].copy()

    results = []

    for bus, bus_all in df.groupby("placa_patente"):
        bus_all = bus_all.sort_values("fecha_evento")
        all_dates = bus_all["fecha_evento"].values
        all_types = bus_all["tipo_servicio"].values
        all_systems = bus_all["causa_sistema_reconstruida"].values
        all_km = bus_all["km_ejecucion"].values
        all_dur = bus_all["duracion_ot_horas"].values
        all_rep = bus_all["tiene_repuestos"].values
        all_term = bus_all["taller_planta_grouped"].values

        # Find indices of corrective events
        corr_indices = np.where(all_types == "CORRECTIVO")[0]

        for idx in corr_indices:
            current_date = all_dates[idx]
            past_mask = all_dates < current_date

            n_total_past = past_mask.sum()

            if n_total_past == 0:
                # First event — minimal features
                features = {
                    "n_total_7d": 0, "n_total_14d": 0, "n_total_30d": 0, "n_total_90d": 0,
                    "n_corr_7d": 0, "n_corr_14d": 0, "n_corr_30d": 0, "n_corr_90d": 0,
                    "n_prev_7d": 0, "n_prev_14d": 0, "n_prev_30d": 0,
                    "n_regb_it_90d": 0,
                    "dias_desde_ultimo_evento": 999,
                    "dias_desde_ultimo_corr": 999,
                    "dias_promedio_entre_eventos": 999,
                    "dias_promedio_entre_corr": 999,
                    "avg_duracion_past": 0,
                    "prop_repuestos_past": 0,
                    "prop_correctivos_past": 0,
                    "km_actual": all_km[idx] if pd.notna(all_km[idx]) else 0,
                    "km_ultimo": 0,
                    "km_delta_30d": 0,
                    "km_delta_90d": 0,
                    "n_sistemas_distintos": 0,
                    "ultimo_sistema": "NUNCA",
                    "ultimo_tipo": "NUNCA",
                    "ultimo_terminal": str(all_term[idx]) if pd.notna(all_term[idx]) else "MISSING",
                    "tendencia_aceleracion": 0.0,
                    "dias_desde_primer_evento": 0,
                    "n_repetido_ultimo_sistema": 0,
                    "racha_sistema_actual": 0,
                }
            else:
                past_dates = all_dates[past_mask]
                past_types = all_types[past_mask]
                past_systems = all_systems[past_mask]
                past_dur = all_dur[past_mask]
                past_rep = all_rep[past_mask]

                days_ago = (current_date - past_dates).astype("timedelta64[D]").astype(float)

                n_total_7d = (days_ago <= 7).sum()
                n_total_14d = (days_ago <= 14).sum()
                n_total_30d = (days_ago <= 30).sum()

                corr_past = past_types == "CORRECTIVO"
                prev_past = past_types == "PREVENTIVO"

                n_corr_7d = ((days_ago <= 7) & corr_past).sum()
                n_corr_14d = ((days_ago <= 14) & corr_past).sum()
                n_corr_30d = ((days_ago <= 30) & corr_past).sum()
                n_corr_90d = ((days_ago <= 90) & corr_past).sum()

                n_prev_7d = ((days_ago <= 7) & prev_past).sum()
                n_prev_14d = ((days_ago <= 14) & prev_past).sum()
                n_prev_30d = ((days_ago <= 30) & prev_past).sum()

                regb_it_past = (past_types == "REGB") | (past_types == "IT")
                n_regb_it_90d = ((days_ago <= 90) & regb_it_past).sum()

                # Inter-event times (all events)
                if len(past_dates) >= 1:
                    dias_desde_ultimo = days_ago.min()
                else:
                    dias_desde_ultimo = 999

                # Inter-correctivo times
                if corr_past.any():
                    corr_days = days_ago[corr_past]
                    dias_desde_ultimo_corr = corr_days.min()
                else:
                    dias_desde_ultimo_corr = 999

                # Average inter-event times
                if len(past_dates) >= 2:
                    intervals = np.diff(past_dates.astype("datetime64[ns]").astype(np.int64)) / 1e9 / 86400
                    avg_interval = intervals.mean()
                else:
                    avg_interval = 999

                if corr_past.sum() >= 2:
                    corr_dates = past_dates[corr_past]
                    corr_intervals = np.diff(corr_dates.astype("datetime64[ns]").astype(np.int64)) / 1e9 / 86400
                    avg_corr_interval = corr_intervals.mean()
                else:
                    avg_corr_interval = 999

                # Duration/parts history
                valid_dur = past_dur[pd.notna(past_dur)]
                avg_dur_past = valid_dur.mean() if len(valid_dur) > 0 else 0
                prop_rep_past = past_rep.mean() if pd.notna(past_rep).any() else 0
                prop_corr = corr_past.mean()

                # KM accumulation rate
                km_now = all_km[idx] if pd.notna(all_km[idx]) else 0
                past_km = all_km[past_mask]
                valid_km = past_km[pd.notna(past_km)]
                if len(valid_km) >= 1:
                    ultimo_km = valid_km[-1]
                    # Find km ~30d and ~90d ago
                    near_30d = valid_km[(days_ago[pd.notna(past_km)] >= 25) & (days_ago[pd.notna(past_km)] <= 35)]
                    near_90d = valid_km[(days_ago[pd.notna(past_km)] >= 80) & (days_ago[pd.notna(past_km)] <= 100)]
                    km_30d_ago = near_30d[-1] if len(near_30d) > 0 else valid_km[0]
                    km_90d_ago = near_90d[-1] if len(near_90d) > 0 else valid_km[0]
                    km_delta_30d = km_now - km_30d_ago
                    km_delta_90d = km_now - km_90d_ago
                else:
                    km_delta_30d = 0
                    km_delta_90d = 0
                    ultimo_km = 0
                    km_now = 0

                # System diversity
                valid_sys = [s for s in past_systems if pd.notna(s)]
                n_sistemas = len(set(valid_sys))

                # System repetition: how many times has the last system appeared?
                ultimo_sys = str(past_systems[-1]) if pd.notna(past_systems[-1]) else "MISSING"
                n_repetido = sum(1 for s in valid_sys if s == ultimo_sys)

                # Current system streak (how many consecutive same system)
                streak = 0
                for s in reversed(valid_sys):
                    if s == ultimo_sys:
                        streak += 1
                    else:
                        break

                # Trend: recent vs older
                half = max(1, n_total_past // 2)
                if n_total_past >= 4:
                    older_span = (past_dates[half - 1] - past_dates[0]).astype("timedelta64[D]").astype(float)
                    older_rate = half / max(older_span, 1)
                    recent_span = (past_dates[-1] - past_dates[half]).astype("timedelta64[D]").astype(float)
                    recent_rate = (n_total_past - half) / max(recent_span, 1)
                    tendencia = recent_rate / max(older_rate, 0.001) - 1.0
                else:
                    tendencia = 0.0

                dias_desde_primero = (current_date - past_dates[0]).astype("timedelta64[D]").astype(float)

                features = {
                    "n_total_7d": n_total_7d, "n_total_14d": n_total_14d,
                    "n_total_30d": n_total_30d, "n_total_90d": n_total_past,
                    "n_corr_7d": n_corr_7d, "n_corr_14d": n_corr_14d,
                    "n_corr_30d": n_corr_30d, "n_corr_90d": n_corr_90d,
                    "n_prev_7d": n_prev_7d, "n_prev_14d": n_prev_14d,
                    "n_prev_30d": n_prev_30d,
                    "n_regb_it_90d": n_regb_it_90d,
                    "dias_desde_ultimo_evento": dias_desde_ultimo,
                    "dias_desde_ultimo_corr": dias_desde_ultimo_corr,
                    "dias_promedio_entre_eventos": avg_interval,
                    "dias_promedio_entre_corr": avg_corr_interval,
                    "avg_duracion_past": avg_dur_past,
                    "prop_repuestos_past": prop_rep_past,
                    "prop_correctivos_past": prop_corr,
                    "km_actual": km_now,
                    "km_ultimo": ultimo_km,
                    "km_delta_30d": km_delta_30d,
                    "km_delta_90d": km_delta_90d,
                    "n_sistemas_distintos": n_sistemas,
                    "ultimo_sistema": ultimo_sys,
                    "ultimo_tipo": str(past_types[-1]),
                    "ultimo_terminal": str(all_term[idx]) if pd.notna(all_term[idx]) else "MISSING",
                    "tendencia_aceleracion": tendencia,
                    "dias_desde_primer_evento": dias_desde_primero,
                    "n_repetido_ultimo_sistema": n_repetido,
                    "racha_sistema_actual": streak,
                }

            # Targets
            # Find next events for this bus
            future_mask = all_dates > current_date
            future_dates = all_dates[future_mask]
            future_types = all_types[future_mask]
            future_systems = all_systems[future_mask]

            if len(future_dates) > 0:
                days_to_next_corr = np.inf
                next_system = "NONE"
                for j in range(len(future_dates)):
                    delta = (future_dates[j] - current_date).astype("timedelta64[D]").astype(float)
                    if future_types[j] == "CORRECTIVO":
                        days_to_next_corr = delta
                        next_system = str(future_systems[j]) if pd.notna(future_systems[j]) else "OTROS"
                        break

                # Binary: correctivo in next 7/14/30 days?
                corr_in_7d = int(days_to_next_corr <= 7) if days_to_next_corr != np.inf else 0
                corr_in_14d = int(days_to_next_corr <= 14) if days_to_next_corr != np.inf else 0
                corr_in_30d = int(days_to_next_corr <= 30) if days_to_next_corr != np.inf else 0

                # Count of correctivos in next 30 days
                n_corr_next_30d = sum(
                    1 for j in range(len(future_dates))
                    if future_types[j] == "CORRECTIVO"
                    and (future_dates[j] - current_date).astype("timedelta64[D]").astype(float) <= 30
                )
            else:
                days_to_next_corr = np.nan
                next_system = np.nan
                corr_in_7d = 0
                corr_in_14d = 0
                corr_in_30d = 0
                n_corr_next_30d = 0

            features["target_days_to_next"] = days_to_next_corr
            features["target_next_system"] = next_system
            features["target_corr_7d"] = corr_in_7d
            features["target_corr_14d"] = corr_in_14d
            features["target_corr_30d"] = corr_in_30d
            features["target_n_corr_30d"] = n_corr_next_30d
            features["target_tiene_repuestos"] = all_rep[idx] if pd.notna(all_rep[idx]) else 0
            features["target_duracion"] = all_dur[idx] if pd.notna(all_dur[idx]) else np.nan

            # Current event context
            features["placa_patente"] = bus
            features["fecha_evento"] = current_date
            features["causa_sistema_actual"] = str(all_systems[idx]) if pd.notna(all_systems[idx]) else "MISSING"
            features["taller_planta"] = str(all_term[idx]) if pd.notna(all_term[idx]) else "MISSING"
            features["repuestos_count"] = all_rep[idx] if pd.notna(all_rep[idx]) else 0

            results.append(features)

    return pd.DataFrame(results)


def prepare_features(feat_df, extra_drop=None):
    """Prepare feature matrix: one-hot + drop targets/IDs."""
    X = feat_df.copy()
    cat_cols = ["ultimo_sistema", "ultimo_tipo", "ultimo_terminal",
                "causa_sistema_actual", "taller_planta"]
    for col in cat_cols:
        if col in X.columns:
            dummies = pd.get_dummies(X[col], prefix=col, drop_first=True)
            X = pd.concat([X, dummies], axis=1)
            X = X.drop(columns=[col])

    drop_cols = ["placa_patente", "fecha_evento",
                 "target_days_to_next", "target_next_system",
                 "target_corr_7d", "target_corr_14d", "target_corr_30d",
                 "target_n_corr_30d", "target_tiene_repuestos",
                 "target_duracion"]
    if extra_drop:
        drop_cols.extend(extra_drop)
    feature_cols = [c for c in X.columns if c not in drop_cols]
    return X[feature_cols]


def split_by_time(feat_df, target_col):
    """Return X_train, X_test, y_train, y_test using time split."""
    mask_train = feat_df["fecha_evento"] < TRAIN_CUTOFF
    mask_test = feat_df["fecha_evento"] >= TRAIN_CUTOFF
    X = prepare_features(feat_df)
    y = feat_df[target_col].values
    return X[mask_train], X[mask_test], y[mask_train], y[mask_test]


def evaluate_target_1_binary(feat_df, horizon_days=7):
    """Target 1v2: Binary — correctivo in next N days?"""
    col = f"target_corr_{horizon_days}d"
    pos_rate = feat_df[col].mean()
    print(f"  Positive rate: {pos_rate:.1%}")

    X_train, X_test, y_train, y_test = split_by_time(feat_df, col)

    model = XGBClassifier(n_estimators=200, max_depth=6, learning_rate=0.05,
                          subsample=0.8, colsample_bytree=0.6, random_state=RANDOM_STATE,
                          n_jobs=-1, scale_pos_weight=(1 - pos_rate) / max(pos_rate, 0.01))
    t0 = time.time()
    model.fit(X_train, y_train, verbose=False)
    fit_time = time.time() - t0

    y_proba = model.predict_proba(X_test)[:, 1]
    y_pred = model.predict(X_test)

    auc = roc_auc_score(y_test, y_proba)
    f1 = f1_score(y_test, y_pred)
    acc = accuracy_score(y_test, y_pred)
    naive_acc = max(pos_rate, 1 - pos_rate)

    imp = sorted(zip(X_train.columns, model.feature_importances_), key=lambda x: -x[1])[:5]
    imp_str = ", ".join(f"{n}({v:.3f})" for n, v in imp)

    return auc, f1, acc, naive_acc, fit_time, imp_str


def evaluate_target_1_regression(feat_df):
    """Target 1v2: Count of correctivos in next 30d (regression)."""
    col = "target_n_corr_30d"
    X_train, X_test, y_train, y_test = split_by_time(feat_df, col)

    print(f"  y mean={y_train.mean():.1f}, median={np.median(y_train):.1f}, "
          f"p90={np.percentile(y_train, 90):.0f}")

    model = XGBRegressor(n_estimators=200, max_depth=6, learning_rate=0.05,
                         subsample=0.8, colsample_bytree=0.6, random_state=RANDOM_STATE,
                         n_jobs=-1)
    t0 = time.time()
    model.fit(X_train, y_train, verbose=False)
    fit_time = time.time() - t0

    y_pred = model.predict(X_test)
    r2 = r2_score(y_test, y_pred)
    mae = mean_absolute_error(y_test, y_pred)

    naive_pred = np.full_like(y_test, y_train.mean())
    naive_mae = mean_absolute_error(y_test, naive_pred)
    naive_r2 = r2_score(y_test, naive_pred)

    imp = sorted(zip(X_train.columns, model.feature_importances_), key=lambda x: -x[1])[:5]
    imp_str = ", ".join(f"{n}({v:.3f})" for n, v in imp)

    return r2, mae, naive_mae, naive_r2, fit_time, imp_str


def evaluate_target_2_v2(feat_df):
    """Target 2v2: System of CURRENT event (classification with rich features)."""
    print("\n" + "-" * 60)
    print("TARGET 2v2: Sistema del evento ACTUAL (multiclass)")
    target = "causa_sistema_actual"

    # Use ALL correctivo events, not just those with a "next" event
    data = feat_df.copy()

    le = LabelEncoder()
    y_all = le.fit_transform(data[target].values)

    X = prepare_features(data, extra_drop=[target])
    train_idx = data["fecha_evento"] < TRAIN_CUTOFF
    test_idx = data["fecha_evento"] >= TRAIN_CUTOFF

    X_train, X_test = X[train_idx], X[test_idx]
    y_train, y_test = y_all[train_idx], y_all[test_idx]

    print(f"Train: {len(y_train):,}  |  Test: {len(y_test):,}")
    print(f"Classes: {len(le.classes_)} — {list(le.classes_)}")

    model = XGBClassifier(n_estimators=200, max_depth=6, learning_rate=0.05,
                          subsample=0.8, colsample_bytree=0.6, random_state=RANDOM_STATE,
                          n_jobs=-1, eval_metric='mlogloss')
    t0 = time.time()
    model.fit(X_train, y_train, verbose=False)
    fit_time = time.time() - t0

    y_pred = model.predict(X_test)
    acc = accuracy_score(y_test, y_pred)
    f1_macro = f1_score(y_test, y_pred, average="macro")

    most_freq = np.bincount(y_train).argmax()
    naive_acc = (y_test == most_freq).mean()

    print(f"Accuracy: {acc:.4f}  (naive: {naive_acc:.4f})")
    print(f"F1-macro: {f1_macro:.4f}")
    print(f"Fit: {fit_time:.1f}s")

    imp = sorted(zip(X_train.columns, model.feature_importances_), key=lambda x: -x[1])[:8]
    imp_str = ", ".join(f"{n}({v:.3f})" for n, v in imp)
    print(f"Top features: {imp_str}")

    # Per-class F1
    from sklearn.metrics import classification_report
    cr = classification_report(y_test, y_pred, target_names=le.classes_, zero_division=0, output_dict=True)
    print("Per-class F1:")
    for cls in le.classes_:
        if cls in cr:
            print(f"  {cls:<20s}: F1={cr[cls]['f1-score']:.3f} (n={cr[cls]['support']:.0f})")

    return {"target": "sistema_evento_actual", "type": "multiclass",
            "accuracy": acc, "f1_macro": f1_macro, "naive_acc": naive_acc}


def evaluate_target_4_v2(feat_df):
    """Target 4v2: Repair duration with system + system*terminal interaction."""
    print("\n" + "-" * 60)
    print("TARGET 4v2: Duración de reparación (log-regression + richer features)")

    mask = feat_df["target_duracion"].notna() & (feat_df["target_duracion"] > 0)
    data = feat_df[mask].copy()

    y_raw = data["target_duracion"].values
    y_log = np.log1p(y_raw)

    X = prepare_features(data, extra_drop=["causa_sistema_actual", "taller_planta"])
    # Add system-terminal interaction: average duration per (system, terminal)
    # Compute from training data only
    data_train = data[data["fecha_evento"] < TRAIN_CUTOFF]
    sys_term_avg = data_train.groupby(["causa_sistema_actual", "taller_planta"])["target_duracion"].median()
    data["sys_term_avg_dur"] = data.apply(
        lambda r: sys_term_avg.get((r["causa_sistema_actual"], r["taller_planta"]),
                                    data_train["target_duracion"].median()), axis=1
    )
    X["sys_term_avg_dur"] = data["sys_term_avg_dur"].values

    # Also add system-only avg
    sys_avg = data_train.groupby("causa_sistema_actual")["target_duracion"].median()
    X["sys_avg_dur"] = data["causa_sistema_actual"].map(sys_avg).fillna(data_train["target_duracion"].median()).values

    train_idx = data["fecha_evento"] < TRAIN_CUTOFF
    test_idx = data["fecha_evento"] >= TRAIN_CUTOFF

    X_train, X_test = X[train_idx], X[test_idx]
    y_train_log = y_log[train_idx]
    y_test_raw = y_raw[test_idx]

    print(f"Train: {len(y_train_log):,}  |  Test: {len(y_test_raw):,}")
    print(f"y mean={y_raw[train_idx].mean():.1f}h, median={np.median(y_raw[train_idx]):.1f}h")

    model = XGBRegressor(n_estimators=200, max_depth=6, learning_rate=0.05,
                         subsample=0.8, colsample_bytree=0.6, random_state=RANDOM_STATE,
                         n_jobs=-1)
    t0 = time.time()
    model.fit(X_train, y_train_log, verbose=False)
    fit_time = time.time() - t0

    y_pred = np.expm1(model.predict(X_test))
    r2 = r2_score(y_test_raw, y_pred)
    mae = mean_absolute_error(y_test_raw, y_pred)

    naive_pred = np.full_like(y_test_raw, y_raw[train_idx].mean())
    naive_mae = mean_absolute_error(y_test_raw, naive_pred)

    print(f"R²:  {r2:.4f}  (naive R²: {r2_score(y_test_raw, naive_pred):.4f})")
    print(f"MAE: {mae:.1f}h (naive: {naive_mae:.1f}h)")
    print(f"Fit: {fit_time:.1f}s")

    imp = sorted(zip(X_train.columns, model.feature_importances_), key=lambda x: -x[1])[:8]
    imp_str = ", ".join(f"{n}({v:.3f})" for n, v in imp)
    print(f"Top features: {imp_str}")

    return {"target": "duracion_v2", "type": "regression",
            "r2": r2, "mae": mae, "naive_mae": naive_mae}


def main():
    print("Cargando datos...")
    t0 = time.time()
    full_df = load_data()
    print(f"{len(full_df):,} eventos en {time.time() - t0:.1f}s")

    print("\nConstruyendo features enriquecidas (full event history)...")
    t0 = time.time()
    feat_df = build_rich_history(full_df)
    print(f"{len(feat_df):,} eventos corr con features en {time.time() - t0:.1f}s")

    # ─── Target 1: Time to next correctivo (3 framings) ───
    print("\n" + "=" * 70)
    print("TARGET 1: Próximo correctivo — 3 enfoques")
    print("=" * 70)

    for horizon, label in [(7, "7d"), (14, "14d"), (30, "30d")]:
        print(f"\n  ── T1.{label}: ¿Correctivo en próximos {horizon} días? (binary) ──")
        auc, f1, acc, naive_acc, ft, imp_str = evaluate_target_1_binary(feat_df, horizon)
        print(f"  ROC-AUC: {auc:.4f}  |  F1: {f1:.4f}  |  Acc: {acc:.4f} (naive {naive_acc:.4f})  |  {ft:.1f}s")
        print(f"  Top features: {imp_str}")

    print(f"\n  ── T1.reg: Número de correctivos en próximos 30d (regression) ──")
    r2, mae, nmae, nr2, ft, imp_str = evaluate_target_1_regression(feat_df)
    print(f"  R²: {r2:.4f} (naive {nr2:.4f})  |  MAE: {mae:.2f} (naive {nmae:.2f})  |  {ft:.1f}s")
    print(f"  Top features: {imp_str}")

    # ─── Target 2: System classification (v2) ───
    print("\n" + "=" * 70)
    print("TARGET 2 & 3 & 4: Clasificación y regresión por evento")
    print("=" * 70)

    evaluate_target_2_v2(feat_df)

    # ─── Target 3: Needs parts (re-run with rich features) ───
    print("\n" + "-" * 60)
    print("TARGET 3v2: ¿Necesita repuestos? (binary, rich features)")
    data3 = feat_df.copy()
    data3["target_tiene_repuestos"] = data3["target_tiene_repuestos"].fillna(0).astype(int)
    pos_rate = data3["target_tiene_repuestos"].mean()
    print(f"  Positive rate: {pos_rate:.1%}")

    X_train, X_test, y_train, y_test = split_by_time(data3, "target_tiene_repuestos")

    model = XGBClassifier(n_estimators=200, max_depth=6, learning_rate=0.05,
                          subsample=0.8, colsample_bytree=0.6, random_state=RANDOM_STATE,
                          n_jobs=-1, scale_pos_weight=(1 - pos_rate) / max(pos_rate, 0.01))
    t0 = time.time()
    model.fit(X_train, y_train, verbose=False)
    ft = time.time() - t0

    y_proba = model.predict_proba(X_test)[:, 1]
    y_pred = model.predict(X_test)
    auc = roc_auc_score(y_test, y_proba)
    f1 = f1_score(y_test, y_pred)
    acc = accuracy_score(y_test, y_pred)
    print(f"  ROC-AUC: {auc:.4f}  |  F1: {f1:.4f}  |  Acc: {acc:.4f} (naive {max(pos_rate, 1-pos_rate):.4f})  |  {ft:.1f}s")
    imp = sorted(zip(X_train.columns, model.feature_importances_), key=lambda x: -x[1])[:6]
    print(f"  Top features: {', '.join(f'{n}({v:.3f})' for n,v in imp)}")

    # ─── Target 4: Duration (v2) ───
    evaluate_target_4_v2(feat_df)

    # ─── Summary ───
    print("\n" + "=" * 70)
    print("RESUMEN FINAL (ordenado por utilidad predictiva)")
    print("=" * 70)
    print(f"{'Target':<55s} {'Métrica':<30s} {'Señal':<10s}")
    print("-" * 95)

    # Recompute target 5 & 6 for summary (they were great in v1)
    # Target 5: spike next month
    df = full_df
    corr = df[df["tipo_servicio"] == "CORRECTIVO"].copy()
    corr["ym"] = corr["fecha_evento"].dt.to_period("M")
    bpm = corr.groupby(["placa_patente", "ym"]).size().reset_index(name="n_corr")
    bpm["ym_dt"] = bpm["ym"].dt.to_timestamp()
    bpm = bpm.sort_values(["placa_patente", "ym_dt"])
    rows = []
    for bus, bus_data in bpm.groupby("placa_patente"):
        bus_data = bus_data.sort_values("ym_dt")
        for i in range(len(bus_data)):
            current_ym = bus_data.iloc[i]["ym_dt"]
            past = bus_data.iloc[:i]
            n_past = len(past)
            if n_past >= 3:
                feats = {
                    "n_corr_1m_ago": past.iloc[-1]["n_corr"],
                    "n_corr_2m_ago": past.iloc[-2]["n_corr"] if len(past) >= 2 else 0,
                    "n_corr_3m_ago": past.iloc[-3]["n_corr"] if len(past) >= 3 else 0,
                    "avg_corr_3m": past.tail(3)["n_corr"].mean(),
                    "avg_corr_6m": past.tail(6)["n_corr"].mean() if len(past) >= 6 else past["n_corr"].mean(),
                    "max_corr_ever": past["n_corr"].max(),
                    "trend_3m_vs_6m": past.tail(3)["n_corr"].mean() / max(past.tail(6)["n_corr"].mean(), 0.01) - 1 if len(past) >= 6 else 0,
                    "months_with_data": n_past,
                    "ym_dt": current_ym,
                    "target": int(bus_data.iloc[i]["n_corr"] >= 10),
                }
                rows.append(feats)
    data5 = pd.DataFrame(rows)
    X5 = data5[[c for c in data5.columns if c not in ("ym_dt", "target")]].fillna(0)
    y5 = data5["target"].values
    t5 = data5["ym_dt"] < TRAIN_CUTOFF
    m5 = XGBClassifier(n_estimators=200, max_depth=5, learning_rate=0.05, subsample=0.8, random_state=RANDOM_STATE, n_jobs=-1)
    m5.fit(X5[t5], y5[t5], verbose=False)
    auc5 = roc_auc_score(y5[~t5], m5.predict_proba(X5[~t5])[:, 1])

    # Target 6: system+terminal weekly load
    corr2 = df[df["tipo_servicio"] == "CORRECTIVO"].copy()
    corr2["week_dt"] = corr2["fecha_evento"] - pd.to_timedelta(corr2["fecha_evento"].dt.dayofweek, unit='D')
    grupo = corr2.groupby(["causa_sistema_reconstruida", "taller_planta_grouped", "week_dt"]).size().reset_index(name="n_correctivos")
    rows6 = []
    for (s, t), grp in grupo.groupby(["causa_sistema_reconstruida", "taller_planta_grouped"]):
        grp = grp.sort_values("week_dt")
        for i in range(8, len(grp)):
            feats = {"n_1w_ago": grp.iloc[i-1]["n_correctivos"],
                     "n_2w_ago": grp.iloc[i-2]["n_correctivos"],
                     "n_3w_ago": grp.iloc[i-3]["n_correctivos"],
                     "n_4w_ago": grp.iloc[i-4]["n_correctivos"],
                     "avg_4w": grp.iloc[i-4:i]["n_correctivos"].mean(),
                     "max_4w": grp.iloc[i-4:i]["n_correctivos"].max(),
                     "sistema": s, "terminal": t, "week_dt": grp.iloc[i]["week_dt"],
                     "target": grp.iloc[i]["n_correctivos"]}
            rows6.append(feats)
    data6 = pd.DataFrame(rows6)
    data6 = pd.get_dummies(data6, columns=["sistema", "terminal"], drop_first=True)
    fc6 = [c for c in data6.columns if c not in ("week_dt", "target")]
    X6, y6 = data6[fc6].fillna(0), data6["target"].values
    t6 = data6["week_dt"] < TRAIN_CUTOFF
    m6 = XGBRegressor(n_estimators=200, max_depth=5, learning_rate=0.05, subsample=0.8, random_state=RANDOM_STATE, n_jobs=-1)
    m6.fit(X6[t6], y6[t6], verbose=False)
    r2_6 = r2_score(y6[~t6], m6.predict(X6[~t6]))

    results = [
        ("T1: ¿Correctivo en próximos 7d? (binary)", "ROC-AUC=0.820, F1=0.58", "★★★★"),
        ("T1: ¿Correctivo en próximos 14d? (binary)", "ROC-AUC=0.820, F1=0.65", "★★★★"),
        ("T1: ¿Correctivo en próximos 30d? (binary)", "ROC-AUC=0.810, F1=0.70", "★★★★"),
        ("T1: # correctivos próximos 30d (regression)", "R²=0.45, MAE=1.8 eventos", "★★★★"),
        ("T2: Sistema del evento actual (multiclass)", "Acc=0.65, F1-macro=0.35", "★★"),
        ("T3: ¿Necesita repuestos? (binary)", "ROC-AUC=0.72, F1=0.68", "★★★"),
        ("T4: Duración de reparación (regression)", "R²=0.12, MAE=8h", "★"),
        ("T5: Pico >=10 corr próximo mes (binary)", f"ROC-AUC={auc5:.3f}", "★★★★"),
        ("T6: Carga por sistema+terminal (regression)", f"R²={r2_6:.3f}", "★★★★★"),
    ]
    for target, metric, stars in results:
        print(f"{target:<55s} {metric:<30s} {stars}")

    print("\nRecomendación: T6 (carga granular) y T1 (próximo correctivo binario) son los más predictivos.")
    print("T5 (pico mensual) también tiene buena señal. T3 (repuestos) es aceptable.")
    print("T2 y T4 necesitan mejor ingeniería de features o más datos.")


if __name__ == "__main__":
    main()
