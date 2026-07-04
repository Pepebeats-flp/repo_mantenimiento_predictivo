#!/usr/bin/env python3
"""Evaluate prediction targets — V3: fixed data leaks, cleaner features.

Targets:
  1a. ¿Correctivo in next 7/14/30 days? (binary, bus-event level)
  1b. # correctivos in next 30 days (regression, bus-event level)
  2.  System of NEXT correctivo (multiclass)
  3.  ¿Current evento needs repuestos? (binary, using only pre-event history)
  4.  Repair duration (regression)
  5.  Spike >=10 corr next month (binary, bus-month level)
  6.  System+terminal weekly load (regression)
"""
from __future__ import annotations

import sys
import time
import warnings
from pathlib import Path
from collections import defaultdict

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


def build_features(full_df: pd.DataFrame) -> pd.DataFrame:
    """Build per-correctivo-event features from PRE-EVENT history only.

    For each corrective event, compute features using ALL prior events
    for that bus. Never use current event attributes to predict current
    event targets (except context like terminal, which is known at entry).
    """
    df = full_df.sort_values(["placa_patente", "fecha_evento"]).copy()
    corr = df[df["tipo_servicio"] == "CORRECTIVO"].copy()

    rows = []
    for bus, bus_all in df.groupby("placa_patente"):
        bus_all = bus_all.sort_values("fecha_evento")
        dates = bus_all["fecha_evento"].values
        types = bus_all["tipo_servicio"].values
        systems = bus_all["causa_sistema_reconstruida"].values
        km_vals = bus_all["km_ejecucion"].values
        dur_vals = bus_all["duracion_ot_horas"].values
        rep_vals = bus_all["tiene_repuestos"].values
        term_vals = bus_all["taller_planta_grouped"].values

        corr_idx = np.where(types == "CORRECTIVO")[0]

        for idx in corr_idx:
            current_date = dates[idx]
            past_mask = dates < current_date

            n_past = past_mask.sum()

            if n_past == 0:
                feats = {f: 0 for f in [
                    "n_total_7d", "n_total_14d", "n_total_30d",
                    "n_corr_7d", "n_corr_14d", "n_corr_30d", "n_corr_90d",
                    "n_prev_7d", "n_prev_14d", "n_prev_30d",
                    "n_regb_it_90d",
                    "dias_desde_ultimo", "dias_desde_ultimo_corr",
                    "dias_promedio_entre", "dias_promedio_entre_corr",
                    "avg_duracion_past", "prop_repuestos_past",
                    "n_sistemas_distintos", "n_repetido_ultimo_sistema",
                    "racha_sistema_actual",
                    "km_delta_30d", "km_delta_90d",
                    "tendencia_aceleracion",
                    "dias_desde_primer_evento",
                ]}
                feats.update({"dias_desde_ultimo": 999,
                              "dias_desde_ultimo_corr": 999,
                              "dias_promedio_entre": 999,
                              "dias_promedio_entre_corr": 999})
                feats["ultimo_sistema"] = "NUNCA"
                feats["ultimo_tipo"] = "NUNCA"
                feats["ultimo_terminal"] = str(term_vals[idx]) if pd.notna(term_vals[idx]) else "MISSING"
                feats["km_actual"] = km_vals[idx] if pd.notna(km_vals[idx]) else 0

            else:
                pdates = dates[past_mask]
                ptypes = types[past_mask]
                psystems = systems[past_mask]
                pdur = dur_vals[past_mask]
                prep = rep_vals[past_mask]

                days_ago = (current_date - pdates).astype("timedelta64[D]").astype(float)

                is_corr = ptypes == "CORRECTIVO"
                is_prev = ptypes == "PREVENTIVO"
                is_regb_it = (ptypes == "REGB") | (ptypes == "IT")

                n_corr_7d = int(((days_ago <= 7) & is_corr).sum())
                n_corr_14d = int(((days_ago <= 14) & is_corr).sum())
                n_corr_30d = int(((days_ago <= 30) & is_corr).sum())
                n_corr_90d = int(((days_ago <= 90) & is_corr).sum())

                dias_desde_ultimo = days_ago.min()
                dias_desde_ultimo_corr = days_ago[is_corr].min() if is_corr.any() else 999

                # Avg intervals
                if n_past >= 2:
                    intervals = np.diff(pdates.astype("datetime64[ns]").astype(np.int64)) / 1e9 / 86400
                    avg_interval = intervals.mean()
                else:
                    avg_interval = 999

                if is_corr.sum() >= 2:
                    cdates = pdates[is_corr]
                    cintervals = np.diff(cdates.astype("datetime64[ns]").astype(np.int64)) / 1e9 / 86400
                    avg_corr = cintervals.mean()
                else:
                    avg_corr = 999

                valid_dur = pdur[pd.notna(pdur)]
                avg_dur = valid_dur.mean() if len(valid_dur) > 0 else 0

                prop_rep = float(prep.mean()) if pd.notna(prep).any() else 0.0

                # KM deltas
                km_now = km_vals[idx] if pd.notna(km_vals[idx]) else 0
                pkm = km_vals[past_mask]
                valid_km_mask = pd.notna(pkm)
                vkm = pkm[valid_km_mask]
                vdays = days_ago[valid_km_mask]
                if len(vkm) >= 2:
                    near_30 = vkm[(vdays >= 25) & (vdays <= 35)]
                    near_90 = vkm[(vdays >= 80) & (vdays <= 100)]
                    km_30 = near_30[-1] if len(near_30) > 0 else vkm[0]
                    km_90 = near_90[-1] if len(near_90) > 0 else vkm[0]
                    km_d30 = km_now - km_30
                    km_d90 = km_now - km_90
                else:
                    km_d30 = 0
                    km_d90 = 0

                # System patterns
                valid_sys = [s for s in psystems if pd.notna(s)]
                n_sistemas = len(set(valid_sys))
                ultimo_sys = str(psystems[-1]) if pd.notna(psystems[-1]) else "MISSING"
                n_repetido = sum(1 for s in valid_sys if s == ultimo_sys)
                streak = 0
                for s in reversed(valid_sys):
                    if s == ultimo_sys:
                        streak += 1
                    else:
                        break

                # Trend
                if n_past >= 4:
                    half = max(1, n_past // 2)
                    older_span = (pdates[half - 1] - pdates[0]).astype("timedelta64[D]").astype(float)
                    recent_span = (pdates[-1] - pdates[half]).astype("timedelta64[D]").astype(float)
                    older_rate = half / max(older_span, 1)
                    recent_rate = (n_past - half) / max(recent_span, 1)
                    tendencia = recent_rate / max(older_rate, 0.001) - 1.0
                else:
                    tendencia = 0.0

                dias_primero = (current_date - pdates[0]).astype("timedelta64[D]").astype(float)

                feats = {
                    "n_total_7d": int(((days_ago <= 7).sum())),
                    "n_total_14d": int(((days_ago <= 14).sum())),
                    "n_total_30d": int(((days_ago <= 30).sum())),
                    "n_corr_7d": n_corr_7d,
                    "n_corr_14d": n_corr_14d,
                    "n_corr_30d": n_corr_30d,
                    "n_corr_90d": n_corr_90d,
                    "n_prev_7d": int(((days_ago <= 7) & is_prev).sum()),
                    "n_prev_14d": int(((days_ago <= 14) & is_prev).sum()),
                    "n_prev_30d": int(((days_ago <= 30) & is_prev).sum()),
                    "n_regb_it_90d": int(((days_ago <= 90) & is_regb_it).sum()),
                    "dias_desde_ultimo": dias_desde_ultimo,
                    "dias_desde_ultimo_corr": dias_desde_ultimo_corr,
                    "dias_promedio_entre": avg_interval,
                    "dias_promedio_entre_corr": avg_corr,
                    "avg_duracion_past": avg_dur,
                    "prop_repuestos_past": prop_rep,
                    "n_sistemas_distintos": n_sistemas,
                    "n_repetido_ultimo_sistema": n_repetido,
                    "racha_sistema_actual": streak,
                    "km_delta_30d": km_d30,
                    "km_delta_90d": km_d90,
                    "tendencia_aceleracion": tendencia,
                    "dias_desde_primer_evento": dias_primero,
                    "ultimo_sistema": ultimo_sys,
                    "ultimo_tipo": str(ptypes[-1]),
                    "ultimo_terminal": str(term_vals[idx]) if pd.notna(term_vals[idx]) else "MISSING",
                    "km_actual": km_now,
                }

            # ── Targets ──
            future_mask = dates > current_date
            fdates = dates[future_mask]
            ftypes = types[future_mask]
            fsystems = systems[future_mask]

            # T1: days to next correctivo, binary flags, count
            corr_future = ftypes == "CORRECTIVO"
            if corr_future.any():
                deltas = (fdates[corr_future] - current_date).astype("timedelta64[D]").astype(float)
                days_to_next = deltas[0]
            else:
                days_to_next = np.nan

            # T1 binary: correctivo in next N days?
            corr_in_7d = int(days_to_next <= 7) if not np.isnan(days_to_next) else 0
            corr_in_14d = int(days_to_next <= 14) if not np.isnan(days_to_next) else 0
            corr_in_30d = int(days_to_next <= 30) if not np.isnan(days_to_next) else 0

            # T1b: count in next 30d
            if len(fdates) > 0:
                n_corr_next_30d = int((
                    (fdates - current_date).astype("timedelta64[D]").astype(float) <= 30
                ).sum())
            else:
                n_corr_next_30d = 0

            # T2: system of NEXT corrective
            if corr_future.any():
                next_sys = str(fsystems[corr_future][0]) if pd.notna(fsystems[corr_future][0]) else "OTROS"
            else:
                next_sys = np.nan

            # T3 & T4: current event attributes (these are targets, NOT features)
            rep_actual = rep_vals[idx] if pd.notna(rep_vals[idx]) else 0
            dur_actual = dur_vals[idx] if pd.notna(dur_vals[idx]) else np.nan

            feats["target_days_to_next"] = days_to_next
            feats["target_corr_7d"] = corr_in_7d
            feats["target_corr_14d"] = corr_in_14d
            feats["target_corr_30d"] = corr_in_30d
            feats["target_n_corr_30d"] = n_corr_next_30d
            feats["target_next_system"] = next_sys
            feats["target_tiene_repuestos"] = rep_actual
            feats["target_duracion"] = dur_actual

            feats["placa_patente"] = bus
            feats["fecha_evento"] = current_date
            # Current event context (known at event time, NOT targets)
            feats["sistema_actual"] = str(systems[idx]) if pd.notna(systems[idx]) else "MISSING"
            feats["terminal_actual"] = str(term_vals[idx]) if pd.notna(term_vals[idx]) else "MISSING"

            rows.append(feats)

    return pd.DataFrame(rows)


def to_features(feat_df, extra_drop=None):
    """Convert event-level DataFrame to X matrix."""
    X = feat_df.copy()
    cat_cols = ["ultimo_sistema", "ultimo_tipo", "ultimo_terminal",
                "sistema_actual", "terminal_actual"]
    for col in cat_cols:
        if col in X.columns:
            dummies = pd.get_dummies(X[col].astype(str), prefix=col, drop_first=True)
            X = pd.concat([X, dummies], axis=1)
            X = X.drop(columns=[col])

    drop_cols = ["placa_patente", "fecha_evento",
                 "target_days_to_next", "target_corr_7d", "target_corr_14d",
                 "target_corr_30d", "target_n_corr_30d",
                 "target_next_system", "target_tiene_repuestos", "target_duracion"]
    if extra_drop:
        drop_cols.extend(extra_drop)
    return X[[c for c in X.columns if c not in drop_cols]]


def time_split(feat_df, target_col):
    train = feat_df["fecha_evento"] < TRAIN_CUTOFF
    test = feat_df["fecha_evento"] >= TRAIN_CUTOFF
    X = to_features(feat_df)
    return X[train], X[test], feat_df[target_col].values[train], feat_df[target_col].values[test]


# ═══════════════════════════════════════════════════════════════════════════════
# T1: Binary — correctivo in next N days?
# ═══════════════════════════════════════════════════════════════════════════════

def eval_t1_binary(feat_df, horizon):
    col = f"target_corr_{horizon}d"
    X_train, X_test, y_train, y_test = time_split(feat_df, col)

    pos_rate = y_train.mean()
    model = XGBClassifier(n_estimators=200, max_depth=6, learning_rate=0.05,
                          subsample=0.8, colsample_bytree=0.6, random_state=RANDOM_STATE,
                          n_jobs=-1, scale_pos_weight=(1 - pos_rate) / max(pos_rate, 0.01))
    t0 = time.time()
    model.fit(X_train, y_train, verbose=False)
    ft = time.time() - t0
    yp = model.predict_proba(X_test)[:, 1]
    ypred = model.predict(X_test)
    imp = sorted(zip(X_train.columns, model.feature_importances_), key=lambda x: -x[1])[:5]
    return {
        "target": f"corr_{horizon}d",
        "roc_auc": roc_auc_score(y_test, yp),
        "f1": f1_score(y_test, ypred),
        "acc": accuracy_score(y_test, ypred),
        "naive_acc": max(pos_rate, 1 - pos_rate),
        "pos_rate": pos_rate,
        "fit_s": ft,
        "top_feat": ", ".join(f"{n}({v:.3f})" for n, v in imp),
        "n_train": len(y_train), "n_test": len(y_test),
    }


def eval_t1_regression(feat_df):
    col = "target_n_corr_30d"
    X_train, X_test, y_train, y_test = time_split(feat_df, col)

    model = XGBRegressor(n_estimators=200, max_depth=6, learning_rate=0.05,
                         subsample=0.8, colsample_bytree=0.6, random_state=RANDOM_STATE, n_jobs=-1)
    t0 = time.time()
    model.fit(X_train, y_train, verbose=False)
    ft = time.time() - t0
    yp = model.predict(X_test)
    imp = sorted(zip(X_train.columns, model.feature_importances_), key=lambda x: -x[1])[:5]
    return {
        "target": "n_corr_30d",
        "r2": r2_score(y_test, yp),
        "mae": mean_absolute_error(y_test, yp),
        "naive_mae": mean_absolute_error(y_test, np.full_like(y_test, y_train.mean())),
        "naive_r2": r2_score(y_test, np.full_like(y_test, y_train.mean())),
        "y_mean": y_train.mean(),
        "fit_s": ft,
        "top_feat": ", ".join(f"{n}({v:.3f})" for n, v in imp),
        "n_train": len(y_train), "n_test": len(y_test),
    }


# ═══════════════════════════════════════════════════════════════════════════════
# T2: System of NEXT corrective
# ═══════════════════════════════════════════════════════════════════════════════

def eval_t2_next_system(feat_df):
    col = "target_next_system"
    valid = feat_df[col].notna() & (feat_df[col] != "nan")
    data = feat_df[valid].copy()

    le = LabelEncoder()
    y_all = le.fit_transform(data[col].values)

    X = to_features(data)
    train = data["fecha_evento"] < TRAIN_CUTOFF
    test = data["fecha_evento"] >= TRAIN_CUTOFF
    X_train, X_test = X[train], X[test]
    y_train, y_test = y_all[train], y_all[test]

    model = XGBClassifier(n_estimators=200, max_depth=6, learning_rate=0.05,
                          subsample=0.8, colsample_bytree=0.6, random_state=RANDOM_STATE,
                          n_jobs=-1, eval_metric='mlogloss')
    t0 = time.time()
    model.fit(X_train, y_train, verbose=False)
    ft = time.time() - t0
    ypred = model.predict(X_test)

    acc = accuracy_score(y_test, ypred)
    f1m = f1_score(y_test, ypred, average="macro")
    mf = np.bincount(y_train).argmax()
    naive = (y_test == mf).mean()

    cr = classification_report(y_test, ypred, target_names=le.classes_, zero_division=0, output_dict=True)
    per_class = {cls: cr[cls]["f1-score"] for cls in le.classes_}

    imp = sorted(zip(X_train.columns, model.feature_importances_), key=lambda x: -x[1])[:5]
    return {
        "target": "next_system",
        "accuracy": acc,
        "f1_macro": f1m,
        "naive_acc": naive,
        "n_classes": len(le.classes_),
        "per_class_f1": per_class,
        "classes": list(le.classes_),
        "fit_s": ft,
        "top_feat": ", ".join(f"{n}({v:.3f})" for n, v in imp),
        "n_train": len(y_train), "n_test": len(y_test),
    }


# ═══════════════════════════════════════════════════════════════════════════════
# T3: Needs repuestos? (using only pre-event history)
# ═══════════════════════════════════════════════════════════════════════════════

def eval_t3_repuestos(feat_df):
    col = "target_tiene_repuestos"
    data = feat_df.copy()
    data[col] = data[col].fillna(0).astype(int)
    X_train, X_test, y_train, y_test = time_split(data, col)

    pos_rate = y_train.mean()
    model = XGBClassifier(n_estimators=200, max_depth=6, learning_rate=0.05,
                          subsample=0.8, colsample_bytree=0.6, random_state=RANDOM_STATE,
                          n_jobs=-1, scale_pos_weight=(1 - pos_rate) / max(pos_rate, 0.01))
    t0 = time.time()
    model.fit(X_train, y_train, verbose=False)
    ft = time.time() - t0
    yp = model.predict_proba(X_test)[:, 1]
    ypred = model.predict(X_test)
    imp = sorted(zip(X_train.columns, model.feature_importances_), key=lambda x: -x[1])[:8]
    return {
        "target": "needs_repuestos",
        "roc_auc": roc_auc_score(y_test, yp),
        "f1": f1_score(y_test, ypred),
        "acc": accuracy_score(y_test, ypred),
        "naive_acc": max(pos_rate, 1 - pos_rate),
        "pos_rate": pos_rate,
        "fit_s": ft,
        "top_feat": ", ".join(f"{n}({v:.3f})" for n, v in imp),
        "n_train": len(y_train), "n_test": len(y_test),
    }


# ═══════════════════════════════════════════════════════════════════════════════
# T4: Repair duration
# ═══════════════════════════════════════════════════════════════════════════════

def eval_t4_duration(feat_df):
    col = "target_duracion"
    valid = feat_df[col].notna() & (feat_df[col] > 0)
    data = feat_df[valid].copy()

    y_raw = data[col].values
    y_log = np.log1p(y_raw)

    X = to_features(data)
    train = data["fecha_evento"] < TRAIN_CUTOFF
    test = data["fecha_evento"] >= TRAIN_CUTOFF

    # Add system+terminal average from train only
    dtr = data[train]
    st_avg = dtr.groupby(["sistema_actual", "terminal_actual"])[col].median()
    data["st_avg_dur"] = data.apply(
        lambda r: st_avg.get((r["sistema_actual"], r["terminal_actual"]), dtr[col].median()), axis=1
    )
    X["st_avg_dur"] = data["st_avg_dur"].values

    X_train, X_test = X[train], X[test]
    y_train = y_log[train]
    y_test_raw = y_raw[test]

    model = XGBRegressor(n_estimators=200, max_depth=6, learning_rate=0.05,
                         subsample=0.8, colsample_bytree=0.6, random_state=RANDOM_STATE, n_jobs=-1)
    t0 = time.time()
    model.fit(X_train, y_train, verbose=False)
    ft = time.time() - t0
    yp = np.expm1(model.predict(X_test))
    imp = sorted(zip(X_train.columns, model.feature_importances_), key=lambda x: -x[1])[:5]
    return {
        "target": "duracion",
        "r2": r2_score(y_test_raw, yp),
        "mae": mean_absolute_error(y_test_raw, yp),
        "naive_mae": mean_absolute_error(y_test_raw, np.full_like(y_test_raw, y_raw[train].mean())),
        "naive_r2": r2_score(y_test_raw, np.full_like(y_test_raw, y_raw[train].mean())),
        "y_mean": y_raw[train].mean(),
        "fit_s": ft,
        "top_feat": ", ".join(f"{n}({v:.3f})" for n, v in imp),
        "n_train": len(y_train), "n_test": len(y_test_raw),
    }


# ═══════════════════════════════════════════════════════════════════════════════
# T5: Spike >=10 correctivos next month
# ═══════════════════════════════════════════════════════════════════════════════

def eval_t5_spike(full_df):
    corr = full_df[full_df["tipo_servicio"] == "CORRECTIVO"].copy()
    corr["ym"] = corr["fecha_evento"].dt.to_period("M")
    bpm = corr.groupby(["placa_patente", "ym"]).size().reset_index(name="n_corr")
    bpm["ym_dt"] = bpm["ym"].dt.to_timestamp()
    bpm = bpm.sort_values(["placa_patente", "ym_dt"])

    rows = []
    for bus, grp in bpm.groupby("placa_patente"):
        grp = grp.sort_values("ym_dt")
        for i in range(len(grp)):
            past = grp.iloc[:i]
            current_ym = grp.iloc[i]["ym_dt"]
            n_past = len(past)
            if n_past == 0:
                feats = {"n_1m": 0, "n_2m": 0, "n_3m": 0, "avg_3m": 0, "avg_6m": 0,
                         "max_ever": 0, "trend": 0, "n_months": 0}
            else:
                a3 = past.tail(3)["n_corr"].mean()
                a6 = past.tail(6)["n_corr"].mean() if n_past >= 6 else past["n_corr"].mean()
                feats = {
                    "n_1m": past.iloc[-1]["n_corr"],
                    "n_2m": past.iloc[-2]["n_corr"] if n_past >= 2 else 0,
                    "n_3m": past.iloc[-3]["n_corr"] if n_past >= 3 else 0,
                    "avg_3m": a3,
                    "avg_6m": a6,
                    "max_ever": past["n_corr"].max(),
                    "trend": a3 / max(a6, 0.01) - 1 if a6 > 0 else 0,
                    "n_months": n_past,
                }
            feats["ym_dt"] = current_ym
            feats["target"] = int(grp.iloc[i]["n_corr"] >= 10)
            rows.append(feats)

    data = pd.DataFrame(rows)
    Xc = [c for c in data.columns if c not in ("ym_dt", "target")]
    X = data[Xc].fillna(0)
    y = data["target"].values
    train = data["ym_dt"] < TRAIN_CUTOFF
    test = ~train

    X_train, X_test = X[train], X[test]
    y_train, y_test = y[train], y[test]
    pos = y_train.mean()

    model = XGBClassifier(n_estimators=200, max_depth=5, learning_rate=0.05,
                          subsample=0.8, random_state=RANDOM_STATE, n_jobs=-1,
                          scale_pos_weight=(1 - pos) / max(pos, 0.01))
    t0 = time.time()
    model.fit(X_train, y_train, verbose=False)
    ft = time.time() - t0
    yp = model.predict_proba(X_test)[:, 1]
    ypred = model.predict(X_test)
    imp = sorted(zip(Xc, model.feature_importances_), key=lambda x: -x[1])[:5]
    return {
        "target": "spike_10_mes",
        "roc_auc": roc_auc_score(y_test, yp),
        "f1": f1_score(y_test, ypred),
        "acc": accuracy_score(y_test, ypred),
        "naive_acc": max(pos, 1 - pos),
        "pos_rate": pos,
        "fit_s": ft,
        "top_feat": ", ".join(f"{n}({v:.3f})" for n, v in imp),
        "n_train": len(y_train), "n_test": len(y_test),
    }


# ═══════════════════════════════════════════════════════════════════════════════
# T6: Weekly load per system+terminal
# ═══════════════════════════════════════════════════════════════════════════════

def eval_t6_weekly(full_df):
    corr = full_df[full_df["tipo_servicio"] == "CORRECTIVO"].copy()
    corr["week_dt"] = corr["fecha_evento"] - pd.to_timedelta(corr["fecha_evento"].dt.dayofweek, unit='D')
    grp = corr.groupby(["causa_sistema_reconstruida", "taller_planta_grouped", "week_dt"]).size()
    grp = grp.reset_index(name="n_corr")

    rows = []
    for (sys, term), g in grp.groupby(["causa_sistema_reconstruida", "taller_planta_grouped"]):
        g = g.sort_values("week_dt")
        for i in range(8, len(g)):
            rows.append({
                "n_1w": g.iloc[i-1]["n_corr"],
                "n_2w": g.iloc[i-2]["n_corr"],
                "n_3w": g.iloc[i-3]["n_corr"],
                "n_4w": g.iloc[i-4]["n_corr"],
                "avg_4w": g.iloc[i-4:i]["n_corr"].mean(),
                "avg_8w": g.iloc[max(0,i-8):i]["n_corr"].mean(),
                "max_4w": g.iloc[i-4:i]["n_corr"].max(),
                "sistema": sys, "terminal": term,
                "week_dt": g.iloc[i]["week_dt"],
                "target": g.iloc[i]["n_corr"],
            })

    data = pd.DataFrame(rows)
    data = pd.get_dummies(data, columns=["sistema", "terminal"], drop_first=True)
    fc = [c for c in data.columns if c not in ("week_dt", "target")]
    X, y = data[fc].fillna(0), data["target"].values
    train = data["week_dt"] < TRAIN_CUTOFF
    test = ~train

    X_train, X_test = X[train], X[test]
    y_train, y_test = y[train], y[test]

    model = XGBRegressor(n_estimators=200, max_depth=5, learning_rate=0.05,
                         subsample=0.8, random_state=RANDOM_STATE, n_jobs=-1)
    t0 = time.time()
    model.fit(X_train, y_train, verbose=False)
    ft = time.time() - t0
    yp = np.maximum(model.predict(X_test), 0)
    imp = sorted(zip(fc, model.feature_importances_), key=lambda x: -x[1])[:5]
    return {
        "target": "weekly_sys_term",
        "r2": r2_score(y_test, yp),
        "mae": mean_absolute_error(y_test, yp),
        "naive_mae": mean_absolute_error(y_test, np.full_like(y_test, y_train.mean())),
        "naive_r2": r2_score(y_test, np.full_like(y_test, y_train.mean())),
        "y_mean": y_train.mean(),
        "fit_s": ft,
        "top_feat": ", ".join(f"{n}({v:.3f})" for n, v in imp),
        "n_train": len(y_train), "n_test": len(y_test),
    }


# ═══════════════════════════════════════════════════════════════════════════════

def main():
    print("=" * 70)
    print("EVALUACIÓN DE TARGETS DE PREDICCIÓN — V3 (sin fugas de datos)")
    print(f"Train cutoff: {TRAIN_CUTOFF.date()}")
    print("=" * 70)

    print("\nCargando datos...")
    t0 = time.time()
    full_df = load_data()
    print(f"  {len(full_df):,} eventos en {time.time() - t0:.1f}s")

    print("\nConstruyendo features (solo historia pre-evento, sin fugas)...")
    t0 = time.time()
    feat_df = build_features(full_df)
    print(f"  {len(feat_df):,} eventos CORRECTIVO con features en {time.time() - t0:.1f}s")

    all_results = []

    # ── T1: Próximo correctivo ──
    print("\n" + "=" * 70)
    print("TARGET 1: Próximo correctivo")
    print("=" * 70)

    for h, lab in [(7, "7d"), (14, "14d"), (30, "30d")]:
        r = eval_t1_binary(feat_df, h)
        all_results.append(r)
        print(f"\n  T1.{lab}: ¿Correctivo en {h} días? (binary)")
        print(f"    ROC-AUC: {r['roc_auc']:.4f}  |  F1: {r['f1']:.4f}  |  "
              f"Acc: {r['acc']:.4f} (naive {r['naive_acc']:.4f})")
        print(f"    Pos rate: {r['pos_rate']:.1%}  |  Train/Test: {r['n_train']:,}/{r['n_test']:,}  |  {r['fit_s']:.1f}s")
        print(f"    Top: {r['top_feat']}")

    r = eval_t1_regression(feat_df)
    all_results.append(r)
    print(f"\n  T1.reg: # correctivos en próximos 30d (regression)")
    print(f"    R²: {r['r2']:.4f} (naive {r['naive_r2']:.4f})  |  "
          f"MAE: {r['mae']:.2f} (naive {r['naive_mae']:.2f})")
    print(f"    y mean: {r['y_mean']:.1f}  |  Train/Test: {r['n_train']:,}/{r['n_test']:,}  |  {r['fit_s']:.1f}s")
    print(f"    Top: {r['top_feat']}")

    # ── T2: Next system ──
    print("\n" + "=" * 70)
    print("TARGET 2: Sistema del PRÓXIMO correctivo (multiclass)")
    print("=" * 70)
    r = eval_t2_next_system(feat_df)
    all_results.append(r)
    print(f"  Accuracy: {r['accuracy']:.4f} (naive most-freq: {r['naive_acc']:.4f})")
    print(f"  F1-macro: {r['f1_macro']:.4f}  |  {r['n_classes']} classes")
    print(f"  Train/Test: {r['n_train']:,}/{r['n_test']:,}  |  {r['fit_s']:.1f}s")
    print(f"  Top: {r['top_feat']}")
    print("  Per-class F1:")
    for cls in r["classes"]:
        f1c = r["per_class_f1"].get(cls, 0)
        bar = "█" * int(f1c * 20)
        print(f"    {cls:<20s} F1={f1c:.3f} {bar}")

    # ── T3: Needs repuestos ──
    print("\n" + "=" * 70)
    print("TARGET 3: ¿El correctivo necesita repuestos? (binary)")
    print("=" * 70)
    r = eval_t3_repuestos(feat_df)
    all_results.append(r)
    print(f"  ROC-AUC: {r['roc_auc']:.4f}  |  F1: {r['f1']:.4f}  |  "
          f"Acc: {r['acc']:.4f} (naive {r['naive_acc']:.4f})")
    print(f"  Pos rate: {r['pos_rate']:.1%}  |  Train/Test: {r['n_train']:,}/{r['n_test']:,}  |  {r['fit_s']:.1f}s")
    print(f"  Top: {r['top_feat']}")

    # ── T4: Duration ──
    print("\n" + "=" * 70)
    print("TARGET 4: Duración de reparación (regression)")
    print("=" * 70)
    r = eval_t4_duration(feat_df)
    all_results.append(r)
    print(f"  R²: {r['r2']:.4f} (naive {r['naive_r2']:.4f})  |  "
          f"MAE: {r['mae']:.1f}h (naive {r['naive_mae']:.1f}h)")
    print(f"  y mean: {r['y_mean']:.1f}h  |  Train/Test: {r['n_train']:,}/{r['n_test']:,}  |  {r['fit_s']:.1f}s")
    print(f"  Top: {r['top_feat']}")

    # ── T5: Spike ──
    print("\n" + "=" * 70)
    print("TARGET 5: Pico >=10 correctivos próximo mes (binary)")
    print("=" * 70)
    r = eval_t5_spike(full_df)
    all_results.append(r)
    print(f"  ROC-AUC: {r['roc_auc']:.4f}  |  F1: {r['f1']:.4f}  |  "
          f"Acc: {r['acc']:.4f} (naive {r['naive_acc']:.4f})")
    print(f"  Pos rate: {r['pos_rate']:.1%}  |  Train/Test: {r['n_train']:,}/{r['n_test']:,}  |  {r['fit_s']:.1f}s")
    print(f"  Top: {r['top_feat']}")

    # ── T6: Weekly load ──
    print("\n" + "=" * 70)
    print("TARGET 6: Carga semanal por sistema+terminal (regression)")
    print("=" * 70)
    r = eval_t6_weekly(full_df)
    all_results.append(r)
    print(f"  R²: {r['r2']:.4f} (naive {r['naive_r2']:.4f})  |  "
          f"MAE: {r['mae']:.1f} (naive {r['naive_mae']:.1f})")
    print(f"  y mean: {r['y_mean']:.1f}  |  Train/Test: {r['n_train']:,}/{r['n_test']:,}  |  {r['fit_s']:.1f}s")
    print(f"  Top: {r['top_feat']}")

    # ═══════════════════════════════════════════════════════════════════════════
    # FINAL SUMMARY
    # ═══════════════════════════════════════════════════════════════════════════
    print("\n\n" + "=" * 90)
    print("RESUMEN FINAL — Targets ordenados por señal predictiva")
    print("=" * 90)
    print(f"{'#':<3s} {'Target':<45s} {'Métrica clave':<35s} {'Señal':<10s}")
    print("-" * 93)

    summaries = []

    # T1 results
    for r in all_results:
        if r["target"].startswith("corr_"):
            h = r["target"].split("_")[1]
            summaries.append((f"¿Correctivo en {h}?", "binary",
                             f"ROC-AUC={r['roc_auc']:.3f} F1={r['f1']:.3f}",
                             r["roc_auc"], r["naive_acc"]))
        elif r["target"] == "n_corr_30d":
            summaries.append(("# correctivos 30d", "regression",
                             f"R²={r['r2']:.3f} MAE={r['mae']:.1f}",
                             r["r2"], r["naive_r2"]))
        elif r["target"] == "next_system":
            summaries.append(("Sistema próximo correctivo", "multiclass",
                             f"Acc={r['accuracy']:.3f} F1m={r['f1_macro']:.3f}",
                             r["f1_macro"], r["naive_acc"]))
        elif r["target"] == "needs_repuestos":
            summaries.append(("¿Necesita repuestos?", "binary",
                             f"ROC-AUC={r['roc_auc']:.3f} F1={r['f1']:.3f}",
                             r["roc_auc"], r["naive_acc"]))
        elif r["target"] == "duracion":
            summaries.append(("Duración reparación", "regression",
                             f"R²={r['r2']:.3f} MAE={r['mae']:.1f}h",
                             r["r2"], r["naive_r2"]))
        elif r["target"] == "spike_10_mes":
            summaries.append(("Pico >=10 corr/mes", "binary",
                             f"ROC-AUC={r['roc_auc']:.3f} F1={r['f1']:.3f}",
                             r["roc_auc"], r["naive_acc"]))
        elif r["target"] == "weekly_sys_term":
            summaries.append(("Carga semanal sis+term", "regression",
                             f"R²={r['r2']:.3f} MAE={r['mae']:.1f}",
                             r["r2"], r["naive_r2"]))

    # Score: for binary/class, use (metric - naive_metric) / (1 - naive)
    # for regression, use (metric - naive_metric)
    scored = []
    for name, typ, metric_str, val, naive in summaries:
        if typ in ("binary", "multiclass"):
            score = (val - naive) / (1 - naive + 0.001)
        else:
            score = val - naive
        scored.append((name, typ, metric_str, score))

    scored.sort(key=lambda x: -x[3])

    for i, (name, typ, metric_str, score) in enumerate(scored):
        stars = "★" * min(5, max(1, int(score * 6)))
        print(f"{i+1:<3d} {name:<45s} {metric_str:<35s} {stars}")

    print("\nLeyenda: ★★★★★ = señal fuerte sobre baseline  |  ★ = señal débil")
    print("Train/test split: < 2025-07-01 / >= 2025-07-01")


if __name__ == "__main__":
    main()
