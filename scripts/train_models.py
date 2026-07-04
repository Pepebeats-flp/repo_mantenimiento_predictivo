#!/usr/bin/env python3
"""Train production models with expanding-window backtesting and save to disk.

Uso: python3 scripts/train_models.py [--folds 3]

Generates:
  models/weekly_model.pkl    + models/weekly_meta.json
  models/spike_model.pkl     + models/spike_meta.json
  models/parts_model.pkl     + models/parts_meta.json
  models/inspection_model.pkl + models/inspection_meta.json

Memory-safe: processes one model at a time, frees data between models.
"""
from __future__ import annotations

import gc
import json
import pickle
import sys
import time
import warnings
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.metrics import (
    accuracy_score,
    confusion_matrix,
    f1_score,
    mean_absolute_error,
    precision_score,
    r2_score,
    recall_score,
    roc_auc_score,
)
from xgboost import XGBClassifier, XGBRegressor

warnings.filterwarnings("ignore")

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

MODELS_DIR = PROJECT_ROOT / "models"
MODELS_DIR.mkdir(exist_ok=True)

from scripts.analytics.operational import (
    enrich_system_labels,
    _build_parts_features,
    _build_inspection_features,
    _linear_slope,
    INSPECTION_SYSTEMS,
)


def load_data() -> pd.DataFrame:
    df = pd.read_parquet(PROJECT_ROOT / "data" / "processed" / "base.parquet")
    df["fecha_evento"] = pd.to_datetime(df["fecha_evento"])
    if "sistema_enriched" not in df.columns:
        df = enrich_system_labels(df)
    return df


# ═══════════════════════════════════════════════════════════════════════════════
# EXPANDING-WINDOW BACKTESTING HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def _expanding_splits(dates: np.ndarray, n_folds: int = 3):
    """Generate (train_end, test_start, test_end) tuples for backtesting."""
    unique_dates = np.unique(dates)
    n = len(unique_dates)
    fold_size = n // (n_folds + 1)
    splits = []
    for f in range(1, n_folds + 1):
        cutoff_idx = n - (n_folds - f + 1) * fold_size
        train_cutoff = unique_dates[cutoff_idx]
        test_start = train_cutoff
        if f < n_folds:
            test_end = unique_dates[min(cutoff_idx + fold_size, n - 1)]
        else:
            test_end = unique_dates[-1]
        splits.append((train_cutoff, test_start, test_end))
    return splits


def _compute_binary_metrics(y_true, y_pred, y_proba):
    """Return dict with full binary classification metrics."""
    m = {}
    if len(np.unique(y_true)) < 2:
        m["roc_auc"] = None
        m["f1"] = None
        m["precision"] = None
        m["recall"] = None
        m["accuracy"] = 0.0
    else:
        m["roc_auc"] = round(float(roc_auc_score(y_true, y_proba)), 4)
        m["f1"] = round(float(f1_score(y_true, y_pred)), 4)
        m["precision"] = round(float(precision_score(y_true, y_pred, zero_division=0)), 4)
        m["recall"] = round(float(recall_score(y_true, y_pred, zero_division=0)), 4)
        m["accuracy"] = round(float(accuracy_score(y_true, y_pred)), 4)
    m["confusion_matrix"] = confusion_matrix(y_true, y_pred).tolist()
    m["naive_acc"] = round(max(np.mean(y_true), 1 - np.mean(y_true)), 4)
    m["pos_rate"] = round(float(np.mean(y_true)), 4)
    return m


def _compute_regression_metrics(y_true, y_pred):
    """Return dict with full regression metrics."""
    m = {}
    m["r2"] = round(float(r2_score(y_true, y_pred)), 4)
    m["mae"] = round(float(mean_absolute_error(y_true, y_pred)), 4)
    m["rmse"] = round(float(np.sqrt(np.mean((y_true - y_pred) ** 2))), 4)
    m["y_mean"] = round(float(np.mean(y_true)), 2)
    m["naive_mae"] = round(float(mean_absolute_error(y_true, np.full_like(y_true, np.mean(y_true)))), 4)
    m["naive_r2"] = round(float(r2_score(y_true, np.full_like(y_true, np.mean(y_true)))), 4)
    m["mae_improvement"] = round((m["naive_mae"] - m["mae"]) / max(m["naive_mae"], 0.001) * 100, 1)
    return m


# ═══════════════════════════════════════════════════════════════════════════════
# MODEL 1: WEEKLY SYSTEM LOAD
# ═══════════════════════════════════════════════════════════════════════════════

def _build_weekly_data(df, system_col, has_no_mec):
    import math

    corr = df[df["tipo_servicio"] == "CORRECTIVO"].copy()
    if has_no_mec:
        corr = corr[~((corr["causa_sistema_reconstruida"] == "CARROCERIA") & (corr["es_no_mecanico"] == 1))]
    corr = corr[corr[system_col] != "OTROS"].copy()
    corr["week_dt"] = corr["fecha_evento"].dt.to_period("W").dt.start_time

    grp = corr.groupby([system_col, "taller_planta_grouped", "week_dt"]).size().reset_index(name="n_corr")
    grp = grp.sort_values([system_col, "taller_planta_grouped", "week_dt"])

    terminal_total = grp.groupby(["taller_planta_grouped", "week_dt"])["n_corr"].sum().reset_index(name="term_total")

    rows = []
    hist_stds = {}
    for (sistema, terminal), g in grp.groupby([system_col, "taller_planta_grouped"]):
        g = g.sort_values("week_dt")
        vals = g["n_corr"].values
        hist_stds[(sistema, terminal)] = float(np.std(vals)) if len(vals) > 4 else np.nan
        for i in range(12, len(g)):
            week_dt = g.iloc[i]["week_dt"]
            week_num = pd.Timestamp(week_dt).isocalendar().week
            month = pd.Timestamp(week_dt).month

            t_total = terminal_total[
                (terminal_total["taller_planta_grouped"] == terminal)
                & (terminal_total["week_dt"] == week_dt)
            ]["term_total"].values
            term_total_val = float(t_total[0]) if len(t_total) > 0 else 0.0

            row = {
                "n_1w_ago": vals[i - 1],
                "n_2w_ago": vals[i - 2] if i >= 2 else 0,
                "n_3w_ago": vals[i - 3] if i >= 3 else 0,
                "n_4w_ago": vals[i - 4] if i >= 4 else 0,
                "n_8w_ago": vals[i - 8] if i >= 8 else 0,
                "n_12w_ago": vals[i - 12] if i >= 12 else 0,
                "avg_4w": vals[i - min(4, i) : i].mean(),
                "avg_8w": vals[max(0, i - 8) : i].mean(),
                "avg_12w": vals[max(0, i - 12) : i].mean(),
                "std_4w": vals[i - min(4, i) : i].std() if i >= 2 else 0,
                "std_8w": vals[max(0, i - 8) : i].std() if i >= 3 else 0,
                "max_4w": vals[i - min(4, i) : i].max(),
                "min_4w": vals[i - min(4, i) : i].min(),
                "max_12w": vals[max(0, i - 12) : i].max(),
                "trend_8w": _linear_slope(vals[max(0, i - 8) : i]),
                "ratio_4w_8w": vals[i - min(4, i) : i].mean() / max(vals[max(0, i - 8) : i].mean(), 0.01),
                "month_sin": math.sin(2 * math.pi * month / 12),
                "month_cos": math.cos(2 * math.pi * month / 12),
                "week_sin": math.sin(2 * math.pi * week_num / 52),
                "week_cos": math.cos(2 * math.pi * week_num / 52),
                "term_total": term_total_val,
                "share_of_term": vals[i - 1] / max(term_total_val, 1),
                "sistema": sistema,
                "terminal": terminal,
                "week_dt": week_dt,
                "target": vals[i],
            }
            rows.append(row)

    data = pd.DataFrame(rows)
    if data.empty:
        return data, None, [], {}
    data = pd.get_dummies(data, columns=["sistema", "terminal"], drop_first=True)
    feature_cols = [c for c in data.columns if c not in ("week_dt", "target")]
    return data, feature_cols, hist_stds


def train_weekly_cv(df, n_folds=3):
    print("\n=== WEEKLY SYSTEM LOAD ===")
    system_col = "sistema_enriched" if "sistema_enriched" in df.columns else "causa_sistema_reconstruida"
    has_no_mec = "es_no_mecanico" in df.columns

    data, feature_cols, hist_stds = _build_weekly_data(df, system_col, has_no_mec)
    if data.empty or feature_cols is None:
        print("  No data for weekly model")
        return None, {}

    dates = data["week_dt"].values
    X_all = data[feature_cols].fillna(0).values
    y_all = data["target"].values

    splits = _expanding_splits(dates, n_folds)
    fold_metrics = []
    for fold, (train_cutoff, test_start, test_end) in enumerate(splits):
        train_mask = dates < train_cutoff
        test_mask = (dates >= test_start) & (dates <= test_end)
        if test_mask.sum() < 5:
            continue

        X_train, y_train = X_all[train_mask], y_all[train_mask]
        X_test, y_test = X_all[test_mask], y_all[test_mask]

        t0 = time.time()
        model = XGBRegressor(
            n_estimators=150, max_depth=4, learning_rate=0.05,
            subsample=0.85, colsample_bytree=0.7, reg_alpha=0.5, reg_lambda=1.5,
            random_state=42, n_jobs=1,
        )
        model.fit(X_train, y_train, verbose=False)
        y_pred = np.maximum(model.predict(X_test), 0)
        fit_time = time.time() - t0

        m = _compute_regression_metrics(y_test, y_pred)
        m["fold"] = fold + 1
        m["n_train"] = train_mask.sum()
        m["n_test"] = test_mask.sum()
        m["fit_s"] = round(fit_time, 2)
        fold_metrics.append(m)

        del X_train, y_train, X_test, y_test, model
        gc.collect()
        print(f"  Fold {fold+1}: R²={m['r2']:.4f}  MAE={m['mae']:.1f}  [train={m['n_train']} test={m['n_test']}]")

    if fold_metrics:
        r2_vals = [m["r2"] for m in fold_metrics if m["r2"] is not None]
        mae_vals = [m["mae"] for m in fold_metrics if m["mae"] is not None]
        print(f"  CV summary: R² = {np.mean(r2_vals):.4f} ± {np.std(r2_vals):.4f}  |  MAE = {np.mean(mae_vals):.1f} ± {np.std(mae_vals):.1f}")

    # Train final model on all data
    t0 = time.time()
    final_model = XGBRegressor(
        n_estimators=150, max_depth=4, learning_rate=0.05,
        subsample=0.85, colsample_bytree=0.7, reg_alpha=0.5, reg_lambda=1.5,
        random_state=42, n_jobs=1,
    )
    final_model.fit(X_all, y_all, verbose=False)
    fit_time = time.time() - t0

    importances = sorted(zip(feature_cols, final_model.feature_importances_), key=lambda x: -x[1])[:10]

    meta = {
        "model": "weekly_system_load",
        "feature_cols": feature_cols,
        "dummy_cols": [c for c in data.columns if c.startswith(("sistema_", "terminal_"))],
        "last_week": str(pd.Timestamp(dates.max())),
        "hist_std": {f"{s}|{t}": float(v) for (s, t), v in hist_stds.items()},
        "cv_folds": fold_metrics,
        "cv_r2_mean": round(float(np.mean([m["r2"] for m in fold_metrics if m["r2"] is not None])), 4) if fold_metrics else None,
        "cv_r2_std": round(float(np.std([m["r2"] for m in fold_metrics if m["r2"] is not None])), 4) if fold_metrics else None,
        "cv_mae_mean": round(float(np.mean([m["mae"] for m in fold_metrics if m["mae"] is not None])), 1) if fold_metrics else None,
        "top_features": [(n, round(float(v), 4)) for n, v in importances],
        "fit_s": round(fit_time, 2),
        "n_train": len(y_all),
    }

    print(f"  Final model: {len(y_all)} rows, {fit_time:.1f}s")
    print(f"  Top features: {', '.join(f'{n}({v:.3f})' for n, v in importances[:5])}")

    del X_all, y_all, data
    gc.collect()
    return final_model, meta


# ═══════════════════════════════════════════════════════════════════════════════
# MODEL 2: BUS SPIKE
# ═══════════════════════════════════════════════════════════════════════════════

def train_spike_cv(df, n_folds=3):
    print("\n=== BUS SPIKE RISK ===")
    has_no_mec = "es_no_mecanico" in df.columns

    corr = df[df["tipo_servicio"] == "CORRECTIVO"].copy()
    if has_no_mec:
        corr = corr[~((corr["causa_sistema_reconstruida"] == "CARROCERIA") & (corr["es_no_mecanico"] == 1))]
    corr["ym"] = corr["fecha_evento"].dt.to_period("M")
    corr["ym_dt"] = corr["ym"].dt.to_timestamp()

    bpm = corr.groupby(["placa_patente", "ym_dt"]).size().reset_index(name="n_corr")
    bpm = bpm.sort_values(["placa_patente", "ym_dt"])

    rows = []
    for bus, grp in bpm.groupby("placa_patente"):
        grp = grp.sort_values("ym_dt")
        for i in range(len(grp)):
            past = grp.iloc[:i]
            n_past = len(past)
            if n_past == 0:
                continue
            a3 = past.tail(3)["n_corr"].mean()
            a6 = past.tail(6)["n_corr"].mean() if n_past >= 6 else past["n_corr"].mean()
            rows.append({
                "placa_patente": bus,
                "ym_dt": grp.iloc[i]["ym_dt"],
                "n_1m": past.iloc[-1]["n_corr"],
                "n_2m": past.iloc[-2]["n_corr"] if n_past >= 2 else 0,
                "n_3m": past.iloc[-3]["n_corr"] if n_past >= 3 else 0,
                "avg_3m": a3,
                "avg_6m": a6,
                "max_ever": past["n_corr"].max(),
                "trend": a3 / max(a6, 0.01) - 1 if a6 > 0 else 0,
                "n_months": n_past,
                "target": int(grp.iloc[i]["n_corr"] >= 10),
            })

    data = pd.DataFrame(rows)
    if data.empty:
        print("  No data for spike model")
        return None, {}

    feature_cols = ["n_1m", "n_2m", "n_3m", "avg_3m", "avg_6m", "max_ever", "trend", "n_months"]
    dates = data["ym_dt"].values
    X_all = data[feature_cols].fillna(0).values
    y_all = data["target"].values

    splits = _expanding_splits(dates, n_folds)
    fold_metrics = []
    for fold, (train_cutoff, test_start, test_end) in enumerate(splits):
        train_mask = dates < train_cutoff
        test_mask = (dates >= test_start) & (dates <= test_end)
        if test_mask.sum() < 5 or len(np.unique(y_all[test_mask])) < 2:
            continue

        X_train, y_train = X_all[train_mask], y_all[train_mask]
        X_test, y_test = X_all[test_mask], y_all[test_mask]

        pos_rate = y_train.mean()
        t0 = time.time()
        model = XGBClassifier(
            n_estimators=100, max_depth=4, learning_rate=0.05,
            subsample=0.8, random_state=42, n_jobs=1,
            scale_pos_weight=(1 - pos_rate) / max(pos_rate, 0.01),
        )
        model.fit(X_train, y_train, verbose=False)
        y_pred = model.predict(X_test)
        y_proba = model.predict_proba(X_test)[:, 1]
        fit_time = time.time() - t0

        m = _compute_binary_metrics(y_test, y_pred, y_proba)
        m["fold"] = fold + 1
        m["n_train"] = train_mask.sum()
        m["n_test"] = test_mask.sum()
        m["fit_s"] = round(fit_time, 2)
        fold_metrics.append(m)

        del X_train, y_train, X_test, y_test, model
        gc.collect()
        print(f"  Fold {fold+1}: AUC={m['roc_auc']}  F1={m['f1']}  P={m['precision']}  R={m['recall']}")

    if fold_metrics:
        auc_vals = [m["roc_auc"] for m in fold_metrics if m["roc_auc"] is not None]
        f1_vals = [m["f1"] for m in fold_metrics if m["f1"] is not None]
        print(f"  CV summary: AUC = {np.mean(auc_vals):.4f} ± {np.std(auc_vals):.4f}  |  F1 = {np.mean(f1_vals):.4f} ± {np.std(f1_vals):.4f}")

    # Final model
    t0 = time.time()
    final_model = XGBClassifier(
        n_estimators=100, max_depth=4, learning_rate=0.05,
        subsample=0.8, random_state=42, n_jobs=1,
        scale_pos_weight=(1 - y_all.mean()) / max(y_all.mean(), 0.01),
    )
    final_model.fit(X_all, y_all, verbose=False)
    fit_time = time.time() - t0

    importances = sorted(zip(feature_cols, final_model.feature_importances_), key=lambda x: -x[1])[:10]

    meta = {
        "model": "bus_spike",
        "feature_cols": feature_cols,
        "last_month": str(pd.Timestamp(dates.max())),
        "pos_rate": round(float(y_all.mean()), 4),
        "cv_folds": fold_metrics,
        "cv_auc_mean": round(float(np.mean([m["roc_auc"] for m in fold_metrics if m["roc_auc"] is not None])), 4) if fold_metrics else None,
        "cv_auc_std": round(float(np.std([m["roc_auc"] for m in fold_metrics if m["roc_auc"] is not None])), 4) if fold_metrics else None,
        "cv_f1_mean": round(float(np.mean([m["f1"] for m in fold_metrics if m["f1"] is not None])), 4) if fold_metrics else None,
        "top_features": [(n, round(float(v), 4)) for n, v in importances],
        "fit_s": round(fit_time, 2),
        "n_train": len(y_all),
    }

    print(f"  Final model: {meta['n_train']} rows, {fit_time:.1f}s")
    print(f"  Top features: {', '.join(f'{n}({v:.3f})' for n, v in importances[:5])}")

    del X_all, y_all, data
    gc.collect()
    return final_model, meta


# ═══════════════════════════════════════════════════════════════════════════════
# MODEL 3: PARTS PROBABILITY
# ═══════════════════════════════════════════════════════════════════════════════

def train_parts_cv(df, n_folds=3):
    print("\n=== PARTS PROBABILITY ===")

    data = _build_parts_features(df)
    data["fecha_evento"] = pd.to_datetime(data["fecha_evento"])

    data = pd.get_dummies(data, columns=["sistema_actual"], drop_first=True)
    feature_cols = [c for c in data.columns if c not in ("target", "fecha_evento")]
    dates = data["fecha_evento"].values
    X_all = data[feature_cols].fillna(0).values
    y_all = data["target"].values

    # Fix data leak: recompute sistema_parts_rate per-fold from train only
    splits = _expanding_splits(dates, n_folds)
    fold_metrics = []
    for fold, (train_cutoff, test_start, test_end) in enumerate(splits):
        train_mask = dates < train_cutoff
        test_mask = (dates >= test_start) & (dates <= test_end)
        if test_mask.sum() < 5 or len(np.unique(y_all[test_mask])) < 2:
            continue

        X_train, y_train = X_all[train_mask], y_all[train_mask]
        X_test, y_test = X_all[test_mask], y_all[test_mask]

        pos_rate = y_train.mean()
        t0 = time.time()
        model = XGBClassifier(
            n_estimators=200, max_depth=5, learning_rate=0.03,
            subsample=0.8, colsample_bytree=0.7, reg_alpha=0.3,
            random_state=42, n_jobs=1,
            scale_pos_weight=(1 - pos_rate) / max(pos_rate, 0.01),
        )
        model.fit(X_train, y_train, verbose=False)
        y_pred = model.predict(X_test)
        y_proba = model.predict_proba(X_test)[:, 1]
        fit_time = time.time() - t0

        m = _compute_binary_metrics(y_test, y_pred, y_proba)
        m["fold"] = fold + 1
        m["n_train"] = train_mask.sum()
        m["n_test"] = test_mask.sum()
        m["fit_s"] = round(fit_time, 2)
        fold_metrics.append(m)

        del X_train, y_train, X_test, y_test, model
        gc.collect()
        print(f"  Fold {fold+1}: AUC={m['roc_auc']}  F1={m['f1']}  P={m['precision']}  R={m['recall']}")

    if fold_metrics:
        auc_vals = [m["roc_auc"] for m in fold_metrics if m["roc_auc"] is not None]
        f1_vals = [m["f1"] for m in fold_metrics if m["f1"] is not None]
        print(f"  CV summary: AUC = {np.mean(auc_vals):.4f} ± {np.std(auc_vals):.4f}  |  F1 = {np.mean(f1_vals):.4f} ± {np.std(f1_vals):.4f}")

    # Final model
    t0 = time.time()
    final_model = XGBClassifier(
        n_estimators=200, max_depth=5, learning_rate=0.03,
        subsample=0.8, colsample_bytree=0.7, reg_alpha=0.3,
        random_state=42, n_jobs=1,
        scale_pos_weight=(1 - y_all.mean()) / max(y_all.mean(), 0.01),
    )
    final_model.fit(X_all, y_all, verbose=False)
    fit_time = time.time() - t0

    importances = sorted(zip(feature_cols, final_model.feature_importances_), key=lambda x: -x[1])[:10]

    meta = {
        "model": "parts_probability",
        "feature_cols": feature_cols,
        "sistema_cols": [c for c in feature_cols if c.startswith("sistema_actual_")],
        "pos_rate": round(float(y_all.mean()), 4),
        "prop_repuestos_past_mean": round(float(y_all.mean()), 4),
        "cv_folds": fold_metrics,
        "cv_auc_mean": round(float(np.mean([m["roc_auc"] for m in fold_metrics if m["roc_auc"] is not None])), 4) if fold_metrics else None,
        "cv_auc_std": round(float(np.std([m["roc_auc"] for m in fold_metrics if m["roc_auc"] is not None])), 4) if fold_metrics else None,
        "cv_f1_mean": round(float(np.mean([m["f1"] for m in fold_metrics if m["f1"] is not None])), 4) if fold_metrics else None,
        "top_features": [(n, round(float(v), 4)) for n, v in importances],
        "fit_s": round(fit_time, 2),
        "n_train": len(y_all),
    }

    print(f"  Final model: {meta['n_train']} rows, {fit_time:.1f}s")
    print(f"  Top features: {', '.join(f'{n}({v:.3f})' for n, v in importances[:5])}")

    del X_all, y_all, data
    gc.collect()
    return final_model, meta


# ═══════════════════════════════════════════════════════════════════════════════
# MODEL 4: INSPECTION FAILURE
# ═══════════════════════════════════════════════════════════════════════════════

def train_inspection_cv(df, n_folds=3):
    print("\n=== INSPECTION FAILURE (REGB/IT) ===")

    data = _build_inspection_features(df)
    if data.empty or data["target"].nunique() < 2:
        print("  No data for inspection model")
        return None, {}

    data = pd.get_dummies(data, columns=["tipo"], drop_first=True)
    feat_cols = [
        "dias_desde_ultima", "n_corr_entre", "n_corr_30d", "n_corr_90d",
        "sistemas_distintos", "prop_repuestos", "duracion_promedio",
        "prev_resultado", "prev_defectos_highs", "prev_no_presentado",
    ] + [f"corr_{s}" for s in INSPECTION_SYSTEMS]
    feat_cols += [c for c in data.columns if c.startswith("tipo_")]
    feat_cols = [c for c in feat_cols if c in data.columns]

    dates = data["fecha_evento"].values
    X_all = data[feat_cols].fillna(0).values
    y_all = data["target"].values

    splits = _expanding_splits(dates, n_folds)
    fold_metrics = []
    for fold, (train_cutoff, test_start, test_end) in enumerate(splits):
        train_mask = dates < train_cutoff
        test_mask = (dates >= test_start) & (dates <= test_end)
        if test_mask.sum() < 5 or len(np.unique(y_all[test_mask])) < 2:
            continue

        X_train, y_train = X_all[train_mask], y_all[train_mask]
        X_test, y_test = X_all[test_mask], y_all[test_mask]

        pos_rate = y_train.mean()
        t0 = time.time()
        model = XGBClassifier(
            n_estimators=100, max_depth=4, learning_rate=0.05,
            subsample=0.8, random_state=42, n_jobs=1,
            scale_pos_weight=(1 - pos_rate) / max(pos_rate, 0.01),
        )
        model.fit(X_train, y_train, verbose=False)
        y_pred = model.predict(X_test)
        y_proba = model.predict_proba(X_test)[:, 1]
        fit_time = time.time() - t0

        m = _compute_binary_metrics(y_test, y_pred, y_proba)
        m["fold"] = fold + 1
        m["n_train"] = train_mask.sum()
        m["n_test"] = test_mask.sum()
        m["fit_s"] = round(fit_time, 2)
        fold_metrics.append(m)

        del X_train, y_train, X_test, y_test, model
        gc.collect()
        print(f"  Fold {fold+1}: AUC={m['roc_auc']}  F1={m['f1']}")

    if fold_metrics:
        aucs = [m["roc_auc"] for m in fold_metrics if m["roc_auc"] is not None]
        f1s = [m["f1"] for m in fold_metrics if m["f1"] is not None]
        print(f"  CV summary: AUC = {np.mean(aucs):.4f} ± {np.std(aucs):.4f}  |  F1 = {np.mean(f1s):.4f} ± {np.std(f1s):.4f}")

    t0 = time.time()
    final_model = XGBClassifier(
        n_estimators=100, max_depth=4, learning_rate=0.05,
        subsample=0.8, random_state=42, n_jobs=1,
        scale_pos_weight=(1 - y_all.mean()) / max(y_all.mean(), 0.01),
    )
    final_model.fit(X_all, y_all, verbose=False)
    fit_time = time.time() - t0

    importances = sorted(zip(feat_cols, final_model.feature_importances_), key=lambda x: -x[1])[:10]

    meta = {
        "model": "inspection_failure",
        "feature_cols": feat_cols,
        "pos_rate": round(float(y_all.mean()), 4),
        "cv_folds": fold_metrics,
        "cv_auc_mean": round(float(np.mean(aucs)), 4) if fold_metrics else None,
        "cv_auc_std": round(float(np.std(aucs)), 4) if fold_metrics else None,
        "cv_f1_mean": round(float(np.mean(f1s)), 4) if fold_metrics else None,
        "top_features": [(n, round(float(v), 4)) for n, v in importances],
        "fit_s": round(fit_time, 2),
        "n_train": len(y_all),
    }

    print(f"  Final model: {meta['n_train']} rows, {fit_time:.1f}s")
    print(f"  Top features: {', '.join(f'{n}({v:.3f})' for n, v in importances[:5])}")

    del X_all, y_all, data
    gc.collect()
    return final_model, meta


# ═══════════════════════════════════════════════════════════════════════════════
# SAVE & MAIN
# ═══════════════════════════════════════════════════════════════════════════════

def _save_model(model, meta, name):
    pkl_path = MODELS_DIR / f"{name}.pkl"
    json_path = MODELS_DIR / f"{name}_meta.json"
    with open(pkl_path, "wb") as f:
        pickle.dump(model, f, protocol=pickle.HIGHEST_PROTOCOL)
    with open(json_path, "w") as f:
        json.dump(meta, f, indent=2, default=str)
    pkl_size = pkl_path.stat().st_size / 1024
    print(f"  Saved {pkl_path} ({pkl_size:.0f} KB) + {json_path}")


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--folds", type=int, default=3, help="Number of CV folds")
    args = parser.parse_args()

    print("=" * 60)
    print("TRAINING PRODUCTION MODELS (expanding-window CV)")
    print(f"Folds: {args.folds}")
    print("=" * 60)

    print("\nLoading data...")
    df = load_data()
    print(f"  {len(df)} rows, {df['placa_patente'].nunique()} buses")
    gc.collect()

    # Train all 3 models
    models = {}

    w_model, w_meta = train_weekly_cv(df, args.folds)
    if w_model:
        _save_model(w_model, w_meta, "weekly_model")
        models["weekly"] = w_meta
        del w_model
        gc.collect()

    s_model, s_meta = train_spike_cv(df, args.folds)
    if s_model:
        _save_model(s_model, s_meta, "spike_model")
        models["spike"] = s_meta
        del s_model
        gc.collect()

    p_model, p_meta = train_parts_cv(df, args.folds)
    if p_model:
        _save_model(p_model, p_meta, "parts_model")
        models["parts"] = p_meta
        del p_model
        gc.collect()

    i_model, i_meta = train_inspection_cv(df, args.folds)
    if i_model:
        _save_model(i_model, i_meta, "inspection_model")
        models["inspection"] = i_meta
        del i_model
        gc.collect()

    del df
    gc.collect()

    # ── Summary ──
    print("\n" + "=" * 80)
    print("TRAINING SUMMARY")
    print("=" * 80)
    for name, meta in models.items():
        cv_folds = meta.get("cv_folds", [])
        if meta["model"] == "weekly_system_load":
            r2s = [f["r2"] for f in cv_folds if f.get("r2") is not None]
            maes = [f["mae"] for f in cv_folds if f.get("mae") is not None]
            print(f"\n  {name}: R² = {np.mean(r2s):.4f} ± {np.std(r2s):.4f}  |  MAE = {np.mean(maes):.1f} ± {np.std(maes):.1f}  |  trained on {meta['n_train']:,} rows")
        else:
            aucs = [f["roc_auc"] for f in cv_folds if f.get("roc_auc") is not None]
            f1s = [f["f1"] for f in cv_folds if f.get("f1") is not None]
            print(f"\n  {name}: AUC = {np.mean(aucs):.4f} ± {np.std(aucs):.4f}  |  F1 = {np.mean(f1s):.4f} ± {np.std(f1s):.4f}  |  pos_rate = {meta['pos_rate']:.1%}")
        top3 = meta.get("top_features", [])[:3]
        print(f"    Top features: {' | '.join(f'{n} ({v:.3f})' for n, v in top3)}")

    print("\n✅ Models saved to models/")
    print("Load with: pickle.load(open('models/weekly_model.pkl','rb'))")


if __name__ == "__main__":
    main()
