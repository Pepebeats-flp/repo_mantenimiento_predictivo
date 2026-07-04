#!/usr/bin/env python3
"""Daily inference: fetch data, optionally retrain, predict per-bus per-day, save + log.

Unified replacement for fetch_and_predict.py, run_fast_inference.py, and the old daily_inference.py.

Usage:
    python3 scripts/daily_inference.py                         # Fetch Firestore, infer with existing models
    python3 scripts/daily_inference.py --local-json            # Use local JSON files
    python3 scripts/daily_inference.py --train --recent-days 90 # Fetch + retrain + infer
    python3 scripts/daily_inference.py --experiment 009_th06_all --train  # With experiment config
"""
from __future__ import annotations

import json
import os
import pickle
import sys
import time
import warnings
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

sys.path.append(str(Path(__file__).resolve().parent.parent))

from src.data_loader import (
    load_from_firestore,
    load_json_files,
    load_single_json,
    normalize_inspection_records,
)
from src.preprocessing import (
    clean_data,
    create_base_dataframe,
    create_eventos_dataframe,
    extract_additional_fields,
    merge_additional_event_fields,
    classify_severity,
)
from src.feature_engineering import (
    create_future_targets,
    create_temporal_features,
    generate_bus_age_features,
    generate_bus_history_features,
    generate_cause_based_features,
    generate_event_type_features,
    generate_inventory_features,
    generate_rolling_features,
    generate_severity_features,
    generate_system_features,
    generate_text_pattern_features,
    generate_bus_day_features,
    get_feature_columns,
)
from src.models import XGBoostModel

PROJECT_ROOT = Path(__file__).resolve().parent.parent
MODELS_DIR = PROJECT_ROOT / "models"
PREDICTIONS_DIR = PROJECT_ROOT / "data" / "predictions"
DATA_RAW_DIR = PROJECT_ROOT / "data" / "raw"
DATA_PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"

HORIZONS = [7, 5, 3]
THRESHOLD = 0.6
THRESHOLDS_BY_HORIZON = {"7": 0.6, "5": 0.6, "3": 0.6}
LABEL = "voy_redbus"

CLIENT_MAP: dict = {
    "14": "VOY", "15": "VOY",
    "11": "REDBUS", "13": "REDBUS",
    "8": "METROPOL", "9": "METROPOL",
    "16": "GRANAMERICAS",
    "17": "CONECTA", "19": "CONECTA",
}

MODEL_PARAMS: dict = {
    "n_estimators": 1200,
    "max_depth": 10,
    "learning_rate": 0.02,
    "subsample": 0.85,
    "colsample_bytree": 0.85,
    "min_child_weight": 3,
    "gamma": 0.1,
    "reg_alpha": 0.1,
    "reg_lambda": 2.0,
    "random_state": 42,
    "eval_metric": "logloss",
    "early_stopping_rounds": 50,
    "n_jobs": -1,
}


def _gpu_available() -> bool:
    try:
        import xgboost as xgb
        X_test = np.random.rand(10, 5)
        y_test = np.random.randint(0, 2, 10)
        m = xgb.XGBClassifier(n_estimators=1, device="cuda", verbosity=0)
        m.fit(X_test, y_test)
        return True
    except Exception:
        return False


def step01_fetch_and_clean(use_firestore: bool = True) -> pd.DataFrame:
    print("=" * 60)
    print("STEP 01: Fetch and clean data")
    print("=" * 60)
    t0 = time.time()

    regb_df: pd.DataFrame | None = None
    it_df: pd.DataFrame | None = None

    if use_firestore:
        prev_raw, corr_raw, regb_raw, it_raw = load_from_firestore()
        regb_df = normalize_inspection_records(regb_raw, "REGB")
        it_df = normalize_inspection_records(it_raw, "IT")
        print(f"  Firestore: PREV={len(prev_raw)}, CORR={len(corr_raw)}, "
              f"REGB={len(regb_df)}, IT={len(it_df)}")
        clean_df = clean_data(prev_raw, corr_raw, regb_df=regb_df, it_df=it_df, empresa_id="ALL")
    else:
        firestore_dir = DATA_RAW_DIR / "firestore"
        prev_raw, corr_raw = load_json_files(
            firestore_dir / "preventivos.json",
            firestore_dir / "correctivos.json",
        )
        regb_raw = load_single_json(firestore_dir / "estado_general.json")
        it_raw = load_single_json(firestore_dir / "inspeccion_tecnica.json")
        regb_df = normalize_inspection_records(regb_raw, "REGB") if regb_raw else None
        it_df = normalize_inspection_records(it_raw, "IT") if it_raw else None
        print(f"  Local JSON: PREV={len(prev_raw)}, CORR={len(corr_raw)}, "
              f"REGB={len(regb_df) if regb_df is not None else 0}, "
              f"IT={len(it_df) if it_df is not None else 0}")
        clean_df = clean_data(prev_raw, corr_raw, regb_df=regb_df, it_df=it_df, empresa_id="ALL")

    clean_df = extract_additional_fields(clean_df)
    if "unidad_negocio" in clean_df.columns:
        clean_df["empresa_id"] = clean_df["unidad_negocio"].map(CLIENT_MAP).fillna("OTROS")

    base_df = create_base_dataframe(clean_df, executed_only=True)
    print(f"  Records: {len(base_df)}, Buses: {base_df['placa_patente'].nunique()}")
    print(f"  Date range: {base_df['fecha_evento'].min()} -> {base_df['fecha_evento'].max()}")
    print(f"  ⏱ {time.time()-t0:.1f}s")
    return base_df


def step02_create_events(base_df: pd.DataFrame) -> pd.DataFrame:
    print("=" * 60)
    print("STEP 02: Create events")
    print("=" * 60)
    t0 = time.time()

    eventos_df = create_eventos_dataframe(base_df)
    eventos_df = merge_additional_event_fields(base_df, eventos_df)
    print(f"  Events: {len(eventos_df)}, Buses: {eventos_df['placa_patente'].nunique()}")
    print(f"  ⏱ {time.time()-t0:.1f}s")
    return eventos_df


def step03_feature_engineering(eventos_df: pd.DataFrame) -> pd.DataFrame:
    print("=" * 60)
    print("STEP 03: Feature engineering")
    print("=" * 60)
    t0 = time.time()

    features_df = generate_bus_history_features(eventos_df)
    features_df = generate_rolling_features(features_df)
    features_df = generate_cause_based_features(features_df)
    features_df = generate_system_features(features_df)
    features_df = generate_inventory_features(features_df)
    features_df = generate_text_pattern_features(features_df)
    features_df = generate_event_type_features(features_df)
    features_df = generate_severity_features(features_df)
    features_df = generate_bus_age_features(features_df)
    features_df = create_temporal_features(features_df)
    features_df = create_future_targets(features_df, windows=(7, 5, 3, 10, 14, 30))

    print(f"  Features: {features_df.shape}")
    print(f"  ⏱ {time.time()-t0:.1f}s")
    return features_df


def step04_train_models(
    features_df: pd.DataFrame,
    cutoff_date: pd.Timestamp,
    experiment_cfg: dict | None = None,
):
    print("=" * 60)
    print("STEP 04: Train models")
    print("=" * 60)
    t0 = time.time()

    train_df = features_df[features_df["fecha_evento"] < cutoff_date].copy()
    print(f"  Train: {len(train_df)} events before {cutoff_date.date()}")

    feature_cols = get_feature_columns(features_df)
    if experiment_cfg:
        drop = experiment_cfg.get("drop_features", [])
        keep = experiment_cfg.get("keep_features", None)
        if keep:
            feature_cols = [c for c in feature_cols if c in keep]
        elif drop:
            feature_cols = [c for c in feature_cols if c not in drop]
    print(f"  Using {len(feature_cols)} features")

    device = "cuda" if _gpu_available() else "cpu"
    print(f"  Device: {device}")

    model_config = dict(MODEL_PARAMS)
    if experiment_cfg and experiment_cfg.get("model_params"):
        model_config.update(experiment_cfg["model_params"])
    model_config["device"] = device

    spw_mult = 1.0
    if experiment_cfg:
        spw_mult = experiment_cfg.get("scale_pos_weight_multiplier", 1.0)

    MODELS_DIR.mkdir(parents=True, exist_ok=True)

    for window in HORIZONS:
        target_col = f"correctivo_prox_{window}d"
        print(f"\n  -- {window}d --")

        X_all = train_df[feature_cols].fillna(0)
        y_all = train_df[target_col].astype(int)

        val_frac = 0.1
        split_idx = int(len(train_df) * (1 - val_frac))
        X_train_data = X_all.iloc[:split_idx]
        y_train_data = y_all.iloc[:split_idx]
        X_val = X_all.iloc[split_idx:]
        y_val = y_all.iloc[split_idx:]

        pos = int(y_train_data.sum())
        neg = int((1 - y_train_data).sum())
        pos_rate = pos / len(y_train_data) if len(y_train_data) else 0.0
        spw = (neg / pos if pos else 1.0) * spw_mult

        params = dict(model_config)
        params["scale_pos_weight"] = spw

        print(f"  Train: {len(y_train_data)} ({pos} pos, {pos_rate:.1%})  Val: {len(y_val)}  SPW={spw:.2f}")

        threshold = 0.5
        thresholds_override = experiment_cfg.get("thresholds", {}) if experiment_cfg else {}
        if str(window) in thresholds_override:
            threshold = float(thresholds_override[str(window)])
        elif experiment_cfg and experiment_cfg.get("threshold"):
            threshold = float(experiment_cfg["threshold"])

        xgb_model = XGBoostModel(**params)
        xgb_model.fit(X_train_data, y_train_data, X_val, y_val)

        with open(MODELS_DIR / f"xgb_{window}d_{LABEL}.pkl", "wb") as f:
            pickle.dump(xgb_model, f)

        meta = {
            "experiment": experiment_cfg.get("_name", None) if experiment_cfg else None,
            "feature_names": feature_cols,
            "feature_count": len(feature_cols),
            "model_params": {k: str(v) if not isinstance(v, (int, float, bool)) else v
                             for k, v in params.items()},
            "scale_pos_weight": spw,
            "cutoff_date": str(cutoff_date),
            "decision_threshold": threshold,
            "model": "XGBoost",
        }
        with open(MODELS_DIR / f"xgb_{window}d_{LABEL}_meta.json", "w") as f:
            json.dump(meta, f, indent=2)

        print(f"  Models saved: xgb_{window}d_{LABEL}.pkl + meta")
        print(f"  Decision threshold: {threshold:.2f}")

    print(f"\n  ⏱ Total train: {time.time()-t0:.1f}s")


def step05_inference(
    features_df: pd.DataFrame,
    eventos_df: pd.DataFrame,
    recent_days: int | None = None,
    experiment_cfg: dict | None = None,
) -> pd.DataFrame:
    print("=" * 60)
    print("STEP 05: Inference (bus-day)")
    print("=" * 60)
    t0 = time.time()

    target_date = pd.Timestamp.now().normalize()

    bus_day_features = generate_bus_day_features(
        eventos_df, features_df, target_date=target_date,
    )
    if bus_day_features.empty:
        print("  WARNING: No active buses with recent events")
        return pd.DataFrame()

    print(f"  Active buses (event in last 90d): {len(bus_day_features)}")

    if recent_days:
        cutoff = target_date - pd.Timedelta(days=recent_days)
        bus_day_features = bus_day_features[
            bus_day_features["fecha_evento"] >= cutoff
        ].copy()
        print(f"  Filtered to last {recent_days}d: {len(bus_day_features)} buses")

    all_preds = []
    for window in HORIZONS:
        model_path = MODELS_DIR / f"xgb_{window}d_{LABEL}.pkl"
        meta_path = MODELS_DIR / f"xgb_{window}d_{LABEL}_meta.json"

        if not model_path.exists():
            print(f"  WARNING: Model {model_path} not found, skipping")
            continue

        with open(model_path, "rb") as f:
            model = pickle.load(f)

        threshold = THRESHOLDS_BY_HORIZON.get(str(window), THRESHOLD)

        if experiment_cfg:
            exp_thresholds = experiment_cfg.get("thresholds", {})
            if str(window) in exp_thresholds:
                threshold = float(exp_thresholds[str(window)])
            elif experiment_cfg.get("threshold"):
                threshold = float(experiment_cfg["threshold"])

        feature_names = (
            meta.get("feature_names", [])
            if meta_path.exists() else get_feature_columns(features_df)
        )

        X = pd.DataFrame(index=bus_day_features.index)
        for c in feature_names:
            if c in bus_day_features.columns:
                X[c] = pd.to_numeric(bus_day_features[c], errors="coerce").fillna(0)
            else:
                X[c] = 0.0

        if hasattr(model, "predict_proba"):
            y_prob = model.predict_proba(X)
        elif hasattr(model, "predict"):
            y_prob = model.predict(X).astype(float)
        else:
            y_prob = model.predict(X).astype(float)
        if hasattr(y_prob, "ndim") and y_prob.ndim > 1:
            y_prob = y_prob[:, 1]
        else:
            y_prob = model.predict(X).astype(float)

        y_pred = (y_prob >= threshold).astype(int)

        severity_df = pd.DataFrame(index=bus_day_features.index)
        for col in ["repuestos_count_evento", "duracion_ot_horas_prom_evento",
                     "num_keywords_tecnicos_evento", "inspeccion_total_highs_evento"]:
            severity_df[col] = (
                bus_day_features[col] if col in bus_day_features.columns
                else pd.Series(0, index=bus_day_features.index)
            )
        bus_day_features["severity"] = severity_df.apply(classify_severity, axis=1)

        preds = pd.DataFrame({
            "placa_patente": bus_day_features["placa_patente"],
            "fecha_prediccion": target_date,
            "fecha_ultimo_evento": bus_day_features["fecha_evento"],
            "horizon_days": window,
            "probability": y_prob,
            "alert": y_pred.astype(bool),
            "severity": bus_day_features["severity"],
            "threshold": threshold,
        })
        all_preds.append(preds)
        alerts = y_pred.sum()
        print(f"  {window}d: th={threshold:.2f} {len(preds)} buses, {alerts} alerts ({alerts/len(preds)*100:.1f}%)")

    if not all_preds:
        print("  ERROR: No predictions generated")
        return pd.DataFrame()

    now = datetime.now()
    predictions_df = pd.concat(all_preds, ignore_index=True)
    predictions_df["prediction_timestamp"] = now

    predictions_df = predictions_df.sort_values(
        ["placa_patente", "horizon_days"]
    ).reset_index(drop=True)

    print(f"  ⏱ {time.time()-t0:.1f}s")
    return predictions_df


def save_predictions(predictions_df: pd.DataFrame):
    PREDICTIONS_DIR.mkdir(parents=True, exist_ok=True)

    path = PREDICTIONS_DIR / "predictions_daily.parquet"
    predictions_df.to_parquet(path, index=False)
    print(f"  Saved: {path}")

    main_path = PREDICTIONS_DIR / "predictions_voy_redbus.parquet"
    predictions_df.to_parquet(main_path, index=False)
    print(f"  Updated: {main_path}")

    log_path = PREDICTIONS_DIR / "predictions_log.parquet"
    if log_path.exists():
        existing_log = pd.read_parquet(log_path)
        combined_log = pd.concat([existing_log, predictions_df], ignore_index=True)
        combined_log = combined_log.drop_duplicates(
            subset=["placa_patente", "fecha_prediccion", "horizon_days", "prediction_timestamp"]
        ).reset_index(drop=True)
        combined_log.to_parquet(log_path, index=False)
        print(f"  Log appended: {log_path} ({len(predictions_df)} new rows, {len(combined_log)} total)")
    else:
        predictions_df.to_parquet(log_path, index=False)
        print(f"  Log created: {log_path}")

    print(f"\n  Total predictions: {len(predictions_df)}")
    print(f"  Unique buses: {predictions_df['placa_patente'].nunique()}")
    print(f"  Alerts: {predictions_df['alert'].sum()}")
    print(f"  Prediction timestamp: {datetime.now().isoformat()}")


def run_daily_inference(
    use_firestore: bool = True,
    recent_days: int | None = None,
    do_train: bool = False,
    experiment_name: str | None = None,
):
    experiment_cfg: dict | None = None
    if experiment_name:
        cfg_path = PROJECT_ROOT / "config" / "experiments" / f"{experiment_name}.json"
        if cfg_path.exists():
            with open(cfg_path) as f:
                experiment_cfg = json.load(f)
            experiment_cfg["_name"] = experiment_name
            print(f"  Experiment config: {experiment_name}")
        else:
            print(f"  WARNING: Experiment config not found: {cfg_path}")

    base_df = step01_fetch_and_clean(use_firestore=use_firestore)
    eventos_df = step02_create_events(base_df)
    features_df = step03_feature_engineering(eventos_df)

    if do_train:
        cutoff_date = pd.Timestamp.now() - pd.Timedelta(days=30)
        step04_train_models(features_df, cutoff_date, experiment_cfg)
    else:
        print("=" * 60)
        print("STEP 04: Using existing trained models (--train not set)")
        print("=" * 60)

    predictions_df = step05_inference(
        features_df, eventos_df, recent_days=recent_days,
        experiment_cfg=experiment_cfg,
    )
    if predictions_df.empty:
        print("  No predictions generated.")
        return None

    save_predictions(predictions_df)
    return predictions_df


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Daily inference for Piloto 1")
    parser.add_argument("--local-json", action="store_true",
                        help="Use local JSON files instead of Firestore")
    parser.add_argument("--recent-days", type=int, default=None,
                        help="Only predict for buses with events in last N days")
    parser.add_argument("--train", action="store_true",
                        help="Retrain models before inference")
    parser.add_argument("--experiment", type=str, default=None,
                        help="Experiment name in config/experiments/<name>.json")
    args = parser.parse_args()

    run_daily_inference(
        use_firestore=not args.local_json,
        recent_days=args.recent_days,
        do_train=args.train,
        experiment_name=args.experiment,
    )
