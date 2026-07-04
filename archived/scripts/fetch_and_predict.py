#!/usr/bin/env python3
"""Fetch fresh data from Firestore, re-run inference, update shadow evaluation.
Safe version that saves intermediate results and can be resumed.
"""
from __future__ import annotations

import json
import os
import pickle
import sys
import time
from datetime import datetime
from pathlib import Path

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parent.parent))

from src.data_loader import load_from_firestore, normalize_inspection_records
from src.preprocessing import (
    clean_data,
    create_base_dataframe,
    create_eventos_dataframe,
    extract_additional_fields,
    merge_additional_event_fields,
)
from src.feature_engineering import (
    create_future_targets, create_temporal_features,
    generate_bus_age_features, generate_bus_history_features,
    generate_cause_based_features, generate_event_type_features,
    generate_inventory_features, generate_rolling_features,
    generate_severity_features, generate_system_features,
    generate_text_pattern_features, generate_trend_features,
)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_RAW_DIR = PROJECT_ROOT / "data" / "raw"
DATA_PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
PREDICTIONS_DIR = PROJECT_ROOT / "data" / "predictions"
MODELS_DIR = PROJECT_ROOT / "models"

HORIZONS = [7, 5, 3]
THRESHOLD = 0.5
LABEL = "voy_redbus"

CLIENT_MAP = {
    "14": "VOY", "15": "VOY",
    "11": "REDBUS", "13": "REDBUS",
    "8": "METROPOL", "9": "METROPOL",
    "16": "GRANAMERICAS",
    "17": "CONECTA", "19": "CONECTA",
}


def step01_download_and_clean():
    """Download from Firestore → base.parquet (if not already cached)."""
    base_path = DATA_PROCESSED_DIR / "base.parquet"
    if base_path.exists():
        base_df = pd.read_parquet(base_path)
        base_df["fecha_evento"] = pd.to_datetime(base_df["fecha_evento"], errors="coerce")
        print(f"  Using cached base.parquet: {len(base_df)} records")
        return base_df

    print("=" * 60)
    print("STEP 01: Download from Firestore + Clean")
    print("=" * 60)

    _cred = str(PROJECT_ROOT / "slared-4de9d5a1e961.json")
    if os.path.exists(_cred):
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = _cred

    prev_raw, corr_raw, regb_raw, it_raw = load_from_firestore()
    regb_df = normalize_inspection_records(regb_raw, "REGB")
    it_df = normalize_inspection_records(it_raw, "IT")

    clean_df = clean_data(prev_raw, corr_raw, regb_df=regb_df, it_df=it_df, empresa_id="ALL")
    clean_df = extract_additional_fields(clean_df)
    if "unidad_negocio" in clean_df.columns:
        clean_df["empresa_id"] = clean_df["unidad_negocio"].map(CLIENT_MAP).fillna("OTROS")

    base_df = create_base_dataframe(clean_df, executed_only=True)
    print(f"  Base: {len(base_df)} records, {base_df['placa_patente'].nunique()} buses")
    print(f"  Date range: {base_df['fecha_evento'].min()} → {base_df['fecha_evento'].max()}")

    save_df = base_df.copy()
    for col in save_df.select_dtypes(include=["object"]).columns:
        if save_df[col].apply(lambda x: isinstance(x, list)).any():
            save_df[col] = save_df[col].apply(
                lambda x: json.dumps(x, default=str) if isinstance(x, list) else x
            )
    save_df.to_parquet(base_path, index=False)
    print(f"  Saved: {base_path}")
    return base_df


def step02_create_events(base_df: pd.DataFrame):
    """Create eventos.parquet (if not already cached)."""
    ev_path = DATA_PROCESSED_DIR / "eventos_all.parquet"
    if ev_path.exists():
        eventos_df = pd.read_parquet(ev_path)
        print(f"  Using cached eventos_all.parquet: {len(eventos_df)} events")
        return eventos_df

    print("=" * 60)
    print("STEP 02: Create events")
    print("=" * 60)
    t0 = time.time()

    eventos_df = create_eventos_dataframe(base_df)
    eventos_df = merge_additional_event_fields(base_df, eventos_df)
    print(f"  Events: {len(eventos_df)}, {eventos_df['placa_patente'].nunique()} buses")

    eventos_df.to_parquet(ev_path, index=False)
    print(f"  Saved: {ev_path}  ⏱ {time.time()-t0:.0f}s")
    return eventos_df


def step03_feature_engineering_and_infer(eventos_df: pd.DataFrame, recent_days: int):
    """Filter events to recent window, feature engineering, inference, save predictions."""
    print("=" * 60)
    print("STEP 03: Filter → Features → Inference")
    print("=" * 60)
    t0 = time.time()

    pred_max = eventos_df["fecha_evento"].max()
    cutoff = pred_max - pd.Timedelta(days=recent_days)
    before = len(eventos_df)
    eventos_df = eventos_df[eventos_df["fecha_evento"] >= cutoff].copy()
    print(f"  Filtered to last {recent_days}d from {pred_max.date()}: {before} → {len(eventos_df)} events")

    # Feature engineering
    features_df = generate_bus_history_features(eventos_df)
    features_df = generate_rolling_features(features_df)
    features_df = generate_cause_based_features(features_df)
    features_df = generate_system_features(features_df)
    features_df = generate_inventory_features(features_df)
    features_df = generate_text_pattern_features(features_df)
    features_df = generate_event_type_features(features_df)
    features_df = generate_severity_features(features_df)
    features_df = generate_trend_features(features_df)
    features_df = generate_bus_age_features(features_df)
    features_df = create_temporal_features(features_df)
    features_df = create_future_targets(features_df, windows=(7, 5, 3, 10, 14, 30))
    print(f"  Features: {features_df.shape}  ⏱ {time.time()-t0:.0f}s")

    # Save eventos_train for fast inference reuse
    eventos_df.to_parquet(DATA_PROCESSED_DIR / "eventos_train.parquet", index=False)
    features_df.to_parquet(DATA_PROCESSED_DIR / "features_all.parquet", index=False)
    print(f"  Saved eventos_train.parquet + features_all.parquet")

    # Inference
    all_preds = []
    for window in HORIZONS:
        model_path = MODELS_DIR / f"xgb_{window}d_{LABEL}.pkl"
        meta_path = MODELS_DIR / f"xgb_{window}d_{LABEL}_meta.json"
        if not model_path.exists():
            print(f"  WARNING: Model {model_path} not found")
            continue

        if meta_path.exists():
            threshold = json.loads(meta_path.read_text()).get("decision_threshold", THRESHOLD)
        else:
            threshold = THRESHOLD

        with open(model_path, "rb") as f:
            model = pickle.load(f)

        feature_columns = model.feature_names_in_.tolist() if hasattr(model, "feature_names_in_") else []

        X = pd.DataFrame(index=features_df.index)
        for c in feature_columns:
            _vals = features_df[c] if c in features_df.columns else pd.Series(0, index=features_df.index)
            X[c] = pd.to_numeric(_vals, errors="coerce").fillna(0)

        y_prob = model.predict_proba(X)[:, 1]
        y_pred = (y_prob >= threshold).astype(int)

        def classify(row):
            hp = float(row.get("repuestos_count_evento", 0) or 0)
            dur = float(row.get("duracion_ot_horas_prom_evento", 0) or 0)
            kw = float(row.get("num_keywords_tecnicos_evento", 0) or 0)
            if not hp and dur < 2 and kw == 0:
                return "LOW"
            elif hp and dur > 4:
                return "HIGH"
            return "MEDIUM"

        sev_df = pd.DataFrame(index=features_df.index)
        for col in ["repuestos_count_evento", "duracion_ot_horas_prom_evento", "num_keywords_tecnicos_evento"]:
            sev_df[col] = features_df[col] if col in features_df.columns else pd.Series(0, index=features_df.index)
        features_df["severity"] = sev_df.apply(classify, axis=1)

        preds = pd.DataFrame({
            "placa_patente": features_df["placa_patente"],
            "fecha_evento": features_df["fecha_evento"],
            "horizon_days": window,
            "probability": y_prob,
            "alert": y_pred.astype(bool),
            "severity": features_df["severity"],
            "threshold": threshold,
        })
        all_preds.append(preds)
        alerts = y_pred.sum()
        print(f"  {window}d: th={threshold:.2f} {len(preds)} events, {alerts} alerts ({alerts/len(preds)*100:.1f}%)")

    now = datetime.now()
    predictions_df = pd.concat(all_preds, ignore_index=True)
    predictions_df["prediction_timestamp"] = now
    predictions_df = predictions_df.sort_values(["placa_patente", "fecha_evento", "horizon_days"]).reset_index(drop=True)

    # Save predictions
    for name in ["predictions_daily.parquet", "predictions_voy_redbus.parquet"]:
        path = PREDICTIONS_DIR / name
        predictions_df.to_parquet(path, index=False)
        print(f"  Saved: {path}")

    # Append to historical log (deduplicate)
    log_path = PREDICTIONS_DIR / "predictions_log.parquet"
    if log_path.exists():
        existing_log = pd.read_parquet(log_path)
        combined_log = pd.concat([existing_log, predictions_df], ignore_index=True)
        combined_log = combined_log.drop_duplicates(
            subset=["placa_patente", "fecha_evento", "horizon_days", "prediction_timestamp"]
        ).reset_index(drop=True)
        combined_log.to_parquet(log_path, index=False)
        print(f"  Log appended: {log_path} ({len(predictions_df)} new rows)")
    else:
        predictions_df.to_parquet(log_path, index=False)
        print(f"  Log created: {log_path}")

    print(f"  Total: {len(predictions_df)} preds, {predictions_df['placa_patente'].nunique()} buses, {predictions_df['alert'].sum()} alerts")
    print(f"  Timestamp: {now.isoformat()}")
    print(f"  ⏱ Done: {time.time()-t0:.0f}s")
    return predictions_df


def main(recent_days: int = 4):
    t_start = time.time()
    base_df = step01_download_and_clean()
    eventos_df = step02_create_events(base_df)
    step03_feature_engineering_and_infer(eventos_df, recent_days)
    print(f"\n{'=' * 60}")
    print(f"✅ TOTAL: {time.time()-t_start:.0f}s")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--recent-days", type=int, default=4)
    args = parser.parse_args()
    main(recent_days=args.recent_days)
