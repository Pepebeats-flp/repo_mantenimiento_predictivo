#!/usr/bin/env python3
"""Fast inference using pre-processed event data + trained models.
Usage: python3 scripts/run_fast_inference.py [--recent-days 30]
"""
from __future__ import annotations

import json
import pickle
import sys
import warnings
from datetime import datetime
from pathlib import Path

import pandas as pd

warnings.filterwarnings("ignore")

sys.path.append(str(Path(__file__).resolve().parent.parent))

from src.feature_engineering import (
    create_temporal_features,
    generate_bus_history_features,
    generate_cause_based_features,
    generate_inventory_features,
    generate_rolling_features,
    generate_system_features,
    generate_text_pattern_features,
    generate_event_type_features,
    generate_severity_features,
    generate_trend_features,
    generate_bus_age_features,
)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
MODELS_DIR = PROJECT_ROOT / "models"
PREDICTIONS_DIR = PROJECT_ROOT / "data" / "predictions"
DATA_PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
HORIZONS = [7, 5, 3]
THRESHOLD = 0.5
LABEL = "voy_redbus"


def run_fast_inference(recent_days: int = 30):
    PREDICTIONS_DIR.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("FAST INFERENCE: Using pre-processed events")
    print("=" * 60)

    # Load pre-processed events (already cleaned + basic features)
    eventos_path = DATA_PROCESSED_DIR / "eventos_train.parquet"
    if not eventos_path.exists():
        print(f"ERROR: {eventos_path} not found. Run the full pipeline first.")
        return None

    eventos_df = pd.read_parquet(eventos_path)
    eventos_df["fecha_evento"] = pd.to_datetime(eventos_df["fecha_evento"], errors="coerce")
    print(f"  Events loaded: {len(eventos_df)}, Buses: {eventos_df['placa_patente'].nunique()}")
    print(f"  Date range: {eventos_df['fecha_evento'].min()} -> {eventos_df['fecha_evento'].max()}")

    # Feature engineering on ALL events (bus_history, rolling etc need full context)
    t0 = datetime.now()
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
    elapsed = (datetime.now() - t0).total_seconds()
    print(f"  Features: {features_df.shape[1]} columns, in {elapsed:.1f}s")

    # Filter features to recent window (after feature engineering)
    if recent_days and recent_days > 0:
        pred_max = eventos_df["fecha_evento"].max()
        cutoff = pred_max - pd.Timedelta(days=recent_days)
        _before = len(features_df)
        features_df = features_df[features_df["fecha_evento"] >= cutoff].copy()
        print(f"  Filtered to last {recent_days}d from {pred_max.date()}: {_before} -> {len(features_df)} events")

    # Inference with trained models
    all_preds = []
    for window in HORIZONS:
        model_path = MODELS_DIR / f"xgb_{window}d_{LABEL}.pkl"
        meta_path = MODELS_DIR / f"xgb_{window}d_{LABEL}_meta.json"

        if not model_path.exists():
            print(f"  WARNING: Model {model_path} not found, skipping")
            continue

        with open(model_path, "rb") as f:
            model = pickle.load(f)

        if meta_path.exists():
            with open(meta_path) as f:
                meta = json.load(f)
            threshold = meta.get("decision_threshold", THRESHOLD)
        else:
            threshold = THRESHOLD

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

        severity_df = pd.DataFrame(index=features_df.index)
        for _col in ["repuestos_count_evento", "duracion_ot_horas_prom_evento", "num_keywords_tecnicos_evento"]:
            severity_df[_col] = features_df[_col] if _col in features_df.columns else pd.Series(0, index=features_df.index)
        features_df["severity"] = severity_df.apply(classify, axis=1)

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
        print(f"  {window}d: threshold={threshold:.2f} {len(preds)} events, {alerts} alerts")

    if not all_preds:
        print("ERROR: No predictions generated")
        return None

    now = datetime.now()
    predictions_df = pd.concat(all_preds, ignore_index=True)
    predictions_df["prediction_timestamp"] = now

    predictions_df = predictions_df.sort_values(
        ["placa_patente", "fecha_evento", "horizon_days"]
    ).reset_index(drop=True)

    # Save
    path = PREDICTIONS_DIR / "predictions_daily.parquet"
    predictions_df.to_parquet(path, index=False)

    # Also update the main predictions file
    main_path = PREDICTIONS_DIR / "predictions_voy_redbus.parquet"
    predictions_df.to_parquet(main_path, index=False)

    print(f"\n  Saved: {path}")
    print(f"  Updated: {main_path}")
    print(f"  Total predictions: {len(predictions_df)}")
    print(f"  Buses: {predictions_df['placa_patente'].nunique()}")
    print(f"  Alerts: {predictions_df['alert'].sum()}")
    print(f"  Prediction timestamp: {now.isoformat()}")
    print(f"  Date range: {predictions_df['fecha_evento'].min()} -> {predictions_df['fecha_evento'].max()}")
    return predictions_df


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Fast inference using pre-processed events")
    parser.add_argument("--recent-days", type=int, default=90,
                        help="Process only last N days (default: 90)")
    args = parser.parse_args()
    run_fast_inference(recent_days=args.recent_days)
