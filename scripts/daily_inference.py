#!/usr/bin/env python3
"""Daily inference: fetch latest data from Firestore, run predictions, save fresh results.
Includes prediction_timestamp and historical log for Shadow Mode (Piloto 1).
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

from src.data_loader import load_from_firestore, load_json_files, load_single_json, normalize_inspection_records
from src.preprocessing import (
    clean_data,
    create_base_dataframe,
    create_eventos_dataframe,
    extract_additional_fields,
    merge_additional_event_fields,
)
from src.feature_engineering import (
    create_future_targets,
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
DATA_RAW_DIR = PROJECT_ROOT / "data" / "raw"

HORIZONS = [7, 5, 3]
THRESHOLD = 0.5
LABEL = "voy_redbus"

CLIENT_MAP: dict = {
    "14": "VOY", "15": "VOY",
    "11": "REDBUS", "13": "REDBUS",
    "8": "METROPOL", "9": "METROPOL",
    "16": "GRANAMERICAS",
    "17": "CONECTA", "19": "CONECTA",
}


def run_daily_inference(use_firestore: bool = True, recent_days: int | None = None):
    PREDICTIONS_DIR.mkdir(parents=True, exist_ok=True)

    if use_firestore:
        print("=" * 60)
        print("DAILY INFERENCE: Fetching latest data from Firestore")
        print("=" * 60)
        prev_raw, corr_raw = load_from_firestore()
        clean_df = clean_data(prev_raw, corr_raw, empresa_id="ALL")
    else:
        print("=" * 60)
        print("DAILY INFERENCE: Loading local JSON files")
        print("=" * 60)
        firestore_dir = DATA_RAW_DIR / "firestore"
        prev_raw, corr_raw = load_json_files(
            firestore_dir / "preventivos.json",
            firestore_dir / "correctivos.json",
        )
        regb_raw = load_single_json(firestore_dir / "estado_general.json")
        it_raw = load_single_json(firestore_dir / "inspeccion_tecnica.json")
        regb_df = normalize_inspection_records(regb_raw, "REGB") if regb_raw else None
        it_df = normalize_inspection_records(it_raw, "IT") if it_raw else None
        print(f"  Preventivos: {len(prev_raw)}, Correctivos: {len(corr_raw)}")
        print(f"  REGB: {len(regb_df) if regb_df is not None else 0}, IT: {len(it_df) if it_df is not None else 0}")
        clean_df = clean_data(prev_raw, corr_raw, regb_df=regb_df, it_df=it_df, empresa_id="ALL")

    clean_df = extract_additional_fields(clean_df)

    if "unidad_negocio" in clean_df.columns:
        clean_df["empresa_id"] = clean_df["unidad_negocio"].map(CLIENT_MAP).fillna("OTROS")

    base_df = create_base_dataframe(clean_df, executed_only=True)
    print(f"  Records: {len(base_df)}, Buses: {base_df['placa_patente'].nunique()}")
    print(f"  Date range: {base_df['fecha_evento'].min()} \u2192 {base_df['fecha_evento'].max()}")

    # Step 2: Create events
    eventos_df = create_eventos_dataframe(base_df)
    eventos_df = merge_additional_event_fields(base_df, eventos_df)
    print(f"  Events: {len(eventos_df)}, Buses: {eventos_df['placa_patente'].nunique()}")

    # Optionally filter to recent data only for faster inference
    if recent_days is not None:
        _cutoff = pd.Timestamp.now() - pd.Timedelta(days=recent_days)
        _before = len(eventos_df)
        eventos_df = eventos_df[eventos_df["fecha_evento"] >= _cutoff].copy()
        print(f"  Filtered to last {recent_days} days: {_before} → {len(eventos_df)} events")

    # Step 3: Feature engineering
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
    print(f"  Features shape: {features_df.shape}")

    # Step 4: Inference with trained models
    all_preds = []
    for window in HORIZONS:
        model_path = MODELS_DIR / f"xgb_{window}d_{LABEL}.pkl"
        meta_path = MODELS_DIR / f"xgb_{window}d_{LABEL}_meta.json"
        if not model_path.exists():
            print(f"  WARNING: Model {model_path} not found, skipping")
            continue

        # Load per-horizon threshold from model meta
        if meta_path.exists():
            with open(meta_path) as f:
                meta = json.load(f)
            threshold = meta.get("decision_threshold", THRESHOLD)
        else:
            threshold = THRESHOLD

        with open(model_path, "rb") as f:
            model = pickle.load(f)

        feature_columns = model.feature_names_in_.tolist() if hasattr(model, "feature_names_in_") else []

        X = pd.DataFrame(index=features_df.index)
        for c in feature_columns:
            X[c] = pd.to_numeric(features_df.get(c, 0), errors="coerce").fillna(0)

        y_prob = model.predict_proba(X)[:, 1]
        y_pred = (y_prob >= threshold).astype(int)

        # Severity classification
        def classify(row):
            hp = row.get("repuestos_count_evento", 0) or 0
            dur = row.get("duracion_ot_horas_prom_evento", 0) or 0
            kw = row.get("num_keywords_tecnicos_evento", 0) or 0
            if not hp and dur < 2 and kw == 0:
                return "LOW"
            elif hp and dur > 4:
                return "HIGH"
            return "MEDIUM"

        severity_df = pd.DataFrame({
            "repuestos_count_evento": features_df.get("repuestos_count_evento", 0),
            "duracion_ot_horas_prom_evento": features_df.get("duracion_ot_horas_prom_evento", 0),
            "num_keywords_tecnicos_evento": features_df.get("num_keywords_tecnicos_evento", 0),
        })
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
        print(f"  {window}d: threshold={threshold:.2f} {len(preds)} events, {alerts} alerts ({alerts/len(preds)*100:.1f}%)")

    now = datetime.now()
    predictions_df = pd.concat(all_preds, ignore_index=True)
    predictions_df["prediction_timestamp"] = now
    predictions_df["operator"] = predictions_df.get("empresa_id", "ALL")

    predictions_df = predictions_df.sort_values(
        ["placa_patente", "fecha_evento", "horizon_days"]
    ).reset_index(drop=True)

    path = PREDICTIONS_DIR / "predictions_daily.parquet"
    predictions_df.to_parquet(path, index=False)

    # Append to historical log
    log_path = PREDICTIONS_DIR / "predictions_log.parquet"
    if log_path.exists():
        existing_log = pd.read_parquet(log_path)
        combined_log = pd.concat([existing_log, predictions_df], ignore_index=True)
        combined_log = combined_log.drop_duplicates(
            subset=["placa_patente", "fecha_evento", "horizon_days", "prediction_timestamp"]
        ).reset_index(drop=True)
        combined_log.to_parquet(log_path, index=False)
    else:
        predictions_df.to_parquet(log_path, index=False)

    print(f"\n  Saved: {path}")
    print(f"  Log appended: {log_path} ({len(predictions_df)} new rows)")
    print(f"  Total predictions: {len(predictions_df)}")
    print(f"  Unique buses: {predictions_df['placa_patente'].nunique()}")
    print(f"  Alerts: {predictions_df['alert'].sum()}")
    print(f"  Prediction timestamp: {now.isoformat()}")
    return predictions_df


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Daily inference for Piloto 1")
    parser.add_argument("--local-json", action="store_true",
                        help="Usar archivos JSON locales en vez de Firestore")
    parser.add_argument("--recent-days", type=int, default=None,
                        help="Procesar solo los últimos N días (más rápido)")
    args = parser.parse_args()
    run_daily_inference(use_firestore=not args.local_json, recent_days=args.recent_days)
