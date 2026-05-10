"""Batch inference: generate predictions for all buses using trained XGBoost models.

Uses the expanded feature set from model_meta.json per horizon.
"""
from __future__ import annotations

import json
import pickle
import sys
import warnings
from pathlib import Path

import pandas as pd

warnings.filterwarnings("ignore")

sys.path.append(str(Path(__file__).resolve().parent.parent))

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
MODELS_DIR = PROJECT_ROOT / "models"
PREDICTIONS_DIR = PROJECT_ROOT / "data" / "predictions"

HORIZONS = [7, 5, 3]
THRESHOLD = 0.5


def load_model(horizon_days: int):
    path = MODELS_DIR / f"xgb_{horizon_days}d.pkl"
    with open(path, "rb") as f:
        return pickle.load(f)


def load_feature_names(horizon_days: int) -> list[str] | None:
    meta_path = MODELS_DIR / f"xgb_{horizon_days}d_meta.json"
    if not meta_path.exists():
        return None
    with open(meta_path) as f:
        meta = json.load(f)
    return meta.get("feature_names")


def load_feature_importance(horizon_days: int) -> list[tuple[str, float]] | None:
    model = load_model(horizon_days)
    if not hasattr(model, "feature_importances_") or not hasattr(model, "feature_names_in_"):
        return None
    names = model.feature_names_in_.tolist() if hasattr(model.feature_names_in_, "tolist") else list(model.feature_names_in_)
    scores = model.feature_importances_.tolist() if hasattr(model.feature_importances_, "tolist") else list(model.feature_importances_)
    pairs = sorted(zip(names, scores), key=lambda x: -x[1])
    return pairs


def classify_severity(row: pd.Series) -> str:
    has_parts = row.get("repuestos_count_evento", 0) > 0
    duration = row.get("duracion_ot_horas_prom_evento", 0) or 0
    keywords = row.get("num_keywords_tecnicos_evento", 0) or 0
    if not has_parts and duration < 2 and keywords == 0:
        return "LOW"
    elif has_parts and duration > 4:
        return "HIGH"
    return "MEDIUM"


def run_inference():
    PREDICTIONS_DIR.mkdir(parents=True, exist_ok=True)

    features_df = pd.read_parquet(DATA_PROCESSED_DIR / "features.parquet")
    eventos_df = pd.read_parquet(DATA_PROCESSED_DIR / "eventos.parquet")

    severity_cols = [
        "repuestos_count_evento", "duracion_ot_horas_prom_evento",
        "num_keywords_tecnicos_evento",
    ]
    available_sev = [c for c in severity_cols if c in eventos_df.columns and c not in features_df.columns]
    if available_sev:
        sev_info = eventos_df[["placa_patente", "fecha_evento"] + available_sev].copy()
        features_df = features_df.merge(sev_info, on=["placa_patente", "fecha_evento"], how="left")

    features_df["severity"] = features_df.apply(classify_severity, axis=1)

    all_preds = []
    for horizon in HORIZONS:
        feature_names = load_feature_names(horizon)
        if feature_names is None:
            print(f"  WARNING: No meta.json for {horizon}d, falling back to DEFAULT_FEATURE_COLUMNS")
            from src.feature_engineering import DEFAULT_FEATURE_COLUMNS
            feature_names = DEFAULT_FEATURE_COLUMNS

        missing = [c for c in feature_names if c not in features_df.columns]
        if missing:
            print(f"  WARNING: {len(missing)} features missing for {horizon}d: {missing}")

        X = pd.DataFrame(index=features_df.index)
        for c in feature_names:
            if c in features_df.columns:
                X[c] = pd.to_numeric(features_df[c], errors="coerce").fillna(0)
            else:
                X[c] = 0.0

        model = load_model(horizon)
        y_prob = model.predict_proba(X)[:, 1]
        y_pred = (y_prob >= THRESHOLD).astype(int)

        top_features = load_feature_importance(horizon)
        top_str = ""
        if top_features:
            top5 = [(n, round(s, 4)) for n, s in top_features[:5]]
            top_str = " [" + ", ".join(f"{n}={s}" for n, s in top5) + "]"

        horizon_preds = pd.DataFrame({
            "placa_patente": features_df["placa_patente"],
            "fecha_evento": features_df["fecha_evento"],
            "horizon_days": horizon,
            "probability": y_prob,
            "alert": y_pred.astype(bool),
            "severity": features_df["severity"],
        })
        all_preds.append(horizon_preds)
        alerts = y_pred.sum()
        print(f"  {horizon}d: {len(horizon_preds)} events, {alerts} alerts ({alerts/len(horizon_preds)*100:.1f}%){top_str}")

    predictions_df = pd.concat(all_preds, ignore_index=True)
    predictions_df = predictions_df.sort_values(
        ["placa_patente", "fecha_evento", "horizon_days"]
    ).reset_index(drop=True)

    output_path = PREDICTIONS_DIR / "predictions.parquet"
    predictions_df.to_parquet(output_path, index=False)
    print(f"\nPredictions saved: {output_path}")
    print(f"Total predictions: {len(predictions_df)}")
    print(f"Unique buses: {predictions_df['placa_patente'].nunique()}")
    print(f"Alerts at threshold={THRESHOLD}: {predictions_df['alert'].sum()}")

    print("\n--- Top 20 buses by alert count (7d horizon) ---")
    last_preds = predictions_df[predictions_df["horizon_days"] == 7].copy()
    bus_risk = (
        last_preds.groupby("placa_patente")
        .agg(
            total_events=("alert", "count"),
            alerts=("alert", "sum"),
            max_prob=("probability", "max"),
            mean_prob=("probability", "mean"),
            last_event=("fecha_evento", "max"),
        )
        .reset_index()
    )
    bus_risk["alert_rate"] = bus_risk["alerts"] / bus_risk["total_events"]
    bus_risk = bus_risk.sort_values("alerts", ascending=False)
    print(bus_risk.head(20).to_string(index=False))


if __name__ == "__main__":
    run_inference()
