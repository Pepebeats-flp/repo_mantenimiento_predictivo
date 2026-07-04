#!/usr/bin/env python3
"""Query bus predictions from the command line (bus-day format).

Usage:
    python scripts/consultar_bus.py                             # list all buses
    python scripts/consultar_bus.py FLXS22                     # query specific bus
    python scripts/consultar_bus.py FLXS22 --alerts             # only alerts
    python scripts/consultar_bus.py --top 10                    # top N risk buses
    python scripts/consultar_bus.py --horizon 3                 # filter by horizon
"""
from __future__ import annotations

import argparse
import json
import pickle
import sys
import warnings
from pathlib import Path

import pandas as pd

warnings.filterwarnings("ignore")

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_LABEL = "voy_redbus"
MODELS_DIR = PROJECT_ROOT / "models"


def load_predictions(label: str = DEFAULT_LABEL) -> pd.DataFrame:
    pred_path = PROJECT_ROOT / "data" / "predictions" / f"predictions_{label}.parquet"
    if not pred_path.exists():
        pred_path = PROJECT_ROOT / "data" / "predictions" / "predictions.parquet"
    if not pred_path.exists():
        pred_path = PROJECT_ROOT / "data" / "predictions" / "predictions_daily.parquet"
    if not pred_path.exists():
        print(f"ERROR: No predictions found")
        print("Run: python scripts/daily_inference.py --train")
        sys.exit(1)
    print(f"  Loading predictions from: {pred_path}")
    return pd.read_parquet(pred_path)


def load_feature_importance(horizon_days: int, label: str = DEFAULT_LABEL):
    model_path = MODELS_DIR / f"xgb_{horizon_days}d_{label}.pkl"
    if not model_path.exists():
        model_path = MODELS_DIR / f"xgb_{horizon_days}d.pkl"
    if not model_path.exists():
        return None
    with open(model_path, "rb") as f:
        model = pickle.load(f)
    if not hasattr(model, "feature_importances_") or not hasattr(model, "feature_names_in_"):
        return None
    names = model.feature_names_in_.tolist() if hasattr(model.feature_names_in_, "tolist") else list(model.feature_names_in_)
    scores = model.feature_importances_.tolist() if hasattr(model.feature_importances_, "tolist") else list(model.feature_importances_)
    return sorted(zip(names, scores), key=lambda x: -x[1])


def print_top_features(horizon: int, n: int = 8):
    pairs = load_feature_importance(horizon)
    if not pairs:
        return
    print(f"\n  Top {n} features ({horizon}d model):")
    for name, score in pairs[:n]:
        bar = "\u2588" * int(score * 200)
        print(f"    {name:<45} {score:.4f} {bar}")


def list_buses(predictions: pd.DataFrame, top: int | None = None):
    date_col = "fecha_prediccion" if "fecha_prediccion" in predictions.columns else "fecha_evento"
    horizon = 7
    df = predictions[predictions["horizon_days"] == horizon].copy()

    summary = (
        df.groupby("placa_patente")
        .agg(
            total_preds=("alert", "count"),
            alerts=("alert", "sum"),
            max_risk=("probability", "max"),
            avg_risk=("probability", "mean"),
            high_severity=("severity", lambda s: (s == "HIGH").sum()),
            last_pred=("fecha_prediccion", "max") if "fecha_prediccion" in df.columns else ("fecha_evento", "max"),
        )
        .reset_index()
    )
    summary["alert_rate"] = (summary["alerts"] / summary["total_preds"] * 100).round(1)
    summary["max_risk"] = (summary["max_risk"] * 100).round(1)
    summary["avg_risk"] = (summary["avg_risk"] * 100).round(1)
    summary = summary.sort_values("max_risk", ascending=False)

    if top:
        summary = summary.head(top)

    print(f"\n  Top buses by max risk ({horizon}d horizon):")
    print(f"  {'Bus':<12} {'Preds':>6} {'Alerts':>6} {'Alert%':>7} {'MaxRisk':>8} {'AvgRisk':>8} {'HighSev':>7}  LastPred")
    print("  " + "-" * 85)
    for _, row in summary.iterrows():
        lp = str(row["last_pred"])[:16] if pd.notna(row["last_pred"]) else "N/A"
        print(
            f"  {row['placa_patente']:<12} {int(row['total_preds']):>6} {int(row['alerts']):>6} "
            f"{float(row['alert_rate']):>6.1f}% {float(row['max_risk']):>7.1f}% {float(row['avg_risk']):>7.1f}% "
            f"{int(row['high_severity']):>6}  {lp:<16}"
        )

    print_top_features(7)


def query_bus(predictions: pd.DataFrame, bus: str, only_alerts: bool = False,
              horizon: int | None = None):
    df = predictions[predictions["placa_patente"] == bus.upper()].copy()
    date_col = "fecha_prediccion" if "fecha_prediccion" in df.columns else "fecha_evento"

    if df.empty:
        print(f"Bus '{bus.upper()}' not found in predictions.")
        matches = predictions[predictions["placa_patente"].str.contains(bus.upper(), na=False)]
        if not matches.empty:
            found = matches["placa_patente"].unique()
            print(f"Did you mean one of: {', '.join(found[:10])}")
        return

    if horizon:
        df = df[df["horizon_days"] == horizon]

    if only_alerts:
        df = df[df["alert"]]

    print(f"\n=== Bus {bus.upper()} ===")
    print(f"Total records: {len(df)}")
    print(f"Horizons: {sorted(df['horizon_days'].unique())}")

    for h in sorted(df["horizon_days"].unique()):
        sub = df[df["horizon_days"] == h]
        alerts = sub["alert"].sum()
        print(f"\n  {h}-day horizon:")
        print(f"    Predictions: {len(sub)}, Alerts: {alerts}, Alert rate: {alerts/len(sub)*100:.1f}%")
        print(f"    Max probability: {sub['probability'].max()*100:.1f}%")
        print(f"    Avg probability: {sub['probability'].mean()*100:.1f}%")

        if only_alerts:
            severe = sub[sub["severity"] == "HIGH"]
            print(f"    HIGH severity alerts: {len(severe)}")

    print(f"\n  Recent predictions:")
    last_df = df.sort_values(date_col, ascending=False).head(10)
    for _, row in last_df.iterrows():
        sev = row["severity"]
        risk = f"{row['probability']*100:.0f}%"
        alert_mark = "\u26a0 ALERT" if row["alert"] else "OK"
        date_str = str(row[date_col])[:16]
        print(f"    {date_str} | Risk: {risk:>4} | {alert_mark:>10} | Sev: {sev}")

    print_top_features(min(h, 7) if h else 7)


def main():
    parser = argparse.ArgumentParser(description="Query bus predictions")
    parser.add_argument("bus", nargs="?", default=None, help="Bus plate to query")
    parser.add_argument("--alerts", action="store_true", help="Show only alerts")
    parser.add_argument("--horizon", type=int, default=None, help="Filter by horizon (3, 5, 7)")
    parser.add_argument("--top", type=int, default=None, help="Show top N buses by max risk")
    parser.add_argument("--label", default=DEFAULT_LABEL, help=f"Model label (default: {DEFAULT_LABEL})")

    args = parser.parse_args()
    predictions = load_predictions(label=args.label)

    if args.bus:
        query_bus(predictions, args.bus, only_alerts=args.alerts, horizon=args.horizon)
    else:
        list_buses(predictions, top=args.top)


if __name__ == "__main__":
    main()
