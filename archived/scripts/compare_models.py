#!/usr/bin/env python3
"""Comparativa de modelos: XGBoost vs LightGBM vs MLP (Red Neuronal).

Uso:
    python scripts/compare_models.py

Genera:
    outputs/model_comparison.json  — métricas detalladas por modelo y ventana
    outputs/model_comparison.csv   — tabla resumen para dashboard
"""
from __future__ import annotations

import json
import sys
import warnings
from pathlib import Path

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

sys.path.append(str(Path(__file__).resolve().parent.parent))

from src.feature_engineering import get_feature_columns
from src.models_comparison import compare_all_models

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
OUTPUTS_DIR = PROJECT_ROOT / "outputs"

CUTOFF_DATE = pd.Timestamp("2025-11-15")
HORIZONS = [7, 5, 3]


def main():
    print("=" * 60)
    print("COMPARATIVA DE MODELOS — XGBoost vs HistGB vs MLP")
    print("=" * 60)

    # ── Load features ───────────────────────────────────────────────
    features_path = DATA_PROCESSED_DIR / "features_train.parquet"
    print(f"\nLoading features: {features_path}")
    features_df = pd.read_parquet(features_path)
    print(f"  Shape: {features_df.shape}")
    print(f"  Date range: {features_df['fecha_evento'].min()} → {features_df['fecha_evento'].max()}")

    feature_columns = get_feature_columns(features_df)
    print(f"  Features: {len(feature_columns)}")

    # ── Temporal train/test split (same as pipeline) ────────────────
    train_df = features_df[features_df["fecha_evento"] < CUTOFF_DATE].copy()
    test_df = features_df[features_df["fecha_evento"] >= CUTOFF_DATE].copy()
    print(f"\n  Train: {len(train_df)} ({train_df['fecha_evento'].min()} → {train_df['fecha_evento'].max()})")
    print(f"  Test:  {len(test_df)} ({test_df['fecha_evento'].min()} → {test_df['fecha_evento'].max()})")

    X_train = train_df[feature_columns].fillna(0)
    X_test = test_df[feature_columns].fillna(0)

    # ── Train per horizon ───────────────────────────────────────────
    all_results: dict[str, Any] = {
        "config": {
            "cutoff_date": str(CUTOFF_DATE),
            "n_features": len(feature_columns),
            "n_train": len(train_df),
            "n_test": len(test_df),
            "horizons": HORIZONS,
        },
        "por_horizonte": {},
        "resumen_global": {},
    }

    for window in HORIZONS:
        target_col = f"correctivo_prox_{window}d"
        y_train = train_df[target_col].astype(int)
        y_test = test_df[target_col].astype(int)

        print(f"\n{'='*60}")
        print(f"HORIZONTE {window}d — Positivos train: {y_train.sum()}/{len(y_train)} "
              f"({y_train.mean()*100:.1f}%)")
        print(f"{'='*60}")

        results = compare_all_models(X_train, y_train, X_test, y_test, horizons=[window])

        hkey = str(window)
        all_results["por_horizonte"][hkey] = results["models"][hkey]
        if results["comparison"].get(hkey):
            all_results["resumen_global"][hkey] = results["comparison"][hkey]

    # ── Build global summary ────────────────────────────────────────
    summary_rows = []
    for hkey in sorted(all_results["por_horizonte"].keys(), key=int):
        for model_name in ["xgboost", "histgb", "mlp"]:
            m = all_results["por_horizonte"][hkey][model_name]
            metrics = m["metrics"]
            summary_rows.append({
                "horizonte": f"{hkey}d",
                "modelo": model_name.upper(),
                "accuracy": metrics["accuracy"],
                "precision": metrics["precision"],
                "recall": metrics["recall"],
                "f1": metrics["f1"],
                "specificity": metrics["specificity"],
                "auc_roc": metrics["auc_roc"] if metrics["auc_roc"] else 0,
                "train_time_sec": m["train_time_sec"],
                "n_test": metrics["n_total"],
            })

    summary_df = pd.DataFrame(summary_rows)
    csv_path = OUTPUTS_DIR / "model_comparison.csv"
    summary_df.to_csv(csv_path, index=False)
    print(f"\n  Summary saved: {csv_path}")

    # ── Print comparison table ──────────────────────────────────────
    print(f"\n{'='*60}")
    print("RESUMEN COMPARATIVO")
    print(f"{'='*60}")
    for hkey in sorted(all_results["por_horizonte"].keys(), key=int):
        print(f"\n  Horizonte {hkey}d:")
        print(f"  {'Modelo':<12} {'Acc':>8} {'F1':>8} {'Prec':>8} {'Rec':>8} {'AUC':>8} {'Tiempo':>8}")
        print(f"  {'-'*56}")
        for _, r in summary_df[summary_df["horizonte"] == f"{hkey}d"].iterrows():
            print(f"  {r['modelo']:<12} {r['accuracy']:>8.4f} {r['f1']:>8.4f} "
                  f"{r['precision']:>8.4f} {r['recall']:>8.4f} {r['auc_roc']:>8.4f} "
                  f"{r['train_time_sec']:>7.1f}s")

        best = all_results["resumen_global"].get(hkey, {}).get("best_by_metric", {})
        if best:
            print(f"\n  🏆 Mejor modelo por métrica ({hkey}d):")
            print(f"     Accuracy: {best.get('accuracy', 'N/A')}")
            print(f"     F1:       {best.get('f1', 'N/A')}")
            print(f"     AUC-ROC:  {best.get('auc_roc', 'N/A')}")

    # ── Save full results ──────────────────────────────────────────
    # Remove model objects for JSON serialization
    serializable = _make_serializable(all_results)

    json_path = OUTPUTS_DIR / "model_comparison.json"
    with open(json_path, "w") as f:
        json.dump(serializable, f, indent=2)
    print(f"\n  Full results saved: {json_path}")
    print(f"\n{'='*60}")
    print("COMPARATIVA COMPLETA")
    print(f"{'='*60}")

    return serializable


def _make_serializable(obj):
    """Recursively remove non-serializable objects (model, scaler, etc.)."""
    if isinstance(obj, dict):
        return {k: _make_serializable(v) for k, v in obj.items()
                if k not in ("model", "scaler", "loss_curve", "y_score", "y_true")}
    elif isinstance(obj, (list, tuple)):
        return [_make_serializable(item) for item in obj]
    elif isinstance(obj, (np.integer,)):
        return int(obj)
    elif isinstance(obj, (np.floating,)):
        return float(obj)
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    return obj


if __name__ == "__main__":
    from typing import Any
    main()
