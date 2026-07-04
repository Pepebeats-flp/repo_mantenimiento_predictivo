#!/usr/bin/env python3
"""Shadow Mode Evaluation: compare predictions vs actual events for Pilot 1.

Usage:
    python scripts/evaluate_shadow.py

Generates:
    outputs/piloto1_report.json  — full evaluation report
    outputs/shadow/              — per-run shadow evaluation snapshots
"""
from __future__ import annotations

import json
import sys
import warnings
from datetime import datetime
from pathlib import Path

import pandas as pd

warnings.filterwarnings("ignore")

sys.path.append(str(Path(__file__).resolve().parent.parent))

from src.evaluation import save_metrics, shadow_evaluate
from src.preprocessing import is_failure_event

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CONFIG_PATH = PROJECT_ROOT / "config" / "piloto1.json"
DATA_PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
PREDICTIONS_DIR = PROJECT_ROOT / "data" / "predictions"
OUTPUTS_DIR = PROJECT_ROOT / "outputs"
SHADOW_DIR = OUTPUTS_DIR / "shadow"
REPORT_PATH = OUTPUTS_DIR / "piloto1_report.json"

BUSES_PILOTO = ["FLXS22", "FLXS23", "LWTK42"]
HORIZONS = [7, 5, 3]
THRESHOLDS = (0.3, 0.4, 0.5, 0.6, 0.7)
EXCLUDE_LOW = True


def load_config() -> dict:
    if CONFIG_PATH.exists():
        with open(CONFIG_PATH) as f:
            return json.load(f)
    return {}


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Shadow Mode Evaluation for Piloto 1")
    parser.add_argument("predictions_path", nargs="?", type=str, default=None,
                        help="Override path to predictions parquet")
    parser.add_argument("--save", type=str, default=None,
                        help="Override output report path")
    parser.add_argument("--skip-ranking", action="store_true",
                        help="Skip per-bus ranking (faster for experiments)")
    args = parser.parse_args()

    config = load_config()
    buses_piloto = config.get("buses_piloto", {}).keys() or BUSES_PILOTO
    buses_piloto = list(buses_piloto)

    print("=" * 60)
    print("SHADOW MODE EVALUATION — Piloto 1")
    print("=" * 60)

    # Load predictions
    if args.predictions_path:
        pred_path = Path(args.predictions_path)
    else:
        pred_path = PREDICTIONS_DIR / "predictions_voy_redbus.parquet"
        if not pred_path.exists():
            pred_path = PREDICTIONS_DIR / "predictions.parquet"
        if not pred_path.exists():
            pred_path = PREDICTIONS_DIR / "predictions_daily.parquet"

    report_path = Path(args.save) if args.save else REPORT_PATH

    print(f"\nLoading predictions from: {pred_path}")
    predictions = pd.read_parquet(pred_path)
    date_col = "fecha_prediccion" if "fecha_prediccion" in predictions.columns else "fecha_evento"
    print(f"  Format: {'bus-day (fecha_prediccion)' if date_col == 'fecha_prediccion' else 'event (fecha_evento)'}")
    print(f"  Predictions: {len(predictions)}")
    print(f"  Buses: {predictions['placa_patente'].nunique()}")
    print(f"  Horizons: {sorted(predictions['horizon_days'].unique())}")
    print(f"  Date range: {predictions[date_col].min()} -> {predictions[date_col].max()}")

    # Load actual failure events from base data (all tipos, filtered by is_failure_event)
    base_path = DATA_PROCESSED_DIR / "base.parquet"
    print(f"\nLoading actual events from: {base_path}")
    base_df = pd.read_parquet(base_path)
    # Use ALL tipos, mark failures
    base_df["es_falla"] = base_df.apply(is_failure_event, axis=1)
    failure_events = base_df[base_df["es_falla"]].copy()
    print(f"  Failure events (all tipos): {len(failure_events)}")
    print(f"  Breakdown: {failure_events['tipo_servicio'].value_counts().to_dict()}")
    print(f"  Buses: {failure_events['placa_patente'].nunique()}")
    print(f"  Date range: {failure_events['fecha_evento'].min()} \u2192 {failure_events['fecha_evento'].max()}")

    print(f"\nPilot buses: {buses_piloto}")
    for bus in buses_piloto:
        bus_preds = predictions[predictions["placa_patente"] == bus]
        bus_events = failure_events[failure_events["placa_patente"] == bus]
        print(f"  {bus}: {len(bus_preds)} predictions, {len(bus_events)} actual events")

    # Run shadow evaluation
    print(f"\n{'='*60}")
    print("Running shadow evaluation...")
    print(f"  Exclude LOW severity: {EXCLUDE_LOW}")
    print(f"  Thresholds: {THRESHOLDS}")
    print(f"{'='*60}")

    resultados = shadow_evaluate(
        predictions=predictions,
        eventos_df=failure_events,
        buses_piloto=buses_piloto,
        thresholds=THRESHOLDS,
        exclude_low_severity=EXCLUDE_LOW,
    )

    # ── Ranking por bus (toda la flota) ─────────────────────────────
    if not args.skip_ranking:
        print(f"\n{'='*60}")
        print("Computing per-bus ranking for all buses...")
        print(f"{'='*60}")
        all_buses = sorted(predictions["placa_patente"].dropna().unique())
        print(f"  Total buses: {len(all_buses)}")

        ranking_raw = shadow_evaluate(
            predictions=predictions,
            eventos_df=failure_events,
            buses_piloto=all_buses,
            thresholds=(0.5,),
            exclude_low_severity=EXCLUDE_LOW,
        )

        ranking_flat: list[dict[str, Any]] = []
        for vkey, buses_data in ranking_raw.get("por_bus", {}).items():
            for bus, bm in buses_data.items():
                if "error" in bm:
                    continue
                tm = bm.get("threshold_metrics", {}).get("0.5", {})
                cr = tm.get("classification_report", {}).get("1", {})
                ranking_flat.append({
                    "bus": bus,
                    "horizonte_dias": int(vkey),
                    "n_predicciones": bm.get("total_predicciones", 0),
                    "n_positivos_reales": bm.get("total_positivos_reales", 0),
                    "accuracy": round(tm.get("accuracy", 0), 4),
                    "precision": round(cr.get("precision", 0), 4),
                    "recall": round(cr.get("recall", 0), 4),
                    "f1": round(cr.get("f1-score", 0), 4),
                    "specificity": round(tm.get("specificity", 0), 4),
                })

        resultados["ranking_por_bus"] = ranking_flat

        # Print top/bottom
        if ranking_flat:
            rank_df = pd.DataFrame(ranking_flat)
            for h in HORIZONS:
                h_df = rank_df[rank_df["horizonte_dias"] == h].sort_values("f1", ascending=False)
                print(f"\n  Top 5 buses por F1 ({h}d):")
                for _, r in h_df.head(5).iterrows():
                    print(f"    {r['bus']:<12} F1={r['f1']:.4f}  Acc={r['accuracy']:.4f}  n={int(r['n_predicciones'])}")
                print(f"  Bottom 5 buses por F1 ({h}d):")
                for _, r in h_df.tail(5).iterrows():
                    print(f"    {r['bus']:<12} F1={r['f1']:.4f}  Acc={r['accuracy']:.4f}  n={int(r['n_predicciones'])}")

    # Tipo breakdown from failure events
    tipo_breakdown = failure_events["tipo_servicio"].value_counts().to_dict()

    resultados["metadata"] = {
        "fecha_evaluacion": datetime.now().isoformat(),
        "modelo": "xgb_{3,5,7}d_voy_redbus",
        "total_predicciones": int(len(predictions)),
        "total_buses": int(predictions["placa_patente"].nunique()),
        "buses_piloto": buses_piloto,
        "exclude_low_severity": EXCLUDE_LOW,
        "tipo_breakdown": {str(k): int(v) for k, v in tipo_breakdown.items()},
        "total_failure_events": int(len(failure_events)),
    }

    save_metrics(resultados, report_path)

    # Save per-run shadow snapshot
    SHADOW_DIR.mkdir(parents=True, exist_ok=True)
    snap_path = SHADOW_DIR / f"shadow_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    save_metrics(resultados, snap_path)

    # Print summary
    print(f"\n{'='*60}")
    print("RESULTS — Piloto 1 Shadow Mode")
    print(f"{'='*60}")

    criterio = config.get("evaluacion", {}).get("criterio_exito_accuracy", 0.75)

    for ventana in HORIZONS:
        vkey = str(ventana)
        if vkey not in resultados.get("por_horizonte", {}):
            continue
        m = resultados["por_horizonte"][vkey]
        th_metrics = m.get("threshold_metrics", {}).get("0.5", {})
        acc = th_metrics.get("accuracy", 0)
        precision = th_metrics.get("classification_report", {}).get("1", {}).get("precision", 0)
        recall = th_metrics.get("classification_report", {}).get("1", {}).get("recall", 0)
        f1 = th_metrics.get("classification_report", {}).get("1", {}).get("f1-score", 0)
        auc_roc = m.get("auc_roc", "N/A")
        total = m.get("total_predicciones", 0)
        pos_rate = m.get("tasa_positivos_reales", 0)

        cumple = "✅ CUMPLE" if acc >= criterio else "❌ NO CUMPLE"
        print(f"\n  Horizonte {ventana}d:")
        print(f"    Total predicciones: {total}")
        print(f"    Tasa positivos reales: {pos_rate*100:.1f}%")
        print(f"    Accuracy (th=0.5): {acc:.4f}  {cumple} (target ≥ {criterio})")
        print(f"    Precision: {precision:.4f}  Recall: {recall:.4f}  F1: {f1:.4f}")
        print(f"    AUC-ROC: {auc_roc}")

    # Per-bus summary
    print(f"\n  {'='*40}")
    print("  Per-bus results (threshold=0.5)")
    print(f"  {'='*40}")
    for ventana in HORIZONS:
        vkey = str(ventana)
        buses_data = resultados.get("por_bus", {}).get(vkey, {})
        if not buses_data:
            continue
        print(f"\n  --- {ventana}d ---")
        for bus, bm in buses_data.items():
            if "error" in bm:
                print(f"    {bus}: {bm['error']} ({bm.get('total', 0)} events)")
                continue
            acc = bm.get("threshold_metrics", {}).get("0.5", {}).get("accuracy", 0)
            p = bm.get("threshold_metrics", {}).get("0.5", {}).get("classification_report", {}).get("1", {}).get("precision", 0)
            r = bm.get("threshold_metrics", {}).get("0.5", {}).get("classification_report", {}).get("1", {}).get("recall", 0)
            n = bm.get("total_predicciones", 0)
            print(f"    {bus}: n={n} acc={acc:.4f} p={p:.4f} r={r:.4f}")

    print(f"\n{'='*60}")
    print(f"Report saved: {report_path}")
    if snap_path:
        print(f"Shadow snapshot: {snap_path}")
    print(f"{'='*60}")
    return resultados


if __name__ == "__main__":
    main()
