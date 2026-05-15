#!/usr/bin/env python3
"""Validate best model config via shadow evaluation on full historical data.

Trains on ALL data (no test split), generates predictions, runs shadow_evaluate.
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

from src.evaluation import shadow_evaluate, save_metrics
from src.feature_engineering import get_feature_columns
from src.preprocessing import is_failure_event
from src.models import XGBoostModel, CatBoostModel

PROJECT_ROOT = Path(__file__).resolve().parent.parent

HORIZONS = [7, 5, 3]
PILOT_BUSES = ["FLXS22", "FLXS23", "LWTK42"]

# Best configs found
DROP_FEATURES = [
    "uuid_gestion_count_evento", "uuid_gestion_unique_count_evento",
    "km_desviacion_relativa_prom_evento", "tiene_uuid_gestion_evento",
    "num_sistemas_inspeccionados",
    "count_causa_mantenimiento_preventivo_ult_3d",
    "count_causa_mantenimiento_preventivo_ult_5d",
    "count_causa_mantenimiento_preventivo_ult_7d",
    "count_unidad_19_ult_7d", "count_unidad_19_ult_30d",
    "count_unidad_15_ult_7d", "count_unidad_15_ult_30d",
    "uuid_gestion_count_ult_7d", "uuid_gestion_count_ult_30d",
]

SPW_BY_HORIZON = {7: 1.0, 5: 2.0, 3: 4.0}


def compute_spw(y_train: pd.Series, multiplier: float) -> float:
    pos = int(y_train.sum())
    neg = int((1 - y_train).sum())
    return (neg / pos * multiplier) if pos else 1.0


def classify_severity(row: dict) -> str:
    hp = float(row.get("repuestos_count_evento", 0) or 0)
    dur = float(row.get("duracion_ot_horas_prom_evento", 0) or 0)
    kw = float(row.get("num_keywords_tecnicos_evento", 0) or 0)
    highs = float(row.get("inspeccion_total_highs_evento", 0) or 0)
    if not hp and dur < 2 and kw == 0 and highs == 0:
        return "LOW"
    elif hp and dur > 4:
        return "HIGH"
    elif highs > 0:
        return "HIGH"
    return "MEDIUM"


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--model", choices=["xgb", "catboost"], default="catboost")
    parser.add_argument("--drop-features", action="store_true", default=True)
    parser.add_argument("--spw-per-horizon", action="store_true", default=True)
    parser.add_argument("--output", type=str, default="outputs/piloto1_report_best.json")
    args = parser.parse_args()

    # ── Load features ─────────────────────────────────────────────
    print("=" * 60)
    print("LOADING FEATURES")
    print("=" * 60)
    features = pd.read_parquet(PROJECT_ROOT / "data" / "processed" / "features_train.parquet")
    features = features.sort_values(["placa_patente", "fecha_evento"]).reset_index(drop=True)
    print(f"  Total events: {len(features)}")
    print(f"  Buses: {features['placa_patente'].nunique()}")

    feat_cols = get_feature_columns(features)
    if args.drop_features:
        feat_cols = [c for c in feat_cols if c not in DROP_FEATURES]
    print(f"  Features: {len(feat_cols)} ({'dropped 14' if args.drop_features else 'all 173'})")

    # ── Train models for each horizon ─────────────────────────────
    print(f"\n{'='*60}")
    print(f"TRAINING {args.model.upper()} — one model per horizon")
    print(f"{'='*60}")

    models = {}
    all_preds = []

    for window in HORIZONS:
        target_col = f"correctivo_prox_{window}d"
        X = features[feat_cols].fillna(0)
        y = features[target_col].astype(int)

        spw_mult = SPW_BY_HORIZON.get(window, 3.0) if args.spw_per_horizon else 3.0
        spw = compute_spw(y, spw_mult)

        print(f"\n  --- {window}d: n={len(X)} pos_rate={y.mean()*100:.1f}% spw={spw:.2f} ---")

        if args.model == "xgb":
            m = XGBoostModel(
                n_estimators=800, max_depth=12, learning_rate=0.03,
                subsample=0.85, colsample_bytree=0.85, min_child_weight=2,
                scale_pos_weight=spw,
            )
        else:
            m = CatBoostModel(
                iterations=800, max_depth=8, learning_rate=0.03,
                subsample=0.85, scale_pos_weight=spw,
            )
        m.fit(X, y)
        models[window] = m

        # Predict
        y_prob = m.predict_proba(X)
        threshold = 0.5
        y_pred = (y_prob >= threshold).astype(int)

        severity_cols = [
            "repuestos_count_evento", "duracion_ot_horas_prom_evento",
            "num_keywords_tecnicos_evento", "inspeccion_total_highs_evento",
        ]
        for c in severity_cols:
            if c not in features.columns:
                features[c] = 0

        preds = pd.DataFrame({
            "placa_patente": features["placa_patente"],
            "fecha_evento": features["fecha_evento"],
            "horizon_days": window,
            "probability": y_prob,
            "alert": y_pred.astype(bool),
            "severity": features[severity_cols].apply(classify_severity, axis=1),
        })
        all_preds.append(preds)
        alerts = y_pred.sum()
        print(f"    Alerts: {alerts}/{len(y_pred)} ({alerts/len(y_pred)*100:.1f}%)")

    predictions = pd.concat(all_preds, ignore_index=True)
    pred_path = PROJECT_ROOT / "data" / "predictions" / "predictions_best_config.parquet"
    predictions.to_parquet(pred_path, index=False)
    print(f"\n  Predictions saved: {pred_path}")

    # ── Shadow evaluation ─────────────────────────────────────────
    print(f"\n{'='*60}")
    print("SHADOW EVALUATION")
    print(f"{'='*60}")

    base_df = pd.read_parquet(PROJECT_ROOT / "data" / "processed" / "base.parquet")
    base_df["es_falla"] = base_df.apply(is_failure_event, axis=1)
    failure_events = base_df[base_df["es_falla"]].copy()
    print(f"  Failure events: {len(failure_events)}")

    thresholds = (0.3, 0.4, 0.5, 0.6, 0.7)

    resultados = shadow_evaluate(
        predictions=predictions,
        eventos_df=failure_events,
        buses_piloto=PILOT_BUSES,
        thresholds=thresholds,
        exclude_low_severity=True,
    )

    output_path = Path(args.output)
    save_metrics(resultados, output_path)

    # ── Print summary ─────────────────────────────────────────────
    print(f"\n{'='*60}")
    print("RESULTS — Shadow Evaluation")
    print(f"{'='*60}")

    for ventana in HORIZONS:
        vkey = str(ventana)
        if vkey not in resultados.get("por_horizonte", {}):
            continue
        m = resultados["por_horizonte"][vkey]
        tm = m.get("threshold_metrics", {}).get("0.5", {})
        acc = tm.get("accuracy", 0)
        cr = tm.get("classification_report", {}).get("1", {})
        print(f"\n  --- {ventana}d Global ---")
        print(f"    Acc={acc:.4f}  P={cr.get('precision',0):.4f}  R={cr.get('recall',0):.4f}  "
              f"F1={cr.get('f1-score',0):.4f}")
        print(f"    AUC-ROC={m.get('auc_roc', 'N/A')}  Total={m.get('total_predicciones',0)}")

        buses_data = resultados.get("por_bus", {}).get(vkey, {})
        if buses_data:
            print(f"  --- {ventana}d Pilot Buses ---")
            for bus, bm in buses_data.items():
                if "error" in bm:
                    print(f"    {bus}: {bm['error']}")
                    continue
                tm2 = bm.get("threshold_metrics", {}).get("0.5", {})
                cr2 = tm2.get("classification_report", {}).get("1", {})
                acc2 = tm2.get("accuracy", 0)
                cm2 = tm2.get("confusion_matrix", [])
                cm_str = f"TN={cm2[0][0]} FP={cm2[0][1]} FN={cm2[1][0]} TP={cm2[1][1]}" if cm2 else ""
                print(f"    {bus}: Acc={acc2:.4f}  P={cr2.get('precision',0):.4f}  "
                      f"R={cr2.get('recall',0):.4f}  n={bm.get('total_predicciones',0)}  {cm_str}")

    # ── Compare with current best ─────────────────────────────────
    print(f"\n{'='*60}")
    print("COMPARISON WITH CURRENT BEST (005_deeper_spw3)")
    print(f"{'='*60}")
    current = {
        "7d": {"FLXS22": 0.7103, "FLXS23": 0.6518, "LWTK42": 0.6703},
        "5d": {"FLXS22": 0.7850, "FLXS23": 0.7232, "LWTK42": 0.7802},
        "3d": {"FLXS22": 0.8318, "FLXS23": 0.8036, "LWTK42": 0.8681},
    }
    print(f"  {'Bus':<10} {'H':>3}  {'Current':>8}  {'New':>8}  {'Δ':>8}")
    print(f"  {'-'*10} {'-'*3}  {'-'*8}  {'-'*8}  {'-'*8}")
    for h in ["3", "5", "7"]:
        for bus in PILOT_BUSES:
            old = current.get(h, {}).get(bus, 0)
            new = 0
            buses_data = resultados.get("por_bus", {}).get(h, {})
            if bus in buses_data and "error" not in buses_data[bus]:
                new = buses_data[bus].get("threshold_metrics", {}).get("0.5", {}).get("accuracy", 0)
            delta = new - old
            sign = "+" if delta > 0 else ""
            print(f"  {bus:<10} {h:>3}  {old:.4f}  {new:.4f}  {sign}{delta:.4f}")

    print(f"\n  Report saved: {output_path}")


if __name__ == "__main__":
    main()
