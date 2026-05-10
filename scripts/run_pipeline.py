#!/usr/bin/env python3
"""Complete pipeline runner: ETL (VOY+REDBUS) → training → inference → evaluation + test-bus holdout."""
from __future__ import annotations

import json
import pickle
import sys
import warnings
from pathlib import Path

import numpy as np
import pandas as pd
import xgboost as xgb

warnings.filterwarnings("ignore")

sys.path.append(str(Path(__file__).resolve().parent.parent))

from src.data_loader import load_json_files
from src.preprocessing import (
    clean_data,
    create_base_dataframe,
    create_eventos_dataframe,
    extract_additional_fields,
    merge_additional_event_fields,
)
from src.feature_engineering import (
    DEFAULT_FEATURE_COLUMNS,
    create_future_targets,
    create_temporal_features,
    generate_cause_based_features,
    generate_inventory_features,
    generate_rolling_features,
    generate_system_features,
    generate_text_pattern_features,
    get_feature_columns,
    summarize_feature_quality,
)
from src.evaluation import evaluate_model, save_metrics

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_RAW_DIR = PROJECT_ROOT / "data" / "raw"
DATA_PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
MODELS_DIR = PROJECT_ROOT / "models"
METRICS_DIR = PROJECT_ROOT / "outputs" / "metrics"
PREDICTIONS_DIR = PROJECT_ROOT / "data" / "predictions"

HORIZONS = [7, 5, 3]
CUTOFF_DATE = pd.Timestamp("2025-11-15")
THRESHOLD = 0.5

# Test buses to hold out for generalization eval
TEST_BUSES = [
    "PFVL15", "PFVL21", "PFVK90", "PFTF88", "PDZH97",
    "PFYH17", "PFVK64", "PFYR84", "PFYG94", "PFTF84",
]

OPTIMIZED_PARAMS = {
    "n_estimators": 800,
    "max_depth": 8,
    "learning_rate": 0.03,
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


def step01_load_and_clean():
    print("=" * 60)
    print("STEP 01: Load and clean data (VOY + REDBUS)")
    print("=" * 60)

    # VOY
    voy_prev, voy_corr = load_json_files(
        DATA_RAW_DIR / "preventivos.json",
        DATA_RAW_DIR / "correctivos.json",
    )
    voy_df = clean_data(voy_prev, voy_corr, empresa_id="VOY")
    voy_df = extract_additional_fields(voy_df)
    print(f"  VOY: {len(voy_df)} registros ({voy_df['fecha_evento'].min()} \u2192 {voy_df['fecha_evento'].max()})")

    # REDBUS
    rbus_prev, rbus_corr = load_json_files(
        DATA_RAW_DIR / "redbus_preventivos.json",
        DATA_RAW_DIR / "redbus_correctivos.json",
    )
    rbus_df = clean_data(rbus_prev, rbus_corr, empresa_id="REDBUS")
    rbus_df = extract_additional_fields(rbus_df)
    print(f"  REDBUS: {len(rbus_df)} registros ({rbus_df['fecha_evento'].min()} \u2192 {rbus_df['fecha_evento'].max()})")

    clean_df = pd.concat([voy_df, rbus_df], ignore_index=True, sort=False)
    base_df = create_base_dataframe(clean_df, executed_only=True)
    print(f"\n  Combined shape: {base_df.shape}")
    print(f"  Buses: {base_df['placa_patente'].nunique()}")
    print(f"  Date range: {base_df['fecha_evento'].min()} \u2192 {base_df['fecha_evento'].max()}")
    print(f"  Empresas: {base_df['empresa_id'].value_counts().to_dict()}")

    # Separate test buses
    base_test = base_df[base_df["placa_patente"].isin(TEST_BUSES)].copy()
    base_train = base_df[~base_df["placa_patente"].isin(TEST_BUSES)].copy()
    print(f"\n  Train buses: {base_train['placa_patente'].nunique()}")
    print(f"  Test holdout buses: {base_test['placa_patente'].nunique()} ({', '.join(TEST_BUSES)})")

    path = DATA_PROCESSED_DIR / "base.parquet"
    base_df.to_parquet(path, index=False)
    path_train = DATA_PROCESSED_DIR / "base_train.parquet"
    base_train.to_parquet(path_train, index=False)
    path_test = DATA_PROCESSED_DIR / "base_test.parquet"
    base_test.to_parquet(path_test, index=False)
    print(f"  Saved train base: {path_train}")
    print(f"  Saved test base:  {path_test}")
    return base_df, base_train, base_test


def step02_create_events(base_df: pd.DataFrame, label: str = "all"):
    print("\n" + "=" * 60)
    print(f"STEP 02: Create technical events ({label})")
    print("=" * 60)
    eventos_df = create_eventos_dataframe(base_df)
    eventos_df = merge_additional_event_fields(base_df, eventos_df)
    print(f"  Events: {len(eventos_df)}")
    print(f"  Columns: {eventos_df.shape[1]}")
    print(f"  Buses: {eventos_df['placa_patente'].nunique()}")

    path = DATA_PROCESSED_DIR / f"eventos_{label}.parquet"
    eventos_df.to_parquet(path, index=False)
    print(f"  Saved: {path}")
    return eventos_df


def step03_feature_engineering(eventos_df: pd.DataFrame, label: str = "all"):
    print("\n" + "=" * 60)
    print(f"STEP 03: Feature engineering ({label})")
    print("=" * 60)
    features_df = generate_rolling_features(eventos_df)
    features_df = generate_cause_based_features(features_df)
    features_df = generate_system_features(features_df)
    features_df = generate_inventory_features(features_df)
    features_df = generate_text_pattern_features(features_df)
    features_df = create_temporal_features(features_df)
    features_df = create_future_targets(features_df, windows=(7, 5, 3, 10, 14, 30))
    quality_df = summarize_feature_quality(features_df, target_windows=(7, 5, 3))
    print(f"  Features shape: {features_df.shape}")

    for w in HORIZONS:
        col = f"correctivo_prox_{w}d"
        counts = features_df[col].value_counts()
        print(f"  {col}: {counts.to_dict()}")

    print(f"\n  Quality:\n{quality_df.to_string()}")

    path = DATA_PROCESSED_DIR / f"features_{label}.parquet"
    features_df.to_parquet(path, index=False)
    print(f"  Saved: {path}")
    return features_df


def get_available_features(features_df: pd.DataFrame) -> list[str]:
    return get_feature_columns(features_df)


def step04_train_models(features_df: pd.DataFrame, label: str = "voy_redbus"):
    print("\n" + "=" * 60)
    print(f"STEP 04: Train XGBoost models ({label})")
    print("=" * 60)

    feature_columns = get_available_features(features_df)
    print(f"  Using {len(feature_columns)} features")

    train_df = features_df[features_df["fecha_evento"] < CUTOFF_DATE].copy()
    test_df = features_df[features_df["fecha_evento"] >= CUTOFF_DATE].copy()
    print(f"  Train: {len(train_df)} ({train_df['fecha_evento'].min()} \u2192 {train_df['fecha_evento'].max()})")
    print(f"  Test:  {len(test_df)} ({test_df['fecha_evento'].min()} \u2192 {test_df['fecha_evento'].max()})")

    results = {}
    for window in HORIZONS:
        target_col = f"correctivo_prox_{window}d"
        X_train = train_df[feature_columns].fillna(0)
        y_train = train_df[target_col].astype(int)
        X_test = test_df[feature_columns].fillna(0)
        y_test = test_df[target_col].astype(int)

        pos_count = int(y_train.sum())
        neg_count = int((1 - y_train).sum())
        scale_pos_weight = neg_count / pos_count if pos_count else 1.0
        base_pos_rate = y_train.mean()
        print(f"\n  {window}d: train positives={pos_count}/{len(y_train)} ({base_pos_rate*100:.1f}%), "
              f"scale_pos_weight={scale_pos_weight:.2f}")

        params = dict(OPTIMIZED_PARAMS)
        params["scale_pos_weight"] = scale_pos_weight
        eval_set = [(X_train, y_train), (X_test, y_test)]

        model = xgb.XGBClassifier(**params)
        model.fit(X_train, y_train, eval_set=eval_set, verbose=False)

        # Restore best iteration from early stopping
        if hasattr(model, "best_iteration"):
            model.n_estimators = model.best_iteration + 1

        y_score = model.predict_proba(X_test)[:, 1]

        model_path = MODELS_DIR / f"xgb_{window}d_{label}.pkl"
        with open(model_path, "wb") as f:
            pickle.dump(model, f)

        # Evaluate with extended metrics
        metrics = evaluate_model(y_test, y_score, thresholds=(0.3, 0.4, 0.5, 0.6, 0.7))
        metrics["window_days"] = window
        metrics["feature_columns"] = feature_columns
        metrics["feature_count"] = len(feature_columns)
        metrics["scale_pos_weight"] = scale_pos_weight
        metrics["cutoff_date"] = str(CUTOFF_DATE)
        metrics["train_size"] = len(X_train)
        metrics["test_size"] = len(X_test)
        metrics["base_pos_rate"] = round(base_pos_rate, 4)

        metrics_path = METRICS_DIR / f"xgb_{window}d_{label}.json"
        save_metrics(metrics, metrics_path)

        # Save meta
        meta = {
            "feature_names": feature_columns,
            "feature_count": len(feature_columns),
            "best_config": "OPTIMIZED_VOY_REDBUS",
            "model_params": {k: str(v) if not isinstance(v, (int, float, bool)) else v
                             for k, v in params.items()},
            "scale_pos_weight": scale_pos_weight,
            "cutoff_date": str(CUTOFF_DATE),
            "best_iteration": getattr(model, "best_iteration", None),
        }
        meta_path = MODELS_DIR / f"xgb_{window}d_{label}_meta.json"
        with open(meta_path, "w") as f:
            json.dump(meta, f, indent=2)

        # Feature importances
        imp = sorted(zip(feature_columns, model.feature_importances_), key=lambda x: -x[1])
        imp_path = METRICS_DIR / f"feature_importance_{window}d_{label}.csv"
        imp_df = pd.DataFrame(imp, columns=["feature", "importance"])
        imp_df.to_csv(imp_path, index=False)

        r = metrics["threshold_metrics"]["0.5"]["classification_report"]
        cm = metrics["threshold_metrics"]["0.5"]["confusion_matrix"]
        tn, fp, fn, tp = cm[0][0], cm[0][1], cm[1][0], cm[1][1]
        acc = (tp + tn) / (tp + tn + fp + fn)
        spec = tn / (tn + fp) if (tn + fp) > 0 else 0.0
        baseline = max(y_test.mean(), 1 - y_test.mean())

        results[str(window)] = {
            "accuracy": round(acc, 4),
            "precision": round(r["1"]["precision"], 4),
            "recall": round(r["1"]["recall"], 4),
            "f1": round(r["1"]["f1-score"], 4),
            "specificity": round(spec, 4),
            "confusion_matrix": cm,
            "feature_count": len(feature_columns),
            "auc_roc": metrics.get("auc_roc"),
            "auc_pr": metrics["precision_recall"]["auc_pr"],
            "brier_score": metrics.get("brier_score"),
            "best_f1": metrics.get("best_f1"),
            "best_f1_threshold": metrics.get("best_f1_threshold"),
            "baseline_always_majority": baseline,
            "lift_top_10pct": metrics.get("lift_top_10pct"),
            "test_positives": int(y_test.sum()),
            "test_negatives": int((1 - y_test).sum()),
            "best_iteration": getattr(model, "best_iteration", None),
        }
        print(f"  {window}d: Acc={acc:.4f} (baseline={baseline:.4f}) P={r['1']['precision']:.4f} "
              f"R={r['1']['recall']:.4f} F1={r['1']['f1-score']:.4f} Spec={spec:.4f} "
              f"AUC-ROC={metrics.get('auc_roc', 'N/A')} AUC-PR={metrics['precision_recall']['auc_pr']:.4f}")
        print(f"    Top-3: {imp[0][0]}={imp[0][1]:.4f}, {imp[1][0]}={imp[1][1]:.4f}, {imp[2][0]}={imp[2][1]:.4f}")

    summary_path = METRICS_DIR / f"evaluation_summary_{label}.json"
    with open(summary_path, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\n  Summary saved: {summary_path}")
    return results


def step05_test_bus_evaluation(features_test: pd.DataFrame, label: str = "voy_redbus"):
    print("\n" + "=" * 60)
    print(f"STEP 05: Test-bus holdout evaluation ({label})")
    print("=" * 60)

    feature_columns = get_available_features(features_test)
    print(f"  Using {len(feature_columns)} features")

    all_results = {}
    for window in HORIZONS:
        target_col = f"correctivo_prox_{window}d"
        model_path = MODELS_DIR / f"xgb_{window}d_{label}.pkl"
        if not model_path.exists():
            print(f"  WARNING: Model {model_path} not found, skipping {window}d")
            continue

        with open(model_path, "rb") as f:
            model = pickle.load(f)

        model_features = model.feature_names_in_.tolist() if hasattr(model, "feature_names_in_") else feature_columns
        X_test = pd.DataFrame(index=features_test.index)
        for c in model_features:
            if c in features_test.columns:
                X_test[c] = pd.to_numeric(features_test[c], errors="coerce").fillna(0)
            else:
                X_test[c] = 0.0
        missing_feats = [c for c in model_features if c not in features_test.columns]
        if missing_feats:
            print(f"    {len(missing_feats)} features missing for {window}d (filled with 0)")
        y_test = features_test[target_col].astype(int)

        y_score = model.predict_proba(X_test)[:, 1]
        metrics = evaluate_model(y_test.values, y_score, thresholds=(0.3, 0.4, 0.5, 0.6, 0.7))
        metrics["window_days"] = window
        metrics_path = METRICS_DIR / f"test_buses_{window}d_{label}.json"
        save_metrics(metrics, metrics_path)

        r = metrics["threshold_metrics"]["0.5"]["classification_report"]
        cm = metrics["threshold_metrics"]["0.5"]["confusion_matrix"]
        tn, fp, fn, tp = cm[0][0], cm[0][1], cm[1][0], cm[1][1]
        acc = (tp + tn) / (tp + tn + fp + fn)
        spec = tn / (tn + fp) if (tn + fp) > 0 else 0.0

        all_results[str(window)] = {
            "accuracy": round(acc, 4),
            "precision": round(r["1"]["precision"], 4),
            "recall": round(r["1"]["recall"], 4),
            "f1": round(r["1"]["f1-score"], 4),
            "specificity": round(spec, 4),
            "confusion_matrix": cm,
            "auc_roc": metrics.get("auc_roc"),
            "auc_pr": metrics["precision_recall"]["auc_pr"],
            "brier_score": metrics.get("brier_score"),
            "best_f1": metrics.get("best_f1"),
            "best_f1_threshold": metrics.get("best_f1_threshold"),
            "test_size": len(y_test),
            "test_positives": int(y_test.sum()),
        }
        print(f"  {window}d: Acc={acc:.4f} P={r['1']['precision']:.4f} R={r['1']['recall']:.4f} "
              f"F1={r['1']['f1-score']:.4f} Spec={spec:.4f} "
              f"AUC-ROC={metrics.get('auc_roc', 'N/A')} AUC-PR={metrics['precision_recall']['auc_pr']:.4f}")

        # Per-bus predictions
        pred_df = pd.DataFrame({
            "placa_patente": features_test["placa_patente"],
            "fecha_evento": features_test["fecha_evento"],
            "actual": y_test.values,
            "probability": y_score,
            "predicted": (y_score >= 0.5).astype(int),
        })
        pred_path = METRICS_DIR / f"test_buses_predictions_{window}d_{label}.csv"
        pred_df.to_csv(pred_path, index=False)
        print(f"    Per-bus predictions saved: {pred_path}")

        # By-bus summary
        bus_summary = (
            pred_df.groupby("placa_patente")
            .agg(
                events=("actual", "count"),
                actual_pos=("actual", "sum"),
                predicted_pos=("predicted", "sum"),
                mean_prob=("probability", "mean"),
            )
            .reset_index()
        )
        bus_summary["accuracy"] = (
            (bus_summary["actual_pos"] == bus_summary["predicted_pos"]).astype(int)
        )
        print(f"\n  Per-bus results ({window}d):")
        for _, row in bus_summary.iterrows():
            print(f"    {row['placa_patente']:<12} events={row['events']:>3} "
                  f"actual_pos={int(row['actual_pos']):>3} predicted_pos={int(row['predicted_pos']):>3} "
                  f"mean_prob={row['mean_prob']*100:.1f}%")

    summary_path = METRICS_DIR / f"test_buses_summary_{label}.json"
    with open(summary_path, "w") as f:
        json.dump(all_results, f, indent=2)
    print(f"\n  Test-bus summary saved: {summary_path}")
    return all_results


def step06_inference(features_df: pd.DataFrame, label: str = "voy_redbus"):
    print("\n" + "=" * 60)
    print(f"STEP 06: Batch inference ({label})")
    print("=" * 60)

    all_preds = []
    for window in HORIZONS:
        model_path = MODELS_DIR / f"xgb_{window}d_{label}.pkl"
        meta_path = MODELS_DIR / f"xgb_{window}d_{label}_meta.json"

        if not model_path.exists():
            print(f"  WARNING: Model {model_path} not found, skipping")
            continue

        with open(model_path, "rb") as f:
            model = pickle.load(f)

        if meta_path.exists():
            with open(meta_path) as f:
                meta = json.load(f)
            feature_columns = meta.get("feature_names", DEFAULT_FEATURE_COLUMNS)
        else:
            feature_columns = DEFAULT_FEATURE_COLUMNS

        missing = [c for c in feature_columns if c not in features_df.columns]
        X = pd.DataFrame(index=features_df.index)
        for c in feature_columns:
            if c in features_df.columns:
                X[c] = pd.to_numeric(features_df[c], errors="coerce").fillna(0)
            else:
                X[c] = 0.0

        y_prob = model.predict_proba(X)[:, 1]
        y_pred = (y_prob >= THRESHOLD).astype(int)

        # Severity classification
        has_parts_col = features_df.get("repuestos_count_evento", pd.Series(0, index=features_df.index))
        duration_col = features_df.get("duracion_ot_horas_prom_evento", pd.Series(0, index=features_df.index))
        keywords_col = features_df.get("num_keywords_tecnicos_evento", pd.Series(0, index=features_df.index))

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
            "repuestos_count_evento": has_parts_col,
            "duracion_ot_horas_prom_evento": duration_col,
            "num_keywords_tecnicos_evento": keywords_col,
        })
        features_df["severity"] = severity_df.apply(classify, axis=1)

        preds = pd.DataFrame({
            "placa_patente": features_df["placa_patente"],
            "fecha_evento": features_df["fecha_evento"],
            "horizon_days": window,
            "probability": y_prob,
            "alert": y_pred.astype(bool),
            "severity": features_df["severity"],
        })
        all_preds.append(preds)
        alerts = y_pred.sum()
        print(f"  {window}d: {len(preds)} events, {alerts} alerts ({alerts/len(preds)*100:.1f}%)")

    if all_preds:
        predictions_df = pd.concat(all_preds, ignore_index=True)
        predictions_df = predictions_df.sort_values(
            ["placa_patente", "fecha_evento", "horizon_days"]
        ).reset_index(drop=True)

        PREDICTIONS_DIR.mkdir(parents=True, exist_ok=True)
        path = PREDICTIONS_DIR / f"predictions_{label}.parquet"
        predictions_df.to_parquet(path, index=False)
        print(f"\n  Saved: {path}")
        print(f"  Total predictions: {len(predictions_df)}")
        print(f"  Unique buses: {predictions_df['placa_patente'].nunique()}")
        print(f"  Alerts: {predictions_df['alert'].sum()}")
        return predictions_df
    return None


def main():
    for d in [DATA_PROCESSED_DIR, MODELS_DIR, METRICS_DIR, PREDICTIONS_DIR]:
        d.mkdir(parents=True, exist_ok=True)

    # Step 1: Load and combine VOY + REDBUS
    base_df, base_train, base_test = step01_load_and_clean()

    # Step 2: Create events for training set
    eventos_train = step02_create_events(base_train, label="train")
    eventos_test = step02_create_events(base_test, label="test")

    # Step 3: Feature engineering
    features_train = step03_feature_engineering(eventos_train, label="train")
    features_test = step03_feature_engineering(eventos_test, label="test")

    # Step 4: Train models
    results = step04_train_models(features_train, label="voy_redbus")

    # Step 5: Test-bus holdout evaluation
    test_results = step05_test_bus_evaluation(features_test, label="voy_redbus")

    # Step 6: Batch inference on full data
    eventos_all = pd.read_parquet(DATA_PROCESSED_DIR / "eventos_train.parquet")
    # Use train events for inference (test events are separate)
    features_all = features_train
    predictions = step06_inference(features_all, label="voy_redbus")

    print("\n" + "=" * 60)
    print("PIPELINE COMPLETE")
    print("=" * 60)
    print(f"  Models: {MODELS_DIR / 'xgb_{3,5,7}d_voy_redbus.pkl'}")
    print(f"  Metrics: {METRICS_DIR / 'evaluation_summary_voy_redbus.json'}")
    print(f"  Test-bus metrics: {METRICS_DIR / 'test_buses_summary_voy_redbus.json'}")
    if predictions is not None:
        print(f"  Predictions: {PREDICTIONS_DIR / 'predictions_voy_redbus.parquet'}")
    print(f"\n  To query predictions:")
    print(f"    python scripts/consultar_bus.py --top 10")


if __name__ == "__main__":
    main()
