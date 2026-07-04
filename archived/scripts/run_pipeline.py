#!/usr/bin/env python3
"""Complete pipeline runner: ETL (VOY+REDBUS) → training → inference → evaluation + test-bus holdout."""
from __future__ import annotations

import json
import pickle
import sys
import time
import warnings
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import xgboost as xgb

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
    generate_event_type_features,
    generate_severity_features,
    generate_bus_history_features,
    get_feature_columns,
    summarize_feature_quality,
    generate_trend_features,
    generate_bus_age_features,
)
from src.evaluation import evaluate_model, save_metrics
from sklearn.metrics import f1_score, accuracy_score

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_RAW_DIR = PROJECT_ROOT / "data" / "raw"
DATA_PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"

HORIZONS = [7, 5, 3]
DEFAULT_THRESHOLD = 0.5

TEST_BUSES = [
    "PFVL15", "PFVL21", "PFVK90", "PFTF88", "PDZH97",
    "PFYH17", "PFVK64", "PFYR84", "PFYG94", "PFTF84",
]

# Auto-detect GPU for XGBoost
_has_gpu = False
try:
    import xgboost as xgb
    import numpy as np
    X_test = np.random.rand(10, 5)
    y_test = np.random.randint(0, 2, 10)
    m = xgb.XGBClassifier(n_estimators=1, device='cuda', verbosity=0)
    m.fit(X_test, y_test)
    _has_gpu = True
    del m, X_test, y_test
except Exception:
    pass

CLIENT_MAP: dict = {
    "14": "VOY", "15": "VOY",
    "11": "REDBUS", "13": "REDBUS",
    "8": "METROPOL", "9": "METROPOL",
    "16": "GRANAMERICAS",
    "17": "CONECTA", "19": "CONECTA",
}

OPTIMIZED_PARAMS: dict = {
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
    "device": "cuda" if _has_gpu else "cpu",
}

# Experiment overrides — populated when --experiment is used
EXPERIMENT_NAME: str | None = None
EXPERIMENT_DIR: Path | None = None
EXPERIMENT_CFG: dict = {}

# CLI overrides
CLI_CUTOFF_DAYS: int = 30
CLI_TARGET_MODE: str = "all"
CLI_BALANCE: str = "none"


# ── Logging helper ──────────────────────────────────────────────────────────

def log_box(title: str, lines: list[str], char: str = "="):
    width = max(len(title), max(len(l) for l in lines) if lines else 0) + 4
    print(char * width)
    print(f"  {title}")
    print(char * width)
    for l in lines:
        print(f"  {l}")
    print(char * width)


def log_table(rows: list[list[str]], header: list[str] | None = None):
    if not rows:
        return
    col_widths = [max(len(str(r[i])) for r in rows) for i in range(len(rows[0]))]
    if header:
        col_widths = [max(col_widths[i], len(header[i])) for i in range(len(header))]
    sep = " | ".join("-" * w for w in col_widths)
    if header:
        print(" | ".join(h.ljust(col_widths[i]) for i, h in enumerate(header)))
        print(sep)
    for row in rows:
        print(" | ".join(str(row[i]).ljust(col_widths[i]) for i in range(len(row))))


def log_eta(start: float, done: int, total: int, label: str = ""):
    if done == 0:
        return
    elapsed = time.time() - start
    rate = done / elapsed
    remaining = (total - done) / rate if rate > 0 else 0
    eta_str = f"{remaining:.0f}s" if remaining < 120 else f"{remaining/60:.1f}m"
    print(f"  ⏱  {label}: {done}/{total} ({elapsed:.0f}s elapsed, ~{eta_str} remaining)")


def get_output_dirs():
    if EXPERIMENT_DIR:
        return {
            "models": EXPERIMENT_DIR / "models",
            "metrics": EXPERIMENT_DIR / "metrics",
            "predictions": EXPERIMENT_DIR / "predictions",
        }
    return {
        "models": PROJECT_ROOT / "models",
        "metrics": PROJECT_ROOT / "outputs" / "metrics",
        "predictions": PROJECT_ROOT / "data" / "predictions",
    }


def resolve_feature_columns(all_features: list[str]) -> list[str]:
    cfg = EXPERIMENT_CFG
    drop = cfg.get("drop_features", [])
    keep = cfg.get("keep_features", None)
    if keep:
        return [c for c in all_features if c in keep]
    if drop:
        return [c for c in all_features if c not in drop]
    return all_features


def resolve_model_params() -> dict:
    cfg = EXPERIMENT_CFG
    params = dict(OPTIMIZED_PARAMS)
    override = cfg.get("model_params", {})
    params.update(override)
    return params


def resolve_scale_pos_weight(pos_count: int, neg_count: int) -> float:
    base = neg_count / pos_count if pos_count else 1.0
    mult = EXPERIMENT_CFG.get("scale_pos_weight_multiplier", 1.0)
    return base * mult


def step01_load_and_clean(
    use_firestore: bool = True,
    credentials_path: str | Path | None = None,
):
    dataset_path = DATA_PROCESSED_DIR / "base.parquet"

    if dataset_path.exists():
        print("=" * 60)
        print("STEP 01: Load dataset local (data/processed/base.parquet)")
        print("=" * 60)
        base_df = pd.read_parquet(dataset_path)
        base_df["fecha_evento"] = pd.to_datetime(base_df["fecha_evento"], errors="coerce")
        print(f"  Registros: {len(base_df)}")
        print(f"  Buses: {base_df['placa_patente'].nunique()}")
        print(f"  Tipos: {base_df['tipo_servicio'].value_counts().to_dict()}")
        print(f"  Rango: {base_df['fecha_evento'].min()} \u2192 {base_df['fecha_evento'].max()}")
        base_test = base_df[base_df["placa_patente"].isin(TEST_BUSES)].copy()
        base_train = base_df[~base_df["placa_patente"].isin(TEST_BUSES)].copy()
        print(f"  Train buses: {base_train['placa_patente'].nunique()}")
        print(f"  Test holdout buses: {base_test['placa_patente'].nunique()} ({', '.join(TEST_BUSES)})")
        return base_df, base_train, base_test

    print("=" * 60)
    print("STEP 01: Load and clean data" + (" (desde Firestore)" if use_firestore else " (desde JSON local)"))
    print("=" * 60)

    regb_df: pd.DataFrame | None = None
    it_df: pd.DataFrame | None = None

    if use_firestore:
        prev_raw, corr_raw, regb_raw, it_raw = load_from_firestore(
            credentials_path=credentials_path
        )
        regb_df = normalize_inspection_records(regb_raw, "REGB")
        it_df = normalize_inspection_records(it_raw, "IT")
        print(f"  Firestore REGB: {len(regb_df) if regb_df is not None else 0} registros")
        print(f"  Firestore IT:   {len(it_df) if it_df is not None else 0} registros")
        clean_df = clean_data(prev_raw, corr_raw, regb_df=regb_df, it_df=it_df, empresa_id="ALL")
        clean_df = extract_additional_fields(clean_df)
        if "unidad_negocio" in clean_df.columns:
            clean_df["empresa_id"] = clean_df["unidad_negocio"].map(CLIENT_MAP).fillna("OTROS")
        print(f"  Firestore total: {len(clean_df)} registros")
        print(f"  Empresas: {clean_df['empresa_id'].value_counts().to_dict()}")
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
        print(f"  Firestore preventivos: {len(prev_raw)}")
        print(f"  Firestore correctivos: {len(corr_raw)}")
        print(f"  Firestore REGB: {len(regb_df) if regb_df is not None else 0}")
        print(f"  Firestore IT:   {len(it_df) if it_df is not None else 0}")
        clean_df = clean_data(prev_raw, corr_raw, regb_df=regb_df, it_df=it_df, empresa_id="ALL")
        clean_df = extract_additional_fields(clean_df)
        if "unidad_negocio" in clean_df.columns:
            clean_df["empresa_id"] = clean_df["unidad_negocio"].map(CLIENT_MAP).fillna("OTROS")
        print(f"  Total: {len(clean_df)} registros")
        print(f"  Empresas: {clean_df['empresa_id'].value_counts().to_dict()}")

    base_df = create_base_dataframe(clean_df, executed_only=True)
    print(f"\n  Combined shape: {base_df.shape}")
    print(f"  Buses: {base_df['placa_patente'].nunique()}")
    print(f"  Tipos: {base_df['tipo_servicio'].value_counts().to_dict()}")
    print(f"  Date range: {base_df['fecha_evento'].min()} \u2192 {base_df['fecha_evento'].max()}")
    print(f"  Empresas: {base_df.get('empresa_id', pd.Series(['N/A'])).value_counts().to_dict()}")

    base_test = base_df[base_df["placa_patente"].isin(TEST_BUSES)].copy()
    base_train = base_df[~base_df["placa_patente"].isin(TEST_BUSES)].copy()
    print(f"\n  Train buses: {base_train['placa_patente'].nunique()}")
    print(f"  Test holdout buses: {base_test['placa_patente'].nunique()} ({', '.join(TEST_BUSES)})")

    for _df, name in [(base_df, "base"), (base_train, "train"), (base_test, "test")]:
        for col in _df.select_dtypes(include=["object"]).columns:
            if _df[col].apply(lambda x: isinstance(x, list)).any():
                _df[col] = _df[col].apply(
                    lambda x: json.dumps(x, default=str) if isinstance(x, list) else x
                )

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
    t0 = time.time()
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

    feature_count_before_target = features_df.shape[1]
    print(f"  Features before targets: {feature_count_before_target}")

    features_df = create_future_targets(features_df, windows=(7, 5, 3, 10, 14, 30))
    quality_df = summarize_feature_quality(features_df, target_windows=(7, 5, 3))
    print(f"  Features shape: {features_df.shape}")
    print(f"  Time: {time.time()-t0:.1f}s")

    log_box("Target distribution (ALL events)", [])
    for w in HORIZONS:
        col = f"correctivo_prox_{w}d"
        counts = features_df[col].value_counts()
        pos = counts.get(True, 0) or counts.get(1, 0)
        neg = counts.get(False, 0) or counts.get(0, 0)
        total = pos + neg
        print(f"  {col}: {pos} positivos ({pos/total*100:.1f}%) | {neg} negativos ({neg/total*100:.1f}%)")

    log_box("Feature quality", [])
    print(f"\n{quality_df.to_string()}")

    if "tipo_servicio" in features_df.columns:
        log_box("Event type breakdown", [])
        for t, c in features_df["tipo_servicio"].value_counts().items():
            print(f"    {t}: {c} eventos")

    path = DATA_PROCESSED_DIR / f"features_{label}.parquet"
    features_df.to_parquet(path, index=False)
    print(f"  Saved: {path}")
    return features_df


def get_available_features(features_df: pd.DataFrame) -> list[str]:
    return get_feature_columns(features_df)


def step04_train_models(
    features_df: pd.DataFrame,
    label: str = "voy_redbus",
    cutoff_date: pd.Timestamp | None = None,
    balance: str = "none",
):
    dirs = get_output_dirs()
    models_dir = dirs["models"]
    metrics_dir = dirs["metrics"]

    log_box(f"STEP 04: Train XGBoost models ({label})", [
        f"Balance mode: {balance}",
        f"Cutoff: {cutoff_date}",
    ])

    if cutoff_date is None:
        cutoff_date = pd.Timestamp.now() - timedelta(days=30)

    feature_columns = resolve_feature_columns(get_available_features(features_df))
    model_params = resolve_model_params()
    print(f"  Using {len(feature_columns)} features")
    print(f"  Model params: max_depth={model_params.get('max_depth')}, "
          f"lr={model_params.get('learning_rate')}, "
          f"subsample={model_params.get('subsample')}, "
          f"colsample={model_params.get('colsample_bytree')}")

    train_df = features_df[features_df["fecha_evento"] < cutoff_date].copy()
    test_df = features_df[features_df["fecha_evento"] >= cutoff_date].copy()
    print(f"\n  Train: {len(train_df)} ({train_df['fecha_evento'].min()} → {train_df['fecha_evento'].max()})")
    print(f"  Test:  {len(test_df)} ({test_df['fecha_evento'].min()} → {test_df['fecha_evento'].max()})")

    results = {}
    for window in HORIZONS:
        target_col = f"correctivo_prox_{window}d"
        print(f"\n  {'─' * 50}")
        print(f"  HORIZONTE: {window}d")
        print(f"  {'─' * 50}")

        X_train = train_df[feature_columns].fillna(0)
        y_train = train_df[target_col].astype(int)
        X_test = test_df[feature_columns].fillna(0)
        y_test = test_df[target_col].astype(int)

        pos_count = int(y_train.sum())
        neg_count = int((1 - y_train).sum())
        scale_pos_weight = resolve_scale_pos_weight(pos_count, neg_count)
        base_pos_rate = y_train.mean()
        print(f"  Train: {pos_count} positivos ({pos_count/len(y_train)*100:.1f}%) / "
              f"{neg_count} negativos ({neg_count/len(y_train)*100:.1f}%)")
        print(f"  Test:  {int(y_test.sum())} positivos ({y_test.mean()*100:.1f}%) / "
              f"{int((1-y_test).sum())} negativos ({(1-y_test.mean())*100:.1f}%)")
        print(f"  scale_pos_weight: {scale_pos_weight:.2f}")

        params = dict(model_params)
        params["scale_pos_weight"] = scale_pos_weight

        # ── Threshold selection from validation split ───────────────
        val_frac = 0.1
        split_idx = int(len(train_df) * (1 - val_frac))
        train_train_df = train_df.iloc[:split_idx]
        train_val_df = train_df.iloc[split_idx:]

        X_train_train = train_train_df[feature_columns].fillna(0)
        y_train_train = train_train_df[target_col].astype(int)
        X_val = train_val_df[feature_columns].fillna(0)
        y_val = train_val_df[target_col].astype(int)

        val_pos = int(y_val.sum())
        print(f"  Val split: {len(train_val_df)} ({val_pos} positives, {val_pos/len(train_val_df)*100:.1f}%)")

        t_train = time.time()
        th_model = xgb.XGBClassifier(**params)
        th_model.fit(
            X_train_train, y_train_train,
            eval_set=[(X_val, y_val)],
            early_stopping_rounds=50,
            verbose=10,
        )
        val_probs = th_model.predict_proba(X_val)[:, 1]
        best_th = 0.5
        best_metric = 0.0
        print(f"  Searching best threshold (max ACC) on val split...")
        scores = []
        for th in np.arange(0.05, 0.95, 0.05):
            preds = (val_probs >= th).astype(int)
            acc = accuracy_score(y_val, preds)
            scores.append((th, acc))
            if acc > best_metric:
                best_metric = acc
                best_th = round(th, 2)
        best_f1_th = 0.5
        best_f1_val = 0.0
        for th in np.arange(0.05, 0.95, 0.05):
            f1 = f1_score(y_val, (val_probs >= th).astype(int), zero_division=0)
            if f1 > best_f1_val:
                best_f1_val = f1
                best_f1_th = round(th, 2)
        print(f"    Best ACC threshold: {best_th:.2f} (ACC={best_metric:.4f})")
        print(f"    Best F1  threshold: {best_f1_th:.2f} (F1={best_f1_val:.4f})")

        # ── Train on full training data ─────────────────────────────
        print(f"  Training full model ({time.time()-t_train:.1f}s for val search)...")
        t_full = time.time()
        model = xgb.XGBClassifier(**params)
        eval_set = [(X_train, y_train), (X_val, y_val)]
        model.fit(X_train, y_train, eval_set=eval_set, early_stopping_rounds=50, verbose=10)
        train_time = time.time() - t_full
        print(f"    Done in {train_time:.1f}s")

        if hasattr(model, "best_iteration"):
            print(f"    Best iteration: {model.best_iteration}")
            model.n_estimators = model.best_iteration + 1

        y_score = model.predict_proba(X_test)[:, 1]

        model_path = models_dir / f"xgb_{window}d_{label}.pkl"
        with open(model_path, "wb") as f:
            pickle.dump(model, f)

        metrics = evaluate_model(y_test, y_score, thresholds=(0.3, 0.4, 0.5, 0.6, 0.7))
        metrics["window_days"] = window
        metrics["feature_columns"] = feature_columns
        metrics["feature_count"] = len(feature_columns)
        metrics["scale_pos_weight"] = scale_pos_weight
        metrics["cutoff_date"] = str(cutoff_date)
        metrics["train_size"] = len(X_train)
        metrics["test_size"] = len(X_test)
        metrics["base_pos_rate"] = round(base_pos_rate, 4)

        metrics_path = metrics_dir / f"xgb_{window}d_{label}.json"
        save_metrics(metrics, metrics_path)

        meta = {
            "experiment": EXPERIMENT_NAME,
            "experiment_config": EXPERIMENT_CFG,
            "feature_names": feature_columns,
            "feature_count": len(feature_columns),
            "model_params": {k: str(v) if not isinstance(v, (int, float, bool)) else v
                             for k, v in params.items()},
            "scale_pos_weight": scale_pos_weight,
            "cutoff_date": str(cutoff_date),
            "best_iteration": getattr(model, "best_iteration", None),
            "decision_threshold": best_th,
            "val_acc_at_threshold": round(best_metric, 4),
            "val_f1_at_threshold": round(best_f1_val, 4),
            "balance": balance,
        }
        meta_path = models_dir / f"xgb_{window}d_{label}_meta.json"
        with open(meta_path, "w") as f:
            json.dump(meta, f, indent=2)

        imp = sorted(zip(feature_columns, model.feature_importances_), key=lambda x: -x[1])
        imp_path = metrics_dir / f"feature_importance_{window}d_{label}.csv"
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
            "decision_threshold": best_th,
            "balance": balance,
        }
        print(f"\n  RESULTS {window}d:")
        print(f"    ACC={acc:.4f} (baseline={baseline:.4f})")
        print(f"    P={r['1']['precision']:.4f} R={r['1']['recall']:.4f} F1={r['1']['f1-score']:.4f}")
        print(f"    Spec={spec:.4f} AUC-ROC={metrics.get('auc_roc', 'N/A'):.4f}")
        print(f"    TP={tp} TN={tn} FP={fp} FN={fn}")
        print(f"    Threshold: {best_th:.2f}")
        print(f"    Top-3: {imp[0][0]}={imp[0][1]:.4f}, {imp[1][0]}={imp[1][1]:.4f}, {imp[2][0]}={imp[2][1]:.4f}")

    summary_path = metrics_dir / f"evaluation_summary_{label}.json"
    with open(summary_path, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\n  Summary saved: {summary_path}")

    # ── Final aggregated results ────────────────────────────────────
    log_box("FINAL AGGREGATED RESULTS", [])
    total_tp = total_tn = total_fp = total_fn = 0
    for w in HORIZONS:
        r = results.get(str(w), {})
        cm = r.get("confusion_matrix", [])
        if cm:
            tn, fp, fn, tp = cm[0][0], cm[0][1], cm[1][0], cm[1][1]
            total_tp += tp
            total_tn += tn
            total_fp += fp
            total_fn += fn

    total_all = total_tp + total_tn + total_fp + total_fn
    global_acc = (total_tp + total_tn) / total_all if total_all > 0 else 0

    log_table(
        [[str(w),
          f"{results.get(str(w), {}).get('accuracy', 0)*100:.1f}%",
          f"{results.get(str(w), {}).get('f1', 0):.3f}",
          f"{results.get(str(w), {}).get('recall', 0)*100:.1f}%",
          f"{results.get(str(w), {}).get('precision', 0)*100:.1f}%",
          str(results.get(str(w), {}).get('decision_threshold', 'N/A'))]
         for w in HORIZONS],
        header=["Horizonte", "ACC", "F1", "Recall", "Precision", "Threshold"]
    )
    print(f"\n  GLOBAL ACC: {global_acc*100:.1f}% (TP={total_tp} TN={total_tn} FP={total_fp} FN={total_fn})")
    print(f"  Total predictions: {total_all}")

    return results


def step05_test_bus_evaluation(features_test: pd.DataFrame, label: str = "voy_redbus"):
    dirs = get_output_dirs()
    models_dir = dirs["models"]
    metrics_dir = dirs["metrics"]

    print("\n" + "=" * 60)
    print(f"STEP 05: Test-bus holdout evaluation ({label})")
    print("=" * 60)

    feature_columns = resolve_feature_columns(get_available_features(features_test))
    print(f"  Using {len(feature_columns)} features")

    all_results = {}
    for window in HORIZONS:
        target_col = f"correctivo_prox_{window}d"
        model_path = models_dir / f"xgb_{window}d_{label}.pkl"
        meta_path = models_dir / f"xgb_{window}d_{label}_meta.json"
        if not model_path.exists():
            print(f"  WARNING: Model {model_path} not found, skipping {window}d")
            continue

        with open(model_path, "rb") as f:
            model = pickle.load(f)

        # Load per-horizon threshold from model meta, then apply experiment overrides
        if meta_path.exists():
            with open(meta_path) as f:
                meta = json.load(f)
            threshold = meta.get("decision_threshold", DEFAULT_THRESHOLD)
        else:
            threshold = DEFAULT_THRESHOLD
        exp_thresholds = EXPERIMENT_CFG.get("thresholds", {})
        if str(window) in exp_thresholds:
            threshold = float(exp_thresholds[str(window)])
        else:
            exp_single = EXPERIMENT_CFG.get("threshold", None)
            if exp_single is not None:
                threshold = float(exp_single)

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
        metrics_path = metrics_dir / f"test_buses_{window}d_{label}.json"
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

        pred_df = pd.DataFrame({
            "placa_patente": features_test["placa_patente"],
            "fecha_evento": features_test["fecha_evento"],
            "actual": y_test.values,
            "probability": y_score,
            "predicted": (y_score >= threshold).astype(int),
            "threshold": threshold,
        })
        pred_path = metrics_dir / f"test_buses_predictions_{window}d_{label}.csv"
        pred_df.to_csv(pred_path, index=False)
        print(f"    Per-bus predictions saved: {pred_path}")

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
        print(f"\n  Per-bus results ({window}d):")
        for _, row in bus_summary.iterrows():
            print(f"    {row['placa_patente']:<12} events={row['events']:>3} "
                  f"actual_pos={int(row['actual_pos']):>3} predicted_pos={int(row['predicted_pos']):>3} "
                  f"mean_prob={row['mean_prob']*100:.1f}%")

    summary_path = metrics_dir / f"test_buses_summary_{label}.json"
    with open(summary_path, "w") as f:
        json.dump(all_results, f, indent=2)
    print(f"\n  Test-bus summary saved: {summary_path}")
    return all_results


def step06_inference(features_df: pd.DataFrame, label: str = "voy_redbus"):
    dirs = get_output_dirs()
    models_dir = dirs["models"]
    predictions_dir = dirs["predictions"]

    print("\n" + "=" * 60)
    print(f"STEP 06: Batch inference ({label})")
    print("=" * 60)

    # Per-horizon thresholds loaded from model meta (with experiment override)
    all_preds = []
    for window in HORIZONS:
        model_path = models_dir / f"xgb_{window}d_{label}.pkl"
        meta_path = models_dir / f"xgb_{window}d_{label}_meta.json"

        if not model_path.exists():
            print(f"  WARNING: Model {model_path} not found, skipping")
            continue

        with open(model_path, "rb") as f:
            model = pickle.load(f)

        if meta_path.exists():
            with open(meta_path) as f:
                meta = json.load(f)
            feature_columns = meta.get("feature_names", DEFAULT_FEATURE_COLUMNS)
            threshold = meta.get("decision_threshold", DEFAULT_THRESHOLD)
        else:
            feature_columns = DEFAULT_FEATURE_COLUMNS
            threshold = DEFAULT_THRESHOLD

        # Experiment config can override per-horizon thresholds
        exp_thresholds = EXPERIMENT_CFG.get("thresholds", {})
        if str(window) in exp_thresholds:
            threshold = float(exp_thresholds[str(window)])
            print(f"    Using experiment threshold override: {threshold:.2f}")
        else:
            exp_single = EXPERIMENT_CFG.get("threshold", None)
            if exp_single is not None:
                threshold = float(exp_single)

        missing = [c for c in feature_columns if c not in features_df.columns]
        X = pd.DataFrame(index=features_df.index)
        for c in feature_columns:
            if c in features_df.columns:
                X[c] = pd.to_numeric(features_df[c], errors="coerce").fillna(0)
            else:
                X[c] = 0.0

        y_prob = model.predict_proba(X)[:, 1]
        y_pred = (y_prob >= threshold).astype(int)

        has_parts_col = features_df.get("repuestos_count_evento", pd.Series(0, index=features_df.index)).fillna(0)
        duration_col = features_df.get("duracion_ot_horas_prom_evento", pd.Series(0, index=features_df.index)).fillna(0)
        keywords_col = features_df.get("num_keywords_tecnicos_evento", pd.Series(0, index=features_df.index)).fillna(0)
        highs_col = features_df.get("inspeccion_total_highs_evento", pd.Series(0, index=features_df.index)).fillna(0)

        def classify(row):
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

        severity_df = pd.DataFrame({
            "repuestos_count_evento": has_parts_col,
            "duracion_ot_horas_prom_evento": duration_col,
            "num_keywords_tecnicos_evento": keywords_col,
            "inspeccion_total_highs_evento": highs_col,
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

        predictions_dir.mkdir(parents=True, exist_ok=True)
        path = predictions_dir / f"predictions_{label}.parquet"
        predictions_df.to_parquet(path, index=False)
        print(f"\n  Saved: {path}")
        print(f"  Total predictions: {len(predictions_df)}")
        print(f"  Unique buses: {predictions_df['placa_patente'].nunique()}")
        print(f"  Alerts: {predictions_df['alert'].sum()}")
        return predictions_df
    return None


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Pipeline completo de mantenimiento predictivo")
    parser.add_argument("--local-json", action="store_true",
                        help="Usar archivos JSON locales en vez de Firestore")
    parser.add_argument("--experiment", type=str, default=None,
                        help="Nombre del experimento en config/experiments/<name>.json")
    parser.add_argument("--cutoff-days", type=int, default=30,
                        help="Días hacia atrás desde hoy para el corte train/test (default: 30)")
    parser.add_argument("--target-mode", type=str, default="all",
                        choices=["all", "last", "decay"],
                        help="Modo de target: 'all'=todos los eventos pre-falla, 'last'=solo el último, "
                             "'decay'=peso decreciente (default: all)")
    parser.add_argument("--balance", type=str, default="none",
                        choices=["none", "smote", "both"],
                        help="Balanceo de clases: 'none', 'smote', 'both' (smote + scale_pos_weight) (default: none)")
    args = parser.parse_args()

    global EXPERIMENT_NAME, EXPERIMENT_DIR, EXPERIMENT_CFG, CLI_CUTOFF_DAYS, CLI_TARGET_MODE, CLI_BALANCE

    CLI_CUTOFF_DAYS = args.cutoff_days
    CLI_TARGET_MODE = args.target_mode
    CLI_BALANCE = args.balance

    cutoff_date = pd.Timestamp.now() - timedelta(days=args.cutoff_days)

    if args.experiment:
        EXPERIMENT_NAME = args.experiment
        cfg_path = PROJECT_ROOT / "config" / "experiments" / f"{args.experiment}.json"
        if not cfg_path.exists():
            print(f"ERROR: Experiment config not found: {cfg_path}")
            sys.exit(1)
        with open(cfg_path) as f:
            EXPERIMENT_CFG = json.load(f)
        EXPERIMENT_DIR = PROJECT_ROOT / "outputs" / "experiments" / args.experiment
        EXPERIMENT_DIR.mkdir(parents=True, exist_ok=True)
        log_box(f"EXPERIMENT: {args.experiment}", [
            EXPERIMENT_CFG.get('description', ''),
            f"Output: {EXPERIMENT_DIR}",
        ])

    dirs = get_output_dirs()
    for d in [DATA_PROCESSED_DIR, dirs["models"], dirs["metrics"], dirs["predictions"]]:
        d.mkdir(parents=True, exist_ok=True)

    # ═══════════════════════════════════════════════════════════════════
    print("\n" + "█" * 60)
    print("  PIPELINE CONFIGURATION")
    print("█" * 60)
    print(f"  Cutoff date:      {cutoff_date.date()} ({args.cutoff_days} days ago)")
    print(f"  Target mode:      {args.target_mode}")
    print(f"  Balance:          {args.balance}")
    print(f"  Data source:      {'Firestore' if not args.local_json else 'Local JSON'}")
    print(f"  Horizons:         {HORIZONS}")
    print(f"  Holdout buses:    {TEST_BUSES}")
    if EXPERIMENT_NAME:
        print(f"  Experiment:       {EXPERIMENT_NAME}")
    print("█" * 60 + "\n")

    t_pipeline = time.time()

    base_df, base_train, base_test = step01_load_and_clean(
        use_firestore=not args.local_json,
    )

    eventos_train = step02_create_events(base_train, label="train")
    eventos_test = step02_create_events(base_test, label="test")

    features_train = step03_feature_engineering(eventos_train, label="train")
    features_test = step03_feature_engineering(eventos_test, label="test")

    results = step04_train_models(
        features_train,
        label="voy_redbus",
        cutoff_date=cutoff_date,
        balance=args.balance,
    )

    test_results = step05_test_bus_evaluation(features_test, label="voy_redbus")

    eventos_all = pd.read_parquet(DATA_PROCESSED_DIR / "eventos_train.parquet")
    features_all = features_train
    predictions = step06_inference(features_all, label="voy_redbus")

    if EXPERIMENT_CFG:
        meta = {
            "experiment": EXPERIMENT_NAME,
            "config": EXPERIMENT_CFG,
            "test_set_results": results,
            "test_bus_results": test_results,
        }
        meta_path = EXPERIMENT_DIR / "results.json"
        with open(meta_path, "w") as f:
            json.dump(meta, f, indent=2)

    # ═══════════════════════════════════════════════════════════════════
    # FINAL EVALUATION REPORT
    # ═══════════════════════════════════════════════════════════════════
    pipeline_time = time.time() - t_pipeline
    print("\n" + "█" * 60)
    print("  PIPELINE COMPLETE")
    print("█" * 60)
    print(f"  Total time: {pipeline_time/60:.1f} minutes\n")

    total_tp = total_tn = total_fp = total_fn = 0
    for w in HORIZONS:
        r = results.get(str(w), {})
        cm = r.get("confusion_matrix", [])
        if cm:
            tn, fp, fn, tp = cm[0][0], cm[0][1], cm[1][0], cm[1][1]
            total_tp += tp
            total_tn += tn
            total_fp += fp
            total_fn += fn

    total_all = total_tp + total_tn + total_fp + total_fn
    global_acc = (total_tp + total_tn) / total_all if total_all > 0 else 0

    log_box("FINAL RESULTS — Test Set", [
        f"ACC global:     {global_acc*100:.1f}%",
        f"TP: {total_tp} | TN: {total_tn} | FP: {total_fp} | FN: {total_fn}",
        f"Total preds:    {total_all:,}",
        f"Baseline (mayoritaria): {results.get(str(HORIZONS[0]), {}).get('baseline_always_majority', 0)*100:.1f}%",
    ])

    log_table(
        [[str(w),
          f"{results.get(str(w), {}).get('accuracy', 0)*100:.1f}%",
          f"{results.get(str(w), {}).get('f1', 0):.3f}",
          f"{results.get(str(w), {}).get('recall', 0)*100:.1f}%",
          f"{results.get(str(w), {}).get('precision', 0)*100:.1f}%",
          f"{results.get(str(w), {}).get('specificity', 0)*100:.1f}%",
          str(results.get(str(w), {}).get('decision_threshold', 'N/A'))]
         for w in HORIZONS],
        header=["Hrz", "ACC", "F1", "Recall", "Prec", "Spec", "Thresh"]
    )

    print(f"\n  Config used:")
    print(f"    cutoff_days={args.cutoff_days}, target_mode={args.target_mode}, balance={args.balance}")
    print(f"    Optimized params: max_depth={OPTIMIZED_PARAMS.get('max_depth')}, "
          f"lr={OPTIMIZED_PARAMS.get('learning_rate')}, n_estimators={OPTIMIZED_PARAMS.get('n_estimators')}")

    print(f"\n  Models: {dirs['models'] / 'xgb_{3,5,7}d_voy_redbus.pkl'}")
    print(f"  Metrics: {dirs['metrics'] / 'evaluation_summary_voy_redbus.json'}")
    print(f"  Test-bus metrics: {dirs['metrics'] / 'test_buses_summary_voy_redbus.json'}")
    if predictions is not None:
        print(f"  Predictions: {dirs['predictions'] / 'predictions_voy_redbus.parquet'}")
    print(f"\n  To query predictions:")
    print(f"    python scripts/consultar_bus.py --top 10")


if __name__ == "__main__":
    main()
