"""Modelos alternativos: HGB (HistGradientBoosting) y MLP (Red Neuronal)
para comparación con XGBoost."""

from __future__ import annotations

import time
from typing import Any

import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.metrics import (
    confusion_matrix,
    roc_auc_score,
    f1_score,
)
from sklearn.neural_network import MLPClassifier
from sklearn.preprocessing import StandardScaler


HGB_PARAMS: dict[str, Any] = {
    "loss": "log_loss",
    "learning_rate": 0.03,
    "max_iter": 800,
    "max_leaf_nodes": 63,
    "max_depth": 8,
    "min_samples_leaf": 20,
    "l2_regularization": 2.0,
    "early_stopping": True,
    "validation_fraction": 0.15,
    "n_iter_no_change": 15,
    "random_state": 42,
    "verbose": 0,
}

MLP_PARAMS: dict[str, Any] = {
    "hidden_layer_sizes": (256, 128, 64),
    "activation": "relu",
    "solver": "adam",
    "alpha": 0.001,
    "batch_size": 256,
    "learning_rate": "adaptive",
    "learning_rate_init": 0.001,
    "max_iter": 200,
    "early_stopping": True,
    "validation_fraction": 0.15,
    "n_iter_no_change": 15,
    "random_state": 42,
    "verbose": False,
}


def evaluate_predictions(
    y_true: np.ndarray,
    y_score: np.ndarray,
    threshold: float = 0.5,
) -> dict[str, Any]:
    """Compute standard evaluation metrics for binary classification."""
    y_pred = (y_score >= threshold).astype(int)
    cm = confusion_matrix(y_true, y_pred).tolist()

    # Handle single-class corner cases
    if len(cm) == 1 or len(cm[0]) == 1:
        if len(cm) == 1:
            tn, fp, fn, tp = cm[0][0] if cm[0] else 0, 0, 0, 0
        else:
            tn = cm[0][0] if len(cm[0]) > 0 else 0
            fp = cm[0][1] if len(cm[0]) > 1 else 0
            fn = cm[1][0] if len(cm) > 1 and len(cm[1]) > 0 else 0
            tp = cm[1][1] if len(cm) > 1 and len(cm[1]) > 1 else 0
    else:
        tn, fp, fn, tp = cm[0][0], cm[0][1], cm[1][0], cm[1][1]

    acc = (tp + tn) / (tp + tn + fp + fn) if (tp + tn + fp + fn) > 0 else 0.0
    prec = tp / (tp + fp) if (tp + fp) > 0 else 0.0
    rec = tp / (tp + fn) if (tp + fn) > 0 else 0.0
    f1 = f1_score(y_true, y_pred, zero_division=0)
    spec = tn / (tn + fp) if (tn + fp) > 0 else 0.0

    try:
        auc_roc = float(roc_auc_score(y_true, y_score))
    except Exception:
        auc_roc = None

    return {
        "accuracy": round(acc, 4),
        "precision": round(prec, 4),
        "recall": round(rec, 4),
        "f1": round(f1, 4),
        "specificity": round(spec, 4),
        "auc_roc": auc_roc,
        "confusion_matrix": [[int(tn), int(fp)], [int(fn), int(tp)]],
        "n_total": int(len(y_true)),
        "n_pos": int(y_true.sum()),
        "n_neg": int((1 - y_true).sum()),
    }


def _scale_features(X_train: pd.DataFrame, X_test: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, StandardScaler]:
    """Standardize features for neural network models."""
    scaler = StandardScaler()
    X_train_scaled = pd.DataFrame(
        scaler.fit_transform(X_train),
        columns=X_train.columns,
        index=X_train.index,
    )
    X_test_scaled = pd.DataFrame(
        scaler.transform(X_test),
        columns=X_test.columns,
        index=X_test.index,
    )
    return X_train_scaled, X_test_scaled, scaler


def train_histgb(
    X_train: pd.DataFrame,
    y_train: pd.Series,
    X_test: pd.DataFrame,
    y_test: pd.Series,
    params: dict[str, Any] | None = None,
    class_weight: str | None = "balanced",
) -> dict[str, Any]:
    """Train HistGradientBoosting classifier (sklearn) — alternativa a LightGBM.

    HistGradientBoosting es un gradient boosting basado en histogramas
    similar a LightGBM pero sin dependencias externas.
    """

    final_params = dict(HGB_PARAMS)
    if params:
        final_params.update(params)
    if class_weight:
        final_params["class_weight"] = class_weight

    X_train_f = X_train.fillna(0).astype(float)
    X_test_f = X_test.fillna(0).astype(float)
    y_train_f = y_train.astype(int)
    y_test_f = y_test.astype(int)

    t0 = time.time()
    model = HistGradientBoostingClassifier(**final_params)
    model.fit(X_train_f, y_train_f)
    train_time = time.time() - t0

    y_score = model.predict_proba(X_test_f)[:, 1]
    metrics = evaluate_predictions(y_test_f.values, y_score)

    return {
        "model": model,
        "model_type": "histgb",
        "train_time_sec": round(train_time, 2),
        "y_score": y_score.tolist(),
        "y_true": y_test_f.values.tolist(),
        "metrics": metrics,
        "n_iter": model.n_iter_,
    }


def train_mlp(
    X_train: pd.DataFrame,
    y_train: pd.Series,
    X_test: pd.DataFrame,
    y_test: pd.Series,
    params: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Train MLP (neural network) classifier and return model + metrics.

    Requires feature scaling. Early stopping uses 15% validation split
    from training data.
    """

    final_params = dict(MLP_PARAMS)
    if params:
        final_params.update(params)

    X_train_f = X_train.fillna(0).astype(float)
    X_test_f = X_test.fillna(0).astype(float)
    y_train_f = y_train.astype(int)
    y_test_f = y_test.astype(int)

    # Scale features
    X_train_s, X_test_s, scaler = _scale_features(X_train_f, X_test_f)

    t0 = time.time()
    model = MLPClassifier(**final_params)
    model.fit(X_train_s, y_train_f)
    train_time = time.time() - t0

    y_score = model.predict_proba(X_test_s)[:, 1]
    metrics = evaluate_predictions(y_test_f.values, y_score)

    return {
        "model": model,
        "model_type": "mlp",
        "train_time_sec": round(train_time, 2),
        "y_score": y_score.tolist(),
        "y_true": y_test_f.values.tolist(),
        "metrics": metrics,
        "n_iter": model.n_iter_,
        "loss_curve": model.loss_curve_,
        "scaler": scaler,
    }


def train_xgboost_reference(
    X_train: pd.DataFrame,
    y_train: pd.Series,
    X_test: pd.DataFrame,
    y_test: pd.Series,
    params: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Train XGBoost as reference model (same params as current pipeline)."""
    import xgboost as xgb

    X_train_f = X_train.fillna(0).astype(float)
    X_test_f = X_test.fillna(0).astype(float)
    y_train_f = y_train.astype(int)
    y_test_f = y_test.astype(int)

    neg = int((1 - y_train_f).sum())
    pos = int(y_train_f.sum())
    scale_pos_weight = neg / pos if pos else 1.0

    default_params = {
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
    if params:
        default_params.update(params)
    default_params["scale_pos_weight"] = scale_pos_weight

    t0 = time.time()
    model = xgb.XGBClassifier(**default_params)
    model.fit(
        X_train_f, y_train_f,
        eval_set=[(X_train_f, y_train_f), (X_test_f, y_test_f)],
        verbose=False,
    )
    train_time = time.time() - t0

    if hasattr(model, "best_iteration"):
        model.n_estimators = model.best_iteration + 1

    y_score = model.predict_proba(X_test_f)[:, 1]
    metrics = evaluate_predictions(y_test_f.values, y_score)

    importance = sorted(
        zip(X_train_f.columns, model.feature_importances_),
        key=lambda x: -x[1],
    )

    return {
        "model": model,
        "model_type": "xgboost",
        "train_time_sec": round(train_time, 2),
        "y_score": y_score.tolist(),
        "y_true": y_test_f.values.tolist(),
        "metrics": metrics,
        "best_iteration": getattr(model, "best_iteration", None),
        "top_features": [(name, int(imp)) for name, imp in importance[:10]],
    }


def compare_all_models(
    X_train: pd.DataFrame,
    y_train: pd.Series,
    X_test: pd.DataFrame,
    y_test: pd.Series,
    horizons: list[int] | None = None,
) -> dict[str, Any]:
    """Train XGBoost, HistGradientBoosting and MLP on the same split.

    Returns dict with per-model, per-horizon results plus global comparison table.
    """
    if horizons is None:
        horizons = [7, 5, 3]

    results: dict[str, Any] = {
        "models": {},
        "comparison": {},
    }

    for window in horizons:
        print(f"\n{'='*60}")
        print(f"  HORIZONTE {window}d")
        print(f"{'='*60}")

        target_col = f"correctivo_prox_{window}d"
        if target_col not in y_train.name and y_train.name is None:
            y_train_w = y_train if hasattr(y_train, 'name') and y_train.name == target_col else None
        else:
            y_train_w = y_train

        # We need separate y for each window - use the full df approach
        print(f"  X_train: {len(X_train)}, X_test: {len(X_test)}")

        window_results: dict[str, Any] = {}

        # 1. XGBoost
        print(f"\n  --- XGBoost ---")
        xgb_res = train_xgboost_reference(X_train, y_train, X_test, y_test)
        print(f"    Acc={xgb_res['metrics']['accuracy']:.4f}  F1={xgb_res['metrics']['f1']:.4f}  "
              f"AUC={xgb_res['metrics']['auc_roc']}  [{xgb_res['train_time_sec']}s]")
        window_results["xgboost"] = xgb_res

        # 2. HistGradientBoosting (alternativa tipo LightGBM)
        print(f"\n  --- HistGradientBoosting ---")
        hgb_res = train_histgb(X_train, y_train, X_test, y_test)
        print(f"    Acc={hgb_res['metrics']['accuracy']:.4f}  F1={hgb_res['metrics']['f1']:.4f}  "
              f"AUC={hgb_res['metrics']['auc_roc']}  [{hgb_res['train_time_sec']}s]")
        window_results["histgb"] = hgb_res

        # 3. MLP
        print(f"\n  --- MLP (Neural Network) ---")
        mlp_res = train_mlp(X_train, y_train, X_test, y_test)
        print(f"    Acc={mlp_res['metrics']['accuracy']:.4f}  F1={mlp_res['metrics']['f1']:.4f}  "
              f"AUC={mlp_res['metrics']['auc_roc']}  [{mlp_res['train_time_sec']}s]")
        window_results["mlp"] = mlp_res

        results["models"][str(window)] = window_results

        # Build comparison table
        comp_rows = []
        for model_name in ["xgboost", "histgb", "mlp"]:
            m = window_results[model_name]["metrics"]
            comp_rows.append({
                "modelo": model_name.upper(),
                "accuracy": m["accuracy"],
                "precision": m["precision"],
                "recall": m["recall"],
                "f1": m["f1"],
                "specificity": m["specificity"],
                "auc_roc": m["auc_roc"] if m["auc_roc"] else 0,
                "train_time_sec": window_results[model_name]["train_time_sec"],
            })

        # Find best per metric
        if comp_rows:
            best = {
                "accuracy": max(comp_rows, key=lambda r: r["accuracy"])["modelo"],
                "f1": max(comp_rows, key=lambda r: r["f1"])["modelo"],
                "auc_roc": max(comp_rows, key=lambda r: r["auc_roc"])["modelo"],
            }
            results["comparison"][str(window)] = {
                "best_by_metric": best,
                "table": comp_rows,
            }

    return results
