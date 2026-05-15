from __future__ import annotations

import sys
import warnings
from pathlib import Path

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

sys.path.append(str(Path(__file__).resolve().parent.parent.parent))

from src.evaluation import evaluate_model
from src.feature_engineering import get_feature_columns
from src.models import BaseModel

HORIZONS = [7, 5, 3]
PILOT_BUSES = ["FLXS22", "FLXS23", "LWTK42"]

# Features with zero importance across all 3 horizons (from feature importance analysis)
DROP_FEATURES = [
    "uuid_gestion_count_evento", "uuid_gestion_unique_count_evento",
    "km_desviacion_relativa_prom_evento", "tiene_uuid_gestion_evento",
    "num_sistemas_inspeccionados",
    "count_causa_mantenimiento_preventivo_ult_7d",
    "count_causa_mantenimiento_preventivo_ult_5d",
    "count_causa_mantenimiento_preventivo_ult_3d",
    "count_unidad_19_ult_7d", "count_unidad_19_ult_30d",
    "count_unidad_15_ult_7d", "count_unidad_15_ult_30d",
    "uuid_gestion_count_ult_7d", "uuid_gestion_count_ult_30d",
]


def compute_scale_pos_weight(y_train: pd.Series, multiplier: float = 1.0) -> float:
    pos = int(y_train.sum())
    neg = int((1 - y_train).sum())
    base = neg / pos if pos else 1.0
    return base * multiplier


def load_features(path: str | Path) -> pd.DataFrame:
    return pd.read_parquet(path)


def get_feature_columns_df(df: pd.DataFrame, drop: list[str] | None = None) -> list[str]:
    cols = get_feature_columns(df)
    if drop:
        cols = [c for c in cols if c not in drop]
    return cols


def select_threshold_from_val(
    model: BaseModel,
    X_train: pd.DataFrame,
    y_train: pd.Series,
    X_val: pd.DataFrame | None = None,
    y_val: pd.Series | None = None,
    val_frac: float = 0.1,
    search_steps: float = 0.05,
) -> float:
    """Select decision threshold that maximizes F1 on a validation split.

    If X_val/y_val are provided, uses them directly.
    Otherwise splits the last val_frac of X_train/y_train by row order (assumes temporal).
    """
    if X_val is None or y_val is None:
        split_idx = int(len(X_train) * (1 - val_frac))
        X_val = X_train.iloc[split_idx:]
        y_val = y_train.iloc[split_idx:]
        # Retrain on reduced training set (minus the val split)
        model.fit(X_train.iloc[:split_idx], y_train.iloc[:split_idx])
    else:
        model.fit(X_train, y_train)

    y_prob = model.predict_proba(X_val)

    best_f1 = 0.0
    best_th = 0.5
    for th in np.arange(search_steps, 1.0, search_steps):
        y_pred = (y_prob >= th).astype(int)
        from sklearn.metrics import f1_score
        f1 = f1_score(y_val, y_pred, zero_division=0)
        if f1 > best_f1:
            best_f1 = f1
            best_th = round(th, 2)

    return best_th


def train_eval_split(
    model: BaseModel,
    features_path: str | Path,
    cutoff_date: str = "2025-11-15",
    spw_multiplier: float | dict[int, float] = 3.0,
    label: str = "full",
    drop_features: list[str] | None = None,
    thresholds: tuple[float, ...] = (0.3, 0.4, 0.5, 0.6, 0.7),
    select_threshold: bool = True,
) -> dict:
    """Train with temporal split, evaluate with configurable SPW and features.

    If select_threshold=True, splits training data into train+val (last 10%
    of training rows), selects the threshold that maximizes F1 on val,
    and reports metrics at that threshold on the test set.
    """
    df = load_features(features_path)
    cutoff = pd.Timestamp(cutoff_date)

    feat_cols = get_feature_columns_df(df, drop=drop_features)
    print(f"  Features: {len(feat_cols)} (dropped {len(drop_features) if drop_features else 0})")

    results = {}
    for window in HORIZONS:
        target_col = f"correctivo_prox_{window}d"

        train = df[df["fecha_evento"] < cutoff].copy()
        test = df[df["fecha_evento"] >= cutoff].copy()

        mult = spw_multiplier if isinstance(spw_multiplier, (int, float)) else spw_multiplier.get(window, 1.0)

        if select_threshold:
            # Split training data into train/val by row order (temporal)
            val_frac = 0.1
            split_idx = int(len(train) * (1 - val_frac))
            train_train = train.iloc[:split_idx]
            train_val = train.iloc[split_idx:]

            X_train = train_train[feat_cols].fillna(0)
            y_train = train_train[target_col].astype(int)
            X_val = train_val[feat_cols].fillna(0)
            y_val = train_val[target_col].astype(int)
        else:
            X_train = train[feat_cols].fillna(0)
            y_train = train[target_col].astype(int)
            X_val = y_val = None

        X_test = test[feat_cols].fillna(0)
        y_test = test[target_col].astype(int)

        spw = compute_scale_pos_weight(y_train, multiplier=mult)
        pos_rate = y_train.mean()
        print(f"\n  --- {window}d: train={len(y_train)} ({pos_rate*100:.1f}% pos) "
              f"test={len(y_test)} spw={spw:.2f} (mult={mult}) ---")

        model.set_scale_pos_weight(spw)

        if select_threshold:
            selected_th = select_threshold_from_val(
                model, X_train, y_train, X_val, y_val
            )
            print(f"    Selected threshold from val: {selected_th:.2f}")
            # Re-fit on full training data with selected threshold
            model.set_scale_pos_weight(spw)
            model.fit(X_train, y_train, X_test, y_test)
        else:
            model.fit(X_train, y_train, X_test, y_test)
            selected_th = 0.5

        y_score = model.predict_proba(X_test)
        metrics = evaluate_model(y_test.values, y_score, thresholds=thresholds)

        # Use selected threshold for reporting
        th_key = str(selected_th)
        if th_key in metrics["threshold_metrics"]:
            r = metrics["threshold_metrics"][th_key]["classification_report"]
            cm = metrics["threshold_metrics"][th_key]["confusion_matrix"]
        else:
            r = metrics["threshold_metrics"]["0.5"]["classification_report"]
            cm = metrics["threshold_metrics"]["0.5"]["confusion_matrix"]
        tn, fp, fn, tp = cm[0][0], cm[0][1], cm[1][0], cm[1][1]
        acc = (tp + tn) / (tp + tn + fp + fn) if (tp + tn + fp + fn) > 0 else 0

        print(f"    Acc={acc:.4f} P={r['1']['precision']:.4f} R={r['1']['recall']:.4f} F1={r['1']['f1-score']:.4f}")
        print(f"    AUC-ROC={metrics.get('auc_roc', 'N/A')} Brier={metrics.get('brier_score', 'N/A')}")
        print(f"    Decision threshold: {selected_th:.2f}")

        # Per-pilot-bus evaluation (at selected threshold)
        pilot_accs = {}
        for bus in PILOT_BUSES:
            bus_test = test[test["placa_patente"] == bus]
            if len(bus_test) < 3:
                pilot_accs[bus] = None
                continue
            X_bus = bus_test[feat_cols].fillna(0)
            y_bus = bus_test[target_col].astype(int)
            bus_score = model.predict_proba(X_bus)
            bus_metrics = evaluate_model(y_bus.values, bus_score, thresholds=thresholds)
            th_key = str(selected_th)
            if th_key in bus_metrics.get("threshold_metrics", {}):
                b_r = bus_metrics["threshold_metrics"][th_key]["classification_report"]
                b_cm = bus_metrics["threshold_metrics"][th_key]["confusion_matrix"]
            else:
                b_r = bus_metrics["threshold_metrics"]["0.5"]["classification_report"]
                b_cm = bus_metrics["threshold_metrics"]["0.5"]["confusion_matrix"]
            b_tn, b_fp, b_fn, b_tp = b_cm[0][0], b_cm[0][1], b_cm[1][0], b_cm[1][1]
            b_acc = (b_tp + b_tn) / (b_tp + b_tn + b_fp + b_fn) if (b_tp + b_tn + b_fp + b_fn) > 0 else 0
            pilot_accs[bus] = {
                "accuracy": round(b_acc, 4),
                "precision": round(b_r["1"]["precision"], 4),
                "recall": round(b_r["1"]["recall"], 4),
                "f1": round(b_r["1"]["f1-score"], 4),
                "n": int(len(bus_test)),
                "n_pos": int(y_bus.sum()),
            }
            print(f"    {bus}: Acc={pilot_accs[bus]['accuracy']:.4f} "
                  f"P={pilot_accs[bus]['precision']:.4f} R={pilot_accs[bus]['recall']:.4f} "
                  f"F1={pilot_accs[bus]['f1']:.4f} n={pilot_accs[bus]['n']}")

        results[str(window)] = {
            "accuracy": round(acc, 4),
            "precision": round(r["1"]["precision"], 4),
            "recall": round(r["1"]["recall"], 4),
            "f1": round(r["1"]["f1-score"], 4),
            "specificity": round(tn / (tn + fp) if (tn + fp) > 0 else 0, 4),
            "auc_roc": metrics.get("auc_roc"),
            "auc_pr": metrics.get("precision_recall", {}).get("auc_pr"),
            "brier_score": metrics.get("brier_score"),
            "decision_threshold": float(selected_th),
            "best_f1_threshold": metrics.get("best_f1_threshold"),
            "best_f1": metrics.get("best_f1"),
            "confusion_matrix": cm,
            "train_size": int(len(train)),
            "val_size": int(len(y_val)) if select_threshold and y_val is not None else 0,
            "test_size": int(len(test)),
            "train_pos_rate": round(float(pos_rate), 4),
            "scale_pos_weight": round(spw, 4),
            "spw_multiplier": mult,
            "n_features": len(feat_cols),
            "pilot_buses": pilot_accs,
        }

    return {
        "model": model.get_name(),
        "params": model.get_params(),
        "mode": label,
        "cutoff_date": cutoff_date,
        "spw_multiplier": spw_multiplier,
        "n_features_initial": len(feat_cols),
        "drop_features": drop_features or [],
        "results": results,
    }
