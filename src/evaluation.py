"""Funciones de evaluación y visualización de modelos de clasificación."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable

import matplotlib.pyplot as plt
import numpy as np
from sklearn.metrics import (
    auc,
    brier_score_loss,
    classification_report,
    confusion_matrix,
    precision_recall_curve,
    roc_auc_score,
    roc_curve,
)


def _lift_at_k(y_true: np.ndarray, y_score: np.ndarray, k: float) -> float:
    """Lift: proportion of positives captured in top-k% vs random."""
    n = len(y_true)
    n_top = max(1, int(n * k / 100))
    idx = np.argsort(y_score)[::-1][:n_top]
    captured = y_true[idx].sum()
    total_pos = y_true.sum()
    if total_pos == 0:
        return 0.0
    return captured / total_pos * 100


def plot_precision_recall(
    y_true: np.ndarray | list[int],
    y_score: np.ndarray | list[float],
    output_path: str | Path,
    title: str,
) -> dict[str, list[float]]:
    """Genera y guarda la curva Precision-Recall."""

    precision, recall, thresholds = precision_recall_curve(y_true, y_score)

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    plt.figure(figsize=(6, 4))
    plt.plot(recall, precision)
    plt.xlabel("Recall")
    plt.ylabel("Precision")
    plt.title(title)
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(output_path, dpi=150)
    plt.close()

    return {
        "precision": precision.tolist(),
        "recall": recall.tolist(),
        "thresholds": thresholds.tolist(),
    }


def evaluate_model(
    y_true: np.ndarray | list[int],
    y_score: np.ndarray | list[float],
    thresholds: Iterable[float] = (0.3, 0.4, 0.5, 0.6),
) -> dict[str, Any]:
    """Calcula reportes por threshold, curvas, y métricas extendidas."""

    y_true = np.asarray(y_true)
    y_score = np.asarray(y_score)

    metrics: dict[str, Any] = {"threshold_metrics": {}}

    for threshold in thresholds:
        y_pred = (y_score >= threshold).astype(int)
        cm = confusion_matrix(y_true, y_pred).tolist()
        r = classification_report(y_true, y_pred, zero_division=0, output_dict=True)
        tn, fp, fn, tp = cm[0][0], cm[0][1], cm[1][0], cm[1][1]
        tnr = tn / (tn + fp) if (tn + fp) > 0 else 0.0
        metrics["threshold_metrics"][str(threshold)] = {
            "classification_report": r,
            "confusion_matrix": cm,
            "specificity": round(tnr, 4),
            "accuracy": round((tp + tn) / (tp + tn + fp + fn), 4),
        }

    # AUC-ROC
    try:
        metrics["auc_roc"] = round(float(roc_auc_score(y_true, y_score)), 4)
    except Exception:
        metrics["auc_roc"] = None

    # AUC-PR (average precision)
    precision, recall, pr_thresholds = precision_recall_curve(y_true, y_score)
    auc_pr = float(auc(recall, precision)) if len(precision) > 1 else 0.0
    metrics["precision_recall"] = {
        "precision": precision.tolist(),
        "recall": recall.tolist(),
        "thresholds": pr_thresholds.tolist(),
        "auc_pr": round(auc_pr, 4),
    }

    # ROC curve data
    fpr, tpr, roc_thresholds = roc_curve(y_true, y_score)
    metrics["roc_curve"] = {
        "fpr": fpr.tolist(),
        "tpr": tpr.tolist(),
        "thresholds": roc_thresholds.tolist(),
    }

    # Brier score (calibration)
    try:
        metrics["brier_score"] = round(float(brier_score_loss(y_true, y_score)), 4)
    except Exception:
        metrics["brier_score"] = None

    # Lift at top-k
    for k in [5, 10, 20]:
        metrics[f"lift_top_{k}pct"] = round(_lift_at_k(y_true, y_score, k), 2)

    # Best F1 threshold
    best_f1 = 0.0
    best_th = 0.5
    for th in np.arange(0.05, 0.95, 0.05):
        y_pred = (y_score >= th).astype(int)
        r = classification_report(y_true, y_pred, zero_division=0, output_dict=True)
        f1 = r["1"]["f1-score"]
        if f1 > best_f1:
            best_f1 = f1
            best_th = round(th, 2)
    metrics["best_f1_threshold"] = best_th
    metrics["best_f1"] = round(best_f1, 4)

    # Baseline comparison
    pos_rate = float(y_true.mean())
    neg_rate = 1.0 - pos_rate
    always_1_acc = max(pos_rate, neg_rate)
    metrics["baseline_always_majority"] = round(always_1_acc, 4)

    return metrics


def save_metrics(metrics: dict[str, Any], output_path: str | Path) -> None:
    """Guarda métricas de evaluación en formato JSON."""

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(metrics, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

