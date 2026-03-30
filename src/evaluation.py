"""Funciones de evaluación y visualización de modelos de clasificación."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable

import matplotlib.pyplot as plt
import numpy as np
from sklearn.metrics import classification_report, confusion_matrix, precision_recall_curve


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
    """Calcula reportes por threshold y la curva Precision-Recall."""

    metrics: dict[str, Any] = {"threshold_metrics": {}}

    for threshold in thresholds:
        y_pred = (np.asarray(y_score) >= threshold).astype(int)
        metrics["threshold_metrics"][str(threshold)] = {
            "classification_report": classification_report(
                y_true,
                y_pred,
                zero_division=0,
                output_dict=True,
            ),
            "confusion_matrix": confusion_matrix(y_true, y_pred).tolist(),
        }

    precision, recall, pr_thresholds = precision_recall_curve(y_true, y_score)
    metrics["precision_recall"] = {
        "precision": precision.tolist(),
        "recall": recall.tolist(),
        "thresholds": pr_thresholds.tolist(),
    }

    return metrics


def save_metrics(metrics: dict[str, Any], output_path: str | Path) -> None:
    """Guarda métricas de evaluación en formato JSON."""

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(metrics, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

