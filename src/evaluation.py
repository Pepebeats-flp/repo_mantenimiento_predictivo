"""Funciones de evaluación y visualización de modelos de clasificación."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
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
        cm_raw = confusion_matrix(y_true, y_pred)
        r = classification_report(y_true, y_pred, zero_division=0, output_dict=True)

        # Handle 1-class confusion matrices (all true negatives or all positives)
        if cm_raw.shape == (1, 1):
            tn = int(cm_raw[0, 0])
            fp = fn = tp = 0
        elif cm_raw.shape == (1, 2):
            tn = int(cm_raw[0, 0])
            fp = int(cm_raw[0, 1])
            fn = tp = 0
        elif cm_raw.shape == (2, 1):
            tn = int(cm_raw[0, 0])
            fn = int(cm_raw[1, 0])
            fp = tp = 0
        else:
            tn = int(cm_raw[0, 0])
            fp = int(cm_raw[0, 1])
            fn = int(cm_raw[1, 0])
            tp = int(cm_raw[1, 1])

        tnr = tn / (tn + fp) if (tn + fp) > 0 else 0.0
        metrics["threshold_metrics"][str(threshold)] = {
            "classification_report": r,
            "confusion_matrix": cm_raw.tolist(),
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


def shadow_evaluate(
    predictions: pd.DataFrame,
    eventos_df: pd.DataFrame,
    buses_piloto: list[str] | None = None,
    thresholds: Iterable[float] = (0.3, 0.4, 0.5, 0.6, 0.7),
    exclude_low_severity: bool = True,
) -> dict[str, Any]:
    """Evalúa predicciones vs eventos reales para Shadow Mode.

    Para cada predicción (bus, fecha, horizonte H), determina si hubo
    un evento correctivo real en los H días posteriores usando la misma
    lógica shift(-1) del entrenamiento.

    Args:
        predictions: DataFrame con columnas placa_patente, fecha_evento,
                     horizon_days, probability, alert, severity
        eventos_df: DataFrame con eventos correctivos (placa_patente, fecha_evento)
        buses_piloto: Lista de buses a reportar individualmente (None = todos)
        thresholds: Lista de thresholds a evaluar
        exclude_low_severity: Si True, excluye eventos LOW de la evaluación

    Returns:
        Dict con métricas por horizonte y por bus
    """
    preds = predictions.copy()
    preds["fecha_evento"] = pd.to_datetime(preds["fecha_evento"])

    eventos = eventos_df.copy()
    eventos["fecha_evento"] = pd.to_datetime(eventos["fecha_evento"])

    if exclude_low_severity and "severity" in preds.columns:
        preds = preds[preds["severity"] != "LOW"].copy()

    resultados: dict[str, Any] = {
        "config": {
            "exclude_low_severity": exclude_low_severity,
            "buses_piloto": buses_piloto or [],
            "thresholds_evaluados": list(thresholds),
        },
        "por_horizonte": {},
        "por_bus": {},
    }

    ventanas = sorted(preds["horizon_days"].unique())

    for ventana in ventanas:
        sub_preds = preds[preds["horizon_days"] == ventana].copy()
        if sub_preds.empty:
            continue

        sub_preds = sub_preds.sort_values(["placa_patente", "fecha_evento"])

        next_event = (
            eventos.sort_values(["placa_patente", "fecha_evento"])
            .groupby("placa_patente")["fecha_evento"]
            .shift(-1)
        )
        eventos_next = eventos[["placa_patente", "fecha_evento"]].copy()
        eventos_next["prox_evento"] = next_event

        sub_preds = sub_preds.merge(
            eventos_next[["placa_patente", "fecha_evento", "prox_evento"]],
            on=["placa_patente", "fecha_evento"],
            how="left",
        )

        delta = (sub_preds["prox_evento"] - sub_preds["fecha_evento"]).dt.days
        y_true = delta.notna() & delta.le(ventana)
        y_true = y_true.astype(int).values

        y_score = sub_preds["probability"].values

        metrics = evaluate_model(y_true, y_score, thresholds=thresholds)
        metrics["total_predicciones"] = len(sub_preds)
        metrics["total_positivos_reales"] = int(y_true.sum())
        metrics["tasa_positivos_reales"] = round(float(y_true.mean()), 4)
        resultados["por_horizonte"][str(ventana)] = metrics

        if buses_piloto:
            resultados["por_bus"][str(ventana)] = {}
            for bus in buses_piloto:
                bus_preds = sub_preds[sub_preds["placa_patente"] == bus]
                if bus_preds.empty:
                    continue

                bus_delta = (
                    (bus_preds["prox_evento"] - bus_preds["fecha_evento"]).dt.days
                )
                bus_y_true = bus_delta.notna() & bus_delta.le(ventana)
                bus_y_true = bus_y_true.astype(int).values
                bus_y_score = bus_preds["probability"].values

                if len(bus_y_true) < 5:
                    resultados["por_bus"][str(ventana)][bus] = {
                        "error": "insufficient_data",
                        "total": int(len(bus_y_true)),
                    }
                    continue

                # Skip if only one class present (can't compute meaningful metrics)
                n_pos = int(bus_y_true.sum())
                n_neg = len(bus_y_true) - n_pos
                if n_pos == 0 or n_neg == 0:
                    resultados["por_bus"][str(ventana)][bus] = {
                        "error": "single_class",
                        "total": int(len(bus_y_true)),
                        "n_pos": n_pos,
                        "n_neg": n_neg,
                    }
                    continue

                bus_metrics = evaluate_model(
                    bus_y_true, bus_y_score, thresholds=thresholds
                )
                bus_metrics["total_predicciones"] = len(bus_preds)
                bus_metrics["total_positivos_reales"] = int(bus_y_true.sum())
                resultados["por_bus"][str(ventana)][bus] = bus_metrics

    return resultados

