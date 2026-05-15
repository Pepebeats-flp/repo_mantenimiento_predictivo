from __future__ import annotations

import numpy as np
import pandas as pd

from .base import BaseModel


class EnsembleModel(BaseModel):
    def __init__(self, models: list[BaseModel], weights: list[float] | None = None):
        self.models = models
        self.weights = weights or [1.0 / len(models)] * len(models)

    def fit(
        self,
        X_train: pd.DataFrame,
        y_train: pd.Series,
        X_val: pd.DataFrame | None = None,
        y_val: pd.Series | None = None,
    ):
        for m in self.models:
            m.fit(X_train, y_train, X_val, y_val)
        return self

    def predict_proba(self, X: pd.DataFrame) -> np.ndarray:
        preds = np.zeros((len(X),))
        total_weight = sum(self.weights)
        for w, m in zip(self.weights, self.models):
            preds += w * m.predict_proba(X)
        return preds / total_weight

    def set_scale_pos_weight(self, spw: float) -> None:
        for m in self.models:
            m.set_scale_pos_weight(spw)

    def get_name(self) -> str:
        return "Ensemble(" + "+".join(m.get_name() for m in self.models) + ")"

    def get_params(self) -> dict:
        return {
            "models": [m.get_params() for m in self.models],
            "weights": self.weights,
        }
