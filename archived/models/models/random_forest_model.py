from __future__ import annotations

import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier

from .base import BaseModel


class RandomForestModel(BaseModel):
    def __init__(
        self,
        n_estimators: int = 500,
        max_depth: int = 15,
        min_samples_split: int = 10,
        min_samples_leaf: int = 5,
        max_features: str = "sqrt",
        class_weight: str | dict | None = "balanced",
        random_state: int = 42,
        n_jobs: int = -1,
    ):
        self.init_params = {k: v for k, v in locals().items() if k != "self"}
        self._actual_class_weight = class_weight
        self.model: RandomForestClassifier | None = None

    def fit(
        self,
        X_train: pd.DataFrame,
        y_train: pd.Series,
        X_val: pd.DataFrame | None = None,
        y_val: pd.Series | None = None,
    ):
        self.model = RandomForestClassifier(**self.init_params)
        self.model.fit(X_train, y_train)
        return self

    def predict_proba(self, X: pd.DataFrame) -> np.ndarray:
        if self.model is None:
            raise RuntimeError("Model not trained yet")
        X_clean = X.fillna(0)
        return self.model.predict_proba(X_clean)[:, 1]

    def get_name(self) -> str:
        return "RandomForest"

    def get_params(self) -> dict:
        return dict(self.init_params)
