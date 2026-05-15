from __future__ import annotations

import numpy as np
import pandas as pd
import lightgbm as lgb

from .base import BaseModel


class LightGBMModel(BaseModel):
    def __init__(
        self,
        n_estimators: int = 800,
        max_depth: int = 12,
        learning_rate: float = 0.03,
        subsample: float = 0.85,
        colsample_bytree: float = 0.85,
        min_child_weight: int = 2,
        num_leaves: int = 63,
        reg_alpha: float = 0.1,
        reg_lambda: float = 2.0,
        scale_pos_weight: float | None = None,
        random_state: int = 42,
        n_jobs: int = -1,
        verbose: int = -1,
        boosting_type: str = "gbdt",
    ):
        self.init_params = {k: v for k, v in locals().items() if k != "self"}
        self.fit_params = {
            "scale_pos_weight": scale_pos_weight,
        }
        self.model: lgb.LGBMClassifier | None = None

    def fit(
        self,
        X_train: pd.DataFrame,
        y_train: pd.Series,
        X_val: pd.DataFrame | None = None,
        y_val: pd.Series | None = None,
    ):
        params = dict(self.init_params)
        params.pop("scale_pos_weight", None)

        spw = self.fit_params.get("scale_pos_weight")
        if spw is None:
            neg = int((1 - y_train).sum())
            pos = int(y_train.sum())
            params["scale_pos_weight"] = neg / pos if pos else 1.0
        else:
            params["scale_pos_weight"] = spw

        eval_set = [(X_train, y_train)]
        if X_val is not None:
            eval_set.append((X_val, y_val))

        self.model = lgb.LGBMClassifier(**params)
        self.model.fit(
            X_train, y_train,
            eval_set=eval_set,
            callbacks=[lgb.early_stopping(50)],
        )
        return self

    def predict_proba(self, X: pd.DataFrame) -> np.ndarray:
        if self.model is None:
            raise RuntimeError("Model not trained yet")
        return self.model.predict_proba(X.fillna(0))[:, 1]

    def set_scale_pos_weight(self, spw: float) -> None:
        self.fit_params["scale_pos_weight"] = spw

    def get_name(self) -> str:
        return "LightGBM"

    def get_params(self) -> dict:
        return dict(self.init_params)
