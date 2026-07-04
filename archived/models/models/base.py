from __future__ import annotations

from abc import ABC, abstractmethod

import numpy as np
import pandas as pd


class BaseModel(ABC):
    @abstractmethod
    def fit(
        self,
        X_train: pd.DataFrame,
        y_train: pd.Series,
        X_val: pd.DataFrame | None = None,
        y_val: pd.Series | None = None,
    ) -> None:
        ...

    @abstractmethod
    def predict_proba(self, X: pd.DataFrame) -> np.ndarray:
        ...

    @abstractmethod
    def get_name(self) -> str:
        ...

    @abstractmethod
    def get_params(self) -> dict:
        ...

    def set_scale_pos_weight(self, spw: float) -> None:
        pass
