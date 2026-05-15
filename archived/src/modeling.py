"""Entrenamiento de modelos XGBoost para distintas ventanas temporales."""

from __future__ import annotations

from typing import Any

import pandas as pd
import xgboost as xgb
from imblearn.over_sampling import SMOTE
from sklearn.model_selection import train_test_split

DEFAULT_MODEL_PARAMS = {
    "n_estimators": 400,
    "max_depth": 6,
    "learning_rate": 0.05,
    "subsample": 0.8,
    "colsample_bytree": 0.8,
    "random_state": 42,
    "eval_metric": "logloss",
    "n_jobs": -1,
}


def train_xgboost_model(
    X: pd.DataFrame,
    y: pd.Series,
    use_smote: bool = True,
    test_size: float = 0.2,
    random_state: int = 42,
    model_params: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Entrena un clasificador XGBoost con split estratificado y SMOTE opcional."""

    X = X.astype(float)
    X_train, X_test, y_train, y_test = train_test_split(
        X,
        y,
        test_size=test_size,
        random_state=random_state,
        stratify=y,
    )

    if use_smote:
        sampler = SMOTE(random_state=random_state)
        X_train, y_train = sampler.fit_resample(X_train, y_train)

    positive_count = int((y_train == 1).sum())
    negative_count = int((y_train == 0).sum())
    scale_pos_weight = negative_count / positive_count if positive_count else 1.0

    final_params = dict(DEFAULT_MODEL_PARAMS)
    if model_params:
        final_params.update(model_params)
    final_params["scale_pos_weight"] = scale_pos_weight

    model = xgb.XGBClassifier(**final_params)
    model.fit(X_train, y_train)

    y_score = model.predict_proba(X_test)[:, 1]

    return {
        "model": model,
        "X_train": X_train,
        "X_test": X_test,
        "y_train": y_train,
        "y_test": y_test,
        "y_score": y_score,
        "scale_pos_weight": scale_pos_weight,
    }
