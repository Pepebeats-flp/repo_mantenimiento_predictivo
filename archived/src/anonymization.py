"""Utilidades para anonimizar identificadores sin alterar la secuencia temporal."""

from __future__ import annotations

import hashlib

import pandas as pd


def hash_bus_identifier(value: object, salt: str = "") -> str | pd.NA:
    """Aplica SHA256 a ``placa_patente`` preservando valores faltantes."""

    if pd.isna(value):
        return pd.NA

    normalized_value = f"{salt}{str(value).strip()}"
    return hashlib.sha256(normalized_value.encode("utf-8")).hexdigest()


def anonymize_dataset(
    df: pd.DataFrame,
    id_column: str = "placa_patente",
    salt: str = "",
) -> pd.DataFrame:
    """Reemplaza el identificador del bus por un hash sin tocar las fechas."""

    anonymized_df = df.copy()
    anonymized_df[id_column] = anonymized_df[id_column].map(
        lambda value: hash_bus_identifier(value, salt=salt),
    )
    return anonymized_df

