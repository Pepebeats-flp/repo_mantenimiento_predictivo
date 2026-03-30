"""Rutinas de limpieza y consolidación de la base histórica."""

from __future__ import annotations

from typing import Any, Iterable

import pandas as pd

from .data_loader import flatten_firebase

DELETED_STATUS = 3
EXECUTED_STATUS = 1
DEFAULT_EMPRESA_ID = "RBUS"
SORT_COLUMNS = ["tipo_revision", "placa_patente", "fecha_evento", "firebase_id"]
EVENT_AGGREGATIONS = {
    "fecha_evento": "min",
    "causa_origen": "first",
    "sistema_componente": "first",
    "taller_planta": "first",
}


def _filter_deleted(records: Iterable[Any]) -> list[Any]:
    """Descarta registros marcados como eliminados."""

    filtered_records: list[Any] = []
    for record in records:
        if not isinstance(record, (list, tuple)) or len(record) != 2:
            continue

        _, payload = record
        estado = payload.get("estado") if isinstance(payload, dict) else None
        if estado != DELETED_STATUS:
            filtered_records.append(record)

    return filtered_records


def clean_data(
    preventivo_raw: Iterable[Any],
    correctivo_raw: Iterable[Any],
    empresa_id: str = DEFAULT_EMPRESA_ID,
) -> pd.DataFrame:
    """Normaliza ambos JSON, agrega metadatos comunes y crea ``fecha_evento``."""

    preventivo_filtered = _filter_deleted(preventivo_raw)
    correctivo_filtered = _filter_deleted(correctivo_raw)

    df_preventivo = flatten_firebase(preventivo_filtered)
    df_correctivo = flatten_firebase(correctivo_filtered)

    if df_preventivo.empty:
        df_preventivo = pd.DataFrame(columns=["firebase_id"])
    if df_correctivo.empty:
        df_correctivo = pd.DataFrame(columns=["firebase_id"])

    df_preventivo["tipo_revision"] = "PREVENTIVO"
    df_correctivo["tipo_revision"] = "CORRECTIVO"
    df_preventivo["empresa_id"] = empresa_id
    df_correctivo["empresa_id"] = empresa_id

    df = pd.concat([df_preventivo, df_correctivo], ignore_index=True, sort=False)

    if "ot_apertura_timestamp" in df.columns:
        df["fecha_evento"] = pd.to_datetime(
            df["ot_apertura_timestamp"],
            unit="s",
            errors="coerce",
        )
    else:
        df["fecha_evento"] = pd.NaT

    return df


def create_base_dataframe(
    df: pd.DataFrame,
    executed_only: bool = True,
) -> pd.DataFrame:
    """Crea la base consolidada usada por el resto de la pipeline."""

    base_df = df.copy()

    if "fecha_evento" not in base_df.columns:
        base_df["fecha_evento"] = pd.NaT

    base_df["fecha_evento"] = pd.to_datetime(base_df["fecha_evento"], errors="coerce")

    if executed_only and "estado" in base_df.columns:
        base_df = base_df.loc[base_df["estado"].eq(EXECUTED_STATUS)].copy()

    for column in SORT_COLUMNS:
        if column not in base_df.columns:
            base_df[column] = pd.NA

    base_df = base_df.sort_values(SORT_COLUMNS, kind="stable").reset_index(drop=True)
    return base_df


def create_eventos_dataframe(base_df: pd.DataFrame) -> pd.DataFrame:
    """Agrupa correctivos por bus y día para formar eventos técnicos únicos."""

    correctivos = base_df.loc[base_df["tipo_revision"].eq("CORRECTIVO")].copy()

    required_columns = ["placa_patente", "fecha_evento", *EVENT_AGGREGATIONS.keys()]
    for column in required_columns:
        if column not in correctivos.columns:
            correctivos[column] = pd.NA

    correctivos["fecha_evento"] = pd.to_datetime(correctivos["fecha_evento"], errors="coerce")
    correctivos = correctivos.dropna(subset=["placa_patente", "fecha_evento"]).copy()
    correctivos["fecha_dia"] = correctivos["fecha_evento"].dt.date

    eventos_df = (
        correctivos.groupby(["placa_patente", "fecha_dia"], as_index=False)
        .agg(EVENT_AGGREGATIONS)
        .sort_values(["placa_patente", "fecha_evento"], kind="stable")
        .reset_index(drop=True)
    )

    eventos_df["dias_desde_correctivo_anterior"] = (
        eventos_df.groupby("placa_patente")["fecha_evento"]
        .diff()
        .dt.total_seconds()
        .div(60 * 60 * 24)
    )

    return eventos_df

