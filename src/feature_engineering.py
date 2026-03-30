"""Transformaciones para construir variables predictivas a partir de eventos."""

from __future__ import annotations

from collections.abc import Iterable

import pandas as pd

DEFAULT_WINDOWS = (7, 5, 3)
DEFAULT_FEATURE_COLUMNS = [
    "dias_desde_correctivo_anterior",
    "correctivos_previos",
    "correctivos_ult_7d",
    "correctivos_ult_5d",
    "correctivos_ult_3d",
    "dias_desde_correctivo_anterior_mean",
    "dias_desde_correctivo_anterior_std",
    "dias_desde_correctivo_anterior_min",
    "dias_desde_correctivo_anterior_max",
    "correctivos_previos_max",
    "mes",
    "dia_semana",
    "fin_mes",
]


def _add_bus_statistics(df: pd.DataFrame) -> pd.DataFrame:
    """Replica las estadísticas por bus usadas en el notebook original."""

    bus_stats = df.groupby("placa_patente").agg(
        {
            "dias_desde_correctivo_anterior": ["mean", "std", "min", "max"],
            "correctivos_previos": ["max"],
        }
    )
    bus_stats.columns = ["_".join(column) for column in bus_stats.columns]

    return df.merge(bus_stats, left_on="placa_patente", right_index=True, how="left")


def generate_rolling_features(
    eventos_df: pd.DataFrame,
    windows: Iterable[int] = DEFAULT_WINDOWS,
) -> pd.DataFrame:
    """Genera features de historial, ventanas móviles y estadísticas por bus."""

    features_df = eventos_df.copy()
    features_df["fecha_evento"] = pd.to_datetime(features_df["fecha_evento"], errors="coerce")
    features_df = features_df.dropna(subset=["placa_patente", "fecha_evento"]).copy()
    features_df = features_df.sort_values(["placa_patente", "fecha_evento"], kind="stable")

    features_df["dias_desde_correctivo_anterior"] = (
        features_df.groupby("placa_patente")["fecha_evento"]
        .diff()
        .dt.total_seconds()
        .div(60 * 60 * 24)
    )
    features_df["correctivos_previos"] = features_df.groupby("placa_patente").cumcount()

    rolling_df = features_df.set_index("fecha_evento")
    for window in windows:
        rolling_df[f"correctivos_ult_{window}d"] = (
            rolling_df.groupby("placa_patente")["placa_patente"]
            .rolling(f"{window}D")
            .count()
            .reset_index(level=0, drop=True)
        )
    features_df = rolling_df.reset_index()

    features_df = _add_bus_statistics(features_df)
    return features_df


def create_temporal_features(df: pd.DataFrame) -> pd.DataFrame:
    """Agrega variables temporales simples derivadas de ``fecha_evento``."""

    temporal_df = df.copy()
    temporal_df["fecha_evento"] = pd.to_datetime(temporal_df["fecha_evento"], errors="coerce")
    temporal_df["mes"] = temporal_df["fecha_evento"].dt.month
    temporal_df["dia_semana"] = temporal_df["fecha_evento"].dt.dayofweek
    temporal_df["fin_mes"] = temporal_df["fecha_evento"].dt.day.ge(25).astype(int)

    return temporal_df


def create_future_targets(
    df: pd.DataFrame,
    windows: Iterable[int] = DEFAULT_WINDOWS,
) -> pd.DataFrame:
    """Crea los targets binarios de correctivo próximo usando la lógica original."""

    target_df = df.copy()
    target_df["fecha_evento"] = pd.to_datetime(target_df["fecha_evento"], errors="coerce")
    target_df = target_df.sort_values(["placa_patente", "fecha_evento"], kind="stable").copy()

    next_delta_days = (
        target_df.groupby("placa_patente")["fecha_evento"]
        .shift(-1)
        .sub(target_df["fecha_evento"])
        .dt.days
    )

    for window in windows:
        target_df[f"correctivo_prox_{window}d"] = next_delta_days.le(window).fillna(False)

    return target_df
