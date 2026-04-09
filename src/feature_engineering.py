"""Transformations that build predictive variables from technical events."""

from __future__ import annotations

import re
from collections.abc import Iterable
from typing import Any

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
    """Replicate the bus-level statistics used by the original notebook."""

    bus_stats = df.groupby("placa_patente").agg(
        {
            "dias_desde_correctivo_anterior": ["mean", "std", "min", "max"],
            "correctivos_previos": ["max"],
        }
    )
    bus_stats.columns = ["_".join(column) for column in bus_stats.columns]

    return df.merge(bus_stats, left_on="placa_patente", right_index=True, how="left")


def _prepare_event_dataframe(eventos_df: pd.DataFrame) -> pd.DataFrame:
    """Keep a stable event ordering before generating rolling features."""

    features_df = eventos_df.copy()
    features_df["fecha_evento"] = pd.to_datetime(features_df["fecha_evento"], errors="coerce")
    features_df = features_df.dropna(subset=["placa_patente", "fecha_evento"]).copy()
    features_df = features_df.sort_values(["placa_patente", "fecha_evento"], kind="stable")
    return features_df


def _sanitize_feature_name(value: Any) -> str:
    """Convert category values into safe column suffixes."""

    sanitized = re.sub(r"[^a-z0-9]+", "_", str(value).lower()).strip("_")
    return sanitized or "missing"


def _rolling_sum_by_bus(
    df: pd.DataFrame,
    source_column: str,
    new_column: str,
    window_days: int,
) -> pd.DataFrame:
    """Add a time-based rolling sum by bus without altering row order."""

    indexed = df.set_index("fecha_evento")
    rolled = (
        indexed.groupby("placa_patente")[source_column]
        .rolling(f"{window_days}D")
        .sum()
        .reset_index(level=0, drop=True)
    )
    df[new_column] = rolled.to_numpy()
    return df


def _rolling_unique_count_by_bus(
    df: pd.DataFrame,
    value_column: str,
    window_days: int,
) -> pd.Series:
    """Compute rolling unique counts for a categorical column."""

    result = pd.Series(index=df.index, dtype="float64")

    for _, group in df.groupby("placa_patente", sort=False):
        dates = group["fecha_evento"].reset_index(drop=True)
        values = group[value_column].fillna("MISSING").astype(str).reset_index(drop=True)
        counts: list[int] = []

        for position in range(len(group)):
            start_time = dates.iloc[position] - pd.Timedelta(days=window_days)
            start_position = int(dates.searchsorted(start_time, side="left"))
            counts.append(len(set(values.iloc[start_position : position + 1])))

        result.loc[group.index] = counts

    return result


def _days_since_last_true_event(
    df: pd.DataFrame,
    flag_column: str,
    new_column: str,
) -> pd.DataFrame:
    """Measure elapsed days since the previous event where a flag was active."""

    values = pd.Series(index=df.index, dtype="float64")

    for _, group in df.groupby("placa_patente", sort=False):
        last_true_date = pd.NaT
        distances: list[float] = []

        for _, row in group.iterrows():
            current_date = row["fecha_evento"]
            if pd.isna(last_true_date):
                distances.append(float("nan"))
            else:
                distances.append((current_date - last_true_date).total_seconds() / (60 * 60 * 24))

            if bool(row.get(flag_column, 0)):
                last_true_date = current_date

        values.loc[group.index] = distances

    df[new_column] = values
    return df


def _streak_length(series: pd.Series) -> pd.Series:
    """Count consecutive identical values in a sorted series."""

    change_points = series.ne(series.shift()).cumsum()
    return series.groupby(change_points).cumcount().add(1)


def _positive_flag_streak(series: pd.Series) -> pd.Series:
    """Count consecutive positive flags and reset to zero otherwise."""

    streak_values: list[int] = []
    running = 0

    for value in series.fillna(0).astype(int):
        if value:
            running += 1
        else:
            running = 0
        streak_values.append(running)

    return pd.Series(streak_values, index=series.index)


def _add_category_window_counts(
    df: pd.DataFrame,
    category_column: str,
    prefix: str,
    windows: Iterable[int],
    top_k: int = 5,
) -> pd.DataFrame:
    """Create rolling counts for the most frequent categories of a column."""

    if category_column not in df.columns:
        return df

    categories = (
        df[category_column]
        .fillna("MISSING")
        .astype(str)
        .loc[lambda series: ~series.isin(["MISSING", "OTHER"])]
        .value_counts()
        .head(top_k)
        .index
    )

    for category in categories:
        safe_category = _sanitize_feature_name(category)
        flag_column = f"__{prefix}_{safe_category}_flag"
        df[flag_column] = df[category_column].eq(category).astype(int)

        for window in windows:
            df = _rolling_sum_by_bus(
                df,
                flag_column,
                f"count_{prefix}_{safe_category}_ult_{window}d",
                window_days=window,
            )

        df = df.drop(columns=[flag_column])

    return df


def generate_rolling_features(
    eventos_df: pd.DataFrame,
    windows: Iterable[int] = DEFAULT_WINDOWS,
) -> pd.DataFrame:
    """Generate baseline history, rolling windows and bus-level statistics."""

    features_df = _prepare_event_dataframe(eventos_df)

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


def generate_cause_based_features(
    eventos_df: pd.DataFrame,
    count_windows: Iterable[int] = DEFAULT_WINDOWS,
    diversity_window: int = 30,
) -> pd.DataFrame:
    """Create recurrence, diversity and rolling count features for causes."""

    features_df = _prepare_event_dataframe(eventos_df)
    cause_column = (
        "causa_origen_grouped"
        if "causa_origen_grouped" in features_df.columns
        else "causa_origen"
        if "causa_origen" in features_df.columns
        else None
    )

    if cause_column is None:
        return features_df

    features_df["dias_desde_ultima_misma_causa"] = (
        features_df.groupby(["placa_patente", cause_column])["fecha_evento"]
        .diff()
        .dt.total_seconds()
        .div(60 * 60 * 24)
    )
    features_df["racha_misma_causa"] = (
        features_df.groupby("placa_patente", sort=False)[cause_column]
        .transform(_streak_length)
    )
    features_df[f"num_causas_distintas_ult_{diversity_window}d"] = _rolling_unique_count_by_bus(
        features_df,
        cause_column,
        window_days=diversity_window,
    )

    return _add_category_window_counts(
        features_df,
        cause_column,
        prefix="causa",
        windows=count_windows,
        top_k=5,
    )


def generate_system_features(
    eventos_df: pd.DataFrame,
    windows: Iterable[int] = (7, 30),
) -> pd.DataFrame:
    """Create rolling features from system-like operational categories."""

    features_df = _prepare_event_dataframe(eventos_df)

    for category_column, prefix, top_k in (
        ("sistema_componente_grouped", "sistema", 5),
        ("taller_planta_grouped", "taller", 5),
        ("unidad_negocio_norm", "unidad", 5),
    ):
        if category_column in features_df.columns and features_df[category_column].nunique(dropna=True) > 1:
            features_df = _add_category_window_counts(
                features_df,
                category_column,
                prefix=prefix,
                windows=windows,
                top_k=top_k,
            )

    if "taller_planta_grouped" in features_df.columns:
        features_df["dias_desde_ultimo_mismo_taller"] = (
            features_df.groupby(["placa_patente", "taller_planta_grouped"])["fecha_evento"]
            .diff()
            .dt.total_seconds()
            .div(60 * 60 * 24)
        )
        features_df["racha_mismo_taller"] = (
            features_df.groupby("placa_patente", sort=False)["taller_planta_grouped"]
            .transform(_streak_length)
        )

    return features_df


def generate_inventory_features(
    eventos_df: pd.DataFrame,
    windows: Iterable[int] = (7, 30),
) -> pd.DataFrame:
    """Create rolling features from repuestos, durations and management counts."""

    features_df = _prepare_event_dataframe(eventos_df)

    for source_column, prefix in (
        ("repuestos_count_evento", "repuestos_count"),
        ("repuestos_cantidad_total_evento", "repuestos_cantidad_total"),
        ("uuid_gestion_count_evento", "uuid_gestion_count"),
        ("filas_correctivo_evento", "filas_correctivo"),
    ):
        if source_column in features_df.columns:
            filled_column = f"__{source_column}_filled"
            features_df[filled_column] = pd.to_numeric(features_df[source_column], errors="coerce").fillna(0)
            for window in windows:
                features_df = _rolling_sum_by_bus(
                    features_df,
                    filled_column,
                    f"{prefix}_ult_{window}d",
                    window_days=window,
                )
            features_df = features_df.drop(columns=[filled_column])

    if "tiene_repuestos_evento" in features_df.columns:
        features_df = _days_since_last_true_event(
            features_df,
            "tiene_repuestos_evento",
            "dias_desde_ultimo_evento_con_repuestos",
        )
        features_df["racha_eventos_con_repuestos"] = (
            features_df.groupby("placa_patente", sort=False)["tiene_repuestos_evento"]
            .transform(_positive_flag_streak)
        )

    if "duracion_ot_horas_prom_evento" in features_df.columns:
        filled_duration = "__duracion_ot_horas_prom_evento_filled"
        features_df[filled_duration] = pd.to_numeric(
            features_df["duracion_ot_horas_prom_evento"],
            errors="coerce",
        ).fillna(0)
        for window in windows:
            features_df = _rolling_sum_by_bus(
                features_df,
                filled_duration,
                f"duracion_ot_horas_prom_evento_ult_{window}d",
                window_days=window,
            )
        features_df = features_df.drop(columns=[filled_duration])

    return features_df


def generate_text_pattern_features(
    eventos_df: pd.DataFrame,
    windows: Iterable[int] = (7, 30),
) -> pd.DataFrame:
    """Create binary and rolling count features from textual technical keywords."""

    features_df = _prepare_event_dataframe(eventos_df)
    keyword_columns = [column for column in features_df.columns if column.startswith("keyword_")]

    if "num_keywords_tecnicos_evento" in features_df.columns:
        filled_count = "__num_keywords_tecnicos_evento_filled"
        features_df[filled_count] = pd.to_numeric(
            features_df["num_keywords_tecnicos_evento"],
            errors="coerce",
        ).fillna(0)
        for window in windows:
            features_df = _rolling_sum_by_bus(
                features_df,
                filled_count,
                f"num_keywords_tecnicos_ult_{window}d",
                window_days=window,
            )
        features_df = features_df.drop(columns=[filled_count])

    for keyword_column in keyword_columns:
        if keyword_column not in features_df.columns:
            continue

        features_df[keyword_column] = pd.to_numeric(features_df[keyword_column], errors="coerce").fillna(0)
        for window in windows:
            features_df = _rolling_sum_by_bus(
                features_df,
                keyword_column,
                f"count_{keyword_column}_ult_{window}d",
                window_days=window,
            )

    for keyword_column in ("keyword_motor", "keyword_freno", "keyword_bateria"):
        if keyword_column in features_df.columns:
            features_df = _days_since_last_true_event(
                features_df,
                keyword_column,
                f"dias_desde_ultimo_{keyword_column}",
            )

    return features_df


def create_temporal_features(df: pd.DataFrame) -> pd.DataFrame:
    """Add simple temporal variables derived from ``fecha_evento``."""

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
    """Create binary future-corrective targets using the original logic."""

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


def summarize_feature_quality(
    df: pd.DataFrame,
    target_windows: Iterable[int] = DEFAULT_WINDOWS,
) -> pd.DataFrame:
    """Return a compact validation summary for the feature dataset."""

    summary_rows: list[dict[str, Any]] = [
        {"check": "rows", "value": len(df)},
        {"check": "columns", "value": df.shape[1]},
        {
            "check": "duplicate_bus_timestamp",
            "value": int(df.duplicated(subset=["placa_patente", "fecha_evento"]).sum())
            if {"placa_patente", "fecha_evento"}.issubset(df.columns)
            else 0,
        },
        {
            "check": "negative_dias_desde_correctivo_anterior",
            "value": int(
                pd.to_numeric(df.get("dias_desde_correctivo_anterior"), errors="coerce")
                .lt(0)
                .fillna(False)
                .sum()
            )
            if "dias_desde_correctivo_anterior" in df.columns
            else 0,
        },
    ]

    for window in target_windows:
        column = f"correctivo_prox_{window}d"
        if column in df.columns:
            summary_rows.append(
                {
                    "check": f"positives_{column}",
                    "value": int(df[column].fillna(False).astype(bool).sum()),
                }
            )

    return pd.DataFrame(summary_rows)
