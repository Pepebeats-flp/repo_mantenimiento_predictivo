"""Transformations that build predictive variables from technical events."""

from __future__ import annotations

import re
from collections.abc import Iterable
from typing import Any

import numpy as np
import pandas as pd

DEFAULT_WINDOWS = (7, 5, 3)
EXCLUDED_FEATURE_COLUMNS = {
    "placa_patente", "fecha_evento", "fecha_dia", "empresa_id", "tipo_evento",
    "tipo_servicio", "tipo_revision", "es_falla", "severity",
    "causa_origen", "causa_origen_grouped", "causa_origen_norm",
    "sistema_componente", "sistema_componente_grouped", "sistema_componente_norm",
    "taller_planta", "taller_planta_grouped", "taller_planta_norm",
    "pauta_ejecutada_grouped", "pauta_ejecutada_norm",
    "pauta_modelo_grouped", "pauta_modelo_norm",
    "pauta_programa_grouped", "pauta_programa_norm",
    "unidad_negocio_norm", "user_name_norm",
    "unidad_servicio_norm", "unidad_servicio",
    "lugar_inspeccion_norm", "lugar_inspeccion",
    "representante_op_norm", "representante_op",
    "obs_inspeccion_norm", "obs_inspeccion",
    "repuestos_codigo_texto_evento", "repuestos_descripcion_texto_evento_clean",
    "inspeccion_total_highs", "inspeccion_total_mediums", "inspeccion_total_lows",
    "inspeccion_curr_highs", "inspeccion_curr_mediums", "inspeccion_curr_lows",
    "resultado_pasa",
    "group_texto", "sistemas_inspeccionados_texto",
    "repuestos_descripcion_texto_clean", "repuestos_descripcion_texto",
    "repuestos_codigo_texto", "repuestos_marca_texto", "repuestos_tipo_texto",
    "uuid_gestion_texto", "insumos_texto",
}


def get_feature_columns(df: pd.DataFrame) -> list[str]:
    target_cols = {c for c in df.columns if c.startswith("correctivo_prox_") or c.startswith("evento_falla_prox_")}
    return [
        c for c in df.columns
        if c not in EXCLUDED_FEATURE_COLUMNS
        and c not in target_cols
        and pd.api.types.is_numeric_dtype(df[c])
    ]


DEFAULT_FEATURE_COLUMNS = [
    "dias_desde_evento_anterior",
    "eventos_previos",
    "eventos_ult_7d",
    "eventos_ult_5d",
    "eventos_ult_3d",
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
    """Compute expanding (past-only) bus-level statistics to avoid leakage."""

    result = df.copy()
    result = result.sort_values(["placa_patente", "fecha_evento"])

    gap_col = "dias_desde_evento_anterior"
    if gap_col not in result.columns:
        gap_col = "dias_desde_correctivo_anterior"

    stats = (
        result.groupby("placa_patente")[gap_col]
        .expanding()
        .agg(["mean", "std", "min", "max"])
        .reset_index(level=0, drop=True)
    )
    stats.columns = [f"dias_desde_correctivo_anterior_{c}" for c in stats.columns]

    max_previos = (
        result.groupby("placa_patente")["eventos_previos"]
        .expanding()
        .max()
        .reset_index(level=0, drop=True)
    )
    stats["correctivos_previos_max"] = max_previos

    result = pd.concat([result, stats], axis=1)
    return result


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


def generate_bus_history_features(
    eventos_df: pd.DataFrame,
) -> pd.DataFrame:
    """Create bus-level historical features: lifetime corrective count, historical failure rate."""

    features_df = _prepare_event_dataframe(eventos_df)

    total_eventos = features_df.groupby("placa_patente").cumcount() + 1
    total_correctivos = features_df.groupby("placa_patente")["filas_correctivo_evento"].cumsum() if "filas_correctivo_evento" in features_df.columns else total_eventos
    features_df["total_eventos_hist"] = total_eventos
    features_df["total_correctivos_hist"] = total_correctivos
    features_df["tasa_correctivos_hist"] = (total_correctivos / total_eventos).fillna(0).clip(0, 1)

    if "es_falla" in features_df.columns:
        total_fallas = features_df.groupby("placa_patente")["es_falla"].cumsum()
        features_df["total_fallas_hist"] = total_fallas
        features_df["tasa_fallas_hist"] = (total_fallas / total_eventos).fillna(0).clip(0, 1)

    return features_df


def generate_rolling_features(
    eventos_df: pd.DataFrame,
    windows: Iterable[int] = DEFAULT_WINDOWS,
) -> pd.DataFrame:
    """Generate baseline history, rolling windows and bus-level statistics.

    Works across ALL event types. ``dias_desde_evento_anterior`` is
    pre-computed in ``create_eventos_dataframe``.
    """

    features_df = _prepare_event_dataframe(eventos_df)

    if "dias_desde_evento_anterior" not in features_df.columns:
        features_df["dias_desde_evento_anterior"] = (
            features_df.groupby("placa_patente")["fecha_evento"]
            .diff()
            .dt.total_seconds()
            .div(60 * 60 * 24)
        )

    features_df["eventos_previos"] = features_df.groupby("placa_patente").cumcount()

    rolling_df = features_df.set_index("fecha_evento")
    for window in windows:
        rolling_df[f"eventos_ult_{window}d"] = (
            rolling_df.groupby("placa_patente")["placa_patente"]
            .rolling(f"{window}D")
            .count()
            .reset_index(level=0, drop=True)
        )

        # Failure rate in window (corrective / total)
        if "es_falla" in rolling_df.columns:
            fail_count = (
                rolling_df.groupby("placa_patente")["es_falla"]
                .rolling(f"{window}D")
                .sum()
                .reset_index(level=0, drop=True)
            )
            total_count = rolling_df[f"eventos_ult_{window}d"]
            rolling_df[f"tasa_falla_ult_{window}d"] = (
                (fail_count / total_count).fillna(0).clip(0, 1)
            )

    features_df = rolling_df.reset_index()

    # Days since last failure
    if "es_falla" in features_df.columns:
        features_df = _days_since_last_true_event(
            features_df, "es_falla", "dias_desde_ultima_falla"
        )

    # Update bus statistics to use the new column name
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


def generate_event_type_features(
    eventos_df: pd.DataFrame,
    short_windows: Iterable[int] = (7, 30),
    long_windows: Iterable[int] = (60, 180),
) -> pd.DataFrame:
    """Create rolling counts and recency features per event type (CORRECTIVO, PREV, REGB, IT)."""

    features_df = _prepare_event_dataframe(eventos_df)

    tipo_col = "tipo_servicio"
    if tipo_col not in features_df.columns:
        return features_df

    tipos_presentes = features_df[tipo_col].dropna().unique()
    all_windows = sorted(set(list(short_windows) + list(long_windows)))

    for tipo in tipos_presentes:
        safe_tipo = _sanitize_feature_name(tipo)
        flag_col = f"__flag_{safe_tipo}"
        features_df[flag_col] = (features_df[tipo_col] == tipo).astype(int)

        for w in all_windows:
            features_df = _rolling_sum_by_bus(
                features_df,
                flag_col,
                f"count_{safe_tipo}_ult_{w}d",
                window_days=w,
            )

        # Days since last event of this type
        features_df = _days_since_last_true_event(
            features_df,
            flag_col,
            f"dias_desde_ultimo_{safe_tipo}",
        )

        features_df = features_df.drop(columns=[flag_col])

    return features_df


def generate_severity_features(
    eventos_df: pd.DataFrame,
    short_windows: Iterable[int] = (30,),
    long_windows: Iterable[int] = (90, 180),
) -> pd.DataFrame:
    """Create rolling severity sum features from REGB/IT inspections."""

    features_df = _prepare_event_dataframe(eventos_df)

    tipo_col = "tipo_servicio"
    severity_cols = [
        ("inspeccion_total_highs_evento", "highs"),
        ("inspeccion_total_mediums_evento", "mediums"),
        ("inspeccion_total_lows_evento", "lows"),
    ]

    for sev_col, sev_name in severity_cols:
        if sev_col not in features_df.columns:
            continue

        for tipo in ("REGB", "IT"):
            safe_tipo = _sanitize_feature_name(tipo)
            tipo_flag = (features_df.get(tipo_col) == tipo).astype(int)
            val_col = f"__sev_{safe_tipo}_{sev_name}"
            features_df[val_col] = (
                pd.to_numeric(features_df.get(sev_col, 0), errors="coerce").fillna(0) * tipo_flag
            )

            windows = long_windows if tipo == "IT" else short_windows
            for w in windows:
                features_df = _rolling_sum_by_bus(
                    features_df,
                    val_col,
                    f"{sev_name}_{safe_tipo}_ult_{w}d",
                    window_days=w,
                )

            features_df = features_df.drop(columns=[val_col])

    # Flag: no presentado
    if "flag_no_presentado" in features_df.columns:
        features_df["no_presentado_ult_30d"] = _rolling_sum_by_bus(
            features_df,
            "flag_no_presentado",
            "_no_presentado_30d_temp",
            window_days=30,
        )["_no_presentado_30d_temp"]
        features_df = features_df.drop(columns=["_no_presentado_30d_temp"])

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
    """Create binary future-failure targets.

    For each event, the target is 1 if the next **failure** event
    (``es_falla == True``) occurs within ``window`` days.
    Non-failure events still contribute features but their target is
    based on the next failure event after them.
    """

    target_df = df.copy()
    target_df["fecha_evento"] = pd.to_datetime(target_df["fecha_evento"], errors="coerce")
    target_df = target_df.sort_values(["placa_patente", "fecha_evento"], kind="stable").copy()

    if "es_falla" not in target_df.columns:
        target_df["es_falla"] = True  # fallback for backward compat

    # Get the next failure event date per bus
    fallas = target_df[target_df["es_falla"]].copy()
    fallas["prox_falla_fecha"] = (
        fallas.groupby("placa_patente")["fecha_evento"]
        .shift(-1)
    )

    # Merge next failure date back to all events
    fallas_map = fallas[["placa_patente", "fecha_evento", "prox_falla_fecha"]].copy()
    target_df = target_df.merge(
        fallas_map,
        on=["placa_patente", "fecha_evento"],
        how="left",
    )

    # Forward-fill: for events after a failure, use the same next-failure date
    target_df["prox_falla_fecha"] = target_df.groupby("placa_patente")["prox_falla_fecha"].bfill()

    next_delta_days = (
        target_df["prox_falla_fecha"]
        .sub(target_df["fecha_evento"])
        .dt.days
    )

    for window in windows:
        col_name = f"correctivo_prox_{window}d"
        target_df[col_name] = next_delta_days.le(window).fillna(False)

    target_df = target_df.drop(columns=["prox_falla_fecha"])
    target_df = target_df.drop(columns=["prox_falla_fecha_y"], errors="ignore")
    target_df = target_df.drop(columns=["prox_falla_fecha_x"], errors="ignore")

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
            "check": "negative_dias_desde_evento_anterior",
            "value": int(
                pd.to_numeric(df.get("dias_desde_evento_anterior"), errors="coerce")
                .lt(0)
                .fillna(False)
                .sum()
            )
            if "dias_desde_evento_anterior" in df.columns
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


def generate_trend_features(
    eventos_df: pd.DataFrame,
    windows: Iterable[int] = (7, 30, 60),
) -> pd.DataFrame:
    """Trend features removed (slow, low predictive value)."""
    return _prepare_event_dataframe(eventos_df)


def generate_bus_age_features(
    eventos_df: pd.DataFrame,
) -> pd.DataFrame:
    """Create bus age / life-cycle features from the event history."""
    features_df = _prepare_event_dataframe(eventos_df)

    bus_first_event = features_df.groupby("placa_patente")["fecha_evento"].transform("min")
    features_df["edad_bus_dias"] = (
        features_df["fecha_evento"] - bus_first_event
    ).dt.total_seconds().div(86400).fillna(0)

    features_df["total_eventos_vida"] = features_df.groupby("placa_patente").cumcount() + 1

    preventivo_flag = (
        features_df["tipo_servicio"].str.upper().eq("PREVENTIVO")
    ).astype(int) if "tipo_servicio" in features_df.columns else pd.Series(0, index=features_df.index)
    features_df["tiene_preventivo"] = preventivo_flag
    features_df = _days_since_last_true_event(
        features_df, "tiene_preventivo", "dias_desde_ultimo_preventivo"
    )

    return features_df


def generate_bus_day_features(
    eventos_df: pd.DataFrame,
    features_df: pd.DataFrame,
    target_date: pd.Timestamp | None = None,
    max_stale_days: int = 90,
) -> pd.DataFrame:
    """Generate one feature row per bus as of ``target_date`` using the latest
    event on or before that date.

    Args:
        eventos_df: Event-level DataFrame with placa_patente and fecha_evento.
        features_df: Full feature DataFrame from generate_* functions.
        target_date: Prediction date (default: now).
        max_stale_days: Max days since last event to consider a bus "active".

    Returns:
        DataFrame with one row per active bus, ready for inference.
    """
    if target_date is None:
        target_date = pd.Timestamp.now().normalize()

    feats = features_df.copy()
    feats["fecha_evento"] = pd.to_datetime(feats["fecha_evento"], errors="coerce")

    past = feats[feats["fecha_evento"] <= target_date].copy()
    if past.empty:
        return pd.DataFrame()

    latest_idx = past.groupby("placa_patente")["fecha_evento"].idxmax()
    bus_day = past.loc[latest_idx].reset_index(drop=True)

    bus_day["fecha_prediccion"] = target_date
    bus_day["prediction_date"] = target_date
    bus_day["dias_desde_ultimo_evento"] = (
        (target_date - bus_day["fecha_evento"]).dt.total_seconds() / 86400
    ).fillna(0)

    if max_stale_days:
        bus_day = bus_day[bus_day["dias_desde_ultimo_evento"] <= max_stale_days].copy()

    return bus_day
