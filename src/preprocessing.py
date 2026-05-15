"""Cleaning and consolidation helpers for the historical maintenance base."""

from __future__ import annotations

import re
import unicodedata
from typing import Any, Iterable

import pandas as pd

from .data_loader import flatten_firebase, flatten_nested_json_fields

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
DEFAULT_CATEGORY_TOP_K = {
    "causa_origen": 5,
    "sistema_componente": 5,
    "taller_planta": 8,
    "pauta_ejecutada": 10,
    "pauta_proyectada": 10,
    "unidad_negocio": 5,
    "unidad_servicio": 5,
    "user_name": 10,
    "lugar_inspeccion": 8,
    "obs_inspeccion": 5,
    "representante_op": 5,
}
KEYWORD_PATTERNS = {
    "keyword_motor": ("MOTOR",),
    "keyword_freno": ("FRENO", "PASTILLA", "CALIPER"),
    "keyword_bateria": ("BATERIA",),
    "keyword_puerta": ("PUERTA",),
    "keyword_aceite": ("ACEITE",),
    "keyword_vidrio": ("VIDRIO",),
    "keyword_correa": ("CORREA",),
    "keyword_refrigerante": ("REFRIGERANTE",),
    "keyword_sensor": ("SENSOR",),
    "keyword_espejo": ("ESPEJO",),
}

CAUSA_SISTEMA_REGEX = {

    "FRENOS": [
        r"\bFRENO[S]?\b",
        r"\bPASTILLA[S]?\b",
        r"\bDISCO[S]?\b",
        r"\bABS\b"
    ],

    "SUSPENSION": [
        r"\bAMORTIGUADOR(ES)?\b",
        r"\bSUSPENSION\b",
        r"\bBUJE[S]?\b"
    ],

    "PUERTAS": [
        r"\bPUERTA[S]?\b",
    ],

    "CARROCERIA": [
        r"\bVIDRIO[S]?\b",
        r"\bPARACHOQUE[S]?\b"
    ],

    "CLIMATIZACION": [
        r"\bAIRE\b",
        r"\bVENTILADOR\b",
        r"\bEVAPORADOR\b"
    ],

    "RUEDAS": [
        r"\bNEUMATICO[S]?\b",
        r"\bLLANTA[S]?\b"
    ],

    "MOTOR": [
        r"\bMOTOR\b",
        r"\bACEITE\b",
        r"\bCORREA[S]?\b",
        r"\bVALVULA[S]?\b"
    ],

    "ELECTRICO": [
        r"\bBATERIA[S]?\b",
        r"\bALTERNADOR\b",
        r"\bFUSIBLE[S]?\b",
        r"\bSENSOR(ES)?\b"
    ]

}

def _reconstruct_causa_sistema(df: pd.DataFrame) -> pd.DataFrame:
    """
    Reconstruye causa técnica desde múltiples columnas de texto.
    """

    text_columns = [

        "observacion_clean",
        "repuestos_descripcion_texto_clean",
        "uuid_gestion_texto_clean",
        "pauta_ejecutada_clean",
        "pauta_proyectada_clean",
        "causa_origen_clean"

    ]

    existing = [

        col for col in text_columns
        if col in df.columns

    ]

    if not existing:
        df["causa_sistema_reconstruida"] = "OTROS"
        return df

    df["texto_total_causa"] = (

        df[existing]
        .fillna("")
        .astype(str)
        .agg(" ".join, axis=1)

    )

    def detectar(texto):

        scores = {}

        for categoria, patrones in CAUSA_SISTEMA_REGEX.items():

            score = 0

            for patron in patrones:

                score += len(
                    re.findall(patron, texto)
                )

            scores[categoria] = score

        mejor = max(scores, key=scores.get)

        if scores[mejor] == 0:
            return "OTROS"

        return mejor

    df["causa_sistema_reconstruida"] = (

        df["texto_total_causa"]
        .apply(detectar)

    )

    return df


def _filter_deleted(records: Iterable[Any]) -> list[Any]:
    """Discard records marked as deleted."""

    filtered_records: list[Any] = []
    for record in records:
        if not isinstance(record, (list, tuple)) or len(record) != 2:
            continue

        _, payload = record
        estado = payload.get("estado") if isinstance(payload, dict) else None
        if estado != DELETED_STATUS:
            filtered_records.append(record)

    return filtered_records


def _normalize_text_value(value: Any) -> str:
    """Normalize free text and categorical strings into a stable representation."""

    if pd.isna(value):
        return "MISSING"

    text = str(value).replace("\n", " ").replace("\r", " ")
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")
    text = text.upper()
    text = re.sub(r"[^A-Z0-9+ ]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text or "MISSING"


def _group_rare_categories(series: pd.Series, top_k: int) -> pd.Series:
    """Collapse infrequent categories into ``OTHER`` while preserving ``MISSING``."""

    normalized = series.fillna("MISSING").astype(str)
    frequent_values = normalized.loc[normalized.ne("MISSING")].value_counts().nlargest(top_k).index
    return normalized.where(normalized.isin(frequent_values) | normalized.eq("MISSING"), "OTHER")


def _extract_pauta_model(text: Any) -> str:
    """Extract a compact technical family from the pauta text."""

    normalized = _normalize_text_value(text)
    if normalized in {"MISSING", "VARIOS"}:
        return normalized

    for label in ("FOTON", "O500", "B8RLE", "ZK6128", "ZK6890"):
        if label in normalized:
            return label

    km_match = re.search(r"(\d{4,6}KM)", normalized)
    if km_match:
        return km_match.group(1)

    tokens = normalized.split()
    return tokens[1] if len(tokens) > 1 else normalized


def _extract_pauta_program(text: Any) -> str:
    """Extract a pauta stage token such as ``SM4`` or ``20000KM``."""

    normalized = _normalize_text_value(text)
    if normalized in {"MISSING", "VARIOS"}:
        return normalized

    program_match = re.search(r"(SM\d(?:\+SART\d+)?)", normalized)
    if program_match:
        return program_match.group(1)

    sart_match = re.search(r"(SART\d+)", normalized)
    if sart_match:
        return sart_match.group(1)

    km_match = re.search(r"(\d{4,6}KM)", normalized)
    if km_match:
        return km_match.group(1)

    if normalized.endswith(" S"):
        return "S"

    return "GENERAL"


def _split_pipe_text(value: Any) -> list[str]:
    """Split pipe-delimited aggregated text into clean unique-friendly tokens."""

    if pd.isna(value):
        return []

    items = [_normalize_text_value(item) for item in str(value).split("|")]
    return [item for item in items if item != "MISSING"]


def _first_non_missing(series: pd.Series) -> Any:
    """Return the first non-null, non-empty value from a series."""

    for value in series:
        if pd.isna(value):
            continue
        if isinstance(value, str) and not value.strip():
            continue
        return value
    return pd.NA


def _numeric_series(group: pd.DataFrame, column: str) -> pd.Series:
    """Read numeric columns safely when optional columns may be absent."""

    if column not in group.columns:
        return pd.Series(0, index=group.index, dtype="float64")
    return pd.to_numeric(group[column], errors="coerce")


def clean_textual_fields(df: pd.DataFrame) -> pd.DataFrame:
    """Create cleaned text columns without replacing the original values."""

    text_df = df.copy()
    text_columns = [
        "observacion",
        "repuestos_descripcion_texto",
        "repuestos_codigo_texto",
        "repuestos_tipo_texto",
        "repuestos_marca_texto",
        "uuid_gestion_texto",
        "insumos_texto",
        "pauta_ejecutada",
        "pauta_proyectada",
        "causa_origen",
        "sistema_componente",
        "taller_planta",
        "user_name",
        "obs_inspeccion",
        "lugar_inspeccion",
        "unidad_servicio",
        "representante_op",
    ]

    for column in text_columns:
        if column in text_df.columns:
            text_df[f"{column}_clean"] = text_df[column].apply(_normalize_text_value)

    return text_df


def normalize_categorical_columns(
    df: pd.DataFrame,
    top_k_map: dict[str, int] | None = None,
) -> pd.DataFrame:
    """Add normalized and grouped categorical columns."""

    normalized_df = df.copy()
    top_k_map = dict(DEFAULT_CATEGORY_TOP_K if top_k_map is None else top_k_map)

    for column, top_k in top_k_map.items():
        if column not in normalized_df.columns:
            continue

        norm_column = f"{column}_norm"
        grouped_column = f"{column}_grouped"
        normalized_df[norm_column] = normalized_df[column].apply(_normalize_text_value)
        normalized_df[grouped_column] = _group_rare_categories(normalized_df[norm_column], top_k=top_k)

    pauta_source = None
    if "pauta_ejecutada" in normalized_df.columns:
        pauta_source = normalized_df["pauta_ejecutada"].where(
            normalized_df["pauta_ejecutada"].notna()
            & normalized_df["pauta_ejecutada"].astype(str).str.strip().ne(""),
            normalized_df.get("pauta_proyectada"),
        )
    elif "pauta_proyectada" in normalized_df.columns:
        pauta_source = normalized_df["pauta_proyectada"]

    if pauta_source is not None:
        normalized_df["pauta_modelo_norm"] = pauta_source.apply(_extract_pauta_model)
        normalized_df["pauta_modelo_grouped"] = _group_rare_categories(
            normalized_df["pauta_modelo_norm"],
            top_k=8,
        )
        normalized_df["pauta_programa_norm"] = pauta_source.apply(_extract_pauta_program)
        normalized_df["pauta_programa_grouped"] = _group_rare_categories(
            normalized_df["pauta_programa_norm"],
            top_k=8,
        )

    return normalized_df


def _add_keyword_indicators(df: pd.DataFrame, source_column: str) -> pd.DataFrame:
    """Create binary technical keyword indicators from cleaned text."""

    keyword_df = df.copy()
    if source_column not in keyword_df.columns:
        return keyword_df

    source_series = keyword_df[source_column].fillna("MISSING").astype(str)
    for column_name, patterns in KEYWORD_PATTERNS.items():
        keyword_df[column_name] = source_series.apply(
            lambda value: int(any(pattern in value for pattern in patterns))
        )

    keyword_df["num_keywords_tecnicos"] = keyword_df[list(KEYWORD_PATTERNS)].sum(axis=1)
    return keyword_df


def extract_additional_fields(df: pd.DataFrame) -> pd.DataFrame:
    """Extend the cleaned base with nested JSON summaries and normalized columns."""

    enriched_df = df.copy()

    # Map inspection place → taller_planta for REGB/IT
    if "lugar_inspeccion" in enriched_df.columns and "taller_planta" not in enriched_df.columns:
        enriched_df["taller_planta"] = enriched_df["lugar_inspeccion"]
    elif "lugar_inspeccion" in enriched_df.columns and "taller_planta" in enriched_df.columns:
        enriched_df["taller_planta"] = enriched_df["taller_planta"].fillna(enriched_df["lugar_inspeccion"])

    # Map inspection obs → observacion
    if "obs_inspeccion" in enriched_df.columns and "observacion" not in enriched_df.columns:
        enriched_df["observacion"] = enriched_df["obs_inspeccion"]
    elif "obs_inspeccion" in enriched_df.columns and "observacion" in enriched_df.columns:
        enriched_df["observacion"] = enriched_df["observacion"].fillna(enriched_df["obs_inspeccion"])

    enriched_df = flatten_nested_json_fields(enriched_df)

    timestamp_columns = [
        "fecha_creacion_timestamp",
        "fecha_actualizacion_timestamp",
        "fecha_proyectada_timestamp",
        "ot_apertura_timestamp",
        "ot_cierre_timestamp",
    ]
    for column in timestamp_columns:
        if column in enriched_df.columns:
            enriched_df[f"{column}_dt"] = pd.to_datetime(
                enriched_df[column],
                unit="s",
                errors="coerce",
            )

    if {"ot_apertura_timestamp", "ot_cierre_timestamp"}.issubset(enriched_df.columns):
        enriched_df["duracion_ot_horas"] = (
            pd.to_numeric(enriched_df["ot_cierre_timestamp"], errors="coerce")
            .sub(pd.to_numeric(enriched_df["ot_apertura_timestamp"], errors="coerce"))
            .div(3600)
        )

    if {"fecha_creacion_timestamp", "ot_apertura_timestamp"}.issubset(enriched_df.columns):
        enriched_df["horas_desde_creacion_hasta_apertura"] = (
            pd.to_numeric(enriched_df["ot_apertura_timestamp"], errors="coerce")
            .sub(pd.to_numeric(enriched_df["fecha_creacion_timestamp"], errors="coerce"))
            .div(3600)
        )

    if {"fecha_proyectada_timestamp", "ot_apertura_timestamp"}.issubset(enriched_df.columns):
        enriched_df["dias_desfase_proyectado_vs_apertura"] = (
            pd.to_numeric(enriched_df["ot_apertura_timestamp"], errors="coerce")
            .sub(pd.to_numeric(enriched_df["fecha_proyectada_timestamp"], errors="coerce"))
            .div(60 * 60 * 24)
        )

    if {"km_diferencia", "frecuencia"}.issubset(enriched_df.columns):
        enriched_df["km_desviacion_relativa"] = (
            pd.to_numeric(enriched_df["km_diferencia"], errors="coerce")
            .div(pd.to_numeric(enriched_df["frecuencia"], errors="coerce"))
        )

    if {"km_ejecucion", "km_proyectado"}.issubset(enriched_df.columns):
        enriched_df["km_ratio_ejecucion_vs_proyectado"] = (
            pd.to_numeric(enriched_df["km_ejecucion"], errors="coerce")
            .div(pd.to_numeric(enriched_df["km_proyectado"], errors="coerce"))
        )

    enriched_df = clean_textual_fields(enriched_df)
    enriched_df = normalize_categorical_columns(enriched_df)
    enriched_df = _add_keyword_indicators(enriched_df, "repuestos_descripcion_texto_clean")
    enriched_df = _reconstruct_causa_sistema(enriched_df) 
    for source, target in (
        ("repuestos_count", "tiene_repuestos"),
        ("uuid_gestion_count", "tiene_uuid_gestion"),
        ("insumos_count", "tiene_insumos"),
    ):
        if source in enriched_df.columns:
            enriched_df[target] = (
                pd.to_numeric(enriched_df[source], errors="coerce")
                .fillna(0)
                .gt(0)
                .astype(int)
            )

    return enriched_df


def summarize_data_quality(
    df: pd.DataFrame,
    timestamp_columns: list[str] | None = None,
    categorical_columns: list[str] | None = None,
    duplicate_subset: list[str] | None = None,
) -> pd.DataFrame:
    """Return a compact, notebook-friendly data quality summary."""

    timestamp_columns = [
        column
        for column in (
            timestamp_columns
            or [
                "fecha_evento",
                "ot_apertura_timestamp_dt",
                "ot_cierre_timestamp_dt",
                "fecha_creacion_timestamp_dt",
                "fecha_proyectada_timestamp_dt",
            ]
        )
        if column in df.columns
    ]
    categorical_columns = [
        column
        for column in (
            categorical_columns
            or [
                "causa_origen_grouped",
                "sistema_componente_grouped",
                "taller_planta_grouped",
                "pauta_modelo_grouped",
                "pauta_programa_grouped",
            ]
        )
        if column in df.columns
    ]

    quality_rows: list[dict[str, Any]] = [
        {"check": "rows", "value": len(df)},
        {"check": "columns", "value": df.shape[1]},
    ]

    for column in timestamp_columns:
        quality_rows.append(
            {
                "check": f"missing_{column}",
                "value": int(pd.to_datetime(df[column], errors="coerce").isna().sum()),
            }
        )

    if duplicate_subset:
        quality_rows.append(
            {
                "check": f"duplicates_{'_'.join(duplicate_subset)}",
                "value": int(df.duplicated(subset=duplicate_subset).sum()),
            }
        )

    for column in categorical_columns:
        quality_rows.append(
            {
                "check": f"unique_{column}",
                "value": int(df[column].fillna("MISSING").astype(str).nunique()),
            }
        )

    return pd.DataFrame(quality_rows)


SIGNIFICANT_FAILURE_CATEGORIES = frozenset({
    "FRENOS", "MOTOR", "ELECTRICO", "CLIMATIZACION", "RUEDAS", "SUSPENSION", "PUERTAS",
})

FAILURE_TIPO_MAP: dict[str, str] = {
    "CORRECTIVO": "always",
    "PREVENTIVO": "resultado_bool",
    "REGB": "highs_gt_0",
    "IT": "highs_gt_0",
}


def is_failure_event(row: pd.Series | dict[str, Any]) -> bool:
    """Determine if a record represents a failure event based on its tipo.

    - CORRECTIVO: a failure only if ``causa_sistema_reconstruida`` is in
      ``SIGNIFICANT_FAILURE_CATEGORIES`` (FRENOS, MOTOR, ELECTRICO,
      CLIMATIZACION, RUEDAS, SUSPENSION, PUERTAS).
    - PREVENTIVO: failure if ``resultado`` is False/0 (rechazado)
    - REGB / IT: failure if ``resultado_pasa == 0`` (rechazado) OR
      ``inspeccion_total_highs > 0`` (defectos criticos)
    """
    tipo = str(row.get("tipo_servicio", row.get("tipo_revision", ""))).upper()
    if tipo == "CORRECTIVO":
        causa = str(row.get("causa_sistema_reconstruida", "")).upper()
        return causa in SIGNIFICANT_FAILURE_CATEGORIES
    if tipo == "PREVENTIVO":
        res = row.get("resultado")
        if isinstance(res, bool):
            return not res
        if isinstance(res, (int, float)):
            return bool(res) is False or res == 0
        return False
    if tipo in ("REGB", "IT"):
        resultado_pasa = row.get("resultado_pasa", 1)
        if resultado_pasa is None:
            resultado_pasa = 1
        resultado_pasa = int(resultado_pasa)
        has_highs = int(row.get("inspeccion_total_highs", 0) or 0) > 0
        return resultado_pasa == 0 or has_highs
    return False


def clean_data(
    preventivo_raw: Iterable[Any],
    correctivo_raw: Iterable[Any],
    regb_df: pd.DataFrame | None = None,
    it_df: pd.DataFrame | None = None,
    empresa_id: str = DEFAULT_EMPRESA_ID,
) -> pd.DataFrame:
    """Normalize all data sources and create the shared ``fecha_evento`` field."""

    preventivo_filtered = _filter_deleted(preventivo_raw)
    correctivo_filtered = _filter_deleted(correctivo_raw)

    df_preventivo = flatten_firebase(preventivo_filtered)
    df_correctivo = flatten_firebase(correctivo_filtered)

    if df_preventivo.empty:
        df_preventivo = pd.DataFrame(columns=["firebase_id"])
    if df_correctivo.empty:
        df_correctivo = pd.DataFrame(columns=["firebase_id"])

    df_preventivo["tipo_servicio"] = "PREVENTIVO"
    df_correctivo["tipo_servicio"] = "CORRECTIVO"
    df_preventivo["empresa_id"] = empresa_id
    df_correctivo["empresa_id"] = empresa_id

    frames = [df_preventivo, df_correctivo]

    if regb_df is not None and not regb_df.empty:
        regb_df = regb_df.copy()
        regb_df["empresa_id"] = empresa_id
        frames.append(regb_df)

    if it_df is not None and not it_df.empty:
        it_df = it_df.copy()
        it_df["empresa_id"] = empresa_id
        frames.append(it_df)

    df = pd.concat(frames, ignore_index=True, sort=False)

    # Set fecha_evento from ot_apertura_timestamp for CORR/PREV
    mask_std = df["tipo_servicio"].isin(["CORRECTIVO", "PREVENTIVO"])
    if "ot_apertura_timestamp" in df.columns:
        df.loc[mask_std, "fecha_evento"] = pd.to_datetime(
            df.loc[mask_std, "ot_apertura_timestamp"],
            unit="s",
            errors="coerce",
        )

    # Also ensure tipo_revision exists for backward compatibility
    df["tipo_revision"] = df["tipo_servicio"]

    return df


def create_base_dataframe(
    df: pd.DataFrame,
    executed_only: bool = True,
) -> pd.DataFrame:
    """Create the consolidated base used by the rest of the pipeline."""

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
    """Create technical events from ALL tipos, keeping separate events per day.

    Each (bus, day, tipo_servicio) combination becomes its own event.
    Also computes ``dias_desde_evento_anterior`` and marks ``es_falla``.
    """

    eventos = base_df.copy()

    required_columns = ["placa_patente", "fecha_evento"]
    for column in required_columns:
        if column not in eventos.columns:
            eventos[column] = pd.NA

    eventos["fecha_evento"] = pd.to_datetime(eventos["fecha_evento"], errors="coerce")
    eventos = eventos.dropna(subset=["placa_patente", "fecha_evento"]).copy()
    eventos["fecha_dia"] = eventos["fecha_evento"].dt.date

    # Keep per-tipo rows separate (same bus/day/tipo → aggregate)
    eventos["es_falla"] = eventos.apply(is_failure_event, axis=1)

    # Build aggregations dynamically (only for columns that exist)
    EVENT_AGGREGATIONS_ALL = dict(EVENT_AGGREGATIONS)
    EVENT_AGGREGATIONS_ALL["tipo_servicio"] = "first"
    EVENT_AGGREGATIONS_ALL["es_falla"] = "max"
    for col in ["resultado_pasa"]:
        if col in eventos.columns:
            EVENT_AGGREGATIONS_ALL[col] = "max"

    eventos_df = (
        eventos.groupby(["placa_patente", "fecha_dia", "tipo_servicio"], as_index=False)
        .agg({k: v for k, v in EVENT_AGGREGATIONS_ALL.items() if k in eventos.columns})
        .sort_values(["placa_patente", "tipo_servicio", "fecha_evento"], kind="stable")
        .reset_index(drop=True)
    )

    # Collapse identical-timestamp duplicates (data artifact: CORR+PREV con misma hora)
    dups_before = eventos_df.duplicated(subset=["placa_patente", "fecha_evento"], keep=False).sum()
    before_count = len(eventos_df)
    eventos_df = eventos_df.drop_duplicates(
        subset=["placa_patente", "fecha_evento"], keep="first"
    ).reset_index(drop=True)
    dup_removed = before_count - len(eventos_df)
    if dup_removed > 0:
        print(f"    Colapsadas {dup_removed} filas duplicadas (mismo bus+timestamp) → {len(eventos_df)} filas")

    eventos_df["dias_desde_evento_anterior"] = (
        eventos_df.groupby("placa_patente")["fecha_evento"]
        .diff()
        .dt.total_seconds()
        .div(60 * 60 * 24)
    )

    return eventos_df


def _aggregate_event_group(group: pd.DataFrame) -> pd.Series:
    """Build additional event-level summaries from enriched corrective rows."""

    repuesto_codes: set[str] = set()
    repuesto_descriptions: set[str] = set()
    uuid_values: set[str] = set()
    insumo_values: set[str] = set()

    for value in group.get("repuestos_codigo_texto", pd.Series(dtype=object)):
        repuesto_codes.update(_split_pipe_text(value))
    for value in group.get("repuestos_descripcion_texto", pd.Series(dtype=object)):
        repuesto_descriptions.update(_split_pipe_text(value))
    for value in group.get("uuid_gestion_texto", pd.Series(dtype=object)):
        uuid_values.update(_split_pipe_text(value))
    for value in group.get("insumos_texto", pd.Series(dtype=object)):
        insumo_values.update(_split_pipe_text(value))

    aggregated: dict[str, Any] = {
        "filas_correctivo_evento": int(len(group)),
        "uuid_gestion_count_evento": int(_numeric_series(group, "uuid_gestion_count").fillna(0).sum()),
        "uuid_gestion_unique_count_evento": len(uuid_values),
        "insumos_count_evento": int(_numeric_series(group, "insumos_count").fillna(0).sum()),
        "insumos_unique_count_evento": len(insumo_values),
        "repuestos_count_evento": int(_numeric_series(group, "repuestos_count").fillna(0).sum()),
        "repuestos_cantidad_total_evento": int(
            _numeric_series(group, "repuestos_cantidad_total").fillna(0).sum()
        ),
        "repuestos_codigos_unicos_evento": len(repuesto_codes),
        "repuestos_descripciones_unicas_evento": len(repuesto_descriptions),
        "duracion_ot_horas_prom_evento": _numeric_series(group, "duracion_ot_horas").mean(),
        "duracion_ot_horas_max_evento": _numeric_series(group, "duracion_ot_horas").max(),
        "horas_desde_creacion_hasta_apertura_prom_evento": _numeric_series(
            group,
            "horas_desde_creacion_hasta_apertura",
        ).mean(),
        "km_desviacion_relativa_prom_evento": _numeric_series(
            group,
            "km_desviacion_relativa",
        ).mean(),
        "repuestos_descripcion_texto_evento_clean": " | ".join(sorted(repuesto_descriptions)),
        "repuestos_codigo_texto_evento": " | ".join(sorted(repuesto_codes)),
    }

    categorical_columns = [
        "causa_origen_norm",
        "causa_origen_grouped",
        "sistema_componente_norm",
        "sistema_componente_grouped",
        "taller_planta_norm",
        "taller_planta_grouped",
        "pauta_ejecutada_norm",
        "pauta_ejecutada_grouped",
        "pauta_modelo_norm",
        "pauta_modelo_grouped",
        "pauta_programa_norm",
        "pauta_programa_grouped",
        "user_name_norm",
        "unidad_negocio_norm",
    ]
    for column in categorical_columns:
        if column in group.columns:
            aggregated[column] = _first_non_missing(group[column])

    for keyword_column in KEYWORD_PATTERNS:
        if keyword_column in group.columns:
            aggregated[keyword_column] = int(_numeric_series(group, keyword_column).fillna(0).max())

    aggregated["tiene_repuestos_evento"] = int(aggregated["repuestos_count_evento"] > 0)
    aggregated["tiene_uuid_gestion_evento"] = int(aggregated["uuid_gestion_count_evento"] > 0)
    aggregated["num_keywords_tecnicos_evento"] = int(
        sum(aggregated.get(keyword_column, 0) for keyword_column in KEYWORD_PATTERNS)
    )

    return pd.Series(aggregated)


def _aggregate_inspection_group(group: pd.DataFrame) -> pd.Series:
    """Aggregate REGB/IT specific fields per (bus, day) group."""
    aggregated: dict[str, Any] = {
        "inspeccion_total_highs_evento": int(
            pd.to_numeric(group.get("inspeccion_total_highs", pd.Series(0)), errors="coerce").fillna(0).max()
        ),
        "inspeccion_total_mediums_evento": int(
            pd.to_numeric(group.get("inspeccion_total_mediums", pd.Series(0)), errors="coerce").fillna(0).max()
        ),
        "inspeccion_total_lows_evento": int(
            pd.to_numeric(group.get("inspeccion_total_lows", pd.Series(0)), errors="coerce").fillna(0).max()
        ),
    }

    no_presentado = group.get("obs_inspeccion", pd.Series("")).fillna("").astype(str).str.contains(
        "BUS NO PRESENTADO", case=False, na=False
    ).any()
    aggregated["flag_no_presentado"] = int(no_presentado)

    # Count how many grupos (system groups) in IT inspections
    group_vals = group.get("group", pd.Series(dtype=object))
    all_groups: list[str] = []
    for gv in group_vals:
        if isinstance(gv, list):
            all_groups.extend(str(x) for x in gv)
    aggregated["num_sistemas_inspeccionados"] = len(set(all_groups))
    aggregated["sistemas_inspeccionados_texto"] = " | ".join(sorted(set(all_groups)))

    return pd.Series(aggregated)


def merge_additional_event_fields(
    base_df: pd.DataFrame,
    eventos_df: pd.DataFrame,
) -> pd.DataFrame:
    """Merge additive event-level fields from all tipos onto eventos."""

    enriched_events = eventos_df.copy()

    # Aggregate CORRECTIVO fields (existing logic)
    correctivos = base_df.loc[base_df["tipo_servicio"].eq("CORRECTIVO")].copy()
    if not correctivos.empty and "fecha_evento" in correctivos.columns:
        correctivos["fecha_evento"] = pd.to_datetime(correctivos["fecha_evento"], errors="coerce")
        correctivos = correctivos.dropna(subset=["placa_patente", "fecha_evento"]).copy()
        correctivos["fecha_dia"] = correctivos["fecha_evento"].dt.date

        corr_fields = (
            correctivos.groupby(["placa_patente", "fecha_dia"])
            .apply(_aggregate_event_group)
            .reset_index()
        )
        enriched_events["fecha_evento"] = pd.to_datetime(enriched_events["fecha_evento"], errors="coerce")
        enriched_events["fecha_dia"] = enriched_events["fecha_evento"].dt.date
        enriched_events = enriched_events.merge(
            corr_fields,
            on=["placa_patente", "fecha_dia"],
            how="left",
        )
        enriched_events = enriched_events.drop(columns=["fecha_dia"])

    # Aggregate REGB/IT fields
    inspections = base_df[base_df["tipo_servicio"].isin(["REGB", "IT"])].copy()
    if not inspections.empty and "fecha_evento" in inspections.columns:
        inspections["fecha_evento"] = pd.to_datetime(inspections["fecha_evento"], errors="coerce")
        inspections = inspections.dropna(subset=["placa_patente", "fecha_evento"]).copy()
        inspections["fecha_dia"] = inspections["fecha_evento"].dt.date

        insp_fields = (
            inspections.groupby(["placa_patente", "fecha_dia"])
            .apply(_aggregate_inspection_group)
            .reset_index()
        )
        enriched_events["fecha_evento"] = pd.to_datetime(enriched_events["fecha_evento"], errors="coerce")
        enriched_events["fecha_dia"] = enriched_events["fecha_evento"].dt.date
        enriched_events = enriched_events.merge(
            insp_fields,
            on=["placa_patente", "fecha_dia"],
            how="left",
        )
        enriched_events = enriched_events.drop(columns=["fecha_dia"])

    return enriched_events
