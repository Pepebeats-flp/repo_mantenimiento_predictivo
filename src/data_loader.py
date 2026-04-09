"""Funciones de carga para los JSON originales del proyecto."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Iterable

import pandas as pd


def load_json_files(
    preventivo_path: str | Path,
    correctivo_path: str | Path,
) -> tuple[list[Any], list[Any]]:
    """Carga los archivos JSON originales y devuelve sus listas de registros."""

    preventivo_path = Path(preventivo_path)
    correctivo_path = Path(correctivo_path)

    with preventivo_path.open("r", encoding="utf-8") as file:
        preventivo_raw = json.load(file)

    with correctivo_path.open("r", encoding="utf-8") as file:
        correctivo_raw = json.load(file)

    return preventivo_raw, correctivo_raw


def flatten_firebase(records: Iterable[Any]) -> pd.DataFrame:
    """Convierte registros tipo Firebase ``[doc_id, payload]`` en un DataFrame plano."""

    rows: list[dict[str, Any]] = []

    for record in records:
        if not isinstance(record, (list, tuple)) or len(record) != 2:
            continue

        doc_id, data = record
        payload = dict(data) if isinstance(data, dict) else {}
        payload["firebase_id"] = doc_id
        rows.append(payload)

    return pd.DataFrame(rows)


def _ensure_list(value: Any) -> list[Any]:
    """Devuelve listas seguras para columnas anidadas."""

    return value if isinstance(value, list) else []


def _normalize_nested_text(value: Any) -> str:
    """Normaliza valores anidados a texto utilizable."""

    if value is None:
        return ""

    text = str(value).replace("\n", " ").replace("\r", " ")
    text = re.sub(r"\s+", " ", text).strip().upper()
    return text


def _summarize_nested_list(value: Any) -> dict[str, Any]:
    """Resume listas simples como conteos y texto consolidado."""

    values = [_normalize_nested_text(item) for item in _ensure_list(value)]
    values = [item for item in values if item]

    return {
        "count": len(values),
        "unique_count": len(set(values)),
        "text": " | ".join(sorted(set(values))),
    }


def _summarize_repuestos(value: Any) -> dict[str, Any]:
    """Resume la lista anidada de repuestos en columnas limpias."""

    items = [item for item in _ensure_list(value) if isinstance(item, dict)]
    codes: list[str] = []
    descriptions: list[str] = []
    item_types: list[str] = []
    brands: list[str] = []
    quantity_total = 0

    for item in items:
        code = _normalize_nested_text(item.get("repuesto_codigo"))
        description = _normalize_nested_text(item.get("repuesto_descripcion"))
        item_type = _normalize_nested_text(item.get("repuesto_tipo"))
        brand = _normalize_nested_text(item.get("repuesto_marca"))
        quantity = item.get("repuesto_cantidad", 0)

        if code:
            codes.append(code)
        if description:
            descriptions.append(description)
        if item_type:
            item_types.append(item_type)
        if brand:
            brands.append(brand)

        if pd.notna(quantity):
            try:
                quantity_total += int(float(quantity))
            except (TypeError, ValueError):
                continue

    return {
        "repuestos_count": len(items),
        "repuestos_cantidad_total": quantity_total,
        "repuestos_codigos_unicos": len(set(codes)),
        "repuestos_descripciones_unicas": len(set(descriptions)),
        "repuestos_original_count": sum(item_type == "ORIGINAL" for item_type in item_types),
        "repuestos_marca_count": len(brands),
        "repuestos_codigo_texto": " | ".join(sorted(set(codes))),
        "repuestos_descripcion_texto": " | ".join(sorted(set(descriptions))),
        "repuestos_tipo_texto": " | ".join(sorted(set(item_types))),
        "repuestos_marca_texto": " | ".join(sorted(set(brands))),
    }


def flatten_nested_json_fields(df: pd.DataFrame) -> pd.DataFrame:
    """Extrae columnas limpias desde listas y objetos anidados del JSON."""

    flat_df = df.copy()

    if "uuid_gestion" in flat_df.columns:
        uuid_summary = flat_df["uuid_gestion"].apply(_summarize_nested_list).apply(pd.Series)
        uuid_summary = uuid_summary.rename(
            columns={
                "count": "uuid_gestion_count",
                "unique_count": "uuid_gestion_unique_count",
                "text": "uuid_gestion_texto",
            }
        )
        flat_df = pd.concat([flat_df, uuid_summary], axis=1)

    if "insumos" in flat_df.columns:
        insumo_summary = flat_df["insumos"].apply(_summarize_nested_list).apply(pd.Series)
        insumo_summary = insumo_summary.rename(
            columns={
                "count": "insumos_count",
                "unique_count": "insumos_unique_count",
                "text": "insumos_texto",
            }
        )
        flat_df = pd.concat([flat_df, insumo_summary], axis=1)

    if "repuestos" in flat_df.columns:
        repuesto_summary = flat_df["repuestos"].apply(_summarize_repuestos).apply(pd.Series)
        flat_df = pd.concat([flat_df, repuesto_summary], axis=1)

    return flat_df

