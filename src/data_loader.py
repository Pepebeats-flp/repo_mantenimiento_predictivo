"""Funciones de carga para los JSON originales del proyecto y conexion directa a Firestore."""

from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

_firebase_app = None


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
        flat_df = flat_df.drop(columns=["uuid_gestion"])

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
        flat_df = flat_df.drop(columns=["insumos"])

    if "repuestos" in flat_df.columns:
        repuesto_summary = flat_df["repuestos"].apply(_summarize_repuestos).apply(pd.Series)
        flat_df = pd.concat([flat_df, repuesto_summary], axis=1)
        flat_df = flat_df.drop(columns=["repuestos"])

    # Drop raw resultado[] array (severity already extracted during normalize)
    if "resultado" in flat_df.columns:
        flat_df = flat_df.drop(columns=["resultado"])

    # Flatten filenames array of objects -> comma-separated names
    if "filenames" in flat_df.columns:
        flat_df["filenames"] = flat_df["filenames"].apply(
            lambda v: ", ".join(
                x.get("name", "") for x in (v or []) if isinstance(x, dict)
            ) if isinstance(v, list) else ""
        )
        flat_df["filenames_ori"] = flat_df["filenames_ori"].apply(
            lambda v: ", ".join(str(x) for x in (v or [])) if isinstance(v, list) else ""
        )

    # Summarize group[] array (IT inspection system groups)
    if "group" in flat_df.columns:
        group_summary = flat_df["group"].apply(_summarize_nested_list).apply(pd.Series)
        group_summary = group_summary.rename(
            columns={
                "count": "group_count",
                "unique_count": "group_unique_count",
                "text": "group_texto",
            }
        )
        flat_df = pd.concat([flat_df, group_summary], axis=1)
        flat_df = flat_df.drop(columns=["group"])

    return flat_df


def _init_firebase(credentials_path: str | Path | None = None):
    """Inicializa Firebase Admin SDK (singleton)."""
    global _firebase_app
    if _firebase_app is not None:
        return _firebase_app

    try:
        import firebase_admin
        from firebase_admin import credentials
    except ImportError:
        raise ImportError(
            "firebase-admin no instalado. Ejecuta: pip install firebase-admin"
        )

    cred_path = credentials_path or os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if cred_path:
        cred = credentials.Certificate(str(cred_path))
        _firebase_app = firebase_admin.initialize_app(cred)
    else:
        _firebase_app = firebase_admin.initialize_app()

    return _firebase_app


def load_single_json(path: str | Path) -> list[Any]:
    """Load a single JSON file of firebase-style ``[doc_id, payload]`` records."""
    path = Path(path)
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def extract_severity_from_resultado(resultado: Any) -> dict[str, Any]:
    """Extract severity totals and pass/fail from a ``resultado[]`` array (REGB/IT).

    ``resultado = 1`` (aprobado) or ``resultado = 0`` (rechazado) is derived
    from the ``certifica`` boolean inside the first array element.
    """
    items = _ensure_list(resultado)
    if not items:
        return {
            "inspeccion_total_highs": 0,
            "inspeccion_total_mediums": 0,
            "inspeccion_total_lows": 0,
            "inspeccion_curr_highs": 0,
            "inspeccion_curr_mediums": 0,
            "inspeccion_curr_lows": 0,
            "resultado_pasa": 0,
        }
    first = items[0] if isinstance(items[0], dict) else {}

    # Derive pass/fail: certifica=true → resultado=1 (aprobado)
    certifica = first.get("certifica")
    if isinstance(certifica, bool):
        resultado_pasa = 1 if certifica else 0
    else:
        resultado_pasa = 1  # assume approved if no explicit rejection

    return {
        "inspeccion_total_highs": int(first.get("total_highs", 0) or 0),
        "inspeccion_total_mediums": int(first.get("total_mediums", 0) or 0),
        "inspeccion_total_lows": int(first.get("total_lows", 0) or 0),
        "inspeccion_curr_highs": int(first.get("curr_highs", 0) or 0),
        "inspeccion_curr_mediums": int(first.get("curr_mediums", 0) or 0),
        "inspeccion_curr_lows": int(first.get("curr_lows", 0) or 0),
        "resultado_pasa": resultado_pasa,
    }


def normalize_inspection_records(
    records: Iterable[Any],
    tipo_servicio: str,
) -> pd.DataFrame:
    """Normalize REGB or IT Firestore records into the standard event schema.

    Maps ``fecha_inicio_timestamp`` → ``fecha_evento``, extracts severity
    from the ``resultado[]`` array, and sets ``tipo_servicio``.
    """
    rows: list[dict[str, Any]] = []
    for record in records:
        if not isinstance(record, (list, tuple)) or len(record) != 2:
            continue
        doc_id, data = record
        if not isinstance(data, dict):
            continue

        payload = dict(data)
        payload["firebase_id"] = doc_id
        payload["tipo_servicio"] = tipo_servicio

        # Map fecha_inicio_timestamp → fecha_evento
        ts = payload.get("fecha_inicio_timestamp")
        if ts is not None:
            try:
                payload["fecha_evento"] = pd.to_datetime(float(ts), unit="s")
            except (TypeError, ValueError):
                payload["fecha_evento"] = pd.NaT
        else:
            payload["fecha_evento"] = pd.NaT

        # Extract severity from resultado[]
        sev = extract_severity_from_resultado(payload.get("resultado"))
        payload.update(sev)

        # Map kilometer field
        km = payload.get("kilometraje")
        if km is not None:
            try:
                payload["km_ejecucion"] = int(float(km))
            except (TypeError, ValueError):
                pass

        rows.append(payload)

    return pd.DataFrame(rows)


def load_from_firestore(
    prev_collection: str = "mantenimiento_preventivo",
    corr_collection: str = "mantenimiento_correctivo",
    regb_collection: str = "estado_general",
    it_collection: str = "inspeccion_tecnica",
    credentials_path: str | Path | None = None,
) -> tuple[list[Any], list[Any], list[Any], list[Any]]:
    """Lee registros desde Firestore desde 4 colecciones.

    Devuelve ``(preventivo_raw, correctivo_raw, regb_raw, it_raw)`` en el
    mismo formato ``[doc_id, payload]``.
    """
    from firebase_admin import firestore

    _init_firebase(credentials_path)
    db = firestore.client()

    preventivo_raw: list[Any] = []
    correctivo_raw: list[Any] = []
    regb_raw: list[Any] = []
    it_raw: list[Any] = []

    for doc in db.collection(prev_collection).stream():
        preventivo_raw.append([doc.id, doc.to_dict()])

    for doc in db.collection(corr_collection).stream():
        correctivo_raw.append([doc.id, doc.to_dict()])

    for doc in db.collection(regb_collection).stream():
        regb_raw.append([doc.id, doc.to_dict()])

    for doc in db.collection(it_collection).stream():
        it_raw.append([doc.id, doc.to_dict()])

    return preventivo_raw, correctivo_raw, regb_raw, it_raw

