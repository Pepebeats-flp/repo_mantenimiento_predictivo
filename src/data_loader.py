"""Funciones de carga para los JSON originales del proyecto."""

from __future__ import annotations

import json
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

