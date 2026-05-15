#!/usr/bin/env python3
"""Descarga las 4 colecciones desde Firestore con paginación y guarda dataset limpio local.

Uso:
    python scripts/download_firestore.py

Genera:
    data/raw/firestore/            — JSONs crudos por colección (para reuso)
    data/processed/base.parquet    — Dataset normalizado (142K+ registros)
"""
from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parent.parent))

from src.data_loader import (
    _init_firebase,
    normalize_inspection_records,
)
from src.preprocessing import (
    clean_data,
    create_base_dataframe,
    extract_additional_fields,
)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw"
FIREBASE_RAW_DIR = RAW_DIR / "firestore"
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"

COLLECTIONS = {
    "mantenimiento_correctivo": "correctivos",
    "mantenimiento_preventivo": "preventivos",
    "estado_general": "estado_general",
    "inspeccion_tecnica": "inspeccion_tecnica",
}

CHUNK_SIZE = 500


def download_collection(db, col_name: str, label: str) -> list:
    """Download ALL docs from a Firestore collection using cursor pagination."""
    print(f"  Descargando {col_name} ({label})...")
    all_docs: list = []
    last_doc = None
    t0 = time.time()

    while True:
        q = db.collection(col_name)
        if last_doc:
            q = q.start_after(last_doc)
        batch = list(q.limit(CHUNK_SIZE).stream())
        if not batch:
            break
        for doc in batch:
            all_docs.append([doc.id, doc.to_dict()])
            last_doc = doc
        if len(all_docs) % 5000 == 0:
            elapsed = time.time() - t0
            print(f"    {len(all_docs)} docs... ({elapsed:.0f}s)")

    elapsed = time.time() - t0
    print(f"    Total: {len(all_docs)} docs ({elapsed:.1f}s)")
    return all_docs


def save_raw_json(all_docs: list, label: str):
    """Save raw Firestore records as JSON for re-use."""
    path = FIREBASE_RAW_DIR / f"{label}.json"
    with open(path, "w") as f:
        json.dump(all_docs, f, default=str)
    print(f"    Guardado: {path}")


def normalize_and_merge(raw_data: dict) -> pd.DataFrame:
    """Normalize all 4 collections and merge into a single base DataFrame."""
    frames: list[pd.DataFrame] = []

    # CORRECTIVO + PREVENTIVO
    prev_raw = raw_data.get("preventivos", [])
    corr_raw = raw_data.get("correctivos", [])
    if prev_raw or corr_raw:
        print(f"  Normalizando CORR ({len(corr_raw)}) + PREV ({len(prev_raw)})...")
        df = clean_data(prev_raw, corr_raw, empresa_id="ALL")
        df = extract_additional_fields(df)
        if "unidad_negocio" in df.columns:
            df["empresa_id"] = df["unidad_negocio"].map(
                {"14": "VOY", "15": "VOY",
                 "11": "REDBUS", "13": "REDBUS",
                 "8": "METROPOL", "9": "METROPOL",
                 "16": "GRANAMERICAS",
                 "17": "CONECTA", "19": "CONECTA"}
            ).fillna("OTROS")
        frames.append(df)
        print(f"    → {len(df)} registros")

    # REGB
    regb_raw = raw_data.get("estado_general", [])
    if regb_raw:
        print(f"  Normalizando REGB ({len(regb_raw)})...")
        regb_df = normalize_inspection_records(regb_raw, "REGB")
        regb_df = extract_additional_fields(regb_df)
        regb_df["empresa_id"] = "ALL"
        frames.append(regb_df)
        print(f"    → {len(regb_df)} registros")

    # IT
    it_raw = raw_data.get("inspeccion_tecnica", [])
    if it_raw:
        print(f"  Normalizando IT ({len(it_raw)})...")
        it_df = normalize_inspection_records(it_raw, "IT")
        it_df = extract_additional_fields(it_df)
        it_df["empresa_id"] = "ALL"
        frames.append(it_df)
        print(f"    → {len(it_df)} registros")

    base_df = pd.concat(frames, ignore_index=True, sort=False)
    base_df = create_base_dataframe(base_df, executed_only=True)

    # Fix list columns for parquet compat
    for col in base_df.select_dtypes(include=["object"]).columns:
        if base_df[col].apply(lambda x: isinstance(x, list)).any():
            base_df[col] = base_df[col].apply(
                lambda x: json.dumps(x, default=str) if isinstance(x, list) else x
            )

    return base_df


def main():
    print("=" * 60)
    print("DESCARGA DESDE FIRESTORE")
    print("=" * 60)

    FIREBASE_RAW_DIR.mkdir(parents=True, exist_ok=True)
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    # ── Download ─────────────────────────────────────────────────
    from firebase_admin import firestore

    app = _init_firebase()
    db = firestore.client()

    raw_data: dict[str, list] = {}
    for col_name, label in COLLECTIONS.items():
        docs = download_collection(db, col_name, label)
        raw_data[label] = docs
        save_raw_json(docs, label)

    # ── Normalize & merge ────────────────────────────────────────
    print(f"\n  Normalizando y combinando...")
    base_df = normalize_and_merge(raw_data)

    # ── Save ────────────────────────────────────────────────────
    # Fix any remaining object-dtype columns with mixed types
    for col in base_df.select_dtypes(include=["object"]).columns:
        try:
            base_df[col].astype(str)
        except (ValueError, TypeError):
            base_df[col] = base_df[col].astype(str)

    path = PROCESSED_DIR / "base.parquet"
    base_df.to_parquet(path, index=False)
    print(f"\n  Dataset guardado: {path}")
    print(f"  Shape: {base_df.shape}")
    print(f"  Buses: {base_df['placa_patente'].nunique()}")
    print(f"  Tipos: {base_df['tipo_servicio'].value_counts().to_dict()}")
    print(f"  Rango: {base_df['fecha_evento'].min()} → {base_df['fecha_evento'].max()}")
    print("\n  ✅ Completo. Ahora puedes ejecutar el pipeline con --local-dataset")


if __name__ == "__main__":
    main()
