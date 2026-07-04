#!/usr/bin/env python3
"""Fetch fresh data from Firestore → rebuild base.parquet
Descarga solo registros nuevos desde el ultimo conocido.

Uso: python3 scripts/refresh_data.py
"""
from __future__ import annotations

import gc
import json
import os
import sys
from pathlib import Path

import pandas as pd

SRC_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(SRC_DIR))

from src.data_loader import _init_firebase, normalize_inspection_records
from src.preprocessing import clean_data, create_base_dataframe, extract_additional_fields
from scripts.analytics.operational import enrich_system_labels

SERVICE_ACCOUNT = SRC_DIR / "slared-4de9d5a1e961.json"
RAW_DIR = SRC_DIR / "data" / "raw"
OUTPUT = SRC_DIR / "data" / "processed" / "base.parquet"

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(SERVICE_ACCOUNT)

CLIENT_MAP = {
    "14": "VOY", "15": "VOY",
    "11": "REDBUS", "13": "REDBUS",
    "8": "METROPOL", "9": "METROPOL",
    "16": "GRANAMERICAS",
    "17": "CONECTA", "19": "CONECTA",
}

COLLECTIONS = {
    "correctivos": ("mantenimiento_correctivo", "ot_apertura_timestamp"),
    "preventivos": ("mantenimiento_preventivo", "ot_apertura_timestamp"),
    "estado_general": ("estado_general", "fecha_inicio_timestamp"),
    "inspeccion_tecnica": ("inspeccion_tecnica", "fecha_inicio_timestamp"),
}


class FirestoreEncoder(json.JSONEncoder):
    def default(self, obj):
        try:
            if hasattr(obj, "isoformat"):
                return obj.isoformat()
            return str(obj)
        except Exception:
            return str(obj)


def to_serializable(val):
    if isinstance(val, dict):
        return {k: to_serializable(v) for k, v in val.items()}
    if isinstance(val, list):
        return [to_serializable(v) for v in val]
    if isinstance(val, pd.Timestamp):
        return val.isoformat()
    return val


def load_json(path: Path) -> list:
    if path.exists():
        return json.loads(path.read_text())
    return []


def save_json(path: Path, records: list):
    path.write_text(json.dumps(to_serializable(records), cls=FirestoreEncoder))
    print(f"  Saved {path} ({len(records)} records)")


def find_last_timestamp(records: list, field: str) -> float:
    max_ts = 0.0
    for item in records:
        if isinstance(item, list) and len(item) == 2:
            val = item[1].get(field, 0) if isinstance(item[1], dict) else 0
        elif isinstance(item, dict):
            val = item.get(field, 0)
        else:
            continue
        if isinstance(val, (int, float)) and val > max_ts:
            max_ts = val
    return max_ts


def fetch_new(collection: str, ts_field: str, min_ts: float) -> list:
    """Fetch records with timestamp >= min_ts, batched."""
    from firebase_admin import firestore

    _init_firebase()
    db = firestore.client()

    records = []
    query = (
        db.collection(collection)
        .where(ts_field, ">=", min_ts)
        .order_by(ts_field)
        .limit(500)
    )  # noqa: E501; positional where is deprecated but works; newer SDK uses .where(field, op, value)

    while True:
        docs = list(query.stream(timeout=120))
        if not docs:
            break
        for doc in docs:
            records.append([doc.id, doc.to_dict()])
        query = query.start_after(docs[-1])
        print(f"  → {len(records)}...", end="\r")

    return records


def main():
    import datetime

    print("=== Cargando JSON locales y descargando nuevos ===")
    for name, (coll, ts_field) in COLLECTIONS.items():
        path = RAW_DIR / f"{name}.json"
        records = load_json(path)
        if not records:
            path2 = RAW_DIR / "firestore" / f"{name}.json"
            records = load_json(path2)

        last_ts = find_last_timestamp(records, ts_field)
        dt = datetime.datetime.fromtimestamp(last_ts) if last_ts else "N/A"
        print(f"  {name}: {len(records)} records locales, ultimo: {dt}")

        if last_ts == 0:
            print(f"  {name}: sin datos locales, saltando fetch incremental")
        else:
            print(f"  {name}: buscando desde ts={last_ts}")
            try:
                new_recs = fetch_new(coll, ts_field, last_ts)
                print(f"  {name}: {len(new_recs)} nuevos" + " " * 20)
                if new_recs:
                    existing_ids = {r[0] for r in records
                                    if isinstance(r, list) and len(r) == 2}
                    records = records + [r for r in new_recs if r[0] not in existing_ids]
                    save_json(path, records)
            except Exception as e:
                print(f"  {name}: ERROR {e}")

        del records
        gc.collect()

    # ── Rebuild parquet from saved files (load one at a time) ──
    print("\n=== Reconstruyendo base.parquet ===")

    regb_records = load_json(RAW_DIR / "estado_general.json")
    if not regb_records:
        regb_records = load_json(RAW_DIR / "firestore" / "estado_general.json")
    regb_df = normalize_inspection_records(regb_records, "REGB")
    del regb_records
    gc.collect()

    it_records = load_json(RAW_DIR / "inspeccion_tecnica.json")
    if not it_records:
        it_records = load_json(RAW_DIR / "firestore" / "inspeccion_tecnica.json")
    it_df = normalize_inspection_records(it_records, "IT")
    del it_records
    gc.collect()

    corr_records = load_json(RAW_DIR / "correctivos.json")
    if not corr_records:
        corr_records = load_json(RAW_DIR / "firestore" / "correctivos.json")

    prev_records = load_json(RAW_DIR / "preventivos.json")
    if not prev_records:
        prev_records = load_json(RAW_DIR / "firestore" / "preventivos.json")

    df = clean_data(prev_records, corr_records,
                    regb_df=regb_df, it_df=it_df, empresa_id="ALL")
    del prev_records, corr_records
    gc.collect()

    df = extract_additional_fields(df)

    if "unidad_negocio" in df.columns:
        df["empresa_id"] = df["unidad_negocio"].map(CLIENT_MAP).fillna("OTROS")

    base = create_base_dataframe(df, executed_only=True)
    base = enrich_system_labels(base)
    gc.collect()

    print(f"  {len(base)} registros, {base['placa_patente'].nunique()} buses")
    print(f"  Rango: {base['fecha_evento'].min()} → {base['fecha_evento'].max()}")

    save = base.copy()
    for col in save.select_dtypes(include=["object", "string"]).columns:
        if save[col].apply(lambda x: isinstance(x, list)).any():
            save[col] = save[col].apply(
                lambda x: json.dumps(x, default=str) if isinstance(x, list) else x
            )
        if save[col].apply(lambda x: hasattr(x, "isoformat") and not isinstance(x, (pd.Timestamp,))).any():
            save[col] = save[col].apply(
                lambda x: str(x) if hasattr(x, "isoformat") and not isinstance(x, (pd.Timestamp,)) else x
            )
    save.to_parquet(OUTPUT, index=False)
    del base, save
    gc.collect()
    print(f"\n✅ Guardado: {OUTPUT}")


if __name__ == "__main__":
    main()
