import pandas as pd
import numpy as np


def compute_recurrence_stats(df, window_days=7):
    corr = df[df["tipo_servicio"] == "CORRECTIVO"].sort_values(
        ["placa_patente", "fecha_evento"]
    )
    corr = corr.copy()
    corr["prev_sistema"] = corr.groupby("placa_patente")["sistema_componente_grouped"].shift(1)
    corr["prev_fecha"] = corr.groupby("placa_patente")["fecha_evento"].shift(1)
    corr["dias_desde_prev"] = (
        (corr["fecha_evento"] - corr["prev_fecha"]).dt.total_seconds().div(86400)
    )
    corr["mismo_sistema"] = (
        corr["sistema_componente_grouped"] == corr["prev_sistema"]
    )
    corr["recurrencia"] = (
        (corr["dias_desde_prev"] <= window_days) & corr["mismo_sistema"]
    ).astype(int)

    stats = (
        corr.groupby("sistema_componente_grouped")
        .agg(
            total_correctivos=("recurrencia", "count"),
            recurrencias=("recurrencia", "sum"),
        )
        .reset_index()
    )
    stats["tasa_recurrencia"] = (stats["recurrencias"] / stats["total_correctivos"] * 100).round(1)
    stats = stats.sort_values("total_correctivos", ascending=False).head(20)
    return stats, corr


def flag_high_recurrence_buses(df, window_days=14, min_events=3):
    corr = df[df["tipo_servicio"] == "CORRECTIVO"].sort_values(
        ["placa_patente", "fecha_evento"]
    )
    corr = corr.copy()
    corr["prev_sistema"] = corr.groupby("placa_patente")["sistema_componente_grouped"].shift(1)
    corr["prev_fecha"] = corr.groupby("placa_patente")["fecha_evento"].shift(1)
    corr["dias_desde_prev"] = (
        (corr["fecha_evento"] - corr["prev_fecha"]).dt.total_seconds().div(86400)
    )
    corr["recurrencia_rapida"] = (
        (corr["dias_desde_prev"] <= window_days)
        & (corr["sistema_componente_grouped"] == corr["prev_sistema"])
    ).astype(int)

    bus_rec = (
        corr.groupby("placa_patente")
        .agg(
            total_correctivos=("recurrencia_rapida", "count"),
            recurrencias=("recurrencia_rapida", "sum"),
            ultimo_evento=("fecha_evento", "max"),
            sistemas=("sistema_componente_grouped", lambda x: x.mode().iloc[0] if len(x.mode()) > 0 else None),
        )
        .reset_index()
    )
    bus_rec["tasa_recurrencia"] = (
        bus_rec["recurrencias"] / bus_rec["total_correctivos"] * 100
    ).round(1)
    bus_rec = bus_rec[
        (bus_rec["total_correctivos"] >= min_events)
        & (bus_rec["tasa_recurrencia"] >= 50)
    ].sort_values("tasa_recurrencia", ascending=False)
    return bus_rec


def compute_terminal_recurrence(df, window_days=7):
    stats, corr = compute_recurrence_stats(df, window_days)
    corr_term = corr.copy()
    corr_term["terminal"] = corr_term["taller_planta_grouped"]
    corr_term = corr_term[corr_term["terminal"].notna()]

    term_rec = (
        corr_term.groupby("terminal")
        .agg(
            total=("recurrencia", "count"),
            recurrencias=("recurrencia", "sum"),
        )
        .reset_index()
    )
    term_rec["tasa"] = (term_rec["recurrencias"] / term_rec["total"] * 100).round(1)
    term_rec = term_rec.sort_values("tasa", ascending=False)
    return term_rec
