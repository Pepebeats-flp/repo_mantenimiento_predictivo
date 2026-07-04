import pandas as pd
import numpy as np


def compute_bus_event_frequency(df, window_days=30):
    cutoff = df["fecha_evento"].max() - pd.Timedelta(days=window_days)
    recent = df[df["fecha_evento"] >= cutoff]
    freq = recent.groupby("placa_patente").agg(
        eventos=("tipo_servicio", "count"),
        correctivos=("tipo_servicio", lambda x: (x == "CORRECTIVO").sum()),
        sistemas=("sistema_componente_grouped", "nunique"),
        ultimo_evento=("fecha_evento", "max"),
        taller_principal=("taller_planta_grouped", lambda x: x.mode().iloc[0] if len(x.mode()) > 0 else None),
    ).reset_index()
    freq["dias_desde_ultimo"] = (
        df["fecha_evento"].max() - freq["ultimo_evento"]
    ).dt.days
    freq["eventos_por_semana"] = (freq["eventos"] / window_days * 7).round(1)
    freq = freq.sort_values("eventos", ascending=False).reset_index(drop=True)
    freq["pct_rank"] = (freq["eventos"].rank(pct=True) * 100).round(1)
    return freq


def flag_anomalous_buses(freq_df, df, zscore_threshold=2.0):
    all_buses = df.groupby("placa_patente").size().reset_index(name="total_eventos")
    baseline = all_buses["total_eventos"].describe()
    p95 = all_buses["total_eventos"].quantile(0.95)
    median = baseline["50%"]

    recent_high = freq_df[freq_df["eventos_por_semana"] > 5].copy()
    recent_high["tipo_alerta"] = "alta_frecuencia"
    recent_high["razon"] = recent_high.apply(
        lambda r: f"{r['eventos_por_semana']} eventos/semana (últimos 30d)", axis=1
    )

    fresh = df.copy()
    fresh["fecha"] = fresh["fecha_evento"].dt.date
    last_week = df["fecha_evento"].max() - pd.Timedelta(days=7)
    this_week = df[fresh["fecha_evento"] >= last_week]
    prev_week = df[
        (fresh["fecha_evento"] >= last_week - pd.Timedelta(days=7))
        & (fresh["fecha_evento"] < last_week)
    ]
    tw_freq = this_week.groupby("placa_patente").size()
    pw_freq = prev_week.groupby("placa_patente").size()
    surge_buses = []
    for bus in tw_freq.index:
        tw = tw_freq.get(bus, 0)
        pw = pw_freq.get(bus, 0)
        if pw > 0 and tw / pw >= 3 and tw >= 3:
            surge_buses.append(
                {
                    "placa_patente": bus,
                    "tipo_alerta": "incremento_subito",
                    "semana_anterior": int(pw),
                    "semana_actual": int(tw),
                    "razon": f"incremento {pw}→{tw} eventos/semana ({int(tw/pw*100)}%)",
                }
            )
    surge_df = pd.DataFrame(surge_buses) if surge_buses else pd.DataFrame()

    alerts = pd.concat([recent_high, surge_df], ignore_index=True)
    if not alerts.empty:
        alerts = alerts.sort_values("eventos_por_semana" if "eventos_por_semana" in alerts.columns else "semana_actual", ascending=False)
    return alerts


def get_bus_timeline(bus, df):
    sub = df[df["placa_patente"] == bus].sort_values("fecha_evento")
    if sub.empty:
        return pd.DataFrame()
    sub = sub.copy()
    sub["fecha"] = sub["fecha_evento"].dt.date
    sub["gap_dias"] = (
        sub.groupby("placa_patente")["fecha_evento"].diff().dt.total_seconds().div(86400).round(1)
    )
    sub["es_correctivo"] = (sub["tipo_servicio"] == "CORRECTIVO").astype(int)
    cols = [
        "fecha_evento",
        "tipo_servicio",
        "sistema_componente_grouped",
        "taller_planta_grouped",
        "duracion_ot_horas",
        "km_ejecucion",
        "gap_dias",
    ]
    out = sub[cols].copy()
    out.columns = [
        "fecha",
        "tipo",
        "sistema",
        "taller",
        "duracion_h",
        "km",
        "gap_dias",
    ]
    return out
