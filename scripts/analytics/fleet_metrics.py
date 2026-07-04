import pandas as pd
import numpy as np


def compute_correctivo_rate_trend(df, months=12):
    df = df.copy()
    df["mes"] = df["fecha_evento"].dt.to_period("M")
    total = df.groupby("mes").size()
    corr = df[df["tipo_servicio"] == "CORRECTIVO"].groupby("mes").size()
    ratio = (corr / total * 100).tail(months).reset_index()
    ratio.columns = ["mes", "tasa_correctivo"]
    ratio["mes_str"] = ratio["mes"].astype(str)
    return ratio


def compute_system_failure_distribution(df, top_n=15):
    corr = df[df["tipo_servicio"] == "CORRECTIVO"]
    dist = (
        corr.groupby("sistema_componente_grouped")
        .size()
        .sort_values(ascending=False)
        .head(top_n)
        .reset_index()
    )
    dist.columns = ["sistema", "eventos"]
    dist["pct"] = (dist["eventos"] / dist["eventos"].sum() * 100).round(1)
    return dist


def compute_terminal_comparison(df):
    terminals = df["taller_planta_grouped"].value_counts().head(9).index.tolist()
    rows = []
    for term in terminals:
        sub = df[df["taller_planta_grouped"] == term]
        daily = sub.groupby(sub["fecha_evento"].dt.date).size()
        n_buses = sub["placa_patente"].nunique()
        corr_sub = sub[sub["tipo_servicio"] == "CORRECTIVO"]
        dur = corr_sub["duracion_ot_horas"].dropna()
        dur = dur[(dur > 0) & (dur < 168)]
        insp_sub = sub[sub["tipo_servicio"].isin(["REGB", "IT"])]
        pass_rate = (
            insp_sub["resultado_pasa"].mean() * 100
            if len(insp_sub) > 0
            else None
        )
        rows.append(
            {
                "terminal": term,
                "eventos_totales": len(sub),
                "buses_unicos": n_buses,
                "carga_diaria_media": round(daily.mean(), 1),
                "carga_diaria_max": daily.max(),
                "duracion_mediana_h": round(dur.median(), 1) if len(dur) > 0 else None,
                "duracion_p90_h": round(dur.quantile(0.9), 1) if len(dur) > 0 else None,
                "tasa_pase_inspeccion": round(pass_rate, 1) if pass_rate else None,
            }
        )
    return pd.DataFrame(rows)


def compute_daily_events(df):
    df = df.copy()
    df["fecha"] = df["fecha_evento"].dt.date
    daily = (
        df.groupby(["fecha", "taller_planta_grouped"])
        .size()
        .unstack(fill_value=0)
    )
    for c in daily.columns:
        daily[c] = daily[c].astype(int)
    return daily
