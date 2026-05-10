#!/usr/bin/env python3
"""Panel de Gestion Predictiva — Mantenimiento de Flota
Uso: streamlit run app.py
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import streamlit as st

sys.path.append(str(Path(__file__).resolve().parent))

PROJECT_ROOT = Path(__file__).resolve().parent


# ── Carga de datos ─────────────────────────────────────────────────────────

@st.cache_data
def load_base():
    return pd.read_parquet(PROJECT_ROOT / "data" / "processed" / "base.parquet")

@st.cache_data
def load_eventos():
    return pd.read_parquet(PROJECT_ROOT / "data" / "processed" / "eventos_train.parquet")

@st.cache_data
def load_predictions():
    p = PROJECT_ROOT / "data" / "predictions" / "predictions_voy_redbus.parquet"
    if p.exists():
        return pd.read_parquet(p)
    return pd.read_parquet(PROJECT_ROOT / "data" / "predictions" / "predictions.parquet")

@st.cache_data
def get_empresa_mapping():
    """Map each placa_patente to su empresa_id (VOY o REDBUS).

    Para buses que aparecen en ambos operadores (solo 1 caso), se usa el mas frecuente.
    """
    base = load_base()
    mapping = base.groupby("placa_patente")["empresa_id"].agg(lambda x: x.mode()[0])
    return mapping


st.set_page_config(page_title="Mantenimiento Predictivo — Flota", page_icon="🔧", layout="wide")

# ── Sidebar ────────────────────────────────────────────────────────────────

st.sidebar.title("🔧 Mantenimiento Predictivo")
st.sidebar.caption("Panel de Gestion Predictiva")

OPERADOR = st.sidebar.selectbox(
    "Operador", ["Todos", "VOY", "REDBUS"],
    help="Seleccione operador para ver su flota completa"
)

HORIZON = st.sidebar.selectbox("Ventana de prediccion", [3, 5, 7], index=2,
                                help="Cuantos dias hacia adelante predecimos? 7d = una semana")
THRESHOLD = st.sidebar.slider("Sensibilidad de alerta", 0.0, 1.0, 0.5, 0.05,
                              help="Mas bajo = mas alertas (pero mas falsas alarmas)")

# ── Carga y filtrado ──────────────────────────────────────────────────────

all_preds = load_predictions()
all_eventos = load_eventos()
empresa_map = get_empresa_mapping()

# Asignar operador a cada prediccion
all_preds["empresa_id"] = all_preds["placa_patente"].map(empresa_map)

# Filtrar por operador
if OPERADOR != "Todos":
    preds = all_preds[all_preds["empresa_id"] == OPERADOR].copy()
    op_buses = empresa_map[empresa_map == OPERADOR].index
    eventos = all_eventos[all_eventos["placa_patente"].isin(op_buses)].copy()
else:
    preds = all_preds.copy()
    eventos = all_eventos.copy()

preds_h = preds[preds["horizon_days"] == HORIZON].copy()
preds_h["alerta"] = (preds_h["probability"] >= THRESHOLD).astype(bool)

# Merge taller/causa info into predictions for quick lookup
event_cols = ["placa_patente", "fecha_evento", "taller_planta", "taller_planta_norm", "causa_origen",
              "sistema_componente", "tiene_repuestos_evento", "num_keywords_tecnicos_evento",
              "duracion_ot_horas_prom_evento", "repuestos_count_evento",
              "keyword_motor", "keyword_freno", "keyword_bateria", "keyword_puerta",
              "keyword_aceite", "keyword_vidrio", "keyword_correa", "keyword_refrigerante",
              "keyword_sensor", "keyword_espejo"]
available_event_cols = [c for c in event_cols if c in eventos.columns]
eventos_merged = eventos[available_event_cols].drop_duplicates(
    subset=["placa_patente", "fecha_evento"])
preds_h = preds_h.merge(eventos_merged, on=["placa_patente", "fecha_evento"], how="left")

# Usar nombre de taller normalizado si existe
taller_col = "taller_planta_norm" if "taller_planta_norm" in preds_h.columns else "taller_planta"
taller_options = sorted(preds_h[taller_col].dropna().unique())
TALLER = st.sidebar.multiselect(
    "Terminal / Taller",
    options=taller_options,
    default=[],
    help="Filtrar por terminal o taller especifico"
)
if TALLER:
    preds_h = preds_h[preds_h[taller_col].isin(TALLER)]

# ── Helper: buscar keyword mas frecuente para un bus ──────────────────────

def top_keywords_for_bus(bus_df: pd.DataFrame, top_n: int = 3) -> list[str]:
    kw_cols = [c for c in bus_df.columns if c.startswith("keyword_") and not c.startswith("keyword_tecnicos")]
    scores = {}
    for c in kw_cols:
        scores[c.replace("keyword_", "")] = int(bus_df[c].fillna(0).sum())
    return sorted(scores.items(), key=lambda x: -x[1])[:top_n]

# ══════════════════════════════════════════════════════════════════════════
# HEADER
# ══════════════════════════════════════════════════════════════════════════

with st.sidebar:
    st.divider()
    total_buses = preds_h["placa_patente"].nunique()
    total_eventos = len(preds_h)
    total_alertas = preds_h["alerta"].sum()
    op_label = f" — {OPERADOR}" if OPERADOR != "Todos" else ""
    st.metric(f"Buses monitoreados{op_label}", f"{total_buses}")
    st.metric("Alertas activas", f"{total_alertas}",
              delta=f"{total_alertas/max(total_eventos,1)*100:.0f}% del total")

    st.divider()
    st.caption("🏠 Inicio")
    st.caption("v1.0 — Datos VOY + REDBUS + Modelo predictivo")

# ── Texto descriptivo para el operador activo ────────────────────────────
operador_txt = OPERADOR if OPERADOR != "Todos" else "ambos operadores"

# ══════════════════════════════════════════════════════════════════════════
# TAB 1: TALLER HOY — para el mecanico
# ══════════════════════════════════════════════════════════════════════════

tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "🏭 Taller Hoy", "🚌 Consultar Bus", "📋 Flota en Riesgo",
    "🔧 Repuestos y Fallas", "📈 Tendencias",
])

with tab1:
    st.header("🏭 Taller Hoy — ¿Que necesita atencion?")
    st.info(
        "Buses con mayor probabilidad de falla en los proximos "
        f"{HORIZON} dias. Ordene por riesgo para priorizar."
    )

    min_risk_filter = st.slider("Riesgo minimo (%)", 0, 100, 60, 5,
                                help="Mostrar solo buses con riesgo >= este valor")

    # Filter data
    df = preds_h[preds_h["alerta"]].copy()
    df = df[df["probability"] >= min_risk_filter / 100]

    if df.empty:
        st.warning("No hay buses que cumplan los filtros.")
    else:
        # Group by bus: take latest event per bus
        latest = df.sort_values("fecha_evento").groupby("placa_patente").last().reset_index()
        latest = latest.sort_values("probability", ascending=False)

        for _, row in latest.head(20).iterrows():
            risk_pct = row["probability"] * 100
            sev = row.get("severity", "MEDIUM")
            sev_icon = {"LOW": "🟢", "MEDIUM": "🟡", "HIGH": "🔴"}.get(sev, "⚪")
            taller = row.get(taller_col, row.get("taller_planta", "?"))
            causa = row.get("causa_origen", "?")

            # Top keywords for this bus
            bus_df = preds_h[preds_h["placa_patente"] == row["placa_patente"]]
            kws = top_keywords_for_bus(bus_df)

            with st.container(border=True):
                c1, c2, c3, c4 = st.columns([2, 1, 2, 2])
                c1.markdown(f"### {sev_icon} **{row['placa_patente']}**")
                c1.caption(f"Taller: {taller} · Causa: {causa}")

                bar_color = "red" if risk_pct >= 75 else "orange" if risk_pct >= 50 else "yellow"
                c2.markdown(f"# {risk_pct:.0f}%")
                c2.progress(int(risk_pct), text="riesgo")
                c2.caption(f"Severidad: {sev}")

                if kws:
                    kw_text = ", ".join(f"**{k}** ({v})" for k, v in kws)
                    c3.markdown("**Posibles problemas:**")
                    c3.markdown(kw_text)
                else:
                    c3.markdown("*Sin datos de falla*")

                ult_evento = str(row["fecha_evento"])[:16] if pd.notna(row["fecha_evento"]) else "?"
                c4.markdown(f"**Ultimo evento:** {ult_evento}")
                c4.markdown(f"**Repuestos:** {row.get('repuestos_count_evento', 0)}")

# ══════════════════════════════════════════════════════════════════════════
# TAB 2: CONSULTAR BUS
# ══════════════════════════════════════════════════════════════════════════

with tab2:
    st.header("🚌 Consultar Bus")
    st.info(f"Seleccione un bus para ver su historial completo, riesgos y patrones de falla ({operador_txt}).")

    bus_list = sorted(preds_h["placa_patente"].unique())
    selected = st.selectbox("Bus o patente:", bus_list)

    if selected:
        bus_preds = preds_h[preds_h["placa_patente"] == selected].sort_values("fecha_evento")
        bus_events = eventos[eventos["placa_patente"] == selected].sort_values("fecha_evento")

        # KPIs
        k1, k2, k3, k4, k5 = st.columns(5)
        total_ev = len(bus_preds)
        alertas = bus_preds["alerta"].sum()
        riesgo_prom = bus_preds["probability"].mean() * 100
        riesgo_max = bus_preds["probability"].max() * 100
        sev_alta = bus_preds["severity"].value_counts().get("HIGH", 0)
        k1.metric("Eventos", total_ev)
        k2.metric("Alertas", alertas, f"{alertas/max(total_ev,1)*100:.0f}%")
        k3.metric("Riesgo Promedio", f"{riesgo_prom:.0f}%")
        k4.metric("Riesgo Maximo", f"{riesgo_max:.0f}%")
        k5.metric("Alta Severidad", sev_alta)

        # Timeline
        st.subheader("Evolucion del riesgo en el tiempo")
        timeline = bus_preds[["fecha_evento", "probability", "alerta", "severity"]].copy()
        timeline["fecha_evento"] = pd.to_datetime(timeline["fecha_evento"])
        timeline = timeline.set_index("fecha_evento")
        st.line_chart(timeline[["probability"]], width="stretch")

        # Ultimos eventos con detalle
        st.subheader("Ultimos eventos")
        cols_show = ["fecha_evento", "probability", "alerta", "severity",
                     taller_col, "causa_origen", "sistema_componente",
                     "repuestos_count_evento", "num_keywords_tecnicos_evento"]
        avail_cols = [c for c in cols_show if c in bus_preds.columns]
        ultimos = bus_preds[avail_cols].sort_values("fecha_evento", ascending=False).head(20).copy()
        ultimos["Riesgo"] = ultimos["probability"].apply(lambda p: f"{p*100:.0f}%")
        ultimos["Alerta"] = ultimos["alerta"].apply(lambda a: "⚠️ SI" if a else "✅ no")
        ultimos["Fecha"] = pd.to_datetime(ultimos["fecha_evento"]).dt.strftime("%d-%m-%Y")
        display_map = {"Fecha": "Fecha", "Riesgo": "Riesgo", "Alerta": "Alerta",
                       "severity": "Sev.", taller_col: "Taller",
                       "causa_origen": "Causa", "repuestos_count_evento": "Respuestos"}
        disp = {k: v for k, v in display_map.items() if k in ultimos.columns}
        st.dataframe(ultimos[list(disp.keys())].rename(columns=disp),
                     width="stretch", hide_index=True)

        # Patrones de falla
        st.subheader("Patrones de falla historicos")
        col_pat1, col_pat2 = st.columns(2)
        with col_pat1:
            kws = top_keywords_for_bus(bus_preds, 5)
            if kws:
                st.markdown("**Problemas mas frecuentes:**")
                for k, v in kws:
                    barr = "█" * min(v, 30)
                    st.markdown(f"{k}: {barr} ({v} eventos)")
        with col_pat2:
            tc = taller_col if taller_col in bus_events.columns else "taller_planta"
            if tc in bus_events.columns:
                talleres = bus_events[tc].value_counts()
                st.markdown("**Talleres que lo atendieron:**")
                for t, c in talleres.items():
                    st.markdown(f"- {t}: {c} veces")

# ══════════════════════════════════════════════════════════════════════════
# TAB 3: FLOTA EN RIESGO
# ══════════════════════════════════════════════════════════════════════════

with tab3:
    st.header("📋 Flota en Riesgo")
    st.info("Todos los buses ordenados por nivel de riesgo. Use los filtros para enfocarse.")

    sev_filter = st.multiselect("Severidad:", ["LOW", "MEDIUM", "HIGH"], default=[])

    top_n = st.slider("Cuantos buses mostrar:", 10, 200, 50, 10)

    df = preds_h.copy()
    if sev_filter:
        df = df[df["severity"].isin(sev_filter)]

    bus_agg = (
        df.groupby("placa_patente")
        .agg(
            eventos=("alerta", "count"),
            alertas=("alerta", "sum"),
            riesgo_prom=("probability", "mean"),
            riesgo_max=("probability", "max"),
            severidad_alta=("severity", lambda s: (s == "HIGH").sum()),
            ultimo_evento=("fecha_evento", "max"),
        )
        .reset_index()
    )
    bus_agg["tasa_alerta"] = bus_agg["alertas"] / bus_agg["eventos"] * 100
    bus_agg["riesgo_prom"] = bus_agg["riesgo_prom"] * 100
    bus_agg["riesgo_max"] = bus_agg["riesgo_max"] * 100
    bus_agg["urgencia"] = bus_agg["riesgo_prom"] * 0.3 + bus_agg["riesgo_max"] * 0.7
    bus_agg = bus_agg.sort_values("urgencia", ascending=False).head(top_n)

    bus_agg["Ultimo"] = pd.to_datetime(bus_agg["ultimo_evento"]).dt.strftime("%d-%m-%Y")
    display = bus_agg.rename(columns={
        "placa_patente": "Bus", "eventos": "Eventos", "alertas": "Alertas",
        "tasa_alerta": "% Alerta", "riesgo_prom": "Riesgo Prom",
        "riesgo_max": "Max Riesgo", "severidad_alta": "Alta Sev",
    })[["Bus", "Eventos", "Alertas", "% Alerta", "Riesgo Prom", "Max Riesgo", "Alta Sev", "Ultimo"]]
    display["% Alerta"] = display["% Alerta"].round(1)
    display["Riesgo Prom"] = display["Riesgo Prom"].round(1)
    display["Max Riesgo"] = display["Max Riesgo"].round(1)

    st.dataframe(display, width="stretch", hide_index=True)

    st.subheader("Distribucion del riesgo en la flota")
    hist_data = preds_h.groupby("placa_patente")["probability"].max().reset_index()
    hist_data["categoria"] = pd.cut(hist_data["probability"],
                                     bins=[0, 0.25, 0.5, 0.75, 1.0],
                                     labels=["Bajo", "Medio", "Alto", "Critico"])
    cat_counts = hist_data["categoria"].value_counts().reindex(["Bajo", "Medio", "Alto", "Critico"], fill_value=0)
    st.bar_chart(cat_counts, width="stretch")

# ══════════════════════════════════════════════════════════════════════════
# TAB 4: REPUESTOS Y FALLAS
# ══════════════════════════════════════════════════════════════════════════

with tab4:
    st.header("🔧 Repuestos y Fallas")
    st.info(f"Patrones de repuestos y tipos de falla en la flota {operador_txt}.")

    col_r1, col_r2 = st.columns(2)
    with col_r1:
        st.subheader("Tipos de problema mas frecuentes")
        kw_cols = [c for c in preds_h.columns if c.startswith("keyword_")
                   and not c.startswith("keyword_tecnicos")]
        kw_data = {}
        for c in kw_cols:
            name = c.replace("keyword_", "").title()
            kw_data[name] = int(preds_h[c].fillna(0).sum())
        kw_df = pd.DataFrame(list(kw_data.items()), columns=["Problema", "Eventos"])
        kw_df = kw_df.sort_values("Eventos", ascending=False)
        st.bar_chart(kw_df.set_index("Problema"), width="stretch")

    with col_r2:
        st.subheader("Eventos con repuestos vs sin repuestos")
        if "tiene_repuestos_evento" in preds_h.columns:
            rep_data = preds_h["tiene_repuestos_evento"].value_counts().rename(
                index={0: "Sin repuestos", 1: "Con repuestos"})
            st.bar_chart(rep_data, width="stretch")

    st.subheader("Distribucion por causa")
    if "causa_origen" in preds_h.columns:
        causa_counts = preds_h["causa_origen"].value_counts().head(10)
        st.bar_chart(causa_counts, width="stretch")

    st.subheader("Talleres por volumen de atencion")
    tc = taller_col if taller_col in preds_h.columns else "taller_planta"
    if tc in preds_h.columns:
        talleres = preds_h[tc].value_counts()
        st.bar_chart(talleres, width="stretch")

# ══════════════════════════════════════════════════════════════════════════
# TAB 5: TENDENCIAS — para el ingeniero
# ══════════════════════════════════════════════════════════════════════════

with tab5:
    st.header("📈 Tendencias")
    st.info("Evolucion de alertas, severidad y salud de flota en el tiempo.")

    preds_h["fecha_evento"] = pd.to_datetime(preds_h["fecha_evento"])
    preds_h["semana"] = preds_h["fecha_evento"].dt.isocalendar().week.astype(int)
    preds_h["anio"] = preds_h["fecha_evento"].dt.year
    preds_h["mes"] = preds_h["fecha_evento"].dt.month

    st.subheader("Alertas por semana")
    weekly = preds_h.groupby(["anio", "semana"]).agg(
        eventos=("alerta", "count"), alertas=("alerta", "sum")
    ).reset_index()
    weekly["label"] = weekly["anio"].astype(str) + "-S" + weekly["semana"].astype(str)
    weekly["tasa"] = weekly["alertas"] / weekly["eventos"] * 100
    st.line_chart(weekly.set_index("label")[["tasa"]], width="stretch")

    st.subheader("Volumen de eventos por mes")
    monthly = preds_h.groupby(["anio", "mes"]).agg(
        eventos=("alerta", "count"), alertas=("alerta", "sum")
    ).reset_index()
    monthly["label"] = monthly["anio"].astype(str) + "-" + monthly["mes"].astype(str).str.zfill(2)
    st.bar_chart(monthly.set_index("label")[["eventos", "alertas"]], width="stretch")

    st.subheader("Severidad a lo largo del tiempo")
    sev_trend = preds_h.copy()
    sev_trend["periodo"] = sev_trend["fecha_evento"].dt.to_period("M").astype(str)
    if "severity" in sev_trend.columns:
        sev_pivot = sev_trend.groupby(["periodo", "severity"]).size().unstack(fill_value=0)
        st.bar_chart(sev_pivot, width="stretch")

    # Metricas del modelo (al fondo, para el ingeniero)
    with st.expander("📐 Metricas del modelo (para referencia)"):
        st.json({
            "horizon": HORIZON,
            "threshold": THRESHOLD,
            "buses_monitoreados": int(preds_h["placa_patente"].nunique()),
            "total_eventos": int(len(preds_h)),
            "total_alertas": int(preds_h["alerta"].sum()),
            "tasa_alertas_pct": round(preds_h["alerta"].mean() * 100, 1),
        })
