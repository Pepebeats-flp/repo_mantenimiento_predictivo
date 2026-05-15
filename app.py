#!/usr/bin/env python3
"""Dashboard Piloto 1 — Shadow Mode
Uso: streamlit run app.py
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

sys.path.append(str(Path(__file__).resolve().parent))

PROJECT_ROOT = Path(__file__).resolve().parent

HORIZONS = [3, 5, 7]
DECISION_THRESHOLD = 0.6

st.set_page_config(
    page_title="Piloto 1 — Predicción de Fallas",
    page_icon="🚍",
    layout="wide",
    initial_sidebar_state="expanded",
)

OUTCOME_EMOJI = {"TP": "✅", "TN": "⬜", "FP": "🟠", "FN": "🔴"}
OUTCOME_COLOR = {"TP": "#22c55e", "TN": "#d1d5db", "FP": "#f97316", "FN": "#ef4444"}
OUTCOME_LABEL = {
    "TP": "Alerta correcta — se predijo la falla y ocurrió",
    "TN": "Tranquilidad — no se predijo y no pasó nada",
    "FP": "Falsa alarma — se predijo pero no ocurrió",
    "FN": "Falla no detectada — ocurrió sin alerta previa",
}
OUTCOME_SYMBOL = {"TP": "circle", "TN": "circle", "FP": "diamond", "FN": "x"}
OUTCOME_SIZE = {"TP": 10, "TN": 6, "FP": 10, "FN": 12}


def fmt_num(n: int) -> str:
    """Thousands with dots: 1234567 → 1.234.567"""
    s = f"{n:,}"
    return s.replace(",", ".")


def fmt_pct(val: float) -> str:
    """0-1 probability → '75.2%'"""
    return f"{val * 100:.1f}%"


def fmt_pct_int(val: float) -> str:
    """0-1 probability → '75%' (no decimal)"""
    return f"{val * 100:.0f}%"


@st.cache_data
def load_base():
    return pd.read_parquet(PROJECT_ROOT / "data" / "processed" / "base.parquet")


@st.cache_data
def load_predictions():
    p = PROJECT_ROOT / "data" / "predictions" / "predictions_voy_redbus.parquet"
    if p.exists():
        return pd.read_parquet(p)
    return pd.DataFrame()


@st.cache_data
def load_shadow_report():
    p = PROJECT_ROOT / "outputs" / "piloto1_report.json"
    if p.exists():
        with open(p) as f:
            return json.load(f)
    return None


@st.cache_data
def get_failure_events():
    base = load_base()
    base = base.copy()
    base["fecha_evento"] = pd.to_datetime(base["fecha_evento"])
    from src.preprocessing import is_failure_event
    base["es_falla"] = base.apply(is_failure_event, axis=1)
    return base[base["es_falla"]].copy()


def enrich_predictions(preds: pd.DataFrame, failures: pd.DataFrame, bus: str, horizon: int | None) -> pd.DataFrame:
    if horizon is not None:
        sub = preds[
            (preds["placa_patente"] == bus) & (preds["horizon_days"] == horizon)
        ].sort_values("fecha_evento").copy()
        if sub.empty:
            return pd.DataFrame()
        sub["fecha_evento"] = pd.to_datetime(sub["fecha_evento"])

        f = failures.sort_values(["placa_patente", "fecha_evento"])
        next_event = f.groupby("placa_patente")["fecha_evento"].shift(-1)
        eventos_next = f[["placa_patente", "fecha_evento", "causa_sistema_reconstruida"]].copy()
        eventos_next["prox_evento"] = next_event

        merged = sub.merge(
            eventos_next[["placa_patente", "fecha_evento", "prox_evento", "causa_sistema_reconstruida"]],
            on=["placa_patente", "fecha_evento"],
            how="left",
        )
        delta = (merged["prox_evento"] - merged["fecha_evento"]).dt.days
        merged["falla_real"] = (delta.notna() & delta.le(horizon)).astype(int)
        merged["alerta"] = (merged["probability"] >= DECISION_THRESHOLD).astype(int)

        merged["resultado"] = "N/A"
        merged.loc[(merged["alerta"] == 1) & (merged["falla_real"] == 1), "resultado"] = "TP"
        merged.loc[(merged["alerta"] == 0) & (merged["falla_real"] == 0), "resultado"] = "TN"
        merged.loc[(merged["alerta"] == 1) & (merged["falla_real"] == 0), "resultado"] = "FP"
        merged.loc[(merged["alerta"] == 0) & (merged["falla_real"] == 1), "resultado"] = "FN"

        now = pd.Timestamp.now()
        merged["ventana_cerrada"] = (merged["fecha_evento"] + pd.Timedelta(days=horizon)) <= now
        return merged

    # ── Combined mode (horizon=None): aggregate all 3 horizons ──────
    frames = []
    for h in HORIZONS:
        enriched = enrich_predictions(preds, failures, bus, h)
        if not enriched.empty:
            enriched = enriched.rename(columns={
                "probability": f"prob_{h}d",
                "alerta": f"alerta_{h}d",
                "falla_real": f"falla_real_{h}d",
            })
            cols = ["fecha_evento"] + [c for c in enriched.columns if c.endswith(f"_{h}d")]
            frames.append(enriched[cols + ["causa_sistema_reconstruida", "ventana_cerrada"]])
    if not frames:
        return pd.DataFrame()
    combined = frames[0]
    for f_df in frames[1:]:
        combined = combined.merge(f_df, on=["fecha_evento", "causa_sistema_reconstruida", "ventana_cerrada"], how="outer", suffixes=("", "_drop"))
        combined = combined.loc[:, ~combined.columns.str.endswith("_drop")]

    combined["probability"] = combined[[c for c in combined.columns if c.startswith("prob_")]].max(axis=1, skipna=True)
    combined["alerta"] = combined[[c for c in combined.columns if c.startswith("alerta_")]].any(axis=1).astype(int)
    combined["falla_real"] = combined.get("falla_real_7d", 0)
    combined["resultado"] = "N/A"
    combined.loc[(combined["alerta"] == 1) & (combined["falla_real"] == 1), "resultado"] = "TP"
    combined.loc[(combined["alerta"] == 0) & (combined["falla_real"] == 0), "resultado"] = "TN"
    combined.loc[(combined["alerta"] == 1) & (combined["falla_real"] == 0), "resultado"] = "FP"
    combined.loc[(combined["alerta"] == 0) & (combined["falla_real"] == 1), "resultado"] = "FN"
    combined["ventana_cerrada"] = (combined["fecha_evento"] + pd.Timedelta(days=7)) <= pd.Timestamp.now()
    return combined


def make_timeline_chart(df: pd.DataFrame, title: str = "", height: int = 300):
    if df.empty:
        return go.Figure()

    fig = go.Figure()

    filled_df = df.sort_values("fecha_evento")
    dates = filled_df["fecha_evento"]
    probs = filled_df["probability"]

    hovertemplate = (
        "%{x|%d %b %Y %H:%M}<br>"
        "Riesgo: %{y:.0%}<extra></extra>"
    )
    # For combined mode, show per-horizon breakdown in tooltip
    has_breakdown = any(c.startswith("prob_") for c in df.columns)
    if has_breakdown:
        hovertemplate = (
            "%{x|%d %b %Y %H:%M}<br>"
            "Riesgo: %{y:.0%}<br>"
            "3d: %{customdata[0]:.0%} | 5d: %{customdata[1]:.0%} | 7d: %{customdata[2]:.0%}"
            "<extra></extra>"
        )
        customdata = filled_df[["prob_3d", "prob_5d", "prob_7d"]].values
    else:
        customdata = None

    fig.add_trace(go.Scatter(
        x=dates, y=probs,
        mode="lines",
        line=dict(color="#3b82f6", width=2),
        fill="tozeroy",
        fillcolor="rgba(59, 130, 246, 0.15)",
        name="Riesgo",
        customdata=customdata,
        hovertemplate=hovertemplate,
    ))

    for oc in ["TP", "TN", "FP", "FN"]:
        sub = df[df["resultado"] == oc]
        if sub.empty:
            continue
        fig.add_trace(go.Scatter(
            x=sub["fecha_evento"],
            y=sub["probability"],
            mode="markers",
            marker=dict(
                symbol=OUTCOME_SYMBOL[oc],
                size=OUTCOME_SIZE[oc],
                color=OUTCOME_COLOR[oc],
                line=dict(color="white", width=1.5),
            ),
            name=OUTCOME_LABEL[oc],
            customdata=sub["causa_sistema_reconstruida"],
            hovertemplate=(
                f"{OUTCOME_EMOJI[oc]} %{{x|%d %b %Y %H:%M}}<br>"
                f"Riesgo: %{{y:.0%}}<br>{OUTCOME_LABEL[oc]}<br>"
                f"Causa: %{{customdata}}<extra></extra>"
            ),
        ))

    fail_dates = df[df["falla_real"] == 1]["fecha_evento"]
    if not fail_dates.empty:
        fig.add_trace(go.Scatter(
            x=fail_dates,
            y=[1.02] * len(fail_dates),
            customdata=df[df["falla_real"] == 1]["causa_sistema_reconstruida"],
            mode="markers",
            marker=dict(symbol="x", size=10, color="#ef4444", line=dict(width=1)),
            name="Falla real ocurrida",
            hovertemplate="💥 Falla real: %{x|%d %b %Y}<br>Causa: %{customdata}<extra></extra>",
        ))

    fig.add_hline(
        y=DECISION_THRESHOLD,
        line=dict(color="#ef4444", width=1.5, dash="dash"),
        name=f"Umbral ({DECISION_THRESHOLD})",
    )

    fig.update_layout(
        title=title,
        xaxis_title="",
        yaxis_title="Riesgo de falla",
        yaxis=dict(range=[0, 1.08], tickformat=".0%"),
        height=height,
        margin=dict(l=10, r=10, t=30, b=30),
        hovermode="x unified",
        legend=dict(orientation="h", yanchor="bottom", y=1.08, xanchor="left", x=0, font_size=10),
        template="plotly_white",
    )
    return fig


# ── Load data ──────────────────────────────────────────────────────────────

shadow_report = load_shadow_report()
preds = load_predictions()
if not preds.empty:
    preds["fecha_evento"] = pd.to_datetime(preds["fecha_evento"])
failures = get_failure_events()

if shadow_report is None:
    st.warning("Reporte no disponible. Ejecute: python3 scripts/evaluate_shadow.py")
    st.stop()

horizons = sorted(shadow_report.get("por_horizonte", {}).keys(), key=int)

# Merge empresa_id into predictions (one empresa per bus)
buses_empresa = load_base()[["placa_patente", "empresa_id"]].drop_duplicates().groupby("placa_patente").first().reset_index()
preds = preds.merge(buses_empresa, on="placa_patente", how="left")

# ── Sidebar ────────────────────────────────────────────────────────────────

with st.sidebar:
    st.header("🚌 Bus a analizar")
    all_empresas = ["Todas"] + sorted(preds["empresa_id"].dropna().unique().tolist())
    sel_empresa = st.selectbox("Cliente", all_empresas, index=0)

    filtered = preds
    if sel_empresa != "Todas":
        filtered = filtered[filtered["empresa_id"] == sel_empresa]
    all_buses = sorted(filtered["placa_patente"].dropna().unique()) if not filtered.empty else []

    if "selected_bus" not in st.session_state:
        st.session_state.selected_bus = "FLXP68" if "FLXP68" in all_buses else (all_buses[0] if all_buses else "")
    if st.session_state.selected_bus not in all_buses:
        st.session_state.selected_bus = st.session_state.selected_bus if st.session_state.selected_bus in all_buses else ""

    sel = st.text_input(
        "Bus",
        value=st.session_state.selected_bus if st.session_state.selected_bus else "",
        placeholder="Escribe una patente…",
        label_visibility="collapsed",
    )
    if sel in all_buses:
        st.session_state.selected_bus = sel
    st.caption("Todas las secciones abajo usan este bus")

    st.divider()
    st.markdown("**📅 Período**")
    pred_date_min = preds["fecha_evento"].min().date()
    pred_date_max = preds["fecha_evento"].max().date()
    date_range = st.slider(
        "Mostrar predicciones desde",
        min_value=pred_date_min,
        max_value=pred_date_max,
        value=(pred_date_max - pd.Timedelta(days=90), pred_date_max),
        format="YYYY-MM-DD",
        label_visibility="collapsed",
    )
    st.session_state.date_range = date_range

# ═══════════════════════════════════════════════════════════════════════════
# HEADER — Global pipeline metrics
# ═══════════════════════════════════════════════════════════════════════════

meta = shadow_report.get("metadata", {})
total_preds = meta.get("total_predicciones", 0)
total_tp = total_tn = total_fp = total_fn = 0
for h in horizons:
    tm = shadow_report["por_horizonte"][h]["threshold_metrics"].get(str(DECISION_THRESHOLD), {})
    cm = tm.get("confusion_matrix", [])
    if cm:
        tn, fp, fn, tp = cm[0][0], cm[0][1], cm[1][0], cm[1][1]
        total_tp += tp
        total_tn += tn
        total_fp += fp
        total_fn += fn
total_all = total_tp + total_tn + total_fp + total_fn
global_acc = (total_tp + total_tn) / total_all if total_all > 0 else 0

st.markdown(
    f"""
    <div style='text-align:center; padding: 0.5rem 0 1rem;'>
        <h1 style='margin:0; font-size:2rem;'>🚍 Predicción de Fallas — Piloto 1</h1>
        <p style='color:#666; font-size:1rem; max-width:700px; margin:0.3rem auto;'>
            ACC global: <b>{fmt_pct(global_acc)}</b> &nbsp;|&nbsp;
            El modelo analiza el historial de cada bus y calcula el riesgo de que tenga
            una falla en los próximos <b>3, 5 y 7 días</b>. Cada predicción se compara
            con lo que realmente ocurrió para medir su precisión.
        </p>
    </div>
    """,
    unsafe_allow_html=True,
)

total_alerts = total_tp + total_fp

kpi1, kpi2, kpi3, kpi4, kpi5 = st.columns(5)
kpi1.metric("🚌 Buses monitoreados", fmt_num(int(meta.get("total_buses", 0))))
kpi2.metric("📊 Predicciones totales", fmt_num(total_preds))
kpi3.metric("🔔 Alertas emitidas", fmt_num(total_alerts))
kpi4.metric("🎯 Acierto global", fmt_pct(global_acc))
kpi5.metric("📅 Última predicción",
            str(pd.to_datetime(preds["fecha_evento"]).max().strftime("%d/%m/%Y")) if not preds.empty else "N/A")

st.divider()

# ═══════════════════════════════════════════════════════════════════════════
# SECTION: GLOBAL RESULTS — per-window model performance
# ═══════════════════════════════════════════════════════════════════════════

st.subheader("📊 Resultados Globales por Ventana")
st.caption("Para cada predicción se verifica si ocurrió una falla real en los días siguientes.")

global_rows = []
for h in horizons:
    m = shadow_report["por_horizonte"][h]
    tm = m["threshold_metrics"].get(str(DECISION_THRESHOLD), {})
    cm = tm.get("confusion_matrix", [])
    if cm:
        tn, fp, fn, tp = cm[0][0], cm[0][1], cm[1][0], cm[1][1]
        correctas = tp + tn
        incorrectas = fp + fn
    else:
        correctas = incorrectas = 0

    global_rows.append({
        "Ventana": f"{h} días",
        "Predicciones": fmt_num(int(m.get('total_predicciones', 0))),
        "Correctas ✅": fmt_num(correctas),
        "Incorrectas ❌": fmt_num(incorrectas),
    })

global_rows.append({
    "Ventana": "**TOTAL**",
    "Predicciones": fmt_num(total_all),
    "Correctas ✅": fmt_num(total_tp + total_tn),
    "Incorrectas ❌": fmt_num(total_fp + total_fn),
})

st.dataframe(global_rows, width="stretch", hide_index=True)

st.info(
    "📈 Las ventanas indican el plazo de la predicción: 3 días (muy corto plazo), "
    "5 días (corto plazo), 7 días (semanal)."
)

st.divider()

# ═══════════════════════════════════════════════════════════════════════════
# SECTION: CURRENT FLEET RISK
# ═══════════════════════════════════════════════════════════════════════════

st.subheader("🔮 Riesgo Actual de la Flota")
_cliente_label = sel_empresa if sel_empresa != "Todas" else "todos los clientes"
st.caption(f"Riesgo basado en el último evento de cada bus ({_cliente_label}). Unificado = máxima probabilidad entre 3d, 5d y 7d.")

# ── Compute current risk for all buses ─────────────────────
risk_base = filtered

if risk_base.empty:
    rk1, rk2, rk3 = st.columns(3)
    rk1.metric("🚌 Buses con datos", "0")
    rk2.metric("🔴 En riesgo (≥60%)", "0")
    rk3.metric("📊 Riesgo promedio", "0%")
    st.info("Sin datos para el filtro seleccionado.")
else:
    latest_dates = risk_base.groupby("placa_patente")["fecha_evento"].max().reset_index()
    latest_preds = risk_base.merge(latest_dates, on=["placa_patente", "fecha_evento"])
    latest_preds = latest_preds.drop_duplicates(subset=["placa_patente", "horizon_days"])

    risk_pivot = latest_preds.pivot_table(
        index="placa_patente", columns="horizon_days", values="probability", aggfunc="first"
    ).reset_index()
    risk_pivot.columns = ["placa_patente", "prob_3d", "prob_5d", "prob_7d"]
    risk_pivot["unified"] = risk_pivot[["prob_3d", "prob_5d", "prob_7d"]].max(axis=1)

    now_ts = pd.Timestamp.now()
    risk_pivot["dias_desde"] = (
        now_ts - latest_dates.set_index("placa_patente")["fecha_evento"]
    ).dt.days.values

    risk_pivot = risk_pivot.merge(
        risk_base[["placa_patente", "empresa_id"]].drop_duplicates("placa_patente"),
        on="placa_patente", how="left",
    )

    total_buses_risk = len(risk_pivot)
    buses_en_riesgo = int((risk_pivot["unified"] >= DECISION_THRESHOLD).sum())
    riesgo_promedio = risk_pivot["unified"].mean()

    rk1, rk2, rk3 = st.columns(3)
    rk1.metric("🚌 Buses con datos", fmt_num(total_buses_risk))
    rk2.metric("🔴 En riesgo (≥60%)", buses_en_riesgo,
               delta=fmt_pct_int(buses_en_riesgo/total_buses_risk) if total_buses_risk else None)
    rk3.metric("📊 Riesgo promedio", fmt_pct(riesgo_promedio))

    risk_col1, risk_col2 = st.columns([1, 2])
    with risk_col1:
        top_n = st.slider("Mostrar top N", 10, 100, 30, key="risk_topn")
    with risk_col2:
        risk_min = st.slider("Riesgo mínimo unificado", 0, 100, 0, key="risk_min", format="%d%%")

    sorted_risk = risk_pivot.sort_values("unified", ascending=False)
    risk_filtered = sorted_risk[sorted_risk["unified"] >= risk_min / 100].head(top_n)

    if risk_filtered.empty:
        st.info("Sin buses con el riesgo mínimo seleccionado.")
    else:
        display_rows = []
        for _, r in risk_filtered.iterrows():
            days = r["dias_desde"]
            days_str = f"{days:.0f}d" if days < 365 else f"{days/365:.1f}a"
            display_rows.append({
                "Bus": r["placa_patente"],
                "Cliente": r.get("empresa_id", "—"),
                "Último evento": days_str,
                "3d": int(r['prob_3d'] * 100),
                "5d": int(r['prob_5d'] * 100),
                "7d": int(r['prob_7d'] * 100),
                "Riesgo": int(r['unified'] * 100),
            })
        st.dataframe(display_rows, width="stretch", hide_index=True,
                     column_config={
                         "3d": st.column_config.ProgressColumn("3d", format="%d%%", width="small",
                             min_value=0, max_value=100),
                         "5d": st.column_config.ProgressColumn("5d", format="%d%%", width="small",
                             min_value=0, max_value=100),
                         "7d": st.column_config.ProgressColumn("7d", format="%d%%", width="small",
                             min_value=0, max_value=100),
                         "Riesgo": st.column_config.ProgressColumn("🔔", format="%d%%", width="small",
                             min_value=0, max_value=100),
                     })

        # ── Bar chart top 20 ──
        top20 = risk_filtered.head(20).sort_values("unified", ascending=True)
        fig_risk = go.Figure()
        fig_risk.add_trace(go.Bar(
            y=top20["placa_patente"],
            x=top20["unified"],
            orientation="h",
            marker_color=top20["unified"].apply(
                lambda x: "#ef4444" if x >= 0.7 else ("#f97316" if x >= 0.4 else "#22c55e")
            ),
            text=top20["unified"].apply(lambda x: f"{x*100:.0f}%"),
            textposition="outside",
            hovertemplate="%{y}<br>Riesgo: %{x:.0%}<extra></extra>",
        ))
        fig_risk.update_layout(
            title="Top 20 buses con mayor riesgo",
            xaxis=dict(title="Riesgo unificado", tickformat="%"),
            yaxis=dict(title=""),
            height=400,
            margin=dict(l=0, r=0, t=30, b=0),
            bargap=0.3,
        )
        st.plotly_chart(fig_risk, width="stretch")

st.divider()

# ═══════════════════════════════════════════════════════════════════════════
# SECTION: BUS ANALYSIS
# ═══════════════════════════════════════════════════════════════════════════

st.subheader("🚌 Análisis por Bus")

if preds.empty:
    st.stop()

bus = st.session_state.get("selected_bus")

if bus is None or bus not in all_buses:
    st.warning("Patente no encontrada para el cliente seleccionado.")
    st.stop()

# ── Per-horizon metrics for selected bus ──────────────────────────────────
bus_rows = []
for h in HORIZONS:
    enriched = enrich_predictions(preds, failures, bus, h)
    closed = enriched[enriched["ventana_cerrada"]]
    n = len(closed)
    if n < 3:
        continue
    tp = int((closed["resultado"] == "TP").sum())
    fp = int((closed["resultado"] == "FP").sum())
    fn = int((closed["resultado"] == "FN").sum())
    tn = int((closed["resultado"] == "TN").sum())
    bus_rows.append({
        "Ventana": f"{h}d",
        "Predicciones": n,
        "TP": tp, "TN": tn, "FP": fp, "FN": fn,
    })

if bus_rows:
    display_bus = []
    for r in bus_rows:
        display_bus.append({
            "Ventana": r["Ventana"],
            "Predicciones": fmt_num(r["Predicciones"]),
            "TP": fmt_num(r["TP"]),
            "TN": fmt_num(r["TN"]),
            "FP": fmt_num(r["FP"]),
            "FN": fmt_num(r["FN"]),
        })
    st.dataframe(display_bus, width="stretch", hide_index=True)
    total_tp_bus = sum(r["TP"] for r in bus_rows)
    total_tn_bus = sum(r["TN"] for r in bus_rows)
    total_fp_bus = sum(r["FP"] for r in bus_rows)
    total_fn_bus = sum(r["FN"] for r in bus_rows)
    total_correctas_bus = total_tp_bus + total_tn_bus
    total_preds_bus = total_tp_bus + total_tn_bus + total_fp_bus + total_fn_bus
    bus_acc = total_correctas_bus / total_preds_bus if total_preds_bus > 0 else 0
    st.metric("🎯 ACC del bus", fmt_pct(bus_acc),
              delta=f"TP={total_tp_bus} TN={total_tn_bus} FP={total_fp_bus} FN={total_fn_bus}")

# ── Timeline for selected bus ────────────────────────────────────────────
st.subheader(f"📅 Línea de Tiempo — {bus}")
st.caption("Cada punto es una predicción. El color indica si acertó o no. Las ❌ en la parte superior marcan fallas reales.")

leg_cols = st.columns(4)
for i, oc in enumerate(["TP", "TN", "FP", "FN"]):
    leg_cols[i].markdown(
        f"<span style='color:{OUTCOME_COLOR[oc]}; font-size:1.2rem;'>{OUTCOME_EMOJI[oc]}</span> "
        f"<span style='font-size:0.85rem;'>{OUTCOME_LABEL[oc]}</span>",
        unsafe_allow_html=True,
    )

h_timeline = st.selectbox("Horizonte", ["Combinado", "3 días", "5 días", "7 días"], index=0, key="tl_horizon")
h_val_map_tl = {"Combinado": None, "3 días": 3, "5 días": 5, "7 días": 7}
h_val = h_val_map_tl[h_timeline]

enriched = enrich_predictions(preds, failures, bus, h_val)
if enriched.empty:
    st.info("Sin datos para este bus en el horizonte seleccionado.")
else:
    fecha_desde, fecha_hasta = st.session_state.get("date_range", (None, None))
    if fecha_desde and fecha_hasta:
        enriched = enriched[
            (enriched["fecha_evento"] >= pd.Timestamp(fecha_desde))
            & (enriched["fecha_evento"] <= pd.Timestamp(fecha_hasta))
        ].copy()
    if enriched.empty:
        st.info("Sin predicciones en el período seleccionado.")
    else:
        cerradas = enriched[enriched["ventana_cerrada"]]
        total_c = len(cerradas)
        if total_c:
            correctas = int((cerradas["alerta"] == cerradas["falla_real"]).sum())
            pct = correctas / total_c * 100
            pend = len(enriched) - total_c
            tp = int((cerradas["resultado"] == "TP").sum())
            fp = int((cerradas["resultado"] == "FP").sum())
            fn = int((cerradas["resultado"] == "FN").sum())
            tn = int((cerradas["resultado"] == "TN").sum())
        else:
            correctas = pct = pend = tp = fp = fn = tn = 0

        mini_cols = st.columns(5)
        mini_cols[0].metric("Predicciones", len(enriched))
        mini_cols[1].metric("✅ Correctas", correctas, delta=f"{pct:.1f}%" if total_c else None)
        mini_cols[2].metric("🟠 Falsas alarmas", fp)
        mini_cols[3].metric("🔴 Fallas no detectadas", fn)
        mini_cols[4].metric("⏳ Pendientes", pend)

        fig = make_timeline_chart(enriched, height=300)
        st.plotly_chart(fig, width="stretch")

st.divider()

# ═══════════════════════════════════════════════════════════════════════════
# FOOTER
# ═══════════════════════════════════════════════════════════════════════════

with st.expander("ℹ️ Detalles técnicos"):
    st.markdown(f"""
**Modelo:** XGBoost | **Config:** `experiments/009_th06_all`

**¿Cómo funciona?**
1. Se entrena un modelo por cada ventana (3, 5, 7 días) con datos históricos recientes.
2. Para cada evento de cada bus, el modelo calcula un riesgo (0-100%).
3. Si el riesgo supera el umbral ({DECISION_THRESHOLD}), se emite una alerta.
4. Cada predicción se compara contra la realidad para medir la efectividad.

**Comandos:**
- `python3 scripts/evaluate_shadow.py` — generar reporte
- `python3 scripts/consultar_bus.py FLXS22` — consultar predicciones CLI
    """)
