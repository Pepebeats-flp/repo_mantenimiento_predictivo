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

    tipo = base.get("tipo_servicio", base.get("tipo_revision", "")).fillna("").astype(str).str.upper()

    # CORRECTIVO: causa in SIGNIFICANT_FAILURE_CATEGORIES
    from src.preprocessing import SIGNIFICANT_FAILURE_CATEGORIES
    causa = base.get("causa_sistema_reconstruida", "").fillna("").astype(str).str.upper()
    es_correctivo = tipo == "CORRECTIVO"
    es_falla_correctivo = es_correctivo & causa.isin(SIGNIFICANT_FAILURE_CATEGORIES)

    # PREVENTIVO: resultado is False/0
    es_preventivo = tipo == "PREVENTIVO"
    res = base.get("resultado")
    if res is not None:
        if res.dtype == bool:
            es_falla_preventivo = es_preventivo & ~res
        else:
            es_falla_preventivo = es_preventivo & (res.fillna(1).astype(int) == 0)
    else:
        es_falla_preventivo = pd.Series(False, index=base.index)

    # REGB / IT: resultado_pasa == 0 OR inspeccion_total_highs > 0
    es_regb_it = tipo.isin(["REGB", "IT"])
    resultado_pasa = base.get("resultado_pasa", pd.Series(1, index=base.index)).fillna(1).astype(int)
    has_highs = base.get("inspeccion_total_highs", pd.Series(0, index=base.index)).fillna(0).astype(int) > 0
    es_falla_regb_it = es_regb_it & ((resultado_pasa == 0) | has_highs)

    base["es_falla"] = es_falla_correctivo | es_falla_preventivo | es_falla_regb_it
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
    st.radio("Navegación", ["📊 Dashboard", "📖 Documentación"], key="nav_page",
             label_visibility="collapsed")
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

nav_page = st.session_state.get("nav_page", "📊 Dashboard")

if nav_page == "📖 Documentación":
    # ═══════════════════════════════════════════════════════════════════════
    # DOCUMENTATION PAGE
    # ═══════════════════════════════════════════════════════════════════════

    st.markdown(
        "<h1 style='text-align:center;'>📖 Documentación del Sistema</h1>"
        "<p style='text-align:center;color:#666;'>Predicción de Eventos — Piloto 1</p>",
        unsafe_allow_html=True,
    )

    st.divider()

    # ── 1. Resumen ───────────────────────────────────────────────────
    with st.expander("📋 Resumen del Proyecto", expanded=True):
        st.markdown(f"""
**Piloto 1** es un sistema de **mantenimiento predictivo** que analiza el historial de
mantenimiento de cada bus y calcula la probabilidad de que tenga un evento en los
próximos **3, 5 y 7 días**. El sistema opera en **Shadow Mode**: las predicciones se
generan y se comparan contra la realidad para medir su precisión, sin intervenir en
las operaciones.

- **Objetivo**: anticipar eventos y notificar de forma oporturna.
- **Alcance**: flota completa de buses de todos los clientes ({shadow_report.get('metadata', {}).get('total_buses', 0)} buses)
- **Estado**: Shadow Mode — evaluación continua contra eventos reales
- **Última evaluación**: {shadow_report.get('metadata', {}).get('fecha_evaluacion', 'N/A')[:10]}
        """)

    # ── 2. Origen de los Datos ───────────────────────────────────────
    with st.expander("🗄️ Origen de los Datos", expanded=True):
        st.markdown("""
Los datos provienen de **Firestore** (base de datos en la nube) y se procesan a través
de un pipeline ETL que consolida **4 tipos de servicio**:

| Tipo  | Volumen |
|---|---|
| **CORRECTIVO** | ~259k registros |
| **PREVENTIVO** | ~108k registros |
| **REGB** |  ~23k registros |
| **IT** |  ~17k registros |

**Clientes incluidos** (mapeo desde unidad_negocio):

| Cliente | Registros |
|---|---|
| **METROPOL** | ~153k |
| **REDBUS** | ~110k |
| **OTROS** (s/inspector, etc.) | ~85k |
| **VOY** | ~37k |
| **CONECTA** | ~22k |
| **GRANAMERICAS** | ~1.7k |

**Total**: ~408k registros de ~2,968 buses.
**Rango de fechas**: 2021-02-09 → 2026-05-13 (~5 años de historia).
        """)

    # ── 3. Definición de Evento ───────────────────────────────────────
    with st.expander("⚙️ Definición de Evento", expanded=True):
        st.markdown("""
No todos los registros de mantenimiento se consideran un **evento**. La condición
`es_evento` se determina según el tipo de servicio:

| Tipo | ¿Cuándo es evento? |
|---|---|
| **CORRECTIVO** | Solo si `causa_sistema_reconstruida` está en categorías significativas: **FRENOS, MOTOR, ELÉCTRICO, CLIMATIZACIÓN, RUEDAS, SUSPENSIÓN, PUERTAS**. |
| **PREVENTIVO** | Cuando el mantenimiento programado es **rechazado** (`resultado == False/0`). Un preventivo que pasa exitosamente no es evento. |
| **REGB / IT** | Cuando la inspección **no pasa** (`resultado_pasa == 0`) o tiene **defectos críticos** (`inspección_total_highs > 0`). |

**Total de eventos identificados**: ~120,507.
        """)

    # ── 4. Pipeline de Entrenamiento ─────────────────────────────────
    _cutoff = shadow_report.get('por_horizonte', {}).get('3', {}).get('auc_roc', 'N/A')
    _meta_3d_path = Path("models/xgb_3d_voy_redbus_meta.json")
    _cutoff_date = "N/A"
    _balance = "N/A"
    if _meta_3d_path.exists():
        import json as _json
        _m = _json.loads(_meta_3d_path.read_text())
        _cutoff_date = _m.get("cutoff_date", "N/A")
        _balance = _m.get("balance", "N/A")

    with st.expander("🧠 Pipeline de Entrenamiento", expanded=True):
        st.markdown(f"""
**División Train / Test (temporal):**
- **Cutoff date**: `{_cutoff_date}` (30 días antes del entrenamiento)
- **Train**: todos los eventos ANTES del cutoff
- **Test**: todos los eventos DESDE el cutoff en adelante
- **Evaluación por ventana temporal**: ~8,308 eventos de test por horizonte

**Buses de prueba (hold-out):** 10 buses totalmente excluidos del entrenamiento:
```
PFVL15  PFVL21  PFVK90  PFTF88  PDZH97
PFYH17  PFVK64  PFYR84  PFYG94  PFTF84
```
Estos buses se usan para medir la capacidad de **generalización a buses nunca vistos**.

**Balanceo de clases:**
- Desbalance natural: ~5-18% de eventos positivos (falla) según el horizonte
- Técnica: **SMOTE** (Synthetic Minority Oversampling) + `scale_pos_weight`
- `scale_pos_weight` = (negativos / positivos) × multiplicador (1.0 por defecto)
  - 3d: 7.54 | 5d: 5.05 | 7d: 3.58
        """)

    # ── 5. Modelo ────────────────────────────────────────────────────
    with st.expander("🤖 Modelo: XGBoost", expanded=True):
        st.markdown(f"""
**Algoritmo**: XGBoost Classifier (3 modelos independientes, uno por horizonte)

**Hiperparámetros** (optimizados):

| Parámetro | Valor | Descripción |
|---|---|---|
| `n_estimators` | 1200 | Número de árboles |
| `max_depth` | 10 | Profundidad máxima |
| `learning_rate` | 0.02 | Tasa de aprendizaje |
| `subsample` | 0.85 | Fracción de muestras por árbol |
| `colsample_bytree` | 0.85 | Fracción de features por árbol |
| `min_child_weight` | 3 | Peso mínimo por hoja |
| `gamma` | 0.1 | Reducción mínima de pérdida |
| `reg_alpha` | 0.1 | Regularización L1 |
| `reg_lambda` | 2.0 | Regularización L2 |
| `early_stopping_rounds` | 50 | Detención temprana |
| `device` | CUDA (GPU) | Aceleración |

**Umbral de decisión interno** (seleccionado por validación):
- 3d: **0.85** | 5d: **0.80** | 7d: **0.80**
- Nota: el dashboard usa un umbral unificado de **{DECISION_THRESHOLD}** para todos los horizontes
        """)

    # ── 6. Features ─────────────────────────────────────────────────
    with st.expander("📊 Features (186 variables predictivas)", expanded=False):
        st.markdown("""
Las features se generan a partir del historial de cada bus, agrupadas en las siguientes familias:

| Familia | Variables | Descripción |
|---|---|---|
| **Bus History** | 5 | Total eventos/correctivos/fallas históricos, tasas |
| **Rolling Windows** | 10 | Conteo de eventos/correctivos/tasa de falla en últimos {3,5,7}d |
| **Event-level** | 14 | Repuestos, duración OT, horas desde creación, keywords técnicos |
| **Inspecciones** | 5 | Highs/Mediums/Lows de REGB/IT, no presentado, sistemas |
| **Bus Stats** | 5 | Media/std/min/max de días entre eventos, máx correctivos previos |
| **Cause-based** | ~15 | Días desde misma causa, racha, diversidad de causas, conteos por causa |
| **System** | ~20 | Conteos por sistema/taller/unidad de negocio en {7,30}d |
| **Inventory** | 14 | Repuestos, uuid gestión, filas correctivo en {7,30}d |
| **Text Patterns** | ~30 | Keywords técnicos (motor, freno, batería, etc.) en {7,30}d |
| **Event Type** | 20 | Conteos por tipo (CORR/PREV/REGB/IT) en {7,30,60,180}d |
| **Severity** | ~10 | Highs/Mediums/Lows por tipo de inspección |
| **Trends** | 6 | Pendiente de eventos/fallas en {7,30,60}d |
| **Bus Age** | 4 | Edad del bus en días, eventos totales, preventivo reciente |
| **Temporal** | 3 | Mes, día de semana, fin de mes |
        """)

    # ── 7. Resultados del Test Set ──────────────────────────────────
    _eval_summary_path = Path("outputs/metrics/evaluation_summary_voy_redbus.json")
    if _eval_summary_path.exists():
        import json as _json
        _eval = _json.loads(_eval_summary_path.read_text())
    else:
        _eval = {}

    with st.expander("📈 Resultados del Test Set", expanded=True):
        if _eval:
            st.markdown("**Métricas sobre el conjunto de test temporal (eventos después del cutoff):**")
            for _w in ["3", "5", "7"]:
                _r = _eval.get(_w, {})
                if not _r:
                    continue
                _cm = _r.get("confusion_matrix", [])
                if _cm:
                    _tn, _fp, _fn, _tp = _cm[0][0], _cm[0][1], _cm[1][0], _cm[1][1]
                else:
                    _tn = _fp = _fn = _tp = 0
                st.markdown(f"""
**Ventana {_w}d** (umbral de decisión interno = {_r.get('decision_threshold', 'N/A')})
- ACC = {_r.get('accuracy', 0)*100:.1f}% | Precision = {_r.get('precision', 0)*100:.1f}% | Recall = {_r.get('recall', 0)*100:.1f}%
- F1 = {_r.get('f1', 0):.3f} | Specificity = {_r.get('specificity', 0)*100:.1f}% | AUC-ROC = {_r.get('auc_roc', 'N/A')}
- TP={_tp} TN={_tn} FP={_fp} FN={_fn}
- Test events: {_r.get('test_positives', 0)} positivos + {_r.get('test_negatives', 0)} negativos = {_r.get('test_positives', 0) + _r.get('test_negatives', 0)} total
- Baseline (mayoritaria): {_r.get('baseline_always_majority', 0)*100:.1f}%
""")
        else:
            st.info("Reporte de evaluación no disponible.")

    # ── 8. Holdout Buses ────────────────────────────────────────────
    _test_bus_path = Path("outputs/metrics/test_buses_summary_voy_redbus.json")
    if _test_bus_path.exists():
        import json as _json
        _test_bus = _json.loads(_test_bus_path.read_text())
    else:
        _test_bus = {}

    with st.expander("🧪 Resultados — Buses de Prueba (Hold-out)", expanded=False):
        if _test_bus:
            st.markdown("""
**10 buses no vistos durante el entrenamiento**, para medir generalización:
```
PFVL15  PFVL21  PFVK90  PFTF88  PDZH97
PFYH17  PFVK64  PFYR84  PFYG94  PFTF84
```
""")
            for _w in ["3", "5", "7"]:
                _r = _test_bus.get(_w, {})
                if not _r:
                    continue
                _cm = _r.get("confusion_matrix", [])
                if _cm:
                    _tn, _fp, _fn, _tp = _cm[0][0], _cm[0][1], _cm[1][0], _cm[1][1]
                else:
                    _tn = _fp = _fn = _tp = 0
                st.markdown(f"""
**Ventana {_w}d** ({_r.get('test_size', 0)} eventos):
- ACC = {_r.get('accuracy', 0)*100:.1f}% | Precision = {_r.get('precision', 0)*100:.1f}% | Recall = {_r.get('recall', 0)*100:.1f}%
- F1 = {_r.get('f1', 0):.3f} | AUC-ROC = {_r.get('auc_roc', 'N/A')}
- TP={_tp} TN={_tn} FP={_fp} FN={_fn}
""")
        else:
            st.info("Reporte de holdout no disponible.")

    # ── 9. Shadow Evaluation ───────────────────────────────────────
    with st.expander("🕵️ Shadow Evaluation — Buses Piloto", expanded=True):
        st.markdown(f"""
Evaluación de todas las predicciones (excluyendo severidad LOW) contra eventos reales,
usando umbral **{DECISION_THRESHOLD}** para los buses piloto FLXS22 (Diesel), FLXS23 (Diesel), LWTK42 (Eléctrico).

**Métricas globales por horizonte:**
""")
        for _h in sorted(horizons, key=int):
            _tm = shadow_report["por_horizonte"][_h]["threshold_metrics"].get(str(DECISION_THRESHOLD), {})
            _cm = _tm.get("confusion_matrix", [])
            if _cm:
                _tn, _fp, _fn, _tp = _cm[0][0], _cm[0][1], _cm[1][0], _cm[1][1]
            else:
                _tn = _fp = _fn = _tp = 0
            _cr = _tm.get("classification_report", {}).get("1", {})
            _auc = shadow_report["por_horizonte"][_h].get("auc_roc", "N/A")
            _n = shadow_report["por_horizonte"][_h].get("total_predicciones", 0)
            st.markdown(f"""
**{_h} días** ({_n} predicciones):
- ACC = {_tm.get('accuracy', 0)*100:.1f}% | Precision = {_cr.get('precision', 0)*100:.1f}% | Recall = {_cr.get('recall', 0)*100:.1f}%
- F1 = {_cr.get('f1-score', 0):.3f} | AUC-ROC = {_auc}
- TP={_tp} TN={_tn} FP={_fp} FN={_fn}

**Por bus:**
""")
            _bus_data = shadow_report.get("por_bus", {}).get(_h, {})
            for _bus, _bm in _bus_data.items():
                if "error" in _bm:
                    continue
                _btm = _bm.get("threshold_metrics", {}).get(str(DECISION_THRESHOLD), {})
                _bcm = _btm.get("confusion_matrix", [])
                if _bcm:
                    btn, bfp, bfn, btp = _bcm[0][0], _bcm[0][1], _bcm[1][0], _bcm[1][1]
                    bacc = (btp + btn) / (btp + btn + bfp + bfn) if (btp + btn + bfp + bfn) > 0 else 0
                else:
                    btn = bfp = bfn = btp = 0
                    bacc = 0
                st.markdown(f"""
  - **{_bus}** (n={_bm.get('total_predicciones', 0)}): ACC = {bacc*100:.1f}%  TP={btp} TN={btn} FP={bfp} FN={bfn}
""")

    # ── 10. Sistema de Alertas ─────────────────────────────────────
    with st.expander("🔔 Sistema de Alertas", expanded=True):
        st.markdown(f"""
**¿Cómo funciona la predicción?**

Para cada evento de mantenimiento de cada bus, los 3 modelos (3d, 5d, 7d) calculan
un **riesgo** (probabilidad de 0% a 100%) de que ocurra una falla dentro de cada ventana.

**¿Las alertas son hacia adelante o hacia atrás?**
- Las alertas **miran hacia adelante**: "Este bus tiene un X% de probabilidad de fallar
  en los próximos N días"
- Cada predicción se genera para un evento y un horizonte específico
- Si la probabilidad supera el umbral ({DECISION_THRESHOLD} = 60%), se emite una alerta

**¿Cómo se mide la precisión?**
- Una vez que la ventana de tiempo se cierra (pasan los N días), se verifica si
  realmente ocurrió una falla en ese período
- Si la alerta se activó y hubo falla → **TP** (acierto)
- Si la alerta se activó y NO hubo falla → **FP** (falsa alarma)
- Si no hubo alerta y no hubo falla → **TN** (tranquilidad)
- Si no hubo alerta pero sí hubo falla → **FN** (falla no detectada)

**Riesgo unificado** en el dashboard = máxima probabilidad entre los 3 horizontes.
        """)

    # ── 11. TP/TN/FP/FN ────────────────────────────────────────────
    with st.expander("📊 Precisión de las Predicciones (TP / TN / FP / FN)", expanded=False):
        st.markdown(f"""
| Resultado | Emoji | Significado | Color |
|---|---|---|---|
| **TP** (True Positive) | {OUTCOME_EMOJI.get('TP', '✅')} | Alerta correcta: se predijo la falla y ocurrió | Verde |
| **TN** (True Negative) | {OUTCOME_EMOJI.get('TN', '⬜')} | Tranquilidad: no se predijo y no pasó nada | Gris |
| **FP** (False Positive) | {OUTCOME_EMOJI.get('FP', '🟠')} | Falsa alarma: se predijo pero no ocurrió | Naranja |
| **FN** (False Negative) | {OUTCOME_EMOJI.get('FN', '🔴')} | Falla no detectada: ocurrió sin alerta previa | Rojo |

**ACC (Accuracy)** = (TP + TN) / (TP + TN + FP + FN)

**Precision** = TP / (TP + FP) — de las alertas emitidas, qué proporción fueron correctas

**Recall** = TP / (TP + FN) — de las fallas reales, qué proporción fueron anticipadas

**F1-Score** = 2 × (Precision × Recall) / (Precision + Recall) — promedio armónico

**Specificity** = TN / (TN + FP) — de los eventos sin falla, qué proporción se clasificaron correctamente
        """)


    # ── 13. Comandos ───────────────────────────────────────────────
    with st.expander("💻 Comandos Útiles", expanded=False):
        st.markdown(f"""
```bash
# Pipeline completo (ETL → Train → Infer)
python3 scripts/run_pipeline.py --local-json

# Shadow Evaluation (genera el reporte que alimenta este dashboard)
python3 scripts/evaluate_shadow.py

# Consultar predicciones de un bus específico
python3 scripts/consultar_bus.py FLXS22

# Top 10 buses con mayor riesgo
python3 scripts/consultar_bus.py --top 10

# Daily inference programada
python3 scripts/daily_inference.py

# Experimentos
python3 scripts/run_experiment.py 009_th06_all --local-json
python3 scripts/compare_experiments.py

# Lanzar este dashboard
streamlit run app.py
```
        """)

    st.stop()

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
            xaxis=dict(title="Riesgo unificado", tickformat=".0%"),
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

st.caption("📖 Para más detalles, ve a la sección Documentación en el sidebar.")
