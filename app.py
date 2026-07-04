#!/usr/bin/env python3
"""Predicción Operacional — Forecast + Riesgo + Búsqueda

Uso: streamlit run app.py
"""
from __future__ import annotations

import gc
import json
import pickle
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st

sys.path.append(str(Path(__file__).resolve().parent))

PROJECT_ROOT = Path(__file__).resolve().parent

st.set_page_config(
    page_title="Predicción Operacional",
    page_icon="🚍",
    layout="wide",
    initial_sidebar_state="expanded",
)

from scripts.analytics import predictive, operational as op

# ═══════════════════════════════════════════════════════════════════════════════
# DATA
# ═══════════════════════════════════════════════════════════════════════════════

@st.cache_data(ttl=3600, max_entries=1)
def load():
    df = pd.read_parquet(PROJECT_ROOT / "data" / "processed" / "base.parquet")
    df["fecha_evento"] = pd.to_datetime(df["fecha_evento"])
    if "sistema_enriched" not in df.columns:
        df = op.enrich_system_labels(df)
    return df


@st.cache_data(ttl=3600, max_entries=1)
def get_health_scores():
    result = predictive.compute_health_scores(load())
    gc.collect()
    return result


@st.cache_data(ttl=3600, max_entries=1)
def get_observation_alerts():
    result = predictive.compute_observation_alerts(load(), window_days=30)
    gc.collect()
    return result


# ── Operational models (disk-first, train as fallback) ─────────────────────

def _load_model_from_disk(name):
    """Load model+metadata from models/ dir. Returns (model, meta) or None."""
    pkl_path = PROJECT_ROOT / "models" / f"{name}.pkl"
    json_path = PROJECT_ROOT / "models" / f"{name}_meta.json"
    if pkl_path.exists() and json_path.exists():
        with open(pkl_path, "rb") as f:
            model = pickle.load(f)
        with open(json_path) as f:
            meta = json.load(f)
        return model, meta
    return None


@st.cache_resource(ttl=86400, max_entries=1)
def get_weekly_model():
    loaded = _load_model_from_disk("weekly_model")
    if loaded:
        return loaded
    result = op.train_weekly_system_load(load())
    gc.collect()
    return result


@st.cache_resource(ttl=86400, max_entries=1)
def get_spike_model():
    loaded = _load_model_from_disk("spike_model")
    if loaded:
        return loaded
    result = op.train_bus_spike_model(load())
    gc.collect()
    return result


@st.cache_resource(ttl=86400, max_entries=1)
def get_parts_model():
    loaded = _load_model_from_disk("parts_model")
    if loaded:
        return loaded
    result = op.train_parts_model(load())
    gc.collect()
    return result


@st.cache_resource(ttl=86400, max_entries=1)
def get_inspection_model():
    loaded = _load_model_from_disk("inspection_model")
    if loaded:
        return loaded
    result = op.train_inspection_model(load())
    gc.collect()
    return result

# ────────────────────────────────────────────────────────────────────────────────


@st.cache_data(ttl=3600, max_entries=1)
def get_weekly_forecast(weeks=8):
    model, meta = get_weekly_model()
    result = op.predict_weekly_system_load(model, meta, load(), weeks_ahead=weeks)
    gc.collect()
    return result


@st.cache_data(ttl=3600, max_entries=1)
def get_spike_risk():
    model, meta = get_spike_model()
    result = op.predict_bus_spikes(model, meta, load())
    gc.collect()
    return result


@st.cache_data(ttl=3600, max_entries=1)
def get_parts_fleet():
    model, meta = get_parts_model()
    result = op.predict_parts_probability(model, meta, load())
    gc.collect()
    return result


@st.cache_data(ttl=3600, max_entries=1)
def get_inspection_alerts():
    result = op.compute_inspection_alerts(load())
    gc.collect()
    return result


@st.cache_data(ttl=3600, max_entries=1)
def get_historical_weekly():
    """Build historical weekly correctivo counts for overlay chart.
    Uses same non-mechanical filter as the forecast model."""
    df = load()
    system_col = "sistema_enriched"
    has_no_mec = "es_no_mecanico" in df.columns
    corr = df[df["tipo_servicio"] == "CORRECTIVO"].copy()
    if has_no_mec:
        corr = corr[~((corr["causa_sistema_reconstruida"] == "CARROCERIA") & (corr["es_no_mecanico"] == 1))]
    corr = corr[corr[system_col] != "OTROS"]
    corr["week_dt"] = corr["fecha_evento"].dt.to_period("W").dt.start_time
    hist = corr.groupby([system_col, "taller_planta_grouped", "week_dt"]).size().reset_index(name="n_corr")
    return hist


def fmt_num(n):
    if n >= 1_000_000:
        return f"{n / 1_000_000:.1f}M"
    if n >= 1_000:
        return f"{n / 1_000:.0f}k"
    return str(n)


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════

st.title("🚍 Predicción Operacional — Flota")
df = load()
last_date = df["fecha_evento"].max().strftime("%d/%m/%Y")
st.caption(
    f"{fmt_num(len(df))} eventos · "
    f"{fmt_num(df['placa_patente'].nunique())} buses · "
    f"Actualizado {last_date}"
)

tab_forecast, tab_riesgo, tab_buscar, tab_eval = st.tabs([
    "📅 Pronóstico", "🚨 Riesgo", "🔍 Buscar Bus", "📊 Evaluación"
])

# ═══════════════════════════════════════════════════════════════════════════════
# TAB 1: PRONÓSTICO — Carga semanal + Repuestos
# ═══════════════════════════════════════════════════════════════════════════════

with tab_forecast:
    st.subheader("Pronóstico de Carga de Correctivos")

    terminals = sorted(
        t for t in df["taller_planta_grouped"].unique()
        if t not in ("MISSING",) and pd.notna(t)
    )

    col_f1, col_f2, col_f3 = st.columns([2, 1, 1])
    with col_f1:
        sel_term = st.selectbox("Terminal", ["Todas"] + terminals, key="fc_term")
    with col_f2:
        sel_sys = st.selectbox(
            "Sistema",
            ["Todos"] + sorted(
                s for s in df["sistema_enriched"].unique()
                if s != "OTROS" and pd.notna(s)
            ),
            key="fc_sys",
        )
    with col_f3:
        n_weeks = st.selectbox("Semanas a futuro", [4, 8, 12], index=1, key="fc_weeks")

    with st.spinner("Calculando pronóstico..."):
        forecast = get_weekly_forecast(weeks=n_weeks)
        hist = get_historical_weekly()
        parts_fleet = get_parts_fleet()
        _, meta_weekly = get_weekly_model()
        _, meta_parts = get_parts_model()

    if forecast.empty:
        st.info("Pronóstico no disponible.")
    else:
        # Filter
        fc = forecast
        if sel_term != "Todas":
            fc = fc[fc["terminal"] == sel_term]
        if sel_sys != "Todos":
            fc = fc[fc["sistema"] == sel_sys]

        hi = hist
        if sel_term != "Todas":
            hi = hi[hi["taller_planta_grouped"] == sel_term]
        if sel_sys != "Todos":
            hi = hi[hi["sistema_enriched"] == sel_sys]

        if fc.empty:
            st.warning("Sin datos para los filtros seleccionados.")
        else:
            # ── KPI cards (aligned to calendar weeks from today) ──
            from datetime import date
            today = pd.Timestamp(date.today())
            today_week = today - pd.Timedelta(days=today.dayofweek)
            hist_4w_start = today_week - pd.Timedelta(weeks=4)
            hist_recent = hi[(hi["week_dt"] >= hist_4w_start) & (hi["week_dt"] < today_week)]
            total_hist_4w = int(hist_recent["n_corr"].sum())

            total_fc_4w = fc["pronostico"].sum()
            trend_symbol = "↑" if total_fc_4w > total_hist_4w else "↓" if total_fc_4w < total_hist_4w else "→"

            m1, m2, m3, m4 = st.columns(4)
            m1.metric(
                "📊 Histórico (últimas 4 sem completas)",
                f"{total_hist_4w} correctivos",
            )
            m2.metric(
                f"🔮 Pronóstico (próx. {n_weeks} sem)",
                f"{total_fc_4w:.0f} correctivos",
                delta=f"{total_fc_4w - total_hist_4w:+.0f} vs histórico {trend_symbol}",
            )
            m3.metric(
                f"📅 Semana pico",
                f"{pd.Timestamp(fc.groupby('semana')['pronostico'].sum().idxmax()).strftime('%d/%m')}",
            )
            m4.metric(
                f"⚡ Promedio semanal",
                f"{(total_fc_4w / n_weeks):.0f} correctivos",
            )

            st.divider()

            # ── Forecast vs Historical chart ──
            st.subheader("Pronóstico semanal con histórico")

            # Aggregate
            hist_agg = hi.groupby("week_dt")["n_corr"].sum().reset_index()
            hist_agg.columns = ["semana", "n_corr"]
            hist_agg = hist_agg[hist_agg["semana"] >= hist_agg["semana"].max() - pd.Timedelta(weeks=12)]

            fc_agg = fc.groupby("semana")[["pronostico", "confianza_baja", "confianza_alta"]].sum().reset_index()
            fc_agg["semana"] = pd.to_datetime(fc_agg["semana"])

            fig = go.Figure()

            # Historical bars
            if not hist_agg.empty:
                fig.add_trace(go.Bar(
                    x=hist_agg["semana"], y=hist_agg["n_corr"],
                    name="Histórico", marker_color="#64748b",
                    hovertemplate="Semana %{x|%d/%m}<br>Histórico: %{y}<extra></extra>",
                ))

            # Forecast line
            if not fc_agg.empty:
                fig.add_trace(go.Scatter(
                    x=fc_agg["semana"], y=fc_agg["pronostico"],
                    name="Pronóstico", mode="lines+markers",
                    line=dict(color="#ef4444", width=3),
                    marker=dict(size=8),
                    hovertemplate="Semana %{x|%d/%m}<br>Pronóstico: %{y:.0f}<extra></extra>",
                ))

                # Confidence band
                fig.add_trace(go.Scatter(
                    x=pd.concat([fc_agg["semana"], fc_agg["semana"][::-1]]),
                    y=pd.concat([fc_agg["confianza_alta"], fc_agg["confianza_baja"][::-1]]),
                    fill="toself", fillcolor="rgba(239,68,68,0.15)",
                    line=dict(color="rgba(239,68,68,0)"),
                    name="Banda confianza",
                    hoverinfo="skip",
                ))

            fig.update_layout(
                height=350, margin=dict(l=0, r=0, t=10, b=0),
                xaxis_title="", yaxis_title="Correctivos por semana",
                hovermode="x unified",
                legend=dict(orientation="h", y=1.15, x=0),
                barmode="overlay",
            )
    st.plotly_chart(fig, width="stretch")


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 4: EVALUACIÓN — Métricas detalladas de modelos
# ═══════════════════════════════════════════════════════════════════════════════

with tab_eval:
    st.subheader("📊 Evaluación de Modelos")

    _, meta_w = get_weekly_model()
    _, meta_s = get_spike_model()
    _, meta_p = get_parts_model()

    # ── Weekly model ──
    st.markdown("### 📅 Pronóstico de Carga Semanal")

    cv_folds_w = meta_w.get("cv_folds", [])
    cw1, cw2, cw3, cw4 = st.columns(4)
    if cv_folds_w:
        r2s = [f["r2"] for f in cv_folds_w if f.get("r2") is not None]
        maes = [f["mae"] for f in cv_folds_w if f.get("mae") is not None]
        cw1.metric("R² (CV)", f"{np.mean(r2s):.3f} ± {np.std(r2s):.3f}")
        cw2.metric("MAE (CV)", f"{np.mean(maes):.1f} ± {np.std(maes):.1f}")
        cw3.metric("Folds", str(len(cv_folds_w)))
    else:
        cw1.metric("R² test", f"{meta_w.get('test_r2', 0):.3f}" if meta_w.get("test_r2") is not None else "—")
        cw2.metric("MAE", f"{meta_w.get('test_mae', 0):.1f}" if meta_w.get("test_mae") is not None else "—")
    cw4.metric("Filas train", str(meta_w.get("n_train", "—")))

    top_feat_w = meta_w.get("top_features", [])
    if top_feat_w:
        feat_names = [f[0] if isinstance(f, (list, tuple)) else str(f) for f in top_feat_w[:8]]
        feat_vals = [f[1] if isinstance(f, (list, tuple)) else 0 for f in top_feat_w[:8]]
        fig_fw = go.Figure(go.Bar(
            x=feat_vals, y=feat_names, orientation="h",
            marker_color="#3b82f6",
        ))
        fig_fw.update_layout(height=220, margin=dict(l=0, r=0, t=5, b=0), xaxis_title="Importancia")
        st.plotly_chart(fig_fw, width="stretch")

    # Per-fold breakdown
    if cv_folds_w:
        st.caption("Desglose por fold")
        fold_df = pd.DataFrame([{
            "Fold": f["fold"], "R²": f.get("r2"), "MAE": f.get("mae"),
            "Train": f["n_train"], "Test": f["n_test"],
        } for f in cv_folds_w])
        st.dataframe(fold_df, hide_index=True, width="stretch")

    st.divider()

    # ── Spike model ──
    st.markdown("### 🔮 Riesgo de Pico de Correctivos")

    cv_folds_s = meta_s.get("cv_folds", [])
    cs1, cs2, cs3, cs4, cs5 = st.columns(5)
    if cv_folds_s:
        aucs = [f["roc_auc"] for f in cv_folds_s if f.get("roc_auc") is not None]
        f1s = [f["f1"] for f in cv_folds_s if f.get("f1") is not None]
        ps = [f["precision"] for f in cv_folds_s if f.get("precision") is not None]
        rs = [f["recall"] for f in cv_folds_s if f.get("recall") is not None]
        cs1.metric("AUC (CV)", f"{np.mean(aucs):.3f} ± {np.std(aucs):.3f}")
        cs2.metric("F1 (CV)", f"{np.mean(f1s):.3f} ± {np.std(f1s):.3f}")
        cs3.metric("Precision", f"{np.mean(ps):.3f}")
        cs4.metric("Recall", f"{np.mean(rs):.3f}")
    else:
        cs1.metric("AUC", f"{meta_s.get('test_auc', 0):.3f}" if meta_s.get("test_auc") is not None else "—")
        cs2.metric("F1", f"{meta_s.get('test_f1', 0):.3f}" if meta_s.get("test_f1") is not None else "—")
    cs5.metric("Pos rate", f"{meta_s.get('pos_rate', 0):.1%}")

    top_feat_s = meta_s.get("top_features", [])
    if top_feat_s:
        feat_names_s = [f[0] if isinstance(f, (list, tuple)) else str(f) for f in top_feat_s[:8]]
        feat_vals_s = [f[1] if isinstance(f, (list, tuple)) else 0 for f in top_feat_s[:8]]
        fig_fs = go.Figure(go.Bar(
            x=feat_vals_s, y=feat_names_s, orientation="h",
            marker_color="#ef4444",
        ))
        fig_fs.update_layout(height=220, margin=dict(l=0, r=0, t=5, b=0), xaxis_title="Importancia")
        st.plotly_chart(fig_fs, width="stretch")

    # Confusion matrix (last fold)
    if cv_folds_s:
        last_fold = cv_folds_s[-1]
        cm = last_fold.get("confusion_matrix")
        if cm and len(cm) == 2:
            st.caption("Matriz de confusión (último fold)")
            tn, fp = cm[0]
            fn, tp = cm[1]
            fig_cm = go.Figure(data=go.Heatmap(
                z=[[tn, fp], [fn, tp]],
                x=["Pred Normal", "Pred Spike"],
                y=["Real Normal", "Real Spike"],
                text=[[str(tn), str(fp)], [str(fn), str(tp)]],
                texttemplate="%{text}",
                colorscale=[(0, "#f0f0f0"), (1, "#ef4444")],
                showscale=False,
            ))
            fig_cm.update_layout(height=200, margin=dict(l=0, r=0, t=5, b=0))
            st.plotly_chart(fig_cm, width="stretch")

    st.divider()

    # ── Parts model ──
    st.markdown("### 🔧 Probabilidad de Repuestos")

    cv_folds_p = meta_p.get("cv_folds", [])
    cp1, cp2, cp3, cp4, cp5 = st.columns(5)
    if cv_folds_p:
        aucs_p = [f["roc_auc"] for f in cv_folds_p if f.get("roc_auc") is not None]
        f1s_p = [f["f1"] for f in cv_folds_p if f.get("f1") is not None]
        ps_p = [f["precision"] for f in cv_folds_p if f.get("precision") is not None]
        rs_p = [f["recall"] for f in cv_folds_p if f.get("recall") is not None]
        cp1.metric("AUC (CV)", f"{np.mean(aucs_p):.3f} ± {np.std(aucs_p):.3f}")
        cp2.metric("F1 (CV)", f"{np.mean(f1s_p):.3f} ± {np.std(f1s_p):.3f}")
        cp3.metric("Precision", f"{np.mean(ps_p):.3f}")
        cp4.metric("Recall", f"{np.mean(rs_p):.3f}")
    else:
        cp1.metric("AUC", f"{meta_p.get('test_auc', 0):.3f}" if meta_p.get("test_auc") is not None else "—")
        cp2.metric("F1", f"{meta_p.get('test_f1', 0):.3f}" if meta_p.get("test_f1") is not None else "—")
    cp5.metric("Pos rate", f"{meta_p.get('pos_rate', 0):.1%}")

    top_feat_p = meta_p.get("top_features", [])
    if top_feat_p:
        feat_names_p = [f[0] if isinstance(f, (list, tuple)) else str(f) for f in top_feat_p[:8]]
        feat_vals_p = [f[1] if isinstance(f, (list, tuple)) else 0 for f in top_feat_p[:8]]
        fig_fp = go.Figure(go.Bar(
            x=feat_vals_p, y=feat_names_p, orientation="h",
            marker_color="#22c55e",
        ))
        fig_fp.update_layout(height=220, margin=dict(l=0, r=0, t=5, b=0), xaxis_title="Importancia")
        st.plotly_chart(fig_fp, width="stretch")

    # Confusion matrix (last fold)
    if cv_folds_p:
        last_fold_p = cv_folds_p[-1]
        cm_p = last_fold_p.get("confusion_matrix")
        if cm_p and len(cm_p) == 2:
            st.caption("Matriz de confusión (último fold)")
            tn, fp = cm_p[0]
            fn, tp = cm_p[1]
            fig_cmp = go.Figure(data=go.Heatmap(
                z=[[tn, fp], [fn, tp]],
                x=["Pred Sin Rep.", "Pred Con Rep."],
                y=["Real Sin Rep.", "Real Con Rep."],
                text=[[str(tn), str(fp)], [str(fn), str(tp)]],
                texttemplate="%{text}",
                colorscale=[(0, "#f0f0f0"), (1, "#22c55e")],
                showscale=False,
            ))
            fig_cmp.update_layout(height=200, margin=dict(l=0, r=0, t=5, b=0))
            st.plotly_chart(fig_cmp, width="stretch")

            # ── Per-system stacked area ──
            st.subheader("Tendencia por sistema")
            sistemas_top = (
                fc.groupby("sistema")["pronostico"].sum()
                .sort_values(ascending=False).head(7).index.tolist()
            )

            colores_sis = {
                "MOTOR": "#ef4444", "FRENOS": "#f97316", "ELECTRICO": "#3b82f6",
                "CARROCERIA": "#ec4899", "PUERTAS": "#eab308",
                "SUSPENSION": "#22c55e", "CLIMATIZACION": "#8b5cf6",
                "RUEDAS": "#14b8a6",
            }

            # Historical per-system
            fig2 = make_subplots(
                rows=1, cols=2,
                subplot_titles=("Histórico (12 semanas)", f"Pronóstico ({n_weeks} semanas)"),
                column_widths=[0.55, 0.45],
            )

            hist_sys = hi[hi["week_dt"] >= hi["week_dt"].max() - pd.Timedelta(weeks=12)]
            for sis in sistemas_top:
                sis_hist = hist_sys[hist_sys["sistema_enriched"] == sis].groupby("week_dt")["n_corr"].sum()
                if not sis_hist.empty:
                    fig2.add_trace(go.Bar(
                        x=sis_hist.index, y=sis_hist.values,
                        name=sis, marker_color=colores_sis.get(sis, "#a1a1aa"),
                        showlegend=True,
                    ), row=1, col=1)

                sis_fc = fc[fc["sistema"] == sis].groupby("semana")["pronostico"].sum()
                if not sis_fc.empty:
                    fig2.add_trace(go.Bar(
                        x=sis_fc.index, y=sis_fc.values,
                        name=sis, marker_color=colores_sis.get(sis, "#a1a1aa"),
                        showlegend=False,
                    ), row=1, col=2)

            fig2.update_layout(
                height=300, barmode="stack",
                margin=dict(l=0, r=0, t=30, b=0),
                legend=dict(orientation="h", y=1.2),
            )
            fig2.update_xaxes(title_text="", row=1, col=1)
            fig2.update_xaxes(title_text="", row=1, col=2)
            fig2.update_yaxes(title_text="Correctivos", row=1, col=1)
            fig2.update_yaxes(title_text="", row=1, col=2)
            st.plotly_chart(fig2, width="stretch")

            # ── Detail table ──
            st.divider()
            st.caption("Detalle por sistema y semana")

            tabla = fc.pivot_table(
                index="sistema", columns="semana", values="pronostico",
                aggfunc="sum", fill_value=0,
            )
            tabla.columns = [pd.Timestamp(c).strftime("%d/%m") for c in tabla.columns]
            tabla["Total"] = tabla.sum(axis=1).round(0).astype(int)
            for c in tabla.columns:
                if c != "Total":
                    tabla[c] = tabla[c].round(0).astype(int)
            tabla = tabla.sort_values("Total", ascending=False)
            st.dataframe(tabla, width="stretch")

            # ── Parts section ──
            st.divider()
            st.subheader("🔧 Estimación de Repuestos")

            if not parts_fleet.empty:
                total_por_sistema = fc.groupby("sistema")["pronostico"].sum().reset_index()
                total_por_sistema.columns = ["sistema", "eventos_esperados"]

                merged = total_por_sistema.merge(parts_fleet, on="sistema", how="left")
                merged["prob_repuestos"] = merged["prob_repuestos"].fillna(0.5)
                merged["repuestos_estimados"] = (
                    merged["eventos_esperados"] * merged["prob_repuestos"]
                ).round(0).astype(int)
                merged = merged.sort_values("repuestos_estimados", ascending=False)

                st.caption(
                    "Estimación basada en probabilidad histórica de uso de repuestos por sistema. "
                    "Cifras en eventos (no unidades de repuesto)."
                )

                st.dataframe(
                    merged,
                    width="stretch", hide_index=True,
                    column_config={
                        "sistema": "Sistema",
                        "eventos_esperados": st.column_config.NumberColumn(
                            f"Eventos esperados ({n_weeks} sem)", format="%d",
                        ),
                        "prob_repuestos": st.column_config.ProgressColumn(
                            "Prob. necesita repuestos", format="%.0f%%",
                            min_value=0, max_value=1,
                        ),
                        "repuestos_estimados": st.column_config.NumberColumn(
                            "Eventos con repuestos (est.)", format="%d",
                        ),
                    },
                )

                total_con_rep = int(merged["repuestos_estimados"].sum())
                total_ev = int(merged["eventos_esperados"].sum())
                st.metric(
                    "Total estimado",
                    f"{total_con_rep} de {total_ev} eventos necesitarán repuestos "
                    f"({total_con_rep / max(total_ev, 1) * 100:.0f}%)",
                )

            # ── Inspection forecast ──
            st.divider()
            st.subheader("🛡️ Próximas Inspecciones (REGB/IT)")

            model_i, meta_i = get_inspection_model()
            insp_forecast = op.predict_inspection_risk(model_i, meta_i, load()) if model_i else pd.DataFrame()

            if sel_term != "Todas" and not insp_forecast.empty:
                insp_forecast = insp_forecast[insp_forecast["taller"] == sel_term]

            if not insp_forecast.empty:
                urg = insp_forecast[insp_forecast["riesgo"] == "🔴 Urgente"]
                venc = insp_forecast[insp_forecast["riesgo"] == "🟠 Vencida"]
                prox = insp_forecast[(insp_forecast["dias_para_prox"] > 0) & (insp_forecast["dias_para_prox"] <= 30)]
                avg_readiness = insp_forecast["readiness"].mean() if "readiness" in insp_forecast.columns else 0

                mi1, mi2, mi3, mi4 = st.columns(4)
                mi1.metric("🔴 Urgentes", len(urg))
                mi2.metric("🟠 Vencidas", len(venc))
                mi3.metric("📅 Próx. 30 días", len(prox))
                mi4.metric("🩺 Readiness prom.", f"{avg_readiness:.0f}/100")

                show_insp = insp_forecast[
                    insp_forecast["riesgo"].isin(["🔴 Urgente", "🟠 Vencida", "🟡 Alto riesgo", "🔵 Riesgo medio"])
                ].head(15)
                if not show_insp.empty:
                    show_insp["ultima_fecha"] = pd.to_datetime(show_insp["ultima_fecha"]).dt.strftime("%d/%m/%Y")
                    cols_show = ["placa_patente", "tipo", "dias_para_prox", "prob_falla", "readiness",
                                 "riesgo", "sistemas_riesgo", "accion_recomendada",
                                 "correctivos_desde", "ultima_fecha", "taller"]
                    cols_show = [c for c in cols_show if c in show_insp.columns]
                    st.dataframe(
                        show_insp[cols_show],
                        width="stretch", hide_index=True,
                        column_config={
                            "placa_patente": "Bus",
                            "tipo": "Tipo",
                            "dias_para_prox": st.column_config.NumberColumn("Días próx.", format="%.0f"),
                            "prob_falla": st.column_config.ProgressColumn("Prob. falla", format="%.0f%%", min_value=0, max_value=1),
                            "readiness": st.column_config.ProgressColumn("Readiness", format="%.0f", min_value=0, max_value=100),
                            "riesgo": "Riesgo",
                            "sistemas_riesgo": st.column_config.TextColumn("Sistemas en riesgo", width="medium"),
                            "accion_recomendada": st.column_config.TextColumn("Acción recomendada", width="large"),
                            "correctivos_desde": st.column_config.NumberColumn("Correct.", format="%d"),
                            "ultima_fecha": "Última insp.",
                            "taller": "Taller",
                        },
                    )
            else:
                st.info("Modelo de inspección no disponible. Ejecutá scripts/train_models.py para generarlo.")

            # ── Model metrics ──
            st.divider()
            st.caption("📊 Métricas de modelos")
            mc1, mc2, mc3, mc4 = st.columns(4)

            cvr2m = meta_weekly.get("cv_r2_mean")
            cvr2s = meta_weekly.get("cv_r2_std")
            if cvr2m is not None:
                mc1.metric("R² (CV)", f"{cvr2m:.3f} ± {cvr2s:.3f}")
            else:
                wr2_test = meta_weekly.get("test_r2")
                mc1.metric("R² test", f"{wr2_test:.3f}" if wr2_test is not None else "—")

            cvmae = meta_weekly.get("cv_mae_mean")
            mc2.metric("MAE (CV)", f"{cvmae:.1f}" if cvmae is not None else "—")
            mc3.metric("Repuestos AUC", f"{meta_parts.get('cv_auc_mean', 0):.3f}" if meta_parts.get("cv_auc_mean") is not None else "—")

            top_feat_w = meta_weekly.get("top_features", [])[:3]
            if top_feat_w:
                mc4.metric("Top feature", top_feat_w[0][0] if isinstance(top_feat_w[0], (list, tuple)) else str(top_feat_w[0]))

# ═══════════════════════════════════════════════════════════════════════════════
# TAB 2: RIESGO — Health Score + Spike Prediction + Alertas
# ═══════════════════════════════════════════════════════════════════════════════

with tab_riesgo:
    with st.spinner("Calculando scores..."):
        health = get_health_scores()
        obs_alerts = get_observation_alerts()
        spikes = get_spike_risk()
        insp_alerts = get_inspection_alerts()
        _, meta_spike = get_spike_model()

    if health.empty:
        st.info("Sin datos.")
        st.stop()

    # ── KPIs ──
    bins = [0, 20, 40, 70, 101]
    health_labels = ["🟢 Normal", "🟡 Moderado", "🟠 Atención", "🔴 Crítico"]
    health["grupo"] = pd.cut(health["health_score"], bins=bins, labels=health_labels, right=False)
    dist_h = health["grupo"].value_counts().reindex(health_labels, fill_value=0)

    n_alto = len(spikes[spikes["riesgo"] == "🔴 Alto"])
    n_medio = len(spikes[spikes["riesgo"] == "🟠 Medio"])

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("🚌 Buses", len(health))
    c2.metric("🔴 Críticos (score≥70)", int(dist_h["🔴 Crítico"]))
    c3.metric("📈 Riesgo pico alto", n_alto)
    c4.metric("⚠️ Riesgo pico medio", n_medio)
    c5.metric("🔍 Insp. pendientes", len(insp_alerts))

    st.caption(
        f"📊 Métricas: Spike AUC={meta_spike.get('test_auc', '—')}"
        if meta_spike.get("test_auc") is not None
        else "📊 Métricas de modelo en cálculo..."
    )

    st.divider()

    # ── Score distribution ──
    col_a, col_b = st.columns([3, 2])

    with col_a:
        st.subheader("Distribución de Scores de Salud")
        colors = {"🟢 Normal": "#22c55e", "🟡 Moderado": "#eab308",
                  "🟠 Atención": "#f97316", "🔴 Crítico": "#ef4444"}
        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=dist_h.index, y=dist_h.values,
            marker_color=[colors[l] for l in dist_h.index],
            text=dist_h.values, textposition="outside",
        ))
        fig.update_layout(height=250, xaxis_title="", yaxis_title="Buses",
                          margin=dict(l=0, r=0, t=10, b=0))
        st.plotly_chart(fig, width="stretch")

    with col_b:
        st.subheader("🚨 Alertas recientes")
        if obs_alerts.empty:
            st.success("Sin alertas en los últimos 30 días.")
        else:
            st.warning(f"{len(obs_alerts)} buses con alertas")
            for _, r in obs_alerts.head(8).iterrows():
                st.markdown(
                    f"- **{r['placa_patente']}** — {r['alerta']} "
                    f"({int(r['eventos'])} eventos)"
                )

    st.divider()

    # ── Spike risk table ──
    st.subheader("🔮 Buses con riesgo de pico de correctivos el próximo mes")

    col_t1, col_t2 = st.columns([1, 1])
    with col_t1:
        riesgo_filter = st.selectbox(
            "Nivel de riesgo",
            ["🔴 Alto", "🟠 Medio", "🟡 Bajo", "Todos"],
            key="risk_filter",
        )
    with col_t2:
        spike_show = spikes
        if riesgo_filter != "Todos":
            spike_show = spike_show[spike_show["riesgo"] == riesgo_filter]

    if not spike_show.empty:
        display = spike_show.head(30)
        display["prob_spike"] = (display["prob_spike"] * 100).round(0).astype(int)

        def _recomendacion(row):
            if row["riesgo"] == "🔴 Alto":
                return f"Programar preventivo — último: {row['ultimo_sistema']}"
            if row["riesgo"] == "🟠 Medio":
                return f"Monitorear sistema {row['ultimo_sistema']}"
            return "Seguimiento normal"

        display["recomendacion"] = display.apply(_recomendacion, axis=1)

        st.dataframe(
            display[["placa_patente", "prob_spike", "riesgo",
                     "correctivos_30d", "ultimo_sistema", "recomendacion"]],
            width="stretch", hide_index=True,
            column_config={
                "placa_patente": "Bus",
                "prob_spike": st.column_config.ProgressColumn(
                    "Probabilidad pico", format="%d%%", min_value=0, max_value=100,
                ),
                "riesgo": "Riesgo",
                "correctivos_30d": st.column_config.NumberColumn(
                    "Correctivos 30d", format="%d",
                ),
                "ultimo_sistema": "Último sistema",
                "recomendacion": st.column_config.TextColumn(
                    "Recomendación", width="large",
                ),
            },
        )

    st.divider()

    # ── Inspection alerts ──
    if not insp_alerts.empty:
        st.subheader("🔍 Buses con inspección rechazada sin correctivo de seguimiento")

        urgentes = insp_alerts[insp_alerts["riesgo"] == "🔴 Urgente"]
        if not urgentes.empty:
            st.error(
                f"{len(urgentes)} buses con inspección rechazada hace ≤7 días "
                "y sin correctivo registrado"
            )

        display_insp = insp_alerts.head(30)
        display_insp["fecha_inspeccion"] = pd.to_datetime(
            display_insp["fecha_inspeccion"]
        ).dt.strftime("%d/%m/%Y")

        st.dataframe(
            display_insp[[
                "placa_patente", "riesgo", "dias_desde",
                "fecha_inspeccion", "defectos_highs", "defectos_totales",
                "observacion", "taller",
            ]],
            width="stretch", hide_index=True,
            column_config={
                "placa_patente": "Bus",
                "riesgo": "Prioridad",
                "dias_desde": st.column_config.NumberColumn(
                    "Días desde inspección", format="%d",
                ),
                "fecha_inspeccion": "Fecha inspección",
                "defectos_highs": st.column_config.NumberColumn(
                    "Defectos altos", format="%d",
                ),
                "defectos_totales": st.column_config.NumberColumn(
                    "Total defectos", format="%d",
                ),
                "observacion": st.column_config.TextColumn(
                    "Observación", width="large",
                ),
                "taller": "Taller",
            },
        )


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 3: BUSCAR BUS
# ═══════════════════════════════════════════════════════════════════════════════

with tab_buscar:
    st.subheader("Buscar un bus")

    search = st.text_input("Patente del bus", placeholder="Ej: LDJW36", key="bs")
    if not search:
        st.info("Ingresa una patente para ver predicciones y eventos.")
        st.stop()

    search = search.upper().strip()
    bus_data = df[df["placa_patente"] == search]
    if bus_data.empty:
        st.error(f"Patente **{search}** no encontrada.")
        st.stop()

    # ── KPIs ──
    hs = get_health_scores()
    bus_hs = hs[hs["placa_patente"] == search]
    hs_val = bus_hs["health_score"].values[0] if not bus_hs.empty else None

    sp = get_spike_risk()
    bus_sp = sp[sp["placa_patente"] == search]
    spike_prob = bus_sp["prob_spike"].values[0] if not bus_sp.empty else None
    spike_risk = bus_sp["riesgo"].values[0] if not bus_sp.empty else "—"

    model_p, meta_p = get_parts_model()
    parts_pred = op.predict_parts_probability(model_p, meta_p, load(), bus=search)

    oa = get_observation_alerts()
    bus_oa = oa[oa["placa_patente"] == search]

    ck1, ck2, ck3, ck4, ck5 = st.columns(5)
    ck1.metric("📅 Eventos totales", len(bus_data))
    ck2.metric("🔧 Correctivos", (bus_data["tipo_servicio"] == "CORRECTIVO").sum())
    ck3.metric("⚙️ Sistemas", bus_data["causa_sistema_reconstruida"].nunique())
    cat = predictive.score_category(hs_val) if hs_val else "—"
    ck4.metric("🩺 Score Salud", f"{hs_val:.0f}/100" if hs_val else "—")
    ck5.metric(
        "📈 Riesgo pico",
        f"{int(spike_prob * 100)}%" if spike_prob is not None else "—",
        delta=spike_risk,
    )

    # ── Parts probability ──
    if isinstance(parts_pred, dict) and "error" not in parts_pred:
        prob_p = parts_pred.get("prob_repuestos", 0)
        st.info(
            f"🔧 **Probabilidad de que el próximo correctivo necesite repuestos: "
            f"{prob_p * 100:.0f}%** (último sistema: {parts_pred.get('ultimo_sistema', '—')})"
        )

    # ── Inspection history ──
    insp_hist = op.bus_inspection_history(load(), search)
    if insp_hist.get("has_inspections"):
        st.divider()
        st.subheader("🛡️ Historial de Inspecciones (REGB/IT)")

        c_i1, c_i2, c_i3, c_i4 = st.columns(4)
        c_i1.metric("Total inspecciones", insp_hist["total_inspections"])
        estado = "✅ Aprobada" if insp_hist["passed"] else "❌ Rechazada"
        c_i2.metric("Última inspección", estado)
        c_i3.metric("Fecha", insp_hist["last_date"].strftime("%d/%m/%Y"))
        c_i4.metric("Defectos altos", insp_hist["defectos_highs"])

        if insp_hist["observacion"]:
            st.caption(f"Observación: _{insp_hist['observacion']}_")

        if insp_hist["sistemas_detectados"]:
            st.markdown(
                "Sistemas detectados en observación: "
                + ", ".join(insp_hist["sistemas_detectados"])
            )

        if not insp_hist["passed"]:
            st.warning(
                "⚠️ Este bus falló su última inspección. "
                "Verificar si requiere correctivo de seguimiento."
            )

    # ── Alertas ──
    if not bus_oa.empty:
        st.warning("⚠️ Alertas en últimos 30 días")
        for _, r in bus_oa.iterrows():
            st.markdown(
                f"- **{r['alerta']}** ({int(r['eventos'])} eventos) — "
                f"Últ: {pd.to_datetime(r['ultimo']).strftime('%d/%m/%Y')}"
            )

    st.divider()

    # ── Timeline ──
    st.subheader("Últimos eventos")
    timeline = bus_data.sort_values("fecha_evento", ascending=False).head(30)
    timeline["fecha"] = pd.to_datetime(timeline["fecha_evento"]).dt.strftime("%d/%m/%Y")
    timeline["obs"] = timeline["observacion_clean"].fillna(
        timeline["causa_origen_clean"]
    )
    st.dataframe(
        timeline[[
            "fecha", "tipo_servicio", "causa_sistema_reconstruida",
            "taller_planta_grouped", "duracion_ot_horas", "obs",
        ]],
        width="stretch", hide_index=True,
        column_config={
            "fecha": "Fecha",
            "tipo_servicio": "Tipo",
            "causa_sistema_reconstruida": "Sistema",
            "taller_planta_grouped": "Taller",
            "duracion_ot_horas": st.column_config.NumberColumn(
                "Horas", format="%.1f",
            ),
            "obs": st.column_config.TextColumn("Observación", width="large"),
        },
    )

    # ── Event strip ──
    fig = go.Figure()
    colores_bus = {
        "CORRECTIVO": "#ef4444", "PREVENTIVO": "#22c55e",
        "REGB": "#3b82f6", "IT": "#8b5cf6",
    }
    for t in bus_data["tipo_servicio"].unique():
        sub = bus_data[bus_data["tipo_servicio"] == t]
        fig.add_trace(go.Scatter(
            x=sub["fecha_evento"], y=[1] * len(sub),
            mode="markers", name=t,
            marker=dict(size=8, color=colores_bus.get(t, "#6b7280")),
            hovertext=sub.apply(
                lambda r: f"{r['tipo_servicio']} | {r['causa_sistema_reconstruida']}",
                axis=1,
            ),
            hovertemplate="%{x}<br>%{hovertext}<extra></extra>",
        ))
    fig.update_layout(
        height=120, xaxis_title="", yaxis_visible=False,
        margin=dict(l=0, r=0, t=0, b=0), hovermode="closest",
    )
    st.plotly_chart(fig, width="stretch")
