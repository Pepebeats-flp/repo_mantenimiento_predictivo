"""Time-series forecasting, change point detection, and seasonal analysis.
Models: Prophet, XGBoost (existing), statsmodels seasonal decompose, ruptures PELT.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
from prophet import Prophet
from pathlib import Path


# ═══════════════════════════════════════════════════════════════════════════
# PROPHET FORECASTING
# ═══════════════════════════════════════════════════════════════════════════

def daily_series(df: pd.DataFrame, group_col: str | None = None,
                 system: str | None = None) -> pd.DataFrame:
    """Build daily time series: ds=fecha, y=count.
    - group_col: taller_planta_grouped for per-terminal, None for fleet-wide
    - system: filter by causa_sistema_reconstruida (FRENOS, MOTOR, etc)
    """
    d = df.copy()
    d["ds"] = d["fecha_evento"].dt.date
    d["ds"] = pd.to_datetime(d["ds"])

    if system:
        d = d[d["causa_sistema_reconstruida"] == system]

    if group_col:
        daily = d.groupby(["ds", group_col]).size().reset_index(name="y")
        daily = daily.rename(columns={group_col: "group"})
    else:
        daily = d.groupby("ds").size().reset_index(name="y")

    return daily.sort_values("ds").reset_index(drop=True)


def train_prophet(
    df: pd.DataFrame,
    group_col: str | None = "taller_planta_grouped",
    systems: list[str] | None = None,
    term: str | None = None,
    days_ahead: int = 14,
) -> dict:
    """Train Prophet models per terminal×system combo.
    Returns dict keyed by label → {model, forecast, mae, trend_changepoints}
    """
    results = {}
    systems = systems or ["__TOTAL__"]
    label_systems = systems.copy()

    # Replace __TOTAL__ with actual train (no system filter)
    for i, s in enumerate(systems):
        pass  # handled below

    terms = df[group_col].dropna().unique().tolist() if group_col else ["__ALL__"]

    for t in terms:
        if term and t != term:
            continue
        for sys_label in label_systems:
            label = f"{t} | {sys_label}"
            sys_filter = sys_label if sys_label != "__TOTAL__" else None
            daily = daily_series(df, group_col=group_col, system=sys_filter)
            if group_col:
                daily = daily[daily["group"] == t]

            if len(daily) < 30:
                continue

            m = Prophet(
                changepoint_prior_scale=0.05,
                seasonality_mode="additive",
                yearly_seasonality=True,
                weekly_seasonality=True,
                daily_seasonality=False,
            )
            m.fit(daily[["ds", "y"]])

            future = m.make_future_dataframe(periods=days_ahead)
            fc = m.predict(future)
            fc["yhat"] = fc["yhat"].clip(lower=0)

            # In-sample error
            train_fc = fc[fc["ds"].isin(daily["ds"])]
            mae = np.mean(np.abs(train_fc["yhat"].values - daily["y"].values))

            # Changepoints
            cps = []
            for cp in m.changepoints:
                cps.append(cp.strftime("%Y-%m-%d"))

            results[label] = {
                "model": m,
                "forecast": fc,
                "daily": daily,
                "mae": mae,
                "changepoints": cps,
                "n_events": len(daily),
            }

    return results


def prophet_forecast_df(results: dict, label: str, days_ahead: int = 14) -> pd.DataFrame:
    """Extract forecast + historical fit as a clean DataFrame for plotting."""
    r = results.get(label)
    if not r:
        return pd.DataFrame()
    fc = r["forecast"]
    daily = r["daily"]
    fc_out = fc[fc["ds"] > daily["ds"].max()][["ds", "yhat", "yhat_lower", "yhat_upper"]].copy()
    fc_out.columns = ["fecha", "pronostico", "lo80", "hi80"]
    fc_out["tipo"] = "pronóstico"
    hist = daily[["ds", "y"]].copy()
    hist.columns = ["fecha", "pronostico"]
    hist["lo80"] = hist["pronostico"]
    hist["hi80"] = hist["pronostico"]
    hist["tipo"] = "real"
    return pd.concat([hist.tail(90), fc_out], ignore_index=True)


# ═══════════════════════════════════════════════════════════════════════════
# CHANGE POINT DETECTION (PELT)
# ═══════════════════════════════════════════════════════════════════════════

def detect_change_points(
    df: pd.DataFrame,
    group_col: str = "taller_planta_grouped",
    metric: str = "total",
    penalty: float = 10.0,
) -> dict[str, list]:
    """Detect structural break dates per terminal or fleet-wide.
    metric: 'total' (all events), 'correctivos', or any sistema like 'FRENOS'
    """
    from ruptures.detection import Pelt
    from ruptures.costs import CostL2

    results = {}

    groups = df[group_col].dropna().unique().tolist() if group_col != "__ALL__" else ["__ALL__"]
    if '__ALL__' in groups and group_col != '__ALL__':
        groups = df[group_col].dropna().unique().tolist()

    for g in groups:
        if metric in ("total", "correctivos"):
            daily = daily_series(
                df[df[group_col] == g] if group_col != "__ALL__" else df,
                group_col=None,
            )
            if metric == "correctivos":
                df_c = df[df["tipo_servicio"] == "CORRECTIVO"]
                daily = daily_series(
                    df_c[df_c[group_col] == g] if group_col != "__ALL__" else df_c,
                    group_col=None,
                )
        else:
            daily = daily_series(df, group_col=None, system=metric)
            if group_col != "__ALL__":
                daily = daily[daily["gs"] == g] if "group" in daily.columns else daily

        signal = daily["y"].values
        if len(signal) < 10:
            continue

        algo = Pelt(model="l2", min_size=14).fit(signal)
        bps = algo.predict(pen=penalty)
        dates = [daily["ds"].iloc[b - 1].strftime("%Y-%m-%d") for b in bps[:-1]]
        results[g] = dates

    return results


# ═══════════════════════════════════════════════════════════════════════════
# SEASONAL DECOMPOSITION
# ═══════════════════════════════════════════════════════════════════════════

def seasonal_decompose(
    df: pd.DataFrame,
    group_col: str | None = "taller_planta_grouped",
    term: str | None = None,
    group: str | None = None,
    period: int = 7,
) -> dict:
    """Decompose daily event count into trend + seasonal + residual.
    Uses statsmodels STL decomposition.
    """
    from statsmodels.tsa.seasonal import STL

    if group_col and term:
        daily = daily_series(df, group_col=group_col)
        daily = daily[daily["group"] == term]
    elif group_col and group:
        daily = daily_series(df, group_col=group_col)
        daily = daily[daily["group"] == group]
    else:
        daily = daily_series(df)

    if len(daily) < period * 4:
        return {}

    daily = daily.set_index("ds").asfreq("D")
    daily["y"] = daily["y"].fillna(0)

    stl = STL(daily["y"], period=period, robust=True)
    res = stl.fit()

    return {
        "trend": res.trend,
        "seasonal": res.seasonal,
        "resid": res.resid,
        "dates": daily.index,
        "y": daily["y"],
    }


# ═══════════════════════════════════════════════════════════════════════════
# FLEET HEALTH TRAJECTORY
# ═══════════════════════════════════════════════════════════════════════════

def health_score_trajectory(
    df: pd.DataFrame,
    freq: str = "W",
    term: str | None = None,
) -> pd.DataFrame:
    """Compute aggregate health score trajectory over time (weekly by default).
    Splits data into rolling windows and computes health scores per window.
    """
    from scripts.analytics.predictive import compute_health_scores

    df = df.copy()
    if term:
        df = df[df["taller_planta_grouped"] == term]

    df["fecha_evento"] = pd.to_datetime(df["fecha_evento"])
    min_d = df["fecha_evento"].min()

    dates = pd.date_range(min_d, df["fecha_evento"].max(), freq=freq)
    rows = []

    for d in dates:
        window = df[df["fecha_evento"] <= d]
        hs = compute_health_scores(window)
        if hs.empty:
            continue
        rows.append({
            "fecha": d,
            "avg_score": hs["health_score"].mean(),
            "p90_score": hs["health_score"].quantile(0.9),
            "criticos": (hs["health_score"] >= 70).sum(),
            "n_buses": len(hs),
        })

    return pd.DataFrame(rows)


# ═══════════════════════════════════════════════════════════════════════════
# CROSS-TERMINAL CORRELATION / PROPAGATION
# ═══════════════════════════════════════════════════════════════════════════

def terminal_correlation_matrix(df: pd.DataFrame, lags: list[int] | None = None) -> dict:
    """Compute cross-correlation between terminal daily event series.
    Returns correlation matrix and top lead-lag pairs.
    """
    lags = lags or [0, 1, 2, 3, 5, 7]
    daily = daily_series(df, group_col="taller_planta_grouped")
    pivot = daily.pivot(index="ds", columns="group", values="y").fillna(0)

    # Same-day correlations
    corr_matrix = pivot.corr()

    # Lead-lag: find max correlation with terminal A lagging terminal B
    pairs = []
    terminals = list(corr_matrix.columns)
    for lag in lags:
        if lag == 0:
            continue
        for a in terminals:
            for b in terminals:
                if a == b:
                    continue
                shifted = pivot[a].shift(lag)
                c = pivot[b].corr(shifted)
                pairs.append({"term_a": a, "term_b": b, "lag": lag, "corr": c})

    pairs_df = pd.DataFrame(pairs).sort_values("corr", ascending=False)
    top_pairs = pairs_df.head(20)

    return {"corr_matrix": corr_matrix, "lead_lag": top_pairs}


# ═══════════════════════════════════════════════════════════════════════════
# SYSTEM-LEVEL WEEKLY PATTERNS
# ═══════════════════════════════════════════════════════════════════════════

def system_weekly_profile(df: pd.DataFrame, n_systems: int = 6) -> pd.DataFrame:
    """Average daily events per system per weekday."""
    systems = df["causa_sistema_reconstruida"].value_counts().head(n_systems).index.tolist()
    df = df.copy()
    df["ds"] = pd.to_datetime(df["fecha_evento"].dt.date)
    df["dow"] = df["fecha_evento"].dt.dayofweek

    rows = []
    for sys_name in systems:
        sub = df[df["causa_sistema_reconstruida"] == sys_name]
        by_dow = sub.groupby("dow").size() / sub["ds"].nunique()
        for dow, val in by_dow.items():
            rows.append({"sistema": sys_name, "dia": dow, "promedio": val})

    return pd.DataFrame(rows)
