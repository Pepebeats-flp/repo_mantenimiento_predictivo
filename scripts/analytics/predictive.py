import gc

import numpy as np
import pandas as pd
from xgboost import XGBRegressor


# ═══════════════════════════════════════════════════════════════════════════
# TERMINAL LOAD FORECASTING  (time series regression)
# ═══════════════════════════════════════════════════════════════════════════

def _build_terminal_features(daily):
    """Build time-series features for terminal daily load prediction."""
    features = daily.copy()
    features["fecha"] = pd.to_datetime(features["fecha"])
    features["dow"] = features["fecha"].dt.dayofweek
    features["mes"] = features["fecha"].dt.month
    features["dom"] = features["fecha"].dt.day
    features["semana"] = features["fecha"].dt.isocalendar().week.astype(int)
    features["finde"] = (features["dow"] >= 5).astype(int)
    features["dia_idx"] = (features["fecha"] - features["fecha"].min()).dt.days

    # Rolling averages per terminal
    terms = features["taller_planta_grouped"].unique()
    for w in [7, 14, 28]:
        col = f"rolling_{w}d"
        vals = []
        for t in terms:
            mask = features["taller_planta_grouped"] == t
            vals.extend(features.loc[mask, "eventos"].rolling(w, min_periods=1).mean().values)
        features[col] = vals

    # Terminal one-hot (keep original column)
    features["_term"] = features["taller_planta_grouped"]
    features = pd.get_dummies(features, columns=["taller_planta_grouped"], prefix="term")
    features["taller_planta_grouped"] = features["_term"]
    features = features.drop(columns=["_term"])

    feats = features.dropna(subset=[c for c in features.columns if c not in ("eventos", "taller_planta_grouped")])
    return feats


def train_terminal_forecast(df, test_days=90):
    df = df.copy()
    df["fecha"] = df["fecha_evento"].dt.date
    daily = (
        df.groupby(["fecha", "taller_planta_grouped"])
        .size()
        .reset_index(name="eventos")
    )

    feats = _build_terminal_features(daily)
    max_date = feats["fecha"].max()
    cutoff = max_date - pd.Timedelta(days=test_days)

    feature_cols = [c for c in feats.columns if c not in ("fecha", "eventos", "taller_planta_grouped")]

    train = feats[feats["fecha"] <= cutoff]
    test = feats[feats["fecha"] > cutoff]

    X_train = train[feature_cols].values
    y_train = train["eventos"].values
    X_test = test[feature_cols].values
    y_test = test["eventos"].values

    model = XGBRegressor(
        n_estimators=100,
        max_depth=6,
        learning_rate=0.1,
        subsample=0.8,
        colsample_bytree=0.8,
        random_state=42,
        n_jobs=1,
    )
    model.fit(X_train, y_train, eval_set=[(X_test, y_test)], verbose=False)

    train_score = model.score(X_train, y_train)
    test_score = model.score(X_test, y_test)

    metadata = {
        "feature_cols": feature_cols,
        "train_r2": round(train_score, 3),
        "test_r2": round(test_score, 3),
        "train_size": len(train),
        "test_size": len(test),
        "terminals": [c.replace("term_", "") for c in feature_cols if c.startswith("term_")],
        "last_date": max_date,
    }
    return model, metadata


def predict_terminal_forecast(model, metadata, df, days_ahead=14):
    last_date = metadata["last_date"]
    terminals = metadata["terminals"]
    feature_cols = metadata["feature_cols"]

    future_dates = pd.date_range(last_date + pd.Timedelta(days=1), periods=days_ahead)
    rows = []
    for date in future_dates:
        for term in terminals:
            rows.append({
                "fecha": date.date(),
                "taller_planta_grouped": term,
                "eventos": np.nan,
            })
    future_df = pd.DataFrame(rows)

    df = df.copy()
    df["fecha"] = df["fecha_evento"].dt.date
    daily = (
        df.groupby(["fecha", "taller_planta_grouped"])
        .size()
        .reset_index(name="eventos")
    )
    # Append future rows to get rolling features
    combined = pd.concat([daily, future_df], ignore_index=True)
    feats = _build_terminal_features(combined)
    future_feats = feats[feats["eventos"].isna()].copy()

    if future_feats.empty:
        return pd.DataFrame()

    X_future = future_feats[feature_cols].values
    future_feats["pronostico"] = np.maximum(0, model.predict(X_future).round(0).astype(int))

    result = future_feats[["fecha", "taller_planta_grouped", "pronostico"]].copy()
    result["fecha"] = result["fecha"].astype(str)
    return result


# ═══════════════════════════════════════════════════════════════════════════
# FLEET HEALTH SCORE  (composite 0-100, higher = worse condition)
# ═══════════════════════════════════════════════════════════════════════════

def compute_health_scores(df):
    """Compute composite health score for each bus (0-100)."""
    last_date = df["fecha_evento"].max()
    cutoff_30d = last_date - pd.Timedelta(days=30)

    # Recent events per bus
    recent = df[df["fecha_evento"] >= cutoff_30d].copy()
    freq = recent.groupby("placa_patente").agg(
        eventos_30d=("tipo_servicio", "count"),
        correctivos_30d=("tipo_servicio", lambda x: (x == "CORRECTIVO").sum()),
        sistemas=("sistema_componente_grouped", "nunique"),
        ultimo_evento=("fecha_evento", "max"),
        taller_principal=("taller_planta_grouped", lambda x: x.mode().iloc[0] if len(x.mode()) > 0 else None),
        duracion_p90=("duracion_ot_horas", lambda x: x.clip(0, 168).quantile(0.9)),
    ).reset_index()

    # ―――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――
    # 1. FREQUENCY SCORE (0-30)
    # ―――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――
    max_events = freq["eventos_30d"].quantile(0.98)
    freq["freq_score"] = (freq["eventos_30d"] / max_events * 30).clip(0, 30)

    # ―――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――
    # 2. TREND SCORE (0-20) — sudden increase in last 7 days
    # ―――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――
    cutoff_7d = last_date - pd.Timedelta(days=7)
    cutoff_14d = last_date - pd.Timedelta(days=14)
    this_week = df[(df["fecha_evento"] >= cutoff_7d)].groupby("placa_patente").size()
    prev_week = df[(df["fecha_evento"] >= cutoff_14d) & (df["fecha_evento"] < cutoff_7d)].groupby("placa_patente").size()
    this_week = this_week.reindex(freq["placa_patente"], fill_value=0)
    prev_week = prev_week.reindex(freq["placa_patente"], fill_value=0)

    trend_ratio = np.where(
        prev_week > 0, (this_week / prev_week).fillna(0), np.where(this_week > 2, 3, 0)
    )
    freq["trend_score"] = np.clip((trend_ratio - 1) * 10, 0, 20)

    # ―――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――
    # 3. RECURRENCE SCORE (0-20) — same system within 7d
    # ―――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――
    corr = df[df["tipo_servicio"] == "CORRECTIVO"].sort_values(["placa_patente", "fecha_evento"])
    corr["prev_sistema"] = corr.groupby("placa_patente")["sistema_componente_grouped"].shift(1)
    corr["prev_fecha"] = corr.groupby("placa_patente")["fecha_evento"].shift(1)
    corr["dias_diff"] = (
        (corr["fecha_evento"] - corr["prev_fecha"]).dt.total_seconds().div(86400)
    )
    corr["same_system"] = (corr["sistema_componente_grouped"] == corr["prev_sistema"])
    corr["rec_rapida"] = (
        corr["dias_diff"].fillna(999) <= 7
        & corr["same_system"]
    ).astype(int)

    bus_rec = corr.groupby("placa_patente").agg(
        total_corr=("rec_rapida", "count"),
        rec_rapidas=("rec_rapida", "sum"),
    ).reset_index()
    bus_rec["rec_rate"] = (bus_rec["rec_rapidas"] / bus_rec["total_corr"].clip(1)).fillna(0)

    freq = freq.merge(bus_rec[["placa_patente", "rec_rate"]], on="placa_patente", how="left")
    freq["rec_rate"] = freq["rec_rate"].fillna(0)
    freq["rec_score"] = (freq["rec_rate"] * 20).clip(0, 20)

    # ―――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――
    # 4. RECENCY SCORE (0-15) — more recent = worse
    # ―――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――
    days_since = (last_date - freq["ultimo_evento"]).dt.total_seconds().div(86400).fillna(999)
    # 0d → 15, 1d → 12, 3d → 8, 7d → 4, 14d → 0
    freq["recency_score"] = np.clip(15 - days_since.clip(0, 15), 0, 15)

    # ―――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――
    # 5. SEVERITY SCORE (0-15) — repair duration + system diversity
    # ―――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――
    max_dur = 72  # 72 hours is very severe
    freq["dur_score"] = (freq["duracion_p90"].clip(0, max_dur) / max_dur * 8).clip(0, 8)
    max_sys = freq["sistemas"].quantile(0.95)
    freq["sys_score"] = (freq["sistemas"] / max_sys * 7).clip(0, 7)
    freq["severity_score"] = freq["dur_score"] + freq["sys_score"]

    # ―――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――
    # 6. OBSERVATION SEVERITY BOOST (0-10) — severe keywords in text
    # ―――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――
    recent_obs = recent[recent["observacion_clean"].notna()].copy()
    severe_text = recent_obs["observacion_clean"].str.upper()
    severe_keywords = ["CHOQUE", "SINIESTRO", "VANDALISMO", "INCENDIO", "COLISION"]
    severe_mask = np.zeros(len(recent_obs), dtype=bool)
    for kw in severe_keywords:
        severe_mask |= severe_text.str.contains(kw, na=False)
    bus_severe = recent_obs[severe_mask].groupby("placa_patente").size().reset_index(name="obs_severas")
    freq = freq.merge(bus_severe, on="placa_patente", how="left")
    freq["obs_severas"] = freq["obs_severas"].fillna(0)
    freq["obs_boost"] = (freq["obs_severas"] * 3).clip(0, 10)

    # ―――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――
    # COMPOSITE (0-100)
    # ―――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――――
    freq["health_score"] = (
        freq["freq_score"]
        + freq["trend_score"]
        + freq["rec_score"]
        + freq["recency_score"]
        + freq["severity_score"]
        + freq["obs_boost"]
    ).round(1)

    freq["health_score"] = freq["health_score"].clip(0, 100)

    result = freq.sort_values("health_score", ascending=False)[
        ["placa_patente", "health_score", "eventos_30d", "correctivos_30d",
         "freq_score", "trend_score", "rec_score", "recency_score", "severity_score", "obs_boost",
         "ultimo_evento", "taller_principal"]
    ].reset_index(drop=True)
    result["dias_desde"] = ((last_date - result["ultimo_evento"]).dt.total_seconds().div(86400)).round(1)

    gc.collect()
    return result


def score_category(score):
    if score >= 70:
        return "🔴 Crítico"
    if score >= 40:
        return "🟠 Atención"
    if score >= 20:
        return "🟡 Moderado"
    return "🟢 Normal"


# ═══════════════════════════════════════════════════════════════════════════
# OBSERVATION-BASED ALERTS
# ═══════════════════════════════════════════════════════════════════════════

SEVERE_PATTERNS = {
    "Choque/Siniestro": ["CHOQUE", "SINIESTRO", "COLISION", "ACCIDENTE"],
    "Vandalismo": ["VANDALISMO", "ROTURA"],
    "Incendio": ["INCENDIO", "FUEGO", "QUEMAD"],
    "Falla Motor": ["FALLA MOTOR", "MOTOR", "EMBRAGUE"],
    "Falla Frenos": ["FALLA FRENO", "FRENO"],
    "Falla Eléctrica": ["FALLA ELECTRIC", "ELECTRICO", "BATERIA"],
    "No Presentado": ["NO PRESENTADO", "NO LLEGA"],
    "Puertas": ["PUERTA", "CIERRE"],
}


def compute_observation_alerts(df, window_days=30):
    """Get recent severe observation alerts per bus."""
    cutoff = df["fecha_evento"].max() - pd.Timedelta(days=window_days)
    recent = df[(df["fecha_evento"] >= cutoff) & df["observacion_clean"].notna()].copy()
    recent["obs_upper"] = recent["observacion_clean"].str.upper()

    rows = []
    for alert_type, keywords in SEVERE_PATTERNS.items():
        mask = np.zeros(len(recent), dtype=bool)
        for kw in keywords:
            mask |= recent["obs_upper"].str.contains(kw, na=False)
        matches = recent[mask]
        for bus, grp in matches.groupby("placa_patente"):
            rows.append({
                "placa_patente": bus,
                "alerta": alert_type,
                "eventos": len(grp),
                "ultimo": grp["fecha_evento"].max(),
                "taller": grp["taller_planta_grouped"].mode().iloc[0] if len(grp) else None,
            })

    if not rows:
        return pd.DataFrame()

    result = pd.DataFrame(rows)
    result = result.sort_values(["eventos", "ultimo"], ascending=[False, False])
    gc.collect()
    return result
