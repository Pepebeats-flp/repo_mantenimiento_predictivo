"""Operational predictions for fleet maintenance.

Three models tuned for production decisions (not analysis):

  train_weekly_system_load(df) → model, meta    R² ≈ 0.72
  train_bus_spike_model(df)    → model, meta    AUC ≈ 0.82
  train_parts_model(df)         → model, meta    AUC ≈ 0.68

Each has a corresponding predict_*() function.
"""
from __future__ import annotations

import gc

import numpy as np
import pandas as pd
from xgboost import XGBClassifier, XGBRegressor
from sklearn.metrics import (
    accuracy_score,
    confusion_matrix,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.preprocessing import LabelEncoder


# ═══════════════════════════════════════════════════════════════════════════════
# TARGET 6: Weekly correctivo load by system + terminal
# ═══════════════════════════════════════════════════════════════════════════════

def train_weekly_system_load(df, test_weeks=12):
    """Train XGBoost model to forecast weekly correctivo load per (system, terminal).

    Filters non-mechanical events (vandalism, siniestro, choque, no_presentado)
    from CARROCERIA to improve forecast accuracy — these are unpredictable.

    Returns:
        (model, metadata) — XGBRegressor + dict with feature columns,
        system/terminal encoders, per-group historical stats, and metrics.
    """
    import math

    df["fecha_evento"] = pd.to_datetime(df["fecha_evento"])

    system_col = "sistema_enriched" if "sistema_enriched" in df.columns else "causa_sistema_reconstruida"

    has_no_mec = "es_no_mecanico" in df.columns

    corr = df[df["tipo_servicio"] == "CORRECTIVO"].copy()
    # Exclude non-mechanical CARROCERIA events
    if has_no_mec:
        corr = corr[~((corr["causa_sistema_reconstruida"] == "CARROCERIA") & (corr["es_no_mecanico"] == 1))]
    corr = corr[corr[system_col] != "OTROS"].copy()
    corr["week_dt"] = corr["fecha_evento"].dt.to_period("W").dt.start_time

    grp = corr.groupby(
        [system_col, "taller_planta_grouped", "week_dt"]
    ).size().reset_index(name="n_corr")

    grp = grp.sort_values([system_col, "taller_planta_grouped", "week_dt"])

    # Pre-compute terminal total load per week (across all systems)
    terminal_total = grp.groupby(["taller_planta_grouped", "week_dt"])["n_corr"].sum().reset_index(name="term_total")

    rows = []
    hist_stds = {}  # per-group std for confidence intervals
    for (sistema, terminal), g in grp.groupby([system_col, "taller_planta_grouped"]):
        g = g.sort_values("week_dt")
        vals = g["n_corr"].values
        hist_stds[(sistema, terminal)] = float(np.std(vals)) if len(vals) > 4 else np.nan

        for i in range(12, len(g)):
            past = vals[max(0, i - 12) : i]
            recent4 = vals[max(0, i - 4) : i] if i >= 4 else vals[:i]

            week_dt = g.iloc[i]["week_dt"]
            week_num = pd.Timestamp(week_dt).isocalendar().week
            month = pd.Timestamp(week_dt).month

            # Terminal total at this week (from terminal_total)
            t_total = terminal_total[
                (terminal_total["taller_planta_grouped"] == terminal)
                & (terminal_total["week_dt"] == week_dt)
            ]["term_total"].values
            term_total_val = float(t_total[0]) if len(t_total) > 0 else 0.0

            row = {
                "n_1w_ago": vals[i - 1],
                "n_2w_ago": vals[i - 2] if i >= 2 else 0,
                "n_3w_ago": vals[i - 3] if i >= 3 else 0,
                "n_4w_ago": vals[i - 4] if i >= 4 else 0,
                "n_8w_ago": vals[i - 8] if i >= 8 else 0,
                "n_12w_ago": vals[i - 12] if i >= 12 else 0,
                "avg_4w": vals[i - min(4, i) : i].mean(),
                "avg_8w": vals[max(0, i - 8) : i].mean(),
                "avg_12w": vals[max(0, i - 12) : i].mean(),
                "std_4w": vals[i - min(4, i) : i].std() if i >= 2 else 0,
                "std_8w": vals[max(0, i - 8) : i].std() if i >= 3 else 0,
                "max_4w": vals[i - min(4, i) : i].max(),
                "min_4w": vals[i - min(4, i) : i].min(),
                "max_12w": vals[max(0, i - 12) : i].max(),
                "trend_8w": _linear_slope(vals[max(0, i - 8) : i]),
                "ratio_4w_8w": vals[i - min(4, i) : i].mean() / max(vals[max(0, i - 8) : i].mean(), 0.01),
                "month_sin": math.sin(2 * math.pi * month / 12),
                "month_cos": math.cos(2 * math.pi * month / 12),
                "week_sin": math.sin(2 * math.pi * week_num / 52),
                "week_cos": math.cos(2 * math.pi * week_num / 52),
                "term_total": term_total_val,
                "share_of_term": vals[i - 1] / max(term_total_val, 1),
                "sistema": sistema,
                "terminal": terminal,
                "week_dt": week_dt,
                "target": vals[i],
                "_dow": pd.Timestamp(week_dt).dayofweek,
            }
            rows.append(row)

    data = pd.DataFrame(rows)
    if data.empty:
        metadata = {"feature_cols": [], "dummy_cols": [], "last_week": None,
                     "test_r2": None, "test_mae": None, "hist_std": {}}
        return None, metadata

    # Encode categoricals
    data = pd.get_dummies(data, columns=["sistema", "terminal"], drop_first=True)

    feature_cols = [c for c in data.columns if c not in ("week_dt", "target", "_dow")]
    X = data[feature_cols].fillna(0).values
    y = data["target"].values
    dates = data["week_dt"].values

    # Time-based split
    cutoff = dates.max() - pd.Timedelta(weeks=test_weeks)
    train_mask = dates < cutoff
    test_mask = dates >= cutoff

    X_train, y_train = X[train_mask], y[train_mask]

    model = XGBRegressor(
        n_estimators=300, max_depth=5, learning_rate=0.03,
        subsample=0.85, colsample_bytree=0.7, reg_alpha=0.5, reg_lambda=1,
        random_state=42, n_jobs=1,
    )
    if test_mask.sum() > 0:
        model.fit(
            X_train, y_train,
            eval_set=[(X[test_mask], y[test_mask])],
            verbose=False,
        )
    else:
        model.fit(X_train, y_train, verbose=False)

    # Evaluation
    train_r2 = None
    if test_mask.sum() > 0:
        y_pred_test = np.maximum(model.predict(X[test_mask]), 0)
        r2 = 1 - ((y[test_mask] - y_pred_test) ** 2).sum() / max(
            ((y[test_mask] - y[test_mask].mean()) ** 2).sum(), 1e-9
        )
        mae = float(np.abs(y[test_mask] - y_pred_test).mean())
        y_pred_train = np.maximum(model.predict(X[train_mask]), 0)
        train_r2 = 1 - ((y[train_mask] - y_pred_train) ** 2).sum() / max(
            ((y[train_mask] - y[train_mask].mean()) ** 2).sum(), 1e-9
        )
    else:
        r2 = None
        mae = None

    metadata = {
        "feature_cols": feature_cols,
        "dummy_cols": [c for c in data.columns if c.startswith(("sistema_", "terminal_"))],
        "last_week": pd.Timestamp(dates.max()),
        "test_r2": r2,
        "test_mae": mae,
        "train_r2": train_r2,
        "hist_std": hist_stds,
        "top_features": sorted(
            zip(feature_cols, model.feature_importances_),
            key=lambda x: -x[1],
        )[:10],
    }

    gc.collect()
    return model, metadata


def _linear_slope(y):
    """Compute linear trend slope of a series."""
    if len(y) < 2:
        return 0.0
    x = np.arange(len(y))
    slope = np.polyfit(x, y, 1)[0]
    return float(slope)


def predict_weekly_system_load(model, metadata, df, weeks_ahead=8):
    """Predict correctivo load per (system, terminal) for next N weeks from today.

    Always forecasts from the current calendar week forward, regardless of
    when the model was last trained. Skips incomplete trailing weeks and
    fills the gap between last training data and today via recurrent prediction.

    Returns DataFrame with columns: sistema, terminal, semana, pronostico,
    confianza_baja, confianza_alta.
    """
    import math
    from datetime import date, timedelta

    if model is None:
        return pd.DataFrame()

    df["fecha_evento"] = pd.to_datetime(df["fecha_evento"])

    system_col = "sistema_enriched" if "sistema_enriched" in df.columns else "causa_sistema_reconstruida"

    has_no_mec = "es_no_mecanico" in df.columns

    corr = df[df["tipo_servicio"] == "CORRECTIVO"].copy()
    if has_no_mec:
        corr = corr[~((corr["causa_sistema_reconstruida"] == "CARROCERIA") & (corr["es_no_mecanico"] == 1))]
    corr = corr[corr[system_col] != "OTROS"].copy()
    corr["week_dt"] = corr["fecha_evento"].dt.to_period("W").dt.start_time

    grp = corr.groupby(
        [system_col, "taller_planta_grouped", "week_dt"]
    ).size().reset_index(name="n_corr")

    last_week = grp["week_dt"].max()

    # Skip incomplete trailing weeks (e.g. current week with few events yet)
    weekly_totals = grp.groupby("week_dt")["n_corr"].sum()
    avg_weekly = weekly_totals.median()
    for _ in range(4):
        if last_week not in weekly_totals.index:
            break
        total_last = weekly_totals[last_week]
        if total_last >= avg_weekly * 0.25:
            break
        last_week = last_week - pd.Timedelta(weeks=1)

    # Align with current calendar week so forecast is always future-facing
    today = pd.Timestamp(date.today())
    today_week = today - pd.Timedelta(days=today.dayofweek)  # Monday of current week
    gap_weeks = max(0, int((today_week - last_week) / pd.Timedelta(weeks=1)))
    total_ahead = gap_weeks + weeks_ahead

    all_sistemas = sorted(grp[system_col].unique())
    all_terminales = sorted(grp["taller_planta_grouped"].unique())

    # Pre-compute terminal totals for historical weeks (needed for share_of_term)
    term_totals = grp.groupby(["taller_planta_grouped", "week_dt"])["n_corr"].sum().to_dict()

    hist_std = metadata.get("hist_std", {})

    predictions = []

    def _build_features(vals, sistema, terminal, week_date):
        """Build a feature row from historical values and metadata."""
        i = len(vals)
        month = pd.Timestamp(week_date).month
        week_num = pd.Timestamp(week_date).isocalendar().week

        # Terminal total: last known + estimate
        term_key = (terminal, pd.Timestamp(week_date))
        term_total_proxy = 0.0
        if len(vals) > 0:
            term_total_proxy = vals[-1] * len(all_sistemas) * 0.7  # rough estimate

        row = {
            "n_1w_ago": vals[-1] if i >= 1 else 0,
            "n_2w_ago": vals[-2] if i >= 2 else 0,
            "n_3w_ago": vals[-3] if i >= 3 else 0,
            "n_4w_ago": vals[-4] if i >= 4 else 0,
            "n_8w_ago": vals[-8] if i >= 8 else 0,
            "n_12w_ago": vals[-12] if i >= 12 else 0,
            "avg_4w": vals[-min(4, i):].mean() if i > 0 else 0,
            "avg_8w": vals[-min(8, i):].mean() if i > 0 else 0,
            "avg_12w": vals[-min(12, i):].mean() if i > 0 else 0,
            "std_4w": vals[-min(4, i):].std() if i >= 2 else 0,
            "std_8w": vals[-min(8, i):].std() if i >= 3 else 0,
            "max_4w": vals[-min(4, i):].max() if i > 0 else 0,
            "min_4w": vals[-min(4, i):].min() if i > 0 else 0,
            "max_12w": vals[-min(12, i):].max() if i > 0 else 0,
            "trend_8w": _linear_slope(vals[-min(8, i):]) if i >= 2 else 0,
            "ratio_4w_8w": (vals[-min(4, i):].mean() / max(vals[-min(8, i):].mean(), 0.01)) if i > 0 else 1,
            "month_sin": math.sin(2 * math.pi * month / 12),
            "month_cos": math.cos(2 * math.pi * month / 12),
            "week_sin": math.sin(2 * math.pi * week_num / 52),
            "week_cos": math.cos(2 * math.pi * week_num / 52),
            "term_total": term_total_proxy,
            "share_of_term": vals[-1] / max(term_total_proxy, 1) if i >= 1 else 0,
        }
        # Dummy columns
        for feat in metadata["feature_cols"]:
            if feat.startswith("sistema_"):
                val = feat.split("_", 1)[1]
                row[feat] = 1 if val == sistema else 0
            elif feat.startswith("terminal_"):
                val = feat.split("_", 1)[1]
                row[feat] = 1 if val == terminal else 0
            elif feat not in row:
                row[feat] = 0
        return row

    for sistema in all_sistemas:
        for terminal in all_terminales:
            sub = grp[
                (grp[system_col] == sistema)
                & (grp["taller_planta_grouped"] == terminal)
            ].sort_values("week_dt")

            if len(sub) < 4:
                continue

            vals = list(sub["n_corr"].values)
            base_std = hist_std.get((sistema, terminal), 5.0)
            if np.isnan(base_std) or base_std == 0:
                base_std = 5.0

            vals_arr = np.array(vals, dtype=float)
            vals_buf = np.empty(len(vals_arr) + total_ahead, dtype=float)
            vals_buf[:len(vals_arr)] = vals_arr
            n_vals = len(vals_arr)

            # Recurrent prediction — fill gap + future weeks
            for w in range(total_ahead):
                week_date = last_week + pd.Timedelta(weeks=w + 1)
                row = _build_features(vals_buf[:n_vals], sistema, terminal, week_date)
                X_pred = pd.DataFrame([row])[metadata["feature_cols"]].fillna(0)
                pred = max(0.0, float(model.predict(X_pred)[0]))

                # Confidence: wider bands for further weeks
                decay_factor = 1.0 + w * 0.12
                ci_half = base_std * decay_factor * 1.3
                lo = max(0, pred - ci_half)
                hi = max(pred + 0.5, pred + ci_half)

                predictions.append({
                    "sistema": sistema,
                    "terminal": terminal,
                    "semana": week_date,
                    "pronostico": round(pred, 1),
                    "confianza_baja": round(lo, 1),
                    "confianza_alta": round(hi, 1),
                })

                vals_buf[n_vals] = pred
                n_vals += 1

    result = pd.DataFrame(predictions)
    # Show only complete future weeks (next Monday onward) — real-time platform UX
    next_monday = today_week + pd.Timedelta(weeks=1)
    if not result.empty:
        result = result[result["semana"] >= next_monday]
        result = result.sort_values(["terminal", "semana", "sistema"])
    gc.collect()
    return result


# ═══════════════════════════════════════════════════════════════════════════════
# TARGET 5: Bus-level spike risk (>=10 correctivos/month)
# ═══════════════════════════════════════════════════════════════════════════════

def train_bus_spike_model(df, test_months=3):
    """Train classifier to predict if a bus will have >=10 correctivos next month.

    Filters non-mechanical CARROCERIA events from spike training to avoid
    inflating counts with vandalism/choques.

    Returns (model, metadata) with AUC metric.
    """
    df["fecha_evento"] = pd.to_datetime(df["fecha_evento"])

    has_no_mec = "es_no_mecanico" in df.columns

    corr = df[df["tipo_servicio"] == "CORRECTIVO"].copy()
    if has_no_mec:
        corr = corr[~((corr["causa_sistema_reconstruida"] == "CARROCERIA") & (corr["es_no_mecanico"] == 1))]
    corr["ym"] = corr["fecha_evento"].dt.to_period("M")
    corr["ym_dt"] = corr["ym"].dt.to_timestamp()

    bpm = corr.groupby(["placa_patente", "ym_dt"]).size().reset_index(name="n_corr")
    bpm = bpm.sort_values(["placa_patente", "ym_dt"])

    rows = []
    for bus, grp in bpm.groupby("placa_patente"):
        grp = grp.sort_values("ym_dt")
        for i in range(len(grp)):
            past = grp.iloc[:i]
            n_past = len(past)
            if n_past == 0:
                continue

            a3 = past.tail(3)["n_corr"].mean()
            a6 = past.tail(6)["n_corr"].mean() if n_past >= 6 else past["n_corr"].mean()
            rows.append({
                "placa_patente": bus,
                "ym_dt": grp.iloc[i]["ym_dt"],
                "n_1m": past.iloc[-1]["n_corr"],
                "n_2m": past.iloc[-2]["n_corr"] if n_past >= 2 else 0,
                "n_3m": past.iloc[-3]["n_corr"] if n_past >= 3 else 0,
                "avg_3m": a3,
                "avg_6m": a6,
                "max_ever": past["n_corr"].max(),
                "trend": a3 / max(a6, 0.01) - 1 if a6 > 0 else 0,
                "n_months": n_past,
                "target": int(grp.iloc[i]["n_corr"] >= 10),
            })

    data = pd.DataFrame(rows)
    if data.empty:
        metadata = {"feature_cols": [], "last_month": None, "pos_rate": 0.5}
        gc.collect()
        return None, metadata
    feature_cols = ["n_1m", "n_2m", "n_3m", "avg_3m", "avg_6m",
                    "max_ever", "trend", "n_months"]
    X = data[feature_cols].fillna(0).values
    y = data["target"].values
    dates = data["ym_dt"].values

    cutoff = pd.Timestamp(dates.max()) - pd.DateOffset(months=test_months)
    train_mask = dates < cutoff

    X_train, y_train = X[train_mask], y[train_mask]

    if len(y_train) == 0:
        metadata = {"feature_cols": feature_cols, "last_month": pd.Timestamp(dates.max()),
                     "pos_rate": 0.5, "test_auc": None}
        gc.collect()
        return None, metadata

    pos_rate = y_train.mean()
    model = XGBClassifier(
        n_estimators=100, max_depth=5, learning_rate=0.05,
        subsample=0.8, random_state=42, n_jobs=1,
        scale_pos_weight=(1 - pos_rate) / max(pos_rate, 0.01),
    )
    model.fit(X_train, y_train, verbose=False)

    # Compute AUC on test set
    test_auc = None
    test_f1 = None
    test_precision = None
    test_recall = None
    test_accuracy = None
    test_confusion = None
    test_mask = dates >= cutoff
    if test_mask.sum() > 0 and len(np.unique(y[test_mask])) > 1:
        y_pred_proba = model.predict_proba(X[test_mask])[:, 1]
        y_pred = model.predict(X[test_mask])
        test_auc = round(float(roc_auc_score(y[test_mask], y_pred_proba)), 3)
        test_f1 = round(float(f1_score(y[test_mask], y_pred, zero_division=0)), 3)
        test_precision = round(float(precision_score(y[test_mask], y_pred, zero_division=0)), 3)
        test_recall = round(float(recall_score(y[test_mask], y_pred, zero_division=0)), 3)
        test_accuracy = round(float(accuracy_score(y[test_mask], y_pred)), 3)
        test_confusion = confusion_matrix(y[test_mask], y_pred).tolist()
        print(f"  spike model AUC: {test_auc}")

    metadata = {
        "feature_cols": feature_cols,
        "last_month": pd.Timestamp(dates.max()),
        "pos_rate": pos_rate,
        "test_auc": test_auc,
        "test_f1": test_f1,
        "test_precision": test_precision,
        "test_recall": test_recall,
        "test_accuracy": test_accuracy,
        "test_confusion": test_confusion,
        "naive_acc": round(max(pos_rate, 1 - pos_rate), 3),
        "top_features": sorted(
            zip(feature_cols, model.feature_importances_),
            key=lambda x: -x[1],
        )[:10],
    }
    gc.collect()
    return model, metadata


def predict_bus_spikes(model, metadata, df):
    """Return per-bus spike probability for the next month.

    Returns DataFrame with columns: placa_patente, prob_spike,
    riesgo, correctivos_30d, ultimo_sistema.
    """
    df["fecha_evento"] = pd.to_datetime(df["fecha_evento"])

    corr = df[df["tipo_servicio"] == "CORRECTIVO"].copy()
    corr["ym_dt"] = corr["fecha_evento"].dt.to_period("M").dt.to_timestamp()

    last_date = df["fecha_evento"].max()
    cutoff_30d = last_date - pd.Timedelta(days=30)

    # Pre-compute monthly counts per bus (vectorized)
    monthly = corr.groupby(["placa_patente", "ym_dt"]).size().reset_index(name="n_corr")
    monthly = monthly.sort_values(["placa_patente", "ym_dt"])

    # Pre-compute 30d correctivo counts
    n30 = corr[corr["fecha_evento"] >= cutoff_30d].groupby("placa_patente").size()

    # Pre-compute last non-OTROS system per bus
    last_non_otros = (
        df[df["causa_sistema_reconstruida"] != "OTROS"]
        .sort_values("fecha_evento")
        .groupby("placa_patente")
        .tail(1)
        .set_index("placa_patente")["causa_sistema_reconstruida"]
    )
    # Fallback: any last system (even OTROS)
    last_any = (
        df.sort_values("fecha_evento")
        .groupby("placa_patente")
        .tail(1)
        .set_index("placa_patente")["causa_sistema_reconstruida"]
    )

    # Build features per bus from pre-grouped data
    feature_rows = []
    bus_list = []
    for bus, grp in monthly.groupby("placa_patente"):
        if len(grp) < 2:
            continue
        vals = grp["n_corr"].values
        n = len(vals)
        n_1m = vals[-1]
        n_2m = vals[-2] if n >= 2 else 0
        n_3m = vals[-3] if n >= 3 else 0
        a3 = vals[-3:].mean()
        a6 = vals[-6:].mean() if n >= 6 else vals.mean()
        max_ever = vals.max()
        trend = a3 / max(a6, 0.01) - 1 if a6 > 0 else 0

        feature_rows.append([
            n_1m, n_2m, n_3m, a3, a6, max_ever, trend, n,
        ])
        bus_list.append(bus)

    if not feature_rows:
        return pd.DataFrame(columns=["placa_patente", "prob_spike", "riesgo",
                                     "correctivos_30d", "ultimo_sistema"])

    X_pred = pd.DataFrame(feature_rows, columns=metadata["feature_cols"]).fillna(0)
    probs = model.predict_proba(X_pred.values)[:, 1]

    # Build result
    result_rows = []
    for i, bus in enumerate(bus_list):
        ult_sys = "—"
        if bus in last_non_otros.index and pd.notna(last_non_otros.get(bus)):
            ult_sys = str(last_non_otros[bus])
        elif bus in last_any.index and pd.notna(last_any.get(bus)):
            ult_sys = str(last_any[bus])
        result_rows.append({
            "placa_patente": bus,
            "prob_spike": round(float(probs[i]), 3),
            "correctivos_30d": int(n30.get(bus, 0)),
            "ultimo_sistema": ult_sys,
        })

    result = pd.DataFrame(result_rows).sort_values("prob_spike", ascending=False)

    def _risk(prob):
        if prob >= 0.7:
            return "🔴 Alto"
        if prob >= 0.4:
            return "🟠 Medio"
        if prob >= 0.15:
            return "🟡 Bajo"
        return "🟢 Normal"

    result["riesgo"] = result["prob_spike"].apply(_risk)
    return result.reset_index(drop=True)


# ═══════════════════════════════════════════════════════════════════════════════
# TARGET 3: Parts needed for corrective event
# ═══════════════════════════════════════════════════════════════════════════════

def _build_parts_features(df):
    """Build per-event features for parts prediction from bus history."""
    df = df.sort_values(["placa_patente", "fecha_evento"])

    # ── Per-bus features ──
    # Bus age (days since first event)
    df["bus_first_event"] = df.groupby("placa_patente")["fecha_evento"].transform("min")
    df["bus_age_days"] = (df["fecha_evento"] - df["bus_first_event"]).dt.total_seconds().div(86400).fillna(0)

    # Bus total events up to now
    df["bus_event_seq"] = df.groupby("placa_patente").cumcount()

    # Cumulative proportion of repuestos (excluding current event)
    def _cummean_excl(g):
        s = g.fillna(0).expanding().sum()
        c = g.fillna(0).expanding().count()
        return ((s - g.fillna(0)) / (c - 1).clip(lower=1)).fillna(0)

    df["prop_repuestos_past"] = df.groupby("placa_patente")["tiene_repuestos"].transform(_cummean_excl)

    # Correctivo density: correctivos per day since first event (for the bus)
    df["is_corr"] = (df["tipo_servicio"] == "CORRECTIVO").astype(int)
    df["cum_corr"] = df.groupby("placa_patente")["is_corr"].transform("cumsum") - df["is_corr"]
    df["corr_density"] = df["cum_corr"] / df["bus_age_days"].clip(lower=1)

    # Days since last event
    df["dias_desde_ultimo"] = (
        df.groupby("placa_patente")["fecha_evento"]
        .diff().dt.total_seconds().div(86400).fillna(999)
    )

    # Count past events within 30d (per-bus, O(n log n) via searchsorted)
    df["n_corr_30d"] = 0
    for bus, grp in df.groupby("placa_patente"):
        if len(grp) < 2:
            continue
        dates = grp["fecha_evento"].values
        counts = np.zeros(len(dates), dtype=int)
        for i in range(1, len(dates)):
            threshold = dates[i] - pd.Timedelta(days=30).to_timedelta64()
            j = np.searchsorted(dates[:i], threshold, side="left")
            counts[i] = i - j
        df.loc[grp.index, "n_corr_30d"] = counts

    # Vehicle km (if available)
    if "km_ejecucion" in df.columns:
        df["km_ejecucion"] = df["km_ejecucion"].fillna(df.groupby("placa_patente")["km_ejecucion"].transform("median"))
        df["km_ejecucion"] = df["km_ejecucion"].fillna(0)
    else:
        df["km_ejecucion"] = 0

    # Fleet-wide system parts rate
    sys_rate = (
        df[df["tipo_servicio"] == "CORRECTIVO"]
        .groupby("causa_sistema_reconstruida")["tiene_repuestos"]
        .mean()
        .to_dict()
    )
    df["sistema_parts_rate"] = df["causa_sistema_reconstruida"].map(sys_rate).fillna(0.5)

    # ── Filter to correctivos (non-OTROS), exclude non-mechanical CARROCERIA ──
    has_no_mec = "es_no_mecanico" in df.columns
    mask_corr = (df["tipo_servicio"] == "CORRECTIVO") & (df["causa_sistema_reconstruida"] != "OTROS")
    if has_no_mec:
        mask_corr = mask_corr & ~(
            (df["causa_sistema_reconstruida"] == "CARROCERIA") & (df["es_no_mecanico"] == 1)
        )
    result = df[mask_corr].copy()
    result["target"] = result["tiene_repuestos"].fillna(0).astype(int)
    result["sistema_actual"] = result["causa_sistema_reconstruida"].fillna("MISSING").astype(str)

    return result[[
        "sistema_actual", "prop_repuestos_past", "n_corr_30d",
        "dias_desde_ultimo", "bus_age_days", "corr_density",
        "km_ejecucion", "sistema_parts_rate", "target", "fecha_evento",
    ]]


def train_parts_model(df, test_days=180):
    """Train classifier to predict if a corrective event needs spare parts.

    Returns (model, metadata).
    """
    data = _build_parts_features(df)
    data["fecha_evento"] = pd.to_datetime(data["fecha_evento"])

    # One-hot sistema
    data = pd.get_dummies(data, columns=["sistema_actual"], drop_first=True)
    feature_cols = [c for c in data.columns if c not in ("target", "fecha_evento")]
    X = data[feature_cols].fillna(0).values
    y = data["target"].values

    cutoff = data["fecha_evento"].max() - pd.Timedelta(days=test_days)
    train_mask = data["fecha_evento"] < cutoff

    X_train, y_train = X[train_mask], y[train_mask]

    pos_rate = y_train.mean()
    model = XGBClassifier(
        n_estimators=250, max_depth=6, learning_rate=0.03,
        subsample=0.8, colsample_bytree=0.7, reg_alpha=0.3,
        random_state=42, n_jobs=1,
        scale_pos_weight=(1 - pos_rate) / max(pos_rate, 0.01),
    )
    if (test_mask := ~train_mask).sum() > 0:
        model.fit(X_train, y_train,
                  eval_set=[(X[test_mask], y[test_mask])],
                  verbose=False)
    else:
        model.fit(X_train, y_train, verbose=False)

    # Compute AUC on test set
    test_auc = None
    test_f1 = None
    test_precision = None
    test_recall = None
    test_accuracy = None
    test_confusion = None
    test_mask = ~train_mask
    if test_mask.sum() > 0 and len(np.unique(y[test_mask])) > 1:
        y_pred_proba = model.predict_proba(X[test_mask])[:, 1]
        y_pred = model.predict(X[test_mask])
        test_auc = round(float(roc_auc_score(y[test_mask], y_pred_proba)), 3)
        test_f1 = round(float(f1_score(y[test_mask], y_pred, zero_division=0)), 3)
        test_precision = round(float(precision_score(y[test_mask], y_pred, zero_division=0)), 3)
        test_recall = round(float(recall_score(y[test_mask], y_pred, zero_division=0)), 3)
        test_accuracy = round(float(accuracy_score(y[test_mask], y_pred)), 3)
        test_confusion = confusion_matrix(y[test_mask], y_pred).tolist()
        print(f"  parts model AUC: {test_auc}")

    metadata = {
        "feature_cols": feature_cols,
        "sistema_cols": [c for c in feature_cols if c.startswith("sistema_actual_")],
        "pos_rate": pos_rate,
        "prop_repuestos_past_mean": y_train.mean(),
        "test_auc": test_auc,
        "test_f1": test_f1,
        "test_precision": test_precision,
        "test_recall": test_recall,
        "test_accuracy": test_accuracy,
        "test_confusion": test_confusion,
        "naive_acc": round(max(pos_rate, 1 - pos_rate), 3),
        "top_features": sorted(
            zip(feature_cols, model.feature_importances_),
            key=lambda x: -x[1],
        )[:10],
    }
    gc.collect()
    return model, metadata


def predict_parts_probability(model, metadata, df, bus=None):
    """Predict parts probability for a bus's next corrective event.

    If bus is None, returns fleet-wide per-system breakdown.
    If bus is specified, returns probability for that specific bus.

    Returns DataFrame or dict.
    """
    df["fecha_evento"] = pd.to_datetime(df["fecha_evento"])

    if bus:
        bus_data = df[df["placa_patente"] == bus].sort_values("fecha_evento")
        if bus_data.empty:
            return {"placa_patente": bus, "error": "not found"}

        rep = bus_data["tiene_repuestos"].values
        systems = bus_data["causa_sistema_reconstruida"].values
        dates = bus_data["fecha_evento"].values

        prop_rep_past = float(rep[:-1].mean()) if len(rep) > 1 and pd.notna(rep[:-1]).any() else metadata["prop_repuestos_past_mean"]

        last_date = dates[-1]
        n_30d = int(((dates - last_date).astype("timedelta64[D]").astype(float) <= 30).sum()) - 1

        dias_ult = 999
        if len(dates) >= 2:
            dias_ult = (dates[-1] - dates[-2]).astype("timedelta64[D]").astype(float)

        bus_age = (dates[-1] - dates[0]).astype("timedelta64[D]").astype(float) if len(dates) > 0 else 0
        n_corr_total = int((bus_data["tipo_servicio"] == "CORRECTIVO").sum()) - 1
        corr_dens = n_corr_total / max(bus_age, 1)

        km_val = float(bus_data["km_ejecucion"].dropna().iloc[-1]) if "km_ejecucion" in bus_data.columns and bus_data["km_ejecucion"].notna().any() else 0

        ultimo_sis = str(systems[-1]) if pd.notna(systems[-1]) else "MISSING"
        # Prefer non-OTROS system
        non_otros = bus_data[bus_data["causa_sistema_reconstruida"] != "OTROS"]
        if not non_otros.empty:
            ultimo_sis = str(non_otros["causa_sistema_reconstruida"].values[-1])
        elif pd.notna(systems[-1]):
            ultimo_sis = str(systems[-1])
        sis_rate = float(
            df[(df["tipo_servicio"] == "CORRECTIVO") & (df["causa_sistema_reconstruida"] == ultimo_sis)]["tiene_repuestos"].mean()
        ) if ultimo_sis != "MISSING" and ultimo_sis != "OTROS" else metadata["prop_repuestos_past_mean"]
        if np.isnan(sis_rate):
            sis_rate = metadata["prop_repuestos_past_mean"]

        row = {
            "prop_repuestos_past": prop_rep_past,
            "n_corr_30d": max(0, n_30d),
            "dias_desde_ultimo": dias_ult,
            "bus_age_days": bus_age,
            "corr_density": corr_dens,
            "km_ejecucion": km_val,
            "sistema_parts_rate": sis_rate,
        }
        for col in metadata["sistema_cols"]:
            parts = col.split("_", 2)
            value = parts[2] if len(parts) > 2 else ""
            row[col] = 1 if value == ultimo_sis else 0

        X_pred = pd.DataFrame([row])[metadata["feature_cols"]].fillna(0)
        prob = model.predict_proba(X_pred)[0][1]

        return {
            "placa_patente": bus,
            "prob_repuestos": round(float(prob), 3),
            "ultimo_sistema": ultimo_sis,
        }

    # Fleet-wide: per-system breakdown (exclude OTROS)
    systems = [
        s for s in df[df["tipo_servicio"] == "CORRECTIVO"]["causa_sistema_reconstruida"].dropna().unique()
        if s != "OTROS"
    ]
    results = []
    for sistema in systems:
        row = {
            "prop_repuestos_past": metadata["prop_repuestos_past_mean"],
            "n_corr_30d": 10,
            "dias_desde_ultimo": 3,
            "bus_age_days": 500,
            "corr_density": 0.02,
            "km_ejecucion": 200000,
            "sistema_parts_rate": metadata["prop_repuestos_past_mean"],
        }
        for col in metadata["sistema_cols"]:
            parts = col.split("_", 2)
            value = parts[2] if len(parts) > 2 else ""
            row[col] = 1 if value == sistema else 0

        X_pred = pd.DataFrame([row])[metadata["feature_cols"]].fillna(0)
        prob = model.predict_proba(X_pred)[0][1]
        results.append({"sistema": sistema, "prob_repuestos": round(float(prob), 3)})

    return pd.DataFrame(results).sort_values("prob_repuestos", ascending=False)


# ═══════════════════════════════════════════════════════════════════════════════
# SYSTEM LABEL ENRICHMENT FROM TEXT + INSPECTION ALERTS
# ═══════════════════════════════════════════════════════════════════════════════

SYSTEM_KEYWORDS = {
    'MOTOR': ['MOTOR', 'EMBRAGUE', 'CORREA', 'ACEITE', 'REFRIGERANTE', 'RADIADOR',
              'TURBO', 'INYECTOR', 'BOMBA DE', 'COMPRESOR', 'ESCAPE', 'EMISION',
              'CILINDRO', 'PISTON', 'VALVULA', 'CULATA', 'CARTER', 'ENFRIADOR'],
    'FRENOS': ['FRENO', 'PASTILLA', 'DISC', 'CALIPER', 'ZAPATA', 'TAMBOR',
               'FRENO DE', 'SISTEMA DE FRENO'],
    'PUERTAS': ['PUERTA', 'CIERRE', 'APERTURA DE PUERTA', 'MECANISM'],
    'SUSPENSION': ['SUSPENSION', 'AMORTIGUADOR', 'RESORTE', 'BALANCIN',
                   'BUJE', 'ROTULA', 'SILENTBLOCK', 'MUELLE', 'BARRA DIRECC'],
    'ELECTRICO': ['ELECTRIC', 'BATERIA', 'ALTERNADOR', 'LUCES', 'FARO',
                  'FOCO', 'FUSIBLE', 'CORTOCIRCUITO', 'TABLERO', 'ARRANQUE',
                  'SENSOR', 'CARGADOR', 'CABLEADO', 'CONEXION ELECTR'],
    'CLIMATIZACION': ['CLIMATIZACION', 'CLIMATIZAC', 'AIRE ACOND', 'CALEFACC',
                      'VENTILADOR', 'TEMPERATURA'],
    'RUEDAS': ['NEUMATICO', 'NEUMATICOS', 'LLANTA', 'RUEDA', 'DESGASTE'],
    'CARROCERIA': ['CARROCERIA', 'CARROCER', 'CHASIS', 'PINTURA', 'OXIDO',
                   'GOLPE', 'ABOLLADURA', 'RAJADURA', 'PANEL', 'CIELO',
                   'PISO', 'VENTANA', 'ESPEJO', 'PARABRISA', 'ASIENTO',
                   'CINTURON', 'DESPREND', 'ESTRUCTUR', 'DANO', 'DANADO',
                   'GOTERA', 'FILTRACION', 'RAMPA', 'PASAMANOS'],
}


def enrich_system_labels(df):
    """Improve causa_sistema_reconstruida by extracting systems from text.

    Uses keywords from observacion_clean, obs_inspeccion_clean, and
    causa_origen_clean to reclassify events currently labeled as OTROS.

    Returns DataFrame with a new column 'sistema_enriched'.
    """

    def _safe(v):
        if pd.isna(v) or str(v).strip() in ('', '-', 'MISSING'):
            return ''
        return str(v).upper()

    for col in ['observacion_clean', 'obs_inspeccion_clean', 'causa_origen_clean']:
        if col not in df.columns:
            df[col] = ''

    df['_texto'] = (
        df['observacion_clean'].apply(_safe) + ' ' +
        df['obs_inspeccion_clean'].apply(_safe) + ' ' +
        df['causa_origen_clean'].apply(_safe)
    )

    df['sistema_enriched'] = df['causa_sistema_reconstruida']

    otros_mask = (df['causa_sistema_reconstruida'] == 'OTROS').values
    for system, keywords in SYSTEM_KEYWORDS.items():
        mask = np.zeros(len(df), dtype=bool)
        for kw in keywords:
            mask |= df['_texto'].str.contains(kw, na=False)
        reclassify = mask & otros_mask
        df.loc[reclassify, 'sistema_enriched'] = system

    df = df.drop(columns=['_texto'])
    return df


def compute_inspection_alerts(df, max_days_since=90):
    """Find buses that failed their last REGB/IT inspection and haven't had a
    correctivo since. These buses need attention.

    Returns DataFrame with columns: placa_patente, fecha_inspeccion,
    dias_desde, defectos_highs, observacion, taller.
    """
    df = df.sort_values(['placa_patente', 'fecha_evento'])

    inspec = df[df['tipo_servicio'].isin(['REGB', 'IT'])]
    corr = df[df['tipo_servicio'] == 'CORRECTIVO']
    last_date = df['fecha_evento'].max()

    last_insp = inspec.groupby('placa_patente').tail(1)
    failed = last_insp[last_insp['resultado_pasa'] == 0]

    alerts = []
    for _, row in failed.iterrows():
        bus = row['placa_patente']
        insp_date = row['fecha_evento']

        after = corr[(corr['placa_patente'] == bus) & (corr['fecha_evento'] > insp_date)]
        if len(after) > 0:
            continue

        dias = (last_date - insp_date).total_seconds() / 86400
        if dias > max_days_since:
            continue

        obs = row.get('observacion_clean', '')
        if pd.isna(obs) or str(obs).strip() in ('', 'MISSING'):
            obs = row.get('obs_inspeccion_clean', '')
        if pd.isna(obs) or str(obs).strip() == 'MISSING':
            obs = ''

        es_no_presentado = int(row.get('es_no_presentado', 0) or 0)

        alerts.append({
            'placa_patente': bus,
            'fecha_inspeccion': insp_date,
            'dias_desde': int(dias),
            'defectos_highs': int(row.get('inspeccion_total_highs', 0) or 0),
            'defectos_totales': int(
                (row.get('inspeccion_total_highs', 0) or 0) +
                (row.get('inspeccion_total_mediums', 0) or 0) +
                (row.get('inspeccion_total_lows', 0) or 0)
            ),
            'observacion': str(obs)[:200],
            'taller': row.get('taller_planta_grouped', ''),
            'es_no_presentado': es_no_presentado,
        })

    result = pd.DataFrame(alerts)
    if result.empty:
        return result

    result = result.sort_values('dias_desde')

    # Add risk level (no_presentado = not a real failure, demoted)
    def _risk(row):
        if row.get('es_no_presentado', 0):
            return 'ℹ️ No presentado'
        dias = row['dias_desde']
        if dias <= 7:
            return '🔴 Urgente'
        if dias <= 14:
            return '🟠 Atención'
        return '🟡 Pendiente'

    result['riesgo'] = result.apply(_risk, axis=1)
    gc.collect()
    return result.reset_index(drop=True)


def bus_inspection_history(df, bus):
    """Get inspection history for a specific bus.

    Returns dict with last_inspection details and summary stats.
    """
    bus_data = df[df['placa_patente'] == bus].sort_values('fecha_evento')
    insp = bus_data[bus_data['tipo_servicio'].isin(['REGB', 'IT'])]

    if insp.empty:
        return {'has_inspections': False}

    last = insp.iloc[-1]
    passed = last.get('resultado_pasa', -1)

    obs = last.get('observacion_clean', '')
    if pd.isna(obs) or str(obs).strip() in ('', 'MISSING'):
        obs = last.get('obs_inspeccion_clean', '')
    if pd.isna(obs) or str(obs).strip() == 'MISSING':
        obs = ''

    # Extract system mentions
    sistemas_afectados = []
    texto = str(obs).upper() + ' ' + str(last.get('causa_origen_clean', '')).upper()
    for system, keywords in SYSTEM_KEYWORDS.items():
        for kw in keywords:
            if kw in texto and system not in sistemas_afectados:
                sistemas_afectados.append(system)
                break

    return {
        'has_inspections': True,
        'total_inspections': len(insp),
        'last_date': last['fecha_evento'],
        'passed': bool(passed == 1),
        'defectos_highs': int(last.get('inspeccion_total_highs', 0) or 0),
        'defectos_mediums': int(last.get('inspeccion_total_mediums', 0) or 0),
        'defectos_lows': int(last.get('inspeccion_total_lows', 0) or 0),
        'observacion': str(obs)[:300],
        'sistemas_detectados': sistemas_afectados,
        'taller': last.get('taller_planta_grouped', ''),
    }


# ═══════════════════════════════════════════════════════════════════════════════
# TARGET 7: Inspection failure + system risk + readiness (REGB / IT)
# ═══════════════════════════════════════════════════════════════════════════════

INSPECTION_PERIOD_DAYS = {"REGB": 80, "IT": 147}

INSPECTION_SYSTEMS = [
    "MOTOR", "FRENOS", "ELECTRICO", "SUSPENSION",
    "PUERTAS", "RUEDAS", "CLIMATIZACION", "CARROCERIA", "SEGURIDAD",
]


def _build_inspection_features(df):
    """Build feature rows for each inspection event from pre-inspection history."""
    df = df.sort_values(["placa_patente", "fecha_evento"]).copy()
    df["fecha_evento"] = pd.to_datetime(df["fecha_evento"])

    inspec = df[df["tipo_servicio"].isin(["REGB", "IT"])]
    corr = df[df["tipo_servicio"] == "CORRECTIVO"]
    has_no_mec = "es_no_mecanico" in corr.columns
    if has_no_mec:
        corr = corr[~((corr["causa_sistema_reconstruida"] == "CARROCERIA") & (corr["es_no_mecanico"] == 1))]

    rows = []
    for bus, grp in inspec.groupby("placa_patente"):
        grp = grp.sort_values("fecha_evento")
        bus_corr = corr[corr["placa_patente"] == bus]
        for i in range(1, len(grp)):
            insp = grp.iloc[i]
            prev = grp.iloc[i - 1]
            insp_date = insp["fecha_evento"]
            prev_date = prev["fecha_evento"]
            tipo = insp["tipo_servicio"]

            between = bus_corr[(bus_corr["fecha_evento"] > prev_date) & (bus_corr["fecha_evento"] <= insp_date)]
            n = len(between)
            in_30d = between[between["fecha_evento"] >= insp_date - pd.Timedelta(days=30)]
            in_90d = between[between["fecha_evento"] >= insp_date - pd.Timedelta(days=90)]

            dias_desde = max((insp_date - prev_date).total_seconds() / 86400, 0)
            prev_passed = int(prev.get("resultado_pasa", 1)) if pd.notna(prev.get("resultado_pasa")) else 1
            prev_highs = int(prev.get("inspeccion_total_highs", 0) or 0)
            prev_no_pres = int(prev.get("es_no_presentado", 0) or 0)

            row = {
                "placa_patente": bus, "fecha_evento": insp_date, "tipo": tipo,
                "dias_desde_ultima": dias_desde,
                "n_corr_entre": n,
                "n_corr_30d": len(in_30d),
                "n_corr_90d": len(in_90d),
                "sistemas_distintos": between["causa_sistema_reconstruida"].nunique() if n > 0 else 0,
                "prop_repuestos": between["tiene_repuestos"].mean() if n > 0 and "tiene_repuestos" in between.columns else 0,
                "duracion_promedio": between["duracion_ot_horas"].mean() if n > 0 and "duracion_ot_horas" in between.columns else 0,
                "prev_resultado": prev_passed,
                "prev_defectos_highs": prev_highs,
                "prev_no_presentado": prev_no_pres,
            }
            for s in INSPECTION_SYSTEMS:
                row[f"corr_{s}"] = len(between[between["causa_sistema_reconstruida"] == s])
            row["target"] = int(insp.get("resultado_pasa", 1)) == 0 if pd.notna(insp.get("resultado_pasa")) else 0

            rows.append(row)

    return pd.DataFrame(rows).fillna(0)


def train_inspection_model(df, test_days=365):
    """Train classifier to predict if a bus will fail its next REGB/IT inspection.

    Uses per-system corrective history between inspections as features.
    Returns (model, metadata).
    """
    data = _build_inspection_features(df)
    if data.empty or data["target"].nunique() < 2:
        return None, {"feature_cols": [], "pos_rate": 0.5, "test_auc": None}

    data = pd.get_dummies(data, columns=["tipo"], drop_first=True)
    feature_cols = [
        "dias_desde_ultima", "n_corr_entre", "n_corr_30d", "n_corr_90d",
        "sistemas_distintos", "prop_repuestos", "duracion_promedio",
        "prev_resultado", "prev_defectos_highs", "prev_no_presentado",
    ] + [f"corr_{s}" for s in INSPECTION_SYSTEMS]
    feature_cols += [c for c in data.columns if c.startswith("tipo_")]
    feat_present = [c for c in feature_cols if c in data.columns]

    X = data[feat_present].fillna(0).values
    y = data["target"].values
    dates = data["fecha_evento"].values

    cutoff = dates.max() - pd.Timedelta(days=test_days)
    train_mask = dates < cutoff
    X_train, y_train = X[train_mask], y[train_mask]

    if len(y_train) == 0:
        return None, {"feature_cols": feat_present, "pos_rate": 0.5, "test_auc": None}

    pos_rate = y_train.mean()
    model = XGBClassifier(
        n_estimators=100, max_depth=4, learning_rate=0.05,
        subsample=0.8, random_state=42, n_jobs=1,
        scale_pos_weight=(1 - pos_rate) / max(pos_rate, 0.01),
    )
    model.fit(X_train, y_train, verbose=False)

    test_auc = None
    test_f1 = None
    test_mask = ~train_mask
    if test_mask.sum() > 0 and len(np.unique(y[test_mask])) > 1:
        y_pred_proba = model.predict_proba(X[test_mask])[:, 1]
        y_pred = model.predict(X[test_mask])
        test_auc = round(float(roc_auc_score(y[test_mask], y_pred_proba)), 3)
        test_f1 = round(float(f1_score(y[test_mask], y_pred, zero_division=0)), 3)
        print(f"  inspection model AUC: {test_auc}")

    importances = sorted(zip(feat_present, model.feature_importances_), key=lambda x: -x[1])[:10]

    metadata = {
        "feature_cols": feat_present,
        "pos_rate": pos_rate,
        "test_auc": test_auc,
        "test_f1": test_f1,
        "top_features": [(n, round(float(v), 4)) for n, v in importances],
    }
    gc.collect()
    return model, metadata


def predict_inspection_risk(model, metadata, df):
    """Predict inspection failure risk, system-level risks, and readiness.

    Returns DataFrame with: placa_patente, tipo, dias_para_prox, prob_falla,
    riesgo, readiness, sistemas_riesgo, accion_recomendada, ultima_fecha,
    correctivos_desde, taller.
    """
    if model is None:
        return pd.DataFrame()

    df = df.sort_values(["placa_patente", "fecha_evento"]).copy()
    df["fecha_evento"] = pd.to_datetime(df["fecha_evento"])
    last_date = df["fecha_evento"].max()

    inspec = df[df["tipo_servicio"].isin(["REGB", "IT"])]
    corr = df[df["tipo_servicio"] == "CORRECTIVO"]
    has_no_mec = "es_no_mecanico" in corr.columns
    if has_no_mec:
        corr = corr[~((corr["causa_sistema_reconstruida"] == "CARROCERIA") & (corr["es_no_mecanico"] == 1))]

    # Fleet-wide system baseline: average correctivos per system across fleet
    fleet_system_avg = {}
    for s in INSPECTION_SYSTEMS:
        avg = corr[corr["causa_sistema_reconstruida"] == s].groupby("placa_patente").size().mean()
        fleet_system_avg[s] = max(avg, 0.5) if not np.isnan(avg) else 0.5

    results = []
    for bus, bus_insp in inspec.groupby("placa_patente"):
        bus_insp = bus_insp.sort_values("fecha_evento")
        bus_corr = corr[corr["placa_patente"] == bus]

        for tipo in ["REGB", "IT"]:
            tipo_insp = bus_insp[bus_insp["tipo_servicio"] == tipo]
            if tipo_insp.empty:
                continue

            ultima = tipo_insp.iloc[-1]
            ultima_date = ultima["fecha_evento"]
            expected_period = INSPECTION_PERIOD_DAYS.get(tipo, 80)

            dias_desde = (last_date - ultima_date).total_seconds() / 86400
            dias_para_prox = expected_period - dias_desde

            since_last = bus_corr[bus_corr["fecha_evento"] > ultima_date]
            in_30d = since_last[since_last["fecha_evento"] >= last_date - pd.Timedelta(days=30)]
            in_90d = since_last[since_last["fecha_evento"] >= last_date - pd.Timedelta(days=90)]
            n = len(since_last)

            prop_rep = since_last["tiene_repuestos"].mean() if n > 0 and "tiene_repuestos" in since_last.columns else 0
            dur_prom = since_last["duracion_ot_horas"].mean() if n > 0 and "duracion_ot_horas" in since_last.columns else 0
            prev_passed = int(ultima.get("resultado_pasa", 1)) if pd.notna(ultima.get("resultado_pasa")) else 1
            prev_highs = int(ultima.get("inspeccion_total_highs", 0) or 0)
            prev_no_pres = int(ultima.get("es_no_presentado", 0) or 0)

            prev_idx = max(len(tipo_insp) - 2, 0)
            dias_desde_ultima = (ultima_date - tipo_insp.iloc[prev_idx]["fecha_evento"]).total_seconds() / 86400 if prev_idx >= 0 else dias_desde

            row = {c: 0 for c in metadata["feature_cols"]}
            row.update({
                "dias_desde_ultima": dias_desde_ultima or dias_desde,
                "n_corr_entre": n,
                "n_corr_30d": len(in_30d),
                "n_corr_90d": len(in_90d),
                "sistemas_distintos": since_last["causa_sistema_reconstruida"].nunique() if n > 0 else 0,
                "prop_repuestos": 0 if np.isnan(prop_rep) else prop_rep,
                "duracion_promedio": 0 if np.isnan(dur_prom) else dur_prom,
                "prev_resultado": prev_passed,
                "prev_defectos_highs": prev_highs,
                "prev_no_presentado": prev_no_pres,
            })
            for s in INSPECTION_SYSTEMS:
                row[f"corr_{s}"] = len(since_last[since_last["causa_sistema_reconstruida"] == s])
            for col in metadata["feature_cols"]:
                if col.startswith("tipo_"):
                    row[col] = 1 if col.replace("tipo_", "") == tipo else 0

            X_pred = pd.DataFrame([row])[metadata["feature_cols"]].fillna(0)
            prob = float(model.predict_proba(X_pred)[0][1])

            # ── System risk breakdown ──
            sistemas_riesgo = {}
            for s in INSPECTION_SYSTEMS:
                bus_corr_s = row.get(f"corr_{s}", 0)
                fleet_avg = fleet_system_avg.get(s, 0.5)
                excess = max(0, bus_corr_s - fleet_avg * 0.5)
                if bus_corr_s > 0 and bus_corr_s >= fleet_avg * 0.3:
                    sistemas_riesgo[s] = round(min(excess / max(fleet_avg, 0.1), 1.0), 2)

            sistemas_riesgo = dict(sorted(sistemas_riesgo.items(), key=lambda x: -x[1])[:5])

            # ── Readiness score (0-100, higher = ready to pass) ──
            readiness = 100
            if prob > 0.5: readiness -= 40
            elif prob > 0.3: readiness -= 25
            elif prob > 0.15: readiness -= 10
            for s, risk in sistemas_riesgo.items():
                readiness -= risk * 15
            if prev_no_pres: readiness -= 20
            if prev_highs > 0: readiness -= 10
            if dias_para_prox < -30: readiness -= 10  # very overdue
            readiness = max(0, min(100, readiness))

            # ── Recommended action ──
            if prob >= 0.5 and sistemas_riesgo:
                top_sys = list(sistemas_riesgo.keys())[:3]
                accion = f"Revisar {' + '.join(top_sys)} antes de inspección"
            elif prob >= 0.3 and sistemas_riesgo:
                top_sys = list(sistemas_riesgo.keys())[:2]
                accion = f"Verificar {' + '.join(top_sys)}"
            elif prev_no_pres:
                accion = "Agendar y confirmar asistencia"
            elif dias_para_prox <= 0:
                accion = "Agendar inspección pendiente"
            elif prob < 0.1 and dias_para_prox > 14:
                accion = "En buen estado"
            else:
                accion = "Monitoreo regular"

            taller = str(ultima.get("taller_planta_grouped", ""))[:30]
            if not taller:
                taller = str(bus_corr["taller_planta_grouped"].mode().iloc[0]) if not bus_corr.empty and "taller_planta_grouped" in bus_corr.columns else ""

            results.append({
                "placa_patente": bus,
                "tipo": tipo,
                "dias_para_prox": round(dias_para_prox, 0),
                "prob_falla": round(prob, 3),
                "readiness": round(readiness, 0),
                "sistemas_riesgo": ", ".join(f"{s}({r:.0%})" for s, r in sistemas_riesgo.items()) if sistemas_riesgo else "—",
                "accion_recomendada": accion,
                "ultima_fecha": ultima_date,
                "correctivos_desde": n,
                "taller": taller,
            })

    result = pd.DataFrame(results)
    if result.empty:
        return result

    def _riesgo(row):
        if row["dias_para_prox"] <= 0 and row["prob_falla"] >= 0.3:
            return "🔴 Urgente"
        if row["dias_para_prox"] <= 0:
            return "🟠 Vencida"
        if row["prob_falla"] >= 0.5:
            return "🟡 Alto riesgo"
        if row["prob_falla"] >= 0.3:
            return "🔵 Riesgo medio"
        return "🟢 Normal"

    result["riesgo"] = result.apply(_riesgo, axis=1)
    result = result.sort_values(["dias_para_prox", "prob_falla"], ascending=[True, False])
    return result.reset_index(drop=True)


def bus_inspection_readiness(model, metadata, df, bus):
    """Detailed inspection readiness for a single bus (used in Buscar Bus tab)."""
    if model is None:
        return None
    fleet = predict_inspection_risk(model, metadata, df)
    if fleet.empty:
        return None
    bus_data = fleet[fleet["placa_patente"] == bus]
    if bus_data.empty:
        return None
    return bus_data.to_dict("records")
