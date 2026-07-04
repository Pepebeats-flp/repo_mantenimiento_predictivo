import pandas as pd
import numpy as np


def compute_daily_loads(df):
    df = df.copy()
    df["fecha"] = df["fecha_evento"].dt.date
    df["dow"] = df["fecha_evento"].dt.dayofweek
    daily = (
        df.groupby(["fecha", "taller_planta_grouped", "dow"])
        .size()
        .reset_index(name="eventos")
    )
    return daily


def compute_weekly_profile(daily_loads):
    profile = (
        daily_loads.groupby(["taller_planta_grouped", "dow"])["eventos"]
        .agg(["mean", "std", "median", "count"])
        .reset_index()
    )
    profile.columns = [
        "terminal",
        "dow",
        "media",
        "std",
        "mediana",
        "num_semanas",
    ]
    profile["dia"] = profile["dow"].map(
        {
            0: "Lun",
            1: "Mar",
            2: "Mie",
            3: "Jue",
            4: "Vie",
            5: "Sab",
            6: "Dom",
        }
    )
    return profile


def compute_today_vs_expected(daily_loads, profile):
    today = pd.Timestamp.now().date()
    today_dow = today.weekday()
    yesterday = today - pd.Timedelta(days=1)
    recent_days = []

    for offset in [0, -1, -2, -3, -4, -5, -6]:
        d = today + pd.Timedelta(days=offset)
        day_data = daily_loads[daily_loads["fecha"] == d]
        expected = profile[profile["dow"] == d.weekday()]
        for _, row in expected.iterrows():
            actual = int(
                day_data[day_data["taller_planta_grouped"] == row["terminal"]][
                    "eventos"
                ].sum()
            )
            recent_days.append(
                {
                    "fecha": str(d),
                    "terminal": row["terminal"],
                    "actual": actual,
                    "esperado": round(row["media"], 1),
                    "desviacion": round(actual - row["media"], 1),
                    "desviacion_pct": round((actual - row["media"]) / row["media"] * 100, 1)
                    if row["media"] > 0
                    else None,
                }
            )
    return pd.DataFrame(recent_days)


def forecast_week(daily_loads, profile, start_date=None):
    if start_date is None:
        start_date = pd.Timestamp.now().date()

    # Get the most recent data to compute trend
    recent_weeks = daily_loads[
        daily_loads["fecha"] >= (start_date - pd.Timedelta(days=56))
    ].copy()
    recent_weeks["semana"] = pd.to_datetime(recent_weeks["fecha"]).dt.isocalendar().week.astype(int)
    weekly_totals = (
        recent_weeks.groupby(["taller_planta_grouped", "semana"])["eventos"]
        .sum()
        .reset_index()
    )

    rows = []
    terminals = profile["terminal"].unique()
    for term in terminals:
        term_profile = profile[profile["terminal"] == term]
        term_weekly = weekly_totals[weekly_totals["taller_planta_grouped"] == term]
        trend = 0
        if len(term_weekly) >= 4:
            x = np.arange(len(term_weekly))
            y = term_weekly["eventos"].values
            if y.std() > 0:
                coeffs = np.polyfit(x, y, 1)
                trend = coeffs[0]

        for day_offset in range(7):
            d = start_date + pd.Timedelta(days=day_offset)
            dow = d.weekday()
            base = term_profile[term_profile["dow"] == dow]["media"].values
            if len(base) > 0:
                predicted = max(0, round(base[0] + trend * 0.02))
                rows.append(
                    {
                        "fecha": str(d),
                        "dia": {0: "Lun", 1: "Mar", 2: "Mie", 3: "Jue", 4: "Vie", 5: "Sab", 6: "Dom"}[
                            dow
                        ],
                        "terminal": term,
                        "pronostico": int(predicted),
                    }
                )
    return pd.DataFrame(rows)
