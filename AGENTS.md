# Commands

```bash
streamlit run app.py                           # Predicción Operacional (localhost:8501)
python3 scripts/refresh_data.py                # Firestore → data/processed/base.parquet
```

**Virtual env:** `venv/` at repo root. Activate: `source venv/bin/activate`

**Requirements:** `requirements.txt` is incomplete (5 packages). Actual deps installed in venv include `firebase-admin`, `xgboost`, `scikit-learn`, `plotly`, `streamlit`, `pyarrow`, `pandas`, `numpy`.

**No tests, no lint, no CI, no pre-commit hooks.**

# Architecture

```
Firestore (4 collections)
  → scripts/refresh_data.py   (incremental fetch + rebuild)
  → data/processed/base.parquet  (167 cols, 423k events, 2976 buses)
  → app.py                    (Streamlit prediction app, 3 tabs)
```

- `src/` — data layer: `data_loader.py` (Firestore + JSON, nested flattening), `preprocessing.py` (cleaning, feature extraction), `evaluation.py` (metrics, shadow eval)
- `scripts/analytics/` — models layer:
  - `predictive.py` — health scores, observation alerts, terminal load forecast (XGBoost)
  - `operational.py` — spike risk (T5), weekly system load (T6), parts probability (T3)
  - `fleet_metrics.py`, `terminal_load.py`, `bus_anomaly.py`, `recurrence.py` — reference only
- `archived/` — old model pipeline (including broken `src.models` package), dead scripts
- `models/` — saved XGBoost from old pipeline, not loaded by dashboard
- `config/piloto1.json` — pilot bus list, archived config

## Dead code (moved to archived/)

- `archived/scripts/run_model_experiment.py` — imports from deleted `src.models`
- `archived/scripts/consultar_bus.py` — same, old pipeline

# Service account

- `slared-4de9d5a1e961.json` required in project root for Firestore
- Gitignored by `*.json` rule (exception: `!outputs/piloto1_report.json`)

# Analytics module reference

### `scripts/analytics/predictive.py`
- `compute_health_scores(df)` → DataFrame per bus (0-100 composite)
- `compute_observation_alerts(df, window_days=30)` → severe observation alerts
- `score_category(score)` → "🔴 Crítico" / "🟠 Atención" / "🟡 Moderado" / "🟢 Normal"
- `train_terminal_forecast(df, test_days=90)` → XGBoost terminal load model (R² ≈ 0.56)

### `scripts/analytics/operational.py`
- `train_weekly_system_load(df)` → XGBoost per-system+terminal weekly forecast (R² ≈ 0.81 with enriched labels)
- `predict_weekly_system_load(model, meta, df, weeks_ahead=4)` → DataFrame
- `train_bus_spike_model(df)` → classifier for ≥10 correctivos/month (AUC ≈ 0.82)
- `predict_bus_spikes(model, meta, df)` → per-bus spike probability + risk category
- `train_parts_model(df)` → classifier for parts needed (AUC ≈ 0.68)
- `predict_parts_probability(model, meta, df, bus=None)` → fleet-wide or per-bus
- `enrich_system_labels(df)` → extracts system from observacion text, reduces OTROS by ~22k events
- `compute_inspection_alerts(df)` → buses that failed REGB/IT with no follow-up correctivo
- `bus_inspection_history(df, bus)` → inspection summary for individual bus

# Dashboard tabs

1. **Riesgo** — health score distribution + spike risk ranking + observation alerts + inspection alerts + recommendations
2. **Planificación** — weekly load forecast per system+terminal + parts estimation
3. **Buscar Bus** — individual bus lookup with health score, spike risk, parts probability, inspection history, event timeline

# Data context

- 423k events, 2976 buses, 9 main terminals, 2021-02 → 2026-06
- Types: CORRECTIVO (64%), PREVENTIVO (26%), REGB (5.5%), IT (4.5%)
- Top systems: OTROS (65%, reduced from 70% via text enrichment), MOTOR (7.3%), FRENOS (6.8%), CARROCERIA (5.1%)
- Events drop ~50% on weekends; median repair duration 1h (mean 11h, skewed)
- 34% of correctivos use spare parts; median inter-correctivo time 4 days
