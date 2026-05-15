# Commands for Piloto 1 — Shadow Mode

## Pipeline
```bash
python3 scripts/run_pipeline.py --local-json               # ETL → Train → Infer (local JSON)
python3 scripts/run_pipeline.py                             # ETL → Train → Infer (Firestore)
python3 scripts/run_pipeline.py --experiment <name>         # Run with experiment config override
```

## Experiments (try different model params, features, thresholds)
```bash
python3 scripts/run_experiment.py <name> --local-json       # Run one experiment
python3 scripts/run_experiment.py --list                    # List available experiments
python3 scripts/compare_experiments.py                      # Compare all experiment results
python3 scripts/deploy_experiment.py <name>                 # Deploy winning experiment to live dirs
```

Experiment configs: `config/experiments/<name>.json`
- `model_params`: overrides OPTIMIZED_PARAMS (max_depth, learning_rate, etc.)
- `scale_pos_weight_multiplier`: multiply default scale_pos_weight (e.g. 3.0)
- `threshold`: single decision threshold override for all horizons
- `thresholds`: dict of per-horizon thresholds, e.g. {"7": 0.6, "5": 0.6, "3": 0.6}
- `select_threshold`: if true, selects best threshold from a held-out validation split
- `drop_features`: list of features to exclude
- `keep_features`: list of features to keep exclusively

## Shadow Mode Evaluation
```bash
python3 scripts/evaluate_shadow.py                          # Compare predictions vs actual events
python3 scripts/evaluate_shadow.py <preds.parquet> --save <out.json> --skip-ranking
```

## Dashboard
```bash
streamlit run app.py                                        # Launch Streamlit dashboard
```

## Daily Inference (scheduled)
```bash
python3 scripts/daily_inference.py                          # Fetch from Firestore, predict, save log
```

## Query Buses
```bash
python3 scripts/consultar_bus.py --top 10                   # Top 10 at-risk buses
python3 scripts/consultar_bus.py FLXS22                     # Specific bus details
```

## Best Config Found (009_th06_all)
```json
{"model_params": {"max_depth": 12, "min_child_weight": 2}, "scale_pos_weight_multiplier": 3.0, "thresholds": {"7": 0.6, "5": 0.6, "3": 0.6}}
```

Pilot bus ACC improvement with th=0.6 vs default th=0.5:
| Bus | H | th=0.5 | th=0.6 | Δ |
|---|---|---|---|---|
| FLXS22 | 3 | 0.832 | **0.935** | +0.103 |
| FLXS23 | 3 | 0.804 | **0.920** | +0.116 |
| LWTK42 | 3 | 0.868 | **0.912** | +0.044 |
| FLXS22 | 5 | 0.785 | **0.860** | +0.075 |
| FLXS23 | 5 | 0.723 | **0.813** | +0.090 |
| LWTK42 | 5 | 0.780 | **0.846** | +0.066 |
| FLXS22 | 7 | 0.710 | **0.785** | +0.075 |
| FLXS23 | 7 | 0.652 | **0.768** | +0.116 |
| LWTK42 | 7 | 0.670 | **0.725** | +0.055 |
