#!/usr/bin/env python3
"""Run model experiments with feature pruning and hyperparameter tuning.

Usage:
    python scripts/run_model_experiment.py --list
    python scripts/run_model_experiment.py --fast xgboost     # Baseline
    python scripts/run_model_experiment.py --fast xgboost --drop-zero  # Drop useless features
    python scripts/run_model_experiment.py --fast xgboost --spw-per-horizon  # Per-horizon SPW
    python scripts/run_model_experiment.py --run-all           # Run all combos fast
    python scripts/run_model_experiment.py --compare           # Compare saved results
"""
from __future__ import annotations

import json
import sys
import warnings
from pathlib import Path

warnings.filterwarnings("ignore")

sys.path.append(str(Path(__file__).resolve().parent.parent))

from src.models import (
    XGBoostModel,
    LightGBMModel,
    CatBoostModel,
)
from src.models.trainer import train_eval_split, DROP_FEATURES

PROJECT_ROOT = Path(__file__).resolve().parent.parent
FEATURES_PATH = PROJECT_ROOT / "data" / "processed" / "features_train.parquet"
OUTPUTS_DIR = PROJECT_ROOT / "outputs" / "model_experiments"

ALL_MODELS = ["xgboost", "lightgbm", "catboost"]

# Per-horizon SPW multipliers (derived from experiments showing 7d needs less)
SPW_BY_HORIZON = {7: 1.0, 5: 2.0, 3: 4.0}


def build_model(name: str, n_est: int = 300):
    name = name.lower()
    common = dict(max_depth=12, learning_rate=0.03, subsample=0.85, colsample_bytree=0.85)
    if name == "xgboost":
        return XGBoostModel(n_estimators=n_est, **common, min_child_weight=2, scale_pos_weight=None)
    elif name == "lightgbm":
        return LightGBMModel(n_estimators=n_est, **common, min_child_weight=2, num_leaves=63, scale_pos_weight=None)
    elif name == "catboost":
        return CatBoostModel(iterations=n_est, max_depth=8, learning_rate=0.03, subsample=0.85, scale_pos_weight=None)
    else:
        raise ValueError(f"Unknown model: {name}. Choose from: {ALL_MODELS}")


def print_results(results: dict, show_pilot: bool = True):
    name = results.get("model", "?")
    mode = results.get("mode", "?")
    spw = results.get("spw_multiplier", "?")
    nf = results.get("n_features_initial", "?")
    drops = results.get("drop_features", [])
    print(f"\n{'='*60}")
    print(f"  RESULTS — {name} ({mode})")
    print(f"  Features: {nf} | SPW: {spw} | Dropped: {len(drops)}")
    print(f"{'='*60}")

    for h in ["3", "5", "7"]:
        r = results.get("results", {}).get(h, {})
        if not r:
            continue
        print(f"\n  --- {h}d Horizon ---")
        print(f"    Acc={r['accuracy']:.4f}  P={r['precision']:.4f}  R={r['recall']:.4f}  F1={r['f1']:.4f}")
        print(f"    AUC-ROC={r.get('auc_roc', 'N/A'):.4f}  Spec={r.get('specificity', 0):.4f}  "
              f"BestTh={r.get('best_f1_threshold', 0.5)}")
        if r.get("pilot_buses"):
            print(f"    Pilot buses:")
            for bus, ba in r["pilot_buses"].items():
                if ba:
                    print(f"      {bus}: Acc={ba['accuracy']:.4f}  P={ba['precision']:.4f}  "
                          f"R={ba['recall']:.4f}  n={ba['n']}")
                    if ba['n_pos']:
                        print(f"             pos_rate={ba['n_pos']/ba['n']*100:.0f}%")


def compare_results():
    files = sorted(OUTPUTS_DIR.glob("*.json"))
    if not files:
        print("No results found in outputs/model_experiments/")
        return

    rows = []
    for f in files:
        with open(f) as fh:
            r = json.load(fh)
        for h in ["3", "5", "7"]:
            hr = r.get("results", {}).get(h, {})
            if not hr:
                continue
            # Short label: "XGBoost (full)" or "XGBoost-drop (fast)"
            base = r['model']
            mode = r.get('mode', '')
            drops = len(r.get('drop_features', []))
            spw = r.get('spw_multiplier', 1.0)
            lbl = f"{base}"
            if drops > 0:
                lbl += f"-drop{drops}"
            if isinstance(spw, dict):
                lbl += "-spwph"
            lbl += f" ({mode})"
            rows.append({
                "label": lbl,
                "horizon": f"{h}d",
                "acc": hr.get("accuracy", 0),
                "f1": hr.get("f1", 0),
                "prec": hr.get("precision", 0),
                "recall": hr.get("recall", 0),
                "auc_roc": hr.get("auc_roc", 0) or 0,
                "spec": hr.get("specificity", 0),
                "nfeat": hr.get("n_features", r.get('n_features_initial', 0)),
            })

    print(f"\n{'='*100}")
    print("  MODEL COMPARISON")
    print(f"{'='*100}")
    print(f"  {'Label':<35} {'H':>3}  {'Acc':>7}  {'F1':>7}  {'Prec':>7}  {'Recall':>7}  "
          f"{'AUC-ROC':>8}  {'Spec':>6}  {'Feat':>5}")
    print(f"  {'-'*35} {'-'*3}  {'-'*7}  {'-'*7}  {'-'*7}  {'-'*7}  {'-'*8}  {'-'*6}  {'-'*5}")
    for r in sorted(rows, key=lambda x: (-x["acc"], -x["f1"])):
        print(f"  {r['label']:<35} {r['horizon']:>3}  {r['acc']:.4f}  {r['f1']:.4f}  "
              f"{r['prec']:.4f}  {r['recall']:.4f}  {r['auc_roc']:.4f}  {r['spec']:.4f}  {r['nfeat']:>3}")


def run_single(model_name: str, label: str, n_est: int, spw_mult, drop: list[str] | None, spw_per_h: bool):
    model = build_model(model_name, n_est=n_est)
    print(f"\n{'#'*60}")
    print(f"# {model_name} ({label}) — {n_est} est, SPW={spw_mult}")
    if spw_per_h:
        print(f"# Per-horizon SPW: {SPW_BY_HORIZON}")
    if drop:
        print(f"# Dropping {len(drop)} features")
    print(f"{'#'*60}")

    spw_to_use = SPW_BY_HORIZON if spw_per_h else spw_mult
    results = train_eval_split(
        model, FEATURES_PATH,
        cutoff_date="2025-11-15",
        spw_multiplier=spw_to_use,
        label=label,
        drop_features=drop,
    )
    print_results(results)

    out_label = model_name
    if drop:
        out_label += f"_drop{len(drop)}"
    if spw_per_h:
        out_label += "_spwph"
    out_label += f"_{label}"
    out_path = OUTPUTS_DIR / f"{out_label}.json"
    with open(out_path, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\n  Saved: {out_path}")


def run_all():
    """Run all experiments systematically."""
    experiments = [
        # (model, label, n_est, spw_mult, drop_features, spw_per_horizon)
        # --- BASELINES ---
        ("xgboost", "baseline", 300, 3.0, None, False),
        ("catboost", "baseline", 300, 3.0, None, False),
        # --- FEATURE PRUNING ---
        ("xgboost", "drop14", 300, 3.0, DROP_FEATURES, False),
        ("catboost", "drop14", 300, 3.0, DROP_FEATURES, False),
        # --- PER-HORIZON SPW ---
        ("xgboost", "spwph", 300, None, None, True),
        ("catboost", "spwph", 300, None, None, True),
        # --- DROP + SPW PER HORIZON ---
        ("xgboost", "drop14_spwph", 300, None, DROP_FEATURES, True),
        ("catboost", "drop14_spwph", 300, None, DROP_FEATURES, True),
    ]
    for args in experiments:
        run_single(*args)


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Run model experiments with feature pruning")
    parser.add_argument("--fast", type=str, default=None, help="Fast test: model name")
    parser.add_argument("--drop-zero", action="store_true", help="Drop zero-importance features")
    parser.add_argument("--spw-per-horizon", action="store_true", help="Per-horizon SPW")
    parser.add_argument("--run-all", action="store_true", help="Run all experiment combos")
    parser.add_argument("--compare", action="store_true", help="Compare saved results")
    parser.add_argument("--list", action="store_true", help="List available models")
    parser.add_argument("--spw-mult", type=float, default=3.0, help="SPW multiplier (default: 3.0)")
    args = parser.parse_args()

    if args.list:
        print("Available models:")
        for m in ALL_MODELS:
            print(f"  {m}")
        print("\nExperiment flags:")
        print("  --drop-zero       Drop 14 zero-importance features")
        print("  --spw-per-horizon Per-horizon SPW: 7d=1.0, 5d=2.0, 3d=4.0")
        print("  --run-all         Run all combos systematically")
        return

    if args.compare:
        compare_results()
        return

    if args.run_all:
        run_all()
        return

    if args.fast:
        drop = DROP_FEATURES if args.drop_zero else None
        spw_per_h = args.spw_per_horizon
        label = "fast"
        if args.drop_zero:
            label = "drop14"
        if args.spw_per_horizon:
            label = "spwph"
        if args.drop_zero and args.spw_per_horizon:
            label = "drop14_spwph"
        run_single(args.fast.lower(), label, 300, args.spw_mult, drop, spw_per_h)
        return

    parser.print_help()


if __name__ == "__main__":
    main()
