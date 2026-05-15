#!/usr/bin/env python3
"""Deploy an experiment's models and predictions to the live directories."""
import argparse
import shutil
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
EXPERIMENTS_DIR = PROJECT_ROOT / "outputs" / "experiments"


def deploy(experiment_name: str):
    exp_dir = EXPERIMENTS_DIR / experiment_name
    if not exp_dir.exists():
        print(f"ERROR: Experiment '{experiment_name}' not found in {EXPERIMENTS_DIR}/")
        sys.exit(1)

    print(f"Deploying experiment: {experiment_name}")
    print(f"  Source: {exp_dir}")

    # Deploy models
    src_models = exp_dir / "models"
    dst_models = PROJECT_ROOT / "models"
    if src_models.exists():
        for f in src_models.iterdir():
            dst = dst_models / f.name
            shutil.copy2(f, dst)
            print(f"  Model: {f.name} -> {dst}")
    else:
        print("  WARNING: No models found")

    # Deploy predictions
    src_preds = exp_dir / "predictions"
    dst_preds = PROJECT_ROOT / "data" / "predictions"
    if src_preds.exists():
        for f in src_preds.iterdir():
            dst = dst_preds / f.name
            shutil.copy2(f, dst)
            print(f"  Predictions: {f.name} -> {dst}")
    else:
        print("  WARNING: No predictions found")

    # Deploy shadow report
    src_shadow = exp_dir / "shadow_report.json"
    dst_shadow = PROJECT_ROOT / "outputs" / "piloto1_report.json"
    if src_shadow.exists():
        shutil.copy2(src_shadow, dst_shadow)
        print(f"  Shadow report -> {dst_shadow}")

    # Deploy metrics
    src_metrics = exp_dir / "metrics"
    dst_metrics = PROJECT_ROOT / "outputs" / "metrics"
    if src_metrics.exists():
        dst_metrics.mkdir(parents=True, exist_ok=True)
        for f in src_metrics.iterdir():
            dst = dst_metrics / f.name
            shutil.copy2(f, dst)
            print(f"  Metrics: {f.name} -> {dst}")

    print(f"\nDeployment complete. Dashboard and scripts will now use '{experiment_name}' models.")


def main():
    parser = argparse.ArgumentParser(description="Deploy experiment to live directories")
    parser.add_argument("name", type=str, help="Experiment name to deploy")
    parser.add_argument("--list", action="store_true", help="List completed experiments")
    args = parser.parse_args()

    if args.list:
        dirs = sorted([d.name for d in EXPERIMENTS_DIR.iterdir() if d.is_dir()])
        print("Completed experiments:")
        for d in dirs:
            results_path = EXPERIMENTS_DIR / d / "results.json"
            if results_path.exists():
                with open(results_path) as f:
                    import json
                    r = json.load(f)
                s7 = r.get("shadow_pilot", {}).get("7", {})
                accs = [f"{bus}:{m.get('accuracy',0):.2f}" for bus, m in s7.items()]
                print(f"  {d:30s} 7d: {', '.join(accs)}")
        return

    deploy(args.name)


if __name__ == "__main__":
    main()
