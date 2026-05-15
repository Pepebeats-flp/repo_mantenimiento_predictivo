#!/usr/bin/env python3
"""Run one experiment by name: train → evaluate shadow → compare."""

import argparse
import json
import sys
import subprocess
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
EXPERIMENTS_DIR = PROJECT_ROOT / "config" / "experiments"
OUTPUTS_DIR = PROJECT_ROOT / "outputs" / "experiments"


def list_experiments():
    files = sorted(EXPERIMENTS_DIR.glob("*.json"))
    if not files:
        print("No experiments found in config/experiments/")
        return
    print("Available experiments:")
    for f in files:
        name = f.stem
        desc = ""
        try:
            with open(f) as fh:
                cfg = json.load(fh)
                desc = cfg.get("description", "")
        except Exception:
            pass
        print(f"  {name:30s}  {desc}")


def run_experiment(name: str, local_json: bool):
    cfg_path = EXPERIMENTS_DIR / f"{name}.json"
    if not cfg_path.exists():
        print(f"ERROR: Experiment '{name}' not found in {EXPERIMENTS_DIR}/")
        list_experiments()
        sys.exit(1)

    out_dir = OUTPUTS_DIR / name
    out_dir.mkdir(parents=True, exist_ok=True)

    # ── Step 1: Run pipeline ─────────────────────────────────────────
    print(f"{'='*60}")
    print(f"EXPERIMENT: {name}")
    with open(cfg_path) as f:
        cfg = json.load(f)
    print(f"  {cfg.get('description', '')}")
    print(f"{'='*60}")

    cmd = [
        sys.executable,
        str(PROJECT_ROOT / "scripts" / "run_pipeline.py"),
        "--experiment", name,
    ]
    if local_json:
        cmd.append("--local-json")

    print(f"\n>>> {' '.join(cmd)}\n")
    r = subprocess.run(cmd, cwd=PROJECT_ROOT)
    if r.returncode != 0:
        print(f"\nERROR: Pipeline failed (exit {r.returncode})")
        sys.exit(1)

    # ── Step 2: Shadow evaluation ────────────────────────────────────
    pred_path = out_dir / "predictions" / "predictions_voy_redbus.parquet"
    shadow_out = out_dir / "shadow_report.json"
    shadow_cmd = [
        sys.executable,
        str(PROJECT_ROOT / "scripts" / "evaluate_shadow.py"),
        str(pred_path),
        "--save", str(shadow_out),
        "--skip-ranking",
    ]
    print(f"\n>>> {' '.join(shadow_cmd)}\n")
    r = subprocess.run(shadow_cmd, cwd=PROJECT_ROOT)
    if r.returncode != 0:
        print(f"\nWARNING: Shadow evaluation failed (exit {r.returncode})")

    # ── Step 3: Compact results ──────────────────────────────────────
    results = collect_results(name, cfg, out_dir)
    summary_path = out_dir / "results.json"
    with open(summary_path, "w") as f:
        json.dump(results, f, indent=2)

    print_summary(results)
    print(f"\nFull results: {summary_path}")


def collect_results(name: str, cfg: dict, out_dir: Path) -> dict:
    r = {"experiment": name, "config": cfg}

    # Test set metrics
    m_path = out_dir / "metrics" / "evaluation_summary_voy_redbus.json"
    if m_path.exists():
        with open(m_path) as f:
            r["test_set"] = json.load(f)

    # Shadow global metrics
    s_path = out_dir / "shadow_report.json"
    if s_path.exists():
        with open(s_path) as f:
            sr = json.load(f)

        r["shadow_global"] = {}
        for h, data in sr.get("por_horizonte", {}).items():
            tm = data.get("threshold_metrics", {}).get("0.5", {})
            cm = tm.get("confusion_matrix", [])
            r["shadow_global"][h] = {
                "accuracy": tm.get("accuracy", 0),
                "precision": tm.get("classification_report", {}).get("1", {}).get("precision", 0),
                "recall": tm.get("classification_report", {}).get("1", {}).get("recall", 0),
                "f1": tm.get("classification_report", {}).get("1", {}).get("f1-score", 0),
                "confusion_matrix": cm,
            }

        # Pilot bus metrics
        pilot_buses = sr.get("config", {}).get("buses_piloto", ["FLXS22", "FLXS23", "LWTK42"])
        r["pilot_buses"] = pilot_buses
        r["shadow_pilot"] = {}
        for h, hdata in sr.get("por_bus", {}).items():
            for bus in pilot_buses:
                bm = hdata.get(bus, {})
                if "error" in bm:
                    continue
                tm = bm.get("threshold_metrics", {}).get("0.5", {})
                r.setdefault("shadow_pilot", {}).setdefault(h, {})[bus] = {
                    "accuracy": tm.get("accuracy", 0),
                    "precision": tm.get("classification_report", {}).get("1", {}).get("precision", 0),
                    "recall": tm.get("classification_report", {}).get("1", {}).get("recall", 0),
                }

    # Feature count
    meta = out_dir / "models" / "xgb_7d_voy_redbus_meta.json"
    if meta.exists():
        with open(meta) as f:
            r["model_meta"] = json.load(f)

    return r


def print_summary(r: dict):
    print(f"\n{'='*60}")
    print(f"RESULTS — {r['experiment']}")
    print(f"{'='*60}")

    ts = r.get("test_set", {})
    sg = r.get("shadow_global", {})
    sp = r.get("shadow_pilot", {})

    print(f"\n  Test Set (held-out):")
    for h in ["7", "5", "3"]:
        m = ts.get(h, {})
        if m:
            print(f"    {h}d: Acc={m.get('accuracy',0):.4f}  P={m.get('precision',0):.4f}  "
                  f"R={m.get('recall',0):.4f}  F1={m.get('f1',0):.4f}")

    print(f"\n  Shadow Global:")
    for h in ["7", "5", "3"]:
        m = sg.get(h, {})
        if m:
            print(f"    {h}d: Acc={m.get('accuracy',0):.4f}  P={m.get('precision',0):.4f}  "
                  f"R={m.get('recall',0):.4f}  F1={m.get('f1',0):.4f}")

    print(f"\n  Shadow Pilot Buses (7d):")
    if "7" in sp:
        for bus, m in sp["7"].items():
            cumple = "✅" if m.get("accuracy", 0) >= 0.70 else "❌"
            print(f"    {bus}: Acc={m.get('accuracy',0):.4f}  "
                  f"P={m.get('precision',0):.4f}  R={m.get('recall',0):.4f}  {cumple}")

    print(f"\n  Shadow Pilot Buses (5d):")
    if "5" in sp:
        for bus, m in sp["5"].items():
            cumple = "✅" if m.get("accuracy", 0) >= 0.70 else "❌"
            print(f"    {bus}: Acc={m.get('accuracy',0):.4f}  "
                  f"P={m.get('precision',0):.4f}  R={m.get('recall',0):.4f}  {cumple}")

    print(f"\n  Shadow Pilot Buses (3d):")
    if "3" in sp:
        for bus, m in sp["3"].items():
            cumple = "✅" if m.get("accuracy", 0) >= 0.70 else "❌"
            print(f"    {bus}: Acc={m.get('accuracy',0):.4f}  "
                  f"P={m.get('precision',0):.4f}  R={m.get('recall',0):.4f}  {cumple}")


def main():
    parser = argparse.ArgumentParser(description="Run a pipeline experiment end-to-end")
    parser.add_argument("name", nargs="?", type=str, default=None,
                        help="Experiment name (config/experiments/<name>.json)")
    parser.add_argument("--local-json", action="store_true",
                        help="Usar archivos JSON locales en vez de Firestore")
    parser.add_argument("--list", action="store_true", help="List available experiments")
    args = parser.parse_args()

    if args.list or not args.name:
        list_experiments()
        return

    run_experiment(args.name, args.local_json)


if __name__ == "__main__":
    main()
