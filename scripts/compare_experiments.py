#!/usr/bin/env python3
"""Compare all experiments: table of test set + shadow pilot bus metrics."""

import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
OUTPUTS_DIR = PROJECT_ROOT / "outputs" / "experiments"


def load_results(name: str) -> dict | None:
    path = OUTPUTS_DIR / name / "results.json"
    if path.exists():
        with open(path) as f:
            return json.load(f)
    return None


def main():
    exp_dirs = sorted(
        [d.name for d in OUTPUTS_DIR.iterdir() if d.is_dir() and d.name != "__pycache__"]
    )
    if not exp_dirs:
        print("No experiments found in outputs/experiments/")
        sys.exit(1)

    print(f"{'='*120}")
    print(f"{'Experiment':<25} {'Test 7d Acc':<12} {'Test 5d Acc':<12} {'Test 3d Acc':<12} "
          f"{'Shad 7d Acc':<12} {'Shad 5d Acc':<12} {'Shad 3d Acc':<12} "
          f"{'Pilot 7d avg':<12} {'Pilot 5d avg':<12} {'Pilot 3d avg':<12}")
    print(f"{'-'*120}")

    for name in exp_dirs:
        r = load_results(name)
        if not r:
            continue

        ts = r.get("test_set", {})
        sg = r.get("shadow_global", {})
        sp = r.get("shadow_pilot", {})
        cfg = r.get("config", {})
        desc = cfg.get("description", "")[:40]

        def g(d, h, k):
            m = d.get(h, {})
            return m.get(k, "—")

        def p(d, h):
            accs = [m.get("accuracy", 0) for m in d.get(h, {}).values() if m]
            return f"{sum(accs)/len(accs):.4f}" if accs else "—"

        row = (
            f"{name:<25} "
            f"{g(ts,'7','accuracy'):<12} {g(ts,'5','accuracy'):<12} {g(ts,'3','accuracy'):<12} "
            f"{g(sg,'7','accuracy'):<12} {g(sg,'5','accuracy'):<12} {g(sg,'3','accuracy'):<12} "
            f"{p(sp,'7'):<12} {p(sp,'5'):<12} {p(sp,'3'):<12}"
        )
        print(row)
        if desc:
            print(f"{'':>25}  // {desc}")

    print(f"{'='*120}")

    # Detail for each pilot bus across experiments
    print(f"\n\n{'='*120}")
    print("Per-pilot-bus 7d accuracy across experiments:")
    print(f"{'='*120}")
    print(f"{'Experiment':<25} {'FLXS22 Acc':<12} {'FLXS23 Acc':<12} {'LWTK42 Acc':<12}")
    print(f"{'-'*61}")
    for name in exp_dirs:
        r = load_results(name)
        if not r:
            continue
        sp = r.get("shadow_pilot", {})
        h7 = sp.get("7", {})
        f22 = h7.get("FLXS22", {}).get("accuracy", "—")
        f23 = h7.get("FLXS23", {}).get("accuracy", "—")
        l42 = h7.get("LWTK42", {}).get("accuracy", "—")
        print(f"{name:<25} {str(f22):<12} {str(f23):<12} {str(l42):<12}")

    print(f"\n\n{'='*120}")
    print("Per-pilot-bus 5d accuracy across experiments:")
    print(f"{'='*120}")
    print(f"{'Experiment':<25} {'FLXS22 Acc':<12} {'FLXS23 Acc':<12} {'LWTK42 Acc':<12}")
    print(f"{'-'*61}")
    for name in exp_dirs:
        r = load_results(name)
        if not r:
            continue
        sp = r.get("shadow_pilot", {})
        h5 = sp.get("5", {})
        f22 = h5.get("FLXS22", {}).get("accuracy", "—")
        f23 = h5.get("FLXS23", {}).get("accuracy", "—")
        l42 = h5.get("LWTK42", {}).get("accuracy", "—")
        print(f"{name:<25} {str(f22):<12} {str(f23):<12} {str(l42):<12}")

    print(f"\n\n{'='*120}")
    print("Per-pilot-bus 3d accuracy across experiments:")
    print(f"{'='*120}")
    print(f"{'Experiment':<25} {'FLXS22 Acc':<12} {'FLXS23 Acc':<12} {'LWTK42 Acc':<12}")
    print(f"{'-'*61}")
    for name in exp_dirs:
        r = load_results(name)
        if not r:
            continue
        sp = r.get("shadow_pilot", {})
        h3 = sp.get("3", {})
        f22 = h3.get("FLXS22", {}).get("accuracy", "—")
        f23 = h3.get("FLXS23", {}).get("accuracy", "—")
        l42 = h3.get("LWTK42", {}).get("accuracy", "—")
        print(f"{name:<25} {str(f22):<12} {str(f23):<12} {str(l42):<12}")


if __name__ == "__main__":
    main()
