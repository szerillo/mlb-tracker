#!/usr/bin/env python3
"""
Compute a unified pitcher quality value: a weighted average of FIP-style
metrics that lands on the same scale as the inputs (≈2.5–6.0, lower = better).
This is *not* a z-score — it's the actual blended FIP projection number.

Reads:  data/pitcher_stats.json
Writes: data/pitcher_stats.json (enriched with `unified_score`, `unified_tier`,
        `unified_components`, `unified_weight_covered` per pitcher)

Weights (Sean's 4/25 spec, after ~5 starts into the season):
    Expected   45%  →  xFIP 15%, SIERA 15%, xERA 15%
    Projection 45%  →  fip_proj 45% (already avg of ATC/BatX/OOPSY/ZiPS)
    Modeling   10%  →  bot_era 10%

Score = Σ(weight_i × value_i) / Σ(weight_i_available)
        ≈ a single FIP-equivalent number (lower = better).

If a pitcher has < half the total weight in available metrics the score is
omitted (too sparse). Renormalization by available weight makes the value
scale consistent regardless of which metrics are missing.

Tier thresholds (FIP scale — lower is better):
    val ≤ 3.25  → "Elite"
    val ≤ 3.75  → "Good"
    val ≤ 4.25  → "Avg"
    val ≤ 4.75  → "Bad"
    val >  4.75 → "Worst"

USAGE:
    python scripts/compute_pitcher_score.py
"""
from __future__ import annotations
import datetime
import json
import os
import sys

INPUT  = os.path.join(os.path.dirname(__file__), "..", "data", "pitcher_stats.json")
OUTPUT = INPUT  # in-place enrichment

# (field_name, weight_pct)
COMPONENTS = [
    ("xfip",      15.0),
    ("siera",     15.0),
    ("xera",      15.0),
    ("fip_proj",  45.0),
    ("bot_era",   10.0),
]
TOTAL_WEIGHT = sum(w for _, w in COMPONENTS)  # 100.0
MIN_WEIGHT_COVERED = 50.0  # need at least half the weight in available metrics

# Tier on the FIP scale (lower = better)
TIERS = [
    (3.25, "Elite"),
    (3.75, "Good"),
    (4.25, "Avg"),
    (4.75, "Bad"),
    (float("inf"), "Worst"),
]


def _f(v):
    """Coerce to float, returning None for missing/non-numeric/NaN."""
    if v is None: return None
    try:
        v = float(v)
        return v if v == v else None
    except (TypeError, ValueError):
        return None


def main() -> int:
    if not os.path.exists(INPUT):
        print(f"[score] no pitcher_stats at {INPUT}", file=sys.stderr)
        return 1

    with open(INPUT) as f:
        d = json.load(f)
    pitchers = d.get("pitchers", {})
    if not pitchers:
        print("[score] no pitchers found", file=sys.stderr)
        return 0
    print(f"[score] loaded {len(pitchers)} pitchers", file=sys.stderr)

    n_scored = 0
    n_sparse = 0
    tier_counts = {label: 0 for _, label in TIERS}
    for k, p in pitchers.items():
        if not isinstance(p, dict): continue
        weighted_sum = 0.0
        weight_avail = 0.0
        components = {}
        for field, weight in COMPONENTS:
            v = _f(p.get(field))
            if v is None: continue
            components[field] = round(v, 3)
            weighted_sum += weight * v
            weight_avail += weight

        if weight_avail < MIN_WEIGHT_COVERED:
            for key in ("unified_score", "unified_tier",
                        "unified_components", "unified_weight_covered"):
                p.pop(key, None)
            n_sparse += 1
            continue

        score = weighted_sum / weight_avail  # weighted FIP projection

        # Tier (lower = better)
        tier = TIERS[-1][1]
        for thr, label in TIERS:
            if score <= thr:
                tier = label; break

        p["unified_score"]          = round(score, 2)
        p["unified_tier"]           = tier
        p["unified_components"]     = components
        p["unified_weight_covered"] = round(weight_avail, 1)
        n_scored += 1
        tier_counts[tier] = tier_counts.get(tier, 0) + 1

    d.setdefault("scoring", {})
    d["scoring"]["unified_score"] = {
        "computed_at": datetime.datetime.utcnow().isoformat() + "Z",
        "scale": "FIP (weighted average — lower = better)",
        "weights": {f: w for f, w in COMPONENTS},
        "tier_thresholds": [{"max_val": t, "label": l}
                            for t, l in TIERS if t < float("inf")],
        "n_scored": n_scored,
        "n_too_sparse": n_sparse,
        "tier_counts": tier_counts,
    }

    with open(OUTPUT, "w") as f:
        json.dump(d, f, indent=2)
    print(f"[score] scored {n_scored} pitchers ({n_sparse} too sparse)",
          file=sys.stderr)
    print("[score] tiers: " + ", ".join(f"{l}={n}" for l, n in tier_counts.items()),
          file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
