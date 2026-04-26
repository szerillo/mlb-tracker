#!/usr/bin/env python3
"""
Compute a unified pitcher quality score by blending expected, projection,
and modeling metrics into a single weighted z-score.

Reads:  data/pitcher_stats.json
Writes: data/pitcher_stats.json (enriched with `unified_score`, `unified_tier`,
        `unified_components` per pitcher)

Weights (Sean's 4/25 spec, after ~5 starts into the season):
    Expected   45%  →  xFIP 15%, SIERA 15%, xERA 15%
    Projection 45%  →  fip_proj 45% (already avg of ATC/BatX/OOPSY/ZiPS)
    Modeling   10%  →  bot_era 10%

Scoring:
    For each metric, z = (val − pool_mean) / pool_sd, then flipped so higher z
    is better (all five inputs are lower-is-better). Score is a weighted average
    of available z-scores; if a pitcher has < half the weight covered,
    score = None (too sparse). Renormalization by available weight makes the
    score scale consistent regardless of which metrics are missing.

Tier thresholds:
    z ≥  1.00  → "Elite"
    z ≥  0.50  → "Good"
    z ≥ -0.50  → "Avg"
    z ≥ -1.00  → "Bad"
    z <  -1.00 → "Worst"

Pool: only pitchers with ≥ 5 IP (a stand-in for "qualified enough to be in the
sample"). This keeps the z-scores stable from the noise of 1-IP rookies. The
score is computed for ALL pitchers but only against the qualified pool's
distribution — so a 2-IP rookie with a brutal SIERA still gets a "Worst" tier
without dragging down everyone else's reference points.

USAGE:
    python scripts/compute_pitcher_score.py
"""
from __future__ import annotations
import json
import os
import statistics
import sys
import datetime

INPUT  = os.path.join(os.path.dirname(__file__), "..", "data", "pitcher_stats.json")
OUTPUT = INPUT  # in-place enrichment

# --- Component definition ------------------------------------------------
# (field_name, weight_pct, lower_is_better)
COMPONENTS = [
    ("xfip",      15.0, True),
    ("siera",     15.0, True),
    ("xera",      15.0, True),
    ("fip_proj",  45.0, True),
    ("bot_era",   10.0, True),
]
TOTAL_WEIGHT = sum(w for _, w, _ in COMPONENTS)  # 100.0
MIN_WEIGHT_COVERED = 50.0  # need at least half the weight in available metrics

QUALIFIED_IP = 5.0  # min IP to be in the z-score reference pool

TIERS = [
    ( 1.00, "Elite"),
    ( 0.50, "Good"),
    (-0.50, "Avg"),
    (-1.00, "Bad"),
    (float("-inf"), "Worst"),
]


def _f(v):
    """Coerce to float, returning None for missing/non-numeric."""
    if v is None: return None
    try:
        v = float(v)
        return v if v == v else None  # filter NaN
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

    # --- Build qualified reference pool stats per metric -------------------
    pool_stats = {}
    for field, _, lower_better in COMPONENTS:
        vals = []
        for p in pitchers.values():
            if not isinstance(p, dict): continue
            ip = _f(p.get("ip")) or 0
            v  = _f(p.get(field))
            if v is None or ip < QUALIFIED_IP: continue
            vals.append(v)
        if len(vals) < 10:
            print(f"[score] {field}: only {len(vals)} qualified — skipping (insufficient pool)",
                  file=sys.stderr)
            continue
        m = statistics.mean(vals)
        s = statistics.stdev(vals)
        pool_stats[field] = (m, s, lower_better)
        print(f"[score]   {field:10}  pool n={len(vals):4d}  mean={m:6.3f}  sd={s:5.3f}",
              file=sys.stderr)

    if not pool_stats:
        print("[score] no metrics had a usable pool — aborting", file=sys.stderr)
        return 0

    # --- Score every pitcher (using pool reference) ------------------------
    n_scored = 0
    n_sparse = 0
    tier_counts = {label: 0 for _, label in TIERS}
    for k, p in pitchers.items():
        if not isinstance(p, dict): continue
        weighted_sum = 0.0
        weight_avail = 0.0
        components = {}
        for field, weight, _ in COMPONENTS:
            if field not in pool_stats: continue
            m, s, lower_better = pool_stats[field]
            v = _f(p.get(field))
            if v is None: continue
            z = (v - m) / s if s > 0 else 0.0
            if lower_better: z = -z   # flip so higher z = better
            components[field] = round(z, 3)
            weighted_sum += weight * z
            weight_avail += weight

        if weight_avail < MIN_WEIGHT_COVERED:
            # Too sparse — clear any prior values
            p.pop("unified_score", None)
            p.pop("unified_tier", None)
            p.pop("unified_components", None)
            p.pop("unified_weight_covered", None)
            n_sparse += 1
            continue

        score = weighted_sum / weight_avail  # weighted avg z-score
        # Tier
        tier = TIERS[-1][1]
        for thr, label in TIERS:
            if score >= thr:
                tier = label; break

        p["unified_score"] = round(score, 3)
        p["unified_tier"]  = tier
        p["unified_components"]      = components
        p["unified_weight_covered"]  = round(weight_avail, 1)
        n_scored += 1
        tier_counts[tier] = tier_counts.get(tier, 0) + 1

    # --- Bookkeeping --------------------------------------------------------
    d.setdefault("scoring", {})
    d["scoring"]["unified_score"] = {
        "computed_at": datetime.datetime.utcnow().isoformat() + "Z",
        "weights": {f: w for f, w, _ in COMPONENTS},
        "qualified_ip_threshold": QUALIFIED_IP,
        "tier_thresholds": [{"min_z": t, "label": l} for t, l in TIERS if t > float("-inf")],
        "pool_stats": {f: {"mean": round(m, 3), "sd": round(s, 3)}
                       for f, (m, s, _) in pool_stats.items()},
        "n_scored": n_scored,
        "n_too_sparse": n_sparse,
        "tier_counts": tier_counts,
    }

    with open(OUTPUT, "w") as f:
        json.dump(d, f, indent=2)
    print(f"[score] scored {n_scored} pitchers ({n_sparse} too sparse to score)",
          file=sys.stderr)
    print(f"[score] tiers: " + ", ".join(f"{l}={n}" for l, n in tier_counts.items()),
          file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
