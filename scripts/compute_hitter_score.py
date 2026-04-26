#!/usr/bin/env python3
"""
Compute a unified hitter quality score by blending projection, expected,
and bat-tracking metrics into a single weighted z-score.

Mirrors compute_pitcher_score.py — same structure, different fields.

Reads:
    data/hitters.json            (Steamer projection + season actuals)
    data/hitter_percentiles.json (Savant percentile ranks + raw rates)
Writes:
    data/hitters.json (enriched with `unified_score`, `unified_tier`,
                      `unified_components`, `unified_weight_covered`)

Weights (mirrors the pitcher 45/45/10 split):
    Projection 45%  →  woba (Steamer projected wOBA, which already blends OPS
                       components)                                       45%
    Expected   45%  →  xwoba_actual (season xwOBA)                       25%
                       barrel_pct   (Savant raw barrel %)                10%
                       hard_hit_pct (Savant raw hard-hit %)              10%
    Bat-track  10%  →  bat_speed    (Savant percentile rank, 0–100)      10%

All five inputs are HIGHER-IS-BETTER (unlike pitcher inputs which are all
lower-is-better).

Pool: only hitters with ≥ 30 PA actual (`pa_actual`) are in the z-score
reference pool. The score is computed for ALL hitters, but only against the
qualified pool's distribution — so a 5-PA call-up with weak xwOBA still gets
a "Worst" tier without dragging the reference points.

Tier thresholds:
    z ≥  1.00  → "Elite"
    z ≥  0.50  → "Good"
    z ≥ -0.50  → "Avg"
    z ≥ -1.00  → "Bad"
    z <  -1.00 → "Worst"

USAGE:
    python scripts/compute_hitter_score.py
"""
from __future__ import annotations
import datetime
import json
import os
import statistics
import sys
import unicodedata

HERE = os.path.dirname(__file__)
HITTERS_PATH     = os.path.join(HERE, "..", "data", "hitters.json")
PERCENTILES_PATH = os.path.join(HERE, "..", "data", "hitter_percentiles.json")

# (field_name, weight_pct, lower_is_better, source_file)
# source_file: "h" = hitters.json (Steamer + actuals), "p" = hitter_percentiles.json (Savant)
COMPONENTS = [
    ("woba",          45.0, False, "h"),
    ("xwoba_actual",  25.0, False, "h"),
    ("barrel_pct",    10.0, False, "p"),
    ("hard_hit_pct",  10.0, False, "p"),
    ("bat_speed",     10.0, False, "p"),
]
TOTAL_WEIGHT = sum(w for _, w, _, _ in COMPONENTS)  # 100.0
MIN_WEIGHT_COVERED = 50.0

QUALIFIED_PA = 30.0  # min PA-actual to be in the reference pool

TIERS = [
    ( 1.00, "Elite"),
    ( 0.50, "Good"),
    (-0.50, "Avg"),
    (-1.00, "Bad"),
    (float("-inf"), "Worst"),
]


def _f(v):
    if v is None: return None
    try:
        v = float(v)
        return v if v == v else None
    except (TypeError, ValueError):
        return None


def norm_name(s: str) -> str:
    if not s: return ""
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = s.lower()
    for suf in [" jr.", " jr", " sr.", " sr", " iii", " ii"]:
        if s.endswith(suf):
            s = s[: -len(suf)]
    return s.replace(".", "").strip()


def main() -> int:
    if not os.path.exists(HITTERS_PATH):
        print(f"[hscore] no hitters.json at {HITTERS_PATH}", file=sys.stderr)
        return 1

    with open(HITTERS_PATH) as f:
        hd = json.load(f)
    hitters = hd.get("hitters", {})
    if not hitters:
        print("[hscore] no hitters found in hitters.json", file=sys.stderr)
        return 0
    print(f"[hscore] loaded {len(hitters)} hitters", file=sys.stderr)

    # Load percentiles (optional — degrades gracefully if missing)
    pcts = {}
    if os.path.exists(PERCENTILES_PATH):
        try:
            with open(PERCENTILES_PATH) as f:
                pd = json.load(f)
            pcts = pd.get("hitters", {})
            print(f"[hscore] loaded {len(pcts)} hitter percentile rows", file=sys.stderr)
        except Exception as e:
            print(f"[hscore] could not read percentiles: {e}", file=sys.stderr)
    else:
        print("[hscore] no hitter_percentiles.json — Savant fields will be missing",
              file=sys.stderr)

    # Build a name-normalized index into pcts. Keys in hitter_percentiles are
    # already norm_name'd, but `hitters.json` keys vary — match on h["name"].
    pcts_by_norm = {}
    for k, v in pcts.items():
        if isinstance(v, dict):
            pcts_by_norm[k] = v
            nm = v.get("name")
            if nm:
                pcts_by_norm.setdefault(norm_name(nm), v)

    def get_field(h_entry, field, source):
        if source == "h":
            return _f(h_entry.get(field))
        # percentiles: try direct map by hitter key, fall back to name match
        nm = h_entry.get("name") or ""
        pc = pcts_by_norm.get(norm_name(nm))
        if pc is None: return None
        return _f(pc.get(field))

    # --- Build qualified reference pool stats per metric -------------------
    pool_stats = {}
    for field, _, lower_better, source in COMPONENTS:
        vals = []
        for h in hitters.values():
            if not isinstance(h, dict): continue
            pa_actual = _f(h.get("pa_actual")) or 0
            if pa_actual < QUALIFIED_PA: continue
            v = get_field(h, field, source)
            if v is None: continue
            vals.append(v)
        if len(vals) < 10:
            print(f"[hscore] {field}: only {len(vals)} qualified — skipping (insufficient pool)",
                  file=sys.stderr)
            continue
        m = statistics.mean(vals)
        s = statistics.stdev(vals)
        pool_stats[field] = (m, s, lower_better, source)
        print(f"[hscore]   {field:14}  pool n={len(vals):4d}  mean={m:7.3f}  sd={s:6.3f}",
              file=sys.stderr)

    if not pool_stats:
        print("[hscore] no metrics had a usable pool — aborting", file=sys.stderr)
        return 0

    # --- Score every hitter (using pool reference) -------------------------
    n_scored = 0
    n_sparse = 0
    tier_counts = {label: 0 for _, label in TIERS}
    for k, h in hitters.items():
        if not isinstance(h, dict): continue
        weighted_sum = 0.0
        weight_avail = 0.0
        components = {}
        for field, weight, _, source in COMPONENTS:
            if field not in pool_stats: continue
            m, s, lower_better, _src = pool_stats[field]
            v = get_field(h, field, source)
            if v is None: continue
            z = (v - m) / s if s > 0 else 0.0
            if lower_better: z = -z
            components[field] = round(z, 3)
            weighted_sum += weight * z
            weight_avail += weight

        if weight_avail < MIN_WEIGHT_COVERED:
            for key in ("unified_score", "unified_tier",
                        "unified_components", "unified_weight_covered"):
                h.pop(key, None)
            n_sparse += 1
            continue

        score = weighted_sum / weight_avail
        tier = TIERS[-1][1]
        for thr, label in TIERS:
            if score >= thr:
                tier = label; break

        h["unified_score"] = round(score, 3)
        h["unified_tier"]  = tier
        h["unified_components"]     = components
        h["unified_weight_covered"] = round(weight_avail, 1)
        n_scored += 1
        tier_counts[tier] = tier_counts.get(tier, 0) + 1

    # --- Bookkeeping -------------------------------------------------------
    hd.setdefault("scoring", {})
    hd["scoring"]["unified_score"] = {
        "computed_at": datetime.datetime.utcnow().isoformat() + "Z",
        "weights": {f: w for f, w, _, _ in COMPONENTS},
        "qualified_pa_threshold": QUALIFIED_PA,
        "tier_thresholds": [{"min_z": t, "label": l}
                            for t, l in TIERS if t > float("-inf")],
        "pool_stats": {f: {"mean": round(m, 3), "sd": round(s, 3)}
                       for f, (m, s, _lb, _src) in pool_stats.items()},
        "n_scored": n_scored,
        "n_too_sparse": n_sparse,
        "tier_counts": tier_counts,
    }

    with open(HITTERS_PATH, "w") as f:
        json.dump(hd, f, indent=2)
    print(f"[hscore] scored {n_scored} hitters ({n_sparse} too sparse to score)",
          file=sys.stderr)
    print("[hscore] tiers: " + ", ".join(f"{l}={n}" for l, n in tier_counts.items()),
          file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
