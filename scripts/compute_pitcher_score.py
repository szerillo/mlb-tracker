#!/usr/bin/env python3
"""
Compute a unified pitcher quality value: a weighted average of FIP-style
metrics that lands on the same scale as the inputs (≈2.5–6.0, lower = better).
This is *not* a z-score — it's the actual blended "weighted FIP" number.

Reads:  data/pitcher_stats.json       (xera, bot_era, fip_proj, season xfip/siera)
        data/pitcher_gamelogs.json    (per-start l5 + season xfip/siera → rolling)
Writes: data/pitcher_stats.json (enriched with `unified_score`, `unified_tier`,
        `unified_components`, `unified_weight_covered` per pitcher)

Weights (Sean's 5/25 recalibration — rolling-led, projection downweighted):
    rolling xFIP   28%   ← stabilized: 0.6·L5 + 0.4·season-to-date
    rolling SIERA  22%   ← stabilized: 0.6·L5 + 0.4·season-to-date
    xERA           18%   ← independent contact-quality estimator
    botERA         17%   ← independent stuff-model estimator
    projection     15%   ← preseason true-talent (least, now in-season)

Why this blend (from the split-half backtest, 120–144 SP sample):
  • K-BB% is the best single forward indicator — and xFIP/SIERA are ~0.82
    correlated with it, i.e. they ARE a recent-K-BB% model. So a rolling
    xFIP/SIERA-led core leans on the strongest signal.
  • A short pure-L5 window is noisier than season-to-date; a 60/40 L5/season
    blend matched full-season forecast accuracy while staying responsive to
    form/velo/injury changes — hence "stabilized rolling".
  • xERA (r≈0.50 vs xFIP) and botERA add genuinely independent contact/stuff
    information that K-BB% misses, so they earn real weight.
  • Projections add the least *new* in-season info, so they're smallest.

Score = Σ(weight_i × value_i) / Σ(weight_i_available)
        ≈ a single FIP-equivalent number (lower = better).

If a pitcher has < half the total weight in available metrics the score is
omitted (too sparse). Renormalization by available weight keeps the value
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
import unicodedata

HERE     = os.path.dirname(__file__)
INPUT    = os.path.join(HERE, "..", "data", "pitcher_stats.json")
GAMELOGS = os.path.join(HERE, "..", "data", "pitcher_gamelogs.json")
OUTPUT   = INPUT  # in-place enrichment

# Stabilized-rolling blend for xFIP/SIERA: 0.6·L5 + 0.4·season-to-date.
ROLL_L5_WEIGHT     = 0.6
ROLL_SEASON_WEIGHT = 0.4

# (component_key, weight_pct). roll_* are computed; the rest read from stats.
COMPONENTS = [
    ("roll_xfip",  28.0),
    ("roll_siera", 22.0),
    ("xera",       18.0),
    ("bot_era",    17.0),
    ("fip_proj",   15.0),
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
    if v is None:
        return None
    try:
        v = float(v)
        return v if v == v else None
    except (TypeError, ValueError):
        return None


def _norm(s: str) -> str:
    """Mirror the frontend normName: strip accents/suffixes/periods, lowercase."""
    s = unicodedata.normalize("NFKD", s or "")
    s = "".join(c for c in s if not unicodedata.combining(c)).lower()
    for suf in (" jr", " sr", " iii", " ii"):
        if s.endswith(suf) or s.endswith(suf + "."):
            s = s[: s.rfind(suf)]
            break
    return s.replace(".", "").strip()


def load_gamelogs():
    """Return {normName: gamelog_entry} or {} if the file is missing/unreadable."""
    if not os.path.exists(GAMELOGS):
        print("[score] no gamelogs file — rolling falls back to season stats",
              file=sys.stderr)
        return {}
    try:
        with open(GAMELOGS) as f:
            gl = json.load(f)
    except Exception as e:
        print(f"[score] gamelogs unreadable ({e}) — rolling falls back to season",
              file=sys.stderr)
        return {}
    out = {}
    for v in (gl.get("pitchers") or {}).values():
        if isinstance(v, dict) and v.get("name"):
            out[_norm(v["name"])] = v
    return out


def stabilized_roll(stat_key, p, g):
    """Stabilized rolling value = 0.6·L5 + 0.4·season-to-date (from gamelogs).
    Falls back to L5-only, then gamelogs-season, then the pitcher_stats season
    value so relievers / sparse arms still get a sane component."""
    if g:
        l5 = _f((g.get("l5") or {}).get(stat_key))
        se = _f((g.get("season") or {}).get(stat_key))
        if l5 is not None and se is not None:
            return ROLL_L5_WEIGHT * l5 + ROLL_SEASON_WEIGHT * se
        if l5 is not None:
            return l5
        if se is not None:
            return se
    return _f(p.get(stat_key))  # pitcher_stats season xfip/siera


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

    glby = load_gamelogs()
    print(f"[score] loaded {len(pitchers)} pitchers, {len(glby)} gamelog arms",
          file=sys.stderr)

    n_scored = n_sparse = n_rolling = 0
    tier_counts = {label: 0 for _, label in TIERS}
    for k, p in pitchers.items():
        if not isinstance(p, dict):
            continue
        g = glby.get(k) or glby.get(_norm(p.get("name", "")))
        vals = {
            "roll_xfip":  stabilized_roll("xfip", p, g),
            "roll_siera": stabilized_roll("siera", p, g),
            "xera":       _f(p.get("xera")),
            "bot_era":    _f(p.get("bot_era")),
            "fip_proj":   _f(p.get("fip_proj")),
        }

        weighted_sum = weight_avail = 0.0
        components = {}
        for field, weight in COMPONENTS:
            v = vals[field]
            if v is None:
                continue
            components[field] = round(v, 3)
            weighted_sum += weight * v
            weight_avail += weight

        if weight_avail < MIN_WEIGHT_COVERED:
            for key in ("unified_score", "unified_tier",
                        "unified_components", "unified_weight_covered",
                        "unified_rolling"):
                p.pop(key, None)
            n_sparse += 1
            continue

        score = weighted_sum / weight_avail  # weighted FIP

        tier = TIERS[-1][1]
        for thr, label in TIERS:
            if score <= thr:
                tier = label
                break

        # True only when the rolling xFIP/SIERA actually came from L5 game logs
        # (so the frontend can mark the score as form-weighted).
        rolling = bool(g and (g.get("l5") or {}).get("xfip") is not None)
        if rolling:
            n_rolling += 1

        p["unified_score"]          = round(score, 2)
        p["unified_tier"]           = tier
        p["unified_components"]     = components
        p["unified_weight_covered"] = round(weight_avail, 1)
        p["unified_rolling"]        = rolling
        n_scored += 1
        tier_counts[tier] = tier_counts.get(tier, 0) + 1

    d.setdefault("scoring", {})
    d["scoring"]["unified_score"] = {
        "computed_at": datetime.datetime.utcnow().isoformat() + "Z",
        "scale": "FIP (weighted average — lower = better)",
        "method": ("rolling-led: rolling xFIP/SIERA are stabilized "
                   "0.6·L5 + 0.4·season; projection downweighted"),
        "weights": {f: w for f, w in COMPONENTS},
        "rolling_blend": {"l5": ROLL_L5_WEIGHT, "season": ROLL_SEASON_WEIGHT},
        "tier_thresholds": [{"max_val": t, "label": l}
                            for t, l in TIERS if t < float("inf")],
        "n_scored": n_scored,
        "n_rolling": n_rolling,
        "n_too_sparse": n_sparse,
        "tier_counts": tier_counts,
    }

    with open(OUTPUT, "w") as f:
        json.dump(d, f, indent=2)
    print(f"[score] scored {n_scored} pitchers "
          f"({n_rolling} with L5 rolling, {n_sparse} too sparse)", file=sys.stderr)
    print("[score] tiers: " + ", ".join(f"{l}={n}" for l, n in tier_counts.items()),
          file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
