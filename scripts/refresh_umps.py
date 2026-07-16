"""
Refresh umpire stats from UmpScorecards.
Outputs data/umps.json with the same schema the site expects.

Methodology:
  - Per-ump value: offense/pitching bias = batter_impact - pitcher_impact (runs).
    2026 games weighted x5, 2025 x4, 2024 x1 (50/40/10), per game.
  - CHALLENGE (ABS) DAMPENING (new 2026): UmpScorecards' impact fields are
    computed on the umpire's CALLED pitches, PRE-challenge — an overturned miss
    still counts against the umpire even though the ABS challenge erased it from
    the actual game. Proof from the data: 2026 mean run-impact is HIGHER than
    2024/25 (1.37 vs ~1.22) and correlates +0.43 with overturns; if overturns
    were netted out that correlation would be negative. So the raw number
    OVERSTATES an umpire's realized game impact in the challenge era. We scale
    every off_adj by (1 - league_overturn_share), where league_overturn_share =
    overturned / called_wrong across all 2026 games (~0.225). This is applied
    UNIFORMLY, not per-ump, because per-ump overturn share is noise at this
    sample (split-half r = 0.08) — trying to differentiate umpires by it would
    add variance, not signal. It's a conservative floor: challenges target the
    highest-leverage misses, so the true impact erased is likely somewhat larger.
  - Baseline: the SAME 50/40/10 recency weighting over 2024-26 on the dampened
    scale (so 0% = current, ABS-realized league norm).
  - Bayesian shrinkage: 50-game prior toward that baseline.
  - Minimum 10 raw games in 2024-2026.
"""
import json, urllib.request, os, datetime
from collections import defaultdict

UMPSCORECARDS_API = "https://umpscorecards.com/api/games"
OUTPUT = os.path.join(os.path.dirname(__file__), "..", "data", "umps.json")

PRIOR = 50
MIN_RAW = 10
YW = {"2026": 5, "2025": 4, "2024": 1}


def off_adj(r):
    hp = (r.get("home_pitcher_impact") or 0) + (r.get("away_pitcher_impact") or 0)
    hb = (r.get("home_batter_impact") or 0) + (r.get("away_batter_impact") or 0)
    return hb - hp


def main():
    import sys
    sys.path.insert(0, os.path.dirname(__file__))
    from _common import skip_if_not_in_window
    if skip_if_not_in_window("refresh_umps", overnight_only=True):
        return
    print("Fetching UmpScorecards games...")
    req = urllib.request.Request(UMPSCORECARDS_API, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=120) as r:
        rows = json.load(r)["rows"]
    print(f"  loaded {len(rows)} games")

    # --- Challenge dampening factor, computed live from the 2026 sample --------
    ov = cw = 0
    for r in rows:
        if r.get("date", "")[:4] == "2026" and r.get("type") == "R" and not r.get("failed"):
            ov += r.get("n_overturned") or 0
            cw += r.get("called_wrong") or 0
    overturn_share = (ov / cw) if cw else 0.0
    dampen = 1.0 - overturn_share
    print(f"  2026 overturn share of wrong calls: {overturn_share:.3f} "
          f"-> challenge dampen x{dampen:.3f}  ({ov}/{cw})")

    def val(r):
        return off_adj(r) * dampen   # ABS-realized offense/pitching bias

    # Baseline = recency-weighted mean of the dampened per-game values.
    base_sw = base_so = 0.0
    for r in rows:
        y = r.get("date", "")[:4]
        w = YW.get(y, 0)
        if w and not r.get("failed"):
            base_sw += w
            base_so += w * val(r)
    baseline = base_so / base_sw
    print(f"  baseline (50/40/10, challenge-dampened): {baseline:+.4f}")

    ump_off = defaultdict(float)
    ump_accx = defaultdict(float)
    ump_wn = defaultdict(float)
    raw_n = defaultdict(int)
    for r in rows:
        u = r.get("umpire")
        y = r.get("date", "")[:4]
        w = YW.get(y, 0)
        if not u or r.get("failed") or w == 0:
            continue
        ump_off[u] += w * val(r)
        ump_accx[u] += w * (r.get("accuracy_above_x") or 0)
        ump_wn[u] += w
        raw_n[u] += 1

    out = {}
    for u in ump_off:
        if raw_n[u] < MIN_RAW:
            continue
        n = raw_n[u]
        raw_off_val = ump_off[u] / ump_wn[u]
        shrunk_off = (n * raw_off_val + PRIOR * baseline) / (n + PRIOR)
        out[u] = {
            "n": n,
            "off_adj_shrunk": round(shrunk_off, 4),
            "off_adj_raw": round(raw_off_val, 3),
            "acc_above_x": round(ump_accx[u] / ump_wn[u], 3),
        }

    if "Alfonso Marquez" in out and "Alfonso Márquez" in out:
        a, b = out["Alfonso Marquez"], out["Alfonso Márquez"]
        tn = a["n"] + b["n"]
        raw_combo = (a["off_adj_raw"] * a["n"] + b["off_adj_raw"] * b["n"]) / tn
        accx_combo = (a["acc_above_x"] * a["n"] + b["acc_above_x"] * b["n"]) / tn
        out["Alfonso Márquez"] = {
            "n": tn,
            "off_adj_shrunk": round((tn * raw_combo + PRIOR * baseline) / (tn + PRIOR), 4),
            "off_adj_raw": round(raw_combo, 3),
            "acc_above_x": round(accx_combo, 3),
        }
        del out["Alfonso Marquez"]

    league_acc = round(sum(v["acc_above_x"] for v in out.values()) / len(out), 4)
    payload = {
        "generated_at": datetime.date.today().isoformat(),
        "source": "UmpScorecards (umpscorecards.com/api)",
        "source_url": "https://umpscorecards.com",
        "methodology": (
            "Offense/pitching bias = batter_impact - pitcher_impact (runs). "
            "2026x5 + 2025x4 + 2024x1 (50/40/10) per-game weighting. "
            f"Challenge-dampened x{dampen:.3f} = 1 - the 2026 ABS overturn share of "
            "wrong calls (UmpScorecards impact is pre-challenge; overturned misses "
            "still count against the ump, so raw overstates realized game impact). "
            f"{PRIOR}-game Bayesian prior toward the dampened baseline."),
        "window_label": "2024-2026 (50/40/10 by year, ABS-challenge adjusted)",
        "shrinkage_prior_games": PRIOR,
        "challenge_dampen": round(dampen, 4),
        "league_overturn_share_2026": round(overturn_share, 4),
        "baseline": baseline,
        "league_avg": {"baseline_off_adj": round(baseline, 4), "acc_above_x_mean": league_acc},
        "umpires": out,
    }

    os.makedirs(os.path.dirname(OUTPUT), exist_ok=True)
    with open(OUTPUT, "w") as f:
        json.dump(payload, f, indent=2)
    print(f"  wrote {len(out)} umpires to {OUTPUT}")


if __name__ == "__main__":
    main()
