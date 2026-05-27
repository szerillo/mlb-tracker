"""
Refresh umpire stats from UmpScorecards.
Outputs data/umps.json with the same schema the site expects.

Methodology:
  - Per-ump value: 2026 games weighted x5, 2025 x4, 2024 x1 (50/40/10), per game.
  - Baseline: the SAME 50/40/10 recency weighting over 2024-26 (current-weighted
    league norm, so the 2026 ABS-era level shift is reflected, not anchored to a
    stale 2025-heavy baseline).
  - Bayesian shrinkage: 50-game prior toward that baseline.
  - Minimum 10 raw games in 2024-2026.
"""
import json, urllib.request, os, datetime
from collections import defaultdict

UMPSCORECARDS_API = "https://umpscorecards.com/api/games"
OUTPUT = os.path.join(os.path.dirname(__file__), "..", "data", "umps.json")

PRIOR = 50
MIN_RAW = 10


def off_adj(r):
    hp = (r.get("home_pitcher_impact") or 0) + (r.get("away_pitcher_impact") or 0)
    hb = (r.get("home_batter_impact") or 0) + (r.get("away_batter_impact") or 0)
    return hb - hp


def main():
    import sys
    sys.path.insert(0, os.path.dirname(__file__))
    from _common import skip_if_not_in_window
    # UmpScorecards updates once per day; we only need to run at overnight anchors.
    if skip_if_not_in_window("refresh_umps", overnight_only=True):
        return
    print("Fetching UmpScorecards games...")
    req = urllib.request.Request(UMPSCORECARDS_API, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=120) as r:
        rows = json.load(r)["rows"]
    print(f"  loaded {len(rows)} games")

    # Baseline = the SAME 50/40/10 (2026/2025/2024) recency weighting applied to
    # the per-ump values, so the "0% ump" reference is the current-weighted league
    # norm. This matters in 2026: the ABS-challenge era shifted league-average
    # offense-favor toward neutral (2025 +0.40 → 2026 +0.05 runs/g), so a recency
    # baseline correctly pulls the reference down rather than anchoring to a stale
    # 2025-heavy level.
    base_sw = 0.0
    base_so = 0.0
    for r in rows:
        y = r.get("date", "")[:4]
        w = 5 if y == "2026" else 4 if y == "2025" else 1 if y == "2024" else 0
        if w and not r.get("failed"):
            base_sw += w
            base_so += w * off_adj(r)
    baseline = base_so / base_sw
    print(f"  baseline (50/40/10 weighted 2024-26): {baseline:+.4f}")

    # Per-ump weighted
    ump_off = defaultdict(float)
    ump_accx = defaultdict(float)
    ump_wn = defaultdict(float)
    raw_n = defaultdict(int)

    for r in rows:
        u = r.get("umpire")
        y = r.get("date", "")[:4]
        if not u or r.get("failed"):
            continue
        w = 5 if y == "2026" else 4 if y == "2025" else 1 if y == "2024" else 0
        if w == 0:
            continue
        ump_off[u] += w * off_adj(r)
        ump_accx[u] += w * (r.get("accuracy_above_x") or 0)
        ump_wn[u] += w
        if y in ("2024", "2025", "2026"):
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

    # Merge accented Alfonso Márquez if both variants exist
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
        "methodology": f"Per-ump offense adj: 2026x5 + 2025x4 + 2024x1 (50/40/10) per-game weighting, {PRIOR}-game Bayesian prior toward the same 50/40/10-weighted 2024-26 baseline.",
        "window_label": "2024-2026 (50/40/10 by year, recency-weighted)",
        "shrinkage_prior_games": PRIOR,
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
