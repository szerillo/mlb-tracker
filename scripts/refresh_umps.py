"""
Refresh umpire stats from UmpScorecards.
Outputs data/umps.json with the same schema the site expects.

Methodology (matches what was agreed with user):
  - Baseline: 2025 + 2026 games, game-weighted mean of offense_adj.
  - Per-ump value: 2024 games weighted x1, 2025 x2, 2026 x2.
  - Bayesian shrinkage: 50-game prior toward league baseline.
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

    # Baseline: 2025 + 2026
    base_sum = 0.0
    base_n = 0
    for r in rows:
        y = r.get("date", "")[:4]
        if y in ("2025", "2026") and not r.get("failed"):
            base_sum += off_adj(r)
            base_n += 1
    baseline = base_sum / base_n
    print(f"  baseline (2025+2026, n={base_n}): {baseline:+.4f}")

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
        w = 2 if y in ("2025", "2026") else 1 if y == "2024" else 0
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
        "methodology": f"Per-ump: 2024x1 + 2025x2 + 2026x2 weighted, {PRIOR}-game Bayesian prior. Baseline = 2025+2026 game-weighted mean.",
        "window_label": "2024-2026 (2025/26 double-weighted)",
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
