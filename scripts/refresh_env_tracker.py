#!/usr/bin/env python3
"""Build data/env_tracker.json for the "Expectations vs Reality" tracker.

Two sections:
  market   -- dependency-free aggregation of data/odds_archive/*.json
              (per-game panel; the frontend computes over-rate / R/G / filters).
  statcast -- daily batted-ball drift vs a season-pooled EV/LA baseline.
              Requires pybaseball; runs in CI. Skipped gracefully if unavailable.

Metric honesty:
  * Market performance  = OVER-RATE (skew-robust). The mean-R/G-vs-line gap is
    right-skew (blowouts pull the daily mean above the median-scale line) and is
    NOT a betting edge -- surfaced only as a run-environment indicator.
  * Statcast metrics are residuals vs a season-pooled EV/LA surface -> relative
    drift over time (drag / COR), not absolute physics.
"""
import json, glob, os, datetime

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ARCH = os.path.join(ROOT, "data", "odds_archive")
OUT  = os.path.join(ROOT, "data", "env_tracker.json")
SEASON = 2026


def build_market():
    rows = []
    for fp in sorted(glob.glob(os.path.join(ARCH, "*.json"))):
        date = os.path.basename(fp)[:-5]
        try:
            j = json.load(open(fp))
        except Exception:
            continue
        for g in j.get("games", []):
            c = (g.get("consensus") or {}).get("total") or {}
            L = c.get("line")
            a = g.get("actual_away_runs")
            h = g.get("actual_home_runs")
            if (isinstance(L, (int, float)) and 3 < L < 20
                    and isinstance(a, (int, float)) and isinstance(h, (int, float))):
                rows.append([date, g.get("home_team"), g.get("away_team"), L, a + h])
    return {"cols": ["date", "home", "away", "line", "total"], "rows": rows}


def build_statcast(season=SEASON):
    try:
        from pybaseball import statcast
        import pandas as pd
        import numpy as np
    except Exception as e:
        print("statcast skip (import):", e)
        return None
    try:
        start = f"{season}-03-20"
        end = datetime.date.today().isoformat()
        df = statcast(start_dt=start, end_dt=end)
    except Exception as e:
        print("statcast skip (fetch):", e)
        return None
    if df is None or getattr(df, "empty", True):
        return None

    df = df[df["type"] == "X"].dropna(subset=["launch_speed", "launch_angle", "game_date"]).copy()
    if df.empty:
        return None
    df["ev_bin"] = (df["launch_speed"] // 2) * 2
    df["la_bin"] = (df["launch_angle"] // 3) * 3
    df["is_hr"] = (df["events"] == "home_run").astype(float)

    hr_rate = df.groupby(["ev_bin", "la_bin"])["is_hr"].mean()
    df["xhr"] = df.apply(lambda r: hr_rate.get((r["ev_bin"], r["la_bin"]), 0.0), axis=1)

    fb = df[(df["launch_angle"] >= 25) & (df["launch_angle"] <= 50)].copy()
    dist = fb.dropna(subset=["hit_distance_sc"]).copy()
    exp_dist = dist.groupby(["ev_bin", "la_bin"])["hit_distance_sc"].mean()
    dist["resid"] = dist.apply(
        lambda r: r["hit_distance_sc"] - exp_dist.get((r["ev_bin"], r["la_bin"]), r["hit_distance_sc"]),
        axis=1)

    days = []
    for d in sorted(df["game_date"].dropna().unique()):
        ds = str(pd.Timestamp(d).date())
        bbday = df[df["game_date"] == d]
        fbday = fb[fb["game_date"] == d]
        rday = dist[dist["game_date"] == d]
        days.append({
            "date": ds,
            "carry_resid": round(float(rday["resid"].mean()), 2) if len(rday) else None,
            "ev_mean": round(float(fbday["launch_speed"].mean()), 2) if len(fbday) else None,
            "ev_p90": round(float(fbday["launch_speed"].quantile(0.9)), 2) if len(fbday) else None,
            "hr": int(bbday["is_hr"].sum()),
            "xhr": round(float(bbday["xhr"].sum()), 2),
        })
    return {
        "method": "residuals vs season-pooled EV(2mph)xLA(3deg) surface; relative drift, not absolute",
        "fb_angle_deg": [25, 50],
        "days": days,
    }


def main():
    out = {
        "generated_at": datetime.datetime.utcnow().isoformat() + "Z",
        "season": SEASON,
        "metric_note": ("over_pct is the market-performance metric (skew-robust). "
                        "mean R/G is run-environment only (right-skewed vs the median-scale line). "
                        "Statcast metrics are residuals vs a season-pooled EV/LA baseline."),
        "market": build_market(),
        "statcast": build_statcast(),
    }
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(OUT, "w") as f:
        json.dump(out, f, separators=(",", ":"))
    print("wrote", OUT,
          "| market rows:", len(out["market"]["rows"]),
          "| statcast days:", len(out["statcast"]["days"]) if out["statcast"] else 0)


if __name__ == "__main__":
    main()
