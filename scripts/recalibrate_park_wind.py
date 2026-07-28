#!/usr/bin/env python3
"""
Empirically re-derive a park's OUT-wind receptivity (wr_out, in %/10mph) from
game-time wind + realized total runs, to check whether v8_weather.py's
BP_BASE[park]["wr_out"] is calibrated right.

Comerica (DET) carries wr_out=0.36 — the lowest of any outdoor park — so a strong
out-to-RF wind barely moves the run adjustment. Comerica changed its OF
dimensions/walls ahead of 2023, so this fits on 2023+ only (pre-2023 is stale).

Method (park-season-demeaned, uses MLB's own ball-relative wind text):
  1. StatsAPI schedule (teamId, hydrate=linescore) -> home gamePks + total runs.
  2. Per game, feed/live gameData.weather.wind e.g. "12 mph, Out To RF".
       "Out To *"  -> out  (+speed);  "In From *" -> in (-speed);
       L To R / R To L / Varies / Calm -> cross (0).
  3. Demean total runs AND signed-out-speed within each season. OLS slope of
     demeaned runs on demeaned signed-out-speed = runs per mph of out-wind.
  4. implied wr_out (%/10mph) = slope * 10 / overall_mean_runs * 100.
  5. Bootstrap 2000x for a 90% CI, plus an out/cross/in mean-runs split.

Usage:  python scripts/recalibrate_park_wind.py --park DET --seasons 2023-2026
"""
from __future__ import annotations
import argparse, json, os, random, re, statistics as st, sys, time, urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
OUTDIR = os.path.join(HERE, "..", "data")
UA = {"User-Agent": "Mozilla/5.0 (mlb-tracker/wind-recal)"}

TEAM_ID = {
    "DET": 116, "CHC": 112, "CIN": 113, "PIT": 134, "KC": 118, "COL": 115,
    "STL": 138, "BOS": 111, "NYY": 147, "BAL": 110, "CLE": 114, "MIN": 142,
    "CWS": 145, "PHI": 143, "WAS": 120, "NYM": 121, "MIL": 158, "SF": 137,
    "SD": 135, "LAD": 119, "SEA": 136, "ATL": 144,
}
SCHED = ("https://statsapi.mlb.com/api/v1/schedule?sportId=1&teamId={tid}"
         "&startDate={s}-03-01&endDate={s}-11-15&hydrate=linescore")
FEED  = ("https://statsapi.mlb.com/api/v1.1/game/{pk}/feed/live"
         "?fields=gameData,weather,condition,temp,wind")


def _get(url, tries=3):
    for i in range(tries):
        try:
            with urllib.request.urlopen(urllib.request.Request(url, headers=UA), timeout=45) as r:
                return json.loads(r.read().decode("utf-8"))
        except Exception as e:
            if i == tries - 1:
                print(f"  fetch fail {url[:70]}: {e}", file=sys.stderr); return None
            time.sleep(1.2)


def parse_wind(wind_str):
    if not wind_str: return None
    s = wind_str.strip()
    if s.lower().startswith("calm"): return (0.0, 0)
    m = re.match(r"\s*(\d+)\s*mph", s, re.I)
    spd = float(m.group(1)) if m else 0.0
    low = s.lower()
    if "out to" in low:  return (spd, 1)
    if "in from" in low: return (spd, -1)
    return (spd, 0)


def collect(park, seasons):
    tid = TEAM_ID[park]; rows = []
    for s in seasons:
        sched = _get(SCHED.format(tid=tid, s=s))
        if not sched: continue
        games = []
        for d in sched.get("dates", []):
            for g in d.get("games", []):
                if g.get("teams", {}).get("home", {}).get("team", {}).get("id") != tid: continue
                if g.get("status", {}).get("abstractGameState") != "Final": continue
                if g.get("gameType") != "R": continue
                ls = g.get("linescore", {}) or {}
                hr = (ls.get("teams", {}).get("home", {}) or {}).get("runs")
                ar = (ls.get("teams", {}).get("away", {}) or {}).get("runs")
                if hr is None or ar is None: continue
                games.append((g["gamePk"], g.get("officialDate") or d.get("date"), hr + ar))
        print(f"  {s}: {len(games)} final home games", file=sys.stderr)
        for pk, date, runs in games:
            feed = _get(FEED.format(pk=pk))
            wx = ((feed or {}).get("gameData", {}) or {}).get("weather", {}) or {}
            pw = parse_wind(wx.get("wind"))
            if pw is None: continue
            spd, sign = pw
            rows.append({"season": s, "date": date, "runs": runs,
                         "wind_speed": spd, "out_sign": sign, "signed_out": sign * spd,
                         "temp": wx.get("temp"), "wind_raw": wx.get("wind"),
                         "condition": wx.get("condition")})
            time.sleep(0.05)
    return rows


def ols_slope(xs, ys):
    n = len(xs); mx = sum(xs) / n; my = sum(ys) / n
    sxx = sum((x - mx) ** 2 for x in xs)
    if sxx == 0: return 0.0
    return sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / sxx


def analyze(rows):
    by = {}
    for r in rows: by.setdefault(r["season"], []).append(r)
    rmean = {s: st.mean(r["runs"] for r in v) for s, v in by.items()}
    omean = {s: st.mean(r["signed_out"] for r in v) for s, v in by.items()}
    for r in rows:
        r["runs_dm"] = r["runs"] - rmean[r["season"]]
        r["out_dm"] = r["signed_out"] - omean[r["season"]]
    overall = st.mean(r["runs"] for r in rows)
    xs = [r["out_dm"] for r in rows]; ys = [r["runs_dm"] for r in rows]
    slope = ols_slope(xs, ys)
    implied = slope * 10 / overall * 100
    boots = []; idx = list(range(len(rows)))
    for _ in range(2000):
        samp = [rows[random.choice(idx)] for _ in idx]
        b = ols_slope([r["out_dm"] for r in samp], [r["runs_dm"] for r in samp])
        boots.append(b * 10 / overall * 100)
    boots.sort()
    ci = (boots[int(0.05 * len(boots))], boots[int(0.95 * len(boots))])
    def grp(p):
        v = [r["runs_dm"] for r in rows if p(r)]
        return (len(v), round(st.mean(v), 2) if v else None)
    split = {
        "strong_out(>=8)": grp(lambda r: r["out_sign"] > 0 and r["wind_speed"] >= 8),
        "light_out(1-7)":  grp(lambda r: r["out_sign"] > 0 and 1 <= r["wind_speed"] < 8),
        "cross/calm":      grp(lambda r: r["out_sign"] == 0),
        "light_in(1-7)":   grp(lambda r: r["out_sign"] < 0 and 1 <= r["wind_speed"] < 8),
        "strong_in(>=8)":  grp(lambda r: r["out_sign"] < 0 and r["wind_speed"] >= 8),
    }
    return {"n_games": len(rows), "seasons": sorted(by), "n_by_season": {s: len(v) for s, v in by.items()},
            "overall_mean_runs": round(overall, 2), "slope_runs_per_mph_out": round(slope, 4),
            "implied_wr_out_pct_per_10mph": round(implied, 3),
            "ci90_wr_out": [round(ci[0], 3), round(ci[1], 3)], "mean_dm_runs_split": split}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--park", default="DET"); ap.add_argument("--seasons", default="2023-2026")
    args = ap.parse_args()
    a, b = args.seasons.split("-"); seasons = list(range(int(a), int(b) + 1))
    print(f"[recal] {args.park} seasons {seasons}", file=sys.stderr)
    rows = collect(args.park, seasons)
    if len(rows) < 20:
        print(f"[recal] too few games ({len(rows)}) — aborting", file=sys.stderr); return 1
    res = analyze(rows); res["park"] = args.park; res["current_wr_out"] = None
    try:
        import importlib.util
        spec = importlib.util.spec_from_file_location("v8", os.path.join(HERE, "v8_weather.py"))
        v8 = importlib.util.module_from_spec(spec); spec.loader.exec_module(v8)
        res["current_wr_out"] = v8.BP_BASE.get(args.park, {}).get("wr_out")
    except Exception: pass
    os.makedirs(OUTDIR, exist_ok=True)
    outpath = os.path.join(OUTDIR, f"wind_recal_{args.park}.json")
    with open(outpath, "w") as f: json.dump(res, f, indent=2)
    print(json.dumps(res, indent=2))
    print(f"[recal] wrote {outpath}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
