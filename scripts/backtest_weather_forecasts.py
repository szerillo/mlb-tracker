#!/usr/bin/env python3
"""
Weather forecast accuracy backtest -> data/weather_forecast_accuracy.json

Answers "whose weather is right, and is our adjustment magnitude fair?" by joining
four sources on MLB gamePk:
  1. OUR forecast inputs + adjustment   (data/weather_calibration_log.csv)
  2. BallparkPal's temp + run adj        (same log: bp_temp_f, bp_pct)
  3. PropFinder OBSERVED actual weather  (api.propfinder.app, source=="obs" hour)
  4. Actual runs scored                  (MLB StatsAPI linescore)

Outputs two scoreboards:
  A) FORECAST-INPUT accuracy: mean abs error of our NWS temp/wind vs the observed
     truth, and BP's temp vs truth. (Whose inputs were closer to reality.)
  B) RUN-ADJUSTMENT calibration: park-demeaned actual total regressed on our
     run_adj_pct and on BP's bp_pct -> realized slope vs the calibrated target,
     plus correlation. (Is our magnitude fair; do we beat BP.)
"""
from __future__ import annotations
import csv, json, math, os, sys, statistics as st, urllib.request, datetime as dt

HERE = os.path.dirname(os.path.abspath(__file__))
LOG  = os.path.join(HERE, "..", "data", "weather_calibration_log.csv")
OUT  = os.path.join(HERE, "..", "data", "weather_forecast_accuracy.json")
PF_API  = "https://api.propfinder.app/mlb/weather-games?date={d}"
MLB_API = "https://statsapi.mlb.com/api/v1/schedule?sportId=1&date={d}&hydrate=linescore"
UA = {"User-Agent": "Mozilla/5.0 (mlb-tracker/wx-backtest)"}

DIR16 = {"N":0,"NNE":22.5,"NE":45,"ENE":67.5,"E":90,"ESE":112.5,"SE":135,"SSE":157.5,
         "S":180,"SSW":202.5,"SW":225,"WSW":247.5,"W":270,"WNW":292.5,"NW":315,"NNW":337.5}


def _get(url):
    return json.loads(urllib.request.urlopen(urllib.request.Request(url, headers=UA), timeout=45).read().decode("utf-8"))


def _f(x):
    try: return float(x)
    except Exception: return None


def _epoch(iso):
    try: return int(dt.datetime.fromisoformat((iso or "").replace("Z", "+00:00")).timestamp())
    except Exception: return None


def ang_err(a, b):
    if a is None or b is None: return None
    d = abs((a - b) % 360)
    return min(d, 360 - d)


def pf_observed(date):
    """gamePk -> observed game-hour weather (prefers source=='obs')."""
    out = {}
    try:
        games = _get(PF_API.format(d=date))
    except Exception as e:
        print(f"[bt] PF fetch failed {date}: {e}", file=sys.stderr); return out
    for g in games or []:
        pk = g.get("id"); wx = g.get("weatherData") or []
        if pk is None or not wx: continue
        ge = _epoch(g.get("gameDate"))
        obs = [h for h in wx if h.get("source") == "obs"] or wx
        h = min(obs, key=lambda x: abs((x.get("dateTimeEpoch") or 0) - (ge or 0))) if ge else obs[len(obs)//2]
        out[int(pk)] = {"temp": _f(h.get("temp")), "wind": _f(h.get("windSpeed")),
                        "dir": _f(h.get("windDir")), "src": h.get("source")}
    return out


def finals(date):
    out = {}
    try:
        d = _get(MLB_API.format(d=date))
    except Exception: return out
    for day in d.get("dates", []):
        for g in day.get("games", []):
            if "Final" not in g.get("status", {}).get("detailedState", ""): continue
            a = g["teams"]["away"].get("score"); h = g["teams"]["home"].get("score")
            if a is not None and h is not None: out[g["gamePk"]] = a + h
    return out


def main():
    rows = list(csv.DictReader(open(LOG)))
    today = dt.date.today().isoformat()
    dates = sorted({r["date"] for r in rows if r["date"] < today})
    pf, fin = {}, {}
    for d in dates:
        pf[d] = pf_observed(d); fin[d] = finals(d)

    recs = []
    for r in rows:
        d = r["date"]
        if d >= today: continue
        pk = int(r["game_pk"])
        o = (pf.get(d) or {}).get(pk)
        if not o or o["temp"] is None: continue
        if r["is_dome"] == "True": continue
        our_t = _f(r["first_pitch_temp_f"]); our_w = _f(r["wind_speed_mph"])
        our_dir = DIR16.get((r["wind_dir"] or "").strip().upper())
        bp_t = _f(r["bp_temp_f"])
        recs.append({
            "date": d, "pk": pk, "venue": r["venue"],
            "obs_temp": o["temp"], "obs_wind": o["wind"], "obs_dir": o["dir"], "obs_src": o["src"],
            "our_temp": our_t, "our_wind": our_w, "our_dir": our_dir,
            "bp_temp": bp_t,
            "our_temp_err": (abs(our_t - o["temp"]) if our_t is not None else None),
            "bp_temp_err": (abs(bp_t - o["temp"]) if bp_t is not None else None),
            "our_wind_err": (abs(our_w - o["wind"]) if (our_w is not None and o["wind"] is not None) else None),
            "our_dir_err": ang_err(our_dir, o["dir"]),
            "total": (fin.get(d) or {}).get(pk),
            "our_adj": _f(r["run_adj_pct"]), "bp_adj": _f(r["bp_pct"]),
        })

    def mae(key, sub=None):
        v = [x[key] for x in recs if x[key] is not None and (sub is None or x[sub] is not None)]
        return round(st.mean(v), 2) if v else None

    # A) forecast-input accuracy (only where BP also has a temp, for fair compare)
    both_t = [x for x in recs if x["our_temp_err"] is not None and x["bp_temp_err"] is not None]
    forecast = {
        "n_games": len(recs),
        "our_temp_MAE_f": mae("our_temp_err"),
        "our_wind_MAE_mph": mae("our_wind_err"),
        "our_winddir_MAE_deg": mae("our_dir_err"),
        "temp_head_to_head": {
            "n": len(both_t),
            "our_temp_MAE_f": round(st.mean([x["our_temp_err"] for x in both_t]), 2) if both_t else None,
            "bp_temp_MAE_f":  round(st.mean([x["bp_temp_err"] for x in both_t]), 2) if both_t else None,
        },
    }

    # B) run-adjustment calibration (park-demeaned actual total ~ adj%)
    def ols(xy):
        xs = [a for a, b in xy]; ys = [b for a, b in xy]; n = len(xs)
        mx = st.mean(xs); my = st.mean(ys)
        sxx = sum((x-mx)**2 for x in xs)
        if sxx == 0: return None
        b = sum((x-mx)*(y-my) for x, y in zip(xs, ys)) / sxx
        sx = st.pstdev(xs); sy = st.pstdev(ys)
        r = (sum((x-mx)*(y-my) for x, y in zip(xs, ys))/n)/(sx*sy) if sx and sy else 0
        return {"slope": round(b, 4), "r": round(r, 3), "n": n}

    played = [x for x in recs if x["total"] is not None]
    from collections import defaultdict
    byp = defaultdict(list)
    for x in played: byp[x["venue"]].append(x["total"])
    pmean = {v: st.mean(t) for v, t in byp.items()}
    cal_slope = st.mean([x["total"] for x in played]) / 100.0 if played else None
    ours_xy = [(x["our_adj"], x["total"]-pmean[x["venue"]]) for x in played if x["our_adj"] is not None]
    bp_xy   = [(x["bp_adj"],  x["total"]-pmean[x["venue"]]) for x in played if x["bp_adj"] is not None]
    run = {
        "n_played": len(played),
        "calibrated_slope_runs_per_pct": round(cal_slope, 4) if cal_slope else None,
        "ours": ols(ours_xy), "bp": ols(bp_xy),
    }
    if run["ours"] and cal_slope:
        run["ours"]["realized_x"] = round(run["ours"]["slope"]/cal_slope, 2)
    if run["bp"] and cal_slope:
        run["bp"]["realized_x"] = round(run["bp"]["slope"]/cal_slope, 2)

    payload = {
        "generated_at": dt.datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "method": "our NWS forecast + BP vs PropFinder observed actuals (source=obs) and actual runs",
        "date_range": [dates[0], dates[-1]] if dates else None,
        "forecast_input_accuracy": forecast,
        "run_adjustment_calibration": run,
        "games": recs,
    }
    with open(OUT, "w") as f: json.dump(payload, f, indent=2)
    print(json.dumps({"forecast": forecast, "run": run}, indent=1))
    return 0


if __name__ == "__main__":
    sys.exit(main())
