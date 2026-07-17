#!/usr/bin/env python3
"""
Longer-sample weather backtest vs CLOSING TOTALS and actual results
-> data/weather_closing_backtest.json

Runs OUR weather model (v8_weather.compute_v8) on PropFinder's OBSERVED weather
(source "obs") for every open-air game back through the season, then joins the
closing total (PropFinder gameRunLine) and the actual runs (MLB StatsAPI). Using
observed weather removes forecast error, so this isolates the COEFFICIENT
calibration — the cleanest read on "is our adjustment magnitude right."

Two tests:
  A) CALIBRATION vs actual: park-demeaned actual total ~ our_adj%. realized slope
     / calibrated slope -> are we high (aggressive) or low.
  B) EDGE vs the market: residual = actual - closing. Does our weather adj predict
     the over/under surprise the market didn't price, and what's the directional
     hit rate when we call a big weather game.

The late-May..mid-June "juiced ball" over-skew is carved out of the headline
numbers and reported separately (SKEW_START..SKEW_END).
"""
from __future__ import annotations
import datetime as dt, json, os, statistics as st, sys, urllib.request
import v8_weather as v8

HERE = os.path.dirname(os.path.abspath(__file__))
OUT  = os.path.join(HERE, "..", "data", "weather_closing_backtest.json")
PF   = "https://api.propfinder.app/mlb/weather-games?date={d}"
MLB  = "https://statsapi.mlb.com/api/v1/schedule?sportId=1&date={d}&hydrate=linescore"
UA   = {"User-Agent": "Mozilla/5.0 (mlb-tracker/wx-closing-backtest)"}

START = os.environ.get("BT_START", "2026-04-01")
SKEW_START, SKEW_END = "2026-05-20", "2026-06-15"   # juiced-ball over-skew window
BUCKET = 5.0   # |adj%| threshold that counts as a "weather call" for hit-rate

# PropFinder homeTeam.code -> our BP_BASE code
ALIAS = {"OAK":"ATH","ATH":"ATH","CWS":"CHW","CHW":"CHW","WSH":"WAS","WAS":"WAS",
         "TBR":"TB","TB":"TB","KCR":"KC","KC":"KC","SDP":"SD","SD":"SD","SFG":"SF",
         "SF":"SF","AZ":"ARI","ARI":"ARI"}


def _get(url):
    return json.loads(urllib.request.urlopen(urllib.request.Request(url, headers=UA), timeout=45).read().decode("utf-8"))


def _f(x):
    try: return float(x)
    except Exception: return None


def _epoch(iso):
    try: return int(dt.datetime.fromisoformat((iso or "").replace("Z","+00:00")).timestamp())
    except Exception: return None


def finals(date):
    out = {}
    try: d = _get(MLB.format(d=date))
    except Exception: return out
    for day in d.get("dates", []):
        for g in day.get("games", []):
            if "Final" not in g.get("status", {}).get("detailedState", ""): continue
            a = g["teams"]["away"].get("score"); h = g["teams"]["home"].get("score")
            if a is not None and h is not None: out[g["gamePk"]] = a + h
    return out


def code_of(g):
    c = ((g.get("homeTeam") or {}).get("code") or "").upper()
    return ALIAS.get(c, c)


def game_window(weather, gepoch):
    """observed hours from first pitch to +3h; returns (mean_temp, hour0)."""
    if not weather or gepoch is None: return None, None
    hrs = sorted(weather, key=lambda h: h.get("dateTimeEpoch") or 0)
    win = [h for h in hrs if 0 <= (h.get("dateTimeEpoch") or 0) - gepoch <= 3*3600]
    if not win:  # fall back to nearest
        win = [min(hrs, key=lambda h: abs((h.get("dateTimeEpoch") or 0) - gepoch))]
    return win, win[0]


def ols(xy):
    xs=[a for a,_ in xy]; ys=[b for _,b in xy]; n=len(xs)
    if n<3: return None
    mx=st.mean(xs); my=st.mean(ys); sxx=sum((x-mx)**2 for x in xs)
    if sxx==0: return None
    b=sum((x-mx)*(y-my) for x,y in zip(xs,ys))/sxx
    sx=st.pstdev(xs); sy=st.pstdev(ys)
    r=(sum((x-mx)*(y-my) for x,y in zip(xs,ys))/n)/(sx*sy) if sx and sy else 0
    return {"slope":round(b,4),"r":round(r,3),"n":n}


def main():
    start = dt.date.fromisoformat(START)
    end = dt.date.today() - dt.timedelta(days=1)
    recs = []
    d = start
    while d <= end:
        ds = d.isoformat()
        try: games = _get(PF.format(d=ds))
        except Exception: games = []
        fin = finals(ds) if games else {}
        for g in games or []:
            pk = g.get("id"); code = code_of(g)
            if pk is None or code not in v8.BP_BASE: continue
            if v8.BP_BASE[code].get("dome"): continue          # skip roofed parks
            close = _f(g.get("gameRunLine")); actual = fin.get(pk)
            if close is None or actual is None: continue
            gepoch = _epoch(g.get("gameDate"))
            win, h0 = game_window(g.get("weatherData") or [], gepoch)
            if not h0 or h0.get("temp") is None: continue
            if (h0.get("source") or "") != "obs":              # observed only
                continue
            tmean = st.mean([w["temp"] for w in win if w.get("temp") is not None]) if win else h0["temp"]
            wx = {"t": tmean, "hum": h0.get("humidity"), "ws": h0.get("windSpeed"),
                  "wd_compass": h0.get("windDir"), "pres": h0.get("pressure"),
                  "precip": h0.get("precipProb"),
                  "t_hours": [w["temp"] for w in win if w.get("temp") is not None]}
            res = v8.compute_v8(code, wx)
            adj = res.get("run_adj_pct")
            if adj is None or res.get("error"): continue
            recs.append({"date": ds, "pk": pk, "code": code, "adj": adj,
                         "close": close, "actual": actual, "resid": actual - close,
                         "skew": SKEW_START <= ds <= SKEW_END})
        d += dt.timedelta(days=1)
    if not recs:
        print("[closing-bt] no games", file=sys.stderr); return 0

    def analyze(rows, label):
        if len(rows) < 10: return {"n": len(rows), "note": "too few"}
        from collections import defaultdict
        byp = defaultdict(list)
        for x in rows: byp[x["code"]].append(x["actual"])
        pmean = {c: st.mean(v) for c, v in byp.items()}
        cal = st.mean([x["actual"] for x in rows]) / 100.0
        calib = ols([(x["adj"], x["actual"]-pmean[x["code"]]) for x in rows])
        if calib: calib["realized_x"] = round(calib["slope"]/cal, 2)
        edge = ols([(x["adj"], x["resid"]) for x in rows])   # does weather predict over/under surprise
        # directional hit rate on "weather calls"
        over = [x for x in rows if x["adj"] >= BUCKET]
        under = [x for x in rows if x["adj"] <= -BUCKET]
        hr_over = round(100*sum(1 for x in over if x["resid"] > 0)/len(over), 1) if over else None
        hr_under = round(100*sum(1 for x in under if x["resid"] < 0)/len(under), 1) if under else None
        return {"n": len(rows),
                "calibrated_slope": round(cal, 4),
                "calibration_vs_actual": calib,
                "edge_vs_closing": edge,
                "mean_resid_actual_minus_close": round(st.mean([x["resid"] for x in rows]), 2),
                "hit_rate": {f"adj>=+{BUCKET:.0f}%_went_over": hr_over, "n_over": len(over),
                             f"adj<=-{BUCKET:.0f}%_went_under": hr_under, "n_under": len(under)}}

    clean = [x for x in recs if not x["skew"]]
    payload = {
        "generated_at": dt.datetime.utcnow().isoformat(timespec="seconds")+"Z",
        "method": "our model on PropFinder OBSERVED weather; closing=PropFinder gameRunLine; actual=MLB",
        "span": [START, end.isoformat()],
        "skew_window_excluded": [SKEW_START, SKEW_END],
        "n_total": len(recs), "n_clean": len(clean),
        "HEADLINE_clean": analyze(clean, "clean"),
        "ball_skew_window": analyze([x for x in recs if x["skew"]], "skew"),
        "all_incl_skew": analyze(recs, "all"),
    }
    with open(OUT, "w") as f: json.dump(payload, f, indent=2)
    print(json.dumps({k: payload[k] for k in ("span","n_total","n_clean","HEADLINE_clean","ball_skew_window")}, indent=1))
    return 0


if __name__ == "__main__":
    sys.exit(main())
