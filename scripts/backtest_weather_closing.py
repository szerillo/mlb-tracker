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


# high wind-out receptivity parks (BP wr_out top tier)
HIGH_WIND = {"CHC","BOS","PHI","COL","STL","ATL","NYY","BAL"}


def ols2(rows, y_of):
    """park-demeaned y ~ t_adj + w_adj via normal equations (no numpy).
    Returns realized-x for temp and wind vs the calibrated slope."""
    import statistics as _st
    from collections import defaultdict
    data=[(x["t_adj"], x["w_adj"], y_of(x)) for x in rows
          if x.get("t_adj") is not None and x.get("w_adj") is not None]
    if len(data) < 25: return None
    cal = _st.mean([x["actual"] for x in rows]) / 100.0
    mt=_st.mean([d[0] for d in data]); mw=_st.mean([d[1] for d in data]); my=_st.mean([d[2] for d in data])
    Stt=sum((d[0]-mt)**2 for d in data); Sww=sum((d[1]-mw)**2 for d in data)
    Stw=sum((d[0]-mt)*(d[1]-mw) for d in data)
    Sty=sum((d[0]-mt)*(d[2]-my) for d in data); Swy=sum((d[1]-mw)*(d[2]-my) for d in data)
    det=Stt*Sww-Stw*Stw
    if det==0: return None
    bt=(Sww*Sty-Stw*Swy)/det; bw=(Stt*Swy-Stw*Sty)/det
    return {"n":len(data),
            "temp_slope":round(bt,4),"temp_realized_x":round(bt/cal,2),
            "wind_slope":round(bw,4),"wind_realized_x":round(bw/cal,2)}


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
            comp = res.get("components", {}) or {}
            recs.append({"date": ds, "pk": pk, "code": code, "adj": adj,
                         "t_adj": comp.get("t_adj_pct"), "w_adj": comp.get("w_adj_pct"),
                         "close": close, "actual": actual, "resid": actual - close,
                         "skew": SKEW_START <= ds <= SKEW_END})
        d += dt.timedelta(days=1)
    if not recs:
        print("[closing-bt] no games", file=sys.stderr); return 0

    import random as _rnd

    def _realized(rows, cal, predkey="adj"):
        """single-var park-demeaned realized_x for a row subset."""
        from collections import defaultdict
        byp = defaultdict(list)
        for x in rows: byp[x["code"]].append(x["actual"])
        pm = {c: st.mean(v) for c, v in byp.items()}
        o = ols([(x[predkey], x["actual"]-pm[x["code"]]) for x in rows if x.get(predkey) is not None])
        return (o["slope"]/cal) if o else None

    def boot_ci(rows, cal, predkey="adj", B=1500):
        vals=[]
        n=len(rows)
        for _ in range(B):
            samp=[rows[_rnd.randrange(n)] for _ in range(n)]
            v=_realized(samp, cal, predkey)
            if v is not None: vals.append(v)
        vals.sort()
        if len(vals)<50: return None
        return {"x": round(_realized(rows, cal, predkey),2),
                "ci90": [round(vals[int(.05*len(vals))],2), round(vals[int(.95*len(vals))],2)], "n": n}

    def boot_diff(a, b, cal, B=1500):
        """CI on realized_x(a) - realized_x(b); if CI excludes 0 the groups truly differ."""
        na, nb = len(a), len(b); diffs=[]
        for _ in range(B):
            sa=[a[_rnd.randrange(na)] for _ in range(na)]
            sb=[b[_rnd.randrange(nb)] for _ in range(nb)]
            va=_realized(sa, cal); vb=_realized(sb, cal)
            if va is not None and vb is not None: diffs.append(va-vb)
        diffs.sort()
        return {"diff": round(_realized(a,cal)-_realized(b,cal),2),
                "ci90": [round(diffs[int(.05*len(diffs))],2), round(diffs[int(.95*len(diffs))],2)],
                "excludes_0": diffs[int(.05*len(diffs))]>0 or diffs[int(.95*len(diffs))]<0}

    def mag_buckets(rows, cal, key, edges):
        """park-independent: within each |component| bucket, did runs move as predicted?
        realized_x = mean(park-demeaned actual) / (mean(adj)*cal)."""
        from collections import defaultdict
        byp = defaultdict(list)
        for x in rows: byp[x["code"]].append(x["actual"])
        pm = {c: st.mean(v) for c, v in byp.items()}
        out=[]
        for lo,hi in edges:
            g=[x for x in rows if x.get(key) is not None and lo<=x[key]<hi]
            if len(g)<12: out.append({"range":[lo,hi],"n":len(g),"note":"thin"}); continue
            madj=st.mean([x[key] for x in g])
            dev=st.mean([x["actual"]-pm[x["code"]] for x in g])
            pred=madj*cal
            out.append({"range":[lo,hi],"n":len(g),"mean_comp_pct":round(madj,1),
                        "predicted_runs":round(pred,2),"actual_dev_runs":round(dev,2),
                        "realized_x":(round(dev/pred,2) if abs(pred)>0.05 else None)})
        return out

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
        # temp-vs-wind split (is the overshoot uniform or wind-only?)
        tw = ols2(rows, lambda x: x["actual"] - pmean[x["code"]])
        # per-park realized-x (parks with enough games; CHC/high-wind highlighted)
        per_park = {}
        for c in sorted(byp):
            pr = [x for x in rows if x["code"] == c]
            if len(pr) < 15: continue
            pcal = st.mean([x["actual"] for x in pr]) / 100.0
            o = ols([(x["adj"], x["actual"] - pmean[c]) for x in pr])
            if o and pcal:
                per_park[c] = {"n": o["n"], "realized_x": round(o["slope"]/pcal, 2), "r": o["r"],
                               "mean_adj": round(st.mean([x["adj"] for x in pr]), 1)}
        # high-wind-tier parks vs the rest
        def grp(codes):
            g = [x for x in rows if x["code"] in codes]
            if len(g) < 20: return None
            gcal = st.mean([x["actual"] for x in g]) / 100.0
            o = ols([(x["adj"], x["actual"] - pmean[x["code"]]) for x in g])
            return {"n": o["n"], "realized_x": round(o["slope"]/gcal, 2), "r": o["r"]} if o else None
        return {"n": len(rows),
                "calibrated_slope": round(cal, 4),
                "calibration_vs_actual": calib,
                "temp_vs_wind_split": tw,
                "high_wind_parks": grp(HIGH_WIND),
                "other_parks": grp(set(byp) - HIGH_WIND),
                "per_park": per_park,
                "robustness": {
                    "boot_all":  boot_ci(rows, cal),
                    "boot_high_wind": boot_ci([x for x in rows if x["code"] in HIGH_WIND], cal),
                    "boot_other":     boot_ci([x for x in rows if x["code"] not in HIGH_WIND], cal),
                    "high_minus_other_diff": boot_diff([x for x in rows if x["code"] in HIGH_WIND],
                                                        [x for x in rows if x["code"] not in HIGH_WIND], cal),
                    "by_total_adj_bucket": mag_buckets(rows, cal, "adj",
                        [(-99,-5),(-5,-2),(-2,2),(2,5),(5,10),(10,99)]),
                    "by_wind_component_bucket": mag_buckets(rows, cal, "w_adj",
                        [(-99,-3),(-3,0),(0,3),(3,7),(7,12),(12,99)]),
                },
                "edge_vs_closing": edge,
                "mean_resid_actual_minus_close": round(st.mean([x["resid"] for x in rows]), 2),
                "hit_rate": {f"adj>=+{BUCKET:.0f}%_went_over": hr_over, "n_over": len(over),
                             f"adj<=-{BUCKET:.0f}%_went_under": hr_under, "n_under": len(under)}}

    def recalibrate_parks(rows, B=1200):
        """Empirical-Bayes per-park wr_out proposal. Each park's realized_x (how
        much runs move vs our prediction) is shrunk toward 1.0 (no change) by its
        reliability: m = 1 + (realized_x - 1) * tau2/(tau2+se^2). Strong-signal
        parks (low bootstrap se) move; noisy small-n parks stay near prior. New
        wr_out = current * m, capped to avoid wild swings. Total-adj realized_x is
        a proxy (wind-dominated parks it fits well; temp-parts have small wind)."""
        import random as _r
        from collections import defaultdict
        byp = defaultdict(list)
        for x in rows: byp[x["code"]].append(x)
        parks = {c: g for c, g in byp.items() if len(g) >= 15}
        # per-park realized_x + bootstrap se
        est = {}
        for c, g in parks.items():
            cal = st.mean([x["actual"] for x in g]) / 100.0
            m = st.mean([x["actual"] for x in g])
            o = ols([(x["adj"], x["actual"] - m) for x in g])
            if not o or cal == 0: continue
            x0 = o["slope"] / cal
            bs = []
            n = len(g)
            for _ in range(B):
                samp = [g[_r.randrange(n)] for _ in range(n)]
                mm = st.mean([z["actual"] for z in samp]); cc = st.mean([z["actual"] for z in samp])/100.0
                oo = ols([(z["adj"], z["actual"]-mm) for z in samp])
                if oo and cc: bs.append(oo["slope"]/cc)
            if len(bs) < 50: continue
            se = st.pstdev(bs)
            est[c] = {"n": n, "r": o["r"], "realized_x": round(x0, 2), "se": round(se, 2)}
        if not est: return {}
        xs = [e["realized_x"] for e in est.values()]
        mean_v = st.mean([e["se"]**2 for e in est.values()])
        tau2 = max(0.0, st.pvariance(xs) - mean_v)   # method-of-moments between-park variance
        out = {}
        for c, e in est.items():
            w = tau2 / (tau2 + e["se"]**2) if (tau2 + e["se"]**2) > 0 else 0.0
            m_shrunk = 1.0 + (e["realized_x"] - 1.0) * w
            m_shrunk = max(0.5, min(2.0, m_shrunk))   # cap the multiplier
            cur = (v8.BP_BASE.get(c) or {}).get("wr_out")
            proposed = round(cur * m_shrunk, 2) if cur is not None else None
            out[c] = {**e, "shrink_weight": round(w, 2), "mult": round(m_shrunk, 2),
                      "wr_out_now": cur, "wr_out_proposed": proposed,
                      "material": abs(m_shrunk - 1.0) >= 0.10}
        return {"tau2": round(tau2, 3), "n_parks": len(out),
                "n_material": sum(1 for v in out.values() if v["material"]), "parks": out}

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
        "park_wind_recalibration": recalibrate_parks(clean),
    }
    with open(OUT, "w") as f: json.dump(payload, f, indent=2)
    print(json.dumps({k: payload[k] for k in ("span","n_total","n_clean","HEADLINE_clean","ball_skew_window")}, indent=1))
    return 0


if __name__ == "__main__":
    sys.exit(main())
