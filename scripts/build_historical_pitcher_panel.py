#!/usr/bin/env python3
"""
build_historical_pitcher_panel.py — leakage-free multi-year pitcher-start panel
for the wFIP backtest power unlock (see historical_pitcher_panel_SCOPE.md).

ONE source: Savant per-pitch (statcast_search CSV, pitcher side), chunked by
date window. Each pitcher-start is reduced to COUNT SUMS (never ratios) so any
as-of window can compute exact rates. Then we build the as-of panel: for each
STARTER's start, season-to-date + last-5 features from STRICTLY PRIOR starts,
plus forward-30d targets (K-BB%, xFIP proxy, RA9) and the immediately-next start.

Runs/allowed comes from Statcast score columns (post_bat_score - bat_score);
IP from an events->outs map; K/BB/HR from events. No FG/MLB-API join needed.

Two phases (default: both, per season):
  pull      — fetch windows -> append per-start rows to data/hist/starts_{season}.csv
  assemble  — read all starts_*.csv -> data/historical_pitcher_panel.csv

USAGE:
  python scripts/build_historical_pitcher_panel.py --seasons 2023,2024,2025,2026
  python scripts/build_historical_pitcher_panel.py --seasons 2026 --start 2026-06-01 --end 2026-06-21   # slice
  python scripts/build_historical_pitcher_panel.py --phase assemble
"""
from __future__ import annotations
import argparse, csv, datetime, io, os, sys, urllib.request
from pathlib import Path
from collections import defaultdict

REPO = Path(__file__).resolve().parent.parent
HIST = REPO / "data" / "hist"
OUT  = REPO / "data" / "historical_pitcher_panel.csv"
UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36"

FB   = {"FF","FA","FT","SI","FC"}
CSW_DESC   = {"called_strike","swinging_strike","swinging_strike_blocked","foul_tip","missed_bunt"}
WHIFF_DESC = {"swinging_strike","swinging_strike_blocked","missed_bunt"}
SWING_DESC = {"swinging_strike","swinging_strike_blocked","foul","foul_tip","hit_into_play","missed_bunt","foul_bunt"}
OUTS_BY_EVENT = {
    "strikeout":1,"strikeout_double_play":2,"field_out":1,"force_out":1,
    "grounded_into_double_play":2,"double_play":2,"sac_fly":1,"sac_bunt":1,
    "sac_fly_double_play":2,"sac_bunt_double_play":2,"fielders_choice_out":1,
    "fielders_choice":1,"other_out":1,"triple_play":3,
    "caught_stealing_2b":1,"caught_stealing_3b":1,"caught_stealing_home":1,
    "pickoff_caught_stealing_2b":1,"pickoff_caught_stealing_3b":1,"pickoff_caught_stealing_home":1,
    "pickoff_1b":1,"pickoff_2b":1,"pickoff_3b":1,"runner_double_play":1,
}
# PA-ending events (define TBF). Baserunning outs (CS/pickoff) are NOT PAs.
NON_PA = {"caught_stealing_2b","caught_stealing_3b","caught_stealing_home",
          "pickoff_caught_stealing_2b","pickoff_caught_stealing_3b","pickoff_caught_stealing_home",
          "pickoff_1b","pickoff_2b","pickoff_3b","runner_double_play","stolen_base_2b",
          "stolen_base_3b","stolen_base_home","caught_stealing","other"}
LG_HR_FB = 0.115   # league HR/FB for the xFIP proxy
FIP_C = 3.10

STAT_COLS = ["date","season","pitcher","game_pk","team","opp","is_sp","first_ab",
             "pitches","tbf","k","bb","hr","outs","ra",
             "csw_ct","whiff_ct","swing_ct","fb_velo_sum","fb_velo_n","fb_spin_sum","fb_spin_n",
             "bip","gb","fbb","barrel","hardhit","xwobacon_sum","xwobacon_n"]

def _f(v):
    try: return float(v)
    except (TypeError,ValueError): return None

def fetch(url, timeout=120):
    return urllib.request.urlopen(urllib.request.Request(url, headers={"User-Agent":UA}), timeout=timeout).read().decode("utf-8-sig","ignore")

def windows(start, end, step=3):
    d=start
    while d<=end:
        d2=min(d+datetime.timedelta(days=step), end)
        yield d, d2
        d=d2+datetime.timedelta(days=1)

def reduce_window(text, season, agg):
    rr=csv.DictReader(io.StringIO(text))
    for x in rr:
        pid=x.get("pitcher"); gpk=x.get("game_pk"); gd=(x.get("game_date") or "")[:10]
        if not pid or not gpk or not gd: continue
        key=(pid,gpk)
        a=agg[key]
        if not a:
            tb=str(x.get("inning_topbot") or "")
            team = (x.get("home_team") if tb=="Top" else x.get("away_team")) or ""
            opp  = (x.get("away_team") if tb=="Top" else x.get("home_team")) or ""
            a.update(date=gd, season=season, pitcher=pid, game_pk=gpk, team=team, opp=opp,
                     pitches=0,tbf=0,k=0,bb=0,hr=0,outs=0,ra=0.0,csw_ct=0,whiff_ct=0,swing_ct=0,
                     fb_velo_sum=0.0,fb_velo_n=0,fb_spin_sum=0.0,fb_spin_n=0,
                     bip=0,gb=0,fbb=0,barrel=0,hardhit=0,xwobacon_sum=0.0,xwobacon_n=0,
                     first_ab=10**9, first_inning=10**9)
        a["pitches"]+=1
        # starter identification: track earliest at-bat this pitcher threw
        ab=_f(x.get("at_bat_number")); inn=_f(x.get("inning"))
        if ab is not None and ab < a["first_ab"]: a["first_ab"]=int(ab)
        if inn is not None and inn < a["first_inning"]: a["first_inning"]=int(inn)
        # runs allowed via score delta
        pb,b=_f(x.get("post_bat_score")),_f(x.get("bat_score"))
        if pb is not None and b is not None: a["ra"]+=(pb-b)
        desc=x.get("description") or ""
        if desc in CSW_DESC: a["csw_ct"]+=1
        if desc in WHIFF_DESC: a["whiff_ct"]+=1
        if desc in SWING_DESC: a["swing_ct"]+=1
        pt=x.get("pitch_type") or ""
        if pt in FB:
            rs=_f(x.get("release_speed")); sp=_f(x.get("release_spin_rate"))
            if rs is not None: a["fb_velo_sum"]+=rs; a["fb_velo_n"]+=1
            if sp is not None: a["fb_spin_sum"]+=sp; a["fb_spin_n"]+=1
        ev=x.get("events") or ""
        if ev:
            a["outs"]+=OUTS_BY_EVENT.get(ev,0)
            if ev not in NON_PA: a["tbf"]+=1
            if ev.startswith("strikeout"): a["k"]+=1
            elif ev=="walk": a["bb"]+=1
            elif ev=="home_run": a["hr"]+=1
        # batted balls
        if (x.get("type") or "")=="X":
            a["bip"]+=1
            bbt=x.get("bb_type") or ""
            if bbt=="ground_ball": a["gb"]+=1
            elif bbt in ("fly_ball","popup"): a["fbb"]+=1
            if _f(x.get("launch_speed_angle"))==6: a["barrel"]+=1
            ls=_f(x.get("launch_speed"))
            if ls is not None and ls>=95: a["hardhit"]+=1
            xw=_f(x.get("estimated_woba_using_speedangle"))
            if xw is not None: a["xwobacon_sum"]+=xw; a["xwobacon_n"]+=1

def pull_season(season, start, end):
    HIST.mkdir(parents=True, exist_ok=True)
    agg=defaultdict(dict)
    for a,b in windows(start,end):
        url=("https://baseballsavant.mlb.com/statcast_search/csv?all=true&type=details"
             f"&player_type=pitcher&hfSea={season}%7C&game_date_gt={a.isoformat()}"
             f"&game_date_lt={b.isoformat()}&min_pitches=0")
        try: txt=fetch(url)
        except Exception as e:
            print(f"[hist] window {a} failed ({e})", file=sys.stderr); continue
        reduce_window(txt, season, agg)
        print(f"[hist] {season} {a}..{b}: {len(agg)} starts so far", file=sys.stderr)
    # mark starter: min at_bat for the game+team side. Approx: a pitcher is SP if
    # they threw the first inning AND their first_ab is <=2 (first PA of the game
    # for their side is at_bat_number 1). We compute per game_pk the min first_ab
    # among each team's pitchers.
    by_game_team=defaultdict(list)
    for k,a in agg.items():
        by_game_team[(a["game_pk"],a["team"])].append(a)
    for grp in by_game_team.values():
        starter=min(grp, key=lambda a:a["first_ab"])
        for a in grp: a["is_sp"] = 1 if (a is starter and a["first_inning"]==1) else 0
    path=HIST/f"starts_{season}.csv"
    with open(path,"w",newline="") as fh:
        w=csv.DictWriter(fh, fieldnames=STAT_COLS, extrasaction="ignore"); w.writeheader()
        for a in agg.values(): w.writerow(a)
    print(f"[hist] wrote {path} ({len(agg)} starts)", file=sys.stderr)

# ---------- assemble ----------
def _ip(outs): return outs/3.0
def _rates(rows):
    """counts over a list of per-start rows -> dict of rates (higher=raw)."""
    s=lambda k: sum(int(float(r[k])) for r in rows)
    sf=lambda k: sum(float(r[k]) for r in rows)
    tbf=s("tbf"); outs=s("outs"); bip=s("bip"); ip=_ip(outs)
    k=s("k"); bb=s("bb"); hr=s("hr"); fbb=s("fbb"); ra=sf("ra")
    o={}
    if tbf:
        o["k_pct"]=100*k/tbf; o["bb_pct"]=100*bb/tbf; o["k_bb_pct"]=100*(k-bb)/tbf
    if s("pitches"): o["csw"]=100*s("csw_ct")/s("pitches")
    if s("swing_ct"): o["whiff"]=100*s("whiff_ct")/s("swing_ct")
    if s("fb_velo_n"): o["velo"]=sf("fb_velo_sum")/s("fb_velo_n")
    if s("fb_spin_n"): o["spin"]=sf("fb_spin_sum")/s("fb_spin_n")
    if bip:
        o["gb_pct"]=100*s("gb")/bip; o["fb_pct"]=100*fbb/bip
        o["barrel_pct"]=100*s("barrel")/bip; o["hardhit_pct"]=100*s("hardhit")/bip
    if s("xwobacon_n"): o["xwobacon"]=sf("xwobacon_sum")/s("xwobacon_n")
    if ip>0:
        o["fip"]=(13*hr+3*bb-2*k)/ip+FIP_C
        o["xfip"]=(13*(fbb*LG_HR_FB)+3*bb-2*k)/ip+FIP_C
        o["ra9"]=9*ra/ip
    o["_ip"]=ip; o["_tbf"]=tbf
    return o

def assemble():
    rows=[]
    for f in sorted(HIST.glob("starts_*.csv")):
        rows+=list(csv.DictReader(open(f)))
    # group by pitcher-season, sort by date
    byp=defaultdict(list)
    for r in rows:
        byp[(r["pitcher"], r["season"])].append(r)
    for v in byp.values(): v.sort(key=lambda r:r["date"])
    out=[]
    ASOF=["k_pct","bb_pct","k_bb_pct","csw","whiff","velo","spin","gb_pct","fb_pct",
          "barrel_pct","hardhit_pct","xwobacon","fip","xfip","ra9"]
    for (pid,season), starts in byp.items():
        for i,cur in enumerate(starts):
            if str(cur.get("is_sp"))!="1": continue
            prior=starts[:i]
            if len(prior)<3: continue                     # need a usable as-of history
            std=_rates(prior)                              # season-to-date (prior only)
            if std["_ip"]<15: continue
            l5=_rates(prior[-5:])
            # forward 30d (strictly after cur.date), same pitcher
            cd=datetime.date.fromisoformat(cur["date"]); end=cd+datetime.timedelta(days=30)
            fwd=[s for s in starts[i+1:] if cd < datetime.date.fromisoformat(s["date"]) <= end]
            nxt=starts[i+1:i+2]
            rec={"pitcher":pid,"season":season,"date":cur["date"],"team":cur["team"],"opp":cur["opp"]}
            for k in ASOF:
                if k in std: rec["asof_"+k]=round(std[k],3)
                if k in l5:  rec["l5_"+k]=round(l5[k],3)
            # trend deltas
            for k in ("velo","spin","csw","k_bb_pct"):
                if k in std and k in l5: rec["delta_"+k]=round(l5[k]-std[k],3)
            # forward targets
            if fwd:
                fr=_rates(fwd)
                if fr["_ip"]>=10:
                    for k in ("k_bb_pct","xfip","ra9","fip"):
                        if k in fr: rec["fwd30_"+k]=round(fr[k],3)
                    rec["fwd30_ip"]=round(fr["_ip"],1)
            if nxt:
                nr=_rates(nxt)
                for k in ("k_bb_pct","xfip","ra9"):
                    if k in nr: rec["fwd1_"+k]=round(nr[k],3)
                rec["fwd1_end"]=nxt[0]["date"]       # embargo helper for purged CV
            out.append(rec)
    # union of columns
    cols=[]
    for r in out:
        for k in r:
            if k not in cols: cols.append(k)
    with open(OUT,"w",newline="") as fh:
        w=csv.DictWriter(fh, fieldnames=cols, extrasaction="ignore"); w.writeheader()
        for r in out: w.writerow(r)
    nT=sum(1 for r in out if "fwd30_ra9" in r)
    print(f"[hist] wrote {OUT}: {len(out)} SP-start rows ({nT} with a forward-30d target)", file=sys.stderr)

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--seasons", default="2026")
    ap.add_argument("--start"); ap.add_argument("--end")
    ap.add_argument("--phase", choices=["pull","assemble","both"], default="both")
    a=ap.parse_args()
    if a.phase in ("pull","both"):
        for season in [int(x) for x in a.seasons.split(",") if x.strip()]:
            if a.start and a.end:
                s=datetime.date.fromisoformat(a.start); e=datetime.date.fromisoformat(a.end)
            else:
                s=datetime.date(season,3,15); e=min(datetime.date(season,10,5), datetime.date.today())
            pull_season(season, s, e)
    if a.phase in ("assemble","both"):
        assemble()

if __name__=="__main__":
    main()
