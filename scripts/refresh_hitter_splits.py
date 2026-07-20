#!/usr/bin/env python3
"""
refresh_hitter_splits.py — AUTO vR/vL wRC+ splits, no upload.

Rolling 1-yr wRC+ vs RHP / LHP, reconstructed from Baseball Savant wOBA splits
using FanGraphs' own calibrated wOBA->wRC+ mapping (recovered from FG actuals:
validated corr 0.91/0.94, zero bias). Blended 50% with the projection baseline
(data/splits_baseline.csv, Book2). Writes hitter_splits.json (repo root; the
path the frontend reads) at the raw wRC+ scale.
"""
import urllib.request, csv, io, json, sys, unicodedata, re, datetime
from pathlib import Path
HERE=Path(__file__).resolve().parent; DATA=HERE/".."/"data"; ROOT=HERE/".."
OUT=ROOT/"hitter_splits.json"; BASE=DATA/"splits_baseline.csv"
UA="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36"
# FG-calibrated wOBA(Savant) -> wRC+ mapping (regressed on FG actual splits).
CAL={"R":(-114.5,676,0.0067),"L":(-112.8,671,0.0032)}   # (intercept, slope, savant->FG wOBA offset)
BLEND=0.5                                                 # 50% rolling + 50% projection
MIN_PA={"R":40,"L":30}

def norm(s):
    s=str(s or "")
    if "," in s: a=s.split(",",1); s=a[1].strip()+" "+a[0].strip()
    s=unicodedata.normalize("NFKD",s).encode("ascii","ignore").decode()
    s=re.sub(r"\b(jr|sr|ii|iii|iv)\b","",s.lower()); return re.sub(r"[^a-z ]","",s).strip()
def get(u): return urllib.request.urlopen(urllib.request.Request(u,headers={"User-Agent":UA}),timeout=60).read().decode("utf-8-sig","ignore")

def savant_split(hand, season):
    u=("https://baseballsavant.mlb.com/statcast_search/csv?all=true&player_type=batter"
       f"&group_by=name&min_pitches=1&sort_col=pitches&sort_order=desc&pitcher_throws={hand}"
       f"&hfSea={season}%7C")
    out={}
    for r in csv.DictReader(io.StringIO(get(u))):
        try: out[norm(r["player_name"])]={"woba":float(r["woba"]),"pa":int(r["pa"]),
                                          "name":r["player_name"],"id":r["player_id"]}
        except Exception: pass
    return out

def main():
    season=datetime.date.today().year
    vr=savant_split("R",season); vl=savant_split("L",season)
    print(f"[splits] season {season}  vsR {len(vr)}  vsL {len(vl)}", file=sys.stderr)
    base={}
    if BASE.exists():
        for r in csv.DictReader(open(BASE)):
            base[r["name_key"]]={"name":r["name"],
                "r":float(r["base_wrc_vs_r"]) if r["base_wrc_vs_r"] not in ("","None") else None,
                "l":float(r["base_wrc_vs_l"]) if r["base_wrc_vs_l"] not in ("","None") else None}
    def roll(d,hand,k):
        v=d.get(k)
        if not v or v["pa"]<MIN_PA[hand]: return None
        a,b,off=CAL[hand]; return a+b*(v["woba"]-off)
    out={}
    for k in set(vr)|set(vl)|set(base):
        b=base.get(k,{}); rr=roll(vr,"R",k); rl=roll(vl,"L",k)
        nm=(vr.get(k) or vl.get(k) or b or {}).get("name") or b.get("name")
        pid=(vr.get(k) or vl.get(k) or {}).get("id")
        rec={"name":nm}
        if pid: rec["mlbam_id"]=pid
        # blend rolling + projection; if only one exists, use it
        def blend(rollv, projv):
            if rollv is not None and projv is not None: return round(BLEND*rollv+(1-BLEND)*projv,1)
            return round(rollv if rollv is not None else projv,1) if (rollv is not None or projv is not None) else None
        wr=blend(rr,b.get("r")); wl=blend(rl,b.get("l"))
        if wr is not None: rec["wrc_vs_r"]=wr; rec["wrc_vs_r_raw"]=round(rr,1) if rr is not None else wr
        if wl is not None: rec["wrc_vs_l"]=wl; rec["wrc_vs_l_raw"]=round(rl,1) if rl is not None else wl
        if vr.get(k): rec["pa_vs_r"]=vr[k]["pa"]
        if vl.get(k): rec["pa_vs_l"]=vl[k]["pa"]
        if len(rec)>1: out[k]=rec
    payload={"generated_at":datetime.datetime.utcnow().isoformat(timespec="seconds")+"Z",
             "source":"AUTO: 50% Book2 projection + 50% current-season rolling wRC+ (Savant wOBA split -> FG-calibrated mapping).",
             "window":f"{season} season","blend":BLEND,"k":100,"hitters":out}
    OUT.write_text(json.dumps(payload,indent=2))
    print(f"[splits] wrote {len(out)} hitters -> {OUT}", file=sys.stderr)
    return 0
if __name__=="__main__": sys.exit(main())
