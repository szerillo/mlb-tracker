#!/usr/bin/env python3
"""
Marcel hitter prior (Fable 2026 spec) — regressed-to-league wOBA for hitters,
keyed by MLBAM id. Feeds compute_staff_offense._prior_fallback for hitters with
no ROS projection (replaces the flat 0.310 stub). Regenerate weekly in CI so
mid-season debuts age in — one statsapi call per season, ~30s.

  marcel_woba = (num25 + 0.6*num24 + 250*0.292) / (den25 + 0.6*den24 + 250)
  num  = 0.69*(BB-IBB) + 0.72*HBP + 0.88*1B + 1.24*2B + 1.56*3B + 1.95*HR
  den  = PA - IBB - SH
  0.292 forward-validated regression target (Fable: 2024 low-PA cohort -> 0.291
  in 2025; true no-history call-ups -> 0.294).
"""
import json, datetime, urllib.request
from pathlib import Path

OUT = Path(__file__).resolve().parent.parent / "data" / "marcel_prior_2026.json"
TARGET, REG_PA = 0.292, 250.0
W_PREV = 0.6
LW = dict(ubb=0.69, hbp=0.72, s=0.88, d=1.24, t=1.56, hr=1.95)

def season(yr):
    u=(f"https://statsapi.mlb.com/api/v1/stats?stats=season&group=hitting&season={yr}"
       f"&gameType=R&limit=5000&sportId=1&playerPool=all")
    j=json.loads(urllib.request.urlopen(u,timeout=60).read().decode())
    out={}
    for sp in j["stats"][0]["splits"]:
        st=sp.get("stat",{}); pl=sp.get("player",{})
        pid=pl.get("id"); nm=pl.get("fullName")
        if pid is None: continue
        g=lambda k:(st.get(k) or 0)
        pa,bb,ibb,hbp=g("plateAppearances"),g("baseOnBalls"),g("intentionalWalks"),g("hitByPitch")
        d,t,hr,h=g("doubles"),g("triples"),g("homeRuns"),g("hits")
        sh=g("sacBunts")
        s=max(0,h-d-t-hr)
        num=LW["ubb"]*(bb-ibb)+LW["hbp"]*hbp+LW["s"]*s+LW["d"]*d+LW["t"]*t+LW["hr"]*hr
        den=pa-ibb-sh
        out[pid]={"name":nm,"num":num,"den":den,"pa":pa}
    return out

def main():
    cur=season(2025); prev=season(2024)   # 25/24 = two most recent completed seasons; 2026 deliberately excluded (enters via the staff blend w=PA/(PA+500))
    ids=set(cur)|set(prev)
    priors={}
    for pid in ids:
        c=cur.get(pid,{}); p=prev.get(pid,{})
        num=c.get("num",0.0)+W_PREV*p.get("num",0.0)+REG_PA*TARGET
        den=c.get("den",0.0)+W_PREV*p.get("den",0.0)+REG_PA
        if den<=0: continue
        pa_ev=(c.get("pa",0) or 0)+W_PREV*(p.get("pa",0) or 0)
        priors[str(pid)]={"name":c.get("name") or p.get("name"),
                          "marcel_woba":round(num/den,4),"pa_evidence":round(pa_ev,1)}
    payload={"generated_at":datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds"),
             "method":"marcel_woba (Fable 2026): (cur + 0.6*prev + 250*0.292)/(den + 250)",
             "regression_target":TARGET,
             "target_provenance":"Fable forward-validated: 2024 low-PA -> 0.291 in 2025; no-history call-ups -> 0.294",
             "no_history_default":TARGET,"priors":priors}
    OUT.write_text(json.dumps(payload,separators=(",",":")))
    print(f"[marcel] wrote {len(priors)} priors -> {OUT}")

if __name__=="__main__":
    main()
