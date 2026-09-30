#!/usr/bin/env python3
"""
v2_live.py  ---  Playoff Fit (v2) re-priced conditional on postseason results so far.

Same engine as run_v2_bracket.py (october_model + bracket.Pricer, frozen end-of-season
inputs), but on the ACTUAL seeded bracket (the realized seeding = heaviest bracket_dist
row) and with every series already under way priced from its current score over the
remaining games (bracket.HOME_PATTERN home/away). Decided series are 1/0.
Writes data/v2_marginalized.json in the same schema. Safe to run every few minutes.
"""
import json, os, sys, datetime as dt, collections
import numpy as np, requests
HERE=os.path.dirname(os.path.abspath(__file__)); sys.path.insert(0,HERE)
import october_model as M, bracket as B
from run_v2_bracket import ABBR, season_pct, load_bracket
REPO=os.path.abspath(os.path.join(HERE,'..','..')); DATA=os.path.join(REPO,'data')
BEST_OF={'F':3,'D':5,'L':7,'W':7}
def state(season):
    out={}
    for gt in BEST_OF:
        try: d=requests.get(f"https://statsapi.mlb.com/api/v1/schedule?sportId=1&gameType={gt}&season={season}",timeout=30).json()
        except Exception: continue
        for dd in d.get('dates',[]):
            for g in dd['games']:
                a=g['teams']['away']['team']['id']; h=g['teams']['home']['team']['id']
                k=(frozenset({a,h}),BEST_OF[gt]); w=out.setdefault(k,{a:0,h:0})
                if g['status']['detailedState']=='Final':
                    sa=g['teams']['away'].get('score'); sh=g['teams']['home'].get('score')
                    if sa is not None and sh is not None: w[a if sa>sh else h]=w.get(a if sa>sh else h,0)+1
    return out
def main():
    season=dt.date.today().year
    bd=load_bracket(os.path.join(DATA,'bracket_dist.json')); al,nl,_=max(bd,key=lambda x:x[2])
    pct=season_pct(season)
    cfg=os.path.join(HERE,'inputs','OPERATIVE_2026_CONFIG.json')
    F=M.build_field_from_raw(season,teams=list(al+nl)); F=F.set_index('team') if 'team' in F.columns else F
    pr=B.Pricer(F,pct)
    ST=state(season)
    base=pr.p_series
    def p_series(a,b,best_of,a_hosts=True):
        w=ST.get((frozenset({a,b}),best_of))
        if not w or sum(w.values())==0: return base(a,b,best_of,a_hosts)
        pat=B.HOME_PATTERN[best_of] if a_hosts else [1-h for h in B.HOME_PATTERN[best_of]]
        need=best_of//2+1; aw=w.get(a,0); bw=w.get(b,0); start=aw+bw
        ps=[pr.p_game(a,b,h==1) for h in pat]; tot=[0.0]
        def rec(i,x,y,p):
            if x==need: tot[0]+=p; return
            if y==need or i>=best_of: return
            rec(i+1,x+1,y,p*ps[i]); rec(i+1,x,y+1,p*(1-ps[i]))
        rec(start,aw,bw,1.0); return tot[0]
    pr.p_series=p_series
    ws,A,N,rcs,rds=B.world_series(pr,al,nl)
    ab=lambda d:{ABBR.get(k,str(k)):round(v,4) for k,v in sorted(d.items(),key=lambda x:-x[1])}
    out=dict(ws=ab(ws),al_pennant=ab(A),nl_pennant=ab(N),reach_cs=ab(rcs),reach_ds=ab(rds),n_realizations=1,
             date=dt.date.today().isoformat(),engine='v2 rot_oct (live conditional)',coef=M.COEF,series_temperature=1.0)
    json.dump(out,open(os.path.join(DATA,'v2_marginalized.json'),'w'),indent=1)
    live=sum(1 for w in ST.values() if sum(w.values())>0)
    print(f"[v2_live] priced actual bracket, {live} series with results -> v2_marginalized.json")
if __name__=='__main__': main()
