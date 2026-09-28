#!/usr/bin/env python3
"""
refresh_series_live.py  ---  live, in-round series probabilities for the futures board.

Runs frequently (every few minutes in October). Reads the analytic all-matchups grid
(data/lane2_grid.json: every reachable bracket series with per-game win probs + starters,
emitted by run_lane2_bracket) and the live postseason state from statsapi, then:

  1. detects the CURRENT round (Wild Card -> Division -> LCS -> World Series),
  2. builds that round's actual matchups from who has advanced,
  3. recomputes each active series' win% / exact-result CONDITIONAL on games already
     decided (e.g. "NYY leads 1-0" -> NYY win% jumps), using only the remaining
     scheduled games' win probs,
  4. writes data/lane2_series.json (same schema the board already reads) with a per-series
     `state` string and `live` flag; the WS-exacta block is passed through unchanged.

Packs-free and cheap: all the heavy pricing lives in the grid; this only does arithmetic
over statsapi scores. Before any postseason game finals it is a pass-through (round=WC,
pregame numbers, no `live` flag), so it is safe to schedule now.
"""
from __future__ import annotations
import json, os, sys, datetime, urllib.request

HERE=os.path.dirname(os.path.abspath(__file__)); REPO=os.path.abspath(os.path.join(HERE,".."))
DATA=os.path.join(REPO,"data")
GRID=os.path.join(DATA,"lane2_grid.json")        # all-matchups analytic grid
SERIES=os.path.join(DATA,"lane2_series.json")    # board feed (we overwrite this)
TPROJ=os.path.join(DATA,"lane2_team_proj.json")  # per-team rotation/pen/offense ranks

AL={'TB','CLE','HOU','NYY','BOS','CWS'}; NL={'MIL','LAD','ATL','SD','CHC','PHI'}
BEST_OF={'F':3,'D':5,'L':7,'W':7}
ROUND_CODE={'F':'WC','D':'LDS','L':'LCS','W':'WS'}
ROUND_LABEL={'WC':'Wild Card','LDS':'Division Series','LCS':'Championship Series','WS':'World Series'}
ROUND_ORDER=['F','D','L','W']

def _get(u,timeout=20):
    return json.load(urllib.request.urlopen(urllib.request.Request(u,headers={'User-Agent':'x'}),timeout=timeout))
def american(p):
    p=min(max(p,1e-6),1-1e-6); return round(-100*p/(1-p)) if p>=0.5 else round(100*(1-p)/p)

# ── conditional series math ──────────────────────────────────────────────────
def cond_dist(games, teamA, aw, bw):
    """Distribution over FINAL (a_wins,b_wins) given current (aw,bw) already banked,
    over the remaining scheduled games. games = [{home,away,home_wp}]."""
    need=len(games)//2+1
    pw=[g['home_wp'] if g['home']==teamA else 1-g['home_wp'] for g in games]
    start=aw+bw
    out={}
    def rec(i,a,b,pr):
        if a==need or b==need: out[(a,b)]=out.get((a,b),0.0)+pr; return
        if i>=len(pw):  # safety (shouldn't happen in a well-formed series)
            out[(a,b)]=out.get((a,b),0.0)+pr; return
        rec(i+1,a+1,b,pr*pw[i]); rec(i+1,a,b+1,pr*(1-pw[i]))
    rec(start,aw,bw,1.0); return out,need

# ── statsapi live state ──────────────────────────────────────────────────────
def series_state(season):
    """Return {gtype: {frozenset(abbrs): {'wins':{ab:n},'played':n,'games':[...]}}}"""
    st={}
    for gt in ROUND_ORDER:
        try:
            d=_get(f"https://statsapi.mlb.com/api/v1/schedule?sportId=1&gameType={gt}&season={season}&hydrate=team")
        except Exception:
            continue
        buckets={}
        for dt in d.get('dates',[]):
            for g in dt['games']:
                a=g['teams']['away']['team'].get('abbreviation'); h=g['teams']['home']['team'].get('abbreviation')
                if not a or not h: continue
                key=frozenset({a,h})
                b=buckets.setdefault(key,{'wins':{a:0,h:0},'played':0})
                if g['status']['detailedState']=='Final':
                    sa=g['teams']['away'].get('score'); sh=g['teams']['home'].get('score')
                    if sa is not None and sh is not None:
                        b['played']+=1; b['wins'][a if sa>sh else h]=b['wins'].get(a if sa>sh else h,0)+1
        if buckets: st[gt]=buckets
    return st

def current_round(state):
    """Latest round that has any games and is not fully decided; falls back to WC."""
    cur='F'
    for gt in ROUND_ORDER:
        b=state.get(gt)
        if not b: continue
        cur=gt
        need=BEST_OF[gt]//2+1
        if any(max(v['wins'].values() or [0])<need for v in b.values()):
            return gt   # this round still has an undecided series -> it's current
    return cur          # else the last round that had games (may all be done)

# ── grid lookup ──────────────────────────────────────────────────────────────
def load_grid():
    g=json.load(open(GRID))
    idx={}  # (frozenset,best_of) -> (aTeam, games)
    for k,obj in g.get("game_grid",g).items():
        teams=set(); bo=3 if 'bo3' in k else 5 if 'bo5' in k else 7
        for gm in obj['games']: teams.update([gm['home'],gm['away']])
        idx[(frozenset(teams),bo)]=(k.split(' v ')[0],obj['games'])
    return g,idx

def build_series(gtype, matchup_key, grid_idx, wins):
    bo=BEST_OF[gtype]
    ent=grid_idx.get((matchup_key,bo))
    if not ent: return None
    A,games=ent
    B=[t for t in matchup_key if t!=A][0] if len(matchup_key)==2 else games[0]['away']
    aw=wins.get(A,0); bw=wins.get(B,0)
    d,need=cond_dist(games,A,aw,bw)
    pA=sum(v for (a,b),v in d.items() if a==need)
    exact={}
    for (a,b),p in sorted(d.items(),key=lambda x:(-x[0][0],x[0][1])):
        exact[(f"a-{a}-{b}" if a>b else f"b-{b}-{a}")]=round(p,4)
    pAsw=sum(v for (a,b),v in d.items() if a==need and b==0)
    pBsw=sum(v for (a,b),v in d.items() if b==need and a==0)
    gm=[{"g":i+1,"home":x['home'],"away":x['away'],
         "home_sp":x.get('home_sp'),"away_sp":x.get('away_sp'),
         "home_sp_rank":x.get('home_sp_rank'),"away_sp_rank":x.get('away_sp_rank'),
         "home_wp":round(x['home_wp'],4)}
        for i,x in enumerate(games)]
    decided=aw+bw
    if decided==0: state=None
    elif aw>bw:    state=f"{A} leads {aw}-{bw}"
    elif bw>aw:    state=f"{B} leads {bw}-{aw}"
    else:          state=f"Series tied {aw}-{aw}"
    return {"round":ROUND_CODE[gtype],"league":"AL" if A in AL else "NL","best_of":bo,"host":A,
            "a":A,"b":B,"games":gm,"state":state,"live":decided>0,
            "wins":{"a":aw,"b":bw},
            "model":{"p_a":round(pA,4),"ml_a":american(pA),"p_b":round(1-pA,4),"ml_b":american(1-pA),
                     "exact":exact,"spread":{"a_minus_1_5":round(pAsw,4),"b_minus_1_5":round(pBsw,4)}},
            "market":{"ml_a":None,"ml_b":None,"exact":None,"spread":None}}

# ── main ─────────────────────────────────────────────────────────────────────
def main():
    if not os.path.exists(GRID):
        print(f"[series_live] no grid at {GRID}; leaving lane2_series.json as-is", file=sys.stderr); return 0
    season=datetime.date.today().year
    grid,idx=load_grid()
    state=series_state(season)
    if not state:
        print("[series_live] no postseason games yet; pass-through (pregame WC feed unchanged)")
        return 0
    gt=current_round(state); buckets=state[gt]
    series=[]
    for key,b in buckets.items():
        s=build_series(gt,key,idx,b['wins'])
        if s: series.append(s)
    # order AL then NL, host first
    series.sort(key=lambda s:(s["league"]!="AL", s["host"]))
    # attach per-team projection ranks (rotation/bullpen/offense) from the engine's feed
    tp=(json.load(open(TPROJ)).get("teams",{}) if os.path.exists(TPROJ) else {})
    if tp:
        for s in series: s["proj"]={"n":len(tp),"a":tp.get(s["a"]),"b":tp.get(s["b"])}
    # keep the existing exacta block if present
    prev=json.load(open(SERIES)) if os.path.exists(SERIES) else {}
    out={"generated_at":datetime.datetime.utcnow().replace(microsecond=0).isoformat()+"+00:00",
         "engine":"lane2 live (refresh_series_live)","round":ROUND_CODE[gt],
         "round_label":ROUND_LABEL[ROUND_CODE[gt]],"series":series,
         "ws_exacta":prev.get("ws_exacta",{})}
    open(SERIES,"w").write(json.dumps(out,indent=1))
    live=sum(1 for s in series if s["live"])
    print(f"[series_live] round={ROUND_CODE[gt]} | {len(series)} series ({live} in progress) -> lane2_series.json")
    return 0

if __name__=="__main__":
    sys.exit(main())
