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
TEAMAB={139:'TB',114:'CLE',145:'CWS',117:'HOU',147:'NYY',111:'BOS',158:'MIL',119:'LAD',144:'ATL',135:'SD',112:'CHC',143:'PHI',140:'TEX',109:'ARI'}
SEED={}  # abbr -> seed no (1..6), filled from bracket_dist in main()
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


# ── live bracket walk: re-price every future round conditional on results so far ──
MARG=os.path.join(DATA,"lane2_marginalized.json")
AL_SEEDS=['TB','CLE','HOU','NYY','BOS','CWS']; NL_SEEDS=['MIL','LAD','ATL','SD','CHC','PHI']
def _grid_idx2(grid):
    idx={}
    for k,o in grid.get("game_grid",grid).items():
        parts=k.split(' '); idx[(frozenset({parts[0],parts[2]}),int(parts[3][2]))]=(parts[0],o)
    return idx
def _p_from_games(games,A,aw,bw):
    need=len(games)//2+1; pw=[g['home_wp'] if g['home']==A else 1-g['home_wp'] for g in games]; out=[0.0]
    def rec(i,a,b,pr):
        if a==need: out[0]+=pr; return
        if b==need or i>=len(pw): return
        rec(i+1,a+1,b,pr*pw[i]); rec(i+1,a,b+1,pr*(1-pw[i]))
    rec(aw+bw,aw,bw,1.0); return out[0]
def rebuild_marginalized(grid,state):
    """Exact bracket walk over the all-matchups grid. Any series with games already played
    (any round) is priced conditional on its current score; decided series are 1/0."""
    idx=_grid_idx2(grid)
    live={}
    for gt,b in state.items():
        for key,v in b.items(): live[(key,BEST_OF[gt])]=v['wins']
    def P(x,y,bo):
        A,o=idx[(frozenset({x,y}),bo)]; B=y if A==x else x
        w=live.get((frozenset({x,y}),bo),{})
        p=_p_from_games(o['games'],A,w.get(A,0),w.get(B,0))
        return p if A==x else 1-p
    out={"reach_ds":{},"reach_cs":{},"al_pennant":{},"nl_pennant":{},"ws":{}}
    for L,pk in ((AL_SEEDS,'al_pennant'),(NL_SEEDS,'nl_pennant')):
        s={i+1:t for i,t in enumerate(L)}
        p36=P(s[3],s[6],3); p45=P(s[4],s[5],3)
        out["reach_ds"].update({s[1]:1.0,s[2]:1.0,s[3]:p36,s[6]:1-p36,s[4]:p45,s[5]:1-p45})
        rc={t:0.0 for t in L}; pen={t:0.0 for t in L}
        for w45,q45 in ((s[4],p45),(s[5],1-p45)):
            for w36,q36 in ((s[3],p36),(s[6],1-p36)):
                q=q45*q36; pA=P(s[1],w45,5); pB=P(s[2],w36,5)
                for x,qx in ((s[1],pA),(w45,1-pA)):
                    for y,qy in ((s[2],pB),(w36,1-pB)):
                        qq=q*qx*qy
                        if qq==0: continue
                        rc[x]+=qq; rc[y]+=qq; pl=P(x,y,7); pen[x]+=qq*pl; pen[y]+=qq*(1-pl)
        out["reach_cs"].update(rc); out[pk]=pen
    ws={}
    for a,pa in out["al_pennant"].items():
        for n,pn in out["nl_pennant"].items():
            if pa*pn==0: continue
            p=P(a,n,7); ws[a]=ws.get(a,0)+pa*pn*p; ws[n]=ws.get(n,0)+pa*pn*(1-p)
    out["ws"]={t:ws.get(t,0.0) for t in AL_SEEDS+NL_SEEDS}
    for k in out: out[k]={t:round(v,4) for t,v in out[k].items()}
    return out

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
            "a":A,"b":B,"seed_a":SEED.get(A),"seed_b":SEED.get(B),"games":gm,"state":state,"live":decided>0,
            "wins":{"a":aw,"b":bw},
            "model":{"p_a":round(pA,4),"ml_a":american(pA),"p_b":round(1-pA,4),"ml_b":american(1-pA),
                     "exact":exact,"spread":{"a_minus_1_5":round(pAsw,4),"b_minus_1_5":round(pBsw,4)}},
            "market":{"ml_a":None,"ml_b":None,"exact":None,"spread":None}}

# ── main ─────────────────────────────────────────────────────────────────────
def main():
    if not os.path.exists(GRID):
        print(f"[series_live] no grid at {GRID}; leaving lane2_series.json as-is", file=sys.stderr); return 0
    season=datetime.date.today().year
    try:
        bd=json.load(open(os.path.join(DATA,"bracket_dist.json"))).get("bracket_dist") or []
        if bd:
            al_ids,nl_ids,_=max(bd,key=lambda x:x[2])
            for i,tid in enumerate(al_ids): SEED[TEAMAB.get(tid,str(tid))]=i+1
            for i,tid in enumerate(nl_ids): SEED[TEAMAB.get(tid,str(tid))]=i+1
    except Exception as e:
        print(f"[series_live] seed map failed: {e}", file=sys.stderr)
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
    # re-price the whole bracket (reach DS/CS, pennant, WS) conditional on results so far
    prev=json.load(open(SERIES)) if os.path.exists(SERIES) else {}
    exacta=prev.get("ws_exacta",{})
    try:
        m=rebuild_marginalized(grid,state)
        old=json.load(open(MARG)) if os.path.exists(MARG) else {}
        m.update({"engine":old.get("engine","lane2"),"source":"refresh_series_live (live bracket walk over lane2_grid)",
                  "scenario":old.get("scenario"),"as_of":datetime.datetime.utcnow().replace(microsecond=0).isoformat()+"+00:00"})
        open(MARG,"w").write(json.dumps(m,indent=1))
        pairs=[]
        for a,pa in m["al_pennant"].items():
            for n,pn in m["nl_pennant"].items():
                p=pa*pn
                if p>0: pairs.append({"al":a,"nl":n,"reach_p":round(p,4),"fair":american(p),"market":None})
        pairs.sort(key=lambda x:-x["reach_p"])
        exacta={"al_pennant":m["al_pennant"],"nl_pennant":m["nl_pennant"],"pairings":pairs}
        print("[series_live] bracket re-walked -> lane2_marginalized.json")
    except Exception as e:
        print(f"[series_live] bracket walk skipped: {e}", file=sys.stderr)
    out={"generated_at":datetime.datetime.utcnow().replace(microsecond=0).isoformat()+"+00:00",
         "engine":"lane2 live (refresh_series_live)","round":ROUND_CODE[gt],
         "round_label":ROUND_LABEL[ROUND_CODE[gt]],"series":series,
         "ws_exacta":exacta}
    open(SERIES,"w").write(json.dumps(out,indent=1))
    live=sum(1 for s in series if s["live"])
    print(f"[series_live] round={ROUND_CODE[gt]} | {len(series)} series ({live} in progress) -> lane2_series.json")
    return 0

if __name__=="__main__":
    sys.exit(main())
