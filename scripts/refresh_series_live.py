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
STARTED=set()
SCHED={}   # statsapi gamePk -> {home, away, n (series game number), date, preview}
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
                if g['status'].get('abstractGameState')!='Preview': STARTED.add(str(g['gamePk']))
                SCHED[str(g['gamePk'])]={'home':h,'away':a,'n':g.get('seriesGameNumber'),'date':(g.get('officialDate') or dt.get('date')),
                                         'preview':g['status'].get('abstractGameState')=='Preview'}
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
        gs=o['games']; nx=w.get(A,0)+w.get(B,0)
        gs=_apply_sheet(gs,nx)
        p=_p_from_games(gs,A,w.get(A,0),w.get(B,0))
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

# Today's Action PRO game projection (sheet GAME UPLOADER) overrides the grid for the NEXT game of a
# series, so the series price agrees with the game card (a deciding game = exactly the card's WP).
FULLNAME={'Tampa Bay Rays':'TB','Cleveland Guardians':'CLE','Chicago White Sox':'CWS','Houston Astros':'HOU',
  'New York Yankees':'NYY','Boston Red Sox':'BOS','Milwaukee Brewers':'MIL','Los Angeles Dodgers':'LAD',
  'Atlanta Braves':'ATL','San Diego Padres':'SD','Chicago Cubs':'CHC','Philadelphia Phillies':'PHI'}
TODAY_WP={}   # (home, away, series_game_no) -> home_wp for EVERY not-yet-started game Sean has projected
SHEET_CSV=("https://docs.google.com/spreadsheets/d/e/2PACX-1vR8rC-5ro6T19a3W6mQDpwDrr5nK6supT0TVYATBk305OgcrlQqeCOlz8mPydvfEZ_XqYR96g7s816P"
           "/pub?gid=580753288&single=true&output=csv")   # Sean's published GAME UPLOADER (all upcoming dates)
def load_today_wp(started):
    """Sean's game projection overrides the grid for EVERY upcoming game he has posted (today and future
    dates), so series %, exact score, spreads and futures always match his game numbers (e.g. MIL 3-0 at
    0-2 = his G3 number). Sources: the published uploader CSV (keyed by Action Network game id, any date)
    and data/sheet_projections.json (today, keyed by statsapi gamePk). Started games are never overridden."""
    import csv, io
    by_pk={}
    try:   # repo copy (today)
        sp=json.load(open(os.path.join(DATA,"sheet_projections.json")))
        for pk,g in (sp.get("games") or {}).items():
            if g.get("home_wp") is not None: by_pk[str(pk)]=float(g["home_wp"])
    except Exception as e:
        print(f"[series_live] sheet_projections.json skipped: {e}", file=sys.stderr)
    try:   # published uploader CSV (all dates) -> AN game id -> statsapi gamePk via date + team names
        txt=urllib.request.urlopen(urllib.request.Request(SHEET_CSV,headers={'User-Agent':'x'}),timeout=20).read().decode()
        rows=[]
        for r in csv.reader(io.StringIO(txt)):
            if len(r)<7 or not r[2].strip().isdigit(): continue
            try: hwp=float(r[6])
            except Exception: continue
            d=next((c for c in r[7:] if c.count('/')==2),None)
            if not d: continue
            m,dd,y=d.split('/'); rows.append((r[2].strip(),f"{y}-{int(m):02d}-{int(dd):02d}",hwp))
        an={}
        for date in sorted({x[1] for x in rows}):
            try: sb=_get(f"https://api.actionnetwork.com/web/v2/scoreboard/mlb?bookIds=15&date={date.replace('-','')}&periods=event")
            except Exception: continue
            for x in sb.get('games',[]):
                T={t['id']:FULLNAME.get(t.get('full_name')) for t in x.get('teams',[])}
                an[str(x['id'])]=(date,T.get(x['home_team_id']),T.get(x['away_team_id']))
        for aid,date,hwp in rows:
            m_=an.get(aid)
            if not m_ or not m_[1] or not m_[2]: continue
            pk=next((p for p,v in SCHED.items() if v['home']==m_[1] and v['away']==m_[2] and v['date']==m_[0]),None)
            if pk: by_pk[pk]=hwp
    except Exception as e:
        print(f"[series_live] uploader CSV skipped: {e}", file=sys.stderr)
    for pk,hwp in by_pk.items():
        v=SCHED.get(pk)
        if not v or pk in started or not v.get('n'): continue
        TODAY_WP[(v['home'],v['away'],int(v['n']))]=hwp
    if TODAY_WP: print(f"[series_live] sheet game WP applied to {len(TODAY_WP)} upcoming games: "+", ".join(f"G{n} {a}@{h} {p:.3f}" for (h,a,n),p in sorted(TODAY_WP.items(),key=lambda x:x[0][2])))

def _apply_sheet(games, nx):
    """Copy of games with Sean's projection on every remaining game he has posted."""
    out=None
    for i in range(nx,len(games)):
        k=(games[i]['home'],games[i]['away'],i+1)
        if k in TODAY_WP:
            if out is None: out=[dict(z) for z in games]
            out[i]['home_wp']=TODAY_WP[k]; out[i]['src']='action_pro_sheet'
    return out or games

def build_series(gtype, matchup_key, grid_idx, wins):
    bo=BEST_OF[gtype]
    ent=grid_idx.get((matchup_key,bo))
    if not ent: return None
    A,games=ent
    B=[t for t in matchup_key if t!=A][0] if len(matchup_key)==2 else games[0]['away']
    aw=wins.get(A,0); bw=wins.get(B,0)
    nx=aw+bw
    games=_apply_sheet(games,nx)

    d,need=cond_dist(games,A,aw,bw)
    pA=sum(v for (a,b),v in d.items() if a==need)
    exact={}
    for (a,b),p in sorted(d.items(),key=lambda x:(-x[0][0],x[0][1])):
        exact[(f"a-{a}-{b}" if a>b else f"b-{b}-{a}")]=round(p,4)
    # series spreads: -1.5g = win by 2+, -2.5g = win by 3+ (bo5/bo7); +x.5 = 1 - opponent's -x.5
    spread={}
    for side,ix in (("a",0),("b",1)):
        for m_,lab in ((2,"1_5"),(3,"2_5")):
            if m_>need: continue
            spread[f"{side}_minus_{lab}"]=round(sum(v for k,v in d.items() if k[ix]==need and k[ix]-k[1-ix]>=m_),4)
    for lab in ("1_5","2_5"):
        if f"a_minus_{lab}" in spread:
            spread[f"a_plus_{lab}"]=round(1-spread[f"b_minus_{lab}"],4); spread[f"b_plus_{lab}"]=round(1-spread[f"a_minus_{lab}"],4)
    # total games over/under at every half line
    tg={}
    for n in range(need, 2*need-1):
        tg[str(n+0.5)]=round(sum(v for (a,b),v in d.items() if a+b>n),4)
    gm=[{"g":i+1,"home":x['home'],"away":x['away'],
         "home_sp":x.get('home_sp'),"away_sp":x.get('away_sp'),
         "home_sp_rank":x.get('home_sp_rank'),"away_sp_rank":x.get('away_sp_rank'),
         "home_wp":round(x['home_wp'],4),"src":x.get('src')}
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
                     "exact":exact,"spread":spread,"games_over":tg},
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
    load_today_wp(STARTED)
    gt=current_round(state); buckets=state[gt]
    series=[]
    nxt=ROUND_ORDER[ROUND_ORDER.index(gt)+1] if gt!='W' else None
    nb=state.get(nxt,{}) if nxt else {}
    need=BEST_OF[gt]//2+1
    # teams already through to the next round -> their decided current-round series drop off the
    # board and the next-round series (once both teams are known) shows instead
    for key,b in buckets.items():
        done=max(b['wins'].values() or [0])>=need
        if done and any(key & k2 for k2 in nb): continue
        s=build_series(gt,key,idx,b['wins'])
        if s: series.append(s)
    for key,b in nb.items():
        s=build_series(nxt,key,idx,b['wins'])
        if s: series.append(s)
    # order: round, then AL before NL, host first
    series.sort(key=lambda s:(ROUND_ORDER.index([k for k,v in ROUND_CODE.items() if v==s["round"]][0]), s["league"]!="AL", s["host"]))
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
                  "scenario":old.get("scenario")})
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
         "round_label":" / ".join(dict.fromkeys(ROUND_LABEL[s_["round"]] for s_ in series)) or ROUND_LABEL[ROUND_CODE[gt]],"series":series,
         "ws_exacta":exacta}
    open(SERIES,"w").write(json.dumps(out,indent=1))
    live=sum(1 for s in series if s["live"])
    print(f"[series_live] round={ROUND_CODE[gt]} | {len(series)} series ({live} in progress) -> lane2_series.json")
    return 0

if __name__=="__main__":
    sys.exit(main())
