#!/usr/bin/env python3
"""
Lane 2 — native playoff bracket on the game-sim engine (Fable 2026-09-25 ruling).

Wraps compute_pro_projections.project_matchup in the actual bracket: WC bo3 (all at
higher seed) / LDS bo5 (2-2-1) / LCS·WS bo7 (2-3-2), rotation advancing per game
(SP_i = game % 4, aces on regular rest), full RESTED pens for every speculative game
(no depletion mechanism — §3), weather=ump=0, tilt=0 for unannounced SPs. Marginalizes
over data/bracket_dist.json (the season sim's seeding distribution, same object v2 uses).

Output: data/lane2_marginalized.json — same shape as v2_marginalized.json
(ws / al_pennant / nl_pennant / reach_cs / reach_ds), so the board toggle reads it
exactly like the v2 column. This is lane 2: logged next to v2 (lane 1, shipped), graded
on the October ledger.

Team packs (nine / rotation / pen / park) are native: projected nine = active-roster
position players by projected PA (hitters.json), catcher guaranteed, DH slotted; rotation
= top-4 SP by unified_adj with >=15 GS; pen = bullpens_rr rested. Drop a
data/projected_nine.json (Fable's cut) to override the nine for exact parity.
"""
from __future__ import annotations
import json, os, re, sys, random, urllib.request
HERE = os.path.dirname(__file__); REPO = os.path.abspath(os.path.join(HERE, ".."))
sys.path.insert(0, HERE)
import compute_pro_projections as E
import build_pro_projections as D   # reuse _norm

TEAM = {108:"LAA",109:"ARI",110:"BAL",111:"BOS",112:"CHC",113:"CIN",114:"CLE",115:"COL",
116:"DET",117:"HOU",118:"KC",119:"LAD",120:"WSH",121:"NYM",133:"ATH",134:"PIT",135:"SD",
136:"SEA",137:"SF",138:"STL",139:"TB",140:"TEX",141:"TOR",142:"MIN",143:"PHI",144:"ATL",
145:"CWS",146:"MIA",147:"NYY",158:"MIL"}
FULL = {"TB":"Tampa Bay Rays","CLE":"Cleveland Guardians","TEX":"Texas Rangers","NYY":"New York Yankees",
"BOS":"Boston Red Sox","CWS":"Chicago White Sox","MIL":"Milwaukee Brewers","LAD":"Los Angeles Dodgers",
"ATL":"Atlanta Braves","SD":"San Diego Padres","CHC":"Chicago Cubs","PHI":"Philadelphia Phillies",
"HOU":"Houston Astros","SEA":"Seattle Mariners","NYM":"New York Mets","ARI":"Arizona Diamondbacks"}

def _load(n):
    try: return json.loads(open(os.path.join(REPO,"data",n)).read())
    except Exception: return {}

def _get(url, t=15):
    return json.load(urllib.request.urlopen(urllib.request.Request(url,headers={"User-Agent":"mlb/1.0"}),timeout=t))

def _roster(team_id):
    try:
        d=_get(f"https://statsapi.mlb.com/api/v1/teams/{team_id}/roster?rosterType=active")
        return [(p["person"]["fullName"], p["position"]["abbreviation"], p["position"]["type"]) for p in d.get("roster",[])]
    except Exception: return []

def build_pack(team_id, bp, pp, ps, pens, tabl, hit, proj_nine):
    abbr=TEAM[team_id]; full=FULL.get(abbr, abbr)
    roster=_roster(team_id)
    # rotation: top-4 SP by unified_adj, >=15 GS
    sps=[]
    for nm,pos,typ in roster:
        if pos!="P": continue
        p=ps.get(D._norm(nm)) or {}
        ra=p.get("unified_adj")   # real starters only (relievers have unified_adj=None -> excluded)
        gs=(pp.get(D._norm(nm)) or {}).get("gs")
        sps.append((nm,ra,gs,p))
    # gs from statsapi season if missing — approximate via ip; keep those plausibly SP
    rot=[s for s in sps if s[1] is not None]
    rot.sort(key=lambda s:(s[1] if s[1] is not None else 9))
    rotation=[{"name":s[0],"ra":s[1],"stamina":(pp.get(D._norm(s[0])) or {}).get("stamina") or 0.58,
               "hand":"LHP" if (s[3].get("hand")=="L") else "RHP","gs":s[2],"ip":s[3].get("ip")}
              for s in rot[:4]] or [{"name":"TBD","ra":4.3,"stamina":0.58,"hand":"RHP","gs":30,"ip":180}]
    # nine
    if proj_nine.get(abbr):
        nine=[(x["name"], x.get("pos","")) for x in proj_nine[abbr]][:9]
    else:
        pos_players=[(nm,pos) for nm,pos,typ in roster if typ!="Pitcher"]
        def pa(nm):
            h=hit.get(D._norm(nm)) or {}; return h.get("pa") or h.get("pa_actual") or 0
        pos_players.sort(key=lambda x:-pa(x[0]))
        nine=[(nm,pos) for nm,pos in pos_players[:9]]
        if nine and not any(p.upper()=="DH" for _,p in nine):
            nine[-1]=(nine[-1][0],"DH")
    # pen: rested (all non-LIKELY_OUT arms, exclude none — rotation SPs already separate)
    arms=[]
    row=D._team_key(full,pens) or []
    rot_names={D._norm(r["name"]) for r in rotation}
    for a in row:
        nm=a.get("player")
        if not nm or str(a.get("state","")).upper()=="LIKELY_OUT" or D._norm(nm) in rot_names: continue
        pr=ps.get(D._norm(nm)) or {}; ra=pr.get("unified_score"); kb=(pp.get(D._norm(nm)) or {}).get("kbb")
        if ra is None or kb is None: continue
        arms.append({"ra":ra,"kbb":kb,"fatigued":False})   # rested, no depletion (§3)
    tm=tabl.get(abbr.lower()) or D._team_key(full,tabl) or {}
    return {"abbr":abbr,"full":full,"nine":nine,"rotation":rotation,"arms":arms,
            "runs_pf":tm.get("runs_pf") or 1.0,"pf_away":tm.get("pf_adj_away") or 1.0,"pf_home":tm.get("pf_adj_home") or 1.0}

def game_home_wp(home, away, h_idx, a_idx, bp, const, cache):
    key=(home["abbr"],away["abbr"],h_idx,a_idx)
    if key in cache: return cache[key]
    hsp=home["rotation"][h_idx % len(home["rotation"])]; asp=away["rotation"][a_idx % len(away["rotation"])]
    mu={"away_lineup":away["nine"],"home_lineup":home["nine"],"bp_players":bp,
        "away_sp_hand":asp["hand"],"home_sp_hand":hsp["hand"],
        "away_park_off":away["pf_away"],"home_park_off":home["pf_home"],"home_park_factor":home["runs_pf"],
        "weather_adj":0.0,"ump_favor":0.0,"away_sp":asp,"home_sp":hsp,
        "away_arms":away["arms"],"home_arms":home["arms"],"tilt_away":0.0,"tilt_home":0.0}
    wp=E.project_matchup(mu,const)["home_wp"]; cache[key]=wp; return wp

def sim_series(hi, lo, best_of, bp, const, cache, rng):
    """hi = higher seed (home advantage). Returns True if hi wins. Home pattern:
    bo3 all@hi; bo5 hi home G1,2,5; bo7 hi home G1,2,6,7."""
    need=best_of//2+1
    if best_of==3: home_is_hi=[True,True,True]
    elif best_of==5: home_is_hi=[True,True,False,False,True]
    else: home_is_hi=[True,True,False,False,False,True,True]
    hw=lw=0
    for g in range(best_of):
        hidx=(g); aidx=(g)  # rotation slot = game number, each side
        if home_is_hi[g]:
            p_home=game_home_wp(hi,lo,hidx,aidx,bp,const,cache); hi_wins = rng.random()<p_home
        else:
            p_home=game_home_wp(lo,hi,hidx,aidx,bp,const,cache); hi_wins = rng.random()>=p_home
        if hi_wins: hw+=1
        else: lw+=1
        if hw==need or lw==need: break
    return hw==need

def _american(p):
    p=min(max(p,1e-6),1-1e-6)
    return round(-100*p/(1-p)) if p>=0.5 else round(100*(1-p)/p)

def _series_dist(A, B, best_of, bp, const, cache):
    """Analytic series result distribution; higher seed A hosts per the bo pattern.
    Returns {(a_wins,b_wins): prob} over series END states, and games-to-clinch need."""
    need=best_of//2+1
    if best_of==3: home_is_A=[True,True,True]
    elif best_of==5: home_is_A=[True,True,False,False,True]
    else: home_is_A=[True,True,False,False,False,True,True]
    pw=[]
    for g in range(best_of):
        p_home = game_home_wp(A,B,g,g,bp,const,cache) if home_is_A[g] else 1-game_home_wp(B,A,g,g,bp,const,cache)
        pw.append(p_home)
    out={}
    def rec(i,a,b,pr):
        if a==need or b==need: out[(a,b)]=out.get((a,b),0.0)+pr; return
        rec(i+1,a+1,b,pr*pw[i]); rec(i+1,a,b+1,pr*(1-pw[i]))
    rec(0,0,0,1.0); return out,need

def emit_series_detail(packs, bd, cnt, N, bp, const, cache):
    """Analytic current-round (WC bo3) series detail + WS exacta -> data/lane2_series.json.
    Market slots are null; the series-odds scraper fills them. Reads the modal seeding
    realization from bracket_dist (a locked bracket is a single realization)."""
    import datetime
    al,nl,_=max(bd,key=lambda x:x[2])
    # per-team projection ranks across the playoff field: rotation wFIP (top-3 staff),
    # bullpen RA, lineup offense. Rendered in the series expander (collapsed on mobile).
    tp={}
    for tid in list(al)+list(nl):
        P=packs[tid]
        rot=sorted(P["rotation"],key=lambda r:r["ra"])[:3]
        staff=sum(r["ra"] for r in rot)/max(1,len(rot))
        try: pen=E.build_bullpen(P["arms"]).get("pen_ra")
        except Exception: pen=None
        try: off=E.build_offense_side(P["nine"],"RHP",P.get("pf_home",1.0),bp,const).get("off_fg")
        except Exception: off=None
        tp[P["abbr"]]={"rot":[{"name":r["name"],"wfip":round(r["ra"],2)} for r in rot],
                       "staff":staff,"pen":pen,"off":off}
    def _rank(k,low):
        vals=[(a,tp[a][k]) for a in tp if tp[a].get(k) is not None]
        return {a:i+1 for i,(a,_) in enumerate(sorted(vals,key=lambda x:x[1],reverse=not low))}
    rr=_rank("staff",True); pr=_rank("pen",True); orr=_rank("off",False); nfield=len(tp)
    for a in tp:
        t=tp[a]; t["rot_rank"]=rr.get(a); t["pen_rank"]=pr.get(a); t["off_rank"]=orr.get(a)
        t["staff_v"]=round(t["staff"],2); t["pen_v"]=None if t["pen"] is None else round(t["pen"],2)
        t["off_v"]=None if t["off"] is None else round(t["off"],3)
    reach_ds={}   # analytic P(reach Division Series) = P(win the WC series); byes = 1.0
    def one(seeds,i,j,league):
        A=packs[seeds[i]]; B=packs[seeds[j]]; best_of=3
        d,need=_series_dist(A,B,best_of,bp,const,cache)
        pA=sum(v for (a,b),v in d.items() if a==need)
        exact={}
        for (a,b),p in sorted(d.items(),key=lambda x:(-x[0][0],x[0][1])):
            exact[(f"a-{a}-{b}" if a>b else f"b-{b}-{a}")]=round(p,4)
        pAsw=sum(v for (a,b),v in d.items() if a==need and b==0)
        pBsw=sum(v for (a,b),v in d.items() if b==need and a==0)
        # per-game SP matchups (bo3: all at higher seed A; rotation slot = game index)
        games=[]
        for g in range(best_of):
            hsp=A["rotation"][g % len(A["rotation"])]["name"]
            asp=B["rotation"][g % len(B["rotation"])]["name"]
            hw=game_home_wp(A,B,g,g,bp,const,cache)
            games.append({"g":g+1,"home":A["abbr"],"away":B["abbr"],
                          "home_sp":hsp,"away_sp":asp,"home_wp":round(hw,4)})
        reach_ds[A["abbr"]]=round(pA,4); reach_ds[B["abbr"]]=round(1-pA,4)
        return {"round":"WC","league":league,"best_of":best_of,"host":A["abbr"],
            "a":A["abbr"],"b":B["abbr"],"seed_a":i+1,"seed_b":j+1,
            "games":games,"proj":{"n":nfield,"a":tp.get(A["abbr"]),"b":tp.get(B["abbr"])},
            "model":{"p_a":round(pA,4),"ml_a":_american(pA),"p_b":round(1-pA,4),"ml_b":_american(1-pA),
                     "exact":exact,"spread":{"a_minus_1_5":round(pAsw,4),"b_minus_1_5":round(pBsw,4)}},
            "market":{"ml_a":None,"ml_b":None,"exact":None,"spread":None}}
    series=[one(al,2,5,"AL"),one(al,3,4,"AL"),one(nl,3,4,"NL"),one(nl,2,5,"NL")]
    for byes in (al[0],al[1],nl[0],nl[1]):     # 1/2 seeds already in the DS
        reach_ds[packs[byes]["abbr"]]=1.0
    alp={a:cnt[a]["al"]/N for a in cnt if cnt[a]["al"]>0}
    nlp={a:cnt[a]["nl"]/N for a in cnt if cnt[a]["nl"]>0}
    pairings=[]
    for a,pa in alp.items():
        for n,pn in nlp.items():
            pr=pa*pn
            pairings.append({"al":a,"nl":n,"reach_p":round(pr,4),"fair":_american(pr),"market":None})
    pairings.sort(key=lambda x:-x["reach_p"])
    out={"generated_at":datetime.datetime.utcnow().replace(microsecond=0).isoformat()+"+00:00",
        "engine":"lane2 native game-sim","source":"run_lane2_bracket.emit_series_detail",
        "round":"WC","round_label":"Wild Card","series":series,
        "ws_exacta":{"al_pennant":{k:round(v,4) for k,v in alp.items()},
                     "nl_pennant":{k:round(v,4) for k,v in nlp.items()},"pairings":pairings}}
    open(os.path.join(REPO,"data","lane2_series.json"),"w").write(json.dumps(out,indent=1))
    open(os.path.join(REPO,"data","lane2_team_proj.json"),"w").write(json.dumps({"n":nfield,"teams":tp},indent=1))
    print(f"[lane2] wrote lane2_series.json | {len(series)} series, {len(pairings)} WS pairings")
    return reach_ds

def _games_for(A, B, best_of, bp, const, cache):
    """Per-game record (venue, starters, home win prob) for a series, higher seed A hosts."""
    if best_of==3: home_is_A=[True,True,True]
    elif best_of==5: home_is_A=[True,True,False,False,True]
    else: home_is_A=[True,True,False,False,False,True,True]
    games=[]
    for g in range(best_of):
        if home_is_A[g]:
            hsp=A["rotation"][g%len(A["rotation"])]["name"]; asp=B["rotation"][g%len(B["rotation"])]["name"]
            hw=game_home_wp(A,B,g,g,bp,const,cache); games.append({"g":g+1,"home":A["abbr"],"away":B["abbr"],"home_sp":hsp,"away_sp":asp,"home_wp":round(hw,4)})
        else:
            hsp=B["rotation"][g%len(B["rotation"])]["name"]; asp=A["rotation"][g%len(A["rotation"])]["name"]
            hw=game_home_wp(B,A,g,g,bp,const,cache); games.append({"g":g+1,"home":B["abbr"],"away":A["abbr"],"home_sp":hsp,"away_sp":asp,"home_wp":round(hw,4)})
    return games

# Final 2026 regular-season W% (audit 2026-09-27). WS home field is by record (cross-league),
# not by seed; same-league rounds host the higher seed.
WPCT_FINAL={'MIL':.636,'LAD':.617,'TB':.605,'ATL':.580,'NYY':.578,'SD':.562,'CHC':.549,'PHI':.543,'BOS':.537,'CLE':.525,'CWS':.519,'HOU':.500}
def emit_grid(packs, bd, cnt, N, bp, const, cache):
    """All reachable bracket matchups (analytic per-game win probs + starters) ->
    data/lane2_grid.json, in the {key: {p_a, games:[...]}} shape the live updater reads."""
    al,nl,_=max(bd,key=lambda x:x[2])
    ab=lambda tid: packs[tid]["abbr"]
    seed={};
    for i,tid in enumerate(al): seed[ab(tid)]=(i+1,"AL",packs[tid])
    for i,tid in enumerate(nl): seed[ab(tid)]=(i+1,"NL",packs[tid])
    ws={a:cnt[a]["ws"]/N for a in cnt}
    grid={}
    def add(P,Q,best_of,tag):
        A,B=(P,Q) if seed[P["abbr"]][0]<=seed[Q["abbr"]][0] else (Q,P)   # higher seed hosts
        games=_games_for(A,B,best_of,bp,const,cache)
        # p_a via DP
        need=best_of//2+1; pw=[g["home_wp"] if g["home"]==A["abbr"] else 1-g["home_wp"] for g in games]
        from functools import lru_cache
        @lru_cache(None)
        def rec(i,w,l):
            if w==need: return 1.0
            if l==need: return 0.0
            return pw[i]*rec(i+1,w+1,l)+(1-pw[i])*rec(i+1,w,l+1)
        grid[f"{A['abbr']} v {B['abbr']} bo{best_of} {tag}"]={"p_a":round(rec(0,0,0),4),"games":games}
    alp=[packs[t] for t in al]; nlp=[packs[t] for t in nl]
    # WC bo3 (real pairs): 3v6, 4v5
    for L in (alp,nlp): add(L[2],L[5],3,"a-hosts"); add(L[3],L[4],3,"a-hosts")
    # same-league pairs: LDS bo5 + LCS bo7 (any two could meet)
    for L in (alp,nlp):
        for i in range(6):
            for j in range(i+1,6):
                add(L[i],L[j],5,"a-hosts"); add(L[i],L[j],7,"a-hosts")
    # WS bo7: every AL x NL pairing (host = higher WS-prob team, record proxy)
    for a in alp:
        for n in nlp:
            A,B=(a,n) if WPCT_FINAL.get(a["abbr"],.5)>=WPCT_FINAL.get(n["abbr"],.5) else (n,a)  # WS host by record
            games=_games_for(A,B,7,bp,const,cache)
            need=4; pw=[g["home_wp"] if g["home"]==A["abbr"] else 1-g["home_wp"] for g in games]
            from functools import lru_cache
            @lru_cache(None)
            def rec(i,w,l):
                if w==need: return 1.0
                if l==need: return 0.0
                return pw[i]*rec(i+1,w+1,l)+(1-pw[i])*rec(i+1,w,l+1)
            grid[f"{A['abbr']} v {B['abbr']} bo7 ws"]={"p_a":round(rec(0,0,0),4),"games":games}
    open(os.path.join(REPO,"data","lane2_grid.json"),"w").write(json.dumps({"game_grid":grid},indent=1))
    print(f"[lane2] wrote lane2_grid.json | {len(grid)} matchups")

def main():
    bp=_load("batter_projected.json").get("players",{})
    pp=_load("pitcher_projected.json").get("pitchers",{})
    ps={D._norm(k):v for k,v in _load("pitcher_stats.json").get("pitchers",{}).items()}
    pens=_load("bullpens_rr.json").get("teams",{})
    tabl=_load("sheet_tables.json").get("teams",{})
    hit={D._norm(k):v for k,v in _load("hitters.json").get("hitters",{}).items()}
    proj_nine=_load("projected_nine.json").get("teams",{}) if os.path.exists(os.path.join(REPO,"data","projected_nine.json")) else {}
    const=_load("sheet_projections.json").get("constants") or E.DEFAULT_CONST
    # HFA held at the shipped ±4% runs (~52.5% home on even teams), per the 2026-09-27
    # audit: that matches 2023-26 data (52.8% pooled, 53.9% postseason). No override.
    bd=_load("bracket_dist.json").get("bracket_dist") or []
    if not bd:
        print("[lane2] no bracket_dist; aborting", file=sys.stderr); return 1
    # build packs for every team appearing in the distribution (cache statsapi roster pulls)
    ids=set()
    for al,nl,w in bd: ids.update(al); ids.update(nl)
    packs={}
    for tid in ids:
        try: packs[tid]=build_pack(tid,bp,pp,ps,pens,tabl,hit,proj_nine)
        except Exception as e: print(f"  pack fail {tid}: {e}",file=sys.stderr)
    wpcache={}; rng=random.Random(42)
    N=1500
    cnt={a:{"ws":0,"al":0,"nl":0,"cs":0,"ds":0} for a in TEAM.values()}
    tot_w=sum(w for _,_,w in bd)
    def bracket(seeds):  # seeds = 6 team ids by seed 1..6
        P=[packs[s] for s in seeds]
        # WC: 3v6, 4v5 (bo3, higher seed home). 1,2 bye.
        w36 = P[2] if sim_series(P[2],P[5],3,bp,const,wpcache,rng) else P[5]
        w45 = P[3] if sim_series(P[3],P[4],3,bp,const,wpcache,rng) else P[4]
        # LDS bo5: 1 vs lower-remaining, 2 vs other (reseed: 1 plays worst remaining)
        rem=sorted([w36,w45], key=lambda p: seeds.index(_id(p,seeds)))
        # 1 vs the lower seed of the two WC winners; 2 vs the higher
        low = w45 if seeds.index(_id(w45,seeds))>seeds.index(_id(w36,seeds)) else w36
        high = w36 if low is w45 else w45
        ds1 = P[0] if sim_series(P[0],low,5,bp,const,wpcache,rng) else low
        ds2 = P[1] if sim_series(P[1],high,5,bp,const,wpcache,rng) else high
        # mark reach
        for p in (P[2],P[3],P[4],P[5],P[0],P[1]): pass
        cs_teams=[ds1,ds2]
        # LCS home field = higher seed (not the bracket line); reorder before the sim
        cs_hi,cs_lo = (ds1,ds2) if seeds.index(_id(ds1,seeds))<=seeds.index(_id(ds2,seeds)) else (ds2,ds1)
        pennant = cs_hi if sim_series(cs_hi,cs_lo,7,bp,const,wpcache,rng) else cs_lo
        return {"wc":[w36,w45],"ds":[ds1,ds2],"pennant":pennant,
                "reach_ds":[P[0],P[1],w36,w45],"reach_cs":cs_teams}
    def _id(p,seeds):
        for s in seeds:
            if packs[s]["abbr"]==p["abbr"]: return s
        return seeds[0]
    for _ in range(N):
        al,nl,w=random.Random(rng.random()).choices(bd,weights=[x[2] for x in bd])[0]
        AL=bracket(al); NL=bracket(nl)
        for side,pen_key in ((AL,"al"),(NL,"nl")):
            for p in side["reach_ds"]: cnt[p["abbr"]]["ds"]+=1
            for p in side["reach_cs"]: cnt[p["abbr"]]["cs"]+=1
            cnt[side["pennant"]["abbr"]][pen_key]+=1
        # WS home field = better regular-season record (cross-league; not seed)
        ws_hi,ws_lo = (AL["pennant"],NL["pennant"]) if WPCT_FINAL.get(AL["pennant"]["abbr"],.5)>=WPCT_FINAL.get(NL["pennant"]["abbr"],.5) else (NL["pennant"],AL["pennant"])
        champ = ws_hi if sim_series(ws_hi,ws_lo,7,bp,const,wpcache,rng) else ws_lo
        cnt[champ["abbr"]]["ws"]+=1
    surv=lambda k:{a:cnt[a][k]/N for a in cnt if cnt[a][k]>0}
    out={"ws":surv("ws"),"al_pennant":surv("al"),"nl_pennant":surv("nl"),
         "reach_cs":surv("cs"),"reach_ds":surv("ds"),
         "n_sims":N,"engine":"lane2 native game-sim (project_matchup)","source":"run_lane2_bracket"}
    # series detail also returns the analytic reach_ds (= exact WC series win%), which we
    # fold back into the marginalized feed so the bracket 'WC' column matches the series
    # board exactly (removes MC-vs-analytic noise for the current round; byes = 1.0).
    try:
        rd=emit_series_detail(packs, bd, cnt, N, bp, const, wpcache)
        if rd: out["reach_ds"]={**out["reach_ds"], **rd}
    except Exception as e:
        print(f"[lane2] series_detail failed: {e}", file=sys.stderr)
    try:
        emit_grid(packs, bd, cnt, N, bp, const, wpcache)   # all-matchups grid for the live updater
    except Exception as e:
        print(f"[lane2] grid emit failed: {e}", file=sys.stderr)
    open(os.path.join(REPO,"data","lane2_marginalized.json"),"w").write(json.dumps(out,indent=1))
    top=sorted(out["ws"].items(),key=lambda x:-x[1])[:6]
    print(f"[lane2] wrote lane2_marginalized.json | top WS: "+", ".join(f"{a} {p*100:.1f}" for a,p in top))
    return 0

if __name__=="__main__":
    sys.exit(main())
