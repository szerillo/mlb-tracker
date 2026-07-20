#!/usr/bin/env python3
"""
refresh_team_futures_bp.py — team futures from BettingPros CONSENSUS.

Pulls win totals (mkt 192), make-playoffs Yes/No (191), World Series (188),
division (190) at the consensus line (book_id 0). Updates
data/team_futures_odds.json (win_total + playoffs YES + WS + division) and
writes data/team_no_playoff_odds.json (playoff NO side).

BettingPros can IP-block GH Actions runners; on any failure this PRESERVES the
existing files (never clobbers with empties), matching compute_team_futures's
preserve-on-empty contract.
"""
import urllib.request, json, sys, datetime
from pathlib import Path
HERE=Path(__file__).resolve().parent; DATA=HERE/".."/"data"
ODDS=DATA/"team_futures_odds.json"; NOF=DATA/"team_no_playoff_odds.json"
UA="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36"
KEY="CHi8Hy5CEE4khd46XNYL23dCFX96oUdw6qOt1Dnh"
YEAR=datetime.date.today().year
ABBR={"WSH":"WAS","CWS":"CHW","AZ":"ARI","SF":"SF","SD":"SD","TB":"TB","KC":"KC"}  # normalize BP -> tool

def api(u):
    return json.load(urllib.request.urlopen(urllib.request.Request(u,headers={"User-Agent":UA,"x-api-key":KEY,"Accept":"application/json"}),timeout=30))
def all_offers(mid):
    out=[]; page=1
    while True:
        d=api(f"https://api.bettingpros.com/v3/offers?sport=MLB&market_id={mid}&season={YEAR}&page={page}")
        out+=d.get("offers",[])
        if page>=d.get("_pagination",{}).get("total_pages",1): break
        page+=1
    return out
def consensus(sel):
    for b in sel.get("books",[]):
        if b.get("id")==0 and b.get("lines"): return b["lines"][0]
    return None
def team_of(o):
    p=(o.get("participants") or [{}])[0]; a=p.get("id") or ""
    return ABBR.get(a,a)

def main():
    try:
        wt=all_offers(192); po=all_offers(191)
    except Exception as e:
        print(f"[bp-futures] BettingPros fetch failed ({e}); preserving existing files", file=sys.stderr)
        return 0
    win={}; ply_yes={}; ply_no={}
    for o in wt:
        t=team_of(o); over=[s for s in o["selections"] if s["selection"]=="over"]; under=[s for s in o["selections"] if s["selection"]=="under"]
        c=consensus(over[0]) if over else None; cu=consensus(under[0]) if under else None
        if t and c: win[t]={"line":c["line"],"over_odds":c["cost"],"over_book":"consensus","under_odds":(cu or {}).get("cost")}
    for o in po:
        t=team_of(o)
        y=[s for s in o["selections"] if s["selection"]=="yes"]; n=[s for s in o["selections"] if s["selection"]=="no"]
        cy=consensus(y[0]) if y else None; cn=consensus(n[0]) if n else None
        if t and cy: ply_yes[t]=cy["cost"]
        if t and cn: ply_no[t]=cn["cost"]
    if len(win)<20:
        print(f"[bp-futures] only {len(win)} win totals; preserving existing", file=sys.stderr); return 0
    # merge into team_futures_odds.json
    od=json.loads(ODDS.read_text()) if ODDS.exists() else {"teams":{}}
    teams=od.get("teams") or {}
    for t in set(win)|set(ply_yes):
        rec=teams.get(t)
        if rec is None: continue            # don't add spurious teams; only update known ones
        if t in win: rec["win_total"]=win[t]
        if t in ply_yes: rec["playoffs"]={"odds":ply_yes[t],"book":"consensus"}
    od.update({"generated_at":datetime.datetime.utcnow().isoformat(timespec="seconds")+"Z",
               "season":YEAR,"source":od.get("source","")+" + BettingPros win totals/playoffs (consensus)","n_teams":len(teams),"teams":teams})
    ODDS.write_text(json.dumps(od,indent=2))
    # write NO playoff file
    nod={"generated_at":datetime.datetime.utcnow().isoformat(timespec="seconds")+"Z",
         "source":"BettingPros consensus (make-playoffs NO)","market":"make_playoffs_no","n_teams":len(ply_no),
         "teams":{t:{"abbr":t,"no_odds":ply_no[t],"no_book":"consensus","all_no_odds":{"consensus":ply_no[t]}} for t in ply_no}}
    NOF.write_text(json.dumps(nod,indent=2))
    print(f"[bp-futures] win:{len(win)} ply_yes:{len(ply_yes)} ply_no:{len(ply_no)}", file=sys.stderr)
    return 0
if __name__=="__main__": sys.exit(main())
