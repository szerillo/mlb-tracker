"""
First-5-innings (F5) odds from ActionNetwork's gameprojections API.

Same v2 endpoint and best-price logic as refresh_odds.py, but requests the
`firstfiveinnings` period instead of `event`, so we get F5 moneyline + F5 total
per game. Writes data/f5_odds.json keyed by MLB gamePk (DH-safe: each game of a
doubleheader is matched to its own gamePk by start time, never collapsed).

"Consensus" here = best available price across the real US books (per Sean's
pick: edge is computed vs the raw best line the tool would actually bet).
"""
import json, os, sys, datetime, urllib.request

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
OUTPUT = os.path.join(REPO_ROOT, "data", "f5_odds.json")
AN_API = ("https://api.actionnetwork.com/web/v2/scoreboard/gameprojections/mlb"
          "?bookIds=15,30,68,69,71,75,79,123,2988&date={yyyymmdd}"
          "&periods=firstfiveinnings")
MLB_API = "https://statsapi.mlb.com/api/v1/schedule?sportId=1&date={iso}"
PERIOD = "firstfiveinnings"

REAL_BOOKS = {68, 69, 71, 75, 79, 123, 2988}  # DK, FanDuel, BetRivers, BetMGM, bet365, Caesars, Fanatics
BOOK_NAMES = {68: "DraftKings", 69: "FanDuel", 71: "BetRivers", 75: "BetMGM",
              79: "bet365", 123: "Caesars", 2988: "Fanatics"}


def _et_today() -> datetime.date:
    return (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=4)).date()


def _http_get(url: str, timeout: int = 25):
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": "mlb-tracker/1.0 (+github.com/szerillo/mlb-tracker)",
            "Accept": "application/json,*/*",
        })
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.load(r)
    except Exception as e:
        print(f"  http err {url}: {e}")
        return None


def _parse_dt(ts):
    if not ts:
        return None
    try:
        return datetime.datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
    except Exception:
        return None


def is_better(cand: int, best: int) -> bool:
    def payout(o): return o / 100 if o > 0 else 100 / abs(o)
    return payout(cand) > payout(best)


def best_market(game: dict, market_type: str, side: str | None = None):
    """Best F5 odds for a market across REAL_BOOKS (reads the F5 period)."""
    best = None
    for mkt_id, mkt in (game.get("markets") or {}).items():
        try:
            bid = int(mkt_id)
        except (TypeError, ValueError):
            continue
        if bid not in REAL_BOOKS:
            continue
        entries = (mkt.get(PERIOD) or {}).get(market_type) or []
        for e in entries:
            if side and e.get("side") != side:
                continue
            odds = e.get("odds")
            if odds is None:
                continue
            if best is None or is_better(odds, best["odds"]):
                best = {"odds": odds, "value": e.get("value"), "book_id": bid}
    return best

def best_total(game: dict):
    """Best F5 over/under pinned to ONE consensus line so the juice pair is
    always consistent (avoids over@7.5 / under@7.0 middles -> impossible pairs)."""
    from collections import defaultdict
    rows = []
    for mkt_id, mkt in (game.get("markets") or {}).items():
        try:
            bid = int(mkt_id)
        except (TypeError, ValueError):
            continue
        if bid not in REAL_BOOKS:
            continue
        for e in (mkt.get(PERIOD) or {}).get("total") or []:
            sd = e.get("side"); ln = e.get("value"); od = e.get("odds")
            if sd in ("over", "under") and ln is not None and od is not None:
                rows.append((bid, sd, ln, od))
    if not rows:
        return None, None
    both = defaultdict(set); allc = defaultdict(int); perbook = defaultdict(set)
    for bid, sd, ln, od in rows:
        perbook[(bid, ln)].add(sd); allc[ln] += 1
    for (bid, ln), sides in perbook.items():
        if "over" in sides and "under" in sides:
            both[ln].add(bid)
    L = max(both, key=lambda k: (len(both[k]), allc[k])) if both else max(allc, key=lambda k: allc[k])
    bo = bu = None
    for bid, sd, ln, od in rows:
        if ln != L:
            continue
        if sd == "over" and (bo is None or is_better(od, bo["odds"])):
            bo = {"odds": od, "value": L, "book_id": bid}
        elif sd == "under" and (bu is None or is_better(od, bu["odds"])):
            bu = {"odds": od, "value": L, "book_id": bid}
    return bo, bu



def _fmt(m):
    if not m:
        return None
    return {"odds": m["odds"], "line": m.get("value"),
            "book": BOOK_NAMES.get(m["book_id"], f"Book {m['book_id']}")}


def _pull(date):
    yyyymmdd = date.strftime("%Y%m%d")
    data = _http_get(AN_API.format(yyyymmdd=yyyymmdd))
    if not data:
        return None
    games = data.get("games", []) or []

    # MLB schedule for DH-safe gamePk join (keep ALL games per matchup)
    sched = _http_get(MLB_API.format(iso=date.isoformat())) or {}
    mlb_games = (sched.get("dates") or [{}])[0].get("games", []) or []
    mlb_by_teams = {}
    for g in mlb_games:
        try:
            a = g["teams"]["away"]["team"]["name"]
            h = g["teams"]["home"]["team"]["name"]
        except Exception:
            continue
        mlb_by_teams.setdefault((a, h), []).append({
            "pk": g.get("gamePk"), "dt": _parse_dt(g.get("gameDate")),
            "num": g.get("gameNumber") or 1,
        })
    for lst in mlb_by_teams.values():
        lst.sort(key=lambda x: (x["num"], x["dt"] or datetime.datetime.max))

    used = set()

    def resolve_pk(away_nm, home_nm, an_start):
        cand = [c for c in (mlb_by_teams.get((away_nm, home_nm)) or []) if c["pk"] not in used]
        if not cand:
            return None
        if len(cand) == 1:
            pk = cand[0]["pk"]
        else:
            ant = _parse_dt(an_start)
            pk = (cand[0]["pk"] if ant is None else
                  min(cand, key=lambda c: abs((c["dt"] - ant).total_seconds()) if c["dt"] else 9e18)["pk"])
        used.add(pk)
        return pk

    out = []
    for g in sorted(games, key=lambda x: str(x.get("start_time") or "")):
        teams = g.get("teams") or []
        away = next((t for t in teams if t.get("id") == g.get("away_team_id")), {})
        home = next((t for t in teams if t.get("id") == g.get("home_team_id")), {})
        away_nm = _asg_alias(away.get("full_name") or away.get("display_name") or "")
        home_nm = _asg_alias(home.get("full_name") or home.get("display_name") or "")
        pk = resolve_pk(away_nm, home_nm, g.get("start_time"))

        ml_away = best_market(g, "moneyline", side="away")
        ml_home = best_market(g, "moneyline", side="home")
        tot_over, tot_under = best_total(g)
        # skip games with no F5 markets at all
        if not any([ml_away, ml_home, tot_over, tot_under]):
            continue
        out.append({
            "game_pk": pk,
            "an_event_id": g.get("id"),
            "matchup": f"{away_nm} @ {home_nm}",
            "start_time": g.get("start_time"),
            "moneyline": {"away": _fmt(ml_away), "home": _fmt(ml_home)},
            "total": {"over": _fmt(tot_over), "under": _fmt(tot_under)},
        })
    return out


# Action Network calls the All-Star squads "American League" / "National League";
# MLB's schedule calls them "American League All-Stars" / "National League
# All-Stars". The odds join is an exact (away_name, home_name) match against the
# MLB schedule, so the ASG landed with game_pk: null and its odds/edges never
# reached the front end. Normalize AN's names to the MLB form.
def _asg_alias(n):
    s = (n or "").strip()
    if s in ("American League", "National League"):
        return s + " All-Stars"
    return s


def main():
    date = _et_today()
    games = _pull(date)
    if games is None:
        print("[f5_odds] fetch failed; leaving previous f5_odds.json intact")
        return 0

    # Also pull TOMORROW's slate, exactly as refresh_odds.py does for full-game.
    # Without this the F5 feed only ever carried the CURRENT ET day, so next-day
    # F5 lines never reached the tool — and on an off-day (the All-Star break,
    # where today has zero games and the only game is tomorrow) f5_odds.json came
    # out completely empty, so the F5 toggle showed "No F5 ML edge / No F5 total
    # edge" even though the books had the market hung.
    _tom = date + datetime.timedelta(days=1)
    _tom_games = _pull(_tom) or []
    if _tom_games:
        print(f"[f5_odds]  +{len(_tom_games)} next-day F5 games for {_tom.isoformat()}")
        games += _tom_games

    pk_ct = sum(1 for g in games if g["game_pk"])
    payload = {
        "generated_at": datetime.datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "source": "ActionNetwork gameprojections (firstfiveinnings, best price / real books)",
        "date": date.isoformat(),
        "n_games": len(games),
        "games": games,
    }
    os.makedirs(os.path.dirname(OUTPUT), exist_ok=True)
    with open(OUTPUT, "w") as f:
        json.dump(payload, f, indent=2)
    print(f"[f5_odds] wrote {len(games)} F5 games ({pk_ct} matched to pk) -> {OUTPUT}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
