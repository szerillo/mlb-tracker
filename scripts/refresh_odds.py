"""
Scrape MLB odds from ActionNetwork's gameprojections API (v2 JSON endpoint).

Previously this scraped https://www.actionnetwork.com/mlb/odds, which returns
"unsettled markets" — a mix of today's pre-game lines + yesterday's in-progress
games that haven't fully settled yet. From a GH Actions runner that meant the
page often returned mostly yesterday's games, so data/odds.json got stuck on
stale content for hours at a time.

The v2 gameprojections API takes an explicit ?date=YYYYMMDD param and returns
exactly today's slate with all major books in one shot. Public, no auth needed
for market data (PRO/edge projections require auth, but we only need ML/RL/total
here — PRO lives in the separate odds_archive via scrape_action_archive.py).

Extracts best-price moneyline, run line, and total across major US books
(DraftKings, FanDuel, BetMGM, Caesars, BetRivers, bet365, Fanatics).

Writes data/odds.json with best prices per game joined to MLB gamePk.
"""
import json, os, sys, datetime, urllib.request

OUTPUT = os.path.join(os.path.dirname(__file__), "..", "data", "odds.json")
AN_API = ("https://api.actionnetwork.com/web/v2/scoreboard/gameprojections/mlb"
          "?bookIds=15,30,68,69,71,75,79,123,2988&date={yyyymmdd}&periods=event")
MLB_API = "https://statsapi.mlb.com/api/v1/schedule?sportId=1&date={iso}"

# Real sportsbooks (exclude Consensus=15 / Open=30 aggregators)
REAL_BOOKS = {68, 69, 71, 75, 79, 123, 2988}  # DK, FanDuel, BetRivers, BetMGM, bet365, Caesars, Fanatics

# Friendly book names — AN's allBooks map isn't returned by this endpoint so we
# hardcode. These are the display_name values they use elsewhere in the UI.
BOOK_NAMES = {
    68:   "DraftKings",
    69:   "FanDuel",
    71:   "BetRivers",
    75:   "BetMGM",
    79:   "bet365",
    123:  "Caesars",
    2988: "Fanatics",
}

sys.path.insert(0, os.path.dirname(__file__))
from _common import skip_if_not_in_window


def _et_today() -> datetime.date:
    """Return the MLB business day in ET — matters around midnight when late
    West-Coast games are still finishing under yesterday's date."""
    return (datetime.datetime.now(datetime.timezone.utc)
            - datetime.timedelta(hours=4)).date()


def _http_get(url: str, timeout: int = 25) -> dict | None:
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


def is_better(cand: int, best: int) -> bool:
    """American-odds comparison: higher payout = better for the bettor."""
    def payout(o): return o / 100 if o > 0 else 100 / abs(o)
    return payout(cand) > payout(best)


def best_market(game: dict, market_type: str, side: str | None = None) -> dict | None:
    """Find best odds for a market across REAL_BOOKS.
       market_type: 'moneyline' | 'spread' | 'total'
       side: 'home'/'away' for ML/RL, 'over'/'under' for total"""
    best = None
    for mkt_id, mkt in (game.get("markets") or {}).items():
        try:
            bid = int(mkt_id)
        except (TypeError, ValueError):
            continue
        if bid not in REAL_BOOKS:
            continue
        entries = (mkt.get("event") or {}).get(market_type) or []
        for e in entries:
            if side and e.get("side") != side:
                continue
            odds = e.get("odds")
            if odds is None:
                continue
            if best is None or is_better(odds, best["odds"]):
                best = {"odds": odds, "value": e.get("value"), "book_id": bid}
    return best


def main():
    if skip_if_not_in_window("refresh_odds"):
        return
    date = _et_today()
    yyyymmdd = date.strftime("%Y%m%d")
    print(f"[refresh_odds] fetching API for date={date.isoformat()}")

    data = _http_get(AN_API.format(yyyymmdd=yyyymmdd))
    if not data:
        print("  ERR: API fetch failed; leaving data/odds.json unchanged")
        return
    games = data.get("games", []) or []
    print(f"  API returned {len(games)} games")

    # Today's MLB games for gamePk join
    sched = _http_get(MLB_API.format(iso=date.isoformat())) or {}
    mlb_games = (sched.get("dates") or [{}])[0].get("games", []) or []
    team_to_pk = {}
    for g in mlb_games:
        try:
            a = g["teams"]["away"]["team"]["name"]
            h = g["teams"]["home"]["team"]["name"]
            team_to_pk[(a, h)] = g.get("gamePk")
        except Exception:
            continue

    games_out = []
    for g in games:
        teams = g.get("teams") or []
        away = next((t for t in teams if t.get("id") == g.get("away_team_id")), {})
        home = next((t for t in teams if t.get("id") == g.get("home_team_id")), {})
        away_nm = away.get("full_name") or away.get("display_name") or ""
        home_nm = home.get("full_name") or home.get("display_name") or ""
        pk = team_to_pk.get((away_nm, home_nm))

        ml_away = best_market(g, "moneyline", side="away")
        ml_home = best_market(g, "moneyline", side="home")
        sp_away = best_market(g, "spread",    side="away")
        sp_home = best_market(g, "spread",    side="home")
        tot_over  = best_market(g, "total", side="over")
        tot_under = best_market(g, "total", side="under")

        def _fmt(m):
            if not m: return None
            return {
                "odds": m["odds"],
                "line": m.get("value"),
                "book": BOOK_NAMES.get(m["book_id"], f"Book {m['book_id']}"),
            }

        games_out.append({
            "game_pk": pk,
            "matchup": f"{away_nm} @ {home_nm}",
            "start_time": g.get("start_time"),
            "status": g.get("status_display") or g.get("status"),
            "moneyline": {"away": _fmt(ml_away), "home": _fmt(ml_home)},
            "run_line":  {"away": _fmt(sp_away), "home": _fmt(sp_home)},
            "total":     {"over": _fmt(tot_over), "under": _fmt(tot_under)},
        })

    payload = {
        "generated_at": datetime.datetime.utcnow().isoformat() + "Z",
        "source": "Action Network gameprojections API (v2) · best across DK/FD/BM/Caesars/BetRivers/bet365/Fanatics",
        "source_url": AN_API.format(yyyymmdd=yyyymmdd),
        "date": date.isoformat(),
        "n_games": len(games_out),
        "games": games_out,
    }
    os.makedirs(os.path.dirname(OUTPUT), exist_ok=True)
    with open(OUTPUT, "w") as f:
        json.dump(payload, f, indent=2)
    pk_ct = sum(1 for g in games_out if g["game_pk"])
    print(f"  wrote {len(games_out)} games ({pk_ct} matched to MLB pk) → {OUTPUT}")


if __name__ == "__main__":
    main()
