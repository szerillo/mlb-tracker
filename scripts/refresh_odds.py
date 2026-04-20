"""
Scrape MLB odds from ActionNetwork (free, no API key required).

Extracts best-price moneyline, run line, and total across major US books
(DraftKings, FanDuel, BetMGM, Caesars, BetRivers, bet365, Fanatics).

Writes data/odds.json with best prices per game joined to MLB gamePk.
"""
import json, os, re, sys, datetime, urllib.request

OUTPUT = os.path.join(os.path.dirname(__file__), "..", "data", "odds.json")
URL = "https://www.actionnetwork.com/mlb/odds"

# Real sportsbooks (exclude Consensus/Open aggregators)
REAL_BOOKS = {68, 69, 71, 75, 79, 123, 2988}  # DK, FanDuel, BetRivers, BetMGM, bet365, Caesars, Fanatics

sys.path.insert(0, os.path.dirname(__file__))
from _common import skip_if_not_in_window


def fetch():
    req = urllib.request.Request(URL, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return r.read().decode("utf-8", errors="replace")


def extract_next_data(html):
    m = re.search(r'<script id="__NEXT_DATA__"[^>]*>(\{.*?\})</script>', html, re.DOTALL)
    if not m: return None
    return json.loads(m.group(1))


def is_better(cand, best, higher_is_better=True):
    """Compare American odds. Higher odds = better for bettor on that side."""
    if best is None:
        return True
    # American-odds-to-payout: +X = X/100 profit per $1, -X = 100/X profit per $1
    def payout(o):
        return o / 100 if o > 0 else 100 / abs(o)
    return payout(cand) > payout(best)


def best_market(game, market_type, side=None):
    """
    Find the best odds for a market across REAL_BOOKS.
    market_type: 'moneyline', 'spread' (run line), or 'total'
    side: 'home'/'away' for ML, 'home'/'away' for spread, 'over'/'under' for total
    """
    best = None
    for mkt_id, mkt in game.get("markets", {}).items():
        if int(mkt_id) not in REAL_BOOKS:
            continue
        entries = mkt.get("event", {}).get(market_type, [])
        for e in entries:
            if side and e.get("side") != side:
                continue
            odds = e.get("odds")
            if odds is None:
                continue
            if best is None or is_better(odds, best["odds"]):
                best = {
                    "odds": odds,
                    "value": e.get("value"),  # line (e.g. 8.5 for totals, -1.5 for spread)
                    "book_id": e.get("book_id"),
                }
    return best


def book_name(books, bid):
    b = books.get(str(bid), {})
    return b.get("display_name") or b.get("name") or f"Book {bid}"


def main():
    if skip_if_not_in_window("refresh_odds"):
        return
    print("Scraping ActionNetwork odds...")
    html = fetch()
    d = extract_next_data(html)
    if not d:
        print("  ERR: could not extract __NEXT_DATA__")
        return
    pp = d["props"]["pageProps"]
    books = pp.get("allBooks", {})
    games = pp.get("scoreboardResponse", {}).get("games", [])

    # Today's MLB games (for joining to our gamePk)
    today = datetime.date.today().isoformat()
    req = urllib.request.Request(
        f"https://statsapi.mlb.com/api/v1/schedule?sportId=1&date={today}",
        headers={"User-Agent": "u/1.0"},
    )
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            sched = json.load(r)
        mlb_games = sched.get("dates", [{}])[0].get("games", [])
    except Exception as e:
        print(f"  MLB schedule fetch failed: {e}")
        mlb_games = []

    # Build team-name → gamePk map
    team_to_pk = {}
    for g in mlb_games:
        a = g["teams"]["away"]["team"]["name"]
        h = g["teams"]["home"]["team"]["name"]
        team_to_pk[(a, h)] = g["gamePk"]

    games_out = []
    for g in games:
        away = g["teams"][0]["full_name"]
        home = g["teams"][1]["full_name"]
        pk = team_to_pk.get((away, home))
        # Fallback: try reversing (ActionNetwork sometimes has home-first)
        if pk is None:
            pk = team_to_pk.get((home, away))
            if pk:
                away, home = home, away  # correct orientation

        ml_away = best_market(g, "moneyline", side="away")
        ml_home = best_market(g, "moneyline", side="home")
        sp_away = best_market(g, "spread", side="away")
        sp_home = best_market(g, "spread", side="home")
        tot_over = best_market(g, "total", side="over")
        tot_under = best_market(g, "total", side="under")

        def _fmt(m):
            if not m: return None
            return {
                "odds": m["odds"],
                "line": m.get("value"),
                "book": book_name(books, m["book_id"]),
            }

        games_out.append({
            "game_pk": pk,
            "matchup": f"{away} @ {home}",
            "start_time": g.get("start_time"),
            "moneyline": {"away": _fmt(ml_away), "home": _fmt(ml_home)},
            "run_line": {"away": _fmt(sp_away), "home": _fmt(sp_home)},
            "total":    {"over": _fmt(tot_over), "under": _fmt(tot_under)},
        })

    payload = {
        "generated_at": datetime.datetime.utcnow().isoformat() + "Z",
        "source": "ActionNetwork (public page scrape, best across DK/FD/BM/Caesars/BetRivers/bet365/Fanatics)",
        "source_url": URL,
        "games": games_out,
    }

    os.makedirs(os.path.dirname(OUTPUT), exist_ok=True)
    with open(OUTPUT, "w") as f:
        json.dump(payload, f, indent=2)
    pk_ct = sum(1 for g in games_out if g["game_pk"])
    print(f"  wrote {len(games_out)} games ({pk_ct} matched to MLB pk)")


if __name__ == "__main__":
    main()
