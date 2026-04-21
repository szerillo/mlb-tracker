#!/usr/bin/env python3
"""
Refresh data/odds.json using The Odds API (https://the-odds-api.com).
Free tier = 500 requests/month; 1 request covers all MLB games of the day.

Produces the same shape the front-end expects:

{
  "generated_at": "2026-04-21T13:02:11Z",
  "source": "the-odds-api",
  "games": [
    {
      "game_pk": 824448,
      "matchup": "Houston Astros @ Cleveland Guardians",
      "moneyline": {
        "away": {"odds": 135, "book": "Fanatics"},
        "home": {"odds": -150, "book": "BetMGM"}
      },
      "run_line": {
        "away": {"odds": -115, "line": 1.5, "book": "DK"},
        "home": {"odds": -105, "line": -1.5, "book": "BetMGM"}
      },
      "total": {
        "over":  {"odds": -102, "line": 8.5, "book": "FanDuel"},
        "under": {"odds": -105, "line": 8.5, "book": "Fanatics"}
      }
    }
  ]
}

USAGE:
  export ODDS_API_KEY=your_key_here
  python scripts/refresh_odds.py > data/odds.json

SOURCE FALLBACK:
If ODDS_API_KEY is missing the script exits non-zero so the GitHub Action
can detect it. The Odds API issues a free key in ~30s at the-odds-api.com.
"""

from __future__ import annotations
import json
import os
import sys
import time
from datetime import datetime, timezone, timedelta
from difflib import SequenceMatcher

import requests

ODDS_API = "https://api.the-odds-api.com/v4/sports/baseball_mlb/odds"
MLB_API = "https://statsapi.mlb.com/api/v1"
UA = "Mozilla/5.0 (compatible; mlb-tracker/1.0)"

# Books we consider (NJ — matches front-end's best-price language)
BOOKS = {
    "draftkings":     "DK NJ",
    "fanduel":        "FanDuel NJ",
    "betmgm":         "BetMGM NJ",
    "williamhill_us": "Caesars NJ",   # aka Caesars
    "betrivers":      "BetRivers NJ",
    "bet365":         "bet365 NJ",
    "fanatics":       "Fanatics NJ",
}


def today_et_iso() -> str:
    et_now = datetime.now(timezone.utc) - timedelta(hours=4)
    return et_now.strftime("%Y-%m-%d")


def fetch_schedule(date_iso: str):
    url = f"{MLB_API}/schedule?sportId=1&date={date_iso}"
    r = requests.get(url, timeout=15, headers={"User-Agent": UA})
    r.raise_for_status()
    data = r.json()
    dates = data.get("dates") or []
    return dates[0]["games"] if dates else []


def fetch_odds(api_key: str):
    params = {
        "apiKey": api_key,
        "regions": "us,us2",
        "markets": "h2h,spreads,totals",
        "oddsFormat": "american",
        "dateFormat": "iso",
    }
    r = requests.get(ODDS_API, params=params, timeout=20, headers={"User-Agent": UA})
    r.raise_for_status()
    return r.json()


def best_price(bookmakers: list, market_key: str, outcome_filter=None):
    """Return the best-for-the-bettor price across allowed books.
    outcome_filter: callable(outcome_dict) -> bool (None = match anything)"""
    best = None
    best_book = None
    for bm in bookmakers:
        if bm["key"] not in BOOKS:
            continue
        for m in bm.get("markets", []):
            if m["key"] != market_key:
                continue
            for o in m.get("outcomes", []):
                if outcome_filter and not outcome_filter(o):
                    continue
                price = o.get("price")
                if price is None:
                    continue
                # American odds "best" = most positive (highest EV for bettor)
                if best is None or price > best:
                    best = price
                    best_book = BOOKS[bm["key"]]
                    best_line = o.get("point")
    if best is None:
        return None
    return {
        "odds": best,
        "book": best_book,
        **({"line": best_line} if best_line is not None else {}),
    }


def match_game(odds_game, mlb_game) -> float:
    """Fuzzy-match odds entry to MLB game by team names."""
    a1 = odds_game.get("away_team", "")
    h1 = odds_game.get("home_team", "")
    a2 = mlb_game["teams"]["away"]["team"]["name"]
    h2 = mlb_game["teams"]["home"]["team"]["name"]
    return (SequenceMatcher(None, a1, a2).ratio()
            + SequenceMatcher(None, h1, h2).ratio()) / 2


def main():
    api_key = os.environ.get("ODDS_API_KEY")
    if not api_key:
        print("ODDS_API_KEY not set. Get one at https://the-odds-api.com",
              file=sys.stderr)
        sys.exit(1)

    date_iso = sys.argv[1] if len(sys.argv) > 1 else today_et_iso()
    mlb_games = fetch_schedule(date_iso)
    odds_resp = fetch_odds(api_key)

    out_games = []
    for g in mlb_games:
        # Fuzzy-match the MLB game to an odds entry (by team names)
        scored = sorted(odds_resp, key=lambda og: match_game(og, g), reverse=True)
        oe = scored[0] if scored and match_game(scored[0], g) > 0.7 else None
        if not oe:
            out_games.append({
                "game_pk": g["gamePk"],
                "matchup": f"{g['teams']['away']['team']['name']} @ {g['teams']['home']['team']['name']}",
                "moneyline": {"away": None, "home": None},
                "run_line":  {"away": None, "home": None},
                "total":     {"over":  None, "under": None},
            })
            continue

        bookmakers = oe.get("bookmakers") or []
        away_name = oe["away_team"]
        home_name = oe["home_team"]

        ml_away = best_price(bookmakers, "h2h",
                             lambda o: o["name"] == away_name)
        ml_home = best_price(bookmakers, "h2h",
                             lambda o: o["name"] == home_name)
        rl_away = best_price(bookmakers, "spreads",
                             lambda o: o["name"] == away_name)
        rl_home = best_price(bookmakers, "spreads",
                             lambda o: o["name"] == home_name)
        total_over = best_price(bookmakers, "totals",
                                lambda o: o["name"] == "Over")
        total_under = best_price(bookmakers, "totals",
                                 lambda o: o["name"] == "Under")

        out_games.append({
            "game_pk": g["gamePk"],
            "matchup": f"{g['teams']['away']['team']['name']} @ {g['teams']['home']['team']['name']}",
            "moneyline": {"away": ml_away, "home": ml_home},
            "run_line":  {"away": rl_away, "home": rl_home},
            "total":     {"over":  total_over, "under": total_under},
        })

    out = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": "the-odds-api",
        "games": out_games,
    }
    json.dump(out, sys.stdout, indent=2)


if __name__ == "__main__":
    main()
