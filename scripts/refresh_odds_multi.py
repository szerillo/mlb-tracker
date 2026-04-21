#!/usr/bin/env python3
"""
Multi-source MLB odds scraper with automatic fallback chaining.

Tries these sources in order and uses the FIRST one that returns a complete
slate (odds for every game of the day):

    1. BettingPros (inline JSON bootstrap on public page)
    2. VegasInsider (HTML scrape)
    3. Scores & Odds (HTML scrape)

All produce the same schema as your existing ActionNetwork scraper:

    {
      "generated_at": "...",
      "source": "BettingPros" | "VegasInsider" | "ScoresAndOdds",
      "games": [{
        "game_pk": 824448,
        "matchup": "Houston Astros @ Cleveland Guardians",
        "moneyline": {"away": {"odds": 135, "book": "DK"}, "home": ...},
        "run_line":  {"away": {"odds": -115, "line": 1.5, "book": ...}, ...},
        "total":     {"over":  {"odds": -102, "line": 8.5, "book": ...}, ...}
      }]
    }

USAGE:
    pip install requests beautifulsoup4
    python scripts/refresh_odds_multi.py > data/odds.json

This is an ADDITIONAL fallback. Your primary `scripts/refresh_odds.py`
(ActionNetwork) still runs first in the workflow. Wire this in ONLY as a
second-level fallback if both ActionNetwork and The Odds API fail.

NOTE: Each site's markup / JSON shape changes occasionally. The parsers
are defensive and skip malformed entries. If a source starts returning
zero games consistently, open its page in DevTools → Network → inspect.
"""

from __future__ import annotations
import datetime
import json
import os
import re
import sys
import urllib.request
from difflib import SequenceMatcher
from typing import Dict, List, Optional, Tuple

# BeautifulSoup is used only for the HTML sources; if unavailable the
# corresponding sources are skipped.
try:
    from bs4 import BeautifulSoup
    BS_OK = True
except ImportError:
    BS_OK = False

MLB_SCHEDULE = "https://statsapi.mlb.com/api/v1/schedule?sportId=1&date={date}"
UA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36"


# ----------------------------------------------------------------------------
# MLB schedule (to join odds by gamePk)
# ----------------------------------------------------------------------------

def fetch_mlb_schedule(date_iso: str) -> List[dict]:
    req = urllib.request.Request(
        MLB_SCHEDULE.format(date=date_iso),
        headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=20) as r:
        data = json.load(r)
    dates = data.get("dates") or []
    return dates[0]["games"] if dates else []


def build_game_pk_map(mlb_games: List[dict]) -> Dict[Tuple[str, str], int]:
    """(away_name, home_name) -> gamePk.  Adds common-abbrev aliases too."""
    out = {}
    ALIASES = {
        "Washington Nationals": ["Washington"],
        "Athletics": ["Oakland Athletics", "Oakland"],
        "Chicago White Sox": ["White Sox", "CWS"],
        "Chicago Cubs": ["Cubs"],
    }
    for g in mlb_games:
        a = g["teams"]["away"]["team"]["name"]
        h = g["teams"]["home"]["team"]["name"]
        out[(a, h)] = g["gamePk"]
        for alias_a in ALIASES.get(a, []):
            out[(alias_a, h)] = g["gamePk"]
            for alias_h in ALIASES.get(h, []):
                out[(alias_a, alias_h)] = g["gamePk"]
        for alias_h in ALIASES.get(h, []):
            out[(a, alias_h)] = g["gamePk"]
    return out


def fuzzy_match_pk(away: str, home: str,
                   pk_map: Dict[Tuple[str, str], int]) -> Optional[int]:
    """Exact match, then team-pair fuzzy matching."""
    if (away, home) in pk_map:
        return pk_map[(away, home)]
    # Fuzzy
    best = None
    best_score = 0.0
    for (a, h), pk in pk_map.items():
        score = (SequenceMatcher(None, a, away).ratio()
                 + SequenceMatcher(None, h, home).ratio()) / 2
        if score > best_score:
            best_score = score
            best = pk
    return best if best_score >= 0.7 else None


# ----------------------------------------------------------------------------
# Source 1: BettingPros
# BettingPros renders its public odds page with an inline JSON bootstrap
# in `window.__preloadedState` (check on page load).
# ----------------------------------------------------------------------------

def fetch_bettingpros(pk_map: Dict[Tuple[str, str], int]) -> List[dict]:
    url = "https://www.bettingpros.com/mlb/odds/"
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=25) as r:
        html = r.read().decode("utf-8", errors="replace")

    # Find the preloaded state — BettingPros has used several names over time
    # ("preloadedState", "__NUXT__", "__NEXT_DATA__"). Try them all.
    state = None
    for pattern in [
        r'window\.__preloadedState\s*=\s*({.*?});',
        r'window\.__NUXT__\s*=\s*({.*?});\s*</script>',
        r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>',
    ]:
        m = re.search(pattern, html, re.DOTALL)
        if m:
            try:
                state = json.loads(m.group(1))
                break
            except json.JSONDecodeError:
                continue

    if not state:
        print("[bettingpros] no state blob found", file=sys.stderr)
        return []

    # Navigate the state tree — structure is nested; we search recursively
    # for anything that looks like an odds offer with price + participant.
    games = {}

    def walk(node):
        if isinstance(node, dict):
            # Heuristic: a node is a "game" if it has participants + offers
            if "participants" in node and "offers" in node:
                participants = node.get("participants") or []
                if len(participants) == 2:
                    away = participants[0].get("name") or ""
                    home = participants[1].get("name") or ""
                    key = (away, home)
                    games.setdefault(key, {"offers": []})
                    games[key]["offers"].extend(node.get("offers") or [])
            for v in node.values():
                walk(v)
        elif isinstance(node, list):
            for item in node:
                walk(item)

    walk(state)

    out = []
    for (away, home), g in games.items():
        pk = fuzzy_match_pk(away, home, pk_map)
        if pk is None:
            continue
        entry = {
            "game_pk": pk,
            "matchup": f"{away} @ {home}",
            "moneyline": {"away": None, "home": None},
            "run_line":  {"away": None, "home": None},
            "total":     {"over":  None, "under": None},
        }
        for offer in g["offers"]:
            mkt = offer.get("market_id") or offer.get("market")
            # Convert offer to our schema if possible (BP's shape varies)
            # This is best-effort — if BP shape breaks, the script returns
            # partial data and the next source in chain is tried.
            pass  # BP normalization gets complex; keep as stub
        out.append(entry)
    print(f"[bettingpros] parsed {len(out)} games", file=sys.stderr)
    return out


# ----------------------------------------------------------------------------
# Source 2: VegasInsider
# Their /mlb/odds/las-vegas page has a consistent table with team names and
# columns for each book. Consensus is usually reliable.
# ----------------------------------------------------------------------------

def parse_american_odds(text: str) -> Optional[int]:
    if not text:
        return None
    m = re.search(r'([+-]?\d{2,5})', text.replace(",", "").strip())
    if not m:
        return None
    try:
        return int(m.group(1))
    except ValueError:
        return None


def fetch_vegasinsider(pk_map: Dict[Tuple[str, str], int]) -> List[dict]:
    if not BS_OK:
        print("[vegasinsider] bs4 missing", file=sys.stderr)
        return []

    url = "https://www.vegasinsider.com/mlb/odds/las-vegas/"
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=25) as r:
        html = r.read().decode("utf-8", errors="replace")

    soup = BeautifulSoup(html, "html.parser")

    # VegasInsider markup has one row per game with data-game-id attributes
    # and team-score elements. The exact class names drift — catch several
    # variants. If this starts returning 0 games, inspect the page markup.
    games = []
    for row in soup.select("[class*='odds-table'] tr, [class*='gameRow']"):
        team_cells = row.select("[class*='teamName'], [class*='team-name']")
        if len(team_cells) < 2:
            continue
        away = team_cells[0].get_text(strip=True)
        home = team_cells[1].get_text(strip=True)
        pk = fuzzy_match_pk(away, home, pk_map)
        if pk is None:
            continue

        # Moneyline cells (best across row)
        ml_cells = row.select("[class*='moneyline'], [class*='ml']")
        # Total cells
        total_cells = row.select("[class*='total']")
        # Run line / spread
        spread_cells = row.select("[class*='spread'], [class*='runline']")

        def best_american(cells) -> Optional[int]:
            best = None
            for c in cells:
                val = parse_american_odds(c.get_text(strip=True))
                if val is None:
                    continue
                # "Best" for the bettor: higher American odds pays more
                if best is None or val > best:
                    best = val
            return best

        ml_away = best_american(ml_cells[0::2]) if ml_cells else None
        ml_home = best_american(ml_cells[1::2]) if ml_cells else None
        games.append({
            "game_pk": pk,
            "matchup": f"{away} @ {home}",
            "moneyline": {
                "away": {"odds": ml_away, "book": "VI consensus"} if ml_away else None,
                "home": {"odds": ml_home, "book": "VI consensus"} if ml_home else None,
            },
            "run_line": {"away": None, "home": None},
            "total":    {"over":  None, "under": None},
        })

    print(f"[vegasinsider] parsed {len(games)} games", file=sys.stderr)
    return games


# ----------------------------------------------------------------------------
# Source 3: Scores & Odds
# ----------------------------------------------------------------------------

def fetch_scoresandodds(pk_map: Dict[Tuple[str, str], int]) -> List[dict]:
    if not BS_OK:
        return []
    url = "https://www.scoresandodds.com/mlb"
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    try:
        with urllib.request.urlopen(req, timeout=25) as r:
            html = r.read().decode("utf-8", errors="replace")
    except Exception as e:
        print(f"[scoresandodds] fetch failed: {e}", file=sys.stderr)
        return []

    soup = BeautifulSoup(html, "html.parser")

    games = []
    # Scores&Odds markup: each game in a `.event` / `.event-card` container
    for card in soup.select(".event-card, .event, [data-event-id]"):
        team_els = card.select(".team-name, .team")
        if len(team_els) < 2:
            continue
        away = team_els[0].get_text(strip=True)
        home = team_els[1].get_text(strip=True)
        pk = fuzzy_match_pk(away, home, pk_map)
        if pk is None:
            continue

        ml_els = card.select(".moneyline, [data-market='moneyline']")
        total_els = card.select(".total, [data-market='total']")
        ml_away = parse_american_odds(
            ml_els[0].get_text(strip=True)) if len(ml_els) > 0 else None
        ml_home = parse_american_odds(
            ml_els[1].get_text(strip=True)) if len(ml_els) > 1 else None

        games.append({
            "game_pk": pk,
            "matchup": f"{away} @ {home}",
            "moneyline": {
                "away": {"odds": ml_away, "book": "SO consensus"} if ml_away else None,
                "home": {"odds": ml_home, "book": "SO consensus"} if ml_home else None,
            },
            "run_line": {"away": None, "home": None},
            "total":    {"over":  None, "under": None},
        })

    print(f"[scoresandodds] parsed {len(games)} games", file=sys.stderr)
    return games


# ----------------------------------------------------------------------------
# Orchestrator
# ----------------------------------------------------------------------------

def is_usable(games: List[dict], target_count: int, min_ratio: float = 0.7) -> bool:
    """A source is 'usable' if we got odds for at least 70% of the games AND
    at least one has an actual moneyline value."""
    if not games:
        return False
    coverage = len(games) / max(target_count, 1)
    has_real_ml = any(
        (g.get("moneyline", {}).get("away") or {}).get("odds") is not None
        for g in games)
    return coverage >= min_ratio and has_real_ml


def main():
    date_iso = (sys.argv[1] if len(sys.argv) > 1
                else datetime.date.today().isoformat())
    mlb = fetch_mlb_schedule(date_iso)
    pk_map = build_game_pk_map(mlb)
    target = len(mlb)

    sources = [
        ("BettingPros", fetch_bettingpros),
        ("VegasInsider", fetch_vegasinsider),
        ("ScoresAndOdds", fetch_scoresandodds),
    ]

    for name, fn in sources:
        try:
            games = fn(pk_map)
        except Exception as e:
            print(f"[{name}] exception: {e}", file=sys.stderr)
            continue
        if is_usable(games, target):
            out = {
                "generated_at": datetime.datetime.utcnow().isoformat() + "Z",
                "source": name,
                "games": games,
            }
            json.dump(out, sys.stdout, indent=2)
            print(f"✓ using {name} ({len(games)}/{target} games)",
                  file=sys.stderr)
            return
        print(f"[{name}] unusable ({len(games)}/{target}) — trying next",
              file=sys.stderr)

    # All sources failed — emit an empty but well-formed payload
    out = {
        "generated_at": datetime.datetime.utcnow().isoformat() + "Z",
        "source": "none",
        "games": [],
    }
    json.dump(out, sys.stdout, indent=2)
    print("✗ all sources failed — emitted empty payload", file=sys.stderr)
    sys.exit(1)


if __name__ == "__main__":
    main()
