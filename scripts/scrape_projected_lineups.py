#!/usr/bin/env python3
"""
Scrape projected lineups from Rotowire (primary) with a fallback to MLB's
confirmed batting orders via the Stats API.

Output: data/lineups.json with the following shape, matching the existing
front-end expectations:

{
  "generated_at": "2026-04-21T13:02:11Z",
  "sources": ["rotowire", "mlb-statsapi"],
  "catcher_dan_flags": {},     # populated by the catcher-day-after-night logic
  "games": [
    {
      "game_pk": 824448,
      "matchup": "Houston Astros @ Cleveland Guardians",
      "away": "Houston Astros",
      "home": "Cleveland Guardians",
      "game_time": "2026-04-21T22:10:00Z",
      "lineups": {
        "away": {
          "status": "projected",                     # confirmed | projected | tbd
          "source": "Rotowire",
          "players": [
            {"order": 1, "name": "Jose Altuve", "pos": "2B", "bats": "R",
             "status": "projected", "flag": null}
          ]
        },
        "home": {...}
      }
    }
  ]
}

USAGE (standalone):
  pip install requests beautifulsoup4 unidecode
  python scripts/scrape_projected_lineups.py > data/lineups.json

In GitHub Actions, wire this into the existing morning refresh.
"""

from __future__ import annotations
import json
import os
import re
import sys
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup
from unidecode import unidecode


# ----------------------------------------------------------------------------
# CONFIG
# ----------------------------------------------------------------------------

ROTOWIRE_URL = "https://www.rotowire.com/baseball/daily-lineups.php"
MLB_API = "https://statsapi.mlb.com/api/v1"
MLB_LIVE = "https://statsapi.mlb.com/api/v1.1"
UA = "Mozilla/5.0 (compatible; mlb-tracker/1.0; +https://github.com/szerillo/mlb-tracker)"
REQ_TIMEOUT = 15

# If your existing pipeline produces catcher-day-after-night flags, load them here
CATCHER_DAN_FILE = os.environ.get("CATCHER_DAN_FILE", "data/_catcher_dan_flags.json")


# ----------------------------------------------------------------------------
# MLB STATS API (authoritative when lineups are posted)
# ----------------------------------------------------------------------------

def today_et_iso() -> str:
    # Baseball "business day" — use ET noon to avoid DST gotchas
    et_now = datetime.now(timezone.utc) - timedelta(hours=4)
    return et_now.strftime("%Y-%m-%d")


def fetch_schedule(date_iso: str) -> List[dict]:
    url = (f"{MLB_API}/schedule?sportId=1&date={date_iso}"
           "&hydrate=probablePitcher,venue,team,linescore")
    r = requests.get(url, timeout=REQ_TIMEOUT,
                     headers={"User-Agent": UA})
    r.raise_for_status()
    data = r.json()
    dates = data.get("dates") or []
    return dates[0]["games"] if dates else []


def fetch_live_lineup(game_pk: int) -> Tuple[List[dict], List[dict]]:
    """Returns (away_lineup, home_lineup). Each player dict includes name, pos,
    bats. Empty list if not yet posted."""
    url = f"{MLB_LIVE}/game/{game_pk}/feed/live"
    try:
        r = requests.get(url, timeout=REQ_TIMEOUT,
                         headers={"User-Agent": UA})
        r.raise_for_status()
        box = r.json().get("liveData", {}).get("boxscore", {})
    except Exception:
        return [], []

    def build(side: str) -> List[dict]:
        t = box.get("teams", {}).get(side, {}) or {}
        order = t.get("battingOrder") or []
        players = t.get("players") or {}
        out = []
        for i, pid in enumerate(order):
            p = players.get(f"ID{pid}") or {}
            person = p.get("person") or {}
            out.append({
                "order": i + 1,
                "name": person.get("fullName") or "",
                "pos": (p.get("position") or {}).get("abbreviation") or "",
                "bats": ((p.get("stats") or {}).get("batting") or {})
                        .get("batSide", {}).get("code") or
                        person.get("batSide", {}).get("code") or "R",
                "status": "confirmed",
                "flag": None,
            })
        return out

    return build("away"), build("home")


# ----------------------------------------------------------------------------
# ROTOWIRE SCRAPE
# ----------------------------------------------------------------------------

# Rotowire markup: the projected-lineups page contains one .lineup box per game.
# Each box has: .lineup__team-name (away / home), .lineup__list (9 players),
# each .lineup__player has position, name, batter handedness.
# Markup changes occasionally — if this breaks, inspect the page and update selectors.

def scrape_rotowire() -> Dict[str, List[dict]]:
    """Returns a mapping: "Away Team Name @ Home Team Name" -> {
        'away': [players], 'home': [players], 'status': 'projected'|'confirmed'
    }

    Confirmed vs. projected distinction: Rotowire marks confirmed lineups with
    the word 'Confirmed' in the game block header. We surface that as status.
    """
    r = requests.get(ROTOWIRE_URL, timeout=REQ_TIMEOUT,
                     headers={"User-Agent": UA})
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    out: Dict[str, List[dict]] = {}

    for box in soup.select(".lineup"):
        team_names = [el.get_text(strip=True) for el in box.select(".lineup__abbr, .lineup__team-name")]
        # Newer markup uses 'lineup__abbr' for 3-letter codes; 'lineup__team-name' for full
        # Fall back to generic team links
        if len(team_names) < 2:
            continue
        away_name = team_names[0]
        home_name = team_names[1]
        key = f"{away_name}@{home_name}"

        # Status
        status = "projected"
        hdr = box.select_one(".lineup__status")
        if hdr and "confirmed" in hdr.get_text(strip=True).lower():
            status = "confirmed"

        def parse_side(side_cls: str) -> List[dict]:
            players: List[dict] = []
            for i, el in enumerate(box.select(f".lineup__list.is-{side_cls} .lineup__player")[:9]):
                pos_el = el.select_one(".lineup__pos")
                name_el = el.select_one("a")
                bats_el = el.select_one(".lineup__bats")
                players.append({
                    "order": i + 1,
                    "name": (name_el.get_text(strip=True) if name_el else ""),
                    "pos": (pos_el.get_text(strip=True) if pos_el else ""),
                    "bats": (bats_el.get_text(strip=True) if bats_el else "R")[:1] or "R",
                    "status": status,
                    "flag": None,
                })
            return players

        out[key] = {
            "away": parse_side("visit"),
            "home": parse_side("home"),
            "status": status,
        }

    return out


# ----------------------------------------------------------------------------
# CATCHER DAY-AFTER-NIGHT
# ----------------------------------------------------------------------------

def load_catcher_dan_flags() -> Dict:
    """If your existing pipeline already writes catcher-day-after-night flags,
    load them here. Expected shape:
        { "Aaron Judge": "day after night — may sit", ... }
    The front-end uses this to render a ⚠ next to the player name.
    """
    if os.path.exists(CATCHER_DAN_FILE):
        with open(CATCHER_DAN_FILE) as f:
            return json.load(f)
    return {}


def apply_catcher_dan_flags(lineup_players: List[dict], flags: Dict) -> List[dict]:
    for p in lineup_players:
        if p["pos"] == "C" and p["name"] in flags:
            p["flag"] = f"🟡 {flags[p['name']]}"
    return lineup_players


# ----------------------------------------------------------------------------
# MERGE + OUTPUT
# ----------------------------------------------------------------------------

def norm_matchup_key(away: str, home: str) -> str:
    return f"{unidecode(away).strip()}@{unidecode(home).strip()}"


def main():
    date_iso = sys.argv[1] if len(sys.argv) > 1 else today_et_iso()
    games = fetch_schedule(date_iso)

    # Best-effort Rotowire scrape. If it fails (markup change, rate-limited,
    # etc.) we still get MLB-confirmed lineups for games that have them.
    try:
        rw = scrape_rotowire()
    except Exception as e:
        print(f"[scrape_projected_lineups] rotowire failed: {e}", file=sys.stderr)
        rw = {}

    dan_flags = load_catcher_dan_flags()
    out_games = []
    sources_used = set()

    for g in games:
        away_name = g["teams"]["away"]["team"]["name"]
        home_name = g["teams"]["home"]["team"]["name"]
        game_pk = g["gamePk"]
        game_time = g.get("gameDate")

        # 1) Try MLB confirmed lineup
        mlb_away, mlb_home = fetch_live_lineup(game_pk)

        # 2) Fallback to Rotowire projected
        rw_entry = rw.get(norm_matchup_key(away_name, home_name)) or {}

        away_players = mlb_away or rw_entry.get("away") or []
        home_players = mlb_home or rw_entry.get("home") or []

        def side_status(mlb_list, rw_list):
            if mlb_list:
                return ("confirmed", "MLB batting order")
            if rw_list:
                status = (rw_entry.get("status") or "projected")
                return (status, "Rotowire")
            return ("tbd", "awaiting")

        away_status, away_src = side_status(mlb_away, rw_entry.get("away") or [])
        home_status, home_src = side_status(mlb_home, rw_entry.get("home") or [])

        if mlb_away or mlb_home:
            sources_used.add("mlb-statsapi")
        if rw_entry:
            sources_used.add("rotowire")

        out_games.append({
            "game_pk": game_pk,
            "matchup": f"{away_name} @ {home_name}",
            "away": away_name,
            "home": home_name,
            "game_time": game_time,
            "lineups": {
                "away": {
                    "status": away_status,
                    "source": away_src,
                    "players": apply_catcher_dan_flags(away_players, dan_flags),
                },
                "home": {
                    "status": home_status,
                    "source": home_src,
                    "players": apply_catcher_dan_flags(home_players, dan_flags),
                },
            },
        })
        # Be polite to MLB API
        time.sleep(0.2)

    out = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "sources": sorted(sources_used),
        "catcher_dan_flags": dan_flags,
        "games": out_games,
    }
    json.dump(out, sys.stdout, indent=2)


if __name__ == "__main__":
    main()
