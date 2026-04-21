#!/usr/bin/env python3
"""
Scrape Rotowire's /baseball/batting-orders.php page for each team's
"Default vs. RHP" and "Default vs. LHP" projected lineups.

Then map to today's MLB games: for each game, apply the BATTING team's
vs-hand lineup based on the OPPOSING starting pitcher's handedness.

Output: writes data/lineups.json preserving any MLB-confirmed lineups
that may already be present, and filling in Rotowire platoon projections
for the rest.

USAGE:
    python scripts/rotowire_platoons.py data/lineups.json > /tmp/l.json && mv /tmp/l.json data/lineups.json
"""

from __future__ import annotations
import datetime
import json
import os
import re
import sys
import urllib.request
from typing import Dict, List, Optional

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0 Safari/537.36")

MLB_API = "https://statsapi.mlb.com/api/v1"

# MLB team abbreviation → Rotowire team code (used in ?team= URL param).
# Most are the same as MLB abbreviations.
RW_TEAM_CODES = [
    "ARI","ATL","BAL","BOS","CHC","CWS","CIN","CLE","COL","DET",
    "HOU","KC","LAA","LAD","MIA","MIL","MIN","NYM","NYY","OAK",
    "PHI","PIT","SD","SEA","SF","STL","TB","TEX","TOR","WAS",
]
# MLB API abbreviations (differ in a few cases)
# MLB now returns 'AZ' for Arizona (was 'ARI' historically), and 'ATH' for Athletics.
MLB_TO_RW = {"CHW": "CWS", "WSH": "WAS", "ATH": "OAK", "AZ": "ARI"}


def fetch(url: str) -> str:
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=25) as r:
        return r.read().decode("utf-8", errors="replace")


def scrape_team_platoons(team_code: str) -> Dict[str, List[dict]]:
    """Returns {'R': [9 players for vs RHP], 'L': [9 players for vs LHP]}"""
    try:
        html = fetch(f"https://www.rotowire.com/baseball/batting-orders.php?team={team_code}")
    except Exception as e:
        print(f"[rotowire] {team_code} fetch failed: {e}", file=sys.stderr)
        return {}

    out = {}
    # Find "Default vs. RHP" and "Default vs. LHP" blocks, each followed by
    # an <ol><li>...</li></ol> containing player names.
    for want, key in [("RHP", "R"), ("LHP", "L")]:
        m = re.search(
            rf'Default vs\.\s*{want}\s*</div>\s*<ol[^>]*>([\s\S]*?)</ol>',
            html,
        )
        if not m:
            continue
        ol_html = m.group(1)
        # Extract player anchors
        players = []
        for i, pm in enumerate(re.finditer(
            r'<li[^>]*>\s*<a href="/baseball/player/([^"]+)">([^<]+)</a>\s*</li>',
            ol_html,
        )):
            slug, name = pm.group(1), pm.group(2).strip()
            players.append({
                "order": i + 1,
                "name": name,
                "pos": "",   # Rotowire default orders don't list positions
                "bats": "",  # handedness filled in later via batSide lookup
                "status": "projected",
                "flag": None,
            })
        if players:
            out[key] = players
    return out


def mlb_sched_today(date_iso: str):
    url = (f"{MLB_API}/schedule?sportId=1&date={date_iso}"
           "&hydrate=team,probablePitcher(person)")
    with urllib.request.urlopen(
        urllib.request.Request(url, headers={"User-Agent": UA}), timeout=20) as r:
        d = json.load(r)
    return (d.get("dates") or [{}])[0].get("games", [])


def probable_pitcher_hand(g, side: str) -> Optional[str]:
    """Returns 'R' or 'L' for the side's probable pitcher, if known."""
    p = g["teams"][side].get("probablePitcher")
    if not p:
        return None
    return p.get("pitchHand", {}).get("code")


def today_iso() -> str:
    et_now = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=4)
    return et_now.strftime("%Y-%m-%d")


def main():
    if len(sys.argv) < 2:
        print("Usage: rotowire_platoons.py data/lineups.json", file=sys.stderr)
        sys.exit(1)

    infile = sys.argv[1]
    with open(infile) as f:
        doc = json.load(f)

    date_iso = today_iso()
    mlb_games = mlb_sched_today(date_iso)

    # Scrape every team's platoon lineups once (reuse across multiple games)
    print(f"[rotowire] scraping {len(RW_TEAM_CODES)} teams…", file=sys.stderr)
    team_platoons = {}
    for code in RW_TEAM_CODES:
        team_platoons[code] = scrape_team_platoons(code)

    scraped = sum(1 for d in team_platoons.values() if d.get("R") or d.get("L"))
    print(f"[rotowire] got platoons for {scraped}/{len(RW_TEAM_CODES)} teams", file=sys.stderr)

    # Index today's games by game_pk for quick merge
    by_pk = {g.get("game_pk"): g for g in doc.get("games", [])}

    def mlb_to_rw(abbr):
        return MLB_TO_RW.get(abbr, abbr)

    filled = 0
    for g in mlb_games:
        pk = g["gamePk"]
        away_abbr = g["teams"]["away"]["team"].get("abbreviation", "")
        home_abbr = g["teams"]["home"]["team"].get("abbreviation", "")
        away_rw = mlb_to_rw(away_abbr)
        home_rw = mlb_to_rw(home_abbr)

        # Opposing SP hand (default to R if unknown — most pitchers are RHP)
        away_opp_hand = probable_pitcher_hand(g, "home") or "R"
        home_opp_hand = probable_pitcher_hand(g, "away") or "R"

        entry = by_pk.get(pk)
        if not entry:
            # Create a new entry for this game
            entry = {
                "game_pk": pk,
                "matchup": f"{g['teams']['away']['team']['name']} @ {g['teams']['home']['team']['name']}",
                "away": g["teams"]["away"]["team"]["name"],
                "home": g["teams"]["home"]["team"]["name"],
                "game_time": g.get("gameDate"),
                "lineups": {"away": {}, "home": {}},
            }
            doc.setdefault("games", []).append(entry)
            by_pk[pk] = entry

        for (side, rw_code, opp_hand) in [
            ("away", away_rw, away_opp_hand),
            ("home", home_rw, home_opp_hand),
        ]:
            existing = entry.get("lineups", {}).get(side) or {}
            # DON'T overwrite MLB-confirmed or Rotowire "expected" lineups
            if existing.get("players") and \
               existing.get("status") in ("confirmed", "expected"):
                continue
            platoon = team_platoons.get(rw_code, {}).get(opp_hand)
            if not platoon:
                continue
            # Tag the lineup with opposing hand context
            entry.setdefault("lineups", {})[side] = {
                "status": "projected",
                "source": f"Rotowire platoon vs. {opp_hand}HP",
                "players": platoon,
            }
            filled += 1

    doc["rotowire_platoon_at"] = datetime.datetime.now(
        datetime.timezone.utc).isoformat()
    doc["rotowire_platoon_filled"] = filled

    json.dump(doc, sys.stdout, indent=2)
    print(f"[rotowire] filled {filled} lineup sides with platoon projections",
          file=sys.stderr)


if __name__ == "__main__":
    main()

