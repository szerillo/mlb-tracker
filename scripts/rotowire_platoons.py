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
    "HOU","KC","LAA","LAD","MIA","MIL","MIN","NYM","NYY","ATH",
    "PHI","PIT","SD","SEA","SF","STL","TB","TEX","TOR","WSH",
]
# MLB API abbreviations (differ in a few cases)
# MLB now returns 'AZ' for Arizona (was 'ARI' historically), and 'ATH' for the
# (relocated) Athletics. Rotowire ALSO switched the A's page code to ?team=ATH —
# requesting the old ?team=OAK returns ZERO lineup blocks and Rotowire silently
# serves the alphabetically-first team (Arizona) instead, which is how the A's
# projected lineup was showing Marte/Carroll/Perdomo. Map both A's variants → ATH.
MLB_TO_RW = {"CHW": "CWS", "ATH": "ATH", "OAK": "ATH", "AZ": "ARI"}  # WSH = WSH on Rotowire (was wrongly "WAS" → fell through to ARI default)


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


_HAND_CACHE: Dict[int, Optional[str]] = {}


def _lookup_pitcher_hand(pid: int) -> Optional[str]:
    """Hit /people/{id} for the pitcher's hand. The /schedule hydration sometimes
    drops pitchHand for next-day probables (e.g. Steven Matz on a 24-hour-out
    schedule pull) — /people always has it. Cached per-process."""
    if pid in _HAND_CACHE:
        return _HAND_CACHE[pid]
    try:
        url = f"{MLB_API}/people/{pid}"
        with urllib.request.urlopen(
            urllib.request.Request(url, headers={"User-Agent": UA}), timeout=10) as r:
            data = json.load(r)
        ppl = data.get("people", [])
        hand = ppl[0].get("pitchHand", {}).get("code") if ppl else None
    except Exception:
        hand = None
    _HAND_CACHE[pid] = hand
    return hand


def probable_pitcher_hand(g, side: str) -> Optional[str]:
    """Returns 'R' or 'L' for the side's probable pitcher, if known.
    Falls back to a direct /people/{id} lookup when the schedule
    hydration didn't include pitchHand."""
    p = g["teams"][side].get("probablePitcher")
    if not p:
        return None
    hand = p.get("pitchHand", {}).get("code")
    if hand:
        return hand
    pid = p.get("id")
    return _lookup_pitcher_hand(pid) if pid else None


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

    # Lookahead: today + next 5 days (in ET) so projections populate well
    # ahead. Days where MLB hasn't set the probable SP yet stay unfilled
    # rather than wrong-handed (see hand check below).
    today_date = today_iso()
    base = datetime.date.fromisoformat(today_date)
    LOOKAHEAD_DAYS = 5
    dates = [(base + datetime.timedelta(days=i)).isoformat()
             for i in range(LOOKAHEAD_DAYS + 1)]
    mlb_games = []
    for d in dates:
        try:
            mlb_games += mlb_sched_today(d)
        except Exception as e:
            print(f"[rotowire] schedule fetch failed for {d}: {e}", file=sys.stderr)
    print(f"[rotowire] looking at {len(mlb_games)} games across {len(dates)} days "
          f"({dates[0]} → {dates[-1]})", file=sys.stderr)

    # Scrape every team's platoon lineups once (reuse across multiple games)
    print(f"[rotowire] scraping {len(RW_TEAM_CODES)} teams…", file=sys.stderr)
    team_platoons = {}
    for code in RW_TEAM_CODES:
        team_platoons[code] = scrape_team_platoons(code)

    # Guard against Rotowire's silent alphabetical-default behavior: an unknown
    # ?team= code returns ARI's page rather than 404ing. If any NON-ARI team's
    # scraped lineup is byte-identical to ARI's, the request fell through to the
    # default — discard it so we never attach Arizona's lineup to another club.
    def _names(pl):
        return tuple(p.get("name") for p in (pl or []))
    ari = team_platoons.get("ARI", {})
    ari_sig = (_names(ari.get("R")), _names(ari.get("L")))
    if ari_sig != ((), ()):
        for code, pl in list(team_platoons.items()):
            if code == "ARI":
                continue
            if (_names(pl.get("R")), _names(pl.get("L"))) == ari_sig:
                print(f"[rotowire] {code} returned ARI's default page — discarding",
                      file=sys.stderr)
                team_platoons[code] = {}

    scraped = sum(1 for d in team_platoons.values() if d.get("R") or d.get("L"))
    print(f"[rotowire] got platoons for {scraped}/{len(RW_TEAM_CODES)} teams", file=sys.stderr)

    # Index games by game_pk for quick merge (both today and tomorrow)
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

        # Opposing SP hand. If unknown, we LEAVE the lineup unfilled rather
        # than assuming RHP — guessing wrong was the original bug (e.g. Matz/
        # Messick on TB@CLE 4/27 had hand=None in schedule hydration, so both
        # teams got vs-RHP when they should've been vs-LHP).
        away_opp_hand = probable_pitcher_hand(g, "home")
        home_opp_hand = probable_pitcher_hand(g, "away")

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
            # Pick which platoon split to use. When the opposing SP hand is KNOWN
            # we use it. When it's UNKNOWN (next-day pitcher not yet announced /
            # TBD) we no longer leave the lineup blank — we fall back to the
            # team's vs-RHP default (~70% of MLB starters are RHP) so a lineup
            # always shows, clearly labeled as a TBD-based projection. The two
            # platoon splits usually differ by only 1-2 bats, and the side will
            # auto-correct once the real SP (and the confirmed lineup) post.
            opp_known = opp_hand in ("R", "L")
            tp = team_platoons.get(rw_code, {})
            use_hand = opp_hand if opp_known else "R"
            platoon = tp.get(use_hand)
            if not platoon:
                # base-rate split missing → try the other hand before giving up
                alt = "L" if use_hand == "R" else "R"
                if tp.get(alt):
                    use_hand, platoon = alt, tp.get(alt)
            if not platoon:
                continue
            # Tag the lineup with opposing hand context
            src = (f"Rotowire platoon vs. {use_hand}HP" if opp_known
                   else f"Rotowire vs. {use_hand}HP (opp SP TBD)")
            entry.setdefault("lineups", {})[side] = {
                "status": "projected",
                "source": src,
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

