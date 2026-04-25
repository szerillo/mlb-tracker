"""
Projected + confirmed lineups scraper.

Strategy:
  1. Scrape Rotowire daily lineups (has projected + confirmed status per game)
  2. Validate against MLB Stats API when confirmed lineups are posted
  3. Post-process: flag catcher-day-after-night (starting catcher who played a night
     game yesterday is likely to sit in today's day game)

Output: data/lineups.json
"""
import json, os, re, datetime, urllib.request
from html.parser import HTMLParser
from concurrent.futures import ThreadPoolExecutor

OUTPUT = os.path.join(os.path.dirname(__file__), "..", "data", "lineups.json")
ROTOWIRE_URL = "https://www.rotowire.com/baseball/daily-lineups.php"

# Team abbreviation / name mapping (Rotowire uses short names in various places)
ROTOWIRE_TEAM_TO_MLB = {
    # Rotowire may use full name or abbreviation; we match on last-word of team name
    # Filled in by the scraper as it encounters them. Fallback map for common short forms:
    "Dodgers":"Los Angeles Dodgers","Angels":"Los Angeles Angels","Yankees":"New York Yankees",
    "Mets":"New York Mets","Cubs":"Chicago Cubs","White Sox":"Chicago White Sox",
    "Red Sox":"Boston Red Sox","Blue Jays":"Toronto Blue Jays","Orioles":"Baltimore Orioles",
    "Rays":"Tampa Bay Rays","Guardians":"Cleveland Guardians","Tigers":"Detroit Tigers",
    "Royals":"Kansas City Royals","Twins":"Minnesota Twins","Astros":"Houston Astros",
    "Mariners":"Seattle Mariners","Rangers":"Texas Rangers","Athletics":"Athletics",
    "Phillies":"Philadelphia Phillies","Braves":"Atlanta Braves","Nationals":"Washington Nationals",
    "Marlins":"Miami Marlins","Pirates":"Pittsburgh Pirates","Reds":"Cincinnati Reds",
    "Brewers":"Milwaukee Brewers","Cardinals":"St. Louis Cardinals","Diamondbacks":"Arizona Diamondbacks",
    "Rockies":"Colorado Rockies","Padres":"San Diego Padres","Giants":"San Francisco Giants",
}


def fetch(url, timeout=30):
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read().decode("utf-8", errors="replace")


# ============================================================================
# Rotowire parser
# ============================================================================
class RotowireParser(HTMLParser):
    """Stateful HTML parser that extracts lineup structure."""

    def __init__(self):
        super().__init__()
        self.games = []           # list of {away_abbr, home_abbr, lineups: {away: [...], home: [...]}, ...}
        self._game = None
        self._side = None         # "away" or "home"
        self._lineup_status = None
        self._text_buffer = ""
        self._in_player = False
        self._in_pos = False
        self._in_bats = False
        self._in_throws = False
        self._in_highlight_name = False
        self._current_player = None
        self._current_pitcher = None
        # Per-game team-abbr capture state. Set when we see <div class="lineup__team is-visit">
        # so the NEXT <div class="lineup__abbr"> within that scope routes to away_abbr.
        # Replaces the brittle global regex-pair scheme that drifted out of alignment when
        # Rotowire's HTML had stray lineup__abbr nodes (widgets, headers, etc.).
        self._team_scope_side = None   # "away" | "home" | None
        self._in_abbr = False          # currently capturing text inside a lineup__abbr div

    def handle_starttag(self, tag, attrs):
        attrs_d = dict(attrs)
        cls = attrs_d.get("class", "")

        # New game container
        if "lineup is-mlb" in cls or "lineup is-tools" in cls:
            # Commit prior game
            self._commit_game()
            self._game = {"away_abbr": None, "home_abbr": None,
                          "lineups": {"away": [], "home": []},
                          "pitchers": {"away": None, "home": None},
                          "status": "unknown"}
            self._team_scope_side = None  # reset between games
        # The matchup-header sub-block — sets WHICH side the next abbr belongs to.
        # Rotowire markup: <div class="lineup__team is-visit"><div class="lineup__abbr">CHC</div>...</div>
        if "lineup__team" in cls and "is-visit" in cls:
            self._team_scope_side = "away"
        elif "lineup__team" in cls and "is-home" in cls:
            self._team_scope_side = "home"
        # Lineup list (player rows) — also tracks side for player ingestion.
        if cls == "lineup__list is-visit":
            self._side = "away"
            self._team_scope_side = None  # past the matchup header now
        elif cls == "lineup__list is-home":
            self._side = "home"
            self._team_scope_side = None
        # Status
        elif cls.startswith("lineup__status"):
            if "is-confirmed" in cls: self._lineup_status = "confirmed"
            elif "is-projected" in cls: self._lineup_status = "projected"
            elif "is-expected" in cls: self._lineup_status = "expected"
            else: self._lineup_status = "unknown"
        # Player row
        elif cls == "lineup__player":
            self._in_player = True
            self._current_player = {"pos": None, "name": None, "bats": None}
        elif cls == "lineup__pos":
            self._in_pos = True; self._text_buffer = ""
        elif cls == "lineup__bats":
            self._in_bats = True; self._text_buffer = ""
        elif cls == "lineup__throws":
            self._in_throws = True; self._text_buffer = ""
        elif cls == "lineup__player-highlight-name":
            self._in_highlight_name = True
            self._current_pitcher = {"name": None, "throws": None}
        # Team abbr — only capture when we're inside a lineup__team scope AND inside a current game.
        # Bare lineup__abbr divs outside a team scope (widgets, sidebars) are ignored.
        if cls.startswith("lineup__abbr") and self._game is not None and self._team_scope_side:
            self._in_abbr = True
            self._text_buffer = ""
        # Player name from <a title="...">
        if self._in_player and tag == "a" and attrs_d.get("title"):
            self._current_player["name"] = attrs_d["title"]

    def handle_endtag(self, tag):
        if self._in_pos:
            if self._game and self._side and self._current_player is not None:
                self._current_player["pos"] = self._text_buffer.strip()
            self._in_pos = False
        elif self._in_bats:
            if self._current_player is not None:
                self._current_player["bats"] = self._text_buffer.strip()
            self._in_bats = False
        elif self._in_throws:
            if self._current_pitcher is not None:
                self._current_pitcher["throws"] = self._text_buffer.strip()
            self._in_throws = False
        elif self._in_abbr:
            # Finished capturing a team abbreviation in the matchup header.
            abbr = self._text_buffer.strip()
            if self._game is not None and self._team_scope_side and abbr:
                key = f"{self._team_scope_side}_abbr"
                # Only set the FIRST abbr seen per side per game (avoid logo+text duplicates).
                if not self._game.get(key):
                    self._game[key] = abbr
            self._in_abbr = False
        if tag == "li" and self._in_player:
            if self._game and self._side and self._current_player and self._current_player.get("name"):
                self._current_player["order"] = len(self._game["lineups"][self._side]) + 1
                self._current_player["status"] = self._lineup_status or "unknown"
                self._game["lineups"][self._side].append(self._current_player)
            self._in_player = False
            self._current_player = None

    def handle_data(self, data):
        if self._in_pos or self._in_bats or self._in_throws or self._in_abbr:
            self._text_buffer += data
        if self._in_highlight_name and self._current_pitcher is not None and self._current_pitcher["name"] is None:
            name = data.strip()
            if name and not name.startswith(("0-", "1-", "2-", "3-", "4-", "5-", "6-", "7-", "8-", "9-")) and "ERA" not in name:
                self._current_pitcher["name"] = name
                if self._game and self._side:
                    self._game["pitchers"][self._side] = self._current_pitcher

    def _commit_game(self):
        if self._game and (self._game["lineups"]["away"] or self._game["lineups"]["home"]):
            self.games.append(self._game)
        self._game = None
        self._side = None
        self._team_scope_side = None

    def close(self):
        self._commit_game()


def _extract_team_names_from_html(html):
    """
    Rotowire game blocks have <div class="lineup__teams"> with visit and home team info.
    Extract ordered pairs of (away_team, home_team) so we can align to parsed lineups.
    """
    # The pattern is: <div class="lineup__abbr">TEAM</div> occurs twice per game (away then home)
    abbrs = re.findall(r'<div class="lineup__abbr">([A-Z]+)</div>', html)
    # Group into pairs
    games = []
    for i in range(0, len(abbrs) - 1, 2):
        games.append((abbrs[i], abbrs[i + 1]))
    return games


# MLB Stats API helpers
def _et_today_iso():
    """Treat the MLB business day in ET, not UTC."""
    et_now = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=4)
    return et_now.date().isoformat()


def get_today_schedule():
    """Pull today + tomorrow (ET business days) so next-day projections are
    ready during the prior evening. Also pull yesterday for catcher-DAN logic.
    """
    today = _et_today_iso()
    yesterday = (datetime.date.fromisoformat(today) - datetime.timedelta(days=1)).isoformat()
    tomorrow  = (datetime.date.fromisoformat(today) + datetime.timedelta(days=1)).isoformat()
    # Range fetch covers today + tomorrow in one call
    url = (f"https://statsapi.mlb.com/api/v1/schedule?sportId=1"
           f"&startDate={today}&endDate={tomorrow}")
    req = urllib.request.Request(url, headers={"User-Agent":"u/1.0"})
    with urllib.request.urlopen(req, timeout=30) as r:
        d = json.load(r)
    games = []
    for day in d.get("dates", []):
        for g in day.get("games", []):
            games.append({
                "game_pk": g["gamePk"],
                "away": g["teams"]["away"]["team"]["name"],
                "home": g["teams"]["home"]["team"]["name"],
                "time": g["gameDate"],
            })
    # Yesterday for catcher-DAN logic
    y_url = f"https://statsapi.mlb.com/api/v1/schedule?sportId=1&date={yesterday}"
    try:
        with urllib.request.urlopen(urllib.request.Request(y_url, headers={"User-Agent":"u/1.0"}), timeout=30) as r:
            yd = json.load(r)
        y_games = yd.get("dates", [{}])[0].get("games", [])
    except Exception:
        y_games = []
    return games, y_games


def get_confirmed_lineup(game_pk):
    """Pull confirmed starting lineup from MLB box score, if posted."""
    try:
        url = f"https://statsapi.mlb.com/api/v1/game/{game_pk}/boxscore"
        req = urllib.request.Request(url, headers={"User-Agent":"u/1.0"})
        with urllib.request.urlopen(req, timeout=20) as r:
            d = json.load(r)
    except Exception:
        return None
    out = {"away": [], "home": []}
    for side in ("away","home"):
        t = d["teams"].get(side, {})
        bo = t.get("battingOrder", [])
        for i, pid in enumerate(bo, 1):
            p = t["players"].get(f"ID{pid}", {})
            out[side].append({
                "order": i,
                "person_id": pid,
                "name": p.get("person",{}).get("fullName"),
                "pos": p.get("position",{}).get("abbreviation","?"),
                "bats": None,  # Filled in batch below
                "status": "confirmed",
            })
    if out["away"] or out["home"]:
        return out
    return None


def batch_fetch_bat_sides(all_pids):
    """Fetch batSide for many players in one call. Returns {pid: 'R'/'L'/'S'}."""
    if not all_pids:
        return {}
    ids = ",".join(str(p) for p in all_pids)
    url = f"https://statsapi.mlb.com/api/v1/people?personIds={ids}&hydrate=batSide"
    try:
        req = urllib.request.Request(url, headers={"User-Agent":"u/1.0"})
        with urllib.request.urlopen(req, timeout=30) as r:
            d = json.load(r)
    except Exception as e:
        print(f"  batSide fetch failed: {e}")
        return {}
    out = {}
    for p in d.get("people", []):
        out[p["id"]] = (p.get("batSide") or {}).get("code")
    return out


def yesterday_starting_catchers(y_games):
    """Return {team_name: catcher_name_from_start} for yesterday's games that were night games."""
    result = {}
    for g in y_games:
        pk = g["gamePk"]
        try:
            url = f"https://statsapi.mlb.com/api/v1/game/{pk}/boxscore"
            req = urllib.request.Request(url, headers={"User-Agent":"u/1.0"})
            with urllib.request.urlopen(req, timeout=20) as r:
                d = json.load(r)
        except Exception:
            continue
        # Game start time (ET)
        gt = g.get("gameDate")
        try:
            gt_dt = datetime.datetime.fromisoformat(gt.replace("Z","+00:00"))
            hour_et = (gt_dt.hour - 4) % 24  # quick EDT conversion; good enough for day/night classification
        except Exception:
            hour_et = 19  # assume night if unknown
        is_night = hour_et >= 17  # 5pm+ ET = night
        if not is_night:
            continue
        for side in ("away","home"):
            t = d["teams"].get(side, {})
            team_name = t.get("team",{}).get("name")
            bo = t.get("battingOrder", [])
            for pid in bo:
                p = t["players"].get(f"ID{pid}", {})
                if p.get("position",{}).get("abbreviation") == "C":
                    result[team_name] = p.get("person",{}).get("fullName")
                    break
    return result


def main():
    import sys
    sys.path.insert(0, os.path.dirname(__file__))
    from _common import skip_if_not_in_window
    if skip_if_not_in_window("refresh_lineups"):
        return
    print("Scraping Rotowire projected lineups...")
    html = fetch(ROTOWIRE_URL)
    parser = RotowireParser()
    parser.feed(html); parser.close()
    parsed_games = parser.games
    # Team abbrs are now captured per-game inside the parser. The legacy global
    # regex pairing (_extract_team_names_from_html) drifted out of alignment when
    # Rotowire HTML had stray lineup__abbr nodes — that was the root cause of
    # cross-game lineup contamination. The per-game capture is bound to each
    # game's <div class="lineup is-mlb"> scope so it can't cross-contaminate.
    n_with_abbrs = sum(1 for g in parsed_games if g.get("away_abbr") and g.get("home_abbr"))
    print(f"  Rotowire games parsed: {len(parsed_games)} ({n_with_abbrs} with both team abbrs captured)")

    print("Fetching MLB schedule + confirmed lineups...")
    mlb_games, y_games = get_today_schedule()
    # Pull confirmed lineups in parallel
    with ThreadPoolExecutor(max_workers=10) as ex:
        confirmed = list(ex.map(lambda g: (g["game_pk"], get_confirmed_lineup(g["game_pk"])), mlb_games))
    confirmed_by_pk = dict(confirmed)

    # Batch batSide fetch for all confirmed players
    all_pids = set()
    for _, c in confirmed:
        if c:
            for side in ("away","home"):
                for p in c.get(side, []):
                    if p.get("person_id"):
                        all_pids.add(p["person_id"])
    print(f"  fetching batSide for {len(all_pids)} players...")
    bat_sides = batch_fetch_bat_sides(list(all_pids))
    for _, c in confirmed:
        if c:
            for side in ("away","home"):
                for p in c.get(side, []):
                    p["bats"] = bat_sides.get(p.get("person_id"))

    # Catcher-DAN detection
    print("Checking catcher day-after-night...")
    y_catchers = yesterday_starting_catchers(y_games)
    dan_flags = {}  # team -> {was_yesterday_catcher: name, note: str}
    # Today's games: is it a day game (first pitch < 5pm ET)?
    for g in mlb_games:
        try:
            gt_dt = datetime.datetime.fromisoformat(g["time"].replace("Z","+00:00"))
            hour_et = (gt_dt.hour - 4) % 24
        except Exception:
            hour_et = 19
        is_day = hour_et < 17
        if not is_day:
            continue
        for team in (g["away"], g["home"]):
            y_c = y_catchers.get(team)
            if y_c:
                dan_flags[team] = {"yesterday_catcher": y_c,
                                   "note": f"Played night game yesterday — {y_c} may sit today"}

    # MLB-team-name → Rotowire-abbr mapping. Strict equality with this canonical
    # map prevents the "DET in DETROIT TIGERS = match" false-positive that was
    # letting the wrong parsed game attach to the wrong MLB game.
    NAME_TO_ABBR = {
        "Arizona Diamondbacks":"ARI", "Atlanta Braves":"ATL", "Baltimore Orioles":"BAL",
        "Boston Red Sox":"BOS", "Chicago Cubs":"CHC", "Chicago White Sox":"CWS",
        "Cincinnati Reds":"CIN", "Cleveland Guardians":"CLE", "Colorado Rockies":"COL",
        "Detroit Tigers":"DET", "Houston Astros":"HOU", "Kansas City Royals":"KC",
        "Los Angeles Angels":"LAA", "Los Angeles Dodgers":"LAD", "Miami Marlins":"MIA",
        "Milwaukee Brewers":"MIL", "Minnesota Twins":"MIN", "New York Mets":"NYM",
        "New York Yankees":"NYY", "Athletics":"OAK", "Oakland Athletics":"OAK",
        "Philadelphia Phillies":"PHI", "Pittsburgh Pirates":"PIT", "San Diego Padres":"SD",
        "Seattle Mariners":"SEA", "San Francisco Giants":"SF", "St. Louis Cardinals":"STL",
        "Tampa Bay Rays":"TB", "Texas Rangers":"TEX", "Toronto Blue Jays":"TOR",
        "Washington Nationals":"WSH",
    }
    # Some Rotowire abbrs differ from MLB API style — normalize Rotowire side too.
    RW_ABBR_NORM = {"WAS":"WSH", "KCR":"KC", "CHW":"CWS", "SDP":"SD", "SFG":"SF",
                    "TBR":"TB", "WSN":"WSH", "ATH":"OAK"}

    # Build final output
    games_out = []
    for g in mlb_games:
        pk = g["game_pk"]
        away_abbr = NAME_TO_ABBR.get(g["away"])
        home_abbr = NAME_TO_ABBR.get(g["home"])
        # Strict abbr equality match — every parsed Rotowire game now has its OWN
        # captured away_abbr/home_abbr from the matchup header (not regex-derived).
        rw = None
        if away_abbr and home_abbr:
            for pg in parsed_games:
                pg_aw = RW_ABBR_NORM.get((pg.get("away_abbr") or "").upper(),
                                         (pg.get("away_abbr") or "").upper())
                pg_hm = RW_ABBR_NORM.get((pg.get("home_abbr") or "").upper(),
                                         (pg.get("home_abbr") or "").upper())
                if pg_aw == away_abbr and pg_hm == home_abbr:
                    rw = pg
                    break

        # Preference: confirmed MLB lineup first, then Rotowire
        c = confirmed_by_pk.get(pk)
        lineups = {"away": None, "home": None}
        for side in ("away","home"):
            team = g["away"] if side == "away" else g["home"]
            if c and c.get(side):
                lineups[side] = {"status": "confirmed", "players": c[side],
                                 "source": "MLB Stats API"}
            elif rw and rw.get("lineups",{}).get(side):
                # Remap handedness / positions into our format
                players = [{
                    "order": p.get("order"),
                    "name": p.get("name"),
                    "pos": p.get("pos"),
                    "bats": p.get("bats"),
                    "status": p.get("status","projected"),
                } for p in rw["lineups"][side]]
                status = rw["lineups"][side][0].get("status","projected") if rw["lineups"][side] else "projected"
                lineups[side] = {"status": status, "players": players,
                                 "source": "Rotowire"}
            else:
                lineups[side] = None

            # Apply catcher-DAN flag
            if lineups[side] and lineups[side].get("players"):
                y_c = (dan_flags.get(team) or {}).get("yesterday_catcher")
                if y_c:
                    for p in lineups[side]["players"]:
                        if p.get("pos") == "C" and p.get("name"):
                            # Simple name compare (last-name heuristic)
                            if p["name"].split()[-1] == y_c.split()[-1]:
                                p["flag"] = "🟡 day after night — may sit"
                    lineups[side]["dan_note"] = f"Day game after {y_c} caught last night"

        games_out.append({
            "game_pk": pk,
            "matchup": f"{g['away']} @ {g['home']}",
            "away": g["away"], "home": g["home"],
            "game_time": g["time"],
            "lineups": lineups,
        })

    payload = {
        "generated_at": datetime.datetime.utcnow().isoformat() + "Z",
        "sources": ["MLB Stats API (confirmed)", "Rotowire (projected)"],
        "catcher_dan_flags": dan_flags,
        "games": games_out,
    }
    os.makedirs(os.path.dirname(OUTPUT), exist_ok=True)
    with open(OUTPUT, "w") as f:
        json.dump(payload, f, indent=2)

    # Report
    confirmed_ct = sum(1 for g in games_out for s in g["lineups"].values() if s and s["status"] == "confirmed")
    projected_ct = sum(1 for g in games_out for s in g["lineups"].values() if s and s["status"] in ("projected","expected"))
    total_slots = len(games_out) * 2
    print(f"  wrote {len(games_out)} games: {confirmed_ct} confirmed sides, {projected_ct} projected sides, {total_slots-confirmed_ct-projected_ct} missing")
    if dan_flags:
        print(f"  catcher-DAN flags: {len(dan_flags)} teams")


if __name__ == "__main__":
    main()
