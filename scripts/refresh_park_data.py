"""
Refresh static park factors (Savant) + daily BP weather-only runs adjustments.

Writes:
  data/park_factors.json  - Savant year-to-date park factor indices per venue
  data/bp_weather.json    - BallparkPal daily weather-only runs adjustment per game
"""
import json, os, sys, re, datetime, urllib.request

SAVANT_URL = "https://baseballsavant.mlb.com/leaderboard/statcast-park-factors?type=year&year={year}&rolling=3"
BP_URL = "https://www.ballparkpal.com/Park-Factors.php"

PARK_FACTORS_OUT = os.path.join(os.path.dirname(__file__), "..", "data", "park_factors.json")
BP_WEATHER_OUT = os.path.join(os.path.dirname(__file__), "..", "data", "bp_weather.json")

sys.path.insert(0, os.path.dirname(__file__))
from _common import skip_if_not_in_window


def fetch(url):
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return r.read().decode("utf-8", errors="replace")


# ============================================================================
# Savant park factors (runs / wOBA / hardhit / etc. indexed to 100 = league avg)
# ============================================================================
def build_park_factors():
    year = datetime.date.today().year
    html = fetch(SAVANT_URL.format(year=year))
    m = re.search(r'var\s+data\s*=\s*(\[.*?\]);', html, re.DOTALL)
    if not m:
        print("  park factors: embedded data not found")
        return
    data = json.loads(m.group(1))

    # Map team ID → abbreviation + canonical name so the front-end can join.
    VENUE_TO_TEAM = {
        "Coors Field":"COL", "Chase Field":"ARI", "Target Field":"MIN",
        "Oriole Park at Camden Yards":"BAL", "Citizens Bank Park":"PHI",
        "UNIQLO Field at Dodger Stadium":"LAD", "Dodger Stadium":"LAD",
        "Rogers Centre":"TOR", "Fenway Park":"BOS",
        "Great American Ball Park":"CIN", "Comerica Park":"DET",
        "Angel Stadium":"LAA", "Yankee Stadium":"NYY",
        "Daikin Park":"HOU", "Minute Maid Park":"HOU",
        "Nationals Park":"WAS", "PNC Park":"PIT", "Truist Park":"ATL",
        "loanDepot park":"MIA", "loanDepot Park":"MIA",
        "Kauffman Stadium":"KC", "Citi Field":"NYM",
        "Rate Field":"CHW", "Guaranteed Rate Field":"CHW",
        "Progressive Field":"CLE", "Oracle Park":"SF",
        "American Family Field":"MIL", "Busch Stadium":"STL",
        "Petco Park":"SD", "Tropicana Field":"TB",
        "Wrigley Field":"CHC", "Globe Life Field":"TEX",
        "T-Mobile Park":"SEA",
        "Sutter Health Park":"ATH",
    }

    out = {}
    for row in data:
        venue = row.get("venue_name") or row.get("venue")
        code = VENUE_TO_TEAM.get(venue)
        if not code:
            # Try fuzzy fallback
            for k, v in VENUE_TO_TEAM.items():
                if k.lower() in (venue or "").lower():
                    code = v
                    break
        if not code:
            continue
        out[code] = {
            "venue": venue,
            "park_factor": int(row.get("index_runs") or 100),   # the 112/95 column
            "woba": int(row.get("index_woba") or 100),
            "wobacon": int(row.get("index_wobacon") or 100),
            "xwobacon": int(row.get("index_xwobacon") or 100),
            "bacon": int(row.get("index_bacon") or 100),
            "xbacon": int(row.get("index_xbacon") or 100),
            "hardhit": int(row.get("index_hardhit") or 100),
            "hr": int(row.get("index_hr") or 100),
            "bb": int(row.get("index_bb") or 100),
            "so": int(row.get("index_so") or 100),
            "year": row.get("key_year") or row.get("year_range"),
        }

    payload = {
        "generated_at": datetime.datetime.utcnow().isoformat() + "Z",
        "source": "Baseball Savant (statcast-park-factors, 3-year rolling)",
        "source_url": SAVANT_URL.format(year=year),
        "parks": out,
    }
    os.makedirs(os.path.dirname(PARK_FACTORS_OUT), exist_ok=True)
    with open(PARK_FACTORS_OUT, "w") as f:
        json.dump(payload, f, indent=2)
    print(f"  park_factors: {len(out)} parks written")


# ============================================================================
# BP daily weather-only runs adjustment
# ============================================================================
VENUE_TO_TEAMS = {
    # Park name -> [home_team_abbrev] for row lookup
    "Angel Stadium":"LAA", "Camden Yards":"BAL", "Fenway Park":"BOS",
    "Rate Field":"CHW", "Progressive Field":"CLE", "Kauffman Stadium":"KC",
    "Tropicana Field":"TB", "Rogers Centre":"TOR", "Yankee Stadium":"NYY",
    "Comerica Park":"DET", "Target Field":"MIN", "Chase Field":"ARI",
    "Citi Field":"NYM", "Citizens Bank Park":"PHI", "Nationals Park":"WAS",
    "LoanDepot Park":"MIA", "loanDepot park":"MIA",
    "Truist Park":"ATL", "PNC Park":"PIT", "Great American Ball Park":"CIN",
    "American Family Field":"MIL", "Busch Stadium":"STL", "Wrigley Field":"CHC",
    "Coors Field":"COL", "Dodger Stadium":"LAD", "Petco Park":"SD",
    "Oracle Park":"SF", "Minute Maid Park":"HOU", "Daikin Park":"HOU",
    "Globe Life Field":"TEX", "T-Mobile Park":"SEA",
    "Sutter Health Park":"ATH",
}


def parse_pct(s):
    m = re.search(r'([+\-]?\d+)%', s)
    return int(m.group(1)) if m else None


def build_bp_weather():
    html = fetch(BP_URL)
    rows = re.findall(r'<tr>(.*?)</tr>', html, re.DOTALL)
    games = []
    for r in rows[1:]:
        tds = re.findall(r'<td[^>]*>(.*?)</td>', r, re.DOTALL)
        vals = [re.sub(r'<[^>]+>', '', t).strip()
                   .replace('&emsp;', '').replace('&ensp;', '').replace('&nbsp;', ' ')
                   .replace('&#176;', '°')
                for t in tds]
        if len(vals) < 5:
            continue
        cell0 = vals[0]
        # "Coors Field 8:40LAD @ COL"
        m = re.search(r'^(.+?)\s*(\d+:\d+)([A-Z]{2,3}\s*@\s*[A-Z]{2,3})', cell0)
        if not m:
            continue
        venue, time, matchup = [g.strip() for g in m.groups()]
        home = None
        for k, v in VENUE_TO_TEAMS.items():
            if k.lower() in venue.lower():
                home = v; break
        # Matchup form "X @ Y" where Y is home
        ma = re.match(r'([A-Z]{2,3})\s*@\s*([A-Z]{2,3})', matchup)
        away_abbr = ma.group(1) if ma else None
        home_abbr = ma.group(2) if ma else None

        # BP table column layout (confirmed via data-column attrs on headers):
        #   [1-4]   Total factor (park + weather combined): HR, 2B/3B, 1B, Runs
        #   [29-32] Stadium ONLY factor (park, no weather): HomeRunsStadium ... FinalRunsStadium
        #   [37-40] Weather ONLY factor:                    HomeRunsWeather ... FinalRunsWeather
        games.append({
            "venue": venue,
            "home_abbr": home_abbr or home,
            "away_abbr": away_abbr,
            "time_et": time,
            "matchup": matchup,
            # Total (what the actual game environment does to runs)
            "bp_total_runs_pct":   parse_pct(vals[4]) if len(vals) > 4 else None,
            "bp_total_hr_pct":     parse_pct(vals[1]) if len(vals) > 1 else None,
            "bp_total_23b_pct":    parse_pct(vals[2]) if len(vals) > 2 else None,
            "bp_total_1b_pct":     parse_pct(vals[3]) if len(vals) > 3 else None,
            # Stadium / park-only factor (ignores today's weather)
            "bp_park_runs_pct":    parse_pct(vals[32]) if len(vals) > 32 else None,
            "bp_park_hr_pct":      parse_pct(vals[29]) if len(vals) > 29 else None,
            # Weather-only factor — THIS is the real weather adjustment
            "bp_weather_runs_pct": parse_pct(vals[40]) if len(vals) > 40 else None,
            "bp_weather_hr_pct":   parse_pct(vals[37]) if len(vals) > 37 else None,
            "bp_weather_23b_pct":  parse_pct(vals[38]) if len(vals) > 38 else None,
            "bp_weather_1b_pct":   parse_pct(vals[39]) if len(vals) > 39 else None,
            # Context
            "humidity_pct":        parse_pct(vals[15]) if len(vals) > 15 else None,
            "pressure_mb":         int(re.sub(r'[^\d]','', vals[16])) if len(vals) > 16 and re.sub(r'[^\d]','',vals[16]) else None,
        })

    payload = {
        "generated_at": datetime.datetime.utcnow().isoformat() + "Z",
        "source": "BallparkPal Park-Factors.php (public page scrape)",
        "source_url": BP_URL,
        "games": games,
    }
    os.makedirs(os.path.dirname(BP_WEATHER_OUT), exist_ok=True)
    with open(BP_WEATHER_OUT, "w") as f:
        json.dump(payload, f, indent=2)
    print(f"  bp_weather: {len(games)} games written")


def main():
    # Park factors change very slowly; only run at overnight anchors.
    # BP weather is today's game data; run when in window OR overnight anchor.
    from _common import is_overnight_anchor, should_run

    if is_overnight_anchor() or os.environ.get("FORCE_RUN"):
        print("Refreshing Savant park factors (overnight)...")
        try:
            build_park_factors()
        except Exception as e:
            print(f"  park factors failed: {e}")

    if should_run():
        print("Refreshing BP weather...")
        try:
            build_bp_weather()
        except Exception as e:
            print(f"  BP weather failed: {e}")
    else:
        print("BP weather skip: not in game window")


if __name__ == "__main__":
    main()
