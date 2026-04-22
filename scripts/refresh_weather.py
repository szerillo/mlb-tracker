"""
Refresh weather forecasts for each MLB park for today's games.

Pulls NWS hourly forecasts per park grid point, aligns to each game's first
pitch time, and stores raw weather conditions in data/weather.json.

V8 scoring (run adjustment %) is pending integration — this script currently
stores the raw weather inputs so the V8 compute function can be wired in as
a follow-up. The V8 methodology doc (V8_WEATHER_MODEL_METHODOLOGY.md) has
the full compute pseudocode.

Data source: NWS (api.weather.gov) — free, no auth required.
"""
import json, os, sys, datetime, urllib.request, urllib.error
from concurrent.futures import ThreadPoolExecutor

# Load V8 model from same scripts folder
sys.path.insert(0, os.path.dirname(__file__))
from v8_weather import compute_v8, TEAM_TO_PARK, nws_wind_to_compass

OUTPUT = os.path.join(os.path.dirname(__file__), "..", "data", "weather.json")

# NWS grid points per park (from V8 methodology doc)
NWS_GRIDS = {
    "Arizona Diamondbacks": ("PSR", 105, 60),
    "Atlanta Braves":       ("FFC", 47, 93),
    "Baltimore Orioles":    ("LWX", 109, 91),
    "Boston Red Sox":       ("BOX", 72, 80),
    "Chicago Cubs":         ("LOT", 75, 76),
    "Chicago White Sox":    ("LOT", 76, 70),
    "Cincinnati Reds":      ("ILN", 36, 38),
    "Cleveland Guardians":  ("CLE", 88, 47),
    "Colorado Rockies":     ("BOU", 61, 60),
    "Detroit Tigers":       ("DTX", 66, 34),
    "Houston Astros":       ("HGX", 56, 88),
    "Kansas City Royals":   ("EAX", 47, 49),
    "Los Angeles Angels":   ("LOX", 160, 45),
    "Los Angeles Dodgers":  ("LOX", 155, 46),
    "Miami Marlins":        ("MFL", 109, 50),
    "Milwaukee Brewers":    ("MKX", 86, 64),
    "Minnesota Twins":      ("MPX", 107, 71),
    "New York Mets":        ("OKX", 38, 38),
    "New York Yankees":     ("OKX", 33, 37),
    "Philadelphia Phillies":("PHI", 50, 74),
    "Pittsburgh Pirates":   ("PBZ", 77, 65),
    "San Diego Padres":     ("SGX", 57, 14),
    "Seattle Mariners":     ("SEW", 124, 67),
    "San Francisco Giants": ("MTR", 85, 105),
    "St. Louis Cardinals":  ("LSX", 95, 74),
    "Tampa Bay Rays":       ("TBW", 64, 89),
    "Texas Rangers":        ("FWD", 83, 107),
    "Washington Nationals": ("LWX", 96, 72),
    "Athletics":            ("STO", 41, 18),
    # Toronto is in Canada — no NWS coverage. Dome anyway.
}

# Parks with roofs. Some are permanent (TB), others retractable. For
# retractable parks we scrape the team's roof page to determine open/closed
# per game — when roof is "Open" we treat the game as outdoor and run V8.
DOMES = {"Tampa Bay Rays", "Toronto Blue Jays", "Houston Astros", "Texas Rangers",
         "Arizona Diamondbacks", "Miami Marlins", "Milwaukee Brewers"}

# Teams whose roof schedule is published on mlb.com. Parse the table and
# determine "Open" vs "Closed" per game date.
ROOF_SCHEDULE_URLS = {
    "Arizona Diamondbacks": "https://www.mlb.com/dbacks/ballpark/information/roof",
}


def fetch_roof_schedule(team_name: str, url: str, year: int):
    """Parse the team's roof schedule page. Returns {date_iso: "open"|"closed"}."""
    import re
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/121.0",
            "Accept": "text/html",
        })
        with urllib.request.urlopen(req, timeout=20) as r:
            html = r.read().decode("utf-8", errors="replace")
    except Exception as e:
        print(f"  [roof] {team_name} fetch failed: {e}")
        return {}
    m = re.search(r"<table[\s\S]+?</table>", html)
    if not m:
        return {}
    MONTHS = {"Jan":1,"Feb":2,"Mar":3,"Apr":4,"April":4,"May":5,"Jun":6,"June":6,
              "Jul":7,"July":7,"Aug":8,"August":8,"Sep":9,"Sept":9,"Oct":10,
              "Nov":11,"Dec":12}
    out = {}
    for row_m in re.finditer(r"<tr>([\s\S]*?)</tr>", m.group(0)):
        cells = [re.sub(r"<[^>]+>", "", c).strip()
                 for c in re.findall(r"<t[hd]>([\s\S]*?)</t[hd]>", row_m.group(1))]
        if len(cells) < 4: continue
        date_cell, _time, _opp, status = cells[:4]
        if status.lower() not in ("open", "closed"): continue
        # date_cell example: "Wed, April 22"
        mdate = re.search(r"(\w+)\s+(\d{1,2})", date_cell)
        if not mdate: continue
        mo_name, day = mdate.group(1), int(mdate.group(2))
        month = MONTHS.get(mo_name[:3]) or MONTHS.get(mo_name)
        if not month: continue
        iso = f"{year:04d}-{month:02d}-{day:02d}"
        out[iso] = status.lower()
    if out:
        print(f"  [roof] {team_name}: {len(out)} dates — {', '.join(f'{k} {v}' for k, v in sorted(out.items())[:5])}")
    return out


def load_all_roof_schedules(year: int):
    out = {}
    for team, url in ROOF_SCHEDULE_URLS.items():
        out[team] = fetch_roof_schedule(team, url, year)
    return out


def fetch(url, timeout=30):
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": "mlb-tracker/1.0 (github repo)",
            "Accept": "application/ld+json",
        })
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        print(f"  HTTP {e.code} for {url}")
    except Exception as e:
        print(f"  ERR {url}: {e}")
    return None


def get_forecast(office, x, y):
    url = f"https://api.weather.gov/gridpoints/{office}/{x},{y}/forecast/hourly"
    return fetch(url)


def _mlb_business_date():
    """MLB 'business day' — treat games from midnight ET onward as today.
    Runs on UTC, so subtract 4h (5h in EST) to align the 'new day' boundary
    with actual overnight ET rollover.
    """
    et_now = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=4)
    return et_now.date().isoformat()


def get_schedule_range(start_iso: str, end_iso: str):
    """Fetch all games between start_iso..end_iso (inclusive)."""
    d = fetch(
        f"https://statsapi.mlb.com/api/v1/schedule?sportId=1"
        f"&startDate={start_iso}&endDate={end_iso}"
    )
    if not d or not d.get("dates"):
        return []
    out = []
    for day in d["dates"]:
        for g in day.get("games", []):
            out.append({
                "game_pk": g["gamePk"],
                "away": g["teams"]["away"]["team"]["name"],
                "home": g["teams"]["home"]["team"]["name"],
                "game_time": g["gameDate"],
                "venue": g.get("venue", {}).get("name", ""),
                "status": g.get("status", {}).get("abstractGameState", ""),
            })
    return out


def get_today_schedule():
    """Today + tomorrow — lets users plan a day ahead without waiting for
    the overnight refresh cycle."""
    today = _mlb_business_date()
    tomorrow = (datetime.date.fromisoformat(today) + datetime.timedelta(days=1)).isoformat()
    return get_schedule_range(today, tomorrow)


def game_has_started(status: str) -> bool:
    """True once first pitch has been thrown (or game is over)."""
    if not status: return False
    return status in ("Live", "Final")


def extract_hour(forecast, target_iso):
    """Find the NWS hourly period closest to the target time and return its values."""
    if not forecast:
        return None
    # JSON-LD format flattens; geo+json nests under .properties
    periods = forecast.get("periods") or forecast.get("properties", {}).get("periods", [])
    if not periods:
        return None
    target = datetime.datetime.fromisoformat(target_iso.replace("Z", "+00:00"))
    best = min(periods,
               key=lambda p: abs(datetime.datetime.fromisoformat(
                   p["startTime"].replace("Z", "+00:00")) - target))
    # Parse wind speed (NWS returns like "10 mph" or "10 to 15 mph")
    ws_str = best.get("windSpeed", "0 mph")
    try:
        ws_val = int(ws_str.split()[0])
    except ValueError:
        # "10 to 15 mph" → use upper bound
        try:
            ws_val = int(ws_str.split()[-2])
        except Exception:
            ws_val = 0
    return {
        "temp_f": best.get("temperature"),
        "humidity_pct": best.get("relativeHumidity", {}).get("value"),
        "wind_speed_mph": ws_val,
        "wind_dir": best.get("windDirection"),
        "precip_pct": best.get("probabilityOfPrecipitation", {}).get("value") or 0,
        "short_forecast": best.get("shortForecast"),
        "start_time": best.get("startTime"),
    }


def _three_hour_trend(forecast, target_iso):
    """Return [t_-1h, t_0, t_+2h] around game time, or None."""
    if not forecast:
        return None
    periods = forecast.get("periods") or forecast.get("properties", {}).get("periods", [])
    if not periods: return None
    try:
        target = datetime.datetime.fromisoformat(target_iso.replace("Z", "+00:00"))
    except Exception:
        return None
    # Sort by start time
    parsed = []
    for p in periods:
        try:
            t = datetime.datetime.fromisoformat(p["startTime"].replace("Z", "+00:00"))
            parsed.append((t, p.get("temperature")))
        except Exception:
            continue
    parsed.sort(key=lambda x: x[0])
    # Pick 3 hours starting at game time
    result = []
    for t, temp in parsed:
        if t >= target and temp is not None:
            result.append(temp)
            if len(result) >= 3:
                break
    return result if len(result) >= 2 else None


def main():
    from _common import skip_if_not_in_window
    if skip_if_not_in_window("refresh_weather"):
        return
    now = datetime.datetime.utcnow().isoformat() + "Z"
    schedule = get_today_schedule()
    print(f"Fetching weather for {len(schedule)} games...")

    # Preserve any game that's already in progress / final — its weather
    # snapshot at first pitch is what matters; later forecasts would just
    # drift. Read the existing file and index by game_pk.
    prior_by_pk = {}
    if os.path.exists(OUTPUT):
        try:
            with open(OUTPUT) as f:
                prior = json.load(f)
            for g in prior.get("games", []):
                prior_by_pk[g.get("game_pk")] = g
        except Exception as e:
            print(f"  could not read prior weather: {e}")

    # Retractable roof status per date (currently ARI only — only mlb.com
    # page that exposes a public schedule). If a game's date is tagged "open"
    # we override the DOMES check and treat as outdoor.
    year = datetime.date.today().year
    roof_by_team = load_all_roof_schedules(year)

    def is_roof_open(home, game_date_iso):
        sched = roof_by_team.get(home, {})
        return sched.get(game_date_iso) == "open"

    # Fetch forecasts in parallel (one per unique grid point). Include teams
    # whose roofs are OPEN today even if they're in DOMES.
    unique_teams = set()
    for g in schedule:
        if g["home"] not in NWS_GRIDS: continue
        if game_has_started(g.get("status", "")): continue
        # ET date of this game (for roof-schedule lookup)
        try:
            gd_et = (datetime.datetime.fromisoformat(g["game_time"].replace("Z","+00:00"))
                     - datetime.timedelta(hours=4)).date().isoformat()
        except Exception:
            gd_et = None
        if g["home"] not in DOMES or (gd_et and is_roof_open(g["home"], gd_et)):
            unique_teams.add(g["home"])
    forecasts = {}

    def _load(team):
        grid = NWS_GRIDS[team]
        return team, get_forecast(*grid)

    with ThreadPoolExecutor(max_workers=10) as ex:
        for team, fc in ex.map(_load, unique_teams):
            forecasts[team] = fc

    games_out = []
    frozen = 0
    roof_open_count = 0
    for g in schedule:
        home = g["home"]
        # FREEZE — if the game has started, reuse the prior snapshot verbatim
        if game_has_started(g.get("status", "")) and g["game_pk"] in prior_by_pk:
            games_out.append(prior_by_pk[g["game_pk"]])
            frozen += 1
            continue
        # ET date for roof-schedule lookup
        try:
            gd_et = (datetime.datetime.fromisoformat(g["game_time"].replace("Z","+00:00"))
                     - datetime.timedelta(hours=4)).date().isoformat()
        except Exception:
            gd_et = None
        roof_open = gd_et and is_roof_open(home, gd_et)
        # If this is a retractable-roof park but roof is OPEN for the date,
        # fall through to normal forecast/V8 path.
        if home in DOMES and not roof_open:
            games_out.append({
                "game_pk": g["game_pk"],
                "matchup": f"{g['away']} @ {home}",
                "venue": g["venue"],
                "game_time": g["game_time"],
                "is_dome": True,
                "weather": None,
                "note": "Dome / retractable roof — weather adjustment minimal",
            })
            continue
        if roof_open:
            roof_open_count += 1
        fc = forecasts.get(home)
        hour = extract_hour(fc, g["game_time"])
        # Compute V8 if weather available
        v8 = None
        park_code = TEAM_TO_PARK.get(home)
        if hour and park_code:
            # Try to get a simple 3-hour trend around game time
            t_hours = _three_hour_trend(fc, g["game_time"])
            wx_in = {
                "t": hour.get("temp_f"),
                "hum": hour.get("humidity_pct"),
                "ws": hour.get("wind_speed_mph") or 0,
                "wd_compass": nws_wind_to_compass(hour.get("wind_dir")),
                "precip": hour.get("precip_pct") or 0,
                "t_hours": t_hours,
            }
            v8 = compute_v8(park_code, wx_in)
        games_out.append({
            "game_pk": g["game_pk"],
            "matchup": f"{g['away']} @ {home}",
            "venue": g["venue"],
            "game_time": g["game_time"],
            "is_dome": False,
            "roof_open": True if roof_open else None,
            "weather": hour,
            "v8": v8,
            "note": None if hour else "NWS forecast unavailable",
        })

    payload = {
        "generated_at": now,
        "source": "NWS (api.weather.gov) hourly forecast",
        "method_note": "Raw weather inputs for V8 compute. Retractable roofs treated as outdoor when team roof-schedule page marks the date Open.",
        "games": games_out,
    }
    os.makedirs(os.path.dirname(OUTPUT), exist_ok=True)
    with open(OUTPUT, "w") as f:
        json.dump(payload, f, indent=2)
    print(f"  wrote {len(games_out)} games to {OUTPUT}")
    good = sum(1 for g in games_out if g.get("weather"))
    print(f"  weather resolved: {good}/{len(games_out)} · frozen mid-game: {frozen} · "
          f"retractable roof open: {roof_open_count}")


if __name__ == "__main__":
    main()
