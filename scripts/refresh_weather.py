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

# Parks with roofs where weather usually has ~0 adjustment
DOMES = {"Tampa Bay Rays", "Toronto Blue Jays", "Houston Astros", "Texas Rangers",
         "Arizona Diamondbacks", "Miami Marlins", "Milwaukee Brewers"}


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


def get_today_schedule():
    today = _mlb_business_date()
    d = fetch(f"https://statsapi.mlb.com/api/v1/schedule?sportId=1&date={today}")
    if not d or not d.get("dates"):
        return []
    return [{
        "game_pk": g["gamePk"],
        "away": g["teams"]["away"]["team"]["name"],
        "home": g["teams"]["home"]["team"]["name"],
        "game_time": g["gameDate"],
        "venue": g.get("venue", {}).get("name", ""),
        "status": g.get("status", {}).get("abstractGameState", ""),
    } for g in d["dates"][0].get("games", [])]


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

    # Fetch forecasts in parallel (one per unique grid point), but skip any
    # home team whose game has started — we won't need a fresh forecast.
    unique_teams = {
        g["home"] for g in schedule
        if g["home"] in NWS_GRIDS and not game_has_started(g.get("status", ""))
    }
    forecasts = {}

    def _load(team):
        grid = NWS_GRIDS[team]
        return team, get_forecast(*grid)

    with ThreadPoolExecutor(max_workers=10) as ex:
        for team, fc in ex.map(_load, unique_teams):
            forecasts[team] = fc

    games_out = []
    frozen = 0
    for g in schedule:
        home = g["home"]
        # FREEZE — if the game has started, reuse the prior snapshot verbatim
        if game_has_started(g.get("status", "")) and g["game_pk"] in prior_by_pk:
            games_out.append(prior_by_pk[g["game_pk"]])
            frozen += 1
            continue
        if home in DOMES:
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
            "weather": hour,
            "v8": v8,
            "note": None if hour else "NWS forecast unavailable",
        })

    payload = {
        "generated_at": now,
        "source": "NWS (api.weather.gov) hourly forecast",
        "method_note": "Raw weather inputs for V8 compute. V8 run-impact scoring pending integration.",
        "games": games_out,
    }
    os.makedirs(os.path.dirname(OUTPUT), exist_ok=True)
    with open(OUTPUT, "w") as f:
        json.dump(payload, f, indent=2)
    print(f"  wrote {len(games_out)} games to {OUTPUT}")
    good = sum(1 for g in games_out if g.get("weather"))
    print(f"  weather resolved: {good}/{len(games_out)} · frozen mid-game: {frozen}")


if __name__ == "__main__":
    main()
