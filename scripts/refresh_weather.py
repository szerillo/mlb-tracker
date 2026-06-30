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
BP_WEATHER_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "bp_weather.json")

# ── V9 backend BP integration ────────────────────────────────────────────────
# We never DISPLAY BallparkPal's raw number, but we use it on the backend two ways:
#   1. PRESSURE input — BP's barometric pressure is the air-density figure BP's own
#      runs respond to, so feeding it into the model sharpens our number on covered
#      games (calibration: MAE 4.83→4.53). NWS doesn't carry pressure; BP does, and
#      we already scrape it. We keep NWS humidity (BP's humidity hurt our dew-point).
#   2. WEIGHT — the final published "V9" number is a weighted blend of our physical
#      model and BP's weather-only runs on the games BP covers. 0 = pure model,
#      1 = pure BP. 0.5 = equal ensemble: anchors us to BP (a strong reference, but
#      not ground truth, and it misses ~half the slate) without surrendering to it.
#      Uncovered games (tomorrow + scrape gaps) stay pure recalibrated model.
BP_BLEND_WEIGHT = 0.5
MODEL_VERSION = "v9"


def load_bp_weather():
    """Return (by_venue, bp_et_date) from the existing bp_weather.json (written by
    the prior refresh cycle). Used to feed BP pressure into the model and to weight
    the final number toward BP on the games BP covers. Returns ({}, None) if absent."""
    try:
        with open(BP_WEATHER_PATH) as f:
            doc = json.load(f)
    except Exception as e:
        print(f"  [v9] no bp_weather.json to integrate ({e})")
        return {}, None
    gen = doc.get("generated_at")
    bp_date = None
    if gen:
        try:
            bp_date = (datetime.datetime.fromisoformat(gen.replace("Z", "+00:00"))
                       - datetime.timedelta(hours=4)).date().isoformat()
        except Exception:
            bp_date = None
    by_venue = {g["venue"]: g for g in doc.get("games", []) if g.get("venue")}
    return by_venue, bp_date

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
    "Athletics":            ("STO", 40, 68),
    # Toronto is in Canada — no NWS coverage. Dome anyway.
}

# Parks with roofs. Some are permanent (TB), others retractable. For
# retractable parks we scrape the team's roof page to determine open/closed
# per game — when roof is "Open" we treat the game as outdoor and run V8.
DOMES = {"Tampa Bay Rays", "Toronto Blue Jays", "Houston Astros", "Texas Rangers",
         "Arizona Diamondbacks", "Miami Marlins", "Milwaukee Brewers"}

# Retractable-roof parks — same set minus Tampa Bay's permanent fixed dome.
# When the roof state is unknown for the date (no public schedule, e.g. TOR/MIL/
# HOU/TEX/MIA), we ASSUME closed (run adj 0) but ALSO compute the pure-model
# open-roof adjustment so the UI can show a dual "closed 0 · open +X%" readout.
RETRACTABLE = DOMES - {"Tampa Bay Rays"}

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


# ── Game-window temperature/humidity average ─────────────────────────────────
# A 9-inning game lasts ~3 hours, so the temperature the ball actually flies in
# is the AVERAGE over first pitch → ~+3h, not the single first-pitch hour. Night
# games can cool 8-12 F across the window, so anchoring to first pitch overstates
# the warm-weather boost. We take a weighted mean over the game window, front/
# middle-weighted (most balls are struck in the first ~2.5h; the +3h bucket is
# half-weighted since not every game reaches it). Humidity is averaged too so the
# dew-point component stays consistent with the temp it pairs with. Wind/pressure
# stay at first pitch (wind direction is what matters and circular averaging is
# error-prone; our wind input is already coarse).
GAME_WINDOW_WEIGHTS = [1.0, 1.0, 0.9, 0.5]   # hours [0, +1, +2, +3] from first pitch


def _game_window_periods(forecast, target_iso, n=4):
    """Return up to n NWS hourly periods covering first pitch .. +(n-1)h."""
    if not forecast:
        return None
    periods = forecast.get("periods") or forecast.get("properties", {}).get("periods", [])
    if not periods:
        return None
    try:
        target = datetime.datetime.fromisoformat(target_iso.replace("Z", "+00:00"))
    except Exception:
        return None
    parsed = []
    for p in periods:
        try:
            t = datetime.datetime.fromisoformat(p["startTime"].replace("Z", "+00:00"))
            parsed.append((t, p))
        except Exception:
            continue
    parsed.sort(key=lambda x: x[0])
    # Include the hour that CONTAINS first pitch (allow a 30-min lead) onward.
    floor = target - datetime.timedelta(minutes=30)
    out = [p for (t, p) in parsed if t >= floor][:n]
    return out or None


def game_window_avg(forecast, target_iso):
    """Weighted-mean (temp_f, humidity_pct) over the game window. Falls back to
    (None, None) if the window can't be built."""
    series = _game_window_periods(forecast, target_iso, n=len(GAME_WINDOW_WEIGHTS))
    if not series:
        return None, None
    tsum = tw = hsum = hw = 0.0
    for i, p in enumerate(series):
        w = GAME_WINDOW_WEIGHTS[i] if i < len(GAME_WINDOW_WEIGHTS) else GAME_WINDOW_WEIGHTS[-1]
        temp = p.get("temperature")
        hum = (p.get("relativeHumidity") or {}).get("value")
        if temp is not None:
            tsum += w * temp; tw += w
        if hum is not None:
            hsum += w * hum; hw += w
    t = round(tsum / tw, 1) if tw else None
    h = round(hsum / hw, 1) if hw else None
    return t, h



def compute_open_v8(home, game, fc):
    """Pure-model open-roof adjustment for a retractable park (NO BP blend).

    Used only for the dual "closed 0 · open +X%" readout on retractable parks
    whose roof state is unknown for the date. We assume the roof is CLOSED for
    the headline number (0), but surface what the model would say if it opens.
    Returns (hour, v8) or (hour, None) if no forecast / unmapped park.
    """
    hour = extract_hour(fc, game["game_time"])
    park_code = TEAM_TO_PARK.get(home)
    if not (hour and park_code):
        return hour, None
    t_hours = _three_hour_trend(fc, game["game_time"])
    win_t, win_h = game_window_avg(fc, game["game_time"])
    if hour is not None:
        hour["game_window_temp_f"] = win_t
        hour["game_window_humidity_pct"] = win_h
        hour["first_pitch_temp_f"] = hour.get("temp_f")
    wx_in = {
        "t": win_t if win_t is not None else hour.get("temp_f"),
        "hum": win_h if win_h is not None else hour.get("humidity_pct"),
        "ws": hour.get("wind_speed_mph") or 0,
        "wd_compass": nws_wind_to_compass(hour.get("wind_dir")),
        "precip": hour.get("precip_pct") or 0,
        "t_hours": t_hours,
    }
    v8 = compute_v8(park_code, wx_in)
    # Mirror the shape of the normal path so the frontend can read it uniformly,
    # but mark it as pure model (no BP pressure input, no blend).
    v8["model_pct"] = v8.get("run_adj_pct")
    v8["pressure_source"] = "default"
    v8["bp_pct"] = None
    v8["bp_blended"] = False
    return hour, v8


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

    # V9: load the existing BP scrape (prior cycle) for pressure + blend.
    bp_by_venue, bp_date = load_bp_weather()
    print(f"  [v9] BP integration: {len(bp_by_venue)} games on slate {bp_date} "
          f"(pressure input + {int(BP_BLEND_WEIGHT*100)}% weight on covered games)")

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
        # Fetch a forecast when: not a dome, OR roof is known-open, OR it's a
        # retractable park (so we can compute the dual open-roof adjustment).
        if (g["home"] not in DOMES
                or (gd_et and is_roof_open(g["home"], gd_et))
                or g["home"] in RETRACTABLE):
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
            entry = {
                "game_pk": g["game_pk"],
                "matchup": f"{g['away']} @ {home}",
                "venue": g["venue"],
                "game_time": g["game_time"],
                "is_dome": True,
                "weather": None,
                "note": "Dome / retractable roof — weather adjustment minimal",
            }
            # Retractable park, roof state unknown: assume CLOSED (headline 0)
            # but also compute the pure-model open-roof adjustment so the UI can
            # show "closed 0 · open +X%". Skip parks we can't forecast (e.g. TOR
            # has no NWS coverage in Canada).
            if home in RETRACTABLE and home in NWS_GRIDS:
                ofc = forecasts.get(home)
                ohour, ov8 = compute_open_v8(home, g, ofc)
                if ov8 is not None:
                    entry["retractable"] = True
                    entry["roof_state"] = "unknown"
                    entry["open_weather"] = ohour
                    entry["open_v8"] = ov8
                    entry["note"] = ("Retractable roof — closed assumed (run adj 0); "
                                     "open-roof model adjustment shown if it opens")
            games_out.append(entry)
            continue
        if roof_open:
            roof_open_count += 1
        fc = forecasts.get(home)
        hour = extract_hour(fc, g["game_time"])
        # Compute V9 if weather available
        v8 = None
        park_code = TEAM_TO_PARK.get(home)
        if hour and park_code:
            # Try to get a simple 3-hour trend around game time
            t_hours = _three_hour_trend(fc, g["game_time"])
            # Game-window average temp/humidity (first pitch .. ~+3h) instead of
            # the single first-pitch hour — see game_window_avg() above.
            win_t, win_h = game_window_avg(fc, g["game_time"])
            if hour is not None:
                hour["game_window_temp_f"] = win_t
                hour["game_window_humidity_pct"] = win_h
                hour["first_pitch_temp_f"] = hour.get("temp_f")
            # V9 step 1 — pull BP's barometric pressure for this game (only when
            # BP's slate matches the game date) and feed it in. Keep NWS humidity.
            bp_match = bp_by_venue.get(g["venue"]) if (gd_et and gd_et == bp_date) else None
            bp_pres = None
            if bp_match:
                p = bp_match.get("pressure_mb")
                if isinstance(p, (int, float)) and 970 <= p <= 1050:
                    bp_pres = p
            wx_in = {
                "t": win_t if win_t is not None else hour.get("temp_f"),
                "hum": win_h if win_h is not None else hour.get("humidity_pct"),
                "ws": hour.get("wind_speed_mph") or 0,
                "wd_compass": nws_wind_to_compass(hour.get("wind_dir")),
                "precip": hour.get("precip_pct") or 0,
                "t_hours": t_hours,
            }
            if bp_pres is not None:
                wx_in["pres"] = bp_pres
            v8 = compute_v8(park_code, wx_in)
            # V9 step 2 — weight the published number toward BP's weather-only runs
            # on the games BP covers. We never display BP's raw number; this blended
            # value IS the displayed "V9". Uncovered games stay pure model.
            model_pct = v8.get("run_adj_pct")
            bp_runs = bp_match.get("bp_weather_runs_pct") if bp_match else None
            v8["model_pct"] = model_pct
            v8["pressure_source"] = "BP" if bp_pres is not None else "default"
            if model_pct is not None and isinstance(bp_runs, (int, float)):
                w = BP_BLEND_WEIGHT
                v8["run_adj_pct"] = round((1.0 - w) * model_pct + w * bp_runs, 1)
                v8["bp_pct"] = bp_runs
                v8["bp_blended"] = True
                v8["blend_weight"] = w
            else:
                v8["bp_pct"] = None
                v8["bp_blended"] = False
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
        "model_version": MODEL_VERSION,
        "source": "NWS (api.weather.gov) hourly forecast",
        "method_note": ("V9: recalibrated per-park weather model on NWS temp/wind/dew/precip, "
                        "with BallparkPal barometric pressure fed in as the air-density input and "
                        f"the published number weighted {int(BP_BLEND_WEIGHT*100)}% toward BP's "
                        "weather-only runs on games BP covers (v8.run_adj_pct = blended; "
                        "v8.model_pct = pure model; v8.bp_pct = BP). Uncovered games are pure model. "
                        "Retractable roofs treated as outdoor when the roof-schedule page marks the date Open. "
                        "When the roof state is unknown, closed is assumed (headline 0) and a pure-model "
                        "open-roof adjustment is emitted under open_v8 for a dual 'closed 0 · open +X%' display."),
        "games": games_out,
    }
    os.makedirs(os.path.dirname(OUTPUT), exist_ok=True)
    with open(OUTPUT, "w") as f:
        json.dump(payload, f, indent=2)
    print(f"  wrote {len(games_out)} games to {OUTPUT}")
    good = sum(1 for g in games_out if g.get("weather"))
    blended = sum(1 for g in games_out if (g.get("v8") or {}).get("bp_blended"))
    bp_pres_used = sum(1 for g in games_out if (g.get("v8") or {}).get("pressure_source") == "BP")
    print(f"  weather resolved: {good}/{len(games_out)} · frozen mid-game: {frozen} · "
          f"retractable roof open: {roof_open_count}")
    print(f"  [v9] BP-pressure used: {bp_pres_used} · BP-weighted: {blended} games")


if __name__ == "__main__":
    main()
