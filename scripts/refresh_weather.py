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

# ── Manual weather override (date-gated; auto-expires) ───────────────────────
# data/weather_override.json lets us pin a corrected forecast for ONE slate when
# an upstream source (e.g. BallparkPal) is clearly wrong. Shape:
#   {"date": "2026-07-11",
#    "no_bp_blend": true,                      # optional, applies to all games
#    "games": {"<game_pk>": {"t":77,"ws":14,"precip":0,"no_bp_blend":true}}}
# It only applies when its "date" matches the game's ET date, so it silently
# stops applying the next day — no cleanup needed.
OVERRIDE_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "weather_override.json")
_WX_OV = None


def _wx_override_for(game_pk, game_date_et):
    global _WX_OV
    if _WX_OV is None:
        try:
            with open(OVERRIDE_PATH) as f:
                _WX_OV = json.load(f)
        except Exception:
            _WX_OV = {}
    if not _WX_OV or not game_date_et or _WX_OV.get("date") != game_date_et:
        return None
    ov = (_WX_OV.get("games") or {}).get(str(game_pk))
    if ov is None:
        # Slate-wide flag with no per-game pins: still drop the BP blend so the
        # published number is our own (fresh) forecast, uncontaminated.
        if _WX_OV.get("no_bp_blend"):
            return {"no_bp_blend": True}
        return None
    ov = dict(ov)
    if _WX_OV.get("no_bp_blend") and "no_bp_blend" not in ov:
        ov["no_bp_blend"] = True
    return ov

# ── V9 backend BP integration ────────────────────────────────────────────────
# We never DISPLAY BallparkPal's raw number, but we use it on the backend two ways:
#   1. PRESSURE input — BP's barometric pressure is the air-density figure BP's own
#      runs respond to, so feeding it into the model sharpens our number on covered
#      games (calibration: MAE 4.83→4.53). NWS doesn't carry pressure; BP does, and
#      we already scrape it. We keep NWS humidity (BP's humidity hurt our dew-point).
#   2. WEIGHT — the published number USED to be a 50/50 ensemble with BP's
#      weather-only runs. Backtested 2026-07-11 on the calibration log (97 non-dome
#      finals w/ closing totals, 6/30-7/11), scoring each signal against actual runs
#      vs the closing total:
#          our pure model   r = +0.162
#          BallparkPal      r = +0.129
#          50/50 blend      r = +0.150   <- strictly between; the blend never won
#      corr(model, BP) = +0.77, so 60% of BP is just restating our own number. The
#      part BP adds ON TOP of us -- its unique component, sd ~5 pts of run-adj --
#      scored r = +0.006 vs actual runs. That is noise, not signal, and it was
#      stable across every leave-one-day-out fold (-0.05..+0.05). RMSE of a
#      total-based run prediction fell monotonically as BP weight went to zero.
#      So: weight 0. We still SCRAPE BP (pressure input below + we keep logging
#      bp_pct and bp_temp_f to keep scoring them), we just don't publish their view.
BP_BLEND_WEIGHT = 0.0
MODEL_VERSION = "v10"


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
# NOTE: these MUST match api.weather.gov/points/{lat},{lon} for the PARK itself.
# Audited 2026-07-12: 15 of 29 were stale/wrong — Citi Field was pulling OKX 38,38,
# a cell 11 mi south out over Jamaica Bay (sea-breeze air: 75F/64% RH vs the park's
# real 83F/46%). Angel Stadium was on the wrong forecast OFFICE entirely (LOX->SGX).
# Re-run scripts/audit_nws_grids.py after any park move.
NWS_GRIDS = {
    "Arizona Diamondbacks": ("PSR", 159, 58),
    "Atlanta Braves":       ("FFC", 47, 93),
    "Baltimore Orioles":    ("LWX", 109, 91),
    "Boston Red Sox":       ("BOX", 70, 100),
    "Chicago Cubs":         ("LOT", 75, 76),
    "Chicago White Sox":    ("LOT", 76, 71),
    "Cincinnati Reds":      ("ILN", 36, 38),
    "Cleveland Guardians":  ("CLE", 84, 65),
    "Colorado Rockies":     ("BOU", 63, 62),
    "Detroit Tigers":       ("DTX", 66, 34),
    "Houston Astros":       ("HGX", 63, 94),
    "Kansas City Royals":   ("EAX", 47, 49),
    "Los Angeles Angels":   ("SGX", 38, 66),
    "Los Angeles Dodgers":  ("LOX", 155, 46),
    "Miami Marlins":        ("MFL", 109, 51),
    "Milwaukee Brewers":    ("MKX", 86, 64),
    "Minnesota Twins":      ("MPX", 108, 72),
    "New York Mets":        ("OKX", 38, 45),
    "New York Yankees":     ("OKX", 35, 48),
    "Philadelphia Phillies":("PHI", 50, 77),
    "Pittsburgh Pirates":   ("PBZ", 77, 67),
    "San Diego Padres":     ("SGX", 57, 14),
    "Seattle Mariners":     ("SEW", 124, 67),
    "San Francisco Giants": ("MTR", 85, 105),
    "St. Louis Cardinals":  ("LSX", 95, 74),
    "Tampa Bay Rays":       ("TBW", 64, 89),
    "Texas Rangers":        ("FWD", 79, 103),
    "Washington Nationals": ("LWX", 98, 70),
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

# Cold-climate retractables that open their roof OFTEN. With no roof-schedule
# source for them, if the roof state is unknown and the weather is mild & dry we
# lean OPEN (compute the real outdoor number) rather than defaulting to closed-0.
# Hot-climate retractables (HOU/TEX/ARI/MIA) stay closed-by-default (summer AC).
LEAN_OPEN_PARKS = {"Toronto Blue Jays", "Milwaukee Brewers"}
def _likely_open(hour):
    """Heuristic: roof probably open when it's dry and comfortably mild."""
    if not hour:
        return False
    t = hour.get("game_window_temp_f") or hour.get("temp_f")
    p = hour.get("precip_pct") or 0
    return t is not None and p < 25 and 55 <= t <= 92

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


# Parks NWS can't cover (Canada). Open-Meteo (free, global, no key) provides the
# hourly forecast; we adapt its output into the NWS hourly shape the rest of the
# pipeline expects so extract_hour / game_window_avg work unchanged.
OPENMETEO_PARKS = {
    "Toronto Blue Jays": (43.6414, -79.3894),   # Rogers Centre
    "Boston Red Sox": (42.3467, -71.0972),   # Fenway Park - NWS hourly gridpoint 500s, use Open-Meteo
}
_DEG16 = ["N","NNE","NE","ENE","E","ESE","SE","SSE","S","SSW","SW","WSW","W","WNW","NW","NNW"]
def _deg_to_compass(deg):
    if deg is None:
        return None
    return _DEG16[int((deg % 360) / 22.5 + 0.5) % 16]

import math as _math

# Home-plate coords per park (team -> lat, lon) for the Open-Meteo wind pull that
# we vector-average with NWS. Cross-checked vs actual airport wind (12 games,
# 6/30-7/01): blended wind MAE 2.08 mph vs NWS 2.61 / Open-Meteo 2.74 — neither
# source is reliably better, but averaging their (independent) errors wins.
PARK_LATLON = {
    "Arizona Diamondbacks": (33.4455, -112.0667), "Atlanta Braves": (33.8908, -84.4678),
    "Baltimore Orioles": (39.2839, -76.6217), "Boston Red Sox": (42.3467, -71.0972),
    "Chicago Cubs": (41.9484, -87.6553), "Chicago White Sox": (41.8300, -87.6339),
    "Cincinnati Reds": (39.0975, -84.5069), "Cleveland Guardians": (41.4962, -81.6852),
    "Colorado Rockies": (39.7559, -104.9942), "Detroit Tigers": (42.3390, -83.0485),
    "Houston Astros": (29.7570, -95.3555), "Kansas City Royals": (39.0517, -94.4803),
    "Los Angeles Angels": (33.8003, -117.8827), "Los Angeles Dodgers": (34.0739, -118.2400),
    "Miami Marlins": (25.7781, -80.2197), "Milwaukee Brewers": (43.0280, -87.9712),
    "Minnesota Twins": (44.9817, -93.2776), "New York Mets": (40.7571, -73.8458),
    "New York Yankees": (40.8296, -73.9262), "Athletics": (38.5800, -121.5130),
    "Philadelphia Phillies": (39.9061, -75.1665), "Pittsburgh Pirates": (40.4469, -80.0057),
    "San Diego Padres": (32.7073, -117.1566), "Seattle Mariners": (47.5914, -122.3325),
    "San Francisco Giants": (37.7786, -122.3893), "St. Louis Cardinals": (38.6226, -90.1928),
    "Tampa Bay Rays": (27.7683, -82.6534), "Texas Rangers": (32.7473, -97.0847),
    "Toronto Blue Jays": (43.6414, -79.3894), "Washington Nationals": (38.8730, -77.0074),
}

def _compass_str_to_deg(s):
    if not s: return None
    return {"N":0,"NNE":22.5,"NE":45,"ENE":67.5,"E":90,"ESE":112.5,"SE":135,"SSE":157.5,
            "S":180,"SSW":202.5,"SW":225,"WSW":247.5,"W":270,"WNW":292.5,"NW":315,"NNW":337.5}.get(s.strip().upper())

def _blend_wind(ws1, wd1, ws2, wd2):
    """Vector-average two winds (speeds mph; dirs = compass degrees FROM)."""
    def uv(ws, wd):
        r = _math.radians(wd); return (-ws * _math.sin(r), -ws * _math.cos(r))
    u1, v1 = uv(ws1, wd1); u2, v2 = uv(ws2, wd2)
    u = (u1 + u2) / 2.0; v = (v1 + v2) / 2.0
    return _math.hypot(u, v), _math.degrees(_math.atan2(-u, -v)) % 360

def _blend_hour_wind(hour, om_fc, game_time):
    """Blend NWS `hour` wind with Open-Meteo (vector avg), writing the blended
    speed/dir back into `hour`. Returns (speed_mph, dir_deg); falls back to NWS
    alone when Open-Meteo is unavailable."""
    if not hour: return None, None
    n_ws = hour.get("wind_speed_mph") or 0
    n_wd = _compass_str_to_deg(hour.get("wind_dir"))
    om_hour = extract_hour(om_fc, game_time) if om_fc else None
    o_ws = (om_hour or {}).get("wind_speed_mph")
    o_wd = _compass_str_to_deg((om_hour or {}).get("wind_dir"))
    if o_ws is None or o_wd is None or n_wd is None:
        return n_ws, n_wd
    ws, wd = _blend_wind(n_ws, n_wd, o_ws, o_wd)
    hour["wind_speed_mph"] = round(ws)
    hour["wind_dir"] = _deg_to_compass(wd)
    hour["wind_blended"] = True
    return round(ws), wd


def get_forecast_openmeteo(lat, lon):
    url = ("https://api.open-meteo.com/v1/forecast?latitude=%s&longitude=%s"
           "&hourly=temperature_2m,relative_humidity_2m,precipitation_probability,"
           "wind_speed_10m,wind_direction_10m&temperature_unit=fahrenheit"
           "&wind_speed_unit=mph&timezone=UTC&forecast_days=4" % (lat, lon))
    d = fetch(url)
    h = (d or {}).get("hourly")
    if not h:
        return None
    times = h.get("time", [])
    def col(k, i):
        a = h.get(k) or []
        return a[i] if i < len(a) else None
    periods = []
    for i, t in enumerate(times):
        periods.append({
            "startTime": t + "Z",   # Open-Meteo UTC time -> NWS-style Z suffix
            "temperature": col("temperature_2m", i),
            "relativeHumidity": {"value": col("relative_humidity_2m", i)},
            "windSpeed": "%d mph" % round(col("wind_speed_10m", i) or 0),
            "windDirection": _deg_to_compass(col("wind_direction_10m", i)),
            "probabilityOfPrecipitation": {"value": col("precipitation_probability", i) or 0},
            "shortForecast": "",
        })
    return {"properties": {"periods": periods}}


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
            out.append(_resolve_asg_host({
                "game_pk": g["gamePk"],
                "away": g["teams"]["away"]["team"]["name"],
                "home": g["teams"]["home"]["team"]["name"],
                "game_time": g["gameDate"],
                "venue": g.get("venue", {}).get("name", ""),
                "status": g.get("status", {}).get("abstractGameState", ""),
            }))
    return out


# ---------------------------------------------------------------------------
# All-Star Game. The schedule lists the home team as "National League All-Stars",
# which matches no NWS grid, no park code and no park model — so the ASG fell out
# of the weather feed entirely (the modal showed "NWS forecast unavailable").
# The game is played at a real stadium, so we rewrite the home team to the HOST
# club and everything downstream (grid, park factors, v8 park constants) works.
VENUE_HOST = {
    "Citizens Bank Park": "Philadelphia Phillies",   # 2026 ASG
    "Truist Park": "Atlanta Braves",
    "Dodger Stadium": "Los Angeles Dodgers",
    "T-Mobile Park": "Seattle Mariners",
}


def _resolve_asg_host(g):
    """Rewrite an All-Star Game's home team to the club that owns the ballpark."""
    home = g.get("home") or ""
    if "All-Stars" not in home:
        return g
    host = VENUE_HOST.get(g.get("venue") or "")
    if not host:
        print(f"  [asg] no host club mapped for venue {g.get('venue')!r}; skipping weather")
        return g
    print(f"  [asg] {g.get('venue')} -> weather/park keyed to {host}")
    g = dict(g)
    g["home"] = host
    g["asg"] = True
    return g


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
    v8 = compute_v8(park_code, wx_in, treat_as_open=True)
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
          f"(pressure input; {int(BP_BLEND_WEIGHT*100)}% weight on covered games)")

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
    # Field of Dreams (Dyersville, IA): host plays ~200 mi from its home park;
    # StatsAPI venue name is "Field of Dreams". Force Open-Meteo at the Dyersville
    # site (no NWS gridpoint exists for a one-off field) so temp/wind reflect Iowa.
    FOD_LATLON = (42.4850, -91.0800)
    _fod_hosts = {g["home"] for g in schedule
                  if "field of dreams" in str(g.get("venue") or "").lower()
                  or "dyersville" in str(g.get("venue") or "").lower()}
    for g in schedule:
        if g["home"] not in NWS_GRIDS and g["home"] not in OPENMETEO_PARKS: continue
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
        if team in _fod_hosts:
            return team, get_forecast_openmeteo(*FOD_LATLON)
        if team in NWS_GRIDS:
            fc = get_forecast(*NWS_GRIDS[team])
            if fc:
                return team, fc
            # NWS hourly gridpoint can 500 (e.g. BOX/Boston); fall back to Open-Meteo if we have coords
            ll = OPENMETEO_PARKS.get(team)
            if ll:
                return team, get_forecast_openmeteo(*ll)
            return team, None
        ll = OPENMETEO_PARKS.get(team)
        return team, (get_forecast_openmeteo(*ll) if ll else None)

    with ThreadPoolExecutor(max_workers=10) as ex:
        for team, fc in ex.map(_load, unique_teams):
            forecasts[team] = fc

    # Open-Meteo wind for the same parks (vector-averaged with NWS below).
    om_forecasts = {}
    def _load_om(team):
        ll = (FOD_LATLON if team in _fod_hosts else PARK_LATLON.get(team))
        return team, (get_forecast_openmeteo(*ll) if ll else None)
    with ThreadPoolExecutor(max_workers=10) as ex:
        for team, fc in ex.map(_load_om, unique_teams):
            om_forecasts[team] = fc

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
        # Lean-open: TOR/MIL open their roof often and have no roof-schedule
        # source. If the roof state is unknown and the weather is mild & dry,
        # treat the game as OPEN (compute the real outdoor number) rather than
        # defaulting to closed-0.
        roof_likely_open = False
        if home in LEAN_OPEN_PARKS and not roof_open:
            if _likely_open(extract_hour(forecasts.get(home), g["game_time"])):
                roof_likely_open = True
        is_open = bool(roof_open or roof_likely_open)
        # Retractable park, roof closed/unknown -> headline 0, but still surface
        # the (correct, undamped) open-roof number for the dual readout.
        if home in DOMES and not is_open:
            entry = {
                "game_pk": g["game_pk"],
                "matchup": f"{g['away']} @ {home}",
                "venue": g["venue"],
                "game_time": g["game_time"],
                "is_dome": True,
                "weather": None,
                "note": "Dome / retractable roof — weather adjustment minimal",
            }
            if home in RETRACTABLE and (home in NWS_GRIDS or home in OPENMETEO_PARKS):
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
        if is_open:
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
            # Vector-average NWS wind with Open-Meteo (mutates hour's wind fields
            # so the stored/displayed wind is the blend too).
            _bws, _bwd = _blend_hour_wind(hour, om_forecasts.get(home), g["game_time"])
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
                "ws": _bws or 0,
                "wd_compass": _bwd if _bwd is not None else nws_wind_to_compass(hour.get("wind_dir")),
                "precip": hour.get("precip_pct") or 0,
                "t_hours": t_hours,
            }
            if bp_pres is not None:
                wx_in["pres"] = bp_pres
            # Date-gated manual override: pin corrected forecast inputs for this
            # slate (used when an upstream source is clearly wrong for the day).
            _ov = _wx_override_for(g["game_pk"], gd_et)
            if _ov:
                for _k in ("t", "hum", "ws", "wd_compass", "precip", "pres"):
                    if _ov.get(_k) is not None:
                        wx_in[_k] = _ov[_k]
                # accept a compass-string wind dir in overrides; v8 wants numeric degrees
                if isinstance(wx_in.get("wd_compass"), str):
                    wx_in["wd_compass"] = _compass_str_to_deg(wx_in["wd_compass"])
            # An open/likely-open retractable computes as a true outdoor park.
            v8 = compute_v8(park_code, wx_in, treat_as_open=(home in RETRACTABLE))
            # V9 step 2 — weight the published number toward BP's weather-only runs
            # on the games BP covers. We never display BP's raw number; this blended
            # value IS the displayed "V9". Uncovered games stay pure model.
            model_pct = v8.get("run_adj_pct")
            bp_runs = bp_match.get("bp_weather_runs_pct") if bp_match else None
            v8["model_pct"] = model_pct
            v8["pressure_source"] = "BP" if bp_pres is not None else "default"
            if _ov and _ov.get("no_bp_blend"):
                # BP is untrustworthy for this slate — publish the pure model.
                v8["run_adj_pct"] = model_pct
                v8["bp_pct"] = bp_runs
                v8["bp_blended"] = False
                v8["overridden"] = True
            elif model_pct is not None and isinstance(bp_runs, (int, float)):
                w = BP_BLEND_WEIGHT
                v8["run_adj_pct"] = round((1.0 - w) * model_pct + w * bp_runs, 1)
                v8["bp_pct"] = bp_runs
                v8["bp_blended"] = w > 0
                v8["blend_weight"] = w
            else:
                v8["bp_pct"] = None
                v8["bp_blended"] = False
            # Always carry BP's forecast temp through so the calibration log can
            # score BP's FORECAST against MLB's recorded game temp over time.
            if bp_match:
                v8["bp_temp_f"] = bp_match.get("bp_temp_f")
            if _ov:
                v8["overridden"] = True
        if v8 is None and g["game_pk"] in prior_by_pk and (prior_by_pk[g["game_pk"]].get("v8") is not None):
            # Deterministic guard: a flaky NWS run can return no fresh forecast (v8 None),
            # which would drop this game weather chip to 0 and flip it back the next run.
            # Keep the last good published value until a real forecast returns.
            _prev = dict(prior_by_pk[g["game_pk"]])
            _prev["carried_forward"] = True
            games_out.append(_prev)
            frozen += 1
            continue
        games_out.append({
            "game_pk": g["game_pk"],
            "matchup": f"{g['away']} @ {home}",
            "venue": g["venue"],
            "game_time": g["game_time"],
            "is_dome": False,
            "roof_open": True if is_open else None,
            "roof_state": ("likely-open (weather)" if roof_likely_open
                           else ("open" if roof_open else None)),
            "weather": hour,
            "v8": v8,
            "note": None if hour else "NWS forecast unavailable",
        })

    payload = {
        "generated_at": now,
        "model_version": MODEL_VERSION,
        "source": "NWS (api.weather.gov) hourly forecast",
        "method_note": ("V10: V9 per-park model + asymmetric HR->runs damper (positive adj 6*tanh(raw/6), Wrigley exempt; suppression untouched). Recalibrated on NWS temp/wind/dew/precip, "
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
