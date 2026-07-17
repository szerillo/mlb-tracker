#!/usr/bin/env python3
"""
Archive PropFinder (Kevin Roth / Visual Crossing) ballpark weather -> data/propfinder_weather.json
plus an append-only history at data/propfinder_archive.csv.

WHY: We want an independent, meteorologist-grade weather feed to (a) compare our
NWS-driven forecast inputs against and (b) grade forecast accuracy over time. The
public page (propfinder.app/weather) is a Next.js app that fetches this JSON:

    https://api.propfinder.app/mlb/weather-games?date=YYYY-MM-DD   (any date, no auth)

Each game carries: id == MLB gamePk (direct join), ballpark (name/lat/lon/azimuth),
gameDate (UTC first pitch), and hourly weatherData with numeric windDir (degrees),
temp, windSpeed, precip, pressure, dew — each hour tagged source "obs" (observed,
once elapsed) or "fcst" (forecast). PropFinder updates ~midnight ET then through
the day, so this runs on the normal refresh cadence and overwrites the day's row
with the freshest forecast (history keeps the last write per pull via logged_at).
"""
from __future__ import annotations
import csv, datetime as dt, json, os, sys, urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
OUT_JSON = os.path.join(HERE, "..", "data", "propfinder_weather.json")
ARCHIVE  = os.path.join(HERE, "..", "data", "propfinder_archive.csv")
API = "https://api.propfinder.app/mlb/weather-games?date={date}"
UA = {"User-Agent": "Mozilla/5.0 (mlb-tracker/propfinder-archive)"}

ARCHIVE_COLS = ["date", "game_pk", "venue", "azimuth", "roof",
                "game_time_utc", "hour_epoch", "hour_source",
                "temp_f", "wind_speed_mph", "wind_dir_deg", "wind_gust_mph",
                "precip_prob", "precip_in", "pressure_mb", "dew_f", "humidity_pct",
                "conditions", "run_line", "home_odds", "away_odds",
                "pulled_for_date", "logged_at"]


def fetch_date(date: str):
    url = API.format(date=date)
    req = urllib.request.Request(url, headers=UA)
    with urllib.request.urlopen(req, timeout=45) as r:
        return json.loads(r.read().decode("utf-8"))


def _pick_hour(weather, game_epoch):
    """The hourly record whose timestamp is closest to first pitch."""
    if not weather:
        return None
    if game_epoch is None:
        return weather[len(weather) // 2]
    return min(weather, key=lambda h: abs((h.get("dateTimeEpoch") or 0) - game_epoch))


def _epoch(iso):
    if not iso:
        return None
    try:
        return int(dt.datetime.fromisoformat(iso.replace("Z", "+00:00")).timestamp())
    except Exception:
        return None


def extract(games, pulled_for_date):
    """One row per game: the weather at first pitch."""
    now = dt.datetime.utcnow().isoformat(timespec="seconds") + "Z"
    out = {}
    for g in games or []:
        pk = g.get("id")
        if pk is None:
            continue
        bp = g.get("ballpark") or {}
        gepoch = _epoch(g.get("gameDate"))
        h = _pick_hour(g.get("weatherData") or [], gepoch) or {}
        out[str(pk)] = {
            "game_pk": pk,
            "venue": bp.get("name"),
            "azimuth": bp.get("azimuthAngle"),
            "roof": bp.get("roofType"),
            "game_time_utc": g.get("gameDate"),
            "hour_epoch": h.get("dateTimeEpoch"),
            "hour_source": h.get("source"),          # "obs" (actual) or "fcst"
            "temp_f": h.get("temp"),
            "wind_speed_mph": h.get("windSpeed"),
            "wind_dir_deg": h.get("windDir"),
            "wind_gust_mph": h.get("windGust"),
            "precip_prob": h.get("precipProb"),
            "precip_in": h.get("precip"),
            "pressure_mb": h.get("pressure"),
            "dew_f": h.get("dew"),
            "humidity_pct": h.get("humidity"),
            "conditions": h.get("conditions"),
            "run_line": g.get("gameRunLine"),
            "home_odds": g.get("homeTeamOdds"),
            "away_odds": g.get("visitorTeamOdds"),
            "pulled_for_date": pulled_for_date,
            "logged_at": now,
        }
    return out


def append_archive(rows):
    exists = os.path.exists(ARCHIVE)
    with open(ARCHIVE, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=ARCHIVE_COLS)
        if not exists:
            w.writeheader()
        for pk, r in rows.items():
            w.writerow({"date": r["pulled_for_date"], **{k: r.get(k) for k in ARCHIVE_COLS if k not in ("date",)}})


def main():
    date = (sys.argv[1] if len(sys.argv) > 1 else
            dt.datetime.now(dt.timezone.utc).astimezone(
                dt.timezone(dt.timedelta(hours=-4))).strftime("%Y-%m-%d"))  # ET slate date
    try:
        games = fetch_date(date)
    except Exception as e:
        print(f"[propfinder] fetch failed for {date}: {e}", file=sys.stderr)
        return 0  # non-fatal: never break the refresh
    rows = extract(games, date)
    if not rows:
        print(f"[propfinder] no games for {date}", file=sys.stderr)
        return 0
    payload = {
        "generated_at": dt.datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "date": date,
        "source": "PropFinder / Kevin Roth (api.propfinder.app, Visual Crossing hourly)",
        "n_games": len(rows),
        "games": rows,
    }
    os.makedirs(os.path.dirname(OUT_JSON), exist_ok=True)
    with open(OUT_JSON, "w") as f:
        json.dump(payload, f, indent=2)
    append_archive(rows)
    obs = sum(1 for r in rows.values() if r["hour_source"] == "obs")
    print(f"[propfinder] {date}: {len(rows)} games "
          f"({obs} observed / {len(rows)-obs} forecast) -> propfinder_weather.json + archive")
    return 0


if __name__ == "__main__":
    sys.exit(main())
