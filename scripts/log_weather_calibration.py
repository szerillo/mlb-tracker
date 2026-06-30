#!/usr/bin/env python3
"""
Append/upsert a per-game weather-calibration row into
data/weather_calibration_log.csv every refresh.

WHY
---
We want to recalibrate the weather model against BallparkPal (and, later, actual
runs). There's no stored weather history, so this accumulates one row per game
per day: our PURE model number (v8.model_pct), our headline blended number
(v8.run_adj_pct), BP's weather runs % (v8.bp_pct), and the raw inputs +
component breakdown — enough to refit TEMP_C / wind / per-park terms in ~2 weeks.

UPSERT, NOT APPEND
------------------
Keyed on (date, game_pk). Each refresh overwrites that game's row with the
CURRENT forecast, so the stored snapshot ends up being the last one before first
pitch (the most accurate). Once a game ages out of weather.json (it covers
today..+2), its game-day row is frozen in place.

Reads  : data/weather.json (already carries model_pct AND bp_pct per game),
         data/bp_weather.json (optional enrichment: BP hr%/2-3b%, pressure).
Writes : data/weather_calibration_log.csv
"""
from __future__ import annotations
import os, csv, json, datetime

REPO = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DATA = os.path.join(REPO, "data")
WX   = os.environ.get("WEATHER_PATH", os.path.join(DATA, "weather.json"))
BPW  = os.environ.get("BP_WEATHER_PATH", os.path.join(DATA, "bp_weather.json"))
OUT  = os.environ.get("OUT_PATH", os.path.join(DATA, "weather_calibration_log.csv"))

FIELDS = [
    "date", "game_pk", "venue", "away", "home",
    "first_pitch_temp_f", "game_window_temp_f", "humidity_pct",
    "wind_speed_mph", "wind_dir", "precip_pct", "short_forecast", "pressure_mb",
    # v8 component breakdown
    "t_adj_pct", "w_adj_pct", "dp_adj_pct", "p_adj_pct", "precip_adj_pct",
    "carry_pct", "wind_out_component", "wind_dir_rarity", "wind_spd_rarity",
    # outputs
    "model_pct", "run_adj_pct", "bp_pct", "bp_weather_hr_pct", "bp_weather_23b_pct",
    "bp_blended", "capped", "pressure_source", "is_dome", "roof_open",
    # provenance
    "forecast_generated_at", "logged_at",
]


def load_json(path):
    try:
        return json.load(open(path))
    except Exception as e:
        print(f"  [warn] could not read {path}: {e}")
        return {}


def norm_venue(s):
    return " ".join((s or "").lower().replace(".", "").split())


def main():
    wx = load_json(WX)
    games = wx.get("games", []) if isinstance(wx, dict) else []
    if not games:
        print("[wx-cal] no weather games; nothing to log")
        return
    gen_at = wx.get("generated_at", "")

    # BP enrichment keyed by venue
    bpw = load_json(BPW)
    bp_by_venue = {}
    for b in (bpw.get("games", []) if isinstance(bpw, dict) else []):
        bp_by_venue[norm_venue(b.get("venue"))] = b

    # load existing rows -> dict keyed (date, game_pk)
    existing = {}
    if os.path.exists(OUT):
        with open(OUT, newline="") as f:
            for row in csv.DictReader(f):
                existing[(row.get("date", ""), str(row.get("game_pk", "")))] = row

    logged_at = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    n_new = n_upd = 0
    for g in games:
        pk = g.get("game_pk")
        if pk is None:
            continue
        gt = g.get("game_time", "") or ""
        date = gt[:10]
        v8 = g.get("v8") or {}
        comp = v8.get("components", {}) or {}
        wind = comp.get("wind", {}) or {}
        wxm = g.get("weather", {}) or {}
        bp = bp_by_venue.get(norm_venue(g.get("venue")), {})

        row = {
            "date": date, "game_pk": pk, "venue": g.get("venue", ""),
            "away": (g.get("matchup", "") or "").split(" @ ")[0],
            "home": (g.get("matchup", "") or "").split(" @ ")[-1],
            "first_pitch_temp_f": wxm.get("first_pitch_temp_f"),
            "game_window_temp_f": wxm.get("game_window_temp_f"),
            "humidity_pct": wxm.get("humidity_pct"),
            "wind_speed_mph": wxm.get("wind_speed_mph"),
            "wind_dir": wxm.get("wind_dir"),
            "precip_pct": wxm.get("precip_pct"),
            "short_forecast": wxm.get("short_forecast", ""),
            "pressure_mb": bp.get("pressure_mb"),
            "t_adj_pct": comp.get("t_adj_pct"), "w_adj_pct": comp.get("w_adj_pct"),
            "dp_adj_pct": comp.get("dp_adj_pct"), "p_adj_pct": comp.get("p_adj_pct"),
            "precip_adj_pct": comp.get("precip_adj_pct"), "carry_pct": comp.get("carry_pct"),
            "wind_out_component": wind.get("out_component"),
            "wind_dir_rarity": wind.get("dir_rarity"), "wind_spd_rarity": wind.get("spd_rarity"),
            "model_pct": v8.get("model_pct"), "run_adj_pct": v8.get("run_adj_pct"),
            "bp_pct": v8.get("bp_pct"),
            "bp_weather_hr_pct": bp.get("bp_weather_hr_pct"),
            "bp_weather_23b_pct": bp.get("bp_weather_23b_pct"),
            "bp_blended": v8.get("bp_blended"), "capped": v8.get("capped"),
            "pressure_source": v8.get("pressure_source"),
            "is_dome": g.get("is_dome"), "roof_open": g.get("roof_open"),
            "forecast_generated_at": gen_at, "logged_at": logged_at,
        }
        key = (date, str(pk))
        if key in existing:
            n_upd += 1
        else:
            n_new += 1
        existing[key] = row

    # write back, sorted by date then game_pk
    rows = sorted(existing.values(), key=lambda r: (r.get("date", ""), str(r.get("game_pk", ""))))
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(OUT, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)
    print(f"[wx-cal] {len(rows)} total rows ({n_new} new, {n_upd} updated) -> {OUT}")


if __name__ == "__main__":
    main()
