#!/usr/bin/env python3
"""
Daily pre-game inputs archive — closes PREGAME_MODEL_SPEC.md decision 4.

Snapshots every input that feeds computePregame() plus the model's output to
data/archive/{ET-date}/pregame_inputs.json. The frontend's projection lives
in JS (index.html → computePregame); this script is a deterministic Python
port that re-runs the SAME formulas on the SAME JSON inputs the frontend
reads, so:

  • the archive captures the model's reasoning at lock time (SP RA, BP RA,
    n_fatigued, team offense, BSR/DEF, park, pw/pqm/bp_factor + projected
    runs/WP/total) for every game with confirmed-or-projected lineups, AND
  • once a few weeks of snapshots stack up alongside the post-game finals,
    we have a clean forward backtest.

USAGE:
    python scripts/refresh_pregame_archive.py
    python scripts/refresh_pregame_archive.py --date 2026-05-30   # backfill a date

Notes:
  - The script mirrors index.html's computePregame() exactly; if you change
    one, change the other (the spec carries the math contract).
  - Output is idempotent for a given date: re-running OVERWRITES the file,
    capturing the latest lineups/pitchers data. The intended use is "snapshot
    once at lineup-lock time" (e.g. 30 min before first pitch of the slate).
"""
from __future__ import annotations
import argparse
import datetime
import json
import math
import os
import sys
import unicodedata
import urllib.request
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = REPO_ROOT / "data"
ARCHIVE_DIR = DATA_DIR / "archive"


# ── constants (mirror index.html) ─────────────────────────────────────────
PG_LG_WOBA = 0.317
PG_RG = 4.5
PG_PYTH = 1.83
PG_SPOT_WT = [1.44, 1.31, 1.22, 1.06, 0.94, 0.88, 0.81, 0.69, 0.61]
SLOT_PA_DELTA = [0.43, 0.32, 0.21, 0.11, 0.01, -0.09, -0.21, -0.33, -0.45]
SLOT_BASE_PA = 4.0

# MLB team name → BaseballSavant park-factor key. The schedule API doesn't
# include abbreviations, so without this lookup the park factor would silently
# fall back to 1.00× for every game. Mirrors refresh_lineups.py's NAME_TO_ABBR.
NAME_TO_ABBR = {
    "Arizona Diamondbacks": "ARI", "Atlanta Braves": "ATL", "Baltimore Orioles": "BAL",
    "Boston Red Sox": "BOS", "Chicago Cubs": "CHC", "Chicago White Sox": "CWS",
    "Cincinnati Reds": "CIN", "Cleveland Guardians": "CLE", "Colorado Rockies": "COL",
    "Detroit Tigers": "DET", "Houston Astros": "HOU", "Kansas City Royals": "KC",
    "Los Angeles Angels": "LAA", "Los Angeles Dodgers": "LAD", "Miami Marlins": "MIA",
    "Milwaukee Brewers": "MIL", "Minnesota Twins": "MIN", "New York Mets": "NYM",
    "New York Yankees": "NYY", "Athletics": "OAK", "Oakland Athletics": "OAK",
    "Philadelphia Phillies": "PHI", "Pittsburgh Pirates": "PIT", "San Diego Padres": "SD",
    "Seattle Mariners": "SEA", "San Francisco Giants": "SF", "St. Louis Cardinals": "STL",
    "Tampa Bay Rays": "TB", "Texas Rangers": "TEX", "Toronto Blue Jays": "TOR",
    "Washington Nationals": "WSH",
}

# park_factors.json uses different keys for two teams than MLB Stats API convention.
# Mirrors index.html's parkFactorFor() ABBREV_ALIAS so lookups succeed for both.
PARK_FACTOR_ALIAS = {"WSH": "WAS", "CWS": "CHW", "ATH": "OAK", "AZ": "ARI"}


def _clamp(x, lo, hi):
    return max(lo, min(hi, x))


def norm_name(s):
    """Match JS normName: lower, strip accents, drop Jr/Sr/II/III, drop periods."""
    if not s:
        return ""
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c)).lower()
    for suf in (" jr.", " jr", " sr.", " sr", " iii", " ii"):
        if s.endswith(suf):
            s = s[: -len(suf)]
    return s.replace(".", "").strip()


def _et_today_iso():
    return (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=4)).date().isoformat()


def _load_json(path, default=None):
    p = DATA_DIR / path
    if not p.exists():
        return default
    return json.loads(p.read_text())


def _hand_of_pitcher(name, pitcher_stats):
    if not name:
        return None
    p = pitcher_stats.get("pitchers", {}).get(norm_name(name)) or {}
    h = (p.get("hand") or "").upper()
    return h if h in ("L", "R") else None


# ── offense ───────────────────────────────────────────────────────────────
def _pg_team_offense(players, opp_hand, hitters, hitter_pct):
    """Mirrors _pgTeamOffense — returns (~runs/game, per-slot detail).

    Per spec: rv = (blended_wOBA / 0.317)^2 · 4.5 × √(wRC+ split/100)
                × spot_weight × (1 + (Power−22)/22·0.12) × (1 + (Eye−10)/10·0.12)
    blended_wOBA = avg(projection, actual-or-xwOBA).
    """
    sum_, n = 0.0, 0
    _detail = []
    for i, p in enumerate((players or [])[:9]):
        nm = p.get("name") if isinstance(p, dict) else p
        h = (hitters.get("hitters") or {}).get(norm_name(nm or ""))
        if not h:
            continue
        proj = h.get("woba")
        act = h.get("xwoba_actual") if h.get("xwoba_actual") is not None else h.get("woba_actual")
        if proj is not None and act is not None:
            w = (proj + act) / 2
        else:
            w = proj if proj is not None else act
        if w is None:
            continue
        rv = (w / PG_LG_WOBA) ** 2 * PG_RG
        split = h.get("wrc_vs_l") if opp_hand == "L" else h.get("wrc_vs_r")
        if split is not None:
            rv *= math.sqrt(_clamp(split / 100.0, 0.5, 1.8))
        hp = ((hitter_pct or {}).get("hitters") or {}).get(norm_name(nm or ""))
        if hp:
            # Power = barrel/HH/xSLG/xISO/bat-speed pctiles avg (need ≥2)
            pwr_vals = [v for v in (hp.get("barrel"), hp.get("hard_hit"), hp.get("xslg"),
                                     hp.get("xiso"), hp.get("bat_speed")) if v is not None]
            if len(pwr_vals) >= 2:
                pPow = sum(pwr_vals) / len(pwr_vals)
                rv *= (1 + ((22 + (pPow - 50) / 50 * 8.4) - 22) / 22 * 0.12)
            # Eye = BB% + chase pctiles avg
            eye_vals = [v for v in (hp.get("bb_pct"), hp.get("chase")) if v is not None]
            if len(eye_vals) >= 1:
                pEye = sum(eye_vals) / len(eye_vals)
                rv *= (1 + ((10 + (pEye - 50) / 50 * 5.14) - 10) / 10 * 0.12)
        wt = (PG_SPOT_WT[i] if i < len(PG_SPOT_WT) else 1)
        _detail.append({"slot": i + 1, "name": nm, "rv": round(rv, 3), "spot_wt": wt})
        rv *= wt
        sum_ += rv
        n += 1
    return ((sum_ / n) if n else None), _detail


def _pg_lineup_kbb(players, hitters):
    s, n = 0.0, 0
    for p in (players or [])[:9]:
        nm = p.get("name") if isinstance(p, dict) else p
        h = (hitters.get("hitters") or {}).get(norm_name(nm or ""))
        if not h:
            continue
        k = h.get("k_pct_actual") if h.get("k_pct_actual") is not None else h.get("k_pct")
        bb = h.get("bb_pct_actual") if h.get("bb_pct_actual") is not None else h.get("bb_pct")
        if k is not None and bb is not None:
            s += (k - bb) / 100.0
            n += 1
    return (s / n) if n else 0.128


def _pg_sum(players, field, hitters):
    s = 0.0
    for p in (players or [])[:9]:
        nm = p.get("name") if isinstance(p, dict) else p
        h = (hitters.get("hitters") or {}).get(norm_name(nm or ""))
        if h and isinstance(h.get(field), (int, float)):
            s += h[field]
    return s


# ── bullpen ───────────────────────────────────────────────────────────────
def _team_bullpen_ra(team_name, pitcher_stats, fatigue_slice):
    """Mirrors _pgTeamBullpenRA + teamAvailRelievers, but operates from a
    fatigue slice (rows of {name, tier}) rather than 26-man roster fetches.

    Per spec: take available (non-LIKELY-OUT) reliever unified_scores and
    average those < 10.
    """
    rows = ((fatigue_slice or {}).get("teams") or {}).get(team_name) or []
    scores = []
    pstats = (pitcher_stats or {}).get("pitchers") or {}
    for r in rows:
        if (r.get("tier") or "").upper() == "LIKELY OUT":
            continue
        s = pstats.get(norm_name(r.get("name") or ""))
        if not s:
            continue
        us = s.get("unified_score")
        # exclude SPs (heuristic — they show up rarely in fatigue rows but guard)
        if us is None or us >= 10:
            continue
        scores.append(us)
    return (sum(scores) / len(scores)) if len(scores) >= 2 else None


def _count_fatigued(team_name, fatigue_slice):
    rows = ((fatigue_slice or {}).get("teams") or {}).get(team_name) or []
    return sum(1 for r in rows if (r.get("tier") or "").upper() == "FATIGUED")


def _pg_staff(sp_name, team_name, opp_lineup_kbb, pitcher_stats, fatigue_slice):
    s = (pitcher_stats.get("pitchers") or {}).get(norm_name(sp_name or "")) or {}
    sp_ra = s.get("unified_score") if (s.get("unified_score") is not None and s.get("unified_score") < 10) \
        else (s.get("fip_proj") or 4.30)
    kbb = s.get("k_bb_pct") if s.get("k_bb_pct") is not None else 0.126
    pq = _clamp((s.get("pitching_plus") if s.get("pitching_plus") is not None else (s.get("stuff_plus") or 100)), 60, 140)
    pqm = 1 - (pq - 100) / 100 * 0.15
    gs = s.get("gs") or 0
    ip = s.get("ip") or 0
    raw = (ip / gs) if gs > 0 else 5.2
    wt = 0 if gs < 5 else min(1.0, gs / 10.0)
    shrunk = raw * wt + 5.2 * (1 - wt)
    ip_f = _clamp(1 - (shrunk - 5.2) / 4, 0.85, 1.15)
    kbb_f = _clamp(1 - (kbb - 0.128) * 2.3, 0.82, 1.18)
    bpf = (ip_f + kbb_f) / 2
    bp_ra_rested = _team_bullpen_ra(team_name, pitcher_stats, fatigue_slice)
    if bp_ra_rested is None:
        bp_ra_rested = 4.20
    n_fatigued = _count_fatigued(team_name, fatigue_slice)
    fatigue_mul = 1 + 0.015 * n_fatigued
    bp_ra = bp_ra_rested * fatigue_mul
    ekbb = kbb + ((opp_lineup_kbb if opp_lineup_kbb is not None else 0.128) - 0.128) * 0.5
    pw = _clamp(0.55 + (ekbb - 0.128) * 2, 0.5, 0.8)
    return {
        "spRA": sp_ra, "bpRA": bp_ra, "bpRArested": bp_ra_rested,
        "pqm": pqm, "bpf": bpf, "pw": pw,
        "spKbb": kbb, "pq": pq, "ipF": ip_f, "kbbF": kbb_f, "ekbb": ekbb,
        "shrunkIpgs": shrunk, "gs": gs, "ip": ip,
        "nFatigued": n_fatigued, "fatigueMul": fatigue_mul,
    }


# ── full model ────────────────────────────────────────────────────────────
def compute_pregame(game, hitters, hitter_pct, pitcher_stats, park_factors, fatigue_slice):
    away_sp = (game.get("teams", {}).get("away", {}).get("probablePitcher") or {}).get("fullName")
    home_sp = (game.get("teams", {}).get("home", {}).get("probablePitcher") or {}).get("fullName")
    if not away_sp or not home_sp:
        return None
    lg = game.get("_lineup") or {}
    ap = ((lg.get("lineups") or {}).get("away") or {}).get("players") or []
    hp = ((lg.get("lineups") or {}).get("home") or {}).get("players") or []
    if not ap or not hp:
        return None
    away_hand = _hand_of_pitcher(away_sp, pitcher_stats) or "R"
    home_hand = _hand_of_pitcher(home_sp, pitcher_stats) or "R"
    away_off, away_lineup = _pg_team_offense(ap, home_hand, hitters, hitter_pct)
    home_off, home_lineup = _pg_team_offense(hp, away_hand, hitters, hitter_pct)
    if away_off is None or home_off is None:
        return None
    home_staff = _pg_staff(home_sp, game["teams"]["home"]["team"]["name"], _pg_lineup_kbb(ap, hitters),
                            pitcher_stats, fatigue_slice)
    away_staff = _pg_staff(away_sp, game["teams"]["away"]["team"]["name"], _pg_lineup_kbb(hp, hitters),
                            pitcher_stats, fatigue_slice)
    home_name = game["teams"]["home"]["team"]["name"]
    home_abbr = game["teams"]["home"]["team"].get("abbreviation") or NAME_TO_ABBR.get(home_name) or ""
    park_key = PARK_FACTOR_ALIAS.get(home_abbr, home_abbr)
    pf_row = (park_factors.get("parks") or {}).get(park_key) or {}
    park = (pf_row.get("park_factor") / 100.0) if pf_row.get("park_factor") else 1.0
    away_bsr = _pg_sum(ap, "bsr", hitters)
    away_def = _pg_sum(ap, "fld", hitters)
    home_bsr = _pg_sum(hp, "bsr", hitters)
    home_def = _pg_sum(hp, "fld", hitters)

    def blend(staff, off):
        staff_ra = (staff["spRA"] * 0.555 + staff["bpRA"] * 0.445 * staff["bpf"]) * staff["pqm"]
        return staff_ra * staff["pw"] + off * (1 - staff["pw"])

    away_runs = (blend(home_staff, away_off) * park + away_bsr - home_def) * 0.96
    home_runs = (blend(away_staff, home_off) * park + home_bsr - away_def) * 1.04
    away_runs = max(0.5, away_runs)
    home_runs = max(0.5, home_runs)
    hp_ = home_runs ** PG_PYTH
    ap_ = away_runs ** PG_PYTH
    home_wp = hp_ / (hp_ + ap_)
    home_for_total = home_runs - (home_runs / 9 * home_wp * (0.7 + 0.3 * home_wp))
    total = away_runs + home_for_total

    away_status = (lg.get("lineups", {}).get("away") or {}).get("status")
    home_status = (lg.get("lineups", {}).get("home") or {}).get("status")
    confirmed = (away_status == "confirmed" and home_status == "confirmed")

    return {
        "away_runs": round(away_runs, 3),
        "home_runs": round(home_runs, 3),
        "home_runs_for_total": round(home_for_total, 3),
        "home_wp": round(home_wp, 4),
        "away_wp": round(1 - home_wp, 4),
        "total": round(total, 3),
        "confirmed": confirmed,
        "lineups_status": {"away": away_status, "home": home_status},
        "components": {
            "away": {
                "offense": round(away_off, 3),
                "lineup": away_lineup,
                "staff": {k: round(v, 4) if isinstance(v, float) else v for k, v in home_staff.items()},
                "BSR": round(away_bsr, 2),
                "DEF": round(away_def, 2),
                "opp_sp": home_sp,
                "opp_hand": home_hand,
            },
            "home": {
                "offense": round(home_off, 3),
                "lineup": home_lineup,
                "staff": {k: round(v, 4) if isinstance(v, float) else v for k, v in away_staff.items()},
                "BSR": round(home_bsr, 2),
                "DEF": round(home_def, 2),
                "opp_sp": away_sp,
                "opp_hand": away_hand,
            },
            "park_factor": pf_row.get("park_factor"),
            "park": round(park, 4),
            "hfa_away": 0.96, "hfa_home": 1.04, "pyth": PG_PYTH,
        },
    }


# ── schedule fetch + driver ───────────────────────────────────────────────
def _fetch_schedule(date_iso):
    url = f"https://statsapi.mlb.com/api/v1/schedule?sportId=1&date={date_iso}&hydrate=probablePitcher"
    req = urllib.request.Request(url, headers={"User-Agent": "u/1.0"})
    with urllib.request.urlopen(req, timeout=30) as r:
        d = json.load(r)
    games = []
    for day in d.get("dates", []):
        for g in day.get("games", []):
            games.append(g)
    return games


def _attach_lineups(games, lineups):
    by_pk = {g.get("game_pk"): g for g in (lineups.get("games") or [])}
    for g in games:
        g["_lineup"] = by_pk.get(g.get("gamePk"))
    return games


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=None, help="ET date (YYYY-MM-DD); defaults to today")
    args = ap.parse_args()
    date_iso = args.date or _et_today_iso()

    hitters = _load_json("hitters.json", {}) or {}
    hitter_pct = _load_json("hitter_percentiles.json", {}) or {}
    pitcher_stats = _load_json("pitcher_stats.json", {}) or {}
    park_factors = _load_json("park_factors.json", {}) or {}
    lineups = _load_json("lineups.json", {}) or {}
    fatigue = _load_json("fatigue.json", {}) or {}
    fatigue_slice = ((fatigue.get("dates") or {}).get(date_iso)) or {}

    if not hitters or not pitcher_stats:
        print(f"[pregame-archive] missing core data files; skipping {date_iso}", file=sys.stderr)
        return 0

    games = _fetch_schedule(date_iso)
    games = _attach_lineups(games, lineups)
    if not games:
        print(f"[pregame-archive] no games scheduled for {date_iso}", file=sys.stderr)
        return 0

    snapshots = {}
    n_with_model = 0
    for g in games:
        pk = g.get("gamePk")
        if not pk:
            continue
        pg = compute_pregame(g, hitters, hitter_pct, pitcher_stats, park_factors, fatigue_slice)
        snap = {
            "game_pk": pk,
            "game_date": (g.get("gameDate") or "")[:10],
            "game_time": g.get("gameDate"),
            "away": g["teams"]["away"]["team"]["name"],
            "home": g["teams"]["home"]["team"]["name"],
            "away_abbr": g["teams"]["away"]["team"].get("abbreviation") or NAME_TO_ABBR.get(g["teams"]["away"]["team"]["name"]),
            "home_abbr": g["teams"]["home"]["team"].get("abbreviation") or NAME_TO_ABBR.get(g["teams"]["home"]["team"]["name"]),
            "venue": (g.get("venue") or {}).get("name"),
            "away_sp": (g["teams"]["away"].get("probablePitcher") or {}).get("fullName"),
            "home_sp": (g["teams"]["home"].get("probablePitcher") or {}).get("fullName"),
            "model": pg,
        }
        if pg is not None:
            n_with_model += 1
        snapshots[str(pk)] = snap

    out_dir = ARCHIVE_DIR / date_iso
    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / "pregame_inputs.json"
    payload = {
        "generated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds"),
        "date": date_iso,
        "n_games": len(snapshots),
        "n_with_model": n_with_model,
        "spec_version": "v9_pregame_2026-05",   # mirrors index.html's computePregame
        "games": snapshots,
    }
    out.write_text(json.dumps(payload, separators=(",", ":")))
    print(f"[pregame-archive] {date_iso}: {n_with_model}/{len(snapshots)} games modeled → {out}",
          file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
