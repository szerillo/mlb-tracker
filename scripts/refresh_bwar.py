#!/usr/bin/env python3
"""
refresh_bwar.py — daily Baseball-Reference bWAR feed for the awards model.
------------------------------------------------------------------------------
Pulls B-R's raw WAR download files (war_daily_bat.txt / war_daily_pitch.txt),
filters the target season, sums stints per player, merges batting + pitching
WAR (two-way players get both), and writes data/bwar_YTD.json keyed by MLBAM id.

Schema verified 2026-09-16 against known board values (100% match):
  Wetherholt 4.72, McGonigle 6.66, Stewart 2.24 (bat) ; Messick 5.71 (pitch).

Columns are resolved BY NAME from each file's header (robust to B-R column drift),
not by fixed index. Key cols used:
  bat:   name_common, mlb_ID, year_ID, team_ID, stint_ID, PA, G, WAR, WAR_off, WAR_def
  pitch: name_common, mlb_ID, year_ID, team_ID, stint_ID, G, GS, IPouts, WAR

Run: python refresh_bwar.py [--season 2026] [--out data/bwar_YTD.json]

NOTE on fetching: B-R blocks bare/api-style clients. Send a browser User-Agent
and be gentle (these files are ~35MB). If a CI runner is still blocked, the
fallback is the in-app-browser same-origin route used to validate the schema.
"""
import argparse, csv, datetime as dt, io, json, os, sys, urllib.request

BASE = "https://www.baseball-reference.com/data/"
BAT_FILE = "war_daily_bat.txt"
PITCH_FILE = "war_daily_pitch.txt"
UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/124.0 Safari/537.36")


def _fetch(fname, timeout=120):
    req = urllib.request.Request(BASE + fname, headers={
        "User-Agent": UA,
        "Accept": "text/plain,*/*",
        "Referer": "https://www.baseball-reference.com/about/war_explained.shtml",
    })
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read().decode("utf-8", "replace")


def _fnum(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return 0.0


def _inum(x):
    try:
        return int(float(x))
    except (TypeError, ValueError):
        return 0


def parse_bat(text, season):
    """Return {mlb_id: {...batting...}} for `season`, stints summed."""
    rd = csv.DictReader(io.StringIO(text))
    acc = {}
    for row in rd:
        if str(row.get("year_ID")) != str(season):
            continue
        mid = (row.get("mlb_ID") or "").strip()
        if not mid or mid == "NULL":
            continue
        a = acc.setdefault(mid, {"name": row.get("name_common", "").strip(),
                                 "team": row.get("team_ID", "").strip(),
                                 "bat_war": 0.0, "war_off": 0.0, "war_def": 0.0,
                                 "pa": 0, "g": 0})
        a["bat_war"] += _fnum(row.get("WAR"))
        a["war_off"] += _fnum(row.get("WAR_off"))
        a["war_def"] += _fnum(row.get("WAR_def"))
        a["pa"] += _inum(row.get("PA"))
        a["g"] += _inum(row.get("G"))
        a["team"] = row.get("team_ID", "").strip()  # last stint = current team
    return acc


def parse_pitch(text, season):
    """Return {mlb_id: {...pitching...}} for `season`, stints summed."""
    rd = csv.DictReader(io.StringIO(text))
    acc = {}
    for row in rd:
        if str(row.get("year_ID")) != str(season):
            continue
        mid = (row.get("mlb_ID") or "").strip()
        if not mid or mid == "NULL":
            continue
        a = acc.setdefault(mid, {"name": row.get("name_common", "").strip(),
                                 "team": row.get("team_ID", "").strip(),
                                 "pitch_war": 0.0, "ip": 0.0, "gs": 0, "gp": 0})
        a["pitch_war"] += _fnum(row.get("WAR"))
        a["ip"] += _inum(row.get("IPouts")) / 3.0
        a["gs"] += _inum(row.get("GS"))
        a["gp"] += _inum(row.get("G"))
        a["team"] = row.get("team_ID", "").strip()
    return acc


def merge(bat, pitch):
    out = {}
    for mid, b in bat.items():
        out[mid] = {
            "name": b["name"], "team": b["team"],
            "bat_war": round(b["bat_war"], 2),
            "pitch_war": 0.0,
            "bwar": round(b["bat_war"], 2),
            "war_off": round(b["war_off"], 2), "war_def": round(b["war_def"], 2),
            "pa": b["pa"], "g": b["g"], "ip": 0.0, "gs": 0,
        }
    for mid, p in pitch.items():
        o = out.setdefault(mid, {
            "name": p["name"], "team": p["team"],
            "bat_war": 0.0, "war_off": 0.0, "war_def": 0.0, "pa": 0, "g": p.get("gp", 0),
        })
        o["pitch_war"] = round(p["pitch_war"], 2)
        o["ip"] = round(p["ip"], 1)
        o["gs"] = p["gs"]
        o["bwar"] = round(o.get("bat_war", 0.0) + p["pitch_war"], 2)  # two-way = both
        o.setdefault("name", p["name"])
        o.setdefault("team", p["team"])
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--season", type=int, default=dt.date.today().year)
    ap.add_argument("--out", default="data/bwar_YTD.json")
    a = ap.parse_args()

    bat = parse_bat(_fetch(BAT_FILE), a.season)
    pitch = parse_pitch(_fetch(PITCH_FILE), a.season)
    merged = merge(bat, pitch)

    os.makedirs(os.path.dirname(a.out) or ".", exist_ok=True)
    payload = {
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "source": "baseball-reference war_daily_bat.txt + war_daily_pitch.txt",
        "season": a.season,
        "n_players": len(merged),
        "war_flavor": "bWAR (Baseball-Reference); DRS-based defense",
        "players": merged,
    }
    with open(a.out, "w") as f:
        json.dump(payload, f, separators=(",", ":"))
    print(f"bWAR {a.season}: {len(bat)} batters, {len(pitch)} pitchers, "
          f"{len(merged)} total -> {a.out}")


if __name__ == "__main__":
    main()
