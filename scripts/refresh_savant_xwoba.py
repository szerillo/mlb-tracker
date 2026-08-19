#!/usr/bin/env python3
"""
refresh_savant_xwoba.py — daily Baseball Savant true xwOBA (est_woba) pull.

Replaces the hand-supplied savant_true_xwoba.json bridge. Writes the same
schema the STAFF_OFF_v2 offense pipeline reads:
    data/savant_true_xwoba.json  ->  { "<mlbam_id>": {"pa": int, "xwoba": float}, ... }

Source: Savant expected_statistics leaderboard CSV (columns player_id, pa,
est_woba). Join key player_id == MLBAM id. Endpoint is reachable from GH
runners (unlike FanGraphs). Fails safe: on any error or an implausibly small
pull, keeps the previous file so a bad fetch never blanks the offense feed.
"""
from __future__ import annotations
import csv, io, json, sys, datetime, urllib.request
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
OUT       = REPO_ROOT / "data" / "savant_true_xwoba.json"
SEASON    = datetime.date.today().year
URL = ("https://baseballsavant.mlb.com/leaderboard/expected_statistics"
       f"?type=batter&year={SEASON}&position=&team=&filterType=bip&min=1&csv=true")
MIN_ROWS  = 200          # sanity floor; a real pull is ~440 batters

def fetch_csv(url):
    req = urllib.request.Request(url, headers={"User-Agent": "mlb-tracker/1.0"})
    with urllib.request.urlopen(req, timeout=60) as r:
        return r.read().decode("utf-8-sig")

def main():
    try:
        text = fetch_csv(URL)
        rows = list(csv.DictReader(io.StringIO(text)))
    except Exception as e:
        print(f"[savant-xwoba] fetch failed: {e}; keeping previous", file=sys.stderr)
        return 0 if OUT.exists() else 1

    out = {}
    for r in rows:
        pid = (r.get("player_id") or "").strip()
        xw  = r.get("est_woba")
        pa  = r.get("pa")
        if not pid or xw in (None, ""):
            continue
        try:
            out[pid] = {"pa": int(float(pa)), "xwoba": round(float(xw), 3)}
        except (TypeError, ValueError):
            continue

    if len(out) < MIN_ROWS:
        print(f"[savant-xwoba] only {len(out)} rows (<{MIN_ROWS}); keeping previous",
              file=sys.stderr)
        return 0 if OUT.exists() else 1

    payload = {"generated_at": datetime.datetime.now(datetime.timezone.utc)
                                .isoformat(timespec="seconds"),
               "season": SEASON, "source": "savant_expected_statistics",
               "n": len(out), "players": out}
    # Written flat ({mlbam: {...}}) at top level so the offense pipeline reads it
    # directly; the meta lives alongside under reserved keys.
    flat = dict(out)
    flat["_meta"] = {k: payload[k] for k in ("generated_at", "season", "source", "n")}
    OUT.write_text(json.dumps(flat, separators=(",", ":")))
    print(f"[savant-xwoba] wrote {OUT} ({len(out)} hitters)", file=sys.stderr)
    return 0

if __name__ == "__main__":
    sys.exit(main())
