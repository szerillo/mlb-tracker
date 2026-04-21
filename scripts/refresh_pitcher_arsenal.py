#!/usr/bin/env python3
"""
Build data/pitcher_arsenal.json — per-pitcher, per-pitch-type breakdown
(usage %, velo, spin, IVB, HB, whiff %, xwOBA against, hard-hit %).

Data sources (both Savant, no auth):
  1. Pitch-arsenal leaderboard — embedded JSON in the HTML page. Gives physics
     per pitch type (velo, spin, IVB/HB, release point, usage %).
  2. Pitch-arsenal-stats CSV — results per pitch type (whiff %, K %, xwOBA,
     hard-hit %) — merged in by (mlbam_id, pitch_type).

Output schema:
  {
    "generated_at": "...",
    "year": 2026,
    "pitchers": {
      "<norm_name>": {
         "name": "Tarik Skubal",
         "mlbam_id": 669373,
         "team": "DET",
         "hand": "L",
         "total_pitches": 501,
         "pitches": [
           {"type":"FF","name":"4-Seam","usage":47.7,"velo":96.3,"spin":2280,
            "ivb":19.1,"hb":-4.5,"whiff":31.4,"k_pct":30.8,"xwoba":0.280,
            "hard_hit":38.1, "pitches": 239}, ...
         ]
       }, ...
    }
  }

USAGE:
    python scripts/refresh_pitcher_arsenal.py > data/pitcher_arsenal.json
"""
from __future__ import annotations
import csv
import datetime
import io
import json
import os
import re
import sys
import unicodedata
import urllib.request

# Pitch-type codes Savant uses, with human labels
PITCH_NAMES = {
    "FF": "4-Seam",  "SI": "Sinker",  "FC": "Cutter",
    "SL": "Slider",  "ST": "Sweeper", "CU": "Curve",
    "KC": "KnuckleCurve", "SV": "Slurve",
    "CH": "Change",  "FS": "Splitter",
    "FO": "Forkball", "EP": "Eephus", "KN": "Knuckle",
    "SC": "Screwball",
}
# Keys in the HTML data blob are lowercase (ff_avg_speed, n_si_formatted, etc.)
PITCH_KEYS = ["ff", "si", "fc", "sl", "st", "cu", "kc", "sv",
              "ch", "fs", "fo", "ep", "kn", "sc"]

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0 Safari/537.36")


def norm_name(s: str) -> str:
    if not s: return ""
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = s.lower()
    for suf in [" jr.", " jr", " sr.", " sr", " iii", " ii"]:
        if s.endswith(suf):
            s = s[: -len(suf)]
    return s.replace(".", "").strip()


def fetch(url: str, timeout: int = 30) -> str:
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read().decode("utf-8", errors="replace")


def parse_arsenal_html(html: str):
    """Extract `var data = [...]` JSON array from the pitch-arsenals page."""
    m = re.search(r"var\s+data\s*=\s*(\[[\s\S]+?\]);", html)
    if not m:
        return []
    try:
        return json.loads(m.group(1))
    except json.JSONDecodeError as e:
        print(f"[arsenal] JSON parse failed: {e}", file=sys.stderr)
        return []


def _f(v):
    if v is None or v == "" or v == "null":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def build_pitcher_from_row(row: dict) -> dict:
    """Given one pitcher entry from the arsenal HTML JSON, build our shape."""
    name = row.get("name_display_first_last") or row.get("name_display_last_first", "")
    if "," in name:
        last, first = [p.strip() for p in name.split(",", 1)]
        name = f"{first} {last}"
    pitches = []
    for key in PITCH_KEYS:
        usage = _f(row.get(f"n_{key}_formatted"))
        velo = _f(row.get(f"{key}_avg_speed"))
        if not usage and not velo:
            continue
        pitches.append({
            "type": key.upper(),
            "name": PITCH_NAMES.get(key.upper(), key.upper()),
            "usage": usage,
            "velo": velo,
            "spin": _f(row.get(f"{key}_avg_spin")),
            "ivb":  _f(row.get(f"{key}_avg_break_z_induced")),
            "hb":   _f(row.get(f"{key}_avg_break_x")),
        })
    # Sort by usage desc so the primary pitch is first
    pitches.sort(key=lambda p: p.get("usage") or 0, reverse=True)

    return {
        "name": name,
        "mlbam_id": int(row.get("pitcher") or 0) or None,
        "team": row.get("name_abbrev") or row.get("team_abbrev") or "",
        "hand": row.get("pitch_hand", ""),
        "total_pitches": int(_f(row.get("n")) or 0),
        "pitches": pitches,
    }


def merge_results_csv(pitchers: dict, csv_text: str):
    """Merge whiff %, K %, xwOBA, hard-hit % per pitch type from the
    pitch-arsenal-stats CSV (keyed by player_id + pitch_type)."""
    reader = csv.DictReader(io.StringIO(csv_text.lstrip("\ufeff")))
    by_pitcher_pitch = {}
    for r in reader:
        pid = r.get("player_id")
        pt = r.get("pitch_type", "").upper()
        if not pid or not pt:
            continue
        by_pitcher_pitch[(int(pid), pt)] = {
            "whiff":     _f(r.get("whiff_percent")),
            "k_pct":     _f(r.get("k_percent")),
            "xwoba":     _f(r.get("est_woba")),
            "hard_hit":  _f(r.get("hard_hit_percent")),
            "pitches":   int(_f(r.get("pitches")) or 0),
            "put_away":  _f(r.get("put_away")),
        }
    merged = 0
    for key, p in pitchers.items():
        pid = p.get("mlbam_id")
        if not pid:
            continue
        for pitch in p["pitches"]:
            stat = by_pitcher_pitch.get((pid, pitch["type"]))
            if not stat:
                continue
            for fld in ("whiff", "k_pct", "xwoba", "hard_hit", "pitches", "put_away"):
                if stat[fld] is not None:
                    pitch[fld] = stat[fld]
            merged += 1
    return merged


def main():
    year = int(os.environ.get("SAVANT_YEAR", datetime.date.today().year))

    # 1) Physics (velo, IVB, HB, spin, usage %) per pitch type
    arsenal_url = (f"https://baseballsavant.mlb.com/leaderboard/pitch-arsenals"
                   f"?type=pitcher&year={year}")
    print(f"[arsenal] fetching {arsenal_url}", file=sys.stderr)
    try:
        html = fetch(arsenal_url)
    except Exception as e:
        print(f"[arsenal] HTTP failed: {e}", file=sys.stderr)
        sys.exit(1)
    rows = parse_arsenal_html(html)
    if not rows:
        print("[arsenal] no rows; maybe HTML shape changed", file=sys.stderr)
        sys.exit(1)

    pitchers = {}
    for r in rows:
        p = build_pitcher_from_row(r)
        if not p["name"] or not p["pitches"]:
            continue
        key = norm_name(p["name"])
        pitchers[key] = p
    print(f"[arsenal] parsed {len(pitchers)} pitchers from physics feed",
          file=sys.stderr)

    # 2) Per-pitch results — whiff %, K %, xwOBA, hard-hit %
    stats_url = (f"https://baseballsavant.mlb.com/leaderboard/pitch-arsenal-stats"
                 f"?type=pitcher&pitchType=&year={year}&min=1&csv=true")
    try:
        csv_text = fetch(stats_url)
        merged = merge_results_csv(pitchers, csv_text)
        print(f"[arsenal] merged result stats on {merged} (pitcher,pitch) pairs",
              file=sys.stderr)
    except Exception as e:
        print(f"[arsenal] stats CSV failed (kept physics only): {e}",
              file=sys.stderr)

    payload = {
        "generated_at": datetime.datetime.utcnow().isoformat() + "Z",
        "year": year,
        "source": "Savant pitch-arsenals + pitch-arsenal-stats",
        "pitchers": pitchers,
    }
    json.dump(payload, sys.stdout, indent=2)


if __name__ == "__main__":
    main()
