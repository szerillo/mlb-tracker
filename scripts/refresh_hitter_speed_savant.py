#!/usr/bin/env python3
"""
Pull Baseball Savant sprint speed (ft/sec) and add a per-hitter speed
percentile. Hitters at >= the cutoff percentile get a `sprint_elite: true`
flag which the front-end can render as a BSR-speed chip.

Savant's sprint_speed leaderboard is NOT Cloudflare-protected and returns
CSV. Free, no key needed.

Adds these fields to each hitter in hitters.json:
    sprint_speed    — ft/s (e.g., 28.7)
    sprint_pct      — percentile rank 0-100 across all leaderboard players
    sprint_elite    — true if sprint_pct >= SPEED_ELITE_CUTOFF (default 80)

Cutoff is configurable via env var SPEED_ELITE_CUTOFF (default 80).

USAGE:
    pip install requests unidecode
    python scripts/refresh_hitter_speed_savant.py data/hitters.json > /tmp/h.json
    mv /tmp/h.json data/hitters.json
"""

from __future__ import annotations
import csv
import io
import json
import os
import re
import sys
from datetime import datetime, timezone
from typing import Dict

try:
    import requests
except ImportError:
    print("requests not installed", file=sys.stderr); sys.exit(1)

try:
    from unidecode import unidecode
except ImportError:
    def unidecode(s): return s

SAVANT_URL_TMPL = ("https://baseballsavant.mlb.com/leaderboard/sprint_speed"
                   "?year={year}&position=&team=&min=0&csv=true")
UA = "Mozilla/5.0 (compatible; mlb-tracker/1.0)"
SPEED_ELITE_CUTOFF = int(os.environ.get("SPEED_ELITE_CUTOFF", "80"))


def norm_name(s: str) -> str:
    s = unidecode(s or "").lower()
    s = re.sub(r"\s+jr\.?$|\s+sr\.?$|\s+iii$|\s+ii$", "", s)
    s = s.replace(".", "").strip()
    return s


def savant_name_to_std(name_from_csv: str) -> str:
    # Savant format: "Lastname, Firstname"
    if "," in name_from_csv:
        last, first = [p.strip() for p in name_from_csv.split(",", 1)]
        return f"{first} {last}"
    return name_from_csv


def fetch_sprint_speed(year: int):
    url = SAVANT_URL_TMPL.format(year=year)
    r = requests.get(url, timeout=30, headers={"User-Agent": UA})
    r.raise_for_status()
    # Strip BOM if present
    text = r.text.lstrip("\ufeff")
    reader = csv.DictReader(io.StringIO(text))
    rows = []
    for row in reader:
        nm = row.get("last_name, first_name") or row.get("\ufefflast_name, first_name") or ""
        spd = row.get("sprint_speed")
        try:
            spd_f = float(spd) if spd else None
        except ValueError:
            spd_f = None
        if nm and spd_f is not None:
            rows.append({
                "name_raw": nm,
                "std_name": savant_name_to_std(nm),
                "sprint_speed": spd_f,
                "key": norm_name(savant_name_to_std(nm)),
            })
    return rows


def compute_percentiles(rows):
    rows_sorted = sorted(rows, key=lambda r: r["sprint_speed"])
    n = len(rows_sorted)
    for i, r in enumerate(rows_sorted):
        r["sprint_pct"] = round(100.0 * (i + 0.5) / n, 1)
    return rows_sorted


def main():
    if len(sys.argv) < 2:
        print("Usage: refresh_hitter_speed_savant.py data/hitters.json", file=sys.stderr)
        sys.exit(1)

    with open(sys.argv[1]) as f:
        hs = json.load(f)

    year = int(os.environ.get("SAVANT_YEAR", datetime.now().year))
    try:
        rows = fetch_sprint_speed(year)
    except Exception as e:
        # Try previous year as fallback (early-season)
        print(f"[savant] {year} fetch failed: {e}; trying {year-1}", file=sys.stderr)
        try:
            rows = fetch_sprint_speed(year - 1)
        except Exception as e2:
            print(f"[savant] {year-1} also failed: {e2}; emitting unchanged",
                  file=sys.stderr)
            json.dump(hs, sys.stdout, indent=2)
            return

    rows = compute_percentiles(rows)
    lookup = {r["key"]: r for r in rows}

    matched = 0
    for key, entry in (hs.get("hitters") or {}).items():
        r = lookup.get(key)
        if not r:
            continue
        entry["sprint_speed"] = r["sprint_speed"]
        entry["sprint_pct"] = r["sprint_pct"]
        entry["sprint_elite"] = r["sprint_pct"] >= SPEED_ELITE_CUTOFF
        matched += 1

    hs["speed_enriched_at"] = datetime.now(timezone.utc).isoformat()
    hs["speed_enriched_count"] = matched
    hs["speed_cutoff_pct"] = SPEED_ELITE_CUTOFF

    json.dump(hs, sys.stdout, indent=2)
    print(f"Matched sprint speed for {matched} hitters "
          f"(cutoff: {SPEED_ELITE_CUTOFF}th pct)", file=sys.stderr)


if __name__ == "__main__":
    main()
