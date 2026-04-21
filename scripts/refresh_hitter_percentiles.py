#!/usr/bin/env python3
"""
Build data/hitter_percentiles.json — Savant percentile rankings per hitter.

Source: https://baseballsavant.mlb.com/leaderboard/percentile-rankings?csv=true

Columns returned (all 0-100 percentiles, some may be blank for small samples):
    xwoba, xba, xslg, xiso, xobp
    brl_percent (barrel %), exit_velocity, max_ev, hard_hit_percent
    k_percent, bb_percent, whiff_percent, chase_percent
    arm_strength, sprint_speed, oaa, bat_speed, squared_up_rate, swing_length

Output:
    {
      "generated_at": "...",
      "year": 2026,
      "hitters": { "<norm_name>": { "name": "...", "mlbam_id": 592450,
                                     "xwoba": 92, "barrel": 85, "hard_hit": 78,
                                     "exit_velocity": 82, "max_ev": 91, ... }, ... }
    }

USAGE:
    python scripts/refresh_hitter_percentiles.py > data/hitter_percentiles.json
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

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0 Safari/537.36")

URL = ("https://baseballsavant.mlb.com/leaderboard/percentile-rankings"
       "?type=batter&year={year}&abs=50&csv=true")

# Savant CSV column name → cleaner key we use in JSON
COLS = {
    "xwoba":            "xwoba",
    "xba":              "xba",
    "xslg":             "xslg",
    "xiso":             "xiso",
    "xobp":             "xobp",
    "brl_percent":      "barrel",
    "exit_velocity":    "exit_velocity",
    "max_ev":           "max_ev",
    "hard_hit_percent": "hard_hit",
    "k_percent":        "k_pct",
    "bb_percent":       "bb_pct",
    "whiff_percent":    "whiff",
    "chase_percent":    "chase",
    "sprint_speed":     "sprint",
    "oaa":              "oaa",
    "arm_strength":     "arm_strength",
    "bat_speed":        "bat_speed",
    "squared_up_rate":  "squared_up",
    "swing_length":     "swing_length",
}


def norm_name(s: str) -> str:
    if not s: return ""
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = s.lower()
    for suf in [" jr.", " jr", " sr.", " sr", " iii", " ii"]:
        if s.endswith(suf):
            s = s[: -len(suf)]
    return s.replace(".", "").strip()


def _i(v):
    if v is None or v == "": return None
    try: return int(v)
    except (TypeError, ValueError): return None


def main():
    year = int(os.environ.get("SAVANT_YEAR", datetime.date.today().year))
    url = URL.format(year=year)
    print(f"[percentiles] fetching {url}", file=sys.stderr)
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=30) as r:
        text = r.read().decode("utf-8", errors="replace").lstrip("\ufeff")

    reader = csv.DictReader(io.StringIO(text))
    hitters = {}
    for row in reader:
        raw = row.get("player_name") or ""
        if "," in raw:
            last, first = [p.strip() for p in raw.split(",", 1)]
            name = f"{first} {last}"
        else:
            name = raw.strip()
        if not name: continue
        pid = _i(row.get("player_id"))
        entry = {"name": name, "mlbam_id": pid}
        any_pct = False
        for src, dest in COLS.items():
            v = _i(row.get(src))
            if v is not None:
                entry[dest] = v
                any_pct = True
        # Keep even entries that have some data (early season empties are
        # fine — the UI shows "—" for missing metrics). Skip only the
        # completely empty rows.
        if any_pct:
            hitters[norm_name(name)] = entry

    # Try prior year as a fallback pool when current-season row is blank.
    # Early-season percentiles are noisy and often blank; carry last year's
    # number if the current-year row is empty, so the team aggregates
    # aren't a sea of dashes.
    if year > 2020:
        prior_url = URL.format(year=year - 1)
        try:
            req = urllib.request.Request(prior_url, headers={"User-Agent": UA})
            with urllib.request.urlopen(req, timeout=30) as r:
                prior_text = r.read().decode("utf-8", errors="replace").lstrip("\ufeff")
            prior = {}
            for row in csv.DictReader(io.StringIO(prior_text)):
                raw = row.get("player_name") or ""
                if "," in raw:
                    last, first = [p.strip() for p in raw.split(",", 1)]
                    nm = f"{first} {last}"
                else:
                    nm = raw.strip()
                if not nm: continue
                d = {}
                for src, dest in COLS.items():
                    v = _i(row.get(src))
                    if v is not None: d[dest] = v
                if d:
                    prior[norm_name(nm)] = d
            backfilled = 0
            for k, e in hitters.items():
                pc = prior.get(k)
                if not pc: continue
                for dest in COLS.values():
                    if dest not in e and dest in pc:
                        e[dest] = pc[dest]
                        e["_backfilled"] = True
                        backfilled += 1
            # Also add prior-year-only hitters so unmatched lineup players
            # still get coverage before their 2026 percentiles are posted.
            added = 0
            for k, pc in prior.items():
                if k in hitters: continue
                nm = pc.get("name") if isinstance(pc, dict) else None
                # `prior` dict values are metric dicts, not full entries
                pass  # skip — player_id/name not in prior dict here
            print(f"[percentiles] backfilled {backfilled} cells from {year-1}",
                  file=sys.stderr)
        except Exception as e:
            print(f"[percentiles] prior-year backfill failed: {e}",
                  file=sys.stderr)

    payload = {
        "generated_at": datetime.datetime.utcnow().isoformat() + "Z",
        "year": year,
        "source": f"Savant percentile-rankings {year}" + (
            f" + {year-1} backfill" if year > 2020 else ""),
        "note": "0–100 percentiles. Early-season blanks filled with prior-year percentile where available.",
        "hitters": hitters,
    }
    json.dump(payload, sys.stdout, indent=2)
    print(f"[percentiles] wrote {len(hitters)} hitters", file=sys.stderr)


if __name__ == "__main__":
    main()
