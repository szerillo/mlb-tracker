#!/usr/bin/env python3
"""
Enrich data/hitters.json with projected OPS (and ISO, if available) from
Fangraphs' batter projection leaderboard.

Output adds per-hitter:
    ops           — projected OPS (float, e.g. 0.842)
    iso           — projected ISO (optional, slugging-minus-AVG proxy)
    pa            — projected PA (for sample-size sanity)

The front-end already has a thresholds.ops tier built in (.900/.800/.700/.650).
Once this script runs, the OPS column on the Lineups tab lights up.

USAGE:
    pip install requests unidecode
    python scripts/refresh_hitter_stats_enrich.py data/hitters.json \\
        > data/hitters.new.json
    mv data/hitters.new.json data/hitters.json

Wire this into the morning refresh AFTER any existing script that produces
the base hitters.json.
"""

from __future__ import annotations
import json
import os
import re
import sys
from datetime import datetime, timezone
from typing import Dict, List

import requests
from unidecode import unidecode


# Fangraphs projections leaderboard (ATC default).
# type=8 is "Standard projections". Switch to 'fangraphsdc' (depth charts) or
# 'atc' / 'steamer' / 'zips' via `projection` param.
FG_URL = ("https://www.fangraphs.com/api/projections"
          "?type=atc&team=0&lg=all&players=0&pos=all&stats=bat")
UA = "Mozilla/5.0 (compatible; mlb-tracker/1.0)"


def norm_name(s: str) -> str:
    s = unidecode(s or "").lower()
    s = re.sub(r"\s+jr\.?$|\s+sr\.?$|\s+iii$|\s+ii$", "", s)
    s = s.replace(".", "").strip()
    return s


def fetch_fg() -> List[dict]:
    r = requests.get(FG_URL, timeout=30, headers={
        "User-Agent": UA,
        "Referer": "https://www.fangraphs.com/projections.aspx",
        "Accept": "application/json",
    })
    r.raise_for_status()
    data = r.json()
    return data.get("data", data) if isinstance(data, dict) else data


def build_enrichment(rows: List[dict]) -> Dict[str, dict]:
    out: Dict[str, dict] = {}
    for r in rows:
        nm = r.get("PlayerName") or r.get("playerName") or r.get("Name") or ""
        if not nm:
            continue

        def pick(*keys):
            for k in keys:
                if k in r and r[k] not in (None, ""):
                    return r[k]
            return None

        def to_float(v):
            try:
                return float(v) if v is not None else None
            except (TypeError, ValueError):
                return None

        ops = to_float(pick("OPS", "ops"))
        iso = to_float(pick("ISO", "iso"))
        pa = to_float(pick("PA", "pa"))

        enrich = {}
        if ops is not None: enrich["ops"] = ops
        if iso is not None: enrich["iso"] = iso
        if pa  is not None: enrich["pa"]  = pa

        if enrich:
            out[norm_name(nm)] = enrich
    return out


def main():
    if len(sys.argv) < 2:
        print("Usage: refresh_hitter_stats_enrich.py data/hitters.json", file=sys.stderr)
        sys.exit(1)

    with open(sys.argv[1]) as f:
        hs = json.load(f)

    try:
        rows = fetch_fg()
    except Exception as e:
        print(f"[refresh_hitter_stats_enrich] FG fetch failed: {e}", file=sys.stderr)
        json.dump(hs, sys.stdout, indent=2)
        return

    enrich = build_enrichment(rows)
    hitters = hs.get("hitters", {})
    matched = 0
    for key, entry in hitters.items():
        e = enrich.get(key)
        if e:
            for fld in ("ops", "iso", "pa"):
                if fld in e:
                    entry[fld] = e[fld]
            matched += 1

    hs["enriched_at"] = datetime.now(timezone.utc).isoformat()
    hs["enriched_count"] = matched
    # Standard OPS tier bands — lines up with what the front-end uses if
    # no thresholds.ops is set there yet.
    if "thresholds" in hs:
        hs["thresholds"].setdefault("ops",
            {"elite": 0.900, "good": 0.800, "bad": 0.700, "worst": 0.650})

    json.dump(hs, sys.stdout, indent=2)
    print(f"Enriched {matched}/{len(hitters)} hitters with OPS/ISO/PA",
          file=sys.stderr)


if __name__ == "__main__":
    main()
