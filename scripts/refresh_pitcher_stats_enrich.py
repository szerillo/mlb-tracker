#!/usr/bin/env python3
"""
Enrich data/pitcher_stats.json with xFIP and Stuff+ / Pitching+ grades from
Fangraphs. Runs AFTER your existing refresh_pitcher_stats script.

Fangraphs leaderboards expose a JSON endpoint used by their own UI — no API
key needed but the parameters occasionally change. If this breaks, open the
leaders page in DevTools → Network and grab the current shape.

OUTPUT (appends to existing fields):
    fip_proj, xera, era, woba, xwoba, k_pct, bb_pct, k_bb_pct, hand, mlbam_id,
    # NEW:
    xfip, stuff_plus, pitching_plus, ip

Front-end logic (handled separately in index.html):
    - If xfip is present AND ip >= 30, display xFIP primary, pFIP fallback.
    - Stuff+ / Pitching+ show as extra chips in the modal SP box.

USAGE:
    pip install requests unidecode
    python scripts/refresh_pitcher_stats_enrich.py data/pitcher_stats.json \\
        > data/pitcher_stats.new.json
    mv data/pitcher_stats.new.json data/pitcher_stats.json

Set FG_SEASON env var to override the season (defaults to current year).
"""

from __future__ import annotations
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from typing import Dict, List

import requests
from unidecode import unidecode


# Fangraphs leaderboard JSON endpoint. `stats=pit` = pitching.
# `type=8` is "Standard"; `type=-1` plus qualifiers of interest gets you
# advanced metrics. Use the `StatsType` query to get xFIP and pitch models.
#
# This URL pattern mirrors what the FG front-end calls. If it 403s, add a
# Referer header matching the leaders page.
FG_URL = ("https://www.fangraphs.com/api/leaders/major-league/data"
          "?pos=all&stats=pit&lg=all&qual=0&season={season}"
          "&month=0&season1={season}&ind=0"
          "&type=c,-1,4,42,5,40,6,62,3,24,43,41,119,120&team=0,ts")
# Types cheat-sheet (comma-separated column indices FG uses internally):
#   4 = Age, 5 = W, 6 = L, 24 = IP, 40 = K/9, 41 = BB/9, 42 = HR/9
#   62 = xFIP, 119 = Stuff+, 120 = Pitching+, 43 = FIP, 3 = ERA
# If the mapping changes, open the leaderboard in a browser and inspect.

UA = "Mozilla/5.0 (compatible; mlb-tracker/1.0)"
MIN_IP_FOR_XFIP_PRIMARY = 30  # consumed client-side


def norm_name(s: str) -> str:
    s = unidecode(s or "").lower()
    s = re.sub(r"\s+jr\.?$|\s+sr\.?$|\s+iii$|\s+ii$", "", s)
    s = s.replace(".", "").strip()
    return s


def fetch_fg(season: int) -> List[dict]:
    url = FG_URL.format(season=season)
    r = requests.get(url, timeout=30, headers={
        "User-Agent": UA,
        "Referer": "https://www.fangraphs.com/leaders/major-league",
        "Accept": "application/json",
    })
    r.raise_for_status()
    data = r.json()
    # FG returns either {"data": [...]} or just a list depending on endpoint
    return data.get("data", data) if isinstance(data, dict) else data


def build_enrichment(rows: List[dict]) -> Dict[str, dict]:
    """Returns {norm_name: {xfip, stuff_plus, pitching_plus, ip}}"""
    out: Dict[str, dict] = {}
    for r in rows:
        nm = r.get("PlayerName") or r.get("playerName") or ""
        if not nm:
            continue
        # FG's JSON keys vary; accept a few aliases.
        def pick(*keys):
            for k in keys:
                if k in r and r[k] not in (None, ""):
                    return r[k]
            return None

        ip = pick("IP", "ip")
        try:
            ip_val = float(ip) if ip is not None else None
        except (TypeError, ValueError):
            ip_val = None

        enrich = {
            "xfip": pick("xFIP", "xfip"),
            "stuff_plus": pick("Stuff+", "stuff_plus", "sp_stuff"),
            "pitching_plus": pick("Pitching+", "pitching_plus", "sp_pitching"),
            "ip": ip_val,
        }
        # Only keep entries that have at least one enrichment field
        if any(v is not None for v in enrich.values()):
            out[norm_name(nm)] = enrich
    return out


def main():
    if len(sys.argv) < 2:
        print("Usage: refresh_pitcher_stats_enrich.py data/pitcher_stats.json", file=sys.stderr)
        sys.exit(1)

    infile = sys.argv[1]
    season = int(os.environ.get("FG_SEASON", datetime.now().year))

    with open(infile) as f:
        ps = json.load(f)

    try:
        rows = fetch_fg(season)
    except Exception as e:
        print(f"[refresh_pitcher_stats_enrich] FG fetch failed: {e}", file=sys.stderr)
        # Don't crash the pipeline — just emit original data unchanged
        json.dump(ps, sys.stdout, indent=2)
        return

    enrich = build_enrichment(rows)

    # Merge into existing pitcher entries
    pitchers = ps.get("pitchers", {})
    matched = 0
    for key, entry in pitchers.items():
        e = enrich.get(key)
        if e:
            for fld in ("xfip", "stuff_plus", "pitching_plus", "ip"):
                if e.get(fld) is not None:
                    entry[fld] = e[fld]
            matched += 1

    ps["enriched_at"] = datetime.now(timezone.utc).isoformat()
    ps["enriched_count"] = matched
    if "thresholds" in ps:
        # xFIP lower-is-better, same general bands as FIP
        ps["thresholds"].setdefault("xfip",
            {"elite": 3.25, "good": 3.75, "bad": 4.25, "worst": 4.75})

    json.dump(ps, sys.stdout, indent=2)
    print(f"Enriched {matched}/{len(pitchers)} pitchers with xFIP/Stuff+/Pitching+",
          file=sys.stderr)


if __name__ == "__main__":
    main()
