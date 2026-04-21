#!/usr/bin/env python3
"""
Enrich data/hitters.json with:
    1. Projected OPS / ISO / PA from Fangraphs ATC projections.
    2. Savant sprint speed + percentile + sprint_elite flag (80th pct+).

Savant is NOT Cloudflare-protected so that fetch is reliable. The FG call
frequently fails from GitHub Actions runners due to Cloudflare interstitial
— the script continues gracefully, keeping existing fields unchanged.

USAGE:
    pip install requests unidecode
    python scripts/refresh_hitter_stats_enrich.py data/hitters.json > /tmp/h.json
    mv /tmp/h.json data/hitters.json

This is the ONLY hitter-enrichment script — it handles both projections and
live Savant speed data. The workflow wires it in once and both data sources
refresh together.
"""

from __future__ import annotations
import csv
import io
import json
import os
import re
import sys
from datetime import datetime, timezone
from typing import Dict, List

try:
    import requests
    REQ_OK = True
except ImportError:
    REQ_OK = False

try:
    from unidecode import unidecode
except ImportError:
    def unidecode(s): return s


UA = "Mozilla/5.0 (compatible; mlb-tracker/1.0)"
UA_DESKTOP = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
              "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

# Fangraphs ATC projections JSON endpoint (usually Cloudflare-protected).
FG_URL = ("https://www.fangraphs.com/api/projections"
          "?type=atc&team=0&lg=all&players=0&pos=all&stats=bat")

# Savant sprint speed (reliable, no auth)
SAVANT_URL_TMPL = ("https://baseballsavant.mlb.com/leaderboard/sprint_speed"
                   "?year={year}&position=&team=&min=0&csv=true")

SPEED_ELITE_CUTOFF = int(os.environ.get("SPEED_ELITE_CUTOFF", "80"))


def norm_name(s: str) -> str:
    s = unidecode(s or "").lower()
    s = re.sub(r"\s+jr\.?$|\s+sr\.?$|\s+iii$|\s+ii$", "", s)
    s = s.replace(".", "").strip()
    return s


# ----------------------------------------------------------------------------
# SOURCE 1 — Fangraphs ATC OPS projections
# ----------------------------------------------------------------------------

def fetch_fg_projections():
    if not REQ_OK:
        return []
    try:
        r = requests.get(FG_URL, timeout=30, headers={
            "User-Agent": UA_DESKTOP,
            "Referer": "https://www.fangraphs.com/projections.aspx",
            "Accept": "application/json, text/plain, */*",
        })
        if not r.ok:
            print(f"[fg-proj] blocked (HTTP {r.status_code})", file=sys.stderr)
            return []
        d = r.json()
        rows = d.get("data", d) if isinstance(d, dict) else d
        return rows if isinstance(rows, list) else []
    except Exception as e:
        print(f"[fg-proj] failed: {e}", file=sys.stderr)
        return []


def build_projection_enrichment(rows: List[dict]) -> Dict[str, dict]:
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
        def to_f(v):
            try: return float(v) if v is not None else None
            except (TypeError, ValueError): return None
        e = {}
        ops = to_f(pick("OPS", "ops"))
        iso = to_f(pick("ISO", "iso"))
        pa  = to_f(pick("PA", "pa"))
        # K% / BB% — FG endpoints return either as fraction (0.22) or
        # percent (22.0). Normalize to percent (22.0).
        k_pct  = to_f(pick("K%", "k_pct", "SO%", "so_pct"))
        bb_pct = to_f(pick("BB%", "bb_pct"))
        if k_pct  is not None and k_pct  < 1: k_pct  = k_pct  * 100
        if bb_pct is not None and bb_pct < 1: bb_pct = bb_pct * 100
        if ops    is not None: e["ops"]    = ops
        if iso    is not None: e["iso"]    = iso
        if pa     is not None: e["pa"]     = pa
        if k_pct  is not None: e["k_pct"]  = round(k_pct, 1)
        if bb_pct is not None: e["bb_pct"] = round(bb_pct, 1)
        if e:
            out[norm_name(nm)] = e
    return out


# ----------------------------------------------------------------------------
# SOURCE 2 — Baseball Savant sprint speed
# ----------------------------------------------------------------------------

def savant_name_to_std(name: str) -> str:
    if "," in name:
        last, first = [p.strip() for p in name.split(",", 1)]
        return f"{first} {last}"
    return name


def fetch_sprint_speed(year: int):
    if not REQ_OK:
        return []
    url = SAVANT_URL_TMPL.format(year=year)
    try:
        r = requests.get(url, timeout=30, headers={"User-Agent": UA})
        r.raise_for_status()
        text = r.text.lstrip("\ufeff")
        reader = csv.DictReader(io.StringIO(text))
        rows = []
        for row in reader:
            # Column key uses bom sometimes
            nm = row.get("last_name, first_name") \
                or row.get("\ufefflast_name, first_name") or ""
            spd = row.get("sprint_speed")
            try:
                spd_f = float(spd) if spd else None
            except ValueError:
                spd_f = None
            if nm and spd_f is not None:
                rows.append({
                    "std_name": savant_name_to_std(nm),
                    "sprint_speed": spd_f,
                })
        return rows
    except Exception as e:
        print(f"[savant] year {year} failed: {e}", file=sys.stderr)
        return []


def compute_percentiles(rows):
    s = sorted(rows, key=lambda r: r["sprint_speed"])
    n = len(s)
    for i, r in enumerate(s):
        r["sprint_pct"] = round(100.0 * (i + 0.5) / n, 1)
        r["key"] = norm_name(r["std_name"])
    return s


# ----------------------------------------------------------------------------
# MAIN
# ----------------------------------------------------------------------------

def main():
    if len(sys.argv) < 2:
        print("Usage: refresh_hitter_stats_enrich.py data/hitters.json",
              file=sys.stderr)
        sys.exit(1)

    with open(sys.argv[1]) as f:
        hs = json.load(f)

    hitters = hs.get("hitters") or {}
    hs["thresholds"] = hs.get("thresholds") or {}
    hs["thresholds"].setdefault("ops",
        {"elite": 0.900, "good": 0.800, "bad": 0.700, "worst": 0.650})

    # --- FG OPS projections --------------------------------------------------
    fg_rows = fetch_fg_projections()
    fg_enrich = build_projection_enrichment(fg_rows)
    ops_matched = 0
    if fg_enrich:
        for key, entry in hitters.items():
            e = fg_enrich.get(key)
            if e:
                for fld in ("ops", "iso", "pa"):
                    if fld in e:
                        entry[fld] = e[fld]
                ops_matched += 1
    # If FG failed, existing values are preserved.

    # --- Savant sprint speed -------------------------------------------------
    year = int(os.environ.get("SAVANT_YEAR", datetime.now().year))
    sprint_rows = fetch_sprint_speed(year) or fetch_sprint_speed(year - 1)
    sprint_rows = compute_percentiles(sprint_rows) if sprint_rows else []
    sprint_lookup = {r["key"]: r for r in sprint_rows}
    sprint_matched = 0
    if sprint_lookup:
        for key, entry in hitters.items():
            r = sprint_lookup.get(key)
            if r:
                entry["sprint_speed"] = r["sprint_speed"]
                entry["sprint_pct"] = r["sprint_pct"]
                entry["sprint_elite"] = r["sprint_pct"] >= SPEED_ELITE_CUTOFF
                sprint_matched += 1

    hs["enriched_at"] = datetime.now(timezone.utc).isoformat()
    hs["enriched_count"] = ops_matched
    hs["speed_enriched_count"] = sprint_matched
    hs["speed_cutoff_pct"] = SPEED_ELITE_CUTOFF

    json.dump(hs, sys.stdout, indent=2)
    print(f"Enriched {ops_matched} OPS · {sprint_matched} sprint-speed "
          f"(elite cutoff p{SPEED_ELITE_CUTOFF})", file=sys.stderr)

    # --- SIDE EFFECT: fill missing lineups with Rotowire platoon projections
    # Uses each team's "Default vs. RHP" / "Default vs. LHP" batting order
    # applied based on the opposing starting pitcher's handedness.
    # Does NOT overwrite MLB-confirmed or Rotowire-expected lineups already posted.
    try:
        here = os.path.dirname(os.path.abspath(__file__))
        repo_root = os.path.dirname(here)
        lineups_path = os.path.join(repo_root, "data", "lineups.json")
        if os.path.exists(lineups_path):
            import subprocess
            # Run the platoon scraper in-process via subprocess so it isolates
            # stdout (it writes the new lineups to stdout).
            result = subprocess.run(
                [sys.executable, os.path.join(here, "rotowire_platoons.py"),
                 lineups_path],
                capture_output=True, text=True, timeout=180,
            )
            if result.returncode == 0 and result.stdout:
                with open(lineups_path, "w") as f:
                    f.write(result.stdout)
                for line in (result.stderr or "").splitlines():
                    if line.strip():
                        print(line, file=sys.stderr)
            else:
                print(f"[rotowire-platoons] exit {result.returncode}: "
                      f"{result.stderr[:200] if result.stderr else '(no stderr)'}",
                      file=sys.stderr)
    except Exception as e:
        print(f"[rotowire-platoons] non-fatal error: {e}", file=sys.stderr)


if __name__ == "__main__":
    main()
