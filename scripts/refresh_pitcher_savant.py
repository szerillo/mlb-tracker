#!/usr/bin/env python3
"""
Enrich data/pitcher_stats.json with Savant season stats:
    hr_per_9   — home runs allowed per 9 IP (computed: HR / IP × 9)
    gb_pct     — ground-ball rate
    fb_pct     — fly-ball rate
    whiff_pct  — overall whiff rate (whiffs / swings)
    hard_hit_pct — Statcast hard-hit rate allowed (≥95 mph EV)
    barrel_pct   — Statcast barrel rate allowed
    xwoba      — expected wOBA allowed (already in source for some, refresh)
    xba        — expected BA allowed
    f_strike_pct — first-pitch strike %

Savant is not Cloudflare-protected — runs cleanly from GitHub Actions.

USAGE:
    python scripts/refresh_pitcher_savant.py data/pitcher_stats.json > /tmp/ps.json
    mv /tmp/ps.json data/pitcher_stats.json
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

# Savant custom-leaderboard CSV with selections we've confirmed return data
SAVANT_URL = (
    "https://baseballsavant.mlb.com/leaderboard/custom"
    "?year={year}&type=pitcher&min=1"
    "&selections=p_formatted_ip,p_total_pitches,p_home_run,"
    "whiff_percent,xwoba,xba,xiso,hard_hit_percent,barrel_batted_rate,"
    "groundballs_percent,flyballs_percent,f_strike_percent,edge_percent"
    "&chart=false&x=p_formatted_ip&y=p_formatted_ip&r=no&csv=true"
)


def norm_name(s: str) -> str:
    if not s: return ""
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = s.lower()
    for suf in [" jr.", " jr", " sr.", " sr", " iii", " ii"]:
        if s.endswith(suf):
            s = s[: -len(suf)]
    return s.replace(".", "").strip()


def _f(v):
    if v is None or v == "" or v == "null": return None
    s = str(v).strip().strip('"')
    if not s or s == ".": return None
    try: return float(s)
    except ValueError: return None


def fetch_savant(year: int):
    url = SAVANT_URL.format(year=year)
    print(f"[savant-pit] fetching {url}", file=sys.stderr)
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=30) as r:
        return r.read().decode("utf-8", errors="replace").lstrip("\ufeff")


def build_enrichment(text: str):
    reader = csv.DictReader(io.StringIO(text))
    out = {}
    for row in reader:
        raw = row.get("last_name, first_name") or ""
        if "," in raw:
            last, first = [p.strip() for p in raw.split(",", 1)]
            nm = f"{first} {last}"
        else:
            nm = raw.strip()
        if not nm: continue
        k = norm_name(nm)
        ip = _f(row.get("p_formatted_ip"))
        hr = _f(row.get("p_home_run"))
        hr_per_9 = (hr / ip * 9) if (ip and ip > 0 and hr is not None) else None

        e = {}
        if hr_per_9 is not None:         e["hr_per_9"]    = round(hr_per_9, 2)
        gb = _f(row.get("groundballs_percent"))
        fb = _f(row.get("flyballs_percent"))
        if gb is not None:                e["gb_pct"]      = gb
        if fb is not None:                e["fb_pct"]      = fb
        whiff = _f(row.get("whiff_percent"))
        if whiff is not None:             e["whiff_pct"]   = whiff
        hh = _f(row.get("hard_hit_percent"))
        if hh is not None:                e["hard_hit_pct"] = hh
        br = _f(row.get("barrel_batted_rate"))
        if br is not None:                e["barrel_pct"]  = br
        xwoba = _f(row.get("xwoba"))
        if xwoba is not None:             e["xwoba_savant"] = xwoba
        xba = _f(row.get("xba"))
        if xba is not None:               e["xba_savant"]  = xba
        fstr = _f(row.get("f_strike_percent"))
        if fstr is not None:              e["f_strike_pct"] = fstr
        edge = _f(row.get("edge_percent"))
        if edge is not None:              e["edge_pct"]    = edge
        if e:
            out[k] = e
    return out


def main():
    if len(sys.argv) < 2:
        print("Usage: refresh_pitcher_savant.py data/pitcher_stats.json",
              file=sys.stderr)
        sys.exit(1)
    infile = sys.argv[1]
    year = int(os.environ.get("SAVANT_YEAR", datetime.date.today().year))

    with open(infile) as f:
        ps = json.load(f)

    text = fetch_savant(year)
    enrichment = build_enrichment(text)
    print(f"[savant-pit] parsed {len(enrichment)} pitchers", file=sys.stderr)

    matched = 0
    fields = ("hr_per_9","gb_pct","fb_pct","whiff_pct","hard_hit_pct",
              "barrel_pct","xwoba_savant","xba_savant","f_strike_pct","edge_pct")
    for key, entry in (ps.get("pitchers") or {}).items():
        e = enrichment.get(key)
        if not e: continue
        for fld in fields:
            if fld in e:
                entry[fld] = e[fld]
        matched += 1

    # Thresholds for the new fields (used by the UI for tier colors)
    ps["thresholds"] = ps.get("thresholds") or {}
    ps["thresholds"].setdefault("hr_per_9",
        {"elite": 0.85, "good": 1.10, "bad": 1.35, "worst": 1.60})  # lower=better
    ps["thresholds"].setdefault("gb_pct",
        {"elite": 52, "good": 46, "bad": 38, "worst": 34})          # higher=better
    ps["thresholds"].setdefault("whiff_pct",
        {"elite": 32, "good": 27, "bad": 22, "worst": 18})          # higher=better
    ps["thresholds"].setdefault("hard_hit_pct",
        {"elite": 34, "good": 38, "bad": 44, "worst": 48})          # lower=better
    ps["thresholds"].setdefault("barrel_pct",
        {"elite": 5, "good": 7, "bad": 9, "worst": 12})             # lower=better

    ps["savant_enriched_at"] = datetime.datetime.now(
        datetime.timezone.utc).isoformat()
    ps["savant_enriched_count"] = matched

    json.dump(ps, sys.stdout, indent=2)
    print(f"Savant-enriched {matched} pitchers", file=sys.stderr)


if __name__ == "__main__":
    main()
