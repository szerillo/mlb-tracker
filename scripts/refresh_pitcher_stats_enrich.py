#!/usr/bin/env python3
"""
Enrich data/pitcher_stats.json with xFIP + Stuff+ + Location+ + Pitching+ + IP.

STRATEGY: Fangraphs blocks unauthenticated server-side requests (Cloudflare
interstitial). So we support TWO sources:

    1. PREFERRED: data/_fg_pitch_model.json — a dump produced by running
       the in-browser snippet below from a logged-in Fangraphs tab. Commit
       that file to the repo (the browser snippet does this automatically
       via the GitHub Contents API); this script then merges it in.

    2. FALLBACK: direct FG API hit with desktop headers. Works occasionally
       but Cloudflare drops us most days.

Running the browser snippet updates the dump with fresh numbers whenever
Fangraphs' public leaderboard updates (which is after every game).

BROWSER SNIPPET (paste into DevTools console on any
https://www.fangraphs.com/leaders/major-league page):

    fetch('/api/leaders/major-league/data?pos=all&stats=pit&lg=all&type=36&season=2026&month=0&season1=2026&ind=0&qual=10&pageitems=2000000000', {credentials:'include'})
      .then(r=>r.json()).then(d=>{
        const arr=d.data||d, norm=s=>(s||'').normalize('NFKD').replace(/[\u0300-\u036f]/g,'').toLowerCase().replace(/ jr\.?$|\sSr\.?$|\siii$|\sii$/g,'').replace(/\./g,'').trim();
        const en={}; for(const r of arr){const k=norm(r.PlayerName||r.Name); if(!k)continue; const e={}; if(r.xFIP!=null)e.xfip=r.xFIP; if(r.sp_stuff!=null)e.stuff_plus=r.sp_stuff; if(r.sp_location!=null)e.location_plus=r.sp_location; if(r.sp_pitching!=null)e.pitching_plus=r.sp_pitching; if(r.IP!=null)e.ip=r.IP; if(Object.keys(e).length)en[k]=e;}
        console.log('pitchers:', Object.keys(en).length);
        copy(JSON.stringify({generated_at:new Date().toISOString(),source:'fangraphs-pitch-modeling',count:Object.keys(en).length,pitchers:en},null,2));
        console.log('copied to clipboard — paste into data/_fg_pitch_model.json and commit');
      });

USAGE:
    python scripts/refresh_pitcher_stats_enrich.py data/pitcher_stats.json > /tmp/ps.json
    mv /tmp/ps.json data/pitcher_stats.json
"""

from __future__ import annotations
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

UA_DESKTOP = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
              "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

FG_URL = ("https://www.fangraphs.com/api/leaders/major-league/data"
          "?pos=all&stats=pit&lg=all&type=36&season={season}"
          "&month=0&season1={season}&ind=0&qual=10&pageitems=2000000000")


def norm_name(s: str) -> str:
    s = unidecode(s or "").lower()
    s = re.sub(r"\s+jr\.?$|\s+sr\.?$|\s+iii$|\s+ii$", "", s)
    s = s.replace(".", "").strip()
    return s


def load_browser_dump(repo_root: str) -> Dict[str, dict]:
    """Load pre-dumped FG data from data/_fg_pitch_model.json if present."""
    path = os.path.join(repo_root, "data", "_fg_pitch_model.json")
    if not os.path.exists(path):
        return {}
    try:
        with open(path) as f:
            dump = json.load(f)
        return dump.get("pitchers", {})
    except Exception as e:
        print(f"[enrich] browser dump parse failed: {e}", file=sys.stderr)
        return {}


def live_fetch_fg(season: int) -> Dict[str, dict]:
    if not REQ_OK:
        return {}
    url = FG_URL.format(season=season)
    try:
        r = requests.get(url, timeout=30, headers={
            "User-Agent": UA_DESKTOP,
            "Referer": "https://www.fangraphs.com/leaders/major-league",
            "Accept": "application/json, text/plain, */*",
        })
        if not r.ok:
            print(f"[enrich] live FG blocked (HTTP {r.status_code})", file=sys.stderr)
            return {}
        rows = r.json()
        rows = rows.get("data", rows) if isinstance(rows, dict) else rows
    except Exception as e:
        print(f"[enrich] live FG failed: {e}", file=sys.stderr)
        return {}

    out = {}
    for r in rows:
        k = norm_name(r.get("PlayerName") or r.get("Name") or "")
        if not k:
            continue
        e = {}
        for src, dst in [("xFIP", "xfip"), ("sp_stuff", "stuff_plus"),
                         ("sp_location", "location_plus"),
                         ("sp_pitching", "pitching_plus"), ("IP", "ip")]:
            v = r.get(src)
            if v is not None:
                e[dst] = v
        if e:
            out[k] = e
    return out


def main():
    if len(sys.argv) < 2:
        print("Usage: refresh_pitcher_stats_enrich.py data/pitcher_stats.json",
              file=sys.stderr)
        sys.exit(1)
    infile = sys.argv[1]
    # Resolve repo root (one above data/)
    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(infile)))
    season = int(os.environ.get("FG_SEASON", datetime.now().year))

    with open(infile) as f:
        ps = json.load(f)

    # Prefer the pre-committed browser dump
    browser_dump = load_browser_dump(repo_root)
    if browser_dump:
        enrichment = browser_dump
        source = "browser-dump"
    else:
        enrichment = live_fetch_fg(season)
        source = "live-fg" if enrichment else "none"

    matched = 0
    for key, entry in (ps.get("pitchers") or {}).items():
        e = enrichment.get(key)
        if e:
            for fld in ("xfip", "stuff_plus", "location_plus",
                        "pitching_plus", "ip"):
                if fld in e:
                    entry[fld] = e[fld]
            matched += 1

    ps["enriched_at"] = datetime.now(timezone.utc).isoformat()
    ps["enriched_count"] = matched
    ps["enriched_source"] = source
    ps["thresholds"] = ps.get("thresholds") or {}
    ps["thresholds"].setdefault("xfip",
        {"elite": 3.25, "good": 3.75, "bad": 4.25, "worst": 4.75})

    json.dump(ps, sys.stdout, indent=2)
    print(f"Enriched {matched} pitchers from {source}", file=sys.stderr)


if __name__ == "__main__":
    main()
