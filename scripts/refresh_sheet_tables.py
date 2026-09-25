#!/usr/bin/env python3
"""
Bridge feed: `Team Modifiers` tab -> data/sheet_tables.json for the internal
engine. Per team: Runs PF (= the park 'New Factor' used in J2 when this team is
home) and PF Adj Away / PF Adj Home (the offense-ratio divisor for a team batting
away / home). Keyed by team nickname so the driver can match statsapi full names.
"""
from __future__ import annotations
import csv, io, json, os, re, sys, time, urllib.parse, urllib.request, datetime
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
OUTPUT = os.path.join(REPO_ROOT, "data", "sheet_tables.json")
SHEET_ID = "1Dq9ma3W_YPOJJzq6ZnqivfaniEk8wZJw3gZvuDrH6DE"
GVIZ = "https://docs.google.com/spreadsheets/d/{sid}/gviz/tq?tqx=out:csv&sheet={tab}&_cb={cb}"
UA = "Mozilla/5.0 (compatible; mlb-tracker/1.0)"

def _f(v):
    try:
        v = str(v).strip().replace(",", "")
        return float(v) if v not in ("", "#N/A", "None") else None
    except (ValueError, TypeError):
        return None

def main():
    url = GVIZ.format(sid=SHEET_ID, tab=urllib.parse.quote("Team Modifiers"), cb=int(time.time()))
    req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": "text/csv,*/*"})
    with urllib.request.urlopen(req, timeout=30) as r:
        rows = list(csv.reader(io.StringIO(r.read().decode("utf-8", "replace"))))
    teams = {}
    for row in rows[1:]:
        nm = row[0].strip() if row and row[0] else ""
        if not nm:
            continue
        teams[nm.lower()] = {
            "team": nm,
            "runs_pf":     _f(row[1]) if len(row) > 1 else None,   # J2 park factor when home
            "pf_adj_away": _f(row[2]) if len(row) > 2 else None,   # offense divisor batting away
            "pf_adj_home": _f(row[3]) if len(row) > 3 else None,   # offense divisor batting home
        }
    payload = {"generated_at": datetime.datetime.utcnow().isoformat(timespec="seconds") + "Z",
               "source": "Google Sheet 'Team Modifiers' tab", "n_teams": len(teams), "teams": teams}
    os.makedirs(os.path.dirname(OUTPUT), exist_ok=True)
    with open(OUTPUT, "w") as fh:
        json.dump(payload, fh, indent=1)
    print(f"[sheet_tables] wrote {len(teams)} team modifiers -> {OUTPUT}")
    return 0

if __name__ == "__main__":
    sys.exit(main())
