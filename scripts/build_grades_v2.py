#!/usr/bin/env python3
"""
Build data/grades_v2.json from:
  - data/power_eye_2026.csv       (refreshed by refresh_power_eye.py)
  - data/def_bsr_2026.csv         (manually uploaded by user from their model)

Output is keyed by normName(player) to drop straight into index.html's lookup.

Structure:
  {
    "generated_at": "...",
    "n_players": 540,
    "by_name": {
      "aaron judge": {
        "name": "Aaron Judge",
        "power": 43.17, "power_pct": 92, "power_grade": "A+",
        "eye":   18.91, "eye_pct":   90, "eye_grade":   "A+",
        "fld":   0.024, "fld_pct":   65, "fld_grade":   "B",
        "bsr":  -0.003, "bsr_pct":   35, "bsr_grade":   "C"
      },
      ...
    }
  }

The frontend overrides powerPct/eyePct/fldPct/bsrPct to prefer this file
when a player matches; falls through to the existing Savant/HITTERS logic
when a player is missing.
"""
import csv, json, sys, unicodedata, datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
DATA = REPO_ROOT / "data"
PWR_EYE_CSV = DATA / "power_eye_2026.csv"
DEF_BSR_CSV = DATA / "def_bsr_2026.csv"
OUT_PATH    = DATA / "grades_v2.json"

GRADE_LADDER = [(95,"A+"),(87,"A"),(80,"A-"),(72,"B+"),(64,"B"),(56,"B-"),
                (46,"C+"),(36,"C"),(26,"C-"),(17,"D+"),(8,"D"),(0,"D-")]

def grade(pct):
    if pct is None: return None
    for cut, ltr in GRADE_LADDER:
        if pct >= cut: return ltr
    return "D-"

import re
# MIRROR of JS normName in index.html — keep these two in sync.
# Steps: NFKD → strip combining marks → lowercase → strip " jr."/" sr."/" iii"/" ii"
# → strip "." chars → trim. Apostrophes/dashes are PRESERVED (matches JS).
SUFFIX_RX = re.compile(r" (jr|sr|iii|ii)\.?$")
DOT_RX    = re.compile(r"\.")
COMBO_RX  = re.compile(r"[̀-ͯ]")
def norm(s):
    if not s: return ""
    s = unicodedata.normalize("NFKD", s)
    s = COMBO_RX.sub("", s).lower()
    s = SUFFIX_RX.sub("", s)
    s = DOT_RX.sub("", s)
    return s.strip()

def rank_pct(rows, key):
    """Rank-percentile rows by `key`, only across rows where key is not None."""
    # First mark all rows missing the key
    for r in rows:
        if r.get(key) is None:
            r[f"{key}_pct"] = None
    # Now rank only non-null rows
    present = [(i, r) for i, r in enumerate(rows) if r.get(key) is not None]
    n = len(present)
    if not n: return
    present.sort(key=lambda x: x[1][key])
    i = 0
    while i < n:
        j = i
        while j+1 < n and present[j+1][1][key] == present[i][1][key]:
            j += 1
        rank_mid = (i + j) / 2 + 1
        p = (rank_mid - 0.5) / n * 100
        for k in range(i, j+1):
            rows[present[k][0]][f"{key}_pct"] = round(p, 2)
        i = j + 1

players = {}  # normname → row

# ---- Power+Eye source ----
if PWR_EYE_CSV.exists():
    with open(PWR_EYE_CSV, encoding="utf-8") as f:
        rdr = csv.DictReader(f)
        for row in rdr:
            n = norm(row.get("name") or "")
            if not n: continue
            try:
                power = float(row["power"]) if row.get("power") else None
                eye   = float(row["eye"])   if row.get("eye")   else None
            except ValueError:
                power = eye = None
            entry = players.setdefault(n, {"name": row["name"], "norm": n})
            entry["power"] = power
            entry["eye"]   = eye

# ---- DEF+BSR source ----
if DEF_BSR_CSV.exists():
    with open(DEF_BSR_CSV, encoding="utf-8") as f:
        rdr = csv.DictReader(f)
        for row in rdr:
            n = norm(row.get("name") or "")
            if not n: continue
            try:
                fld = float(row["fld"]) if row.get("fld") else None
                bsr = float(row["bsr"]) if row.get("bsr") else None
            except ValueError:
                fld = bsr = None
            entry = players.setdefault(n, {"name": row["name"], "norm": n})
            # Duplicate names (e.g. the two Max Muncys): FIRST row wins — the
            # user lists the primary player (LAD Muncy) first in the CSV.
            if entry.get("fld") is None:
                entry["fld"] = fld
            if entry.get("bsr") is None:
                entry["bsr"] = bsr

# Rank-percentile each metric *within its own population* (so a Power-only
# player still gets a Power grade even if FLD is missing).
rows = list(players.values())
for k in ("power", "eye", "fld", "bsr"):
    rank_pct(rows, k)
    for r in rows:
        r[f"{k}_grade"] = grade(r.get(f"{k}_pct"))

# Round raws for compactness
for r in rows:
    for k in ("power","eye","fld","bsr"):
        if r.get(k) is not None:
            r[k] = round(r[k], 4)

by_name = {r["norm"]: {kk: r[kk] for kk in r if kk != "norm"} for r in rows}

payload = {
    "generated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds"),
    "n_players": len(by_name),
    "by_name": by_name,
}

OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
OUT_PATH.write_text(json.dumps(payload, separators=(",",":")))

# diagnostics
pwr_n = sum(1 for r in rows if r.get("power") is not None)
fld_n = sum(1 for r in rows if r.get("fld")   is not None)
both  = sum(1 for r in rows if r.get("power") is not None and r.get("fld") is not None)
print(f"[grades_v2] wrote {OUT_PATH} — {len(by_name)} players")
print(f"  with Power+Eye: {pwr_n}")
print(f"  with FLD+BSR:   {fld_n}")
print(f"  with both:      {both}")
