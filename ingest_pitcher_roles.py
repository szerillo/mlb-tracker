#!/usr/bin/env python3
"""
Build data/pitcher_roles.json from FanGraphs Steamer pitcher projections.

Classification:
    role = "SP" if projected GS/G >= 0.5  (i.e. majority of appearances are starts)
    role = "RP" otherwise

Rationale: simple ratio cleanly separates true starters (32 GS / 32 G = 1.0)
from closers/setup (0 GS), swingmen (8-14 GS of ~30), and openers (Grant
Taylor projects 3 GS of 79 G = 0.04 → RP, which is right — he opens but
isn't a starter).

Edge case: projected GS >= 20 also gets SP regardless of ratio, catching
the rare true-starter-with-substantial-relief profile.

Input: CSV at data/_steamer_pitchers.csv (or $STEAMER_PITCHERS_CSV env var)
Output: data/pitcher_roles.json

USAGE:
    python scripts/ingest_pitcher_roles.py > data/pitcher_roles.json
"""
from __future__ import annotations
import csv
import datetime
import io
import json
import os
import sys
import unicodedata

HERE = os.path.dirname(__file__)
DATA = os.path.join(HERE, "..", "data")
SRC = os.environ.get("STEAMER_PITCHERS_CSV") or os.path.join(DATA, "_steamer_pitchers.csv")


def norm_name(s: str) -> str:
    if not s:
        return ""
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = s.lower()
    for suf in (" jr.", " jr", " sr.", " sr", " iii", " ii"):
        if s.endswith(suf):
            s = s[: -len(suf)]
    return s.replace(".", "").strip()


def _int(v):
    try: return int(float(v))
    except (TypeError, ValueError): return 0


def _float(v):
    try: return float(v)
    except (TypeError, ValueError): return 0.0


def classify(gs: int, g: int) -> str:
    if g == 0:
        return "RP"
    # 20+ projected GS is unambiguous SP territory.
    if gs >= 20:
        return "SP"
    return "SP" if (gs / g) >= 0.5 and gs >= 5 else "RP"


def main():
    if not os.path.exists(SRC):
        print(f"[roles] missing {SRC}", file=sys.stderr); return 1
    with open(SRC, "rb") as f:
        raw = f.read().decode("utf-8-sig")  # strip BOM if present
    rows = list(csv.DictReader(io.StringIO(raw)))
    out = {}
    n_sp = n_rp = 0
    for r in rows:
        nm = (r.get("Name") or "").strip()
        if not nm:
            continue
        g  = _int(r.get("G"))
        gs = _int(r.get("GS"))
        ip = _float(r.get("IP"))
        # Drop projections with ~no workload (minor leaguers, never-projected)
        if g < 1 and ip < 1:
            continue
        role = classify(gs, g)
        try:
            mlbam_id = int(r.get("MLBAMID") or 0) or None
        except (TypeError, ValueError):
            mlbam_id = None
        out[norm_name(nm)] = {
            "name": nm, "team": (r.get("Team") or "").strip(),
            "role": role, "g": g, "gs": gs, "ip": round(ip, 1),
            "mlbam_id": mlbam_id,
        }
        if role == "SP": n_sp += 1
        else: n_rp += 1
    payload = {
        "generated_at": datetime.datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "source": "FanGraphs Steamer pitcher projections",
        "methodology": "SP if projected GS/G >= 0.5 (min 5 GS) OR GS >= 20; else RP",
        "n_sp": n_sp, "n_rp": n_rp,
        "pitchers": out,
    }
    json.dump(payload, sys.stdout, indent=2)
    print(f"[roles] wrote {n_sp} SPs + {n_rp} RPs = {len(out)} total", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
