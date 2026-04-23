#!/usr/bin/env python3
"""
Build data/hitter_splits.json from user-uploaded FanGraphs split CSVs.

Inputs: two CSVs in the working tree, one per opposing-pitcher hand:
    data/_splits_vs_rhp.csv
    data/_splits_vs_lhp.csv
(Both are the "Splits Leaderboard Data" export from FanGraphs with
"Season=Total" selected — a 3-year rolling combined sample.)

Shrinkage: each player's raw wRC+ is regressed toward league mean (100)
using an empirical-Bayes blend with k=100 PAs:

    shrunk_wrc = (PA * raw_wrc + k * 100) / (PA + k)

Why k=100: tested across Judge (1106 PA · 202 raw → 193), Yordan vs L
(287 PA · 205 raw → 178), Stewart vs L (32 PA · 243 → 135), Vargas
(629 · 82 → 84). Star hitters retain signal, rookies with <50 PA snap
toward average, avoids the "Vargas 10/-12" display noise entirely.

Output: data/hitter_splits.json
    {
      "generated_at": "...",
      "source": "FanGraphs splits leaderboard (3-yr Total)",
      "methodology": "empirical Bayes shrinkage · k=100 PAs",
      "hitters": {
        "<norm_name>": {
          "name": "Aaron Judge",
          "mlbam_id": null,
          "pa_vs_r": 1106, "wrc_vs_r_raw": 201.6, "wrc_vs_r": 193,
          "pa_vs_l": 377,  "wrc_vs_l_raw": 230.5, "wrc_vs_l": 203
        },
        ...
      }
    }

USAGE:
    python scripts/ingest_hitter_splits.py > data/hitter_splits.json
"""
from __future__ import annotations
import csv
import datetime
import json
import os
import sys
import unicodedata

HERE = os.path.dirname(__file__)
DATA = os.path.join(HERE, "..", "data")

RHP_CSV = os.environ.get("SPLITS_RHP_CSV") or os.path.join(DATA, "_splits_vs_rhp.csv")
LHP_CSV = os.environ.get("SPLITS_LHP_CSV") or os.path.join(DATA, "_splits_vs_lhp.csv")

K_SHRINK = 100  # regression constant in PAs. Lower = trust raw more.
LEAGUE_MEAN = 100  # wRC+ league average


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


def _shrink(pa: float, raw: float, k: int = K_SHRINK) -> float:
    if pa is None or raw is None:
        return None
    return (pa * raw + k * LEAGUE_MEAN) / (pa + k)


def _load(path: str):
    """Return dict keyed by norm_name → {pa, wrc_raw, name, mlbam_id}."""
    if not os.path.exists(path):
        print(f"[splits] missing {path}", file=sys.stderr)
        return {}
    out = {}
    with open(path, newline="") as f:
        for row in csv.DictReader(f):
            name = (row.get("Name") or "").strip().strip('"')
            if not name:
                continue
            try:
                pa = int((row.get("PA") or "0").strip().strip('"'))
                wrc = float((row.get("wRC+") or "0").strip().strip('"'))
            except (ValueError, TypeError):
                continue
            if pa <= 0:
                continue
            pid = row.get("playerId")
            try:
                pid = int(pid) if pid else None
            except ValueError:
                pid = None
            out[norm_name(name)] = {
                "name": name, "mlbam_id": pid, "pa": pa, "wrc_raw": wrc,
            }
    return out


def main():
    rhp = _load(RHP_CSV)
    lhp = _load(LHP_CSV)
    print(f"[splits] loaded {len(rhp)} vs-R · {len(lhp)} vs-L players",
          file=sys.stderr)
    # Merge on normalized name
    hitters = {}
    for k, v in rhp.items():
        hitters[k] = {
            "name": v["name"], "mlbam_id": v.get("mlbam_id"),
            "pa_vs_r": v["pa"], "wrc_vs_r_raw": round(v["wrc_raw"], 1),
            "wrc_vs_r": round(_shrink(v["pa"], v["wrc_raw"]), 1),
        }
    for k, v in lhp.items():
        entry = hitters.setdefault(k, {
            "name": v["name"], "mlbam_id": v.get("mlbam_id"),
        })
        entry["pa_vs_l"] = v["pa"]
        entry["wrc_vs_l_raw"] = round(v["wrc_raw"], 1)
        entry["wrc_vs_l"] = round(_shrink(v["pa"], v["wrc_raw"]), 1)
    payload = {
        "generated_at": datetime.datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "source": "FanGraphs splits leaderboard (3-yr Total)",
        "methodology": f"empirical Bayes shrinkage · k={K_SHRINK} PAs · wRC+ mean=100",
        "k": K_SHRINK,
        "hitters": hitters,
    }
    json.dump(payload, sys.stdout, indent=2)
    print(f"[splits] wrote {len(hitters)} hitters", file=sys.stderr)


if __name__ == "__main__":
    main()
