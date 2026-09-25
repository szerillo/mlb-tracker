#!/usr/bin/env python3
"""
refresh_pitcher_proc.py — enrich pitcher_stats.json with the SP process index.

Reads data/sp_process.json (produced by scripts/proc_index.py — Fable's reference,
a daily Baseball-Savant pull; 60-day trailing window, six components each = last 14
days minus prior 46 days scaled by fixed 2021-2026 panel SDs, averaged, re-scaled,
clipped to ±2.5). Joins to pitcher_stats.json on MLBAM id (never name) and writes,
for every starter present in sp_process:

    proc_z            the clipped process index (higher = better recent form)
    ra9_adj           = round(-0.10 * proc_z, 3)      runs/9 (Fable PROC_COEF = 0.10)
    proc_components   {z_velo, z_whiff, z_csw, z_kbb, z_sec_velo, z_pitches14}
    unified_adj       = round(unified_score + ra9_adj, 3)   # = unified_score - 0.10*proc_z

unified_score stays the raw wFIP composite. unified_adj is the process-adjusted SP RA9
that the pregame / F5 / K-props / pro consumers read. Relievers get nothing (SP-only).

Invoked automatically at the end of compute_pitcher_score.py (after unified_score is
written, before the wFIP lookup is emitted); also runnable standalone.
Spec: IMPLEMENTATION_BRIEF_for_BARTOLO_2026-09-24.md §1, SP_PROCESS_INDEX_SIX_SEASONS.
"""
from __future__ import annotations
import json, math, sys
from pathlib import Path

REPO_ROOT  = Path(__file__).resolve().parent.parent
PROC_FILE  = REPO_ROOT / "data" / "sp_process.json"
STATS_FILE = REPO_ROOT / "data" / "pitcher_stats.json"

PROC_COEF  = 0.10   # runs/9 per SD (Fable proc6, pooled -0.108). ra9_adj = -PROC_COEF*proc_z
ZKEYS = ("z_velo", "z_whiff", "z_csw", "z_kbb", "z_sec_velo", "z_pitches14")


def _num(v):
    try:
        f = float(v); return f if f == f else None
    except (TypeError, ValueError):
        return None


def main():
    if not PROC_FILE.exists() or not STATS_FILE.exists():
        print("[proc-enrich] sp_process.json or pitcher_stats.json missing; skipping", file=sys.stderr)
        return 0
    try:
        proc = json.loads(PROC_FILE.read_text())
    except Exception as e:
        print(f"[proc-enrich] bad sp_process.json: {e}", file=sys.stderr)
        return 0
    stats = json.loads(STATS_FILE.read_text())
    pit = stats.get("pitchers") or {}

    # index pitcher_stats by mlbam id (id-join only — no name matching)
    by_id = {}
    for key, p in pit.items():
        if isinstance(p, dict) and p.get("mlbam_id") is not None:
            by_id[int(p["mlbam_id"])] = p

    n_adj = n_miss = 0
    for sid, r in proc.items():
        try:
            mid = int(r.get("mlbam_id", sid))
        except (TypeError, ValueError):
            continue
        p = by_id.get(mid)
        if p is None:
            n_miss += 1
            continue
        pz = _num(r.get("proc_z"))
        if pz is None:
            continue
        ra9 = _num(r.get("ra9_adj"))
        if ra9 is None:
            ra9 = round(-PROC_COEF * pz, 3)
        p["proc_z"] = round(pz, 3)
        p["ra9_adj"] = ra9
        # NaN (missing velo etc.) must not leak into pitcher_stats.json: bare NaN is
        # invalid JSON and nulls out the whole feed in a strict browser parse. Coerce
        # any non-finite component to null.
        p["proc_components"] = {k: (None if isinstance(r.get(k), float) and not math.isfinite(r.get(k)) else r.get(k)) for k in ZKEYS}
        us = _num(p.get("unified_score"))
        if us is not None and us < 10:                 # sane RA9-scale composite only
            p["unified_adj"] = round(us + ra9, 3)
            n_adj += 1

    stats["proc_meta"] = {
        "source": "sp_process.json (proc_index.py, Fable 2026-09-24)",
        "coef": PROC_COEF, "date": (proc.get(next(iter(proc))) or {}).get("date") if proc else None,
        "n_sp": len(proc), "n_adjusted": n_adj,
    }
    STATS_FILE.write_text(json.dumps(stats, separators=(",", ":")))
    print(f"[proc-enrich] {n_adj} SP unified_adj set from sp_process ({n_miss} ids not in pitcher_stats)",
          file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
