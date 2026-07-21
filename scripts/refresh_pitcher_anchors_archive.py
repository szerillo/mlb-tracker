#!/usr/bin/env python3
"""
Per-date pitcher anchor snapshots.

Writes  data/archive/{today_ET}/pitcher_anchors.json  every refresh. Each
record captures the **AS-OF-THIS-DATE** value of the metrics that the wFIP
blend treats as "season anchors": xERA, botERA, plus the in-season
projection (fip_proj) and the stabilized rolling components.

WHY THIS EXISTS
═══════════════
Our per-pitcher backtest (scripts/backtest_wfip_weights.py) is sound for the
rolling xFIP/SIERA components — they're per-start, so we can rebuild any
prior-date snapshot from the gamelogs. But xERA and botERA are not
per-start; they're season-cumulative figures that get refreshed nightly,
and reading the **CURRENT** snapshot during a backtest leaks future data
into the historical predictor (a pitcher's June xERA "knows" about their
May outings, which is exactly what we were trying to predict).

By archiving these values every day, future backtests can read the
date-stamped file matching the prediction date instead, removing the
leakage. After ~6 weeks of accumulation we'll have enough date-anchored
snapshots to redo the weight grid search cleanly.

USAGE
═════
    python scripts/refresh_pitcher_anchors_archive.py

OUTPUT (data/archive/{date}/pitcher_anchors.json):
{
  "date": "2026-06-01",
  "generated_at": "ISO timestamp",
  "n_pitchers": int,
  "fields": ["ip", "xera", "bot_era", "fip_proj", "fip_proj_n_sources",
             "xfip", "siera", "k_bb_pct", "unified_score"],
  "pitchers": {
      "<normalized_name>": { mlbam_id, ip, xera, bot_era, fip_proj,
                             fip_proj_n_sources, xfip, siera, k_bb_pct,
                             unified_score, unified_rolling }
  }
}

Re-runs on the same date overwrite — anchors only change once per nightly
refresh anyway, so we always want the freshest snapshot for that day.
"""
from __future__ import annotations
import datetime
import json
import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = REPO_ROOT / "data"
ARCHIVE_DIR = DATA_DIR / "archive"
INPUT = DATA_DIR / "pitcher_stats.json"

# Subset of pitcher_stats fields we want frozen for backtesting. Kept lean so
# the daily snapshot stays small (~6233 pitchers × ~10 fields).
SNAPSHOT_FIELDS = [
    "mlbam_id", "ip",
    # incumbent wFIP anchors
    "xera", "bot_era", "fip_proj", "fip_proj_n_sources",
    "xfip", "siera", "k_bb_pct", "unified_score", "unified_rolling",
    # --- pitch-model grades (Fable's #1 ask: previously unfrozen) ---
    "stuff_plus", "location_plus", "pitching_plus",
    # --- plate skills ---
    "whiff_pct", "k_pct", "bb_pct", "edge_pct", "f_strike_pct", "meatball_pct",
    # --- contact allowed ---
    "barrel_pct", "hard_hit_pct", "gb_pct", "fb_pct", "hr_per_9", "xwoba_savant",
]

# Rolling velo/spin/CSW trend + season baseline, pulled from pitcher_gamelogs.json
# (l5/season aggregates). Frozen so the Sept re-run can test velo/health trends.
GAMELOGS = DATA_DIR / "pitcher_gamelogs.json"
TREND_KEYS = ["velo", "fb_spin", "csw", "whiff", "stuff"]

def _load_trends():
    """key -> {l5_velo, l5_spin, l5_csw, l5_whiff, l5_stuff, velo_season,
              spin_season, velo_delta, spin_delta, csw_delta, stuff_delta}."""
    out = {}
    if not GAMELOGS.exists():
        return out
    try:
        gd = json.loads(GAMELOGS.read_text()).get("pitchers") or {}
    except Exception as e:
        print(f"[anchors] gamelogs read failed: {e}", file=sys.stderr); return out
    for key, rec in gd.items():
        l5 = rec.get("l5") or {}; se = rec.get("season") or {}
        d = {}
        if l5.get("velo") is not None:    d["l5_velo"]  = l5["velo"]
        if l5.get("fb_spin") is not None: d["l5_spin"]  = l5["fb_spin"]
        if l5.get("csw") is not None:     d["l5_csw"]   = l5["csw"]
        if l5.get("whiff") is not None:   d["l5_whiff"] = l5["whiff"]
        if l5.get("stuff") is not None:   d["l5_stuff"] = round(l5["stuff"], 1)
        if se.get("velo") is not None:    d["velo_season"] = se["velo"]
        if se.get("fb_spin") is not None: d["spin_season"] = se["fb_spin"]
        # deltas (recent minus season baseline) — the health/decline signal
        if l5.get("velo") is not None and se.get("velo") is not None:
            d["velo_delta"] = round(l5["velo"] - se["velo"], 2)
        if l5.get("fb_spin") is not None and se.get("fb_spin") is not None:
            d["spin_delta"] = round(l5["fb_spin"] - se["fb_spin"], 0)
        if l5.get("csw") is not None and se.get("csw") is not None:
            d["csw_delta"] = round(l5["csw"] - se["csw"], 3)
        if l5.get("stuff") is not None and se.get("stuff") is not None:
            d["stuff_delta"] = round(l5["stuff"] - se["stuff"], 1)
        if d: out[key] = d
    return out

# Only snapshot pitchers with at least this much IP. Below 5 IP the metrics
# are too noisy / sparse to be useful in a backtest predictor pool, and most
# of these rows are minor-leaguers + spring-training cameos.
MIN_IP_TO_SNAPSHOT = 5.0


def _et_today() -> datetime.date:
    """Use the MLB business day in ET, matching the rest of the pipeline.
    Around midnight UTC late games are still finishing under yesterday's date
    so anchor archives keyed by ET stay aligned with bartolo_wp + pregame."""
    return (datetime.datetime.now(datetime.timezone.utc)
            - datetime.timedelta(hours=4)).date()


def main():
    if not INPUT.exists():
        print(f"[anchors] {INPUT} missing — skipping", file=sys.stderr)
        return 0

    try:
        data = json.loads(INPUT.read_text())
    except Exception as e:
        print(f"[anchors] failed to read pitcher_stats.json: {e}", file=sys.stderr)
        return 1

    pitchers = data.get("pitchers") or {}
    trends = _load_trends()
    today = _et_today()

    out = {}
    n_skipped_no_ip = 0
    n_skipped_no_anchor = 0
    for key, p in pitchers.items():
        if not isinstance(p, dict):
            continue
        ip = p.get("ip")
        if not isinstance(ip, (int, float)) or ip < MIN_IP_TO_SNAPSHOT:
            n_skipped_no_ip += 1
            continue
        # Need at least one anchor for the row to be useful; if all are null
        # there's nothing to backtest with later.
        if (p.get("xera") is None and p.get("bot_era") is None
                and p.get("fip_proj") is None):
            n_skipped_no_anchor += 1
            continue
        rec = {}
        for f in SNAPSHOT_FIELDS:
            v = p.get(f)
            if v is not None:
                rec[f] = v
        t = trends.get(key)
        if t:
            rec.update(t)
        out[key] = rec

    archive_dir = ARCHIVE_DIR / today.isoformat()
    archive_dir.mkdir(parents=True, exist_ok=True)
    path = archive_dir / "pitcher_anchors.json"

    payload = {
        "date": today.isoformat(),
        "generated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds"),
        "source": "pitcher_stats.json snapshot — leakage-free anchors for future wFIP weight backtests",
        "n_pitchers": len(out),
        "min_ip": MIN_IP_TO_SNAPSHOT,
        "fields": SNAPSHOT_FIELDS + ["l5_velo","l5_spin","l5_csw","l5_whiff","l5_stuff","velo_season","spin_season","velo_delta","spin_delta","csw_delta","stuff_delta"],
        "skipped_low_ip": n_skipped_no_ip,
        "skipped_no_anchor": n_skipped_no_anchor,
        "pitchers": out,
    }
    # Compact JSON — this file accumulates daily so size matters.
    path.write_text(json.dumps(payload, separators=(",", ":")))
    size_kb = path.stat().st_size / 1024
    print(f"[anchors] wrote {path} ({len(out)} pitchers, {size_kb:.1f} KB)",
          file=sys.stderr)
    print(f"          skipped: {n_skipped_no_ip} low-IP, {n_skipped_no_anchor} no-anchor",
          file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
