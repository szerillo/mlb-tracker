#!/usr/bin/env python3
"""
Pre-game model parameter tuning harness.

Re-runs the pre-game projection across the archived snapshot/actuals dataset
under a grid of parameter configurations, then reports which combination
minimizes total-runs MAE and WP Brier. Designed to be run AD HOC once enough
data accrues (target: ~3 weeks of snapshots), not on a cron.

USAGE:
    python scripts/tune_pregame.py
    python scripts/tune_pregame.py --output /tmp/tune_report.json

What it sweeps (small grid by default; expand in CONFIG_GRID below):
  • hfa_away / hfa_home     — home-field advantage multipliers (default 0.96/1.04)
  • park_mul                — multiplier on park_factor away from 1.0 (e.g., 0.5
                              dampens park; 1.5 amplifies). Default 1.0.
  • bp_weight               — bullpen weight in staff blend (default 0.445)
  • pw_floor / pw_ceil      — clamp on pw (default 0.50/0.80)

Sweep against the archived snapshots: we replay the spec's blend formula on
the SAME inputs the lock-time model used (read from each snapshot's
`components` field), so we don't need to re-fetch pitcher_stats / hitters /
lineups for every config. That's the whole point of the snapshot — it
captures everything the model saw.

Output (data/tune_report.json or --output):
  configs: [{config, n_games, total_mae, total_bias, wp_brier, fav_cover}]
  best_total_mae: { config, ... }
  best_wp_brier: { config, ... }
"""
from __future__ import annotations
import argparse, json, sys
from itertools import product
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = REPO_ROOT / "data"
ARCHIVE_DIR = DATA_DIR / "archive"

PG_PYTH = 1.83  # held constant


def _walk_snapshots():
    """Yield (date, pk, snap) for every archived pregame snapshot that has a
    `components` block (i.e., enough info to replay the model)."""
    if not ARCHIVE_DIR.exists():
        return
    for date_dir in sorted(ARCHIVE_DIR.iterdir()):
        if not date_dir.is_dir():
            continue
        f = date_dir / "pregame_inputs.json"
        if not f.exists():
            continue
        try:
            d = json.loads(f.read_text())
        except Exception:
            continue
        for pk, snap in (d.get("games") or {}).items():
            m = snap.get("model")
            if not m or not m.get("components"):
                continue
            yield date_dir.name, str(pk), snap


def _load_finals():
    p = DATA_DIR / "bartolo_wp.json"
    if not p.exists():
        return {}
    d = json.loads(p.read_text())
    out = {}
    for pk, g in (d.get("games") or {}).items():
        if g.get("actual_away_runs") is None or g.get("actual_home_runs") is None:
            continue
        out[str(pk)] = (g["actual_away_runs"], g["actual_home_runs"])
    return out


def _replay(snap, cfg):
    """Mirror the spec's run-projection formula using the snapshot's archived
    components, swapping in the config's parameter overrides. Returns (away,
    home, home_wp, total) or None on missing inputs."""
    c = snap["model"]["components"]
    if not c.get("away") or not c.get("home"):
        return None
    aw, hm = c["away"], c["home"]
    away_off = aw.get("offense")
    home_off = hm.get("offense")
    if away_off is None or home_off is None:
        return None
    park = (c.get("park") or 1.0)
    # park multiplier: shrink/amplify the deviation from 1.0
    if cfg["park_mul"] != 1.0:
        park = 1.0 + (park - 1.0) * cfg["park_mul"]
    # Override the bullpen weight in the staff blend
    bp_w = cfg["bp_weight"]
    sp_w = 1.0 - bp_w
    # Reproduce blend with our cfg
    def blend(staff, off):
        # pw clamped to cfg's range (was 0.50/0.80)
        pw = max(cfg["pw_floor"], min(cfg["pw_ceil"], staff["pw"]))
        staff_ra = (staff["spRA"] * sp_w + staff["bpRA"] * bp_w * staff["bpf"]) * staff["pqm"]
        return staff_ra * pw + off * (1 - pw)

    if not aw.get("staff") or not hm.get("staff"):
        return None
    # Components.away.staff = the staff the AWAY team faces (= HOME staff)
    away_runs = (blend(aw["staff"], away_off) * park + aw["BSR"] - hm["DEF"]) * cfg["hfa_away"]
    home_runs = (blend(hm["staff"], home_off) * park + hm["BSR"] - aw["DEF"]) * cfg["hfa_home"]
    away_runs = max(0.5, away_runs)
    home_runs = max(0.5, home_runs)
    ap = away_runs ** PG_PYTH
    hp_ = home_runs ** PG_PYTH
    home_wp = hp_ / (hp_ + ap)
    home_for_total = home_runs - (home_runs / 9 * home_wp * (0.7 + 0.3 * home_wp))
    return away_runs, home_runs, home_wp, away_runs + home_for_total


def _stats(rows):
    """rows: list of (proj_total, actual_total, home_wp, actual_home_win)"""
    n = len(rows)
    if not n:
        return None
    abs_err = sum(abs(r[0] - r[1]) for r in rows) / n
    bias = sum(r[0] - r[1] for r in rows) / n
    brier = sum((r[2] - r[3]) ** 2 for r in rows) / n
    fav_correct = sum(1 for r in rows if (r[2] >= 0.5) == (r[3] == 1)) / n
    return {
        "n": n,
        "total_mae": round(abs_err, 3),
        "total_bias": round(bias, 3),
        "wp_brier": round(brier, 4),
        "fav_cover_pct": round(fav_correct * 100, 1),
    }


# ── parameter grid ─────────────────────────────────────────────────────────
# Small starter grid — expand once we have enough data to support it.
CONFIG_GRID = {
    "hfa_away": [0.92, 0.96, 1.00],
    "hfa_home": [1.04, 1.08, 1.12],
    "park_mul": [0.5, 1.0, 1.5],
    "bp_weight": [0.40, 0.445, 0.50],
    "pw_floor": [0.50],
    "pw_ceil": [0.80, 0.85],
}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--output", default=str(DATA_DIR / "tune_report.json"))
    ap.add_argument("--grid-size", type=int, default=0,
                    help="cap config count (0=full grid)")
    args = ap.parse_args()

    finals = _load_finals()
    if not finals:
        print("[tune] no finals; abort", file=sys.stderr)
        return 1

    snapshots = list(_walk_snapshots())
    print(f"[tune] {len(snapshots)} snapshots, {len(finals)} finals available",
          file=sys.stderr)

    # Build full config grid
    keys = list(CONFIG_GRID.keys())
    combos = list(product(*[CONFIG_GRID[k] for k in keys]))
    if args.grid_size and len(combos) > args.grid_size:
        combos = combos[: args.grid_size]
    print(f"[tune] sweeping {len(combos)} configs", file=sys.stderr)

    results = []
    for combo in combos:
        cfg = dict(zip(keys, combo))
        rows = []
        for _, pk, snap in snapshots:
            actual = finals.get(pk)
            if not actual:
                continue
            replay = _replay(snap, cfg)
            if not replay:
                continue
            _, _, home_wp, total = replay
            actual_total = actual[0] + actual[1]
            actual_home_win = 1 if actual[1] > actual[0] else 0
            rows.append((total, actual_total, home_wp, actual_home_win))
        s = _stats(rows)
        if s:
            results.append({"config": cfg, **s})

    # Sort by total MAE for the report header
    results.sort(key=lambda r: r["total_mae"])
    best_mae = results[0] if results else None
    best_brier = min(results, key=lambda r: r["wp_brier"]) if results else None

    payload = {
        "n_snapshots": len(snapshots),
        "n_finals": len(finals),
        "n_configs": len(results),
        "best_total_mae": best_mae,
        "best_wp_brier": best_brier,
        "configs": results,
    }
    Path(args.output).write_text(json.dumps(payload, indent=2))
    print(f"[tune] wrote {args.output}", file=sys.stderr)
    if best_mae:
        print(f"[tune] best total MAE: {best_mae['total_mae']} runs at "
              f"{best_mae['config']} (n={best_mae['n']})", file=sys.stderr)
    if best_brier:
        print(f"[tune] best WP Brier: {best_brier['wp_brier']} at "
              f"{best_brier['config']}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
