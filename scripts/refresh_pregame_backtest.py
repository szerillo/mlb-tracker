#!/usr/bin/env python3
"""
Pre-game backtest aggregator. Joins every archived pregame_inputs.json with
the post-game bartolo_wp finals (actual runs + resampled WP) and writes
data/pregame_backtest.json. The frontend reads this to show:

  • per-game model proj vs actual outcome
  • headline calibration stats (total MAE, ML log-loss / Brier, home-WP
    calibration buckets, fav-cover %)

Pre-game snapshots first start landing 2026-05-30 (see refresh_pregame_archive.py).
Bartolo_wp finals cover the season since opening day.

Output schema:
{
  "generated_at": ISO,
  "n_games": int,
  "stats": {
    "total_mae": float,            # mean abs error on projected total vs actual total
    "total_bias": float,           # mean (proj - actual)
    "wp_brier": float,             # mean (proj_home_wp - actual_home_win)^2
    "fav_cover_pct": float,        # how often the model's favorite actually won
    "calibration": [               # 5 buckets of model home_wp
      {"bucket": "0.45-0.55", "n": int, "model": float, "actual": float}, ...
    ]
  },
  "games": [
    {date, game_pk, away_abbr, home_abbr, proj_away, proj_home, proj_total,
     proj_home_wp, actual_away, actual_home, actual_total, actual_home_win,
     total_err, ml_correct},
    ...
  ]
}

USAGE:
  python scripts/refresh_pregame_backtest.py            # rebuild from all archives
"""
from __future__ import annotations
import datetime, json, sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = REPO_ROOT / "data"
ARCHIVE_DIR = DATA_DIR / "archive"
OUTPUT = DATA_DIR / "pregame_backtest.json"


def _load_finals():
    """Map game_pk → {actual_away, actual_home, home_wp_resampled} from the
    flat bartolo_wp.json (which the daily/backfill flow keeps current)."""
    p = DATA_DIR / "bartolo_wp.json"
    if not p.exists():
        return {}
    d = json.loads(p.read_text())
    out = {}
    for pk, g in (d.get("games") or {}).items():
        aa = g.get("actual_away_runs")
        ah = g.get("actual_home_runs")
        if aa is None or ah is None:
            continue
        out[str(pk)] = {
            "actual_away": aa,
            "actual_home": ah,
            "home_wp_resampled": g.get("home_wp"),
            "ump_adj_home_wp": g.get("ump_adj_home_wp"),
            "game_date": g.get("game_date"),
        }
    return out


def _walk_pregame_snapshots():
    """Yield (date_iso, game_pk_str, snap_dict) for every archived snapshot."""
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
            if not snap.get("model"):
                continue
            yield date_dir.name, str(pk), snap


def main():
    finals = _load_finals()
    if not finals:
        print("[backtest] no finals loaded; skipping", file=sys.stderr)
        OUTPUT.write_text(json.dumps({
            "generated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds"),
            "n_games": 0, "stats": None, "games": [],
            "note": "no bartolo_wp finals yet",
        }))
        return 0

    rows = []
    for date_iso, pk, snap in _walk_pregame_snapshots():
        actual = finals.get(pk)
        if not actual:
            continue
        m = snap["model"]
        proj_away = m["away_runs"]
        proj_home = m["home_runs_for_total"]  # already 9th-inning adjusted
        proj_total = m["total"]
        proj_home_wp = m["home_wp"]
        actual_away = actual["actual_away"]
        actual_home = actual["actual_home"]
        actual_total = actual_away + actual_home
        actual_home_win = 1 if actual_home > actual_away else 0
        proj_fav_home = proj_home_wp >= 0.5
        ml_correct = 1 if (proj_fav_home == bool(actual_home_win)) else 0
        rows.append({
            "date": date_iso,
            "game_pk": pk,
            "away_abbr": snap.get("away_abbr"),
            "home_abbr": snap.get("home_abbr"),
            "proj_away": round(proj_away, 2),
            "proj_home": round(proj_home, 2),
            "proj_total": round(proj_total, 2),
            "proj_home_wp": round(proj_home_wp, 4),
            "actual_away": actual_away,
            "actual_home": actual_home,
            "actual_total": actual_total,
            "actual_home_win": actual_home_win,
            "total_err": round(proj_total - actual_total, 2),
            "ml_correct": ml_correct,
            "confirmed_lineup": m.get("confirmed", False),
        })

    rows.sort(key=lambda r: (r["date"], r["game_pk"]))

    # ── headline stats ────────────────────────────────────────────────────
    stats = None
    if rows:
        n = len(rows)
        total_errs = [r["total_err"] for r in rows]
        total_mae = sum(abs(e) for e in total_errs) / n
        total_bias = sum(total_errs) / n
        wp_brier = sum((r["proj_home_wp"] - r["actual_home_win"]) ** 2 for r in rows) / n
        fav_cover = sum(r["ml_correct"] for r in rows) / n
        # Home-WP calibration buckets — does the model's "60% home" cohort
        # actually win 60% of the time? Five buckets across the range.
        BUCKETS = [(0.00, 0.40), (0.40, 0.50), (0.50, 0.60), (0.60, 0.70), (0.70, 1.00)]
        calibration = []
        for lo, hi in BUCKETS:
            sub = [r for r in rows if lo <= r["proj_home_wp"] < hi]
            if not sub:
                calibration.append({"bucket": f"{lo:.2f}-{hi:.2f}", "n": 0,
                                    "model": None, "actual": None})
                continue
            mod = sum(r["proj_home_wp"] for r in sub) / len(sub)
            act = sum(r["actual_home_win"] for r in sub) / len(sub)
            calibration.append({
                "bucket": f"{lo:.2f}-{hi:.2f}",
                "n": len(sub),
                "model": round(mod, 4),
                "actual": round(act, 4),
            })
        stats = {
            "total_mae": round(total_mae, 3),
            "total_bias": round(total_bias, 3),
            "wp_brier": round(wp_brier, 4),
            "fav_cover_pct": round(fav_cover * 100, 1),
            "calibration": calibration,
        }

    payload = {
        "generated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds"),
        "n_games": len(rows),
        "n_pregame_snapshots": sum(1 for _ in _walk_pregame_snapshots()),
        "stats": stats,
        "games": rows,
    }
    OUTPUT.write_text(json.dumps(payload, separators=(",", ":")))
    if stats:
        print(f"[backtest] n={len(rows)}; total MAE={stats['total_mae']}, "
              f"WP Brier={stats['wp_brier']}, fav-cover={stats['fav_cover_pct']}%",
              file=sys.stderr)
    else:
        print(f"[backtest] n={len(rows)} (no stats yet — need finals)", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
