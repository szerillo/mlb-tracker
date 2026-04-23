#!/usr/bin/env python3
"""
B.A.R.T.O.L.O. | Backtest runner.

Walks data/archive/*/bartolo_wp.json, computes model calibration stats
against actual results, writes data/bartolo_backtest.json.

Pure stdlib — no pandas/numpy needed. Runs in under a second for a
season of games, so it's cheap to re-run on every workflow tick.
"""
from __future__ import annotations
import datetime
import json
import math
import os
import sys
from collections import defaultdict
from pathlib import Path

SCRIPTS_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPTS_DIR.parent
ARCHIVE_DIR = REPO_ROOT / "data" / "archive"
OUTPUT = REPO_ROOT / "data" / "bartolo_backtest.json"
# Flat per-game WP file — frontend reads this for the Win Prob detail view.
# Normalized to the key names the frontend expects (see `_frontend_shape`).
FLAT_WP_OUTPUT = REPO_ROOT / "data" / "bartolo_wp.json"


def _frontend_shape(game_date: str, game_pk: str, g: dict) -> dict | None:
    """Translate a raw archive/daily game summary into the keys the frontend reads.

    Backend emits: away_win_prob, sim_away_mean, ump_adjusted_away_wp,
    umpire_name, ump_favor_away_runs, etc.
    Frontend reads: away_wp, away_exp_runs, ump_adj_away_wp, ump_name,
    ump_favor_away, etc.

    Drops games with no win-prob data.
    """
    awp = g.get("away_win_prob")
    hwp = g.get("home_win_prob")
    if awp is None or hwp is None:
        return None
    return {
        "game_date": game_date,
        "game_pk": str(game_pk),
        "away_team": g.get("away_team"),
        "home_team": g.get("home_team"),
        "actual_away_runs": g.get("actual_away_runs"),
        "actual_home_runs": g.get("actual_home_runs"),
        "away_wp": awp,
        "home_wp": hwp,
        "away_exp_runs": g.get("sim_away_mean"),
        "home_exp_runs": g.get("sim_home_mean"),
        "ump_name": g.get("umpire_name"),
        "ump_favor_away": g.get("ump_favor_away_runs"),
        "ump_favor_home": g.get("ump_favor_home_runs"),
        "ump_adj_away_wp": g.get("ump_adjusted_away_wp"),
        "ump_adj_home_wp": g.get("ump_adjusted_home_wp"),
        "venue": g.get("venue"),
        "n_batted_balls": g.get("n_batted_balls"),
        # Histograms aren't in archives today; frontend renders empty state if missing
        "away_hist": g.get("away_hist"),
        "home_hist": g.get("home_hist"),
        "away_actual_idx": g.get("away_actual_idx"),
        "home_actual_idx": g.get("home_actual_idx"),
        # Edges come from Action Network scrape — placeholder empty list for now
        "edges": g.get("edges", []),
    }


def _build_flat_wp_map() -> dict:
    """Walk data/archive/*/bartolo_wp.json and the current data/bartolo_wp.json,
    merge into a single games map keyed by gamePk (normalized for frontend).
    Today's entries win over archived copies of the same gamePk."""
    out: dict[str, dict] = {}
    # Archive first (oldest → newest)
    if ARCHIVE_DIR.exists():
        for date_dir in sorted(ARCHIVE_DIR.iterdir()):
            if not date_dir.is_dir():
                continue
            f = date_dir / "bartolo_wp.json"
            if not f.exists():
                continue
            try:
                payload = json.loads(f.read_text())
            except Exception as e:
                print(f"[backtest] skip archive {date_dir.name}: {e}", file=sys.stderr)
                continue
            for pk, g in (payload.get("games") or {}).items():
                shaped = _frontend_shape(date_dir.name, pk, g)
                if shaped is not None:
                    out[str(pk)] = shaped
    # Today's flat file (overrides archive copies for same gamePk)
    if FLAT_WP_OUTPUT.exists():
        try:
            today = json.loads(FLAT_WP_OUTPUT.read_text())
            wd = today.get("window_date", "")
            for pk, g in (today.get("games") or {}).items():
                shaped = _frontend_shape(wd, pk, g)
                if shaped is not None:
                    out[str(pk)] = shaped
        except Exception as e:
            print(f"[backtest] skip today's flat file: {e}", file=sys.stderr)
    return out


def _write_flat_wp_file(games_map: dict) -> None:
    """Write the frontend-shaped flat file, preserving any existing top-level
    status/window_date written by bartolo_daily.py (for debugging), but
    with the merged archive+today games map."""
    preserved = {}
    if FLAT_WP_OUTPUT.exists():
        try:
            prev = json.loads(FLAT_WP_OUTPUT.read_text())
            preserved = {k: v for k, v in prev.items() if k in ("window_date", "model_path")}
        except Exception:
            pass
    payload = {
        "generated_at": datetime.datetime.now(datetime.timezone.utc)
                                          .isoformat(timespec="seconds"),
        **preserved,
        "status": "ok",
        "n_games": len(games_map),
        "games": games_map,
    }
    FLAT_WP_OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    FLAT_WP_OUTPUT.write_text(json.dumps(payload, indent=2))
    print(f"[backtest] wrote {FLAT_WP_OUTPUT.relative_to(REPO_ROOT)} "
          f"— n_games={len(games_map)} (archive + today merged)")


def _winner(g):
    """Return 'away' / 'home' / None (tie or unknown)."""
    a = g.get("actual_away_runs")
    h = g.get("actual_home_runs")
    if a is None or h is None:
        return None
    if a > h: return "away"
    if h > a: return "home"
    return None


def _brier(pred, actual):
    return (pred - actual) ** 2


def _log_loss(pred, actual, eps=1e-9):
    p = max(eps, min(1 - eps, pred))
    return -(actual * math.log(p) + (1 - actual) * math.log(1 - p))


def collect_games():
    """Yield (date_iso, gamePk, away_wp, actual_away_win) for every completed
    game we have a sim + final score for."""
    if not ARCHIVE_DIR.exists():
        return
    for date_dir in sorted(ARCHIVE_DIR.iterdir()):
        if not date_dir.is_dir():
            continue
        f = date_dir / "bartolo_wp.json"
        if not f.exists():
            continue
        try:
            payload = json.loads(f.read_text())
        except Exception as e:
            print(f"[backtest] skip {date_dir.name}: {e}", file=sys.stderr)
            continue
        if payload.get("status") != "ok":
            continue
        for pk, g in (payload.get("games") or {}).items():
            wp = g.get("away_win_prob")
            winner = _winner(g)
            if wp is None or winner is None:
                continue
            yield {
                "date": date_dir.name,
                "game_pk": pk,
                "away_team": g.get("away_team"),
                "home_team": g.get("home_team"),
                "away_wp": float(wp),
                "away_win": 1 if winner == "away" else 0,
                "final": (g.get("actual_away_runs"), g.get("actual_home_runs")),
                "ump_adj_wp": g.get("ump_adjusted_away_wp"),
            }


def reliability_buckets(games, n_buckets=10):
    """10 buckets across [0, 1) of predicted away WP. For each bucket, report
    mean predicted probability and actual fraction that won."""
    buckets = [[] for _ in range(n_buckets)]
    for g in games:
        idx = min(n_buckets - 1, int(g["away_wp"] * n_buckets))
        buckets[idx].append(g)
    out = []
    for i, b in enumerate(buckets):
        lo, hi = i / n_buckets, (i + 1) / n_buckets
        n = len(b)
        if n == 0:
            out.append({"bin": f"{int(lo*100)}-{int(hi*100)}%", "n": 0,
                        "predicted_mean": None, "actual_rate": None})
            continue
        pred_mean = sum(g["away_wp"] for g in b) / n
        actual_rate = sum(g["away_win"] for g in b) / n
        out.append({
            "bin": f"{int(lo*100)}-{int(hi*100)}%",
            "n": n,
            "predicted_mean": round(pred_mean, 4),
            "actual_rate": round(actual_rate, 4),
        })
    return out


def hit_rates_at_thresholds(games, thresholds=(0.55, 0.60, 0.65, 0.70, 0.75, 0.80)):
    """For each threshold, report hit rate when the model picked either side
    at that confidence or higher. 'Picked side' = whichever side has wp >= T.
    """
    out = {}
    for t in thresholds:
        picks = []
        for g in games:
            awp = g["away_wp"]
            hwp = 1 - awp
            # Model picks the side with wp >= t (if either)
            if awp >= t:
                picks.append(g["away_win"])
            elif hwp >= t:
                picks.append(1 - g["away_win"])
        n = len(picks)
        hit_rate = (sum(picks) / n) if n else None
        out[f"{int(t*100)}pct"] = {"n": n, "hit_rate": round(hit_rate, 4) if hit_rate is not None else None}
    return out


def daily_summary(games):
    by_date = defaultdict(list)
    for g in games:
        by_date[g["date"]].append(g)
    out = []
    for date in sorted(by_date.keys()):
        gs = by_date[date]
        brier = sum(_brier(g["away_wp"], g["away_win"]) for g in gs) / len(gs)
        # Hit rate: always pick the favorite
        hits = 0
        for g in gs:
            fav_is_away = g["away_wp"] > 0.5
            fav_won = (g["away_win"] == 1) if fav_is_away else (g["away_win"] == 0)
            hits += 1 if fav_won else 0
        out.append({
            "date": date,
            "n_games": len(gs),
            "brier": round(brier, 4),
            "fav_hit_rate": round(hits / len(gs), 4),
        })
    return out


def rolling_7d_brier(daily):
    """Rolling-7 Brier: for each date, avg Brier across that date and prior 6."""
    # index daily by date for quick lookup
    idx = {d["date"]: d for d in daily}
    dates = sorted(idx.keys())
    out = []
    date_objs = [datetime.date.fromisoformat(d) for d in dates]
    for i, d in enumerate(dates):
        window_start = date_objs[i] - datetime.timedelta(days=6)
        window = [dates[j] for j in range(len(dates))
                  if window_start <= date_objs[j] <= date_objs[i]]
        total_g = sum(idx[w]["n_games"] for w in window)
        if total_g == 0:
            continue
        weighted = sum(idx[w]["brier"] * idx[w]["n_games"] for w in window) / total_g
        out.append({"date": d, "brier_7d": round(weighted, 4), "n_games": total_g})
    return out


def main() -> int:
    games = list(collect_games())
    if not games:
        print("[backtest] no games in archive — emitting empty backtest")
        OUTPUT.parent.mkdir(parents=True, exist_ok=True)
        OUTPUT.write_text(json.dumps({
            "generated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds"),
            "n_games": 0,
            "status": "empty",
        }, indent=2))
        return 0

    n = len(games)
    brier = sum(_brier(g["away_wp"], g["away_win"]) for g in games) / n
    logloss = sum(_log_loss(g["away_wp"], g["away_win"]) for g in games) / n

    # Hit rate picking the favorite every game
    fav_correct = sum(
        1 if (g["away_wp"] > 0.5 and g["away_win"] == 1)
           or (g["away_wp"] <= 0.5 and g["away_win"] == 0)
        else 0
        for g in games
    )
    fav_hit_rate = fav_correct / n

    dates = sorted({g["date"] for g in games})
    daily = daily_summary(games)
    payload = {
        "generated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds"),
        "status": "ok",
        "n_games": n,
        "date_range": {"start": dates[0], "end": dates[-1]},
        "headline": {
            "brier": round(brier, 4),
            "log_loss": round(logloss, 4),
            "fav_hit_rate": round(fav_hit_rate, 4),
        },
        "thresholds": hit_rates_at_thresholds(games),
        "reliability": reliability_buckets(games, n_buckets=10),
        "daily": daily,
        "rolling_7d": rolling_7d_brier(daily),
    }

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(json.dumps(payload, indent=2))
    print(f"[backtest] wrote {OUTPUT.relative_to(REPO_ROOT)} — n={n}, brier={brier:.4f}, "
          f"log_loss={logloss:.4f}, fav_hit={fav_hit_rate:.4f}")

    # Also emit the frontend-shaped flat bartolo_wp.json so past-date games
    # light up in the Win Prob detail view (not just today's).
    try:
        flat_map = _build_flat_wp_map()
        _write_flat_wp_file(flat_map)
    except Exception as e:
        print(f"[backtest] flat-wp merge failed (non-fatal): {e}", file=sys.stderr)

    return 0


if __name__ == "__main__":
    sys.exit(main())
