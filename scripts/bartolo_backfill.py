#!/usr/bin/env python3
"""
B.A.R.T.O.L.O. | Historical backfill.

Iterates dates from BACKFILL_START → BACKFILL_END (defaults: 2026-03-25 →
yesterday), pulls Statcast per completed game via pybaseball, runs the sim,
and writes per-date archives to data/archive/YYYY-MM-DD/bartolo_wp.json.

Resume-safe: skips dates where the archive already exists unless
BACKFILL_FORCE=1. Designed to be fired via a .backfill-now push trigger or
manual workflow_dispatch.
"""
from __future__ import annotations
import datetime
import json
import os
import sys
from pathlib import Path

SCRIPTS_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPTS_DIR))

REPO_ROOT = SCRIPTS_DIR.parent
DATA_DIR = REPO_ROOT / "data"
ARCHIVE_DIR = DATA_DIR / "archive"
MODEL_PATH = SCRIPTS_DIR / "bartolo" / "bartolo_model.pkl"

DEFAULT_START = datetime.date(2026, 3, 25)


def _et_yesterday() -> datetime.date:
    et_now = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=4)
    return et_now.date() - datetime.timedelta(days=1)


def _parse_date(s: str, default: datetime.date) -> datetime.date:
    if not s:
        return default
    try:
        return datetime.date.fromisoformat(s)
    except ValueError:
        print(f"[backfill] bad date '{s}'; using default {default}", file=sys.stderr)
        return default


def _date_range(start: datetime.date, end: datetime.date):
    d = start
    one = datetime.timedelta(days=1)
    while d <= end:
        yield d
        d = d + one


def main() -> int:
    start = _parse_date(os.environ.get("BACKFILL_START", ""), DEFAULT_START)
    end   = _parse_date(os.environ.get("BACKFILL_END", ""), _et_yesterday())
    force = os.environ.get("BACKFILL_FORCE") == "1"

    if start > end:
        print(f"[backfill] start {start} > end {end}; nothing to do")
        return 0

    print(f"[backfill] range: {start} → {end}  (force={force})")

    if not MODEL_PATH.exists():
        print(f"[backfill] model not at {MODEL_PATH}; aborting", file=sys.stderr)
        return 1

    try:
        import pandas as pd
        import pybaseball as pyb
        from bartolo.model import BattedBallModel
        from bartolo.simulator import run_simulation
        from bartolo.ump_adjust import apply_ump_adjustment
        from bartolo.ingest import fetch_schedule, fetch_game_pbp, extract_umpire, ump_favor_for_game
    except ImportError as e:
        print(f"[backfill] missing dep: {e}", file=sys.stderr)
        return 1

    print(f"[backfill] loading model from {MODEL_PATH}")
    model = BattedBallModel(model_path=MODEL_PATH)
    if model.clf is None:
        print("[backfill] model loaded but .clf is None; aborting", file=sys.stderr)
        return 1

    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)

    total_games = 0
    dates_processed = 0
    dates_skipped = 0

    for target in _date_range(start, end):
        date_dir = ARCHIVE_DIR / target.isoformat()
        out_path = date_dir / "bartolo_wp.json"
        if out_path.exists() and not force:
            dates_skipped += 1
            continue

        games = fetch_schedule(target)
        if not games:
            # Off-day or no Finals — write empty marker so we don't retry forever
            date_dir.mkdir(parents=True, exist_ok=True)
            out_path.write_text(json.dumps({
                "generated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds"),
                "window_date": target.isoformat(),
                "status": "no_finals",
                "games": {},
            }, indent=2))
            dates_processed += 1
            print(f"[backfill] {target}: no Finals → marker written")
            continue

        print(f"[backfill] {target}: pulling Statcast for {len(games)} Finals...")
        try:
            day_df = pyb.statcast(start_dt=target.isoformat(), end_dt=target.isoformat())
        except Exception as e:
            print(f"[backfill] {target}: statcast error: {e}", file=sys.stderr)
            continue
        if day_df is None or len(day_df) == 0:
            print(f"[backfill] {target}: statcast empty")
            date_dir.mkdir(parents=True, exist_ok=True)
            out_path.write_text(json.dumps({
                "generated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds"),
                "window_date": target.isoformat(),
                "status": "statcast_empty",
                "games": {},
            }, indent=2))
            dates_processed += 1
            continue

        out_games: dict = {}
        for g in games:
            gdf = day_df[day_df["game_pk"] == g.game_pk]
            if len(gdf) == 0:
                continue

            try:
                pbp = fetch_game_pbp(g.game_pk)
                ump_name = extract_umpire(pbp) if pbp else ""
            except Exception:
                ump_name = ""

            ump_away, ump_home = ump_favor_for_game(g)

            payload = {
                "game_pk": g.game_pk,
                "game_date": g.game_date.isoformat(),
                "away_team": g.away_team,
                "home_team": g.home_team,
                "actual_away_runs": g.away_runs,
                "actual_home_runs": g.home_runs,
                "statcast": gdf,
            }
            try:
                sim = run_simulation(payload, model, n_sims=10000, seed=42)
                adj = apply_ump_adjustment(sim, ump_away, ump_home)
            except Exception as e:
                print(f"[backfill] {target}: sim error on {g.game_pk}: {e}", file=sys.stderr)
                continue

            out_games[str(g.game_pk)] = {
                **adj.summary,
                "umpire_name": ump_name,
                "venue": g.venue,
                "n_batted_balls": int((gdf["type"] == "X").sum()),
            }

        date_dir.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps({
            "generated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds"),
            "window_date": target.isoformat(),
            "status": "ok",
            "n_games": len(out_games),
            "games": out_games,
        }, indent=2, default=str))
        total_games += len(out_games)
        dates_processed += 1
        print(f"[backfill] {target}: wrote {len(out_games)} games to {out_path.relative_to(REPO_ROOT)}")

    print(f"[backfill] DONE — processed {dates_processed} dates, skipped {dates_skipped}, total games={total_games}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
