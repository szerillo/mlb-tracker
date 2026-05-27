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
import time
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


def _archive_complete(out_path: Path) -> bool:
    """True if a date's archive is fully populated and needs no deserved-runs
    backfill. A date counts as complete when its archive exists and either:
      - status is an off-day / empty marker (no_finals, statcast_empty), or
      - every game already carries non-null away/home deserved_runs.
    Missing archives, unreadable archives, or any game lacking deserved_runs
    are treated as incomplete so a re-run repairs them."""
    if not out_path.exists():
        return False
    try:
        d = json.loads(out_path.read_text())
    except Exception:
        return False
    if d.get("status") in ("no_finals", "statcast_empty"):
        return True
    games = d.get("games", {})
    if not games:
        # An "ok" status with zero games is genuinely complete (no Finals had
        # batted balls); anything else with no games we retry.
        return d.get("status") == "ok"
    for g in games.values():
        if g.get("away_deserved_runs") is None or g.get("home_deserved_runs") is None:
            return False
        # Win Prob must be anchored on deserved runs. Older archives were anchored
        # on the actual score (no wp_basis marker) — treat those as incomplete so a
        # re-run replaces them with the deserved-anchored sim.
        if g.get("wp_basis") != "deserved":
            return False
    return True


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
        from bartolo.ump_adjust import apply_ump_adjustment, compute_ump_favor
        from bartolo.ingest import fetch_schedule, fetch_game_pbp, extract_umpire
    except ImportError as e:
        print(f"[backfill] missing dep: {e}", file=sys.stderr)
        return 1

    print(f"[backfill] loading model from {MODEL_PATH}")
    model = BattedBallModel(model_path=MODEL_PATH)
    if model.clf is None:
        print("[backfill] model loaded but .clf is None; aborting", file=sys.stderr)
        return 1

    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)

    max_dates = int(os.environ.get("BACKFILL_MAX_DATES", "22") or "22")

    # Self-converging selection: a "force" run re-sims everything in range;
    # otherwise we only touch dates whose archive is missing deserved_runs.
    # Process most-recent dates first so the dates users browse fill earliest,
    # and cap each run (max_dates) so a single batch finishes well inside the
    # workflow's cancel-in-progress window. Any dates left over keep the
    # .backfill-now trigger in place so the next scheduled run resumes
    # automatically until coverage is complete.
    all_dates = list(_date_range(start, end))
    pending = [
        t for t in all_dates
        if force or not _archive_complete(ARCHIVE_DIR / t.isoformat() / "bartolo_wp.json")
    ]
    pending.sort(reverse=True)
    total_pending = len(pending)
    batch = pending[:max_dates] if max_dates > 0 else pending
    print(f"[backfill] {total_pending} dates need work; processing {len(batch)} "
          f"this run (max_dates={max_dates}, recent-first)")

    total_games = 0
    dates_processed = 0
    dates_skipped = max(0, len(all_dates) - total_pending)

    for target in batch:
        date_dir = ARCHIVE_DIR / target.isoformat()
        out_path = date_dir / "bartolo_wp.json"

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
        # Statcast can rate-limit / transiently fail on a long sequential backfill.
        # Retry with backoff, and pace requests so a full-season run doesn't get
        # throttled (which previously left most dates unprocessed).
        day_df = None
        for attempt in range(4):
            try:
                day_df = pyb.statcast(start_dt=target.isoformat(), end_dt=target.isoformat())
                break
            except Exception as e:
                print(f"[backfill] {target}: statcast error (attempt {attempt+1}/4): {e}", file=sys.stderr)
                time.sleep(5 * (attempt + 1))
        if day_df is None:
            print(f"[backfill] {target}: statcast failed after retries; leaving prior archive", file=sys.stderr)
            continue
        time.sleep(1.0)  # pace requests to stay under Statcast rate limits
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

            ump_away, ump_home = compute_ump_favor(gdf)

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
                **adj.frontend_dict(ump_name=ump_name, venue=g.venue),
                "game_pk": str(g.game_pk),
                "game_date": g.game_date.isoformat(),
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

    # Rebuild the flat data/bartolo_wp.json from the full archive so the Win Prob
    # tab immediately reflects everything we just wrote.
    from bartolo.archive import aggregate_archives
    payload = aggregate_archives(REPO_ROOT)

    # Own the .backfill-now trigger so coverage converges across runs. If any
    # dates still need work after this capped batch, leave (or re-create) the
    # trigger so the next scheduled run resumes; only clear it once everything
    # is filled. The workflow no longer removes the trigger unconditionally —
    # that previously let a single cancelled run strand the backfill.
    remaining = [
        t for t in all_dates
        if not _archive_complete(ARCHIVE_DIR / t.isoformat() / "bartolo_wp.json")
    ]
    flag = REPO_ROOT / ".backfill-now"
    if remaining:
        flag.write_text(
            "auto-heal: deserved-runs backfill still in progress\n"
            f"remaining={len(remaining)} (next: {remaining[-1].isoformat()})\n"
        )
        print(f"[backfill] {len(remaining)} dates still need work — keeping "
              f".backfill-now so the next scheduled run continues")
    else:
        if flag.exists():
            flag.unlink()
        print("[backfill] all dates complete — removed .backfill-now")

    print(f"[backfill] DONE — processed {dates_processed} dates, skipped {dates_skipped}, "
          f"total games={total_games}; flat bartolo_wp.json now {payload['n_games']} games")
    return 0


if __name__ == "__main__":
    sys.exit(main())
