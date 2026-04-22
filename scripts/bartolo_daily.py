"""
B.A.R.T.O.L.O. | Daily post-game WP runner.

For each Final game today (ET business day), resample batted-ball outcomes
via the trained BattedBallModel, apply the HP-ump favor-runs adjustment,
and emit per-game summary dicts to data/bartolo_wp.json.

GATED EXIT: if the model pickle (scripts/bartolo/bartolo_model.pkl) is not
present â Phase 2 writes it â this script logs + exits 0 so the scheduled
workflow stays green while we build up.

Runtime budget target: < 2 min for ~15 games @ 10k sims each on GitHub's
ubuntu-latest runner.
"""
from __future__ import annotations
import datetime
import json
import os
import sys
from pathlib import Path

# Make scripts/ importable so we can `from bartolo import ...`
SCRIPTS_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPTS_DIR))

from _common import skip_if_not_in_window  # noqa: E402

REPO_ROOT = SCRIPTS_DIR.parent
DATA_DIR = REPO_ROOT / "data"
OUTPUT = DATA_DIR / "bartolo_wp.json"
MODEL_PATH = SCRIPTS_DIR / "bartolo" / "bartolo_model.pkl"


def _today_et() -> datetime.date:
    """Return the MLB business day in ET â matters around midnight when
    late West-Coast games are still finishing under yesterday's date."""
    return (datetime.datetime.now(datetime.timezone.utc)
            - datetime.timedelta(hours=4)).date()


def _emit_stub(reason: str) -> None:
    """Write a stub bartolo_wp.json so the frontend never sees a missing file.
    Keeps whatever valid games we may have already written on a prior run
    intact â we only overwrite if we actually produced results.
    """
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    stub = {
        "generated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds"),
        "window_date": _today_et().isoformat(),
        "status": reason,
        "games": {},
    }
    # Preserve prior games if the file exists and has content.
    if OUTPUT.exists():
        try:
            prior = json.loads(OUTPUT.read_text())
            if prior.get("games"):
                stub["games"] = prior["games"]
                stub["status"] = f"{reason} (prior games preserved)"
        except Exception:
            pass
    OUTPUT.write_text(json.dumps(stub, indent=2))
    print(f"  wrote stub to {OUTPUT} (status: {stub['status']})")


def main() -> int:
    if skip_if_not_in_window("bartolo_daily"):
        return 0

    # Gate: Phase 2 ships the trained model. Until then, emit a stub and exit.
    if not MODEL_PATH.exists():
        print(f"[bartolo_daily] model not found at {MODEL_PATH} â Phase 2 not shipped yet")
        _emit_stub("awaiting_model")
        return 0

    # Lazy imports so the gated-exit path doesn't pay the import cost.
    try:
        import pandas as pd  # noqa: F401
        from bartolo.model import BattedBallModel
        from bartolo.simulator import run_simulation
        from bartolo.ump_adjust import apply_ump_adjustment
        from bartolo.ingest import fetch_schedule, fetch_game_pbp, extract_umpire, ump_favor_for_game
    except ImportError as e:
        print(f"[bartolo_daily] missing dep: {e} â skipping")
        _emit_stub(f"missing_dep:{e}")
        return 0

    try:
        import pybaseball as pyb
    except ImportError:
        print("[bartolo_daily] pybaseball not installed â skipping")
        _emit_stub("missing_pybaseball")
        return 0

    target = _today_et()
    print(f"[bartolo_daily] target date: {target.isoformat()}")

    games = fetch_schedule(target)
    if not games:
        print("  no Final games today; emitting empty payload")
        _emit_stub("no_final_games")
        return 0
    print(f"  found {len(games)} Final games")

    # Load the trained model once.
    print(f"  loading model from {MODEL_PATH}")
    model = BattedBallModel(model_path=MODEL_PATH)
    if model.clf is None:
        print("  model loaded but .clf is None â aborting")
        _emit_stub("model_unloadable")
        return 0

    # ONE day-level Savant pull, filter per-game locally.
    print(f"  pulling day-level Statcast for {target.isoformat()}")
    try:
        day_df = pyb.statcast(start_dt=target.isoformat(), end_dt=target.isoformat())
    except Exception as e:
        print(f"  pybaseball.statcast failed: {e}")
        _emit_stub(f"statcast_error:{e}")
        return 0
    if day_df is None or len(day_df) == 0:
        print("  statcast returned empty â emitting stub")
        _emit_stub("statcast_empty")
        return 0
    print(f"  pulled {len(day_df)} pitches")

    # Per-game sim loop.
    out_games: dict[str, dict] = {}
    for g in games:
        gdf = day_df[day_df["game_pk"] == g.game_pk]
        if len(gdf) == 0:
            print(f"  skip {g.display}: no statcast rows")
            continue
        # Fetch HP ump name (best-effort; ump_favor uses team match anyway)
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
            print(f"  ERR simulating {g.display}: {e}")
            continue

        out_games[str(g.game_pk)] = {
            **adj.summary,
            "umpire_name": ump_name,
            "venue": g.venue,
            "n_batted_balls": int((gdf["type"] == "X").sum()),
        }
        print(f"  {g.display}: WP away={adj.base.away_win_prob:.3f} "
              f"(ump-adj={adj.ump_adjusted_away_wp:.3f})")

    # Write output.
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    payload_out = {
        "generated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds"),
        "window_date": target.isoformat(),
        "model_path": str(MODEL_PATH.relative_to(REPO_ROOT)),
        "status": "ok",
        "n_games": len(out_games),
        "games": out_games,
    }
    OUTPUT.write_text(json.dumps(payload_out, indent=2, default=str))
    print(f"  wrote {len(out_games)} games to {OUTPUT}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
