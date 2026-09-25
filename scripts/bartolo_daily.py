"""
B.A.R.T.O.L.O. | Daily post-game WP runner.

For each Final game today (ET business day), resample batted-ball outcomes
via the trained BattedBallModel, apply the HP-ump favor-runs adjustment,
and emit per-game summary dicts to data/bartolo_wp.json.

POST-GAME REPLAY, PRO-SEEDED (Fable 2026-09-25 ruling). This is an explicitly
separate, labelled "post-game replay (pro-seeded)" product — NOT a pregame price,
and it never competes with the board. Its game-start (t=0) state must be the pro
number: the Monte-Carlo is seeded from the sheet's pro cells for that game
(home_wp = S10 at lock; per-inning expected runs from the pro F5/L4 split:
away F5 = R2/5, away L4 = (R9-R2)/4; home F5 = S2/5, home L4 = (X9-S2)/4), NOT from
an internal run model. Seeded from pro, every replay opens at the pro WP and ends
at 0/1, so the path grades the pro line rather than a second engine. `_pro_seed()`
below assembles that seed and attaches it to the sim payload as `pro_seed`; the
Phase-2 model must consume it as the t=0 baseline.

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


def _pro_seed(game_pk):
    """Assemble the pro-seed for a game's replay (Fable 9/25): home win prob at
    lock plus per-inning expected-run rates from the pro F5/L4 split. Reads the
    published pro feeds (data/pro_projections.json when the internal engine is
    live, else data/sheet_projections.json for full-game + data/f5_projections.json
    for F5). Returns None if the pro number isn't available for this game."""
    import json as _json
    pk = str(game_pk)
    def _load(rel):
        p = REPO_ROOT / "data" / rel
        try: return _json.loads(p.read_text())
        except Exception: return {}
    pro = (_load("pro_projections.json").get("games") or {}).get(pk)
    if pro and pro.get("home_wp") is not None:
        R9, X9 = pro.get("away_runs"), pro.get("home_runs_full9")
        R2, S2 = pro.get("f5_away_runs"), pro.get("f5_home_runs")
        home_wp = pro.get("home_wp")
    else:
        sp = (_load("sheet_projections.json").get("games") or {}).get(pk) or {}
        f5 = (_load("f5_projections.json").get("games") or {}).get(pk) or {}
        home_wp = sp.get("home_wp")
        R9 = sp.get("away_runs")
        X9 = (sp.get("home_runs") / 0.936) if sp.get("home_runs") is not None else None   # un-trim the 9th
        R2, S2 = f5.get("away_runs"), f5.get("home_runs")
    if home_wp is None or R9 is None or X9 is None or R2 is None or S2 is None:
        return None
    return {
        "home_wp": home_wp, "away_wp": 1 - home_wp,
        "away_rate_f5": R2 / 5.0, "away_rate_l4": max(0.0, (R9 - R2)) / 4.0,
        "home_rate_f5": S2 / 5.0, "home_rate_l4": max(0.0, (X9 - S2)) / 4.0,
        "source": "pro",
    }


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
        from bartolo.ump_adjust import apply_ump_adjustment, compute_ump_favor
        from bartolo.ingest import fetch_schedule, fetch_game_pbp, extract_umpire
        from bartolo.game_stats import compute_game_stats
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

    # The script runs at overnight anchors (~midnight / 2 AM / 8 AM ET) plus
    # during the day. At the overnight anchors, _today_et() returns the
    # upcoming slate whose games haven't started — fetch_schedule then returns
    # 0 Final games and we emit a stub, freezing bartolo_wp.json. Target
    # YESTERDAY when we're in the overnight window so the post-game sim runs
    # against games that actually finished.
    today_et = _today_et()
    now_utc = datetime.datetime.now(datetime.timezone.utc)
    is_overnight = now_utc.hour < 13  # before 9 AM ET, target yesterday
    target = (today_et - datetime.timedelta(days=1)) if is_overnight else today_et
    print(f"[bartolo_daily] now_utc={now_utc.hour}h, today_et={today_et.isoformat()}, "
          f"target={target.isoformat()} (overnight={is_overnight})")

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
        pbp = None
        try:
            pbp = fetch_game_pbp(g.game_pk)
            ump_name = extract_umpire(pbp) if pbp else ""
        except Exception:
            ump_name = ""

        # Umpire favor computed from this game's called pitches (Statcast).
        ump_away, ump_home = compute_ump_favor(gdf)
        gstats = compute_game_stats(gdf, pbp)

        payload = {
            "game_pk": g.game_pk,
            "game_date": g.game_date.isoformat(),
            "away_team": g.away_team,
            "home_team": g.home_team,
            "actual_away_runs": g.away_runs,
            "actual_home_runs": g.home_runs,
            "statcast": gdf,
            "pro_seed": _pro_seed(g.game_pk),   # t=0 baseline (Fable 9/25): seed the MC from the pro line, not an internal model
        }
        try:
            sim = run_simulation(payload, model, n_sims=10000, seed=42)
            adj = apply_ump_adjustment(sim, ump_away, ump_home)
        except Exception as e:
            print(f"  ERR simulating {g.display}: {e}")
            continue

        out_games[str(g.game_pk)] = {
            **adj.frontend_dict(ump_name=ump_name, venue=g.venue, game_stats=gstats),
            "game_pk": str(g.game_pk),
            "game_date": g.game_date.isoformat(),
            "n_batted_balls": int((gdf["type"] == "X").sum()),
        }
        print(f"  {g.display}: WP away={adj.base.away_win_prob:.3f} "
              f"(ump-adj={adj.ump_adjusted_away_wp:.3f})")

    # Snapshot today into the per-date archive, then rebuild the flat file from
    # the WHOLE archive. This makes the daily job accumulate + self-heal (a flat
    # file is just a projection of the archive), instead of overwriting it with
    # only today's games.
    from bartolo.archive import write_date_archive, aggregate_archives
    write_date_archive(REPO_ROOT, target.isoformat(), out_games, status="ok")
    payload = aggregate_archives(REPO_ROOT)
    print(f"  wrote {len(out_games)} games for {target.isoformat()} to archive; "
          f"flat bartolo_wp.json now has {payload['n_games']} games")
    return 0


if __name__ == "__main__":
    sys.exit(main())
