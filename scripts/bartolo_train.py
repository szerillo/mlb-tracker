#!/usr/bin/env python3
"""
B.A.R.T.O.L.O. | One-shot GBM trainer.

Pulls Statcast batted balls and fits a HistGradientBoostingClassifier via
bartolo.model.BattedBallModel. Pickles to scripts/bartolo/bartolo_model.pkl.

Trigger: touch .train-now and commit. Workflow fires on the push, runs this
script, removes .train-now in the commit step. Model pickle is committed and
bartolo_daily.py starts using it on the NEXT workflow tick.

Runtime: ~5-10 min on GitHub's ubuntu-latest (pybaseball cold cache).
"""
from __future__ import annotations
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPTS_DIR = REPO_ROOT / "scripts"
BARTOLO_DIR = SCRIPTS_DIR / "bartolo"
MODEL_PATH = BARTOLO_DIR / "bartolo_model.pkl"

# Make scripts/ importable so we can `from bartolo import ...`
sys.path.insert(0, str(SCRIPTS_DIR))

try:
    import pandas as pd
    from pybaseball import statcast
    from bartolo.model import BattedBallModel, EVENT_TO_OUTCOME
except ImportError as e:
    print(f"Missing deps: {e}", file=sys.stderr)
    sys.exit(1)


# Use 2025 regular season only for first pass (faster, sufficient signal).
# Can expand to 2024+2025 later for ~2x data.
TRAIN_START = "2025-03-27"
TRAIN_END   = "2025-10-01"


def fetch_batted_balls(start: str, end: str) -> pd.DataFrame:
    print(f"[train] pulling Statcast {start} → {end} via pybaseball...")
    df = statcast(start_dt=start, end_dt=end)
    print(f"[train]   raw rows: {len(df):,}")
    if "events" not in df.columns:
        print("[train]   ERR: 'events' column missing from Statcast pull", file=sys.stderr)
        sys.exit(1)
    df = df[df["events"].isin(EVENT_TO_OUTCOME.keys())].copy()
    df = df.dropna(subset=["launch_speed", "launch_angle"])
    print(f"[train]   batted balls after filter: {len(df):,}")
    return df


def main():
    df = fetch_batted_balls(TRAIN_START, TRAIN_END)
    if len(df) < 10000:
        print(f"[train] too few rows ({len(df)}); aborting without writing model", file=sys.stderr)
        sys.exit(1)

    print("[train] fitting HistGradientBoostingClassifier (max_iter=400, depth=7)...")
    model = BattedBallModel()
    test_score = model.fit(df)
    print(f"[train]   holdout accuracy: {test_score:.4f}")

    if test_score < 0.40:
        print(f"[train] suspiciously low accuracy; refusing to overwrite existing model", file=sys.stderr)
        sys.exit(1)

    BARTOLO_DIR.mkdir(parents=True, exist_ok=True)
    model.save(MODEL_PATH)
    size_mb = MODEL_PATH.stat().st_size / 1e6
    print(f"[train] wrote {MODEL_PATH.relative_to(REPO_ROOT)} ({size_mb:.1f} MB)")


if __name__ == "__main__":
    main()
