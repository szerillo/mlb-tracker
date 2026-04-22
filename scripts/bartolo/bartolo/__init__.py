"""B.A.R.T.O.L.O. post-game win-probability package.

Ported from the standalone bartolo project into the mlb-tracker repo as
scripts/bartolo/. Entry point is scripts/bartolo_daily.py (one level up).

Modules:
  model        — batted-ball outcome classifier (GradientBoosting)
  simulator    — event-level resampling sim → SimResult(away_runs, home_runs)
  ingest       — MLB StatsAPI schedule + Statcast pull helpers
  ump_adjust   — apply HP-ump favor-runs shift to a SimResult
"""
