# wFIP — private methodology (not shown in the tool UI)

The app tooltips only say "Weighted FIP — lower is better." The full method is kept
here (and in the `scoring.unified_score` block written into `data/pitcher_stats.json`).

## Pitcher wFIP (`unified_score`, `scripts/compute_pitcher_score.py`)
In-season CORE = availability-weighted mean of four components (85-pt pool):
rolling xFIP 28 / rolling SIERA 22 / xERA 18 / botERA 17. The two rolling
components are `0.2·L5 + 0.8·season` (recalibrated 2026-07 on the 2023–26
historical panel; was 0.6/0.4 — recency was subtracting skill info).

Blend with ROS FIP projection (FG ATC/BatX/OOPSY/ZiPS): dynamic
`w_proj = K/(IP+K)`, K = 14 SP / 20 RP; `score = w_season·core + w_proj·proj`.

Adjustments (scaled by w_season, capped):
- **K-BB% nudge:** −0.03·(L5 − season) K-BB%, cap ±0.20.
- **Velo + CSW level** (2026-07 recalibration): −0.049·(velo − lgVelo) −0.049·(CSW% − lgCSW%),
  cap ±0.40, league means from ≥30-IP arms. xFIP underweights velocity/CSW level.

`unified_score_legacy` (old 0.6/0.4, no velo/CSW) is stored alongside for comparison.

## Bullpen (combined average)
`teamAvailRelievers` returns rested arms at full wFIP + the best (n−1) tired/out
("compromised") arms, each penalized **+0.40 wFIP / −4pp K-BB%**; the single
most-disregardable compromised arm is dropped. 1 down → disregard; 2 → weight in 1;
3 → weight in 2; etc. Replaces the old rested-only pool + the ×(1+0.015·n_fatigued)
RA multiplier (which is now retired to avoid double-counting).
