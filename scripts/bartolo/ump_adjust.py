"""
B.A.R.T.O.L.O. | Umpire favor adjustment layer.

Given a simulated run distribution and ump-favor-runs for each team (from
Ump Scorecards), produce an "ump-neutral" version by shifting each team's
run distribution DOWN by the favor runs the ump gave them.

Rationale:
  - Ump Scorecards reports, for each game, how many runs the HP ump's called
    strike zone added/subtracted for each team vs. the rulebook zone.
  - If AWAY got +0.4 ump-favor runs (i.e., ump's zone HELPED the away team by 0.4 runs),
    then a "what would have happened with a correct zone" version of the sim
    shifts AWAY's run distribution down by 0.4.
  - Then recalc WP from the adjusted distributions.

This is a first-order adjustment â we're not resampling plate appearances
based on different count trajectories. That's a v2 refinement if the data
supports it. For v1, this simple shift gives us a directionally correct
"ump-adjusted WP" number we can compare to the raw sim WP.
"""
from __future__ import annotations
import numpy as np
from dataclasses import dataclass

from .simulator import SimResult


@dataclass
class UmpAdjustedResult:
    base: SimResult
    ump_favor_away_runs: float
    ump_favor_home_runs: float
    adjusted_away_runs: np.ndarray
    adjusted_home_runs: np.ndarray

    @property
    def ump_adjusted_away_wp(self) -> float:
        ties = (self.adjusted_away_runs == self.adjusted_home_runs).mean()
        return float(((self.adjusted_away_runs > self.adjusted_home_runs).mean()) + 0.5 * ties)

    @property
    def ump_adjusted_home_wp(self) -> float:
        return 1 - self.ump_adjusted_away_wp

    @property
    def wp_shift_away(self) -> float:
        """How much the ump helped the away team's WP (negative = hurt)."""
        return self.base.away_win_prob - self.ump_adjusted_away_wp

    @property
    def summary(self) -> dict:
        base_sum = self.base.summary
        return {
            **base_sum,
            "ump_favor_away_runs": self.ump_favor_away_runs,
            "ump_favor_home_runs": self.ump_favor_home_runs,
            "ump_adjusted_away_mean": float(self.adjusted_away_runs.mean()),
            "ump_adjusted_home_mean": float(self.adjusted_home_runs.mean()),
            "ump_adjusted_away_wp": self.ump_adjusted_away_wp,
            "ump_adjusted_home_wp": self.ump_adjusted_home_wp,
            "wp_shift_away": self.wp_shift_away,
        }


def apply_ump_adjustment(sim: SimResult,
                         ump_favor_away_runs: float,
                         ump_favor_home_runs: float) -> UmpAdjustedResult:
    """Return an ump-neutral version of the sim.

    ump_favor_X_runs: positive = ump helped team X. We subtract that from team X's
    simulated run distribution.
    """
    adj_away = np.clip(sim.away_runs - ump_favor_away_runs, 0, None)
    adj_home = np.clip(sim.home_runs - ump_favor_home_runs, 0, None)
    return UmpAdjustedResult(
        base=sim,
        ump_favor_away_runs=float(ump_favor_away_runs),
        ump_favor_home_runs=float(ump_favor_home_runs),
        adjusted_away_runs=adj_away,
        adjusted_home_runs=adj_home,
    )
