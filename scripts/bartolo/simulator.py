"""
B.A.R.T.O.L.O. | Game-level resampling simulator.

Given a past game's events, resample batted-ball outcomes many times to
produce a distribution of plausible final scores. Returns win probability,
run distributions, and summary statistics.

The approach:
    1. Keep walks, Ks, HBP, steals, pickoffs as-is (they are what they are)
    2. Resample each batted ball's outcome via BattedBallModel
    3. Convert each simulated sequence of events into a final score using a
       simple bases-out run-scoring model
    4. Tabulate the win-probability distribution across all simulations

This is NOT a pitch-by-pitch simulator — it's an event-level resampler. That
makes it MUCH faster than a Monte Carlo approach while still capturing the
outcome uncertainty that matters most (batted ball luck).
"""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Optional

import numpy as np
import pandas as pd

from .model import BattedBallModel, OUTCOMES


# -------------------------------------------------------------------
# Event-to-runs conversion (simplified)
# -------------------------------------------------------------------
# Approximate run values — in a full build, use actual base/out state
# transition matrix. This is fine for v1.
OUTCOME_RUN_VALUES = {
    "out":      -0.03,  # small negative (uses an out)
    "single":   +0.47,
    "double":   +0.78,
    "triple":   +1.03,
    "home_run": +1.40,
    "walk":     +0.32,
    "strikeout":-0.27,
    "hbp":      +0.34,
}

# Precomputed run-value vector in OUTCOMES order (for vectorized model ops)
_BB_RUN_VALUES = np.array([OUTCOME_RUN_VALUES[o] for o in OUTCOMES])

# "Deserved runs" calibration. estimate_model_expected_lw() returns the model's
# expected linear-weight run value for a team's batted balls + fixed events, but
# the raw LW scale is inflated (~+2 runs/team) because the batted-ball "out" weight
# is small. Calibrated against 264 team-games (spread across the 2026 season) so
# deserved_runs recenters on the real league mean (~4.43) and correlates ~0.67 with
# actual: deserved = A * raw_lw + B, clamped at 0.
DESERVED_CAL_A = 0.9452
DESERVED_CAL_B = -1.6018


def calibrate_deserved(raw_lw: float) -> float:
    """Map a raw model linear-weight run estimate to the calibrated runs scale."""
    return max(0.0, DESERVED_CAL_A * float(raw_lw) + DESERVED_CAL_B)


@dataclass
class GameEvents:
    """Normalized per-team event list for a game.
    All non-batted-ball events are held fixed; only batted balls get resampled.
    """
    batted_balls: pd.DataFrame = field(default_factory=pd.DataFrame)
    walks: int = 0
    strikeouts: int = 0
    hit_by_pitches: int = 0
    other_runs_scored: float = 0.0  # baserunning, errors, WPA not captured by the model
    catch_prob: Optional[pd.Series] = None  # aligned with batted_balls


def split_game_events(statcast_df: pd.DataFrame, home_team: str, away_team: str) -> tuple[GameEvents, GameEvents]:
    """Split a Statcast dataframe into home-batting and away-batting event sets."""
    # Home batters = inning_topbot == "Bot"
    home_df = statcast_df[statcast_df["inning_topbot"] == "Bot"]
    away_df = statcast_df[statcast_df["inning_topbot"] == "Top"]

    def build(df):
        bb = df[df["type"] == "X"].copy()
        walks = int((df["events"] == "walk").sum())
        ks = int((df["events"] == "strikeout").sum() + (df["events"] == "strikeout_double_play").sum())
        hbp = int((df["events"] == "hit_by_pitch").sum())
        # Extract catch_prob if present in the feed
        cp = bb["estimated_ba_using_speedangle"] if "estimated_ba_using_speedangle" in bb.columns else None
        return GameEvents(batted_balls=bb, walks=walks, strikeouts=ks, hit_by_pitches=hbp,
                          catch_prob=cp)
    return build(away_df), build(home_df)


# -------------------------------------------------------------------
# Simulator
# -------------------------------------------------------------------
@dataclass
class SimResult:
    n_sims: int
    away_runs: np.ndarray  # shape (n_sims,)
    home_runs: np.ndarray
    away_team: str = ""
    home_team: str = ""
    actual_away_runs: int = 0
    actual_home_runs: int = 0
    # Calibrated "deserved" runs from quality of contact. This is the sim ANCHOR:
    # each team's run distribution is centered here (E[sim] = deserved), so the
    # win probability reflects luck-neutral contact quality rather than the actual
    # score. Compare against actual_*_runs to see who out-/under-hit their result.
    deserved_away_runs: float = 0.0
    deserved_home_runs: float = 0.0

    @property
    def away_win_prob(self) -> float:
        return float(((self.away_runs > self.home_runs).mean() +
                      0.5 * (self.away_runs == self.home_runs).mean()))

    @property
    def home_win_prob(self) -> float:
        return 1 - self.away_win_prob

    @property
    def summary(self) -> dict:
        return {
            "n_sims": self.n_sims,
            "away_team": self.away_team,
            "home_team": self.home_team,
            "actual_away_runs": self.actual_away_runs,
            "actual_home_runs": self.actual_home_runs,
            "sim_away_mean": float(self.away_runs.mean()),
            "sim_home_mean": float(self.home_runs.mean()),
            "sim_away_median": float(np.median(self.away_runs)),
            "sim_home_median": float(np.median(self.home_runs)),
            "away_win_prob": self.away_win_prob,
            "home_win_prob": self.home_win_prob,
            "p_away_wins_exactly": float((self.away_runs > self.home_runs).mean()),
            "p_home_wins_exactly": float((self.home_runs > self.away_runs).mean()),
            "p_tied": float((self.away_runs == self.home_runs).mean()),
        }


def simulate_team_runs(events: GameEvents, model: BattedBallModel,
                       n_sims: int = 10000, rng=None) -> np.ndarray:
    """Resample batted-ball outcomes and estimate run distribution for one team.

    Uses OUTCOME_RUN_VALUES to map event → expected runs added, then sums
    + adds walks/Ks/HBPs as fixed run-value contributions + team's other_runs_scored.
    This is a *linear-weights approximation*; for v2, replace with base/out
    state simulation.
    """
    if rng is None:
        rng = np.random.default_rng()
    n_bb = len(events.batted_balls)

    # Fixed contribution from non-batted-ball events
    fixed = (events.walks * OUTCOME_RUN_VALUES["walk"]
             + events.strikeouts * OUTCOME_RUN_VALUES["strikeout"]
             + events.hit_by_pitches * OUTCOME_RUN_VALUES["hbp"]
             + events.other_runs_scored)

    if n_bb == 0:
        return np.full(n_sims, fixed)

    # Sample outcome indices
    outcomes = model.sample_outcomes(events.batted_balls, n_sims=n_sims,
                                     catch_prob=events.catch_prob, rng=rng)
    # Vectorized run calc: map outcome idx → run value
    bb_runs = _BB_RUN_VALUES[outcomes].sum(axis=1)  # shape (n_sims,)
    return bb_runs + fixed


def run_simulation(game_payload: dict, model: BattedBallModel,
                   n_sims: int = 10000, seed: int = 42) -> SimResult:
    """Main entry: run full game simulation.

    game_payload is the dict returned by scripts.bartolo.ingest.load_or_fetch_game().
    """
    rng = np.random.default_rng(seed)
    away_events, home_events = split_game_events(
        game_payload["statcast"],
        home_team=game_payload["home_team"],
        away_team=game_payload["away_team"],
    )
    # DESERVED-RUNS ANCHOR.
    # Each team's run distribution is centered on its calibrated *deserved* runs
    # (luck-neutral quality of contact) rather than on the actual score, so the
    # resulting Win Prob answers "who deserved to win given how they hit?" instead
    # of trivially "who scored more?".
    #
    # Mechanics: bb_runs_sim is drawn from model.predict_proba(), so its expected
    # value is the model's per-BB expectation (== away_lw). Setting
    #   other_runs_scored = deserved - away_lw
    # makes E[sim_total] = E[bb_runs_sim] + other_runs_scored
    #                    = away_lw + (deserved - away_lw) = deserved  ✓
    # The model-EV anchor (away_lw) still cancels cleanly, so we keep the
    # +5 runs/game de-bias fix while shifting the center from actual → deserved.
    away_lw = estimate_model_expected_lw(away_events, model)
    home_lw = estimate_model_expected_lw(home_events, model)
    des_away = calibrate_deserved(away_lw)
    des_home = calibrate_deserved(home_lw)
    away_events.other_runs_scored = des_away - away_lw
    home_events.other_runs_scored = des_home - home_lw

    away_runs = simulate_team_runs(away_events, model, n_sims=n_sims, rng=rng)
    home_runs = simulate_team_runs(home_events, model, n_sims=n_sims, rng=rng)

    return SimResult(
        n_sims=n_sims,
        away_runs=np.clip(away_runs, 0, None),
        home_runs=np.clip(home_runs, 0, None),
        away_team=game_payload["away_team"],
        home_team=game_payload["home_team"],
        actual_away_runs=game_payload["actual_away_runs"],
        actual_home_runs=game_payload["actual_home_runs"],
        deserved_away_runs=des_away,
        deserved_home_runs=des_home,
    )


def estimate_linear_weights(events: GameEvents) -> float:
    """Estimate expected runs from events using ACTUAL outcomes' linear weights.

    DEPRECATED for use as the simulator anchor — kept for reference/diagnostics.
    Anchoring the residual on this quantity causes a systematic ~+5 runs/game
    bias because the model's resampling distribution does not exactly match
    the actual outcome distribution. Use estimate_model_expected_lw() instead.
    """
    from .model import EVENT_TO_OUTCOME
    if len(events.batted_balls) == 0:
        return (events.walks * OUTCOME_RUN_VALUES["walk"]
                + events.strikeouts * OUTCOME_RUN_VALUES["strikeout"]
                + events.hit_by_pitches * OUTCOME_RUN_VALUES["hbp"])
    actual = events.batted_balls["events"].map(EVENT_TO_OUTCOME).fillna("out")
    lw = actual.map(OUTCOME_RUN_VALUES).sum()
    return float(lw
                 + events.walks * OUTCOME_RUN_VALUES["walk"]
                 + events.strikeouts * OUTCOME_RUN_VALUES["strikeout"]
                 + events.hit_by_pitches * OUTCOME_RUN_VALUES["hbp"])


def estimate_model_expected_lw(events: GameEvents, model: BattedBallModel) -> float:
    """Expected linear-weights run value using the MODEL's per-BB outcome
    distribution (NOT the actual realized outcomes).

    For each batted ball, compute Σ_k P_model(outcome_k | ball_features) * lw_k.
    Sum across the team's BBs and add the deterministic walks/Ks/HBP lw
    contributions. Walks, Ks, and HBPs are held fixed (not resampled) so their
    lw values go into the residual anchor directly, exactly cancelling in the
    sim math — E[sim_total] = actual_runs.
    """
    fixed_event_lw = (events.walks * OUTCOME_RUN_VALUES["walk"]
                      + events.strikeouts * OUTCOME_RUN_VALUES["strikeout"]
                      + events.hit_by_pitches * OUTCOME_RUN_VALUES["hbp"])
    if len(events.batted_balls) == 0:
        return fixed_event_lw
    # Model predicts per-BB probability vector (n_bb, 5) in OUTCOMES order.
    p = model.predict_proba(events.batted_balls, catch_prob=events.catch_prob)
    # Expected run value per BB = dot(p_row, lw_vector); sum across BBs.
    bb_model_lw = float((p * _BB_RUN_VALUES).sum())
    return bb_model_lw + fixed_event_lw
