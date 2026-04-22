"""
B.A.R.T.O.L.O. | Batted-ball outcome classifier.

Trains a gradient-boosted multiclass classifier that predicts the outcome
distribution of a batted ball given:
    launch_speed (EV)
    launch_angle (LA)
    hit_distance_sc
    stand / p_throws
    estimated spray angle
    park effect (venue_id or one-hot)
    optional: Savant catch_probability (when present)

Improvements over dgrifka's baseline:
    1. Blend Savant catch_probability into P(out) for balls in play
    2. Add an explicit spray-angle feature (hc_x / hc_y)
    3. Use park factors as numeric features rather than one-hot (smaller model)
"""
from __future__ import annotations
import math
import pickle
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

try:
    from sklearn.ensemble import GradientBoostingClassifier, HistGradientBoostingClassifier
    from sklearn.model_selection import train_test_split
except ImportError:
    GradientBoostingClassifier = None
    HistGradientBoostingClassifier = None

# Outcome classes
OUTCOMES = ["out", "single", "double", "triple", "home_run"]
# Map Statcast 'events' into one of these classes.
EVENT_TO_OUTCOME = {
    "field_out": "out", "grounded_into_double_play": "out",
    "force_out": "out", "fielders_choice": "out", "fielders_choice_out": "out",
    "double_play": "out", "triple_play": "out", "sac_fly": "out", "sac_bunt": "out",
    "field_error": "single",  # reached on error counts as single-like outcome
    "single": "single", "double": "double", "triple": "triple", "home_run": "home_run",
}


def derive_spray_angle(hc_x: float, hc_y: float, stand: str) -> float:
    """Compute spray angle in degrees from home plate. Positive = pulled."""
    if pd.isna(hc_x) or pd.isna(hc_y):
        return np.nan
    # Home plate in Statcast coords ~ (125, 198). Pull side flip for LH.
    dx = hc_x - 125.0
    dy = 198.0 - hc_y
    ang = math.degrees(math.atan2(dx, dy))
    return -ang if stand == "L" else ang


def build_features(df: pd.DataFrame) -> pd.DataFrame:
    """Given a Statcast dataframe of batted balls, produce model features."""
    out = pd.DataFrame(index=df.index)
    out["launch_speed"] = df.get("launch_speed").astype(float)
    out["launch_angle"] = df.get("launch_angle").astype(float)
    out["hit_distance_sc"] = df.get("hit_distance_sc").astype(float)
    out["stand_R"] = (df.get("stand") == "R").astype(int)
    out["p_throws_R"] = (df.get("p_throws") == "R").astype(int)
    out["spray_angle"] = df.apply(lambda r: derive_spray_angle(r.get("hc_x"), r.get("hc_y"), r.get("stand")), axis=1)
    out["venue_id"] = df.get("home_team").fillna("UNK").astype("category").cat.codes
    # Launch-angle buckets are useful features
    out["la_groundball"] = (out["launch_angle"] <= 10).astype(int)
    out["la_line"] = ((out["launch_angle"] > 10) & (out["launch_angle"] <= 25)).astype(int)
    out["la_fly"] = ((out["launch_angle"] > 25) & (out["launch_angle"] <= 50)).astype(int)
    out["la_popup"] = (out["launch_angle"] > 50).astype(int)
    return out.fillna(out.median(numeric_only=True))


def events_to_labels(df: pd.DataFrame) -> pd.Series:
    """Map Statcast 'events' column to our outcome classes."""
    return df["events"].map(EVENT_TO_OUTCOME).fillna("out")


class BattedBallModel:
    """Trained batted-ball outcome classifier with catch-prob blending."""
    def __init__(self, model_path: Optional[Path] = None):
        self.clf = None
        self.classes_ = OUTCOMES
        if model_path and Path(model_path).exists():
            self.load(model_path)

    def fit(self, df: pd.DataFrame, random_state: int = 42):
        """Train on a Statcast batted-ball dataframe. df must have 'events' column."""
        if HistGradientBoostingClassifier is None:
            raise ImportError("scikit-learn required for fit()")
        X = build_features(df)
        y = events_to_labels(df)
        X_tr, X_te, y_tr, y_te = train_test_split(X, y, test_size=0.15, random_state=random_state)
        self.clf = HistGradientBoostingClassifier(
            max_iter=400, max_depth=7, learning_rate=0.06, random_state=random_state,
        )
        self.clf.fit(X_tr, y_tr)
        self.classes_ = list(self.clf.classes_)
        return self.clf.score(X_te, y_te)

    def predict_proba(self, df: pd.DataFrame, catch_prob: Optional[pd.Series] = None) -> np.ndarray:
        """Return (n, 5) probability matrix. If catch_prob provided, blend into P(out).
        Blending rule: p_out_adj = 0.5 * p_out_model + 0.5 * catch_prob
        (remaining probability mass renormalized across hit classes in original ratios)
        """
        X = build_features(df)
        proba = self.clf.predict_proba(X)
        # Reorder columns to OUTCOMES order
        col_idx = {c: list(self.clf.classes_).index(c) for c in OUTCOMES if c in self.clf.classes_}
        p = np.zeros((len(df), 5))
        for i, oc in enumerate(OUTCOMES):
            if oc in col_idx:
                p[:, i] = proba[:, col_idx[oc]]
        if catch_prob is not None:
            cp = np.asarray(catch_prob).astype(float)
            mask = ~np.isnan(cp)
            # Blend P(out) with catch prob
            p_out_blend = np.where(mask, 0.5 * p[:, 0] + 0.5 * cp, p[:, 0])
            # Renormalize hit probs so they sum to 1 - p_out_blend
            hit_sum = p[:, 1:].sum(axis=1)
            scale = np.where(hit_sum > 0, (1 - p_out_blend) / hit_sum, 1.0)
            p[:, 0] = p_out_blend
            p[:, 1:] = p[:, 1:] * scale[:, None]
        return p

    def sample_outcomes(self, df: pd.DataFrame, n_sims: int = 10000,
                        catch_prob: Optional[pd.Series] = None, rng=None) -> np.ndarray:
        """Return an (n_sims, n_batted_balls) array of outcome indices.
        Each row is one simulated game's batted-ball outcome sequence.
        """
        if rng is None:
            rng = np.random.default_rng()
        p = self.predict_proba(df, catch_prob=catch_prob)
        n_bb = len(df)
        # Precompute cumulative probs for vectorized sampling
        cum = np.cumsum(p, axis=1)
        rand = rng.random((n_sims, n_bb))
        out = np.empty((n_sims, n_bb), dtype=np.int8)
        for j in range(n_bb):
            out[:, j] = np.searchsorted(cum[j], rand[:, j])
        return out

    def save(self, path: Path):
        with open(path, "wb") as f:
            pickle.dump({"clf": self.clf, "classes_": self.classes_}, f)

    def load(self, path: Path):
        with open(path, "rb") as f:
            blob = pickle.load(f)
        self.clf = blob["clf"]
        self.classes_ = blob["classes_"]
