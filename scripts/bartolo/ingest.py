"""
B.A.R.T.O.L.O. | Data ingest helpers.

Thin layer over:
  - MLB StatsAPI (https://statsapi.mlb.com) â schedule, play-by-play, HP ump
  - Ump Scorecards data â loaded from data/ump_scorecards/YYYY-MM-DD.csv
    (the Phase 3 scraper writes files in that shape; absent file = no favor data)

The Savant / pybaseball Statcast pull lives in bartolo_daily.py (we do ONE
day-level pull and filter per-game locally, which is much faster than one
per-game call).
"""
from __future__ import annotations
import os
import urllib.request
import json
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Optional

import pandas as pd

# data/ lives at repo root (scripts/bartolo/ingest.py â ../../data)
REPO_ROOT = Path(__file__).resolve().parent.parent.parent
DATA_ROOT = Path(os.environ.get("BARTOLO_DATA", REPO_ROOT / "data"))
UMP_DIR = DATA_ROOT / "ump_scorecards"

MLB_API = "https://statsapi.mlb.com/api/v1"


# -------------------------------------------------------------------
# Game metadata
# -------------------------------------------------------------------
@dataclass
class Game:
    """Minimal game identifier. game_pk is the MLB StatsAPI primary key."""
    game_pk: int
    game_date: date
    away_team: str
    home_team: str
    away_runs: int = 0
    home_runs: int = 0
    venue: str = ""
    umpire_name: str = ""

    @property
    def key(self) -> str:
        return f"{self.game_date.isoformat()}_{self.away_team}@{self.home_team}_{self.game_pk}"

    @property
    def display(self) -> str:
        return f"{self.away_team} @ {self.home_team} ({self.game_date.isoformat()})"


def _http_json(url: str, timeout: int = 20) -> Optional[dict]:
    """GET JSON via stdlib (no requests dep needed)."""
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "bartolo/1.0"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read())
    except Exception as e:
        print(f"  http err {url}: {e}")
        return None


# -------------------------------------------------------------------
# MLB StatsAPI fetchers
# -------------------------------------------------------------------
def fetch_schedule(d: date) -> list[Game]:
    """Return all Final / Game Over MLB games for a given calendar date."""
    data = _http_json(f"{MLB_API}/schedule?sportId=1&date={d.isoformat()}")
    if not data:
        return []
    games = []
    for game_block in data.get("dates", []):
        for g in game_block.get("games", []):
            status = g.get("status", {}).get("detailedState", "")
            if status not in ("Final", "Game Over", "Completed Early"):
                continue
            try:
                games.append(Game(
                    game_pk=g["gamePk"],
                    game_date=d,
                    # Use full team names so downstream keys match fatigue.json
                    # and the frontend's team-name lookup.
                    away_team=g["teams"]["away"]["team"]["name"],
                    home_team=g["teams"]["home"]["team"]["name"],
                    away_runs=g["teams"]["away"].get("score", 0),
                    home_runs=g["teams"]["home"].get("score", 0),
                    venue=g.get("venue", {}).get("name", ""),
                ))
            except Exception:
                continue
    return games


def fetch_game_pbp(game_pk: int) -> Optional[dict]:
    """Full play-by-play + lineup + umpire metadata for a game."""
    return _http_json(f"{MLB_API.replace('/v1', '/v1.1')}/game/{game_pk}/feed/live")


def extract_umpire(pbp: dict) -> str:
    """Given a full pbp feed, find the HP umpire's full name."""
    try:
        officials = pbp["liveData"]["boxscore"]["officials"]
        for o in officials:
            if o.get("officialType") == "Home Plate":
                return o["official"]["fullName"]
    except Exception:
        pass
    return ""


# -------------------------------------------------------------------
# Ump Scorecards integration (reads files written by the Phase 3 scraper)
# -------------------------------------------------------------------
def load_ump_scorecards(d: Optional[date] = None) -> pd.DataFrame:
    """Load Ump Scorecards data. Phase 3 scraper writes CSVs to
    data/ump_scorecards/YYYY-MM-DD.csv with columns:
      date, home_team, away_team, umpire,
      home_favor_runs, away_favor_runs
    """
    if d:
        path = UMP_DIR / f"{d.isoformat()}.csv"
        if not path.exists():
            return pd.DataFrame()
        return pd.read_csv(path)
    if not UMP_DIR.exists():
        return pd.DataFrame()
    paths = sorted(UMP_DIR.glob("*.csv"))
    if not paths:
        return pd.DataFrame()
    return pd.concat([pd.read_csv(p) for p in paths], ignore_index=True)


def ump_favor_for_game(game: Game) -> tuple[float, float]:
    """Return (away_favor_runs, home_favor_runs) from ump scorecards.
    Positive = runs the ump's zone ADDED to that team. Defaults to (0, 0)
    when no scorecard is available (Phase 3 wires the scraper).
    """
    df = load_ump_scorecards(game.game_date)
    if df.empty:
        return (0.0, 0.0)
    mask = ((df.get("away_team", "") == game.away_team) &
            (df.get("home_team", "") == game.home_team))
    hit = df[mask]
    if hit.empty:
        return (0.0, 0.0)
    row = hit.iloc[0]
    return (float(row.get("away_favor_runs", 0.0)),
            float(row.get("home_favor_runs", 0.0)))
