"""
B.A.R.T.O.L.O. | Archive aggregation.

Merges every per-date snapshot under data/archive/<YYYY-MM-DD>/bartolo_wp.json
into the flat data/bartolo_wp.json that the Win Prob tab reads. Idempotent and
pure-stdlib — safe to call at the end of both the daily runner and the backfill.

Keeping the flat file as a pure projection of the archive means the daily job
self-heals: a day that failed to commit simply reappears once its archive lands.
"""
from __future__ import annotations
import datetime
import json
import os
from pathlib import Path


def write_date_archive(repo_root: Path, date_iso: str, games: dict, status: str = "ok") -> Path:
    """Write one day's games to data/archive/<date>/bartolo_wp.json."""
    date_dir = repo_root / "data" / "archive" / date_iso
    date_dir.mkdir(parents=True, exist_ok=True)
    out = date_dir / "bartolo_wp.json"
    out.write_text(json.dumps({
        "generated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds"),
        "window_date": date_iso,
        "status": status,
        "n_games": len(games),
        "games": games,
    }, indent=2, default=str))
    return out


def aggregate_archives(repo_root: Path) -> dict:
    """Rebuild data/bartolo_wp.json from every per-date archive. Returns the payload."""
    archive_dir = repo_root / "data" / "archive"
    flat = repo_root / "data" / "bartolo_wp.json"
    merged: dict = {}
    latest_date = None
    if archive_dir.is_dir():
        for date_dir in sorted(archive_dir.iterdir()):
            f = date_dir / "bartolo_wp.json"
            if not f.is_file():
                continue
            try:
                doc = json.loads(f.read_text())
            except Exception:
                continue
            for pk, g in (doc.get("games") or {}).items():
                g.setdefault("game_date", doc.get("window_date"))
                merged[str(pk)] = g
            if doc.get("games"):
                latest_date = doc.get("window_date") or latest_date
    payload = {
        "generated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds"),
        "window_date": latest_date,
        "status": "ok",
        "n_games": len(merged),
        "games": merged,
    }
    flat.parent.mkdir(parents=True, exist_ok=True)
    flat.write_text(json.dumps(payload, indent=2, default=str))
    return payload
