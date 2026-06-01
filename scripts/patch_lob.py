#!/usr/bin/env python3
"""One-shot patch: refresh team LOB across every archived BARTOLO game.

Walks data/archive/{date}/bartolo_wp.json, fetches the MLB live feed for each
game, reads the *correct* LOB straight off linescore.teams.{side}.leftOnBase
(the broadcast value), and rewrites the game's game_stats.away.lob /
game_stats.home.lob in place. Cheap — no Statcast, no sim re-roll, just an
HTTP fetch per game.

Why this exists: the BAL-TOR 2026-05-30 game showed LOB 17 / 14 (the inflated
sum-of-player-LOB from boxscore.teamStats.batting.leftOnBase). The real number
is 8 / 9. game_stats.py now reads from linescore directly, but the per-day
archives still hold the old inflated values until they're patched. A full
bartolo_backfill rebuild would take hours; this script does the same fix in
~3 min by re-pulling only the linescore and writing back the lob field.

Idempotent: skips games whose lob values would not change.
"""
import datetime
import json
import os
import sys
import time
import urllib.request
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
ARCHIVE_DIR = REPO_ROOT / "data" / "archive"

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
MLB_LIVE = "https://statsapi.mlb.com/api/v1.1/game/{pk}/feed/live"


def _http_json(url, timeout=20, retries=3):
    last = None
    for i in range(retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA,
                                                       "Accept": "application/json"})
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return json.load(r)
        except Exception as e:
            last = e
            time.sleep(1 + i)
    print(f"  http err {url}: {last}", file=sys.stderr)
    return None


def _read_lob_from_linescore(live):
    """Mirror of game_stats.extract_lob — read the broadcast LOB directly."""
    out = {"away": None, "home": None}
    try:
        ld = live["liveData"]
    except Exception:
        return out

    ls_teams = ((ld.get("linescore") or {}).get("teams")) or {}
    for side in ("away", "home"):
        v = (ls_teams.get(side) or {}).get("leftOnBase")
        if isinstance(v, int):
            out[side] = v

    if out["away"] is None or out["home"] is None:
        innings = (ld.get("linescore") or {}).get("innings") or []
        sums = {"away": 0, "home": 0}
        seen = {"away": False, "home": False}
        for inn in innings:
            for side in ("away", "home"):
                v = (inn.get(side) or {}).get("leftOnBase")
                if isinstance(v, int):
                    sums[side] += v
                    seen[side] = True
        for side in ("away", "home"):
            if out[side] is None and seen[side]:
                out[side] = sums[side]

    return out


def _patch_game(g_entry, fresh_lob):
    """Set game_stats.away.lob / home.lob from the linescore. Returns True if
    the entry actually changed."""
    gs = g_entry.get("game_stats")
    if not isinstance(gs, dict):
        return False
    changed = False
    for side in ("away", "home"):
        v = fresh_lob.get(side)
        if v is None:
            continue
        side_block = gs.get(side)
        if not isinstance(side_block, dict):
            continue
        if side_block.get("lob") != v:
            side_block["lob"] = v
            changed = True
    return changed


def main():
    if not ARCHIVE_DIR.exists():
        print(f"[patch_lob] no archive dir at {ARCHIVE_DIR}", file=sys.stderr)
        return 1

    # Optional date range
    start = os.environ.get("PATCH_START")  # YYYY-MM-DD
    end   = os.environ.get("PATCH_END")    # YYYY-MM-DD
    if start: start = datetime.date.fromisoformat(start)
    if end:   end   = datetime.date.fromisoformat(end)

    targets = []
    for d in sorted(ARCHIVE_DIR.iterdir()):
        try:
            di = datetime.date.fromisoformat(d.name)
        except ValueError:
            continue
        if start and di < start: continue
        if end   and di > end:   continue
        bw = d / "bartolo_wp.json"
        if bw.exists():
            targets.append((di, bw))

    if not targets:
        print(f"[patch_lob] no archives match range start={start} end={end}",
              file=sys.stderr)
        return 0

    print(f"[patch_lob] {len(targets)} archive dates to scan")
    n_games_seen = n_games_changed = n_files_written = 0

    for di, bw in targets:
        try:
            doc = json.loads(bw.read_text())
        except Exception as e:
            print(f"  {di}: read failed: {e}", file=sys.stderr)
            continue

        games = doc.get("games") or {}
        if not isinstance(games, dict) or not games:
            continue

        file_dirty = False
        for pk_str, g_entry in games.items():
            if not isinstance(g_entry, dict):
                continue
            try:
                pk = int(pk_str)
            except (TypeError, ValueError):
                continue
            n_games_seen += 1
            live = _http_json(MLB_LIVE.format(pk=pk))
            if not live:
                continue
            fresh_lob = _read_lob_from_linescore(live)
            if _patch_game(g_entry, fresh_lob):
                n_games_changed += 1
                file_dirty = True
            time.sleep(0.2)  # pace MLB API

        if file_dirty:
            bw.write_text(json.dumps(doc, separators=(",", ":")))
            n_files_written += 1
            print(f"  {di}: patched, {len(games)} games scanned")

    print(f"[patch_lob] done: {n_games_seen} games scanned, "
          f"{n_games_changed} changed, {n_files_written} files written")
    return 0


if __name__ == "__main__":
    sys.exit(main())
