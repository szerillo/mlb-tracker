#!/usr/bin/env python3
"""capture_midnight_snapshot.py - freeze our number + the market line at ~midnight ET.

grade_model.py wants an edge measured at "midnight ET before the game" (the
overnight number, before lineups shift the market). sheet_projections.json and
odds.json both get overwritten as the day progresses, so we freeze a write-once
copy at the first refresh run inside the midnight-ET window into
data/archive/{slate-date}/midnight_lines.json.

Runs every refresh; only writes when (a) ET hour is in the midnight window
[0, 5] AND (b) the file doesn't already exist for that slate. So the first
overnight run of the slate (the 00:00 ET / 04:00 UTC cron) captures it and
nothing later clobbers it. --force / --date override for manual snapshots.

Per game: our total + win% (sheet_projections) and the market ML + total line
(odds.json), joined on game_pk.
"""
from __future__ import annotations
import argparse, datetime, json, sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
DATA = REPO / "data"
ARCHIVE = DATA / "archive"
MIDNIGHT_WINDOW = range(0, 6)   # ET hours 00:00-05:59


def _et_now():
    return datetime.datetime.utcnow() - datetime.timedelta(hours=4)


def _load(path, default=None):
    p = Path(path)
    if not p.exists():
        return default
    try:
        return json.loads(p.read_text())
    except Exception:
        return default


def _mkt_lines(g):
    ml = g.get("moneyline") or {}
    tot = g.get("total") or {}
    over = tot.get("over") or {}
    under = tot.get("under") or {}
    line = over.get("line")
    if line is None:
        line = under.get("line")
    return {
        "ml_away": (ml.get("away") or {}).get("odds"),
        "ml_home": (ml.get("home") or {}).get("odds"),
        "total_line": line,
        "over": over.get("odds"),
        "under": under.get("odds"),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--force", action="store_true", help="ignore the midnight-window / write-once guards")
    ap.add_argument("--date", default=None, help="stamp this slate date instead of the sheet's own")
    args = ap.parse_args()

    et = _et_now()
    if not args.force and et.hour not in MIDNIGHT_WINDOW:
        print(f"[midnight] ET {et:%H:%M} outside midnight window; skipping", file=sys.stderr)
        return 0

    sp = _load(DATA / "sheet_projections.json") or {}
    games_sp = sp.get("games") or {}
    if not games_sp:
        print("[midnight] no sheet_projections games; skipping", file=sys.stderr)
        return 0
    date = args.date or sp.get("date") or et.date().isoformat()

    out_dir = ARCHIVE / date
    out_path = out_dir / "midnight_lines.json"
    if out_path.exists() and not args.force:
        print(f"[midnight] {out_path} already frozen; leaving intact", file=sys.stderr)
        return 0

    odds = _load(DATA / "odds.json") or {}
    mkt_by_pk = {str(g.get("game_pk")): g for g in (odds.get("games") or [])}

    games = {}
    for pk, s in games_sp.items():
        rec = {"an_event_id": s.get("an_event_id"),
               "our_total": s.get("total"),
               "our_away_wp": s.get("away_wp"),
               "our_home_wp": s.get("home_wp")}
        mg = mkt_by_pk.get(str(pk))
        if mg:
            rec.update(_mkt_lines(mg))
        games[str(pk)] = rec

    have_line = sum(1 for r in games.values() if r.get("total_line") is not None)
    out_dir.mkdir(parents=True, exist_ok=True)
    payload = {"date": date, "captured_at": datetime.datetime.utcnow().isoformat(timespec="seconds") + "Z",
               "et_hour": et.hour, "n_games": len(games), "n_with_line": have_line, "games": games}
    out_path.write_text(json.dumps(payload, separators=(",", ":")))
    print(f"[midnight] {date}: froze {len(games)} games ({have_line} with a line) → {out_path}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
