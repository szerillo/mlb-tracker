#!/usr/bin/env python3
"""
Import Sean's own first-5-innings (F5) projections from the model sheet's
F5 UPLOADER tab into the site as data/f5_projections.json.

Identical format + join logic to refresh_sheet_projections.py (Action Network
expert upload columns; game_id == AN event id -> teams -> MLB gamePk, DH-safe),
but reads a DIFFERENT tab (the F5 uploader) via SHEET_F5_CSV_URL and writes a
separate output. Values here are the F5 numbers: away_score/home_score = F5 runs,
total = F5 total, ml_away/ml_home = F5 moneyline, *_win_p = F5 win prob.

USAGE:
    SHEET_F5_CSV_URL="https://docs.google.com/.../pub?gid=<F5_TAB_GID>&single=true&output=csv" \
        python scripts/refresh_f5_projections.py
"""
from __future__ import annotations
import json, os, sys, datetime

sys.path.insert(0, os.path.dirname(__file__))
# reuse the exact parsing + DH-safe id resolution from the full-game emitter
from refresh_sheet_projections import (
    parse_sheet_csv, build_id_maps, _nick, _parse_dt, _et_today, _http_get_text,
    pick_slate_date, keep_previous, sheet_csv_url,
)

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
OUTPUT = os.path.join(REPO_ROOT, "data", "f5_projections.json")
SHEET_F5_CSV_URL = os.environ.get("SHEET_F5_CSV_URL", "").strip()


def main():
    url = sheet_csv_url(SHEET_F5_CSV_URL, "F5 UPLOADER")
    try:
        text = _http_get_text(url)
    except Exception as e:
        print(f"ERR: could not fetch F5 sheet CSV: {e}", file=sys.stderr)
        return 1

    # F5 tab has total/win%/ML but no team-run split, so require 'total' (not away_score).
    all_rows = parse_sheet_csv(text, None, require_col="total")
    iso = pick_slate_date(all_rows)
    rows = [r for r in all_rows if (r.get("date") or iso) == iso]
    print(f"[f5_projections] slate date {iso} ({len(rows)} F5 projection rows)")
    an_teams, pk_map = build_id_maps(iso)

    used_pks = set()

    def _resolve_pk(a, h, an_start):
        cand = [c for c in (pk_map.get((_nick(a), _nick(h))) or []) if c["pk"] not in used_pks]
        if not cand:
            return None
        if len(cand) == 1:
            gpk = cand[0]["pk"]
        else:
            ant = _parse_dt(an_start)
            gpk = (cand[0]["pk"] if ant is None else
                   min(cand, key=lambda c: abs((c["dt"] - ant).total_seconds()) if c["dt"] else 9e18)["pk"])
        used_pks.add(gpk)
        return gpk

    games = {}
    n_join = n_miss = 0
    for row in sorted(rows, key=lambda r: str((an_teams.get(r["game_id"]) or (None, None, ""))[2] or "")):
        nm = an_teams.get(row["game_id"])
        if not nm or not nm[0] or not nm[1]:
            n_miss += 1; continue
        a, h, an_start = nm
        gpk = _resolve_pk(a, h, an_start)
        if gpk is None:
            n_miss += 1; continue
        games[str(gpk)] = {
            "an_event_id": row["game_id"],
            "away_team": a, "home_team": h,
            "away_runs": row["away_runs"], "home_runs": row["home_runs"],
            "total": row["total"],
            "away_wp": row["away_wp"], "home_wp": row["home_wp"],
            "ml_away": row["ml_away"], "ml_home": row["ml_home"],
        }
        n_join += 1

    # Don't let an empty run (tab still being filled) wipe a good live feed —
    # keep the previous non-empty snapshot instead.
    n_sched = sum(len(v) for v in pk_map.values())
    if keep_previous(OUTPUT, iso, n_join, n_sched):
        print(f"[f5_projections] only {n_join}/{n_sched} games joined for {iso}; "
              f"keeping previous fuller feed (won't clobber)")
        return 0

    payload = {
        "generated_at": datetime.datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "date": iso,
        "source": "Google Sheet F5 UPLOADER tab (Action Network expert upload format, first 5 innings)",
        "n_games": len(games),
        "games": games,
    }
    os.makedirs(os.path.dirname(OUTPUT), exist_ok=True)
    with open(OUTPUT, "w") as f:
        json.dump(payload, f, indent=2)
    print(f"[f5_projections] {n_join} games joined, {n_miss} unmatched -> data/f5_projections.json")
    return 0


if __name__ == "__main__":
    sys.exit(main())
