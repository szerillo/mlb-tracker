#!/usr/bin/env python3
"""
Apply manual lineup overrides (data/lineup_overrides.json) onto data/lineups.json.

Runs AFTER the Rotowire/MLB scrape + platoon step so a hand-entered lineup (e.g.
copied from the model) wins over the scraped projection — fixing games where the
scraped projected order has wrong positions / no DH.

Safety:
  - A side is overridden ONLY while it's still "projected". Once MLB posts the
    official confirmed lineup (status == "confirmed"), the real lineup wins and
    the override is skipped for that side.
  - Stale overrides auto-ignore: if the game_pk has aged out of lineups.json's
    lookahead window, there's nothing to override, so it's a no-op.
  - Fully defensive: any error leaves lineups.json untouched (and the workflow
    step is continue-on-error anyway).

Override file shape (data/lineup_overrides.json):
  { "games": { "<game_pk>": {
        "away": {"players": [{"order":1,"name":"...","pos":"2B","bats":"R","person_id":123}, ...]},
        "home": {"players": [ ... ]} } } }
"""
import json, os, sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
LINEUPS = os.path.join(ROOT, "data", "lineups.json")
OVERRIDES = os.path.join(ROOT, "data", "lineup_overrides.json")


def main():
    try:
        ov = json.load(open(OVERRIDES))
    except FileNotFoundError:
        print("[overrides] no data/lineup_overrides.json — nothing to do")
        return 0
    except Exception as e:
        print(f"[overrides] could not read overrides ({e}) — skipping", file=sys.stderr)
        return 0

    games = ov.get("games", {}) if isinstance(ov, dict) else {}
    if not games:
        print("[overrides] no game overrides defined")
        return 0

    try:
        data = json.load(open(LINEUPS))
    except Exception as e:
        print(f"[overrides] could not read lineups.json ({e}) — skipping", file=sys.stderr)
        return 0

    by_pk = {str(g.get("game_pk")): g for g in data.get("games", [])}
    applied = 0
    for pk, spec in games.items():
        g = by_pk.get(str(pk))
        if not g:
            print(f"[overrides] game_pk {pk} not in current slate — skip")
            continue
        for side in ("away", "home"):
            side_spec = spec.get(side) or {}
            players_in = side_spec.get("players") or []
            if not players_in:
                continue
            cur = (g.get("lineups", {}) or {}).get(side) or {}
            if cur.get("status") == "confirmed":
                print(f"[overrides] {pk} {side}: already CONFIRMED by MLB — keeping real lineup")
                continue
            players = [{
                "order": p.get("order"),
                "person_id": p.get("person_id"),
                "name": p.get("name"),
                "pos": p.get("pos", ""),
                "bats": p.get("bats", ""),
                "status": "projected",
            } for p in players_in]
            g.setdefault("lineups", {})[side] = {
                "status": "projected",
                "players": players,
                "source": "sheet-override",
            }
            applied += 1
            print(f"[overrides] {pk} {side}: applied {len(players)} players ({spec.get('matchup','')})")

    if applied:
        with open(LINEUPS, "w") as f:
            json.dump(data, f, indent=2)
        print(f"[overrides] wrote {applied} side(s) -> data/lineups.json")
    else:
        print("[overrides] nothing applied")
    return 0


if __name__ == "__main__":
    sys.exit(main())
