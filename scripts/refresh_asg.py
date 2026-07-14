#!/usr/bin/env python3
"""
All-Star Game roster feed  ->  data/asg.json  (+ patches data/lineups.json)

The ASG is a real game in the MLB schedule (gameType "A", gamePk 823443 in 2026:
AL All-Stars @ NL All-Stars, Citizens Bank Park), so Bartolo's scoreboard already
draws a card for it. What it CANNOT get is the roster: StatsAPI publishes no
batting order for the ASG until first pitch, and there is no "team" whose depth
chart we can read. So the rosters are curated in data/asg_2026.json and this
script wires them into the normal render path:

  1) PATCHES data/lineups.json for the ASG gamePk with the projected starting 9
     per league. That makes the existing lineup table — and every projected stat
     that hangs off it (wRC+, xwOBA, K%, splits, grades) — light up for free,
     because those all join by player NAME, which we verified matches 41/41.

  2) EMITS data/asg.json carrying the pieces the normal path has nowhere to put:
     the combined BENCH (position reserves + additional bench) and the FULL
     12-man PITCHING STAFF per league, tagged Starter/Reliever.

ORDERING MATTERS: refresh_lineups.py rewrites lineups.json from scratch, so this
step must run AFTER it in refresh.yml or the injection is clobbered.
"""
from __future__ import annotations
import datetime as dt
import json
import os
import sys

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
ROSTERS = os.path.join(REPO_ROOT, "data", "asg_2026.json")
LINEUPS = os.path.join(REPO_ROOT, "data", "lineups.json")
OUTPUT = os.path.join(REPO_ROOT, "data", "asg.json")
SHEET_PROJ = os.path.join(REPO_ROOT, "data", "sheet_projections.json")
F5_PROJ = os.path.join(REPO_ROOT, "data", "f5_projections.json")

# Runs-per-point-of-win-probability. Calibrated on this season's slate: a +0.31
# run home margin corresponds to ~2.9% of win probability, so ~0.107 runs per
# point. Used ONLY to split a total into team runs, since the ASG projection is
# entered as (total, moneyline) and the front-end card wants a runs-per-side.
RUNS_PER_WP_PT = 0.107


def load(path):
    with open(path) as f:
        return json.load(f)


def _implied(ml):
    """American odds -> implied probability."""
    ml = float(ml)
    return (-ml) / ((-ml) + 100.0) if ml < 0 else 100.0 / (ml + 100.0)


def _fair_mirror(ml):
    """The opposite side of a FAIR (no-vig) price: -122 -> +122."""
    return int(round(-float(ml)))


def inject_projection(asg, pk):
    """Write the hand-entered ASG projection into sheet_projections.json and
    f5_projections.json, so the scoreboard card, the F5 toggle and the edge
    pills all light up exactly as they do for a normal game.

    The ASG is never in the GAME UPLOADER sheet, so those two feeds have no row
    for it and the card would read 'No projection yet'. We take (total, home ML)
    from data/asg_2026.json, mirror the away price as the fair opposite, and
    split the total into team runs using the implied win probability."""
    proj = asg.get("projection") or {}
    if not proj:
        print("[asg] no projection block in asg_2026.json; skipping")
        return

    lg = asg["leagues"]
    away_lg, home_lg = asg.get("away_league", "AL"), asg.get("home_league", "NL")
    away_name = f"{'American' if away_lg == 'AL' else 'National'} League All-Stars"
    home_name = f"{'American' if home_lg == 'AL' else 'National'} League All-Stars"

    for key, path, split_runs in (("full", SHEET_PROJ, True), ("f5", F5_PROJ, False)):
        p = proj.get(key)
        if not p or p.get("total") is None or p.get("ml_home") is None:
            continue
        ml_home = int(p["ml_home"])
        ml_away = _fair_mirror(ml_home)
        home_wp = _implied(ml_home)
        total = float(p["total"])

        entry = {
            "an_event_id": None,
            "away_team": away_name, "home_team": home_name,
            "total": round(total, 2),
            # NB: the sheet feed stores win prob as a FRACTION (0.562), not a
            # percent — the card multiplies by 100 itself. Writing 55.0 here is
            # what produced "5500.0% win".
            "away_wp": round(1.0 - home_wp, 4),
            "home_wp": round(home_wp, 4),
            "ml_away": ml_away, "ml_home": ml_home,
            "away_runs": None, "home_runs": None,
            "source": "static ASG projection (data/asg_2026.json)",
        }
        if split_runs:
            margin = (home_wp * 100 - 50.0) * RUNS_PER_WP_PT   # home run margin
            entry["home_runs"] = round((total + margin) / 2.0, 2)
            entry["away_runs"] = round((total - margin) / 2.0, 2)

        try:
            doc = load(path)
        except FileNotFoundError:
            print(f"[asg] {os.path.basename(path)} missing; skipping {key} projection")
            continue
        games = doc.setdefault("games", {})
        games[str(pk)] = entry
        doc["n_games"] = len(games)
        with open(path, "w") as f:
            json.dump(doc, f, indent=2)
        runs = (f" ({entry['away_runs']}-{entry['home_runs']})"
                if entry["away_runs"] is not None else "")
        print(f"[asg] {key}: total {entry['total']}, {home_lg} {ml_home:+d} "
              f"(wp {entry['home_wp']*100:.1f}%){runs} -> {os.path.basename(path)}")


def main():
    try:
        asg = load(ROSTERS)
    except FileNotFoundError:
        print(f"[asg] no {ROSTERS}; nothing to do (not an error out of season)")
        return 0

    pk = asg["game_pk"]
    away_lg = asg.get("away_league", "AL")
    home_lg = asg.get("home_league", "NL")
    lg = asg["leagues"]

    # ---- 1) inject the projected starting 9 into lineups.json -----------------
    def side(code):
        players = []
        for p in sorted(lg[code]["starters"], key=lambda x: x["order"]):
            players.append({
                "order": p["order"],
                "name": p["name"],
                "pos": p.get("pos", ""),
                "bats": "",
                "status": "projected",
                "flag": None,
                "team": p.get("team", ""),     # club the All-Star comes from
                "mlbamid": p.get("mlbamid"),
            })
        return {
            "status": "projected",
            "source": f"{code} All-Star roster (projected order)",
            "players": players,
        }

    patched = False
    try:
        lu = load(LINEUPS)
        for g in lu.get("games", []):
            if g.get("game_pk") != pk:
                continue
            g["lineups"] = {"away": side(away_lg), "home": side(home_lg)}
            patched = True
            break
        if patched:
            with open(LINEUPS, "w") as f:
                json.dump(lu, f, indent=2)
            print(f"[asg] injected {away_lg}/{home_lg} starting 9s into lineups.json "
                  f"(game_pk {pk})")
        else:
            # Not on the slate right now (lineups.json only carries today+tomorrow).
            print(f"[asg] game_pk {pk} not in lineups.json today; skipping injection")
    except FileNotFoundError:
        print("[asg] lineups.json missing; skipping injection")

    # ---- 2) emit the bench + full pitching staff ------------------------------
    def staff(code):
        # one combined list, starters first, then relievers — each role-tagged
        rank = {"Starter": 0, "Reliever": 1}
        return sorted(lg[code]["pitchers"],
                      key=lambda p: (rank.get(p.get("role"), 9), p["name"]))

    payload = {
        "generated_at": dt.datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "game_pk": pk,
        "date": asg["date"],
        "matchup": asg["matchup"],
        "venue": asg["venue"],
        "game_time": asg["game_time"],
        "away_league": away_lg,
        "home_league": home_lg,
        "source": asg.get("source", ""),
        "grades": (asg.get("grades") or {}).get("wrc") or {},
        "leagues": {
            code: {
                "starters": sorted(lg[code]["starters"], key=lambda x: x["order"]),
                "bench": lg[code]["bench"],
                "pitchers": staff(code),
                "notes": lg[code].get("notes", []),
            }
            for code in (away_lg, home_lg)
        },
    }
    os.makedirs(os.path.dirname(OUTPUT), exist_ok=True)
    with open(OUTPUT, "w") as f:
        json.dump(payload, f, indent=2)

    inject_projection(asg, pk)

    for code in (away_lg, home_lg):
        d = payload["leagues"][code]
        sp = sum(1 for p in d["pitchers"] if p.get("role") == "Starter")
        rp = sum(1 for p in d["pitchers"] if p.get("role") == "Reliever")
        print(f"[asg] {code}: {len(d['starters'])} starters · {len(d['bench'])} bench · "
              f"{len(d['pitchers'])} pitchers ({sp} SP / {rp} RP)")
    print(f"[asg] wrote {OUTPUT}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
