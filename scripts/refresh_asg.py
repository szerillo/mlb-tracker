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


def load(path):
    with open(path) as f:
        return json.load(f)


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
