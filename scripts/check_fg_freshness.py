#!/usr/bin/env python3
"""Fail the workflow if the browser-pulled FanGraphs snapshots are stale.

The five data/_fg_*.json snapshots are committed by a Claude-in-Chrome scheduled
task (refresh-fangraphs-fwar) because FanGraphs 403-blocks CI runners. When that
task silently fails, the futures workflows keep rebuilding from yesterday's
numbers and the site looks frozen with no error anywhere. This turns that silent
staleness into a red X plus the usual GitHub failure email.
"""
import datetime as dt
import json
import os
import sys

MAX_AGE_HOURS = float(os.environ.get("FG_MAX_AGE_HOURS", "20"))
SNAPSHOTS = [
    "data/_fg_ytd.json",
    "data/_fg_pitch_model.json",
    "data/_fg_ros.json",
    "data/_fg_roll.json",
    "data/_fg_playoff_odds.json",
]

now = dt.datetime.now(dt.timezone.utc)
stale = []
broken = []

for path in SNAPSHOTS:
    try:
        with open(path) as fh:
            gen = json.load(fh)["generated_at"]
        ts = dt.datetime.fromisoformat(gen.replace("Z", "+00:00"))
    except (OSError, ValueError, KeyError) as exc:
        print("BROKEN {}: {}".format(path, exc))
        broken.append("{} ({})".format(path, exc))
        continue
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=dt.timezone.utc)
    age = (now - ts).total_seconds() / 3600.0
    label = "STALE" if age > MAX_AGE_HOURS else "ok"
    print("{:6s} {:30s} {}  ({:.1f}h old)".format(label, path, gen, age))
    if age > MAX_AGE_HOURS:
        stale.append("{} {:.1f}h".format(path, age))

if broken:
    print("::error title=FanGraphs snapshot unreadable::" + "; ".join(broken))

if stale:
    print(
        "::error title=Stale FanGraphs snapshots::"
        "{} of {} snapshots are older than {:.0f}h ({}). The daily Chrome pull "
        "(refresh-fangraphs-fwar) did not complete, so this run rebuilt from "
        "stale FanGraphs data.".format(
            len(stale), len(SNAPSHOTS), MAX_AGE_HOURS, "; ".join(stale)
        )
    )

sys.exit(1 if (stale or broken) else 0)
