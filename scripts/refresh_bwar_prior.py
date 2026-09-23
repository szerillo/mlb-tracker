#!/usr/bin/env python3
"""
refresh_bwar_prior.py — prior-season (2024+2025) hitter WAR-rate feed.

Fable ruling 2026-09-23: compute_sean_team_projections shrinks each hitter's
2026 WAR-rate toward the player's OWN two-prior-season rate (league-mean fallback)
with k=400 PA. This builds that prior target from the SAME Baseball-Reference
war_daily_bat.txt that refresh_bwar.py already uses — observed data only, no
projection dependence.

Static by nature (2024/2025 don't change) — run once, commit the JSON. Re-run
only to refresh the roster of names or extend the window.

Output: data/bwar_prior_2425.json
  { generated_at, source, seasons:[2024,2025], league_mean_rate620,
    players: { "<mlbam_id>": {war, pa, rate620, name} } }   # rate620 = WAR per 620 PA

Run: python scripts/refresh_bwar_prior.py
"""
import csv, io, json, datetime as dt
from pathlib import Path

BASE = "https://www.baseball-reference.com/data/"
BAT_FILE = "war_daily_bat.txt"
UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/124.0 Safari/537.36")
SEASONS = {"2024", "2025"}
QUALIFY_PA = 200            # min prior PA to enter the league-mean anchor
OUT = Path(__file__).resolve().parent.parent / "data" / "bwar_prior_2425.json"


def _fetch(fname, timeout=120):
    import urllib.request
    req = urllib.request.Request(BASE + fname, headers={
        "User-Agent": UA, "Accept": "text/plain,*/*",
        "Referer": "https://www.baseball-reference.com/about/war_explained.shtml",
    })
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read().decode("utf-8", "replace")


def _f(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return 0.0


def _i(x):
    try:
        return int(float(x))
    except (TypeError, ValueError):
        return 0


def main():
    text = _fetch(BAT_FILE)
    acc = {}
    for row in csv.DictReader(io.StringIO(text)):
        if str(row.get("year_ID")) not in SEASONS:
            continue
        mid = (row.get("mlb_ID") or "").strip()
        if not mid or mid == "NULL":
            continue
        a = acc.setdefault(mid, {"name": (row.get("name_common") or "").strip(),
                                 "war": 0.0, "pa": 0})
        a["war"] += _f(row.get("WAR"))
        a["pa"] += _i(row.get("PA"))

    rates, anchor = {}, []
    for mid, a in acc.items():
        if a["pa"] < QUALIFY_PA:       # only players with a real prior sample
            continue
        r620 = a["war"] / a["pa"] * 620
        rates[mid] = round(r620, 3)
        anchor.append((r620, a["pa"]))

    tot = sum(p for _, p in anchor)
    lm = sum(r * p for r, p in anchor) / tot if tot else 2.2

    out = {"generated_at": dt.datetime.utcnow().isoformat() + "Z",
           "source": "Baseball-Reference war_daily_bat 2024+2025",
           "seasons": [2024, 2025],
           "league_mean_rate620": round(lm, 3),
           "n_players": len(rates),
           "rates": rates}          # {mlbam_id: WAR per 620 PA}, >=QUALIFY_PA prior PA only
    OUT.write_text(json.dumps(out, separators=(",", ":")))
    print(f"[bwar-prior] wrote {OUT} ({len(rates)} hitters, league_mean_rate620={lm:.3f})")


if __name__ == "__main__":
    main()
