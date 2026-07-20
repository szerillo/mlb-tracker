#!/usr/bin/env python3
"""
Build data/hitter_percentiles.json — percentiles computed OURSELVES from Savant
min=1 leaderboards, so every hitter (not just the qualified board) gets a value.

Each stat is pulled as a RAW value at min=1 from the relevant Savant leaderboard,
merged by player_id, then percentile-ranked across the full pulled population
(higher = better; K%/Whiff/Chase inverted). A `qualified` flag marks players below
the qualified-PA bar so the UI can asterisk their (small-sample) percentiles.

Output per hitter: name, mlbam_id, pa, qualified, + 0-100 percentiles:
  xwoba xba xslg barrel hard_hit exit_velocity k_pct bb_pct whiff chase
  sprint oaa arm_strength  (+ raw rates: barrel_pct, hard_hit_pct, exit_velocity_avg)

USAGE: python scripts/refresh_hitter_percentiles.py > data/hitter_percentiles.json
"""
from __future__ import annotations
import csv, datetime, io, json, os, sys, unicodedata, urllib.request

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/121.0 Safari/537.36")
YEAR = int(os.environ.get("SAVANT_YEAR", datetime.date.today().year))

def norm_name(s):
    if not s: return ""
    s = unicodedata.normalize("NFKD", s); s = "".join(c for c in s if not unicodedata.combining(c)); s = s.lower()
    for suf in [" jr.", " jr", " sr.", " sr", " iii", " ii", " iv"]:
        if s.endswith(suf): s = s[:-len(suf)]
    return s.replace(".", "").strip()

def _f(v):
    if v in (None, ""): return None
    try: return float(v)
    except ValueError: return None
def _i(v):
    if v in (None, ""): return None
    try: return int(float(v))
    except ValueError: return None

def fetch_csv(url):
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=60) as r:
        return list(csv.DictReader(io.StringIO(r.read().decode("utf-8","replace").lstrip("﻿"))))

def std_name(row):
    raw = row.get("last_name, first_name") or row.get("player_name") or ""
    if "," in raw:
        last, first = [p.strip() for p in raw.split(",", 1)]; return f"{first} {last}"
    return raw.strip()

Y = YEAR
# (url, {savant_col: our_raw_key})  — every board pulled at min=1
BOARDS = [
    (f"https://baseballsavant.mlb.com/leaderboard/expected_statistics?type=batter&year={Y}&position=&team=&filterType=bip&min=1&csv=true",
     {"pa":"pa","est_woba":"xwoba","est_ba":"xba","est_slg":"xslg"}),
    (f"https://baseballsavant.mlb.com/leaderboard/statcast?type=batter&year={Y}&min=1&csv=true",
     {"brl_percent":"barrel","ev95percent":"hard_hit"}),
    (f"https://baseballsavant.mlb.com/leaderboard/custom?year={Y}&type=batter&filter=&min=1&selections=pa,k_percent,bb_percent,whiff_percent,exit_velocity_avg,xiso,avg_swing_speed,squared_up_swing&chart=false&x=pa&y=pa&r=no&chartType=beeswarm&csv=true",
     {"pa":"pa","k_percent":"k_pct","bb_percent":"bb_pct","whiff_percent":"whiff","exit_velocity_avg":"exit_velocity","xiso":"xiso","avg_swing_speed":"bat_speed","squared_up_swing":"squared_up"}),
    (f"https://baseballsavant.mlb.com/leaderboard/custom?year={Y}&type=batter&filter=&min=1&selections=pa,oz_swing_percent&chart=false&x=pa&y=pa&r=no&chartType=beeswarm&csv=true",
     {"oz_swing_percent":"chase"}),
    (f"https://baseballsavant.mlb.com/leaderboard/arm-strength?type=player&year={Y}&min=1&csv=true",
     {"arm_overall":"arm_strength"}),
    (f"https://baseballsavant.mlb.com/leaderboard/sprint_speed?type=batter&year={Y}&min=0&csv=true",
     {"sprint_speed":"sprint"}),
    (f"https://baseballsavant.mlb.com/leaderboard/outs_above_average?type=Fielder&year={Y}&min=1&csv=true",
     {"outs_above_average":"oaa"}),
]
# stats where LOWER raw is better (invert the percentile)
INVERT = {"k_pct", "whiff", "chase"}

def main():
    players = {}   # pid -> {name, raw:{key:val}}
    for url, cols in BOARDS:
        try:
            rows = fetch_csv(url)
        except Exception as e:
            print(f"[percentiles] board failed {url[:70]}… ({e})", file=sys.stderr); continue
        for row in rows:
            pid = _i(row.get("player_id"))
            if pid is None: continue
            p = players.setdefault(pid, {"name": std_name(row), "raw": {}})
            if not p["name"]: p["name"] = std_name(row)
            for sc, key in cols.items():
                v = _f(row.get(sc))
                if v is None: continue
                if key == "pa": p["raw"]["pa"] = max(p["raw"].get("pa", 0), v)
                else: p["raw"][key] = v
        print(f"[percentiles] {url.split('/leaderboard/')[1][:22]}… {len(rows)} rows", file=sys.stderr)

    # qualified bar: ~Savant's rate-stat qualification (2.1 PA per team game).
    # Approx team games from the busiest bat's PA (~4.6 PA/game for a full-time
    # leadoff), giving threshold ~= 0.45 * max PA. Scales through the season.
    max_pa = max((p["raw"].get("pa", 0) or 0) for p in players.values()) if players else 0
    QUAL_PA = 0.45 * max_pa
    def is_qual(p): return (p["raw"].get("pa") or 0) >= QUAL_PA

    STAT_KEYS = ["xwoba","xba","xslg","xiso","barrel","hard_hit","exit_velocity","bat_speed","squared_up",
                 "k_pct","bb_pct","whiff","chase","sprint","oaa","arm_strength"]
    # percentile-rank each stat across everyone who has it
    def pctl_map(key):
        vals = sorted(p["raw"][key] for p in players.values() if key in p["raw"])
        n = len(vals)
        if n < 2: return {}, n
        import bisect
        out = {}
        for pid, p in players.items():
            if key not in p["raw"]: continue
            v = p["raw"][key]
            pct = 100.0 * bisect.bisect_right(vals, v) / n
            if key in INVERT: pct = 100.0 - pct
            out[pid] = max(1, min(99, round(pct)))
        return out, n
    pmaps = {k: pctl_map(k)[0] for k in STAT_KEYS}

    hitters = {}
    for pid, p in players.items():
        nm = p["name"]
        if not nm: continue
        pa = p["raw"].get("pa")
        entry = {"name": nm, "mlbam_id": pid,
                 "pa": int(pa) if pa is not None else None,
                 "qualified": is_qual(p)}
        for k in STAT_KEYS:
            if pid in pmaps[k]: entry[k] = pmaps[k][pid]
        # keep a few raw rates for modal displays
        if "barrel" in p["raw"]: entry["barrel_pct"] = round(p["raw"]["barrel"], 1)
        if "hard_hit" in p["raw"]: entry["hard_hit_pct"] = round(p["raw"]["hard_hit"], 1)
        if "exit_velocity" in p["raw"]: entry["exit_velocity_avg"] = round(p["raw"]["exit_velocity"], 1)
        if len(entry) > 4: hitters[norm_name(nm)] = entry

    payload = {"generated_at": datetime.datetime.utcnow().isoformat() + "Z", "year": YEAR,
               "source": f"Savant min=1 leaderboards, percentiles computed over full pool {YEAR}",
               "note": "0-100 percentiles vs the min=1 population. 'qualified'= PA >= ~0.45*max (Savant rate-stat bar); UI asterisks the rest.",
               "hitters": hitters}
    json.dump(payload, sys.stdout, indent=2)
    nq = sum(1 for h in hitters.values() if h.get("qualified"))
    print(f"[percentiles] wrote {len(hitters)} hitters ({nq} qualified)", file=sys.stderr)

if __name__ == "__main__":
    main()
