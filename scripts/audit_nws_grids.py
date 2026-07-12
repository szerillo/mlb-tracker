#!/usr/bin/env python3
"""
Audit the hardcoded NWS_GRIDS table in refresh_weather.py against the authoritative
api.weather.gov/points/{lat},{lon} lookup for each ballpark.

WHY THIS EXISTS
---------------
NWS grid cells are ~2.5 km and the (office, x, y) triples are opaque — a wrong one
fails silently, returning a perfectly valid forecast for the wrong place. On
2026-07-12 an audit found 15 of 29 parks pointing at the wrong cell:

  * Citi Field was on OKX 38,38 — a cell 11 miles SOUTH, out over Jamaica Bay.
    It published sea-breeze air (75F / 64% RH) for a park that was actually
    83F / 46%. Eight degrees of pure phantom cold, every Mets home game.
  * Angel Stadium was on the wrong forecast OFFICE entirely (LOX, not SGX).
  * Chase Field was 84 miles off; Fenway 31; Progressive 29; Yankee Stadium 17.

Run this after any park relocation, and periodically — NWS does re-grid.

    python scripts/audit_nws_grids.py            # report only
    python scripts/audit_nws_grids.py --fix      # rewrite refresh_weather.py in place

Exit code 1 if any park is >2 miles off (so it can gate CI if you want).
"""
from __future__ import annotations
import json, math, os, re, sys, urllib.request
import concurrent.futures as cf

HERE = os.path.dirname(os.path.abspath(__file__))
TARGET = os.path.join(HERE, "refresh_weather.py")
UA = {"User-Agent": "mlb-tracker/1.0 (+github.com/szerillo/mlb-tracker)"}
TOLERANCE_MI = 2.0


def _get(url):
    return json.load(urllib.request.urlopen(urllib.request.Request(url, headers=UA), timeout=30))


def _haversine_mi(lat1, lon1, lat2, lon2):
    R, r = 3959.0, math.radians
    a = (math.sin(r(lat2 - lat1) / 2) ** 2
         + math.cos(r(lat1)) * math.cos(r(lat2)) * math.sin(r(lon2 - lon1) / 2) ** 2)
    return 2 * R * math.asin(math.sqrt(a))


def load_tables(src: str):
    grids = {m[0]: (m[1], int(m[2]), int(m[3]))
             for m in re.findall(r'"([^"]+)":\s*\("([A-Z]{3})",\s*(\d+),\s*(\d+)\)', src)}
    coords = {m[0]: (float(m[1]), float(m[2]))
              for m in re.findall(r'"([^"]+)":\s*\((-?\d+\.\d+),\s*(-?\d+\.\d+)\)', src)}
    return grids, coords


def check(team, grid, coord):
    lat, lon = coord
    office, x, y = grid
    try:
        want = _get(f"https://api.weather.gov/points/{lat},{lon}")["properties"]
        have = _get(f"https://api.weather.gov/gridpoints/{office}/{x},{y}"
                    )["geometry"]["coordinates"][0][0]
        miles = _haversine_mi(lat, lon, have[1], have[0])
        return team, grid, (want["gridId"], want["gridX"], want["gridY"]), miles
    except Exception as e:
        return team, grid, None, float("nan")


def main():
    src = open(TARGET).read()
    grids, coords = load_tables(src)
    teams = sorted(set(grids) & set(coords))

    results = []
    with cf.ThreadPoolExecutor(max_workers=6) as ex:
        for r in ex.map(lambda t: check(t, grids[t], coords[t]), teams):
            results.append(r)
    results.sort(key=lambda r: -(r[3] if r[3] == r[3] else -1))

    bad = []
    print(f"{'team':26s}{'hardcoded':>15s}{'correct':>15s}{'off':>10s}")
    for team, have, want, miles in results:
        if want is None:
            print(f"{team[:26]:26s}{'lookup failed':>40s}")
            continue
        h = f"{have[0]} {have[1]},{have[2]}"
        w = f"{want[0]} {want[1]},{want[2]}"
        flag = ""
        if have != want and miles > TOLERANCE_MI:
            flag = "  <-- WRONG"
            bad.append((team, have, want, miles))
        elif have != want:
            flag = "  (adjacent cell, ok)"
        print(f"{team[:26]:26s}{h:>15s}{w:>15s}{miles:>8.1f}mi{flag}")

    if not bad:
        print("\nAll parks within tolerance.")
        return 0

    print(f"\n{len(bad)} park(s) more than {TOLERANCE_MI} miles off:")
    for team, have, want, miles in bad:
        print(f"    {team}: {have[0]} {have[1]},{have[2]} -> "
              f"{want[0]} {want[1]},{want[2]}   ({miles:.1f} mi)")

    if "--fix" in sys.argv:
        for team, _have, want, _m in bad:
            pat = re.compile(r'("%s":\s*)\("[A-Z]{3}",\s*\d+,\s*\d+\)' % re.escape(team))
            src, n = pat.subn(r'\1("%s", %d, %d)' % want, src)
            if n != 1:
                print(f"    !! could not rewrite {team}", file=sys.stderr)
        open(TARGET, "w").write(src)
        print(f"\nRewrote {len(bad)} grid(s) in refresh_weather.py")
        return 0

    print("\n(re-run with --fix to rewrite refresh_weather.py)")
    return 1


if __name__ == "__main__":
    sys.exit(main())
