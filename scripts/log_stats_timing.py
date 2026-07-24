#!/usr/bin/env python3
"""
Stats-timing fingerprint logger.

Appends ONE row per run to data/stats_timing_log.csv capturing, for each
FG/Savant season-stat file: coverage counts + a value-HASH (rounded to 2 dp,
so sub-decimal noise is ignored). Run at 6/7/8/9 AM ET for a few days: if a
source's hash is identical across hours on the same day, that source was
already fully settled by the earlier hour -> we can pull earlier safely.
Compare hours with:  scripts/log_stats_timing.py --report
"""
import json, os, csv, sys, hashlib, datetime

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
LOG = os.path.join(ROOT, "data", "stats_timing_log.csv")

def _load(rel):
    try:
        return json.load(open(os.path.join(ROOT, rel)))
    except Exception:
        return None

def _hash(pairs):
    """pairs = iterable of (id, value); rounds numbers, sorts, md5[:10]."""
    items = []
    for k, v in pairs:
        if v is None:
            continue
        if isinstance(v, (int, float)):
            v = round(float(v), 2)
        items.append((str(k), v))
    items.sort()
    return hashlib.md5(repr(items).encode()).hexdigest()[:10], len(items)

def fingerprint():
    et = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=4)
    row = {
        "ts_utc": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "date_et": et.strftime("%Y-%m-%d"),
        "hour_et": et.hour,
    }
    # pitcher_stats: coverage + hashes for the source-driven enrichments
    ps = _load("data/pitcher_stats.json") or {}
    P = ps.get("pitchers", {}) or {}
    row["pit_n"] = len(P)
    for field in ("fip_proj", "xfip", "bot_era", "stuff_plus"):
        h, n = _hash((k, v.get(field)) for k, v in P.items() if isinstance(v, dict))
        row[f"pit_{field}_n"] = n
        row[f"h_{field}"] = h
    # arsenal (Savant per-pitch-type) -> hash total_pitches per pitcher
    ar = (_load("data/pitcher_arsenal.json") or {}).get("pitchers", {}) or {}
    row["ars_n"] = len(ar)
    row["h_arsenal"] = _hash((k, v.get("total_pitches")) for k, v in ar.items() if isinstance(v, dict))[0]
    # hitter percentiles (Savant) -> hash xwoba
    hp = (_load("data/hitter_percentiles.json") or {}).get("hitters", {}) or {}
    row["hit_n"] = len(hp)
    row["hit_qual"] = sum(1 for v in hp.values() if isinstance(v, dict) and v.get("qualified"))
    row["h_hit_xwoba"] = _hash((k, v.get("xwoba")) for k, v in hp.items() if isinstance(v, dict))[0]
    # grades_v2 -> hash power+eye
    gr = (_load("data/grades_v2.json") or {}).get("by_name", {}) or {}
    row["grd_n"] = len(gr)
    row["h_grades"] = _hash((k, (v.get("power"), v.get("eye"))) for k, v in gr.items() if isinstance(v, dict))[0]
    # power_eye csv row count
    try:
        with open(os.path.join(ROOT, "data", "power_eye_2026.csv")) as fh:
            row["pe_rows"] = sum(1 for _ in fh) - 1
    except Exception:
        row["pe_rows"] = 0
    return row

def append(row):
    cols = list(row.keys())
    exists = os.path.exists(LOG)
    # keep a stable column order across runs
    if exists:
        with open(LOG) as fh:
            hdr = fh.readline().strip().split(",")
        cols = hdr + [c for c in row if c not in hdr]
    with open(LOG, "a", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=cols)
        if not exists:
            w.writeheader()
        w.writerow({c: row.get(c, "") for c in cols})
    print(f"[stats-timing] logged {row['date_et']} {row['hour_et']:02d}:00 ET  "
          f"pit={row['pit_n']} fip_proj_n={row.get('pit_fip_proj_n')} ars={row['ars_n']} "
          f"hit={row['hit_n']} grd={row['grd_n']}")

def report():
    if not os.path.exists(LOG):
        print("no log yet"); return
    rows = list(csv.DictReader(open(LOG)))
    from collections import defaultdict
    by_day = defaultdict(dict)
    for r in rows:
        by_day[r["date_et"]][int(r["hour_et"])] = r
    hashcols = [c for c in rows[0] if c.startswith("h_")]
    print("Per day: for each source hash, the earliest hour it matches the day's LAST (settled) hour.")
    for day in sorted(by_day):
        hours = sorted(by_day[day])
        last = by_day[day][hours[-1]]
        line = [f"{day} (hours {hours})"]
        for hc in hashcols:
            first_match = next((h for h in hours if by_day[day][h].get(hc) == last.get(hc)), None)
            line.append(f"{hc.replace('h_','')}={first_match}")
        print("  " + "  ".join(line))

if __name__ == "__main__":
    if "--report" in sys.argv:
        report()
    else:
        append(fingerprint())
