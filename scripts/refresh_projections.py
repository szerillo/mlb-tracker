#!/usr/bin/env python3
"""
Merge ROS (rest-of-season) FIP projections from Fangraphs into pitcher_stats.json.

Sources (ROS, not preseason/full-season):
  rfangraphsdc → ROS FG Depth Charts (multi-system blend; substitutes for ATC)
  rthebatx     → ROS The BAT X
  steamerr     → ROS Steamer (substitutes for OOPSY, which has no public ROS API)
  rzips        → ROS ZiPS

We previously hit ?type=atc / thebatx / oopsy / zips which are FULL-SEASON
projections — they anchor heavily to preseason talent estimates and barely
move in response to actual in-season performance. For breakout pitchers like
Misiorowski (1.65 ERA mid-season) the full-season blend stayed around fip_proj
3.9, while ROS reflects his current form at ~3.3. Switched to ROS endpoints
in June 2026 to surface in-season form properly.

Field-name mapping (kept old keys so frontend doesn't need to update labels
this same push — TODO: rename fip_atc→fip_fgdc and fip_oopsy→fip_steamer in
the frontend tooltips on a follow-up commit):
  fip_atc   ← rfangraphsdc  (display label will be relabeled later)
  fip_batx  ← rthebatx
  fip_oopsy ← steamerr      (display label will be relabeled later)
  fip_zips  ← rzips

Reads  pitcher_stats.json from argv[1]
Writes enriched JSON to stdout (pipe to /tmp/ps.json && mv into place).
"""
import json, os, sys, urllib.request, unicodedata, datetime

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36")

# Each tuple: (output-field name, Fangraphs API `type` param)
# All four point at ROS endpoints. ATC and OOPSY don't expose ROS variants
# (their `?type=ratc` / `?type=roopsy` return HTTP 500), so we substitute
# rfangraphsdc and steamerr — both are FG-published ROS projections that
# also blend multiple systems internally.
SYSTEMS = [
    ("fip_atc",   "rfangraphsdc"),  # ROS FG Depth Charts (substitute for ATC)
    ("fip_batx",  "rthebatx"),       # ROS The BAT X
    ("fip_oopsy", "steamerr"),       # ROS Steamer (substitute for OOPSY)
    ("fip_zips",  "rzips"),          # ROS ZiPS
]


def strip_accents(s):
    if not isinstance(s, str): return s
    return "".join(c for c in unicodedata.normalize('NFKD', s) if not unicodedata.combining(c))


def norm_name(s):
    if not isinstance(s, str): return ""
    s = strip_accents(s).lower()
    for suffix in (' jr.', ' jr', ' sr.', ' sr', ' iii', ' ii'):
        if s.endswith(suffix): s = s[:-len(suffix)]
    return s.replace('.', '').strip()


def fetch_projection(proj_type, season):
    url = (f"https://www.fangraphs.com/api/projections"
           f"?pos=all&type={proj_type}&stats=pit&season={season}&players=0")
    req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read().decode("utf-8", errors="replace"))


def main():
    if len(sys.argv) < 2:
        print("usage: refresh_projections.py path/to/pitcher_stats.json > out.json", file=sys.stderr)
        sys.exit(2)

    with open(sys.argv[1]) as f:
        payload = json.load(f)

    season = datetime.date.today().year
    pitchers = payload.get("pitchers", {})

    # CRITICAL: clear stale full-season projection values BEFORE writing fresh
    # ROS values. Otherwise pitchers who got dropped from a ROS source (e.g.
    # rzips has 620 rows vs zips' ~700) keep their old full-season number,
    # producing a misleading "mixed" blend with one stale full-season source.
    proj_keys = [f for f, _ in SYSTEMS]
    for k, row in pitchers.items():
        for f in proj_keys:
            if f in row:
                row[f] = None  # explicit null, not delete — keeps schema stable

    enriched_count = {k: 0 for k, _ in SYSTEMS}
    for field, proj_type in SYSTEMS:
        try:
            rows = fetch_projection(proj_type, season)
        except Exception as e:
            print(f"  {proj_type} fetch failed: {e}", file=sys.stderr)
            continue
        print(f"  {proj_type}: {len(rows)} rows", file=sys.stderr)
        for r in rows:
            name = r.get("PlayerName") or r.get("playerName") or r.get("Name")
            fip  = r.get("FIP")
            if not name or fip is None:
                continue
            k = norm_name(name)
            if not k:
                continue
            if k in pitchers:
                pitchers[k][field] = round(float(fip), 2)
                enriched_count[field] += 1
            else:
                # Add a stub so rendering still works if pitcher wasn't in base dump
                pitchers[k] = pitchers.get(k, {})
                pitchers[k][field] = round(float(fip), 2)

    # fip_proj = MEAN of available source projections.
    #
    # History: this used to be aliased to fip_atc when fip_proj was null, but
    # that left stale single-source values in place when older pipeline runs
    # had populated fip_proj from a different system (e.g. Mason Miller ended
    # up with fip_proj=5.23 from OOPSY's outlier instead of his ~3.07 blend
    # across ATC/BatX/OOPSY/ZiPS, which dragged his wFIP down ~0.3 runs).
    #
    # The fix: recompute fip_proj every run as the mean of whatever subset of
    # [fip_atc, fip_batx, fip_oopsy, fip_zips] is populated. ALWAYS overwrite -
    # so a stuck value from an older script can't survive a refresh.
    for k, row in pitchers.items():
        vals = [row[f] for f in proj_keys if isinstance(row.get(f), (int, float))]
        if vals:
            row["fip_proj"] = round(sum(vals) / len(vals), 2)
            row["fip_proj_n_sources"] = len(vals)
        elif "fip_proj" in row:
            # No sources available - clear any stuck old value rather than
            # leaving an unverifiable number sitting on the record.
            row["fip_proj"] = None
            row["fip_proj_n_sources"] = 0

    payload["pitchers"] = pitchers
    payload["projections_enriched_at"] = datetime.datetime.utcnow().isoformat() + "Z"
    payload["projections_counts"] = enriched_count
    payload.setdefault("sources", [])
    src_line = "Fangraphs ROS projections — FG Depth Charts / The BAT X / Steamer / ZiPS (FIP)"
    # Drop old preseason source line if present
    payload["sources"] = [s for s in payload["sources"]
                          if "ATC / The BAT X / OOPSY / ZiPS" not in s
                          and s != src_line]
    payload["sources"].append(src_line)

    json.dump(payload, sys.stdout, indent=2)
    for field, ct in enriched_count.items():
        print(f"  {field}: {ct} pitchers matched", file=sys.stderr)


if __name__ == "__main__":
    main()
