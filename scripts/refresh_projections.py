#!/usr/bin/env python3
"""
Merge multiple Fangraphs FIP projections into pitcher_stats.json.

Sources: ATC, The BAT X, OOPSY, ZiPS (each has its own FIP projection).
Keyed on normalized name (matches the rest of the pipeline).

Reads  pitcher_stats.json from argv[1]
Writes enriched JSON to stdout (pipe to /tmp/ps.json && mv into place).
"""
import json, os, sys, urllib.request, unicodedata, datetime

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36")

# Each tuple: (output-field name, Fangraphs API `type` param)
SYSTEMS = [
    ("fip_atc",   "atc"),
    ("fip_batx",  "thebatx"),   # The BAT X
    ("fip_oopsy", "oopsy"),
    ("fip_zips",  "zips"),
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

    # Keep fip_proj as a back-compat alias for fip_atc until the UI is fully
    # migrated (index.html still falls through to fip_proj on older caches).
    for k, row in pitchers.items():
        if row.get("fip_atc") is not None and row.get("fip_proj") is None:
            row["fip_proj"] = row["fip_atc"]

    payload["pitchers"] = pitchers
    payload["projections_enriched_at"] = datetime.datetime.utcnow().isoformat() + "Z"
    payload["projections_counts"] = enriched_count
    payload.setdefault("sources", [])
    src_line = "Fangraphs projections — ATC / The BAT X / OOPSY / ZiPS (FIP)"
    if src_line not in payload["sources"]:
        payload["sources"].append(src_line)

    json.dump(payload, sys.stdout, indent=2)
    for field, ct in enriched_count.items():
        print(f"  {field}: {ct} pitchers matched", file=sys.stderr)


if __name__ == "__main__":
    main()
