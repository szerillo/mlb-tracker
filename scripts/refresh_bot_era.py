#!/usr/bin/env python3
"""
Merge FG Pitcher Bot ERA (pb_ERA) into pitcher_stats.json.

Independent of refresh_pitcher_stats_enrich.py's browser-dump flow so that
bot_era is populated even when the committed _fg_pitch_model.json dump is
stale or incomplete. Fangraphs API hit usually works from GH Actions.

Reads  pitcher_stats.json from argv[1]
Writes enriched JSON to stdout (pipe to /tmp/ps.json && mv into place).
"""
import json, sys, urllib.request, unicodedata, datetime

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36")

FG_URL = ("https://www.fangraphs.com/api/leaders/major-league/data"
          "?pos=all&stats=pit&lg=all&type=36&season={season}"
          "&month=0&season1={season}&ind=0&qual=10&pageitems=2000000000")


def strip_accents(s):
    if not isinstance(s, str): return s
    return "".join(c for c in unicodedata.normalize('NFKD', s) if not unicodedata.combining(c))


def norm_name(s):
    if not isinstance(s, str): return ""
    s = strip_accents(s).lower()
    for suffix in (' jr.', ' jr', ' sr.', ' sr', ' iii', ' ii'):
        if s.endswith(suffix): s = s[:-len(suffix)]
    return s.replace('.', '').strip()


def fetch_fg(season):
    url = FG_URL.format(season=season)
    req = urllib.request.Request(url, headers={
        "User-Agent": UA,
        "Referer": "https://www.fangraphs.com/leaders/major-league",
        "Accept": "application/json, text/plain, */*",
    })
    with urllib.request.urlopen(req, timeout=30) as r:
        body = json.loads(r.read().decode("utf-8", errors="replace"))
    return body.get("data", body) if isinstance(body, dict) else body


def main():
    if len(sys.argv) < 2:
        print("usage: refresh_bot_era.py path/to/pitcher_stats.json > out.json", file=sys.stderr)
        sys.exit(2)

    with open(sys.argv[1]) as f:
        payload = json.load(f)

    season = datetime.date.today().year
    try:
        rows = fetch_fg(season)
    except Exception as e:
        print(f"  FG live fetch failed: {e}", file=sys.stderr)
        json.dump(payload, sys.stdout, indent=2)
        return

    print(f"  FG type=36: {len(rows)} rows", file=sys.stderr)

    pitchers = payload.get("pitchers", {})
    matched = 0
    siera_matched = 0
    for r in rows:
        name = r.get("PlayerName") or r.get("Name")
        if not name:
            continue
        k = norm_name(name)
        if k not in pitchers:
            continue
        pb = r.get("pb_ERA") or r.get("botERA")
        if pb is not None:
            pitchers[k]["bot_era"] = round(float(pb), 2)
            matched += 1
        siera = r.get("SIERA")
        if siera is not None:
            pitchers[k]["siera"] = round(float(siera), 2)
            siera_matched += 1

    payload["pitchers"] = pitchers
    payload["bot_era_enriched_at"] = datetime.datetime.utcnow().isoformat() + "Z"
    payload["bot_era_count"] = matched
    sources = payload.setdefault("sources", [])
    src_line = "Fangraphs Pitcher Bot — pb_ERA"
    if src_line not in sources:
        sources.append(src_line)

    json.dump(payload, sys.stdout, indent=2)
    print(f"  bot_era: {matched} pitchers matched", file=sys.stderr)
    print(f"  siera: {siera_matched} pitchers matched", file=sys.stderr)


if __name__ == "__main__":
    main()
