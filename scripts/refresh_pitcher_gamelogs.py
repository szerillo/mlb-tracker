#!/usr/bin/env python3
"""
Build data/pitcher_gamelogs.json — per-START trend data for starting pitchers.

For every pitcher who has made a start this season (MLB universe), pull their
FanGraphs game log (one API call each) and extract, per start:

    date, opp, IP, H, R, ER, BB, K, HR, TBF, pitches,
    K%, BB%, avg fastball velo (FBv), Stuff+ (sp_stuff), xFIP, SIERA

Then compute L5 and season aggregates with sensible weighting:
    • K% / BB%      → true ratios over the window (ΣK/ΣTBF, ΣBB/ΣTBF)
    • xFIP / SIERA  → IP-weighted average of the per-start values
                      (so a 0.2-IP blowup start doesn't dominate)
    • velo / Stuff+ → pitch-weighted average

WHY FanGraphs game logs (and not the FG leaderboard): the leaderboard API is
Cloudflare-gated for server-side callers (see refresh_pitcher_stats_enrich.py),
but the per-player game-log endpoint currently answers server-side and carries
every column we need in a single call. We still degrade gracefully:
    • FG id map comes from pybaseball (Chadwick register); we persist it to
      data/_mlbam_fg_map.json so a register hiccup can't wipe known ids.
    • If FG returns nothing for a pitcher MLB says has starts, we fall back to
      the MLB Stats API game log for the line scores + K%/BB% (velo/Stuff+/
      xFIP/SIERA simply stay null for that pitcher until FG answers again).

Output is keyed by the SAME normalized name the frontend uses
(index.html normName), so the SP trend card joins by probablePitcher.fullName.

USAGE:
    python scripts/refresh_pitcher_gamelogs.py            # writes data/pitcher_gamelogs.json
    python scripts/refresh_pitcher_gamelogs.py --limit 12 # quick subset (dev)
"""
from __future__ import annotations
import argparse
import datetime
import json
import os
import re
import sys
import time
import unicodedata
import urllib.request
import urllib.error

HERE = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(HERE, "..", "data")
OUTPUT = os.path.join(DATA, "pitcher_gamelogs.json")
FG_MAP_CACHE = os.path.join(DATA, "_mlbam_fg_map.json")

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

LOOKBACK_L5 = 5
THROTTLE_S = 0.5      # polite delay between FG calls
MAX_RETRIES = 3


# ── name normalization (must match index.html normName exactly) ───────────
def norm_name(s: str) -> str:
    if not s:
        return ""
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = s.lower()
    s = re.sub(r" jr\.?$", "", s)
    s = re.sub(r" sr\.?$", "", s)
    s = re.sub(r" iii$", "", s)
    s = re.sub(r" ii$", "", s)
    s = s.replace(".", "")
    return s.strip()


def _f(v):
    """Coerce to float; None for missing/non-numeric/NaN."""
    if v is None:
        return None
    try:
        v = float(v)
        return v if v == v else None
    except (TypeError, ValueError):
        return None


def strip_html(s):
    if s is None:
        return None
    return re.sub(r"<[^>]+>", "", str(s)).strip()


def ip_to_outs(ip) -> int:
    """FanGraphs/MLB IP is baseball notation: 6.1 = 6⅓ (19 outs), 0.2 = 2 outs."""
    ip = _f(ip)
    if ip is None:
        return 0
    whole = int(ip)
    frac = round((ip - whole) * 10)   # .1 -> 1, .2 -> 2
    return whole * 3 + (frac if frac in (0, 1, 2) else 0)


def http_json(url, timeout=25):
    req = urllib.request.Request(url, headers={
        "User-Agent": UA,
        "Referer": "https://www.fangraphs.com/",
        "Accept": "application/json, text/plain, */*",
    })
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())


# ── universe: every pitcher with a start this season (MLB Stats API) ──────
def get_starter_universe(season: int):
    url = (f"https://statsapi.mlb.com/api/v1/stats?stats=season&season={season}"
           f"&group=pitching&gameType=R&sportId=1&limit=2000&playerPool=all")
    try:
        d = http_json(url)
        splits = d["stats"][0]["splits"]
    except Exception as e:
        print(f"[gamelogs] starter universe fetch failed: {e}", file=sys.stderr)
        return []
    out = []
    for s in splits:
        st = s.get("stat", {})
        if (st.get("gamesStarted") or 0) <= 0:
            continue
        p = s.get("player", {})
        if p.get("id") and p.get("fullName"):
            out.append((int(p["id"]), p["fullName"]))
    # de-dupe by mlbam id (a traded player can appear twice)
    seen, uniq = set(), []
    for mid, nm in out:
        if mid not in seen:
            seen.add(mid)
            uniq.append((mid, nm))
    return uniq


# ── mlbam -> fangraphs id map (pybaseball + persisted cache) ──────────────
def load_fg_cache():
    if os.path.exists(FG_MAP_CACHE):
        try:
            with open(FG_MAP_CACHE) as f:
                return {int(k): v for k, v in json.load(f).get("map", {}).items()}
        except Exception:
            return {}
    return {}


def save_fg_cache(m):
    try:
        os.makedirs(DATA, exist_ok=True)
        with open(FG_MAP_CACHE, "w") as f:
            json.dump({"updated_at": datetime.datetime.utcnow().isoformat() + "Z",
                       "map": {str(k): v for k, v in m.items()}}, f, indent=2)
    except Exception as e:
        print(f"[gamelogs] could not write fg map cache: {e}", file=sys.stderr)


def build_fg_map(mlbam_ids):
    """Return {mlbam_id: fg_id}. Cache-first, then pybaseball for the rest."""
    cache = load_fg_cache()
    have = {mid: cache[mid] for mid in mlbam_ids if mid in cache and cache[mid]}
    missing = [mid for mid in mlbam_ids if mid not in have]
    if missing:
        try:
            from pybaseball import playerid_reverse_lookup
            df = playerid_reverse_lookup(missing, key_type="mlbam")
            for _, row in df.iterrows():
                fg = row.get("key_fangraphs")
                mid = row.get("key_mlbam")
                if fg and mid and int(fg) > 0:
                    have[int(mid)] = int(fg)
        except Exception as e:
            print(f"[gamelogs] pybaseball lookup failed ({e}); "
                  f"using cache only ({len(have)} ids)", file=sys.stderr)
    # merge into cache and persist
    merged = dict(cache)
    merged.update(have)
    save_fg_cache(merged)
    return have


# ── per-pitcher extraction ────────────────────────────────────────────────
def fg_starts(fg_id, season):
    """Return list of per-start dicts from a FanGraphs game log, or None on error."""
    url = (f"https://www.fangraphs.com/api/players/game-log?playerid={fg_id}"
           f"&position=P&type=1&season={season}")
    data = None
    for attempt in range(MAX_RETRIES):
        try:
            data = http_json(url)
            break
        except Exception as e:
            if attempt == MAX_RETRIES - 1:
                print(f"[gamelogs] FG fetch failed id={fg_id}: {e}", file=sys.stderr)
                return None
            time.sleep(1.0 * (attempt + 1))
    rows = data.get("mlb", []) if isinstance(data, dict) else []
    starts = []
    for r in rows:
        date = strip_html(r.get("Date"))
        if not date or "2050" in date or not re.match(r"\d{4}-\d{2}-\d{2}", date):
            continue
        if (_f(r.get("GS")) or 0) < 1:
            continue  # relief outing — starters only
        kpct = _f(r.get("K%"))
        bbpct = _f(r.get("BB%"))
        starts.append({
            "date": date,
            "opp": strip_html(r.get("Opp")),
            "ip": _f(r.get("IP")),
            "outs": ip_to_outs(r.get("IP")),
            "h": _f(r.get("H")),
            "r": _f(r.get("R")),
            "er": _f(r.get("ER")),
            "bb": _f(r.get("BB")),
            "k": _f(r.get("SO")),
            "hr": _f(r.get("HR")),
            "tbf": _f(r.get("TBF")),
            "pitches": _f(r.get("Pitches")),
            "k_pct": round(kpct * 100, 1) if kpct is not None else None,
            "bb_pct": round(bbpct * 100, 1) if bbpct is not None else None,
            "velo": round(_f(r.get("FBv")), 1) if _f(r.get("FBv")) is not None else None,
            "stuff": round(_f(r.get("sp_stuff")), 0) if _f(r.get("sp_stuff")) is not None else None,
            "xfip": round(_f(r.get("xFIP")), 2) if _f(r.get("xFIP")) is not None else None,
            "siera": round(_f(r.get("SIERA")), 2) if _f(r.get("SIERA")) is not None else None,
        })
    starts.sort(key=lambda s: s["date"])
    return starts


def mlb_starts_fallback(mlbam_id, season):
    """Line scores + K%/BB% only, when FG is unavailable for this pitcher."""
    url = (f"https://statsapi.mlb.com/api/v1/people/{mlbam_id}/stats"
           f"?stats=gameLog&group=pitching&season={season}")
    try:
        d = http_json(url)
        splits = d["stats"][0]["splits"]
    except Exception:
        return []
    starts = []
    for s in splits:
        st = s.get("stat", {})
        if (_f(st.get("gamesStarted")) or 0) < 1:
            continue
        tbf = _f(st.get("battersFaced"))
        k = _f(st.get("strikeOuts"))
        bb = _f(st.get("baseOnBalls"))
        opp = (s.get("opponent") or {}).get("name")
        starts.append({
            "date": s.get("date"),
            "opp": opp,
            "ip": _f(st.get("inningsPitched")),
            "outs": ip_to_outs(st.get("inningsPitched")),
            "h": _f(st.get("hits")), "r": _f(st.get("runs")),
            "er": _f(st.get("earnedRuns")), "bb": bb, "k": k,
            "hr": _f(st.get("homeRuns")), "tbf": tbf,
            "pitches": _f(st.get("numberOfPitches")),
            "k_pct": round(k / tbf * 100, 1) if (k is not None and tbf) else None,
            "bb_pct": round(bb / tbf * 100, 1) if (bb is not None and tbf) else None,
            "velo": None, "stuff": None, "xfip": None, "siera": None,
        })
    starts.sort(key=lambda s: s["date"])
    return starts


# ── aggregation ───────────────────────────────────────────────────────────
def aggregate(starts):
    """K%/BB% as true ratios; xFIP/SIERA IP-weighted; velo/Stuff+ pitch-weighted."""
    if not starts:
        return None
    sum_k = sum(s["k"] or 0 for s in starts)
    sum_bb = sum(s["bb"] or 0 for s in starts)
    sum_tbf = sum(s["tbf"] or 0 for s in starts)
    sum_outs = sum(s["outs"] or 0 for s in starts)

    def ip_weighted(field):
        num = sum((s[field] or 0) * (s["outs"] or 0)
                  for s in starts if s[field] is not None)
        den = sum((s["outs"] or 0) for s in starts if s[field] is not None)
        return round(num / den, 2) if den else None

    def pitch_weighted(field, ndigits):
        num = sum((s[field] or 0) * (s["pitches"] or 0)
                  for s in starts if s[field] is not None and s["pitches"])
        den = sum((s["pitches"] or 0)
                  for s in starts if s[field] is not None and s["pitches"])
        return round(num / den, ndigits) if den else None

    return {
        "n": len(starts),
        "ip_outs": sum_outs,
        "k_pct": round(sum_k / sum_tbf * 100, 1) if sum_tbf else None,
        "bb_pct": round(sum_bb / sum_tbf * 100, 1) if sum_tbf else None,
        "xfip": ip_weighted("xfip"),
        "siera": ip_weighted("siera"),
        "velo": pitch_weighted("velo", 1),
        "stuff": pitch_weighted("stuff", 0),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=0, help="cap pitchers (dev)")
    ap.add_argument("--season", type=int,
                    default=int(os.environ.get("FG_SEASON", datetime.date.today().year)))
    args = ap.parse_args()
    season = args.season

    universe = get_starter_universe(season)
    if args.limit:
        universe = universe[:args.limit]
    print(f"[gamelogs] starter universe: {len(universe)} pitchers (season {season})",
          file=sys.stderr)

    fg_map = build_fg_map([mid for mid, _ in universe])
    print(f"[gamelogs] resolved {len(fg_map)} FanGraphs ids", file=sys.stderr)

    pitchers = {}
    fg_ok = fg_fail = mlb_fb = 0
    for i, (mlbam_id, full_name) in enumerate(universe):
        starts = None
        fg_id = fg_map.get(mlbam_id)
        if fg_id:
            starts = fg_starts(fg_id, season)
            time.sleep(THROTTLE_S)
        if starts:
            fg_ok += 1
            src = "fangraphs"
        else:
            # FG missing/blocked → MLB line-score fallback
            starts = mlb_starts_fallback(mlbam_id, season)
            if starts:
                mlb_fb += 1
                src = "mlb-fallback"
            else:
                fg_fail += 1
                continue
        if not starts:
            continue
        key = norm_name(full_name)
        pitchers[key] = {
            "name": full_name,
            "mlbam_id": mlbam_id,
            "fg_id": fg_id,
            "source": src,
            "starts": starts,
            "l5": aggregate(starts[-LOOKBACK_L5:]),
            "season": aggregate(starts),
        }
        if (i + 1) % 25 == 0:
            print(f"[gamelogs]   {i+1}/{len(universe)} processed "
                  f"(fg={fg_ok}, mlb_fb={mlb_fb}, fail={fg_fail})", file=sys.stderr)

    payload = {
        "generated_at": datetime.datetime.utcnow().isoformat() + "Z",
        "season": season,
        "source": "FanGraphs game logs (server-side); MLB Stats API universe + fallback",
        "count": len(pitchers),
        "coverage": {"universe": len(universe), "fg_matched": len(fg_map),
                     "fg_ok": fg_ok, "mlb_fallback": mlb_fb, "failed": fg_fail},
        "lookback_l5": LOOKBACK_L5,
        "pitchers": pitchers,
    }
    os.makedirs(DATA, exist_ok=True)
    with open(OUTPUT, "w") as f:
        json.dump(payload, f, indent=2)
    print(f"[gamelogs] wrote {len(pitchers)} pitchers → {OUTPUT} "
          f"(fg_ok={fg_ok}, mlb_fb={mlb_fb}, fail={fg_fail})", file=sys.stderr)


if __name__ == "__main__":
    main()
