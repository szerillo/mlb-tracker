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

LOOKBACK_L5 = 5         # legacy SP last-N appearances target (kept for back-compat)
LOOKBACK_MIN_IP  = 20   # IP target for the "rolling" window — applies to ALL pitchers
LOOKBACK_MIN_APP = 5    # always span at least 5 appearances (so an SP w/ short
                        # outings still gets enough sample; an RP almost always
                        # needs more appearances than this to clear 20 IP).
LOOKBACK_MAX_APP = 30   # hard cap so we never grab months-old reliever data
THROTTLE_S = 0.5      # polite delay between FG calls
MAX_RETRIES = 3


def _select_rolling_window(starts):
    """Return the trailing slice of `starts` that satisfies the rolling-IP
    policy: span at least LOOKBACK_MIN_IP innings, never fewer than
    LOOKBACK_MIN_APP appearances, never more than LOOKBACK_MAX_APP.

    Sean's ask (2026-06): the old 'last 5 appearances' window meant relievers
    had a 5-IP rolling sample, which produced wildly volatile component values
    and pushed top arms down the rankings. This window is IP-anchored — for SPs
    it still resolves to roughly 5 starts; for RPs it grows to whatever count of
    appearances is needed to clear 20 IP."""
    if not starts:
        return []
    # starts are time-ordered ascending; we want the most recent end
    rev = list(reversed(starts))
    picked = []
    outs = 0
    for s in rev:
        picked.append(s)
        outs += (s.get("outs") or 0)
        if (len(picked) >= LOOKBACK_MIN_APP
                and outs >= LOOKBACK_MIN_IP * 3):
            break
        if len(picked) >= LOOKBACK_MAX_APP:
            break
    # restore chronological order before returning
    return list(reversed(picked))


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


# ── universe: every pitcher who's either started OR thrown enough innings ─
# to be a candidate for the rotation. Broadening from "GS > 0" so bulk relievers
# / openers / call-ups who transition to SP have gamelog history available the
# moment they're announced. MIN_IP_FOR_UNIVERSE = 20 keeps fringe arms out.
MIN_IP_FOR_UNIVERSE = 20.0

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
        gs = st.get("gamesStarted") or 0
        try:
            ip = float(st.get("inningsPitched") or 0)
        except (TypeError, ValueError):
            ip = 0.0
        # Include if they've started any games OR thrown >= MIN_IP_FOR_UNIVERSE.
        if gs <= 0 and ip < MIN_IP_FOR_UNIVERSE:
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
    """Per-appearance line scores + K%/BB% from MLB Stats API gameLog.

    Returns ALL appearances (starts + relief), not just starts — Sean's ask:
    bulk pitchers like Luinder Avila who relieve between spot starts were
    showing only their 1 start with no relief data. The K/BB% rolling
    aggregates downstream are PA-weighted via TBF, so relief outings just
    flow into the rolling pool naturally.
    """
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
        # Was: if gamesStarted < 1: continue  (filtered out relief). Now we
        # keep every appearance — the IP/TBF gate downstream still excludes
        # zero-batter cameos.
        tbf = _f(st.get("battersFaced"))
        k = _f(st.get("strikeOuts"))
        bb = _f(st.get("baseOnBalls"))
        opp = (s.get("opponent") or {}).get("name")
        starts.append({
            "date": s.get("date"),
            "opp": opp,
            "is_home": s.get("isHome"),
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


# ── per-start plate-discipline from Statcast (CSW% / whiff% / ball% / mix) ──
# FanGraphs game logs carry SwStr% and Balls but NOT a called-strike count, so
# true CSW% isn't derivable there. Statcast pitch-level data is the accurate
# source: one statcast_pitcher() call per arm yields every pitch's description,
# from which we compute per-GAME CSW% (called+swinging strikes / pitches),
# whiff% (swinging strikes / pitches), ball% (balls / pitches) and pitch mix.
_SC_CSW = {"called_strike", "swinging_strike", "swinging_strike_blocked"}
_SC_WHIFF = {"swinging_strike", "swinging_strike_blocked", "missed_bunt"}
_SC_BALL = {"ball", "blocked_ball", "pitchout", "hit_by_pitch"}
_FB_TYPES = {"FF", "SI", "FT", "FC"}   # fastball family for per-start velo

# Full MLB team name -> Baseball-Reference-style 3-letter abbreviation, for the
# per-start game-log "Opp" column. Away games get an "@" prefix.
NAME2ABBR = {
    "Arizona Diamondbacks":"ARI","Athletics":"ATH","Atlanta Braves":"ATL",
    "Baltimore Orioles":"BAL","Boston Red Sox":"BOS","Chicago Cubs":"CHC",
    "Chicago White Sox":"CHW","Cincinnati Reds":"CIN","Cleveland Guardians":"CLE",
    "Colorado Rockies":"COL","Detroit Tigers":"DET","Houston Astros":"HOU",
    "Kansas City Royals":"KCR","Los Angeles Angels":"LAA","Los Angeles Dodgers":"LAD",
    "Miami Marlins":"MIA","Milwaukee Brewers":"MIL","Minnesota Twins":"MIN",
    "New York Mets":"NYM","New York Yankees":"NYY","Philadelphia Phillies":"PHI",
    "Pittsburgh Pirates":"PIT","San Diego Padres":"SDP","San Francisco Giants":"SFG",
    "Seattle Mariners":"SEA","St. Louis Cardinals":"STL","Tampa Bay Rays":"TBR",
    "Texas Rangers":"TEX","Toronto Blue Jays":"TOR","Washington Nationals":"WSN",
}
def fmt_opp(name, is_home):
    """'Kansas City Royals', is_home=False -> '@KCR'. Unknown home/away -> no @."""
    if not name:
        return name
    ab = NAME2ABBR.get(name, name)
    return ("@" + ab) if is_home is False else ab


def statcast_discipline(mlbam_id, season):
    """Per-game {date_iso: {csw, whiff, ball_pct, mix}} from Statcast pitch data.
    Returns {} on any failure so the FG line-score data still ships."""
    try:
        from pybaseball import statcast_pitcher
        df = statcast_pitcher(f"{season}-03-01",
                              datetime.date.today().isoformat(), mlbam_id)
    except Exception as e:
        print(f"[gamelogs] statcast discipline failed id={mlbam_id}: {e}",
              file=sys.stderr)
        return {}
    if df is None or len(df) == 0 or "game_date" not in df.columns \
            or "description" not in df.columns:
        return {}
    out = {}
    try:
        for gd, g in df.groupby("game_date"):
            n = len(g)
            if not n:
                continue
            desc = g["description"].astype(str)
            ev = g["events"].astype(str) if "events" in g.columns else desc.iloc[0:0]
            bbt = g["bb_type"].astype(str) if "bb_type" in g.columns else desc.iloc[0:0]
            rec = {
                "csw": round(int(desc.isin(_SC_CSW).sum()) / n, 3),
                "whiff": round(int(desc.isin(_SC_WHIFF).sum()) / n, 3),
                "ball_pct": round(int(desc.isin(_SC_BALL).sum()) / n, 3),
                "velo": (lambda fb: round(float(fb.mean()), 1)
                         if len(fb) and fb.mean() == fb.mean() else None)(
                    g.loc[g["pitch_type"].astype(str).isin(_FB_TYPES), "release_speed"]
                    if ("pitch_type" in g.columns and "release_speed" in g.columns)
                    else g["description"].iloc[0:0]),
                "fb_spin": (lambda fb: round(float(fb.mean()))
                            if len(fb) and fb.mean() == fb.mean() else None)(
                    g.loc[g["pitch_type"].astype(str).isin(_FB_TYPES), "release_spin_rate"]
                    if ("pitch_type" in g.columns and "release_spin_rate" in g.columns)
                    else g["description"].iloc[0:0]),
                # raw component counts for self-computed xFIP / SIERA
                "comp": {
                    "K":   int((ev == "strikeout").sum() + (ev == "strikeout_double_play").sum()),
                    "BB":  int((ev == "walk").sum()),
                    "HBP": int((ev == "hit_by_pitch").sum()),
                    "HR":  int((ev == "home_run").sum()),
                    "FB":  int((bbt == "fly_ball").sum()),
                    "GB":  int((bbt == "ground_ball").sum()),
                    "LD":  int((bbt == "line_drive").sum()),
                    "PU":  int((bbt == "popup").sum()),
                    "PA":  int((ev != "").sum() - (ev == "nan").sum()),
                },
            }
            if "pitch_type" in g.columns:
                vc = g["pitch_type"].astype(str).value_counts(normalize=True)
                mix = {k: round(float(v), 3) for k, v in vc.items()
                       if k and k != "nan"}
                if mix:
                    rec["mix"] = mix
            out[str(gd)[:10]] = rec
    except Exception as e:
        print(f"[gamelogs] statcast group failed id={mlbam_id}: {e}",
              file=sys.stderr)
    return out


def merge_discipline(starts, disc):
    """Attach csw/whiff/ball_pct/mix to each start by date; default None so
    downstream aggregate() can rely on the keys existing."""
    for s in starts:
        d = disc.get(s.get("date"))
        s["csw"] = d["csw"] if d else None
        s["whiff"] = d["whiff"] if d else None
        s["ball_pct"] = d["ball_pct"] if d else None
        if d and d.get("velo") is not None:
            s["velo"] = d["velo"]   # Statcast FB velo (ground truth; overrides FG FBv)
        s["fb_spin"] = d.get("fb_spin") if d else None
        s["_comp"] = d.get("comp") if d else None
        if d and d.get("mix"):
            s["mix"] = d["mix"]


# ── self-computed xFIP / SIERA (FanGraphs formulas, anchored to FG season) ──
# FanGraphs blocks its per-start game-log xFIP/SIERA server-side, but the inputs
# (K, BB, HBP, fly balls, batted-ball mix, PA) come from Statcast + MLB logs,
# which are NOT blocked. We compute each start's xFIP/SIERA with the published
# formulas, then ANCHOR each pitcher so their season aggregate matches FG's
# season value (from data/_fg_pitch_model.json). The league constants below get
# absorbed by that per-pitcher offset, so their exact values don't matter — the
# anchor fixes the level while our per-start math supplies the recent-form shape.
LG_HR_PER_FB = 0.116   # league HR/FB (absorbed by anchor)
FIP_CONSTANT = 3.17    # FIP constant   (absorbed by anchor)


def _raw_xfip(c, outs):
    if not c or not outs:
        return None
    ip = outs / 3.0
    if ip <= 0:
        return None
    return (13.0 * (c["FB"] * LG_HR_PER_FB) + 3.0 * (c["BB"] + c["HBP"])
            - 2.0 * c["K"]) / ip + FIP_CONSTANT


def _raw_siera(c):
    if not c or not c.get("PA"):
        return None
    PA = c["PA"]
    so, bb = c["K"] / PA, c["BB"] / PA
    ng = (c["GB"] - c["FB"] - c["PU"]) / PA
    ng_sq = (-6.664 * ng * ng) if ng > 0 else (6.664 * ng * ng)
    return (6.145 - 16.986 * so + 11.434 * bb - 1.858 * ng
            + 7.653 * so * so + ng_sq + 10.130 * so * ng - 5.195 * bb * ng)


def apply_self_metrics(starts, fg_season):
    """Fill each start's xfip/siera from Statcast components, anchored so the
    season aggregate matches FG's committed season xFIP/SIERA (fg_season may be
    None — then we ship the raw self-computed values uncalibrated)."""
    comps = [(s, s.get("_comp")) for s in starts]
    usable = [(s, c) for s, c in comps if c and s.get("outs")]
    if not usable:
        return
    # Per-start raw values, then anchor offset = FG season minus the IP-weighted
    # season aggregate of those raw values. Because the offset is additive and the
    # aggregate is IP-weighted-linear, shifting every start by the offset makes the
    # season aggregate equal FG exactly (for both xFIP and the non-linear SIERA).
    raws = []
    for s_, c in usable:
        raws.append((s_, _raw_xfip(c, s_.get("outs")), _raw_siera(c)))
    def _ipw(idx):
        num = sum((v[idx] or 0) * (v[0].get("outs") or 0) for v in raws if v[idx] is not None)
        den = sum((v[0].get("outs") or 0) for v in raws if v[idx] is not None)
        return (num / den) if den else None
    season_raw_x, season_raw_s = _ipw(1), _ipw(2)
    off_x = off_s = 0.0
    if fg_season:
        if fg_season.get("xfip") is not None and season_raw_x is not None:
            off_x = fg_season["xfip"] - season_raw_x
        if fg_season.get("siera") is not None and season_raw_s is not None:
            off_s = fg_season["siera"] - season_raw_s
    for s_, rx, rs in raws:
        if rx is not None:
            s_["xfip"] = round(rx + off_x, 2)
        if rs is not None:
            s_["siera"] = round(rs + off_s, 2)


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
        num = sum((s.get(field) or 0) * (s["pitches"] or 0)
                  for s in starts if s.get(field) is not None and s["pitches"])
        den = sum((s["pitches"] or 0)
                  for s in starts if s.get(field) is not None and s["pitches"])
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
        "csw": pitch_weighted("csw", 3),
        "whiff": pitch_weighted("whiff", 3),
        "ball_pct": pitch_weighted("ball_pct", 3),
        "fb_spin": pitch_weighted("fb_spin", 0),
    }


def backfill_pitcher_stats(pitchers):
    """Write fresh FanGraphs xFIP / Stuff+ / SIERA / IP from these live game logs back
    into data/pitcher_stats.json, OVERRIDING the season values that came from the
    _fg_pitch_model.json browser dump.

    Why: FanGraphs' pitch-modeling leaderboard API is 403 server-side, so xFIP/Stuff+/
    Pitching+/Location+ are sourced from a MANUALLY captured browser dump that does not
    auto-refresh — it goes stale (e.g. frozen weeks back), so pitchers who debuted,
    returned from injury, or crossed the qual threshold afterward show NO xFIP/Stuff+,
    and even covered pitchers show stale values. These per-start game logs ARE pulled
    live every night and cover the full starter universe, so they're the freshest
    comprehensive source. We aggregate them (IP-weighted xFIP/SIERA, pitch-weighted
    Stuff+) and write them over the dump values. Pitching+/Location+ stay dump-only
    (the game-log feed doesn't expose them). Relievers (no starts) keep dump values.
    Runs before compute_pitcher_score so the unified score uses fresh numbers."""
    ps_path = os.path.join(DATA, "pitcher_stats.json")
    try:
        with open(ps_path) as f:
            doc = json.load(f)
    except Exception as e:
        print(f"[gamelogs] backfill skipped (pitcher_stats.json unreadable: {e})",
              file=sys.stderr)
        return
    ps = doc.get("pitchers")
    if not isinstance(ps, dict):
        print("[gamelogs] backfill skipped (no pitchers dict)", file=sys.stderr)
        return
    updated = created = 0
    for key, g in pitchers.items():
        if g.get("source") != "fangraphs":
            continue  # only trust live FG per-start data, not MLB line-score fallback
        season = g.get("season") or {}
        xfip, stuff, siera = season.get("xfip"), season.get("stuff"), season.get("siera")
        ip_outs = season.get("ip_outs")
        if xfip is None and stuff is None:
            continue
        rec = ps.get(key)
        if rec is None:
            rec = {"mlbam_id": g.get("mlbam_id"), "hand": None}
            ps[key] = rec
            created += 1
        else:
            updated += 1
        if xfip is not None:
            rec["xfip"] = xfip
        if stuff is not None:
            rec["stuff_plus"] = stuff
        if siera is not None and rec.get("siera") is None:
            rec["siera"] = siera
        if ip_outs:
            rec["ip"] = round(ip_outs / 3, 1)
        rec["fg_live_source"] = "gamelogs"
    doc["pitchers"] = ps
    doc["gamelogs_backfill_at"] = datetime.datetime.utcnow().isoformat() + "Z"
    doc["gamelogs_backfill_count"] = updated + created
    with open(ps_path, "w") as f:
        json.dump(doc, f, indent=2)
    print(f"[gamelogs] backfilled pitcher_stats: {updated} updated + {created} created "
          f"with fresh FG xFIP/Stuff+/SIERA (override stale dump)", file=sys.stderr)



def _backfill_fg_l5(pitchers):
    """FanGraphs' per-start game-log endpoint is 403 server-side, which leaves
    l5.xfip / l5.siera / l5.stuff null (per-start sparkline source is gone).
    Fill the *rolling headline* values from the committed browser dumps:
    data/_fg_roll.json (last-30-day xFIP/SIERA recent form) and
    data/_fg_pitch_model.json (season Stuff+). Per-start bars stay empty, but the
    SP rolling values (and l5 SIERA) display again and feed the unified score."""
    def _load(name):
        path = os.path.join(DATA, name)
        if not os.path.exists(path):
            return {}
        try:
            return (json.load(open(path)) or {}).get("pitchers") or {}
        except Exception as e:
            print(f"[gamelogs] backfill load {name} failed: {e}", file=sys.stderr)
            return {}
    roll = _load("_fg_roll.json")
    pm = _load("_fg_pitch_model.json")
    if not roll and not pm:
        return 0
    n = 0
    for key, v in pitchers.items():
        if not isinstance(v, dict):
            continue
        nk = norm_name(v.get("name") or "") or key
        l5 = v.get("l5") or {}
        r = roll.get(key) or roll.get(nk)
        m = pm.get(key) or pm.get(nk)
        touched = False
        if r:
            if l5.get("xfip") is None and r.get("xfip") is not None:
                l5["xfip"] = r["xfip"]; touched = True
            if l5.get("siera") is None and r.get("siera") is not None:
                l5["siera"] = r["siera"]; touched = True
        if m and l5.get("stuff") is None and m.get("stuff_plus") is not None:
            l5["stuff"] = m["stuff_plus"]; touched = True
        if touched:
            v["l5"] = l5
            v["l5_fg_backfilled"] = True
            n += 1
    print(f"[gamelogs] FG l5 backfill: filled {n} pitchers from dumps",
          file=sys.stderr)
    return n


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

    # FG season anchors for self-computed xFIP/SIERA
    _pm_path = os.path.join(DATA, "_fg_pitch_model.json")
    try:
        _pm_anchor = (json.load(open(_pm_path)) or {}).get("pitchers") or {} \
            if os.path.exists(_pm_path) else {}
    except Exception:
        _pm_anchor = {}

    pitchers = {}
    fg_ok = fg_fail = mlb_fb = 0
    for i, (mlbam_id, full_name) in enumerate(universe):
        starts = None
        full = None
        fg_id = fg_map.get(mlbam_id)
        if fg_id:
            starts = fg_starts(fg_id, season)
            # FG's game-log endpoint (type=1) returns starts only. Pull the
            # MLB Stats API gameLog for ALL appearances, then merge in any
            # relief outings FG missed. Keyed by date.
            try:
                full = mlb_starts_fallback(mlbam_id, season)
                if full:
                    fg_dates = {s["date"] for s in (starts or [])}
                    extras = [s for s in full if s.get("date") and s["date"] not in fg_dates]
                    if extras:
                        starts = (starts or []) + extras
                        starts.sort(key=lambda s: s.get("date") or "")
            except Exception:
                pass
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
        # Opponent -> 3-letter abbr with @ for away. Home/away comes from the MLB
        # gameLog (isHome), fetched for every arm regardless of primary source.
        _home = {}
        for _src in (full, starts):
            if not _src:
                continue
            for _s in _src:
                if _s.get("date") and _s.get("is_home") is not None:
                    _home[_s["date"]] = _s["is_home"]
        for _s in starts:
            ih = _s.get("is_home")
            if ih is None:
                ih = _home.get(_s.get("date"))
            _s["opp"] = fmt_opp(_s.get("opp"), ih)
            _s.pop("is_home", None)
        # Enrich each start with Statcast CSW% / whiff% / ball% / pitch mix
        # (one Savant call per arm; degrades to nulls if Savant is unavailable).
        merge_discipline(starts, statcast_discipline(mlbam_id, season))
        time.sleep(THROTTLE_S)
        key = norm_name(full_name)
        # Self-compute xFIP/SIERA per start (FG game-log endpoint is blocked),
        # anchored to FG's season value for this pitcher.
        apply_self_metrics(starts, _pm_anchor.get(key))
        for _s in starts:
            _s.pop("_comp", None)
        rolling_window = _select_rolling_window(starts)
        pitchers[key] = {
            "name": full_name,
            "mlbam_id": mlbam_id,
            "fg_id": fg_id,
            "source": src,
            "starts": starts,
            # `l5` keeps its key (so compute_pitcher_score + the frontend keep
            # working) but is now IP-anchored, not appearance-anchored. For SPs
            # this is ≈5 starts; for RPs it's whatever many appearances clear
            # ~20 IP (typical RP rolling window is 15-25 appearances).
            "l5": aggregate(rolling_window),
            "l5_n": len(rolling_window),
            "l5_ip_outs": sum((s.get("outs") or 0) for s in rolling_window),
            "season": aggregate(starts),
        }
        if (i + 1) % 25 == 0:
            print(f"[gamelogs]   {i+1}/{len(universe)} processed "
                  f"(fg={fg_ok}, mlb_fb={mlb_fb}, fail={fg_fail})", file=sys.stderr)

    _backfill_fg_l5(pitchers)
    payload = {
        "generated_at": datetime.datetime.utcnow().isoformat() + "Z",
        "season": season,
        "source": "FanGraphs game logs (server-side); MLB Stats API universe + fallback",
        "count": len(pitchers),
        "coverage": {"universe": len(universe), "fg_matched": len(fg_map),
                     "fg_ok": fg_ok, "mlb_fallback": mlb_fb, "failed": fg_fail},
        "lookback_l5": LOOKBACK_L5,
        "rolling_policy": {
            "min_ip": LOOKBACK_MIN_IP,
            "min_appearances": LOOKBACK_MIN_APP,
            "max_appearances": LOOKBACK_MAX_APP,
            "note": ("IP-anchored rolling window: SPs ≈ 5 starts, RPs grow "
                     "to whatever appearance count clears the IP floor."),
        },
        "pitchers": pitchers,
    }
    os.makedirs(DATA, exist_ok=True)
    with open(OUTPUT, "w") as f:
        json.dump(payload, f, indent=2)
    print(f"[gamelogs] wrote {len(pitchers)} pitchers → {OUTPUT} "
          f"(fg_ok={fg_ok}, mlb_fb={mlb_fb}, fail={fg_fail})", file=sys.stderr)

    # Push fresh FG xFIP/Stuff+/SIERA into pitcher_stats.json (override stale dump).
    backfill_pitcher_stats(pitchers)


if __name__ == "__main__":
    main()
