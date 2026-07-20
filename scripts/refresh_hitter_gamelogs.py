#!/usr/bin/env python3
"""
Build data/hitter_gamelogs.json — per-game rolling form + vs-pitch-type splits
for hitters, from Statcast (one statcast_batter call per hitter).

Per hitter:
  games[]  : per-game {date, opp, pa, k_pct, bb_pct, whiff, barrel}
  l10/l5   : windowed aggregates (true ratios over the window)
  season   : season aggregate (+ xwOBA)
  vs_pitch : performance vs Fastball / Breaking / Offspeed
             {pa, woba, whiff, barrel, n_bb}

whiff%  = swinging strikes / swings
barrel% = barrels (launch_speed_angle == 6) / batted balls
K%/BB%  = K or BB / plate appearances
wOBA    = Σ woba_value / Σ woba_denom over PAs ending on that pitch group

Keyed by the SAME normalized name index.html uses (normName), so lineup rows
join by the batter's fullName.

USAGE:
    python scripts/refresh_hitter_gamelogs.py            # writes data/hitter_gamelogs.json
    python scripts/refresh_hitter_gamelogs.py --limit 8  # quick subset (dev)
"""
from __future__ import annotations
import argparse, datetime, json, os, re, sys, time, unicodedata, urllib.request
import warnings
warnings.filterwarnings("ignore")

HERE = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(HERE, "..", "data")
OUTPUT = os.path.join(DATA, "hitter_gamelogs.json")
UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
MIN_PA = 25          # universe floor (covers regulars + most platoon bats)
LOOKBACK_L10 = 10
LOOKBACK_L5 = 5
THROTTLE_S = 0.15

# ── pitch-type groups ───────────────────────────────────────────────────────
FB = {"FF", "FA", "FT", "SI", "FC"}
BR = {"SL", "ST", "CU", "KC", "SV", "CS", "SC"}
OS = {"CH", "FS", "FO", "EP", "KN"}
def pgroup(pt):
    if pt in FB: return "FB"
    if pt in BR: return "BR"
    if pt in OS: return "OS"
    return None

_SWING = {"swinging_strike", "swinging_strike_blocked", "foul", "foul_tip",
          "hit_into_play", "missed_bunt", "foul_bunt", "bunt_foul_tip"}
_WHIFF = {"swinging_strike", "swinging_strike_blocked", "missed_bunt"}
_K = {"strikeout", "strikeout_double_play"}
_BB = {"walk", "intent_walk"}


def norm_name(s: str) -> str:
    if not s:
        return ""
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c)).lower()
    for suf in (" jr.", " jr", " sr.", " sr", " iii", " ii"):
        if s.endswith(suf):
            s = s[: -len(suf)]
    return s.replace(".", "").strip()


def _f(v):
    try:
        v = float(v)
        return v if v == v else None
    except (TypeError, ValueError):
        return None


def http_json(url, timeout=30):
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())


def get_hitter_universe(season: int):
    """Every hitter with >= MIN_PA this season (MLB Stats API)."""
    url = (f"https://statsapi.mlb.com/api/v1/stats?stats=season&season={season}"
           f"&group=hitting&gameType=R&sportId=1&limit=2000&playerPool=all")
    out = []
    try:
        for s in http_json(url)["stats"][0]["splits"]:
            st = s.get("stat", {})
            pa = st.get("plateAppearances") or 0
            p = s.get("player", {})
            if pa >= MIN_PA and p.get("id") and p.get("fullName"):
                out.append((p["id"], p["fullName"]))
    except Exception as e:
        print(f"[hit-gl] universe fetch failed: {e}", file=sys.stderr)
    return out


def _ratio(num, den, pct=True, nd=1):
    if not den:
        return None
    return round(num / den * (100 if pct else 1), nd)


def per_game(df):
    """Group a hitter's Statcast frame into per-game rows + collect totals."""
    import pandas as pd
    games = []
    for gd, g in df.groupby("game_date"):
        ev = g["events"].astype(str)
        pa = int(g["events"].notna().sum())
        if pa == 0:
            continue
        desc = g["description"].astype(str)
        swings = int(desc.isin(_SWING).sum())
        whiffs = int(desc.isin(_WHIFF).sum())
        # Chase (O-Swing): swings at out-of-zone pitches / out-of-zone pitches.
        # Statcast zone 11-14 = the four out-of-zone quadrants (>=10).
        if "zone" in g.columns:
            oz = g[g["zone"].fillna(0) >= 10]
            chase_pit = int(len(oz))
            chase_sw = int(oz["description"].astype(str).isin(_SWING).sum())
        else:
            chase_pit = 0; chase_sw = 0
        bip = g[g["type"] == "X"]
        nbb = len(bip)
        barrels = int((bip["launch_speed_angle"] == 6).sum()) if "launch_speed_angle" in bip else 0
        # Per-PA xwOBA: estimated_woba_using_speedangle for BIP, actual woba_value
        # (the Statcast wOBA constants) for non-BIP K/BB/HBP. Denominator is the
        # sum of woba_denom (PA-ending pitches). Sums (not ratios) are stored so
        # the rolling-window code can compute true ratios over arbitrary windows.
        if "woba_denom" in g.columns:
            pa_rows = g[g["woba_denom"].fillna(0) > 0]
            if "estimated_woba_using_speedangle" in pa_rows.columns:
                xw_num_series = pa_rows["estimated_woba_using_speedangle"].fillna(
                    pa_rows["woba_value"]
                )
            else:
                xw_num_series = pa_rows["woba_value"]
            xwoba_num = float(xw_num_series.dropna().sum())
            xwoba_den = float(pa_rows["woba_denom"].dropna().sum())
        else:
            xwoba_num = 0.0
            xwoba_den = 0.0
        opp = None
        if "home_team" in g and "away_team" in g and "inning_topbot" in g:
            # batter's team is on offense; opponent is the fielding side
            tb = str(g["inning_topbot"].iloc[0])
            opp = str(g["home_team"].iloc[0]) if tb == "Top" else str(g["away_team"].iloc[0])
        games.append({
            "date": str(gd)[:10], "opp": opp, "pa": pa,
            "k": int(ev.isin(_K).sum()), "bb": int(ev.isin(_BB).sum()),
            "swings": swings, "whiffs": whiffs, "bip": nbb, "barrels": barrels,
            "chase_sw": chase_sw, "chase_pit": chase_pit,
            "chase": _ratio(chase_sw, chase_pit),
            "xwoba_num": round(xwoba_num, 5), "xwoba_den": round(xwoba_den, 3),
            "xwoba": round(xwoba_num / xwoba_den, 3) if xwoba_den else None,
            "k_pct": _ratio(int(ev.isin(_K).sum()), pa),
            "bb_pct": _ratio(int(ev.isin(_BB).sum()), pa),
            "whiff": _ratio(whiffs, swings),
            "barrel": _ratio(barrels, nbb),
        })
    games.sort(key=lambda x: x["date"])
    return games


def agg(games):
    if not games:
        return None
    sk = sum(g["k"] for g in games); sbb = sum(g["bb"] for g in games)
    spa = sum(g["pa"] for g in games); ssw = sum(g["swings"] for g in games)
    swh = sum(g["whiffs"] for g in games); sbip = sum(g["bip"] for g in games)
    sba = sum(g["barrels"] for g in games)
    xwn = sum(g.get("xwoba_num") or 0 for g in games)
    xwd = sum(g.get("xwoba_den") or 0 for g in games)
    return {"n": len(games), "pa": spa,
            "k_pct": _ratio(sk, spa), "bb_pct": _ratio(sbb, spa),
            "whiff": _ratio(swh, ssw), "barrel": _ratio(sba, sbip),
            "xwoba_pa": round(xwn / xwd, 3) if xwd else None}


def vs_pitch(df):
    """Performance vs FB / BR / OS over the season."""
    import pandas as pd
    out = {}
    pg = df["pitch_type"].astype(str).map(pgroup)
    for grp in ("FB", "BR", "OS"):
        sub = df[pg == grp]
        if not len(sub):
            continue
        desc = sub["description"].astype(str)
        swings = int(desc.isin(_SWING).sum()); whiffs = int(desc.isin(_WHIFF).sum())
        bip = sub[sub["type"] == "X"]; nbb = len(bip)
        barrels = int((bip["launch_speed_angle"] == 6).sum()) if "launch_speed_angle" in bip else 0
        den = float(sub["woba_denom"].dropna().sum())
        num = float(sub["woba_value"].dropna().sum())
        pa = int(sub["events"].notna().sum())
        out[grp] = {
            "pa": pa, "pitches": int(len(sub)),
            "woba": round(num / den, 3) if den else None,
            "whiff": _ratio(whiffs, swings),
            "barrel": _ratio(barrels, nbb),
        }
    return out


def mlb_gamelog(mid, season):
    """Per-game box-score stats from the MLB Stats API gameLog (regular season).

    Returns (by_date, season_slash):
      by_date[date] = {pa, ab, r, h, hr, rbi, bb, k, sb}  (summed across DH)
      season_slash  = {avg, obp, slg, ops}  (cumulative — from the latest split)

    NB: in gameLog splits the COUNTING stats are per-game while the RATE stats
    (avg/obp/slg/ops) are season-to-date cumulative, so the latest split's slash
    is the season slash.
    """
    url = (f"https://statsapi.mlb.com/api/v1/people/{mid}/stats?stats=gameLog"
           f"&group=hitting&season={season}&gameType=R")
    by_date, slash = {}, {}

    def _i(v):
        try:
            return int(v)
        except (TypeError, ValueError):
            return 0

    def _f3(v):
        try:
            return round(float(v), 3)
        except (TypeError, ValueError):
            return None

    try:
        splits = http_json(url)["stats"][0]["splits"]
    except Exception:
        return by_date, slash
    for s in splits:
        d = s.get("date")
        st = s.get("stat", {})
        if not d:
            continue
        rec = by_date.setdefault(d, {"pa": 0, "ab": 0, "r": 0, "h": 0,
                                     "hr": 0, "rbi": 0, "bb": 0, "k": 0, "sb": 0})
        rec["pa"]  += _i(st.get("plateAppearances"))
        rec["ab"]  += _i(st.get("atBats"))
        rec["r"]   += _i(st.get("runs"))
        rec["h"]   += _i(st.get("hits"))
        rec["hr"]  += _i(st.get("homeRuns"))
        rec["rbi"] += _i(st.get("rbi"))
        rec["bb"]  += _i(st.get("baseOnBalls"))
        rec["k"]   += _i(st.get("strikeOuts"))
        rec["sb"]  += _i(st.get("stolenBases"))
    if splits:
        last = max(splits, key=lambda s: s.get("date") or "").get("stat", {})
        slash = {"avg": _f3(last.get("avg")), "obp": _f3(last.get("obp")),
                 "slg": _f3(last.get("slg")), "ops": _f3(last.get("ops"))}
    return by_date, slash


def merge_box(games, gl_box):
    """Attach MLB box-score stats to each statcast game (matched by date).
    Prefers authoritative gameLog PA/K/BB for the rate denominators when present.
    """
    for g in games:
        b = gl_box.get(g["date"])
        if not b:
            continue
        g["ab"] = b["ab"]; g["r"] = b["r"]; g["h"] = b["h"]
        g["hr"] = b["hr"]; g["rbi"] = b["rbi"]; g["sb"] = b["sb"]
        if b["pa"]:
            g["pa"] = b["pa"]; g["k"] = b["k"]; g["bb"] = b["bb"]
            g["k_pct"] = _ratio(b["k"], b["pa"])
            g["bb_pct"] = _ratio(b["bb"], b["pa"])
    return games


def main():
    from pybaseball import statcast_batter
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--season", type=int,
                    default=int(os.environ.get("FG_SEASON", datetime.date.today().year)))
    args = ap.parse_args()
    season = args.season
    start = f"{season}-03-01"
    end = datetime.date.today().isoformat()

    # No run-window gate: this pull is wired only into the once-daily
    # nightly_stats workflow (mirrors refresh_pitcher_gamelogs.py, which also
    # always runs when invoked), so it should execute whenever that job runs.

    universe = get_hitter_universe(season)
    if args.limit:
        universe = universe[:args.limit]
    print(f"[hit-gl] universe: {len(universe)} hitters (>= {MIN_PA} PA, season {season})",
          file=sys.stderr)

    hitters = {}
    ok = fail = 0
    for i, (mid, name) in enumerate(universe):
        try:
            df = statcast_batter(start, end, mid)
        except Exception as e:
            fail += 1
            continue
        if df is None or len(df) == 0 or "game_date" not in df.columns:
            fail += 1
            time.sleep(THROTTLE_S)
            continue
        # Regular season only — statcast_batter returns spring-training games in
        # the early-March window too, which would pollute season totals + the
        # box-score join (gameLog is regular-season only).
        if "game_type" in df.columns:
            df = df[df["game_type"] == "R"]
            if len(df) == 0:
                fail += 1
                time.sleep(THROTTLE_S)
                continue
        games = per_game(df)
        if not games:
            time.sleep(THROTTLE_S)
            continue
        # Merge official MLB box-score stats (AB/R/H/HR/RBI/SB + slash) per game.
        gl_box, gl_slash = mlb_gamelog(mid, season)
        merge_box(games, gl_box)
        season_agg = agg(games)
        # season xwOBA (batted-ball quality) for context
        bip = df[df["type"] == "X"]
        xw = bip["estimated_woba_using_speedangle"].dropna() if len(bip) else []
        if season_agg:
            season_agg["xwoba"] = round(float(xw.mean()), 3) if len(xw) else None
            if gl_slash:
                season_agg.update(gl_slash)        # avg / obp / slg / ops
            # season box totals (gameLog where present)
            for fld in ("ab", "r", "h", "hr", "rbi", "sb"):
                season_agg[fld] = sum(g.get(fld) or 0 for g in games)
        hitters[norm_name(name)] = {
            "name": name, "mlbam_id": mid,
            "games": [{k: g.get(k) for k in ("date", "opp", "pa", "k", "bb", "swings", "whiffs", "bip", "barrels", "chase_sw", "chase_pit", "chase", "xwoba_num", "xwoba_den", "xwoba", "k_pct", "bb_pct", "whiff", "barrel", "ab", "r", "h", "hr", "rbi", "sb")} for g in games],
            "l5": agg(games[-LOOKBACK_L5:]),
            "l10": agg(games[-LOOKBACK_L10:]),
            "season": season_agg,
            "vs_pitch": vs_pitch(df),
        }
        ok += 1
        time.sleep(THROTTLE_S)
        if (i + 1) % 50 == 0:
            print(f"[hit-gl]   {i+1}/{len(universe)} (ok={ok}, fail={fail})", file=sys.stderr)

    payload = {
        "generated_at": datetime.datetime.utcnow().isoformat() + "Z",
        "season": season,
        "source": "Statcast (pybaseball statcast_batter) + MLB Stats API gameLog box scores",
        "min_pa": MIN_PA, "lookback_l10": LOOKBACK_L10, "lookback_l5": LOOKBACK_L5,
        "count": len(hitters),
        "hitters": hitters,
    }
    os.makedirs(DATA, exist_ok=True)
    with open(OUTPUT, "w") as f:
        json.dump(payload, f, separators=(",", ":"))
    print(f"[hit-gl] wrote {len(hitters)} hitters → {OUTPUT} (ok={ok}, fail={fail})",
          file=sys.stderr)


if __name__ == "__main__":
    main()
