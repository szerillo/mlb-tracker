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
        bip = g[g["type"] == "X"]
        nbb = len(bip)
        barrels = int((bip["launch_speed_angle"] == 6).sum()) if "launch_speed_angle" in bip else 0
        opp = None
        if "home_team" in g and "away_team" in g and "inning_topbot" in g:
            # batter's team is on offense; opponent is the fielding side
            tb = str(g["inning_topbot"].iloc[0])
            opp = str(g["home_team"].iloc[0]) if tb == "Top" else str(g["away_team"].iloc[0])
        games.append({
            "date": str(gd)[:10], "opp": opp, "pa": pa,
            "k": int(ev.isin(_K).sum()), "bb": int(ev.isin(_BB).sum()),
            "swings": swings, "whiffs": whiffs, "bip": nbb, "barrels": barrels,
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
    return {"n": len(games), "pa": spa,
            "k_pct": _ratio(sk, spa), "bb_pct": _ratio(sbb, spa),
            "whiff": _ratio(swh, ssw), "barrel": _ratio(sba, sbip)}


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

    try:
        sys.path.insert(0, HERE)
        from _common import skip_if_not_in_window
        if skip_if_not_in_window("refresh_hitter_gamelogs", overnight_only=True):
            return
    except Exception:
        pass

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
        games = per_game(df)
        if not games:
            time.sleep(THROTTLE_S)
            continue
        season_agg = agg(games)
        # season xwOBA (batted-ball quality) for context
        bip = df[df["type"] == "X"]
        xw = bip["estimated_woba_using_speedangle"].dropna() if len(bip) else []
        if season_agg:
            season_agg["xwoba"] = round(float(xw.mean()), 3) if len(xw) else None
        hitters[norm_name(name)] = {
            "name": name, "mlbam_id": mid,
            "games": [{k: g[k] for k in ("date", "opp", "pa", "k", "bb", "swings", "whiffs", "bip", "barrels", "k_pct", "bb_pct", "whiff", "barrel")} for g in games],
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
        "source": "Statcast (pybaseball statcast_batter)",
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
