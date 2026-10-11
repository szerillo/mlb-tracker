"""Regular season + postseason blending for MLB Stats API pulls (Sean 10/11: full blend).

StatsAPI returns ONE split per game type when gameType lists several (R, F, D, L, W), and the
rate fields on each split (avg / obp / slg / ops / era / whip) cover that game type only.
sum_stats() adds the counting fields and rebuilds the rates, so callers get one combined line.
"""
GAME_TYPES = "R,F,D,L,W"          # regular season, Wild Card, Division Series, LCS, World Series
PS_TYPES = {"F", "D", "L", "W"}

_RATE_KEYS = {"avg", "obp", "slg", "ops", "era", "whip", "babip", "strikePercentage", "winPercentage",
              "groundOutsToAirouts", "pitchesPerInning", "strikeoutWalkRatio", "strikeoutsPer9Inn",
              "walksPer9Inn", "hitsPer9Inn", "runsScoredPer9", "homeRunsPer9", "stolenBasePercentage",
              "atBatsPerHomeRun", "inningsPitched", "outs"}


def _ip_to_outs(v):
    try:
        w, _, f = str(v).partition(".")
        return int(w or 0) * 3 + int(f or 0)
    except Exception:
        return 0


def _r3(x):
    return None if x is None else f"{x:.3f}".lstrip("0") if x < 1 else f"{x:.3f}"


def sum_stats(stats):
    """Combine StatsAPI stat dicts (one per game type) into a single line."""
    stats = [s for s in stats if s]
    if not stats:
        return None
    if len(stats) == 1:
        return dict(stats[0])
    out = {}
    for s in stats:
        for k, v in s.items():
            if k in _RATE_KEYS:
                continue
            if isinstance(v, bool):
                out.setdefault(k, v)
            elif isinstance(v, (int, float)):
                out[k] = out.get(k, 0) + v
            else:
                out.setdefault(k, v)
    outs = sum(_ip_to_outs(s.get("inningsPitched")) for s in stats)
    if any("inningsPitched" in s for s in stats):
        out["outs"] = outs
        out["inningsPitched"] = f"{outs // 3}.{outs % 3}"
    g = lambda k: out.get(k) or 0
    ab, h = g("atBats"), g("hits")
    tb = g("totalBases") or (h + g("doubles") + 2 * g("triples") + 3 * g("homeRuns"))
    obp_den = ab + g("baseOnBalls") + g("hitByPitch") + g("sacFlies")
    if ab:
        out["avg"] = _r3(h / ab)
        out["slg"] = _r3(tb / ab)
    if obp_den:
        out["obp"] = _r3((h + g("baseOnBalls") + g("hitByPitch")) / obp_den)
    if ab and obp_den:
        out["ops"] = _r3((h + g("baseOnBalls") + g("hitByPitch")) / obp_den + tb / ab)
    if outs:
        ip = outs / 3
        if "earnedRuns" in out:
            out["era"] = f"{9 * g('earnedRuns') / ip:.2f}"
        out["whip"] = f"{(g('baseOnBalls') + h) / ip:.2f}"
        out["strikeoutsPer9Inn"] = f"{9 * g('strikeOuts') / ip:.2f}"
        out["walksPer9Inn"] = f"{9 * g('baseOnBalls') / ip:.2f}"
        out["homeRunsPer9"] = f"{9 * g('homeRuns') / ip:.2f}"
    return out


def combine_splits(splits, key=lambda s: None):
    """[split...] -> {key: combined stat}. Within one (key, game type): duplicate rows count once,
    and a traded player's total row (no team) is used instead of his per-team rows."""
    by = {}
    for s in splits or []:
        by.setdefault((key(s), s.get("gameType")), []).append(s)
    groups = {}
    for (k, _gt), rows in by.items():
        tot = [r for r in rows if not (r.get("team") or {}).get("id")]
        if tot:
            pick = [tot[0]]
        else:
            seen, pick = set(), []
            for r in rows:
                t = r["team"]["id"]
                if t not in seen:
                    seen.add(t); pick.append(r)
        groups.setdefault(k, []).extend(r.get("stat") or {} for r in pick)
    return {k: sum_stats(v) for k, v in groups.items()}
