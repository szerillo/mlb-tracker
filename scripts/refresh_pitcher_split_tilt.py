#!/usr/bin/env python3
"""Per-game pitcher-split F5 tilt (Fable 2026-09-13, half weight).
tilt_team = sum_slot slot_wt * shr*(oppSP_ops_vs_hitterhand - oppSP_ops_overall) / sum slot_wt
shr = TBF_side/(TBF_side+150); zeroed if the opposing SP is an opener.
away_tilt uses the HOME SP vs the AWAY lineup; home_tilt the AWAY SP vs HOME lineup.
Applied downstream as F5_team_runs += 7.5 * tilt (half of Fable's fitted 15.1)."""
import json, os, datetime
REPO = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SPLITS = os.path.join(REPO, "data", "pitcher_splits.json")
LINEUPS = os.path.join(REPO, "data", "lineups.json")
OUTPUT  = os.path.join(REPO, "data", "pitcher_split_tilt.json")
SHR_K = 150.0
F5_SLOT_WT = [2.3, 2.2, 2.1, 2.0, 1.9, 1.7, 1.6, 1.5, 1.4]


def _ops(x):
    try:
        return float(str(x).lstrip("0")) if str(x).startswith("0.") or str(x).startswith(".") else float(x)
    except Exception:
        try:
            return float(str(x))
        except Exception:
            return None


def sp_overall_ops(sp):
    r, l = sp.get("vs_r", {}), sp.get("vs_l", {})
    orr, orl = _ops(r.get("ops_against")), _ops(l.get("ops_against"))
    tr, tl = r.get("tbf") or 0, l.get("tbf") or 0
    if orr is None and orl is None:
        return None
    if orr is None:
        return orl
    if orl is None:
        return orr
    if (tr + tl) <= 0:
        return (orr + orl) / 2
    return (tr * orr + tl * orl) / (tr + tl)


def hitter_side(bats, sp_hand):
    """Which SP-split a hitter faces. Switch hitters bat opposite the pitcher."""
    if bats == "S":
        return "L" if sp_hand == "R" else "R"
    return "R" if bats == "R" else "L"


def tilt(opp_sp, lineup_players):
    if not opp_sp or opp_sp.get("is_opener"):
        return 0.0
    ov = sp_overall_ops(opp_sp)
    if ov is None:
        return 0.0
    hand = opp_sp.get("hand") or "R"
    num = den = 0.0
    for p in lineup_players:
        o = p.get("order")
        if not o or o < 1 or o > 9:
            continue
        side = hitter_side(p.get("bats") or "R", hand)
        vs = opp_sp.get("vs_l" if side == "L" else "vs_r", {})
        o_side = _ops(vs.get("ops_against")); tbf = vs.get("tbf") or 0
        if o_side is None:
            continue
        shr = tbf / (tbf + SHR_K)
        w = F5_SLOT_WT[o - 1]
        num += w * shr * (o_side - ov); den += w
    return round(num / den, 4) if den else 0.0


def main():
    sp = json.load(open(SPLITS)).get("pitchers", {})
    by_game = {}
    for vv in sp.values():
        gp = vv.get("game_pk")
        if gp is None:
            continue
        by_game.setdefault(str(gp), {})[vv.get("team")] = vv
    lu = json.load(open(LINEUPS)).get("games", [])
    out = {}
    for g in lu:
        gp = str(g.get("game_pk")); L = g.get("lineups", {})
        away_pl = (L.get("away") or {}).get("players") or []
        home_pl = (L.get("home") or {}).get("players") or []
        sps = by_game.get(gp, {})
        # "team" is the full team name, identical to the lineup away/home -> exact match.
        away_sp = sps.get(g.get("away"))
        home_sp = sps.get(g.get("home"))
        away_tilt = tilt(home_sp, away_pl)   # away bats vs the HOME sp
        home_tilt = tilt(away_sp, home_pl)   # home bats vs the AWAY sp
        out[gp] = {"matchup": g.get("matchup"),
                   "away_tilt": away_tilt, "home_tilt": home_tilt,
                   "away_sp": (away_sp or {}).get("name"), "home_sp": (home_sp or {}).get("name"),
                   "home_sp_opener": bool((home_sp or {}).get("is_opener")),
                   "away_sp_opener": bool((away_sp or {}).get("is_opener"))}
    payload = {"generated_at": datetime.datetime.utcnow().isoformat() + "Z",
               "note": "F5 pitcher-split tilt; apply F5_team_runs += 7.5*tilt",
               "n_games": len(out), "games": out}
    os.makedirs(os.path.dirname(OUTPUT), exist_ok=True)
    json.dump(payload, open(OUTPUT, "w"), indent=2)
    print(f"[split-tilt] wrote {len(out)} games -> {OUTPUT}")
    return 0


if __name__ == "__main__":
    import sys; sys.exit(main())
