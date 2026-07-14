#!/usr/bin/env python3
"""
"Sean" team projections — player-data-driven ROS wins + season/playoff Monte
Carlo. Built 2026-07-14 per user spec.

Idea: the public systems project full-roster depth into October. This model
builds team strength bottom-up from OUR player-level data, layers in manual
trade-deadline direction (buyers/sellers, IL returns, prospect call-ups), and
— crucially — switches to a CONSOLIDATED roster for the playoff sim: top 3-4
SP, top 6-7 RP, best 9-10 hitters. Star-heavy teams with front-loaded
rotations (e.g. MIL) should grade out better in October than depth-driven
systems say; depth-reliant teams worse.

Inputs
  data/player_war_projections.json   per-player YTD + ROS-blend WAR/PA/IP
                                     (FG WAR: DEF + BSR already included)
  data/bullpens_rr.json              per-team pen depth chart (mlbamid, role)
  data/deadline_adjustments.json     manual buyers/sellers layer (optional):
                                     { "teams": { "TB": {"ros_war_adj": 1.0,
                                       "note": "buyers"}, ... } }
                                     ros_war_adj adds to ROS team WAR;
                                     playoff_war_adj (optional) overrides the
                                     adjustment for the playoff-strength calc.
  MLB statsapi                       live standings + remaining schedule
                                     (accessible from GH runners; FG is not)

Method
  ROS strength      team ROS WAR = Σ hitter ros.blend.war (playing-time
                    weighted by the blend itself) + Σ pitcher ros.blend.war,
                    players keyed to teams via team_abbr (includes IL players
                    projected to return + prospects with projected PT).
                    ROS wins = 0.294 · G_rem + ROS WAR  (replacement baseline)
  Season sim        N sims of every remaining game; per-game win prob = log5
                    of the two teams' ROS true-talent win% + 3.5% home edge.
                    Tracks division titles, wild cards, seeds, first-round
                    byes (seeds 1-2 per league).
  Playoff sim       team strength REBUILT from consolidated roster rates:
                      lineup  = top 9 hitters by ROS WAR-rate (min 60 ROS PA)
                      rotation= top 4 SP by ROS WAR-rate, weights .38/.28/.20/.14
                      pen     = top 7 RP in bullpens_rr order (min 5 ROS IP)
                    Playoff RA blend: 55% rotation / 45% leverage-weighted pen;
                    lineup + pen star/leverage-weighted (top arms & bats).
                    DEF + BSR ride along inside FG WAR (not double-counted).
                    Bracket: WC bo3 (all @ higher seed), DS bo5, CS bo7, WS
                    bo7 with 2-3-2 HFA to better record.
Output
  data/sean_team_projections.json  { teams: {abbr: {wins, losses, ros_war,
    hit_war, sp_war, pen_war, adj, playoff_war_eq, div_pct, wc_pct,
    playoff_pct, bye_pct, ws_app_pct, ws_pct}}, ... }

compute_team_futures.py copies these into the `sean` slot of team_futures.json.
"""
from __future__ import annotations
import datetime, json, random, sys, urllib.request
from functools import lru_cache
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
PWP_FILE  = REPO_ROOT / "data" / "player_war_projections.json"
PEN_FILE  = REPO_ROOT / "data" / "bullpens_rr.json"
ADJ_FILE  = REPO_ROOT / "data" / "deadline_adjustments.json"
OUTPUT    = REPO_ROOT / "data" / "sean_team_projections.json"

N_SIMS   = 4000
HFA      = 0.035          # home-field add-on, per game
REPL_PCT = 0.294          # replacement-level win%
SEASON   = datetime.date.today().year

MLBAM_TO_ABBR = {
    108: "LAA", 109: "ARI", 110: "BAL", 111: "BOS", 112: "CHC", 113: "CIN",
    114: "CLE", 115: "COL", 116: "DET", 117: "HOU", 118: "KC",  119: "LAD",
    120: "WSH", 121: "NYM", 133: "ATH", 134: "PIT", 135: "SD",  136: "SEA",
    137: "SF",  138: "STL", 139: "TB",  140: "TEX", 141: "TOR", 142: "MIN",
    143: "PHI", 144: "ATL", 145: "CWS", 146: "MIA", 147: "NYY", 158: "MIL",
}
NICK_TO_ABBR = {
    "Yankees": "NYY", "Red Sox": "BOS", "Blue Jays": "TOR", "Rays": "TB",
    "Orioles": "BAL", "Guardians": "CLE", "Twins": "MIN", "Tigers": "DET",
    "Royals": "KC", "White Sox": "CWS", "Astros": "HOU", "Mariners": "SEA",
    "Rangers": "TEX", "Angels": "LAA", "Athletics": "ATH", "Braves": "ATL",
    "Mets": "NYM", "Phillies": "PHI", "Nationals": "WSH", "Marlins": "MIA",
    "Brewers": "MIL", "Cubs": "CHC", "Reds": "CIN", "Cardinals": "STL",
    "Pirates": "PIT", "Dodgers": "LAD", "Padres": "SD", "Giants": "SF",
    "Diamondbacks": "ARI", "Rockies": "COL",
}
LEAGUE_DIV = {
    "NYY": ("AL","E"), "BOS": ("AL","E"), "TOR": ("AL","E"), "TB": ("AL","E"), "BAL": ("AL","E"),
    "CLE": ("AL","C"), "MIN": ("AL","C"), "DET": ("AL","C"), "KC": ("AL","C"), "CWS": ("AL","C"),
    "HOU": ("AL","W"), "SEA": ("AL","W"), "TEX": ("AL","W"), "LAA": ("AL","W"), "ATH": ("AL","W"),
    "ATL": ("NL","E"), "NYM": ("NL","E"), "PHI": ("NL","E"), "WSH": ("NL","E"), "MIA": ("NL","E"),
    "MIL": ("NL","C"), "CHC": ("NL","C"), "CIN": ("NL","C"), "STL": ("NL","C"), "PIT": ("NL","C"),
    "LAD": ("NL","W"), "SD": ("NL","W"), "SF": ("NL","W"), "ARI": ("NL","W"), "COL": ("NL","W"),
}


def _get_json(url):
    req = urllib.request.Request(url, headers={"User-Agent": "mlb-tracker/1.0"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read().decode())


def fetch_standings():
    d = _get_json(f"https://statsapi.mlb.com/api/v1/standings?leagueId=103,104&season={SEASON}")
    out = {}
    for rec in d.get("records", []):
        for tr in rec.get("teamRecords", []):
            abbr = MLBAM_TO_ABBR.get(tr["team"]["id"])
            if abbr:
                out[abbr] = {"w": tr["wins"], "l": tr["losses"]}
    return out


def fetch_remaining_schedule():
    today = datetime.date.today().isoformat()
    d = _get_json(f"https://statsapi.mlb.com/api/v1/schedule?sportId=1&gameType=R"
                  f"&startDate={today}&endDate={SEASON}-10-15")
    games = []
    for day in d.get("dates", []):
        for g in day.get("games", []):
            if g.get("status", {}).get("abstractGameState") == "Final":
                continue  # already reflected in standings
            h = MLBAM_TO_ABBR.get(g["teams"]["home"]["team"]["id"])
            a = MLBAM_TO_ABBR.get(g["teams"]["away"]["team"]["id"])
            if h and a:
                games.append((h, a))
    return games


def _rate(war, pt, per):
    if not war or not pt or pt <= 0:
        return 0.0
    return war / pt * per


def build_strengths(pwp, pen_data, adj):
    pen_ids = {}
    for nick, arr in (pen_data.get("teams") or {}).items():
        ab = NICK_TO_ABBR.get(nick)
        if ab:
            pen_ids[ab] = [r.get("mlbamid") for r in arr if r.get("mlbamid")]

    hit_war, hitters_by_team = {}, {}
    for p in pwp["hitters"].values():
        ab = p.get("team_abbr")
        if ab not in LEAGUE_DIV:
            continue
        blend = ((p.get("ros") or {}).get("blend")) or {}
        w, pa = blend.get("war") or 0.0, blend.get("pa") or 0.0
        hit_war[ab] = hit_war.get(ab, 0.0) + w
        hitters_by_team.setdefault(ab, []).append((w, pa))

    sp_war, pen_war = {}, {}
    sp_by_team, rp_by_team = {}, {}
    for p in pwp["pitchers"].values():
        ab = p.get("team_abbr")
        if ab not in LEAGUE_DIV:
            continue
        blend = ((p.get("ros") or {}).get("blend")) or {}
        w, ip = blend.get("war") or 0.0, blend.get("ip") or 0.0
        mid = p.get("mlbam_id")
        if mid in set(pen_ids.get(ab, [])):
            pen_war[ab] = pen_war.get(ab, 0.0) + w
            rp_by_team.setdefault(ab, []).append((w, ip, mid))
        else:
            sp_war[ab] = sp_war.get(ab, 0.0) + w
            sp_by_team.setdefault(ab, []).append((w, ip))

    teams = {}
    for ab in LEAGUE_DIV:
        a = (adj.get(ab) or {})
        ros_war = (hit_war.get(ab, 0.0) + sp_war.get(ab, 0.0) + pen_war.get(ab, 0.0)
                   + (a.get("ros_war_adj") or 0.0))

        hs = [(w, pa) for w, pa in hitters_by_team.get(ab, []) if pa >= 60]
        hs.sort(key=lambda x: _rate(x[0], x[1], 600), reverse=True)
        # 2026-07-14: star-weighted lineup — top of the order soaks up playoff
        # PA; weights sum to 9 lineup-slots-equivalent
        H_WTS = [1.25, 1.20, 1.15, 1.10, 1.00, 0.90, 0.85, 0.80, 0.75]
        lineup_eq = sum(_rate(w, pa, 620) * hw for (w, pa), hw in zip(hs[:9], H_WTS))

        ss = [(w, ip) for w, ip in sp_by_team.get(ab, []) if ip >= 25]
        ss.sort(key=lambda x: _rate(x[0], x[1], 180), reverse=True)
        wts = [0.38, 0.28, 0.20, 0.14]  # 2026-07-14: pushed top-heavier (ace matters most in October)
        rot_eq = sum(_rate(w, ip, 180) * 4 * wt for (w, ip), wt in zip(ss[:4], wts))

        order = {m: i for i, m in enumerate(pen_ids.get(ab, []))}
        rr = sorted([(w, ip, m) for w, ip, m in rp_by_team.get(ab, []) if ip >= 5],
                    key=lambda x: order.get(x[2], 99))
        # 2026-07-14: leverage-weighted pen — top 3 arms throw the innings
        # that decide October games; weights sum to 7 slots-equivalent
        P_WTS = [1.55, 1.35, 1.15, 0.90, 0.75, 0.70, 0.60]
        pen_eq = sum(_rate(w, ip, 65) * pw for (w, ip, _), pw in zip(rr[:7], P_WTS))

        playoff_war = (lineup_eq + 0.55 * rot_eq + 0.45 * pen_eq
                       + (a.get("playoff_war_adj") if a.get("playoff_war_adj") is not None
                          else (a.get("ros_war_adj") or 0.0)))

        teams[ab] = {
            "hit_war": round(hit_war.get(ab, 0.0), 2),
            "sp_war": round(sp_war.get(ab, 0.0), 2),
            "pen_war": round(pen_war.get(ab, 0.0), 2),
            "adj": a.get("ros_war_adj") or 0.0,
            "adj_note": a.get("note"),
            "ros_war": round(ros_war, 2),
            "playoff_war_eq": round(playoff_war, 2),
        }
    return teams


def log5(pa, pb):
    d = pa * (1 - pb) + pb * (1 - pa)
    return pa * (1 - pb) / d if d > 0 else 0.5


def series_win(p_neutral, n, hha):
    """P(higher seed wins best-of-n); hha = home flags for higher seed."""
    need = n // 2 + 1
    @lru_cache(maxsize=None)
    def f(wh, wl):
        if wh == need: return 1.0
        if wl == need: return 0.0
        g = wh + wl
        p = p_neutral + (HFA if hha[g] else -HFA)
        p = min(max(p, 0.02), 0.98)
        return p * f(wh + 1, wl) + (1 - p) * f(wh, wl + 1)
    return f(0, 0)


def main():
    if not PWP_FILE.exists() or not PEN_FILE.exists():
        print("[sean-proj] missing inputs; keeping previous output", file=sys.stderr)
        return 0 if OUTPUT.exists() else 1
    pwp = json.loads(PWP_FILE.read_text())
    pen = json.loads(PEN_FILE.read_text())
    adj = {}
    if ADJ_FILE.exists():
        try:
            adj = json.loads(ADJ_FILE.read_text()).get("teams", {}) or {}
        except Exception:
            adj = {}

    try:
        standings = fetch_standings()
        games = fetch_remaining_schedule()
    except Exception as e:
        print(f"[sean-proj] statsapi failed: {e}; keeping previous output", file=sys.stderr)
        return 0 if OUTPUT.exists() else 1
    if len(standings) < 30 or not games:
        print("[sean-proj] incomplete statsapi data; keeping previous output", file=sys.stderr)
        return 0 if OUTPUT.exists() else 1

    strengths = build_strengths(pwp, pen, adj)

    g_rem = {ab: 0 for ab in LEAGUE_DIV}
    for h, a in games:
        g_rem[h] += 1; g_rem[a] += 1
    talent = {}
    for ab, s in strengths.items():
        gr = max(g_rem[ab], 1)
        ros_wins = REPL_PCT * gr + s["ros_war"]
        talent[ab] = min(max(ros_wins / gr, 0.30), 0.70)
        s["ros_wins_talent"] = round(ros_wins, 1)

    po_talent = {ab: min(max((REPL_PCT * 162 + s["playoff_war_eq"]) / 162, 0.32), 0.72)
                 for ab, s in strengths.items()}

    counts = {ab: {"div": 0, "wc": 0, "po": 0, "bye": 0, "ws_app": 0, "ws": 0,
                   "wins_sum": 0.0} for ab in LEAGUE_DIV}
    rng = random.Random(20260714)
    lg_teams = {lg: [ab for ab, (l, _) in LEAGUE_DIV.items() if l == lg]
                for lg in ("AL", "NL")}
    for _ in range(N_SIMS):
        w = {ab: standings[ab]["w"] for ab in LEAGUE_DIV}
        for h, a in games:
            p = log5(talent[h], talent[a]) + HFA
            if rng.random() < p: w[h] += 1
            else:                w[a] += 1
        for ab in LEAGUE_DIV:
            counts[ab]["wins_sum"] += w[ab]
        finalists = {}
        for lg in ("AL", "NL"):
            divs = {}
            for ab in lg_teams[lg]:
                divs.setdefault(LEAGUE_DIV[ab][1], []).append(ab)
            champs = []
            for dv, abs_ in divs.items():
                abs_.sort(key=lambda x: (w[x], rng.random()), reverse=True)
                champs.append(abs_[0])
            champs.sort(key=lambda x: (w[x], rng.random()), reverse=True)
            rest = [ab for ab in lg_teams[lg] if ab not in champs]
            rest.sort(key=lambda x: (w[x], rng.random()), reverse=True)
            wcs = rest[:3]
            seeds = champs + wcs
            for ab in champs: counts[ab]["div"] += 1
            for ab in wcs:    counts[ab]["wc"] += 1
            for ab in seeds:  counts[ab]["po"] += 1
            for ab in seeds[:2]: counts[ab]["bye"] += 1

            def duel(hi, lo, n, hha):
                p = log5(po_talent[hi], po_talent[lo])
                return hi if rng.random() < series_win(p, n, tuple(hha)) else lo
            wc1 = duel(seeds[2], seeds[5], 3, (1, 1, 1))
            wc2 = duel(seeds[3], seeds[4], 3, (1, 1, 1))
            ds1 = duel(seeds[0], wc2, 5, (1, 1, 0, 0, 1))
            ds2 = duel(seeds[1], wc1, 5, (1, 1, 0, 0, 1))
            hi, lo = (ds1, ds2) if (w[ds1], rng.random()) >= (w[ds2], rng.random()) else (ds2, ds1)
            finalists[lg] = duel(hi, lo, 7, (1, 1, 0, 0, 0, 1, 1))
        al, nl = finalists["AL"], finalists["NL"]
        counts[al]["ws_app"] += 1; counts[nl]["ws_app"] += 1
        hi, lo = (al, nl) if (w[al], rng.random()) >= (w[nl], rng.random()) else (nl, al)
        p = log5(po_talent[hi], po_talent[lo])
        champ = hi if rng.random() < series_win(p, 7, (1, 1, 0, 0, 0, 1, 1)) else lo
        counts[champ]["ws"] += 1

    teams_out = {}
    for ab in LEAGUE_DIV:
        c = counts[ab]; s = strengths[ab]
        exp_w = c["wins_sum"] / N_SIMS
        teams_out[ab] = {
            "abbr": ab,
            "wins": round(exp_w, 1),
            "losses": round(162 - exp_w, 1),
            "div_pct": round(100 * c["div"] / N_SIMS, 1),
            "wc_pct": round(100 * c["wc"] / N_SIMS, 1),
            "playoff_pct": round(100 * c["po"] / N_SIMS, 1),
            "bye_pct": round(100 * c["bye"] / N_SIMS, 1),
            "ws_app_pct": round(100 * c["ws_app"] / N_SIMS, 1),
            "ws_pct": round(100 * c["ws"] / N_SIMS, 1),
            **s,
        }

    payload = {
        "generated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds"),
        "season": SEASON,
        "n_sims": N_SIMS,
        "method": ("player-level ROS WAR (depth) -> season sim; consolidated "
                   "star-weighted top-9 hitters / top-heavy top-4 SP (55%) / leverage-weighted top-7 RP (45%) -> playoff sim; "
                   "manual deadline_adjustments.json layer"),
        "teams": teams_out,
    }
    OUTPUT.write_text(json.dumps(payload, separators=(",", ":")))
    print(f"[sean-proj] wrote {OUTPUT} ({len(teams_out)} teams, {N_SIMS} sims)", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
