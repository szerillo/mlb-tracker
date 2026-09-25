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
  data/player_war_projections.json   per-player YTD + ROS-blend + EOS WAR/PA/IP
                                     (FG WAR: DEF + BSR already included)
  data/bullpens_rr.json              per-team pen depth chart (mlbamid, role)
  data/deadline_adjustments.json     manual buyers/sellers layer (optional):
                                     { "teams": { "TB": {"ros_war_adj": 1.0,
                                       "note": "buyers"}, ... } }
                                     ros_war_adj adds to ROS team WAR;
                                     playoff_war_adj (optional) overrides the
                                     adjustment for the playoff-strength calc;
                                     playoff_outs (optional) is a list of
                                     mlbam ids or player names that are OUT for
                                     October (season-ending injury etc.) — they
                                     are dropped from the consolidated playoff
                                     roster. Guys who missed regular-season time
                                     but will be BACK for the playoffs need no
                                     flag: their full-season quality carries.
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
  Playoff sim       team strength REBUILT from consolidated roster QUALITY,
                    keyed to each player's FULL-SEASON rate (eos -> ytd -> ros
                    fallback) so a star who missed regular-season time still
                    grades at his established level — the tiny rest-of-season
                    window near the end of the year no longer starves the calc:
                      lineup  = top 9 hitters by full-season WAR-rate
                                (min 150 full-season PA)
                      rotation= top 4 SP by full-season WAR-rate, .38/.28/.20/.14
                                (min 40 full-season IP)
                      pen     = top 7 RP in bullpens_rr order (min 15 IP)
                    Playoff RA blend: 55% rotation / 45% leverage-weighted pen;
                    lineup + pen star/leverage-weighted (top arms & bats).
                    Season-ending outs are removed via deadline playoff_outs.
                    DEF + BSR ride along inside FG WAR (not double-counted).
                    Bracket: WC bo3 (all @ higher seed), DS bo5, CS bo7, WS
                    bo7 with 2-3-2 HFA to better record.
Output
  data/sean_team_projections.json  { teams: {abbr: {wins, losses, ros_war,
    hit_war, sp_war, pen_war, adj, playoff_war_eq, div_pct, wc_pct,
    playoff_pct, bye_pct, reach_ds_pct, reach_cs_pct, ws_app_pct, ws_pct}}, ... }

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

# Full-season eligibility floors for the CONSOLIDATED playoff roster. These are
# season-long (eos/ytd) counting stats, which stay large & stable all year —
# unlike the rest-of-season window, which shrinks to ~1 week of PA/IP by late
# September and used to filter out entire rosters (collapsing playoff strength
# to replacement level for elite, front-loaded teams like LAD/MIL).
PO_MIN_PA   = 150.0       # a real everyday-ish contributor (incl. IL returnees)
PO_MIN_SP_IP = 40.0       # a real starter
PO_MIN_RP_IP = 15.0       # a real reliever
# Steepen the WAR -> win% conversion for the PLAYOFF series. The flat linear map
# compressed the whole field into ~0.45-0.60 and left the elite tier (LAD/MIL/CHC)
# as near coin-flips, so no team could separate. This spreads talent around .500
# by PO_STEEP so elite rosters push toward ~0.66-0.70 and the field re-expands to
# roughly real-MLB true-talent width. 1.95 matches the full-game pythag exponent.
PO_STEEP     = 1.0  # Fable ruling 2026-09-22: 1.0 = identity (plain log5); >1 unsupported by 99-series test

# Fable ruling 2026-09-23: shrink hitter WAR-rate toward the player's own 2024-25
# rate (league mean fallback), k=400 PA. Fixes part-timer rate inflation (regress
# to prior) and injured-star deflation (Acuna/Riley regress UP toward their level).
PRIOR_FILE   = REPO_ROOT / "data" / "bwar_prior_2425.json"
try:
    _prj = json.loads(PRIOR_FILE.read_text())
    PRIOR_RATES = _prj.get("rates", {})   # {mlbam_id: rate620}, pre-filtered to >=200 prior PA
    PRIOR_LM    = _prj.get("league_mean_rate620", 2.2)
except Exception:
    PRIOR_RATES, PRIOR_LM = {}, 2.2
HIT_SHRINK_K = 400          # Fable-fitted shrinkage constant (PA) for hitter WAR-rate

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

# statsapi team ids, keyed by the abbreviations LEAGUE_DIV uses above. Emitted with
# the seeding distribution (bracket_dist.json) so Fable's v2 October engine can price
# each realization by id. AZ/ARI + ATH stay as the app's abbrs here; the v2 runner
# re-labels by its own map on output.
ABBR2ID = {
    "NYY":147,"BOS":111,"TOR":141,"TB":139,"BAL":110,"CLE":114,"MIN":142,"DET":116,
    "KC":118,"CWS":145,"HOU":117,"SEA":136,"TEX":140,"LAA":108,"ATH":133,"ATL":144,
    "NYM":121,"PHI":143,"WSH":120,"MIA":146,"MIL":158,"CHC":112,"CIN":113,"STL":138,
    "PIT":134,"LAD":119,"SD":135,"SF":137,"ARI":109,"COL":115,
}
BRACKET_OUT = REPO_ROOT / "data" / "bracket_dist.json"


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
                out[abbr] = {"w": tr["wins"], "l": tr["losses"],
                             "rs": tr.get("runsScored"), "ra": tr.get("runsAllowed")}
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


# player_war_projections uses FG-style abbrs for a few teams
ABBR_FIX = {"WSN": "WSH", "TBR": "TB", "SDP": "SD", "SFG": "SF", "KCR": "KC",
            "CHW": "CWS", "OAK": "ATH"}


def _team_of(p):
    ab = p.get("team_abbr")
    return ABBR_FIX.get(ab, ab)


def _rate(war, pt, per):
    if not war or not pt or pt <= 0:
        return 0.0
    return war / pt * per

def _shr_hit_rate(war, pt, mid):
    """Hitter WAR-rate per 620 PA, shrunk toward own 2024-25 rate (league mean
    fallback) with k=HIT_SHRINK_K PA. Fable 2026-09-23."""
    obs = _rate(war, pt, 620)
    prior = PRIOR_RATES.get(str(mid), PRIOR_LM)   # own 2024-25 rate, or league mean if no qualified prior
    return (obs * pt + prior * HIT_SHRINK_K) / (pt + HIT_SHRINK_K)


def _fs(p, is_pit):
    """Full-season (war, playing_time) for the playoff-quality calc.

    Prefer eos (YTD + ROS projection), fall back to ytd, then the ros blend.
    Returns the first window that has a non-null WAR and positive PT. PT is IP
    for pitchers, PA for hitters. This is the fix for the late-season collapse:
    eos/ytd stay large all year, so consolidated rosters don't empty out in the
    final week the way the rest-of-season window does.
    """
    for key in ("eos", "ytd"):
        d = p.get(key) or {}
        w = d.get("war")
        pt = d.get("ip") if is_pit else d.get("pa")
        if w is not None and pt:
            return float(w), float(pt)
    d = ((p.get("ros") or {}).get("blend")) or {}
    w = d.get("war") or 0.0
    pt = (d.get("ip") if is_pit else d.get("pa")) or 0.0
    return float(w), float(pt)


def build_strengths(pwp, pen_data, adj):
    pen_ids = {}
    for nick, arr in (pen_data.get("teams") or {}).items():
        ab = NICK_TO_ABBR.get(nick)
        if ab:
            pen_ids[ab] = [r.get("mlbamid") for r in arr if r.get("mlbamid")]

    # hit_war / sp_war / pen_war = rest-of-season team WAR (drives the season sim
    # of the remaining games — correct to keep on the ROS window). The *_pool
    # lists carry FULL-SEASON (war, pt, mlbam_id, name) for the consolidated
    # playoff-roster calc.
    hit_war, hit_pool = {}, {}
    for p in pwp["hitters"].values():
        ab = _team_of(p)
        if ab not in LEAGUE_DIV:
            continue
        blend = ((p.get("ros") or {}).get("blend")) or {}
        hit_war[ab] = hit_war.get(ab, 0.0) + (blend.get("war") or 0.0)
        fw, fpa = _fs(p, False)
        hit_pool.setdefault(ab, []).append((fw, fpa, p.get("mlbam_id"), p.get("name")))

    sp_war, pen_war = {}, {}
    sp_pool, rp_pool = {}, {}
    for p in pwp["pitchers"].values():
        ab = _team_of(p)
        if ab not in LEAGUE_DIV:
            continue
        blend = ((p.get("ros") or {}).get("blend")) or {}
        w = blend.get("war") or 0.0
        mid = p.get("mlbam_id")
        fw, fip = _fs(p, True)
        if mid in set(pen_ids.get(ab, [])):
            pen_war[ab] = pen_war.get(ab, 0.0) + w
            rp_pool.setdefault(ab, []).append((fw, fip, mid, p.get("name")))
        else:
            sp_war[ab] = sp_war.get(ab, 0.0) + w
            sp_pool.setdefault(ab, []).append((fw, fip, mid, p.get("name")))

    teams = {}
    short = []
    for ab in LEAGUE_DIV:
        a = (adj.get(ab) or {})
        ros_war = (hit_war.get(ab, 0.0) + sp_war.get(ab, 0.0) + pen_war.get(ab, 0.0)
                   + (a.get("ros_war_adj") or 0.0))

        # Manual season-ending outs (dropped from the playoff roster). Match on
        # mlbam id or normalized name. IL guys returning for October need NO
        # flag — their full-season quality already carries them.
        outs = set()
        for o in (a.get("playoff_outs") or []):
            outs.add(str(o).strip().lower())
        def _is_out(mid, nm):
            return (str(mid).strip().lower() in outs
                    or (nm or "").strip().lower() in outs)

        # lineup: top 9 hitters by full-season WAR-rate, star-weighted (top of
        # the order soaks up playoff PA); weights sum to 9 lineup-slots-equiv
        hs = [(w, pt, mid) for (w, pt, mid, nm) in hit_pool.get(ab, [])
              if pt >= PO_MIN_PA and not _is_out(mid, nm)]
        hs.sort(key=lambda x: _shr_hit_rate(x[0], x[1], x[2]), reverse=True)
        H_WTS = [1.25, 1.20, 1.15, 1.10, 1.00, 0.90, 0.85, 0.80, 0.75]
        lineup_eq = sum(_shr_hit_rate(w, pt, mid) * hw for (w, pt, mid), hw in zip(hs[:9], H_WTS))

        # rotation: top 4 SP by full-season WAR-rate (ace matters most in Oct)
        ss = [(w, pt) for (w, pt, mid, nm) in sp_pool.get(ab, [])
              if pt >= PO_MIN_SP_IP and not _is_out(mid, nm)]
        ss.sort(key=lambda x: _rate(x[0], x[1], 180), reverse=True)
        wts = [0.38, 0.28, 0.20, 0.14]
        rot_eq = sum(_rate(w, pt, 180) * 4 * wt for (w, pt), wt in zip(ss[:4], wts))

        # pen: top 7 RP in bullpens_rr (closer-first) order, leverage-weighted —
        # top 3 arms throw the innings that decide October games
        order = {m: i for i, m in enumerate(pen_ids.get(ab, []))}
        rr = sorted([(w, ip, m) for (w, ip, m, nm) in rp_pool.get(ab, [])
                     if ip >= PO_MIN_RP_IP and not _is_out(m, nm)],
                    key=lambda x: order.get(x[2], 99))
        P_WTS = [1.55, 1.35, 1.15, 0.90, 0.75, 0.70, 0.60]
        pen_eq = sum(_rate(w, ip, 65) * pw for (w, ip, _), pw in zip(rr[:7], P_WTS))

        if len(hs) < 9 or len(ss) < 4 or len(rr) < 3:
            short.append(f"{ab}(H{len(hs)}/SP{len(ss)}/RP{len(rr)})")

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

    if short:
        # Surface thin consolidated rosters loudly — a data gap (e.g. the WAR
        # feed didn't populate) would otherwise quietly deflate playoff strength.
        print("[sean-proj] WARNING short consolidated roster: " + ", ".join(short),
              file=sys.stderr)
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


def _skip_daily(output_path, label, am_hour_et=9):
    """Recompute at most once per day — the first pipeline run at/after ~9 AM ET, so the
    published generated_at stays steady all day AND in lockstep with compute_team_futures,
    which uses the identical guard. Keeps the sim, the composite and the market board (and
    therefore Team Futures / Playoff Picture / Playoff Futures) all on the same daily run.
    FORCE_RUN bypasses (manual workflow_dispatch)."""
    import os, json, datetime
    if os.environ.get("FORCE_RUN"):
        return False
    try:
        if not output_path.exists():
            return False
        gen = json.loads(output_path.read_text()).get("generated_at", "") or ""
        g = gen.replace("Z", "")
        if "+" in g:
            g = g.split("+")[0]
        gen_dt = datetime.datetime.fromisoformat(g)
    except Exception:
        return False
    et_now = datetime.datetime.utcnow() - datetime.timedelta(hours=4)
    gen_et = gen_dt - datetime.timedelta(hours=4)
    if gen_et.date() >= et_now.date():
        print(f"[{label}] skip recompute: already refreshed today ({gen}); keeping steady.")
        return True
    if et_now.hour < am_hour_et:
        print(f"[{label}] skip recompute: before {am_hour_et}AM ET; holding until the AM data pull.")
        return True
    return False


def main():
    if _skip_daily(OUTPUT, "sean-proj"):
        return 0
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
        ros_talent = ros_wins / gr                       # ROS-WAR-implied win%
        st = standings.get(ab, {})
        G = (st.get("w") or 0) + (st.get("l") or 0)
        rs, ra = st.get("rs"), st.get("ra")
        if rs and ra and (rs + ra) > 0:
            pyth = rs**1.83 / (rs**1.83 + ra**1.83)       # season-to-date Pythagorean win%
        elif G > 0:
            pyth = st["w"] / G                            # fallback: actual win%
        else:
            pyth = ros_talent
        # Fable 2026-09-08: blend banked pythag + ROS-WAR talent, w=G/(G+140)
        # (~0.50 at G~143). Fixes shading against leaders overperforming their
        # ROS priors (HOU/CWS) and toward underperforming chasers (SEA).
        wgt = G / (G + 140.0)
        blended = wgt * pyth + (1.0 - wgt) * ros_talent
        talent[ab] = min(max(blended, 0.30), 0.70)
        s["ros_wins_talent"] = round(ros_wins, 1)
        s["talent_pyth"] = round(pyth, 3)
        s["talent_ros"]  = round(ros_talent, 3)
        s["talent_wgt"]  = round(wgt, 3)

    po_talent = {ab: min(max(0.5 + PO_STEEP * ((REPL_PCT * 162 + s["playoff_war_eq"]) / 162 - 0.5), 0.02), 0.98)
                 for ab, s in strengths.items()}

    counts = {ab: {"div": 0, "wc": 0, "po": 0, "bye": 0, "ws_app": 0, "ws": 0, "ds": 0, "cs": 0,
                   "wins_sum": 0.0} for ab in LEAGUE_DIV}
    rng = random.Random(20260714)
    bracket_counts = {}   # (al_seed_ids, nl_seed_ids) -> realization count, for v2 October pricing
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
        seeds_lg = {}
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
            seeds_lg[lg] = list(seeds)   # seed order 1..6
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
            for ab in (seeds[0], seeds[1], wc1, wc2): counts[ab]["ds"] += 1
            for ab in (ds1, ds2): counts[ab]["cs"] += 1
            hi, lo = (ds1, ds2) if (w[ds1], rng.random()) >= (w[ds2], rng.random()) else (ds2, ds1)
            finalists[lg] = duel(hi, lo, 7, (1, 1, 0, 0, 0, 1, 1))
        _bk = (tuple(ABBR2ID[a] for a in seeds_lg["AL"]),
               tuple(ABBR2ID[a] for a in seeds_lg["NL"]))
        bracket_counts[_bk] = bracket_counts.get(_bk, 0) + 1
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
            "reach_ds_pct": round(100 * c["ds"] / N_SIMS, 1),
            "reach_cs_pct": round(100 * c["cs"] / N_SIMS, 1),
            "ws_app_pct": round(100 * c["ws_app"] / N_SIMS, 1),
            "ws_pct": round(100 * c["ws"] / N_SIMS, 1),
            **s,
        }

    payload = {
        "generated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds"),
        "season": SEASON,
        "n_sims": N_SIMS,
        "method": ("player-level ROS WAR (depth) -> season sim; consolidated "
                   "FULL-SEASON-rate top-9 hitters / top-heavy top-4 SP (55%) / leverage-weighted top-7 RP (45%) -> playoff sim; "
                   "manual deadline_adjustments.json layer (playoff_outs drops season-ending injuries)"),
        "teams": teams_out,
    }
    OUTPUT.write_text(json.dumps(payload, separators=(",", ":")))
    print(f"[sean-proj] wrote {OUTPUT} ({len(teams_out)} teams, {N_SIMS} sims)", file=sys.stderr)

    # Seeding distribution for Fable's v2 October engine (run_v2_bracket.py). One row
    # per distinct (AL seeds 1-6, NL seeds 1-6) realization with its count; statsapi
    # team ids in seed order. The v2 runner prices each realization on the frozen
    # 16-team field and marginalizes by count.
    bracket_dist = sorted(
        ([list(al), list(nl), n] for (al, nl), n in bracket_counts.items()),
        key=lambda e: -e[2])
    BRACKET_OUT.write_text(json.dumps(
        {"generated_at": payload["generated_at"], "season": SEASON, "n_sims": N_SIMS,
         "n_realizations": len(bracket_dist), "bracket_dist": bracket_dist},
        separators=(",", ":")))
    print(f"[sean-proj] wrote {BRACKET_OUT} ({len(bracket_dist)} distinct brackets)", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
