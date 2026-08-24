#!/usr/bin/env python3
"""
STAFF_OFF_v2 — per-hitter offense projection (wOBA) + team aggregate.

Replaces the sheet's ad-hoc in-season/projection blend (the "0.11 disease":
in-season data was under-weighted so team offense collapsed toward the prior).

Blend (Fable/Sean spec, 2026-08):
    prior   = ROS projection composite  (mean of ATC + THE BAT X wOBA)
    insea   = 0.6 * xwOBA + 0.4 * wOBA           (season-to-date)
    w       = PA / (PA + 500)                     (K = 500, was 350 in sheet)
    proj    = (1 - w) * prior + w * insea

Join key is MLBAM id (xMLBAMID / mlbam_id) everywhere — never name.

Inputs (all local to the repo checkout; no network needed):
    data/_fg_ros.json            bat.atc[], bat.batx[]  -> prior wOBA + ROS proj PA
    data/_fg_ytd.json            bat[]                  -> season wOBA, PA, Team
    data/savant_true_xwoba.json  {mlbam:{pa,xwoba}}     -> season xwOBA (Savant, raw scale)
    data/hitter_gamelogs.json    hitters{}.season       -> xwOBA fallback if Savant missing

Output:
    data/staff_offense.json  slim lookup the sheet pulls:
      { generated_at, method, K, prior_systems, league_avg,
        teams:  { ABBR: {proj_woba, n} },
        players:{ mlbam: {team, prior, prior_real, woba, xwoba, insea, pa, w, proj} } }

Notes / open items for Fable's acceptance gate:
  * Prior composite is ATC + THE BAT X only. OOPSY is not present in _fg_ros.json
    (repo carries atc + batx). If an OOPSY feed is added, drop it into PRIOR_SYS.
  * PRIOR_FLOOR (0.310) is the Marcel fallback stub for hitters with no ROS
    projection. A real statsapi-Marcel can replace _prior_fallback() later; today
    those players carry negligible PA weight so the team numbers are unaffected.
"""
from __future__ import annotations
import datetime, json, sys
from pathlib import Path

REPO_ROOT   = Path(__file__).resolve().parent.parent
ROS_FILE    = REPO_ROOT / "data" / "_fg_ros.json"
YTD_FILE    = REPO_ROOT / "data" / "_fg_ytd.json"
SAVANT_FILE = REPO_ROOT / "data" / "savant_true_xwoba.json"
GL_FILE     = REPO_ROOT / "data" / "hitter_gamelogs.json"
OUTPUT      = REPO_ROOT / "data" / "staff_offense.json"

K            = 500          # in-season shrinkage constant
XWOBA_W      = 0.6          # in-season composite: 0.6*xwOBA + 0.4*wOBA
PRIOR_FLOOR  = 0.310        # Marcel fallback when no ROS projection exists
PRIOR_SYS    = ("atc", "batx")

# _fg_* files use a few FanGraphs-style abbreviations
ABBR_FIX = {"WSN": "WSH", "TBR": "TB", "SDP": "SD", "SFG": "SF", "KCR": "KC",
            "CHW": "CWS", "OAK": "ATH"}
REAL_TEAMS = {"NYY","BOS","TOR","TB","BAL","CLE","MIN","DET","KC","CWS",
              "HOU","SEA","TEX","LAA","ATH","ATL","NYM","PHI","WSH","MIA",
              "MIL","CHC","CIN","STL","PIT","LAD","SD","SF","ARI","COL"}


def _fix(ab):
    return ABBR_FIX.get(ab, ab)


def _prior_fallback(mlbam):
    """Marcel-from-statsAPI stub. Returns the 0.310 floor today; hook for a
    real regressed-to-league estimate later. Kept a function so the fallback
    policy lives in one place."""
    return PRIOR_FLOOR


def build_prior(ros):
    """mlbam -> {woba_composite, proj_pa, team, name, real}"""
    acc = {}
    for sysname in PRIOR_SYS:
        for p in ((ros.get("bat") or {}).get(sysname) or []):
            mid = p.get("xMLBAMID")
            if not mid:
                continue
            d = acc.setdefault(mid, {"ws": [], "proj_pa": 0.0,
                                     "team": _fix(p.get("Team")), "name": p.get("PlayerName")})
            if p.get("wOBA") is not None:
                d["ws"].append(p["wOBA"])
            if p.get("PA") is not None:
                d["proj_pa"] = max(d["proj_pa"], p["PA"])
    out = {}
    for mid, d in acc.items():
        w = sum(d["ws"]) / len(d["ws"]) if d["ws"] else None
        out[mid] = {"woba": w, "proj_pa": d["proj_pa"], "team": d["team"],
                    "name": d["name"], "real": w is not None}
    return out


def build_ytd(ytd):
    """mlbam -> {woba, pa, team, name}"""
    out = {}
    for p in (ytd.get("bat") or []):
        mid = p.get("xMLBAMID")
        if not mid:
            continue
        out[mid] = {"woba": p.get("wOBA"), "pa": p.get("PA") or 0,
                    "team": _fix(p.get("Team")), "name": p.get("PlayerName")}
    return out


def build_xwoba(savant, gl):
    """mlbam(int) -> {xwoba, pa}. Primary = Savant true xwOBA (raw wOBA scale);
    fallback = hitter_gamelogs season.xwoba. Savant keys are string mlbam ids."""
    out = {}
    for mid, d in (savant or {}).items():
        if not str(mid).isdigit():   # skip _meta / any non-id key
            continue
        if isinstance(d, dict) and d.get("xwoba") is not None:
            out[int(mid)] = {"xwoba": d["xwoba"], "pa": d.get("pa") or 0}
    cont = (gl or {}).get("hitters") or gl or {}
    for rec in cont.values():
        if not isinstance(rec, dict):
            continue
        mid = rec.get("mlbam_id")
        s = rec.get("season") or {}
        if mid and mid not in out and s.get("xwoba") is not None:
            out[mid] = {"xwoba": s["xwoba"], "pa": s.get("pa") or 0}
    return out


def blend(prior, ytd, xw):
    ids = set(prior) | set(ytd)
    players = {}
    for mid in ids:
        P, Y, X = prior.get(mid), ytd.get(mid), xw.get(mid)
        prior_real = bool(P and P["real"])
        prior_w = P["woba"] if prior_real else _prior_fallback(mid)
        woba = Y["woba"] if Y else None
        xwoba = X["xwoba"] if X else None
        pa = (Y["pa"] if Y else 0) or (X["pa"] if X else 0)

        if xwoba is not None and woba is not None:
            insea = XWOBA_W * xwoba + (1 - XWOBA_W) * woba
        elif woba is not None:
            insea = woba
        elif xwoba is not None:
            insea = xwoba
        else:
            insea = None

        if insea is None:
            proj, w = prior_w, 0.0
        else:
            w = pa / (pa + K)
            proj = (1 - w) * prior_w + w * insea

        # current team: prefer the ROS projection's (depth-chart) team so traded
        # players — who carry "- - -"/"2 Tms" in the YTD file — land on their
        # current club instead of being dropped. YTD team only as fallback.
        pt, yt_ = (P["team"] if P else None), (Y["team"] if Y else None)
        if pt in REAL_TEAMS:
            team = pt
        elif yt_ in REAL_TEAMS:
            team = yt_
        else:
            team = pt or yt_
        ptwt = (P["proj_pa"] if P and P["proj_pa"] else pa) or 1.0
        nm = (P.get("name") if P else None) or (Y.get("name") if Y else None)
        players[mid] = {"team": team, "name": nm, "prior": round(prior_w, 4), "prior_real": prior_real,
                        "woba": None if woba is None else round(woba, 4),
                        "xwoba": None if xwoba is None else round(xwoba, 4),
                        "insea": None if insea is None else round(insea, 4),
                        "pa": int(pa), "w": round(w, 4), "proj": round(proj, 4),
                        "_ptwt": ptwt}
    return players


def aggregate_teams(players):
    T = {}
    for p in players.values():
        t = p["team"]
        if t not in REAL_TEAMS:
            continue
        d = T.setdefault(t, {"sw": 0.0, "wt": 0.0, "n": 0})
        d["sw"] += p["proj"] * p["_ptwt"]
        d["wt"] += p["_ptwt"]
        d["n"] += 1
    return {t: {"proj_woba": round(d["sw"] / d["wt"], 4), "n": d["n"]}
            for t, d in T.items() if d["wt"] > 0}


def main():
    for f in (ROS_FILE, YTD_FILE, GL_FILE):
        if not f.exists():
            print(f"[staff-off] missing input {f}; keeping previous output", file=sys.stderr)
            return 0 if OUTPUT.exists() else 1
    ros = json.loads(ROS_FILE.read_text())
    ytd = json.loads(YTD_FILE.read_text())
    gl  = json.loads(GL_FILE.read_text()) if GL_FILE.exists() else {}
    savant = json.loads(SAVANT_FILE.read_text()) if SAVANT_FILE.exists() else {}

    prior = build_prior(ros)
    yt    = build_ytd(ytd)
    xw    = build_xwoba(savant, gl)
    players = blend(prior, yt, xw)
    teams = aggregate_teams(players)

    # strip internal weight from the published lookup
    pub_players = {str(mid): {k: v for k, v in p.items() if k != "_ptwt"}
                   for mid, p in players.items() if p["team"] in REAL_TEAMS}
    lg = round(sum(t["proj_woba"] for t in teams.values()) / len(teams), 4) if teams else None

    payload = {
        "generated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds"),
        "method": "STAFF_OFF_v2",
        "K": K, "xwoba_weight": XWOBA_W, "prior_floor": PRIOR_FLOOR,
        "prior_systems": list(PRIOR_SYS),
        "league_avg": lg,
        "teams": dict(sorted(teams.items(), key=lambda kv: -kv[1]["proj_woba"])),
        "players": pub_players,
    }
    OUTPUT.write_text(json.dumps(payload, separators=(",", ":")))
    print(f"[staff-off] wrote {OUTPUT} ({len(teams)} teams, {len(pub_players)} hitters, lgAvg {lg})",
          file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
