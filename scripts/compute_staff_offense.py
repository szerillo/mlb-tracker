#!/usr/bin/env python3
"""
STAFF_OFF_v3 — per-hitter offense projection (wOBA) + team aggregate.

Projection-only hitter input (Fable, 2026-09-23). The old v2 blended a 2-system
ROS prior with a season-to-date term:
    proj = (1-w)*prior + w*insea,  w = PA/(PA+500),  insea = 0.6*xwOBA + 0.4*wOBA
Fable's forward-30-day backtest showed that blend scores *worse* than giving every
hitter the league average: the observed-season term double-counts, because the
projection systems already ingest 2026 (properly regressed). So the in-season
branch is deleted, not re-tuned, and the input becomes the 5-system blend directly:

    woba_input = ros.blend.woba          # player_war_projections.json, joined on mlbam_id
                                         # (ATC + THE BAT X + OOPSY + ZiPS + Steamer)

This is a talent *rate*, not a remaining-PA sample — each system builds it from its
full multi-year prior plus all of 2026, so it does not decay as the season shortens.
No observed-season weight, no shrink. Retires the hitters.json name-join hazard
(everything here is MLBAM-id joined).

WRITE-THROUGH OCTOBER FREEZE
    ros.blend is a *rest-of-season* projection: once the regular season ends the feed
    stops publishing it, so ros.blend.woba goes null in the playoffs — exactly when the
    model still needs it. Each run therefore snapshots every live ros.blend.woba to
    data/staff_offense_frozen_woba.json; when a hitter's live value is missing (season
    over, or a system dropped out for a fringe bat) we fall back to that last snapshot.
    The freshest regular-season talent rate persists through October automatically, with
    no manual freeze date. (Fable's "freeze the talent input until October grades it.")

Fallback, no live projection and no snapshot (~deep-minors, negligible PA weight):
    the existing Marcel prior (regressed-to-league, 0.292 default) — unchanged.

Inputs (all local to the repo checkout; no network needed):
    data/player_war_projections.json   hitters[].ros.blend.woba  -> projection (talent rate)
    data/_fg_ros.json                   bat.atc[]/batx[]          -> proj PA + depth-chart team
    data/_fg_ytd.json                   bat[]                     -> season wOBA/PA/Team (reporting + team fallback)
    data/savant_true_xwoba.json         {mlbam:{pa,xwoba}}        -> season xwOBA (reporting only)
    data/hitter_gamelogs.json           hitters{}.season          -> xwOBA fallback (reporting only)
    data/staff_offense_frozen_woba.json {woba:{mlbam:rate}}       -> write-through freeze snapshot

Output:
    data/staff_offense.json  slim lookup the sheet pulls:
      { generated_at, method, league_avg, prior_systems,
        teams:  { ABBR: {proj_woba, n} },
        players:{ mlbam: {team, name, proj, proj_source, woba, xwoba, pa} } }
"""
from __future__ import annotations
import datetime, json, sys
from pathlib import Path

REPO_ROOT   = Path(__file__).resolve().parent.parent
PWP_FILE    = REPO_ROOT / "data" / "player_war_projections.json"
ROS_FILE    = REPO_ROOT / "data" / "_fg_ros.json"
YTD_FILE    = REPO_ROOT / "data" / "_fg_ytd.json"
SAVANT_FILE = REPO_ROOT / "data" / "savant_true_xwoba.json"
GL_FILE     = REPO_ROOT / "data" / "hitter_gamelogs.json"
OUTPUT      = REPO_ROOT / "data" / "staff_offense.json"
FROZEN_FILE = REPO_ROOT / "data" / "staff_offense_frozen_woba.json"

PRIOR_SYS   = ("atc", "batx", "oopsy", "zips", "steamer")   # the ros.blend components (for the header)

# Marcel hitter prior (Fable 2026, forward-validated regression target 0.292): the
# no-projection fallback, regressed-to-league wOBA per MLBAM id with a no-history default.
_MARCEL_PATH = REPO_ROOT / "data" / "marcel_prior_2026.json"
try:
    _mj = json.loads(_MARCEL_PATH.read_text())
    MARCEL_PRIORS  = _mj.get("priors", {}) or {}
    MARCEL_DEFAULT = _mj.get("no_history_default", 0.292)
except Exception:
    MARCEL_PRIORS, MARCEL_DEFAULT = {}, 0.292

# _fg_* files use a few FanGraphs-style abbreviations
ABBR_FIX = {"WSN": "WSH", "TBR": "TB", "SDP": "SD", "SFG": "SF", "KCR": "KC",
            "CHW": "CWS", "OAK": "ATH"}
REAL_TEAMS = {"NYY","BOS","TOR","TB","BAL","CLE","MIN","DET","KC","CWS",
              "HOU","SEA","TEX","LAA","ATH","ATL","NYM","PHI","WSH","MIA",
              "MIL","CHC","CIN","STL","PIT","LAD","SD","SF","ARI","COL"}


def _fix(ab):
    return ABBR_FIX.get(ab, ab)


def _prior_fallback(mlbam):
    """Marcel hitter prior (Fable 2026, forward-validated to 0.292). Regressed-to-league
    wOBA for hitters with no ROS projection and no snapshot, keyed by MLBAM id; falls back
    to the no-history default (0.292) when the player is absent from the Marcel file."""
    e = MARCEL_PRIORS.get(str(mlbam))
    if e and e.get("marcel_woba") is not None:
        return e["marcel_woba"]
    return MARCEL_DEFAULT


def build_pwp(pwp):
    """mlbam(int) -> ros.blend.woba (the 5-system projection talent rate)."""
    hitters = pwp.get("hitters", {})
    it = hitters.values() if isinstance(hitters, dict) else hitters
    out = {}
    for x in it:
        if not isinstance(x, dict):
            continue
        mid = x.get("mlbam_id")
        w = ((x.get("ros") or {}).get("blend") or {}).get("woba")
        if mid is not None and w is not None:
            out[int(mid)] = float(w)
    return out


def build_prior(ros):
    """mlbam -> {proj_pa, team, name}. Used for PA weighting and depth-chart team only;
    the wOBA value comes from ros.blend, not from this 2-system composite anymore."""
    acc = {}
    for sysname in ("atc", "batx"):
        for p in ((ros.get("bat") or {}).get(sysname) or []):
            mid = p.get("xMLBAMID")
            if not mid:
                continue
            d = acc.setdefault(mid, {"proj_pa": 0.0,
                                     "team": _fix(p.get("Team")), "name": p.get("PlayerName")})
            if p.get("PA") is not None:
                d["proj_pa"] = max(d["proj_pa"], p["PA"])
    return {mid: {"proj_pa": d["proj_pa"], "team": d["team"], "name": d["name"]}
            for mid, d in acc.items()}


def build_ytd(ytd):
    """mlbam -> {woba, pa, team, name} (season observed — reporting + team/PA fallback)."""
    out = {}
    for p in (ytd.get("bat") or []):
        mid = p.get("xMLBAMID")
        if not mid:
            continue
        out[mid] = {"woba": p.get("wOBA"), "pa": p.get("PA") or 0,
                    "team": _fix(p.get("Team")), "name": p.get("PlayerName")}
    return out


def build_xwoba(savant, gl):
    """mlbam(int) -> {xwoba, pa} (season observed — reporting only)."""
    out = {}
    for mid, d in (savant or {}).items():
        if not str(mid).isdigit():
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


def blend(prior, ytd, xw, pwp, frozen):
    """Projection-only: proj = ros.blend.woba (write-through frozen), else Marcel fallback.
    Mutates `frozen` in place with every live projection value so the snapshot stays current.
    Returns (players, counts)."""
    ids = set(prior) | set(ytd) | set(pwp)
    players = {}
    counts = {"proj": 0, "frozen": 0, "marcel": 0}
    for mid in ids:
        P, Y, X = prior.get(mid), ytd.get(mid), xw.get(mid)

        live = pwp.get(mid)
        if live is not None:
            proj, src = live, "proj"
            frozen[mid] = round(live, 4)          # write-through: refresh the snapshot
        elif frozen.get(mid) is not None:
            proj, src = frozen[mid], "frozen"     # season over / system dropped -> last good rate
        else:
            proj, src = _prior_fallback(mid), "marcel"
        counts[src] += 1

        woba  = Y["woba"] if Y else None          # season observed — reporting only
        xwoba = X["xwoba"] if X else None
        pa = (Y["pa"] if Y else 0) or (X["pa"] if X else 0)

        # current team: prefer the ROS depth-chart team so traded players (who carry
        # "- - -"/"2 Tms" in the YTD file) land on their current club. YTD as fallback.
        pt, yt_ = (P["team"] if P else None), (Y["team"] if Y else None)
        if pt in REAL_TEAMS:
            team = pt
        elif yt_ in REAL_TEAMS:
            team = yt_
        else:
            team = pt or yt_
        # weight the team aggregate by season-to-date PA (playing time, not
        # performance — stays projection-only on the *value*). ROS proj_pa collapses
        # to ~uniform at season's end and to zero in October; YTD PA is stable and
        # reflects who actually plays. Falls back to proj_pa, then 1.0.
        ptwt = pa or (P["proj_pa"] if P and P["proj_pa"] else 0) or 1.0
        nm = (P.get("name") if P else None) or (Y.get("name") if Y else None)
        players[mid] = {"team": team, "name": nm,
                        "proj": round(proj, 4), "proj_source": src,
                        "woba": None if woba is None else round(woba, 4),
                        "xwoba": None if xwoba is None else round(xwoba, 4),
                        "pa": int(pa), "_ptwt": ptwt}
    return players, counts


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
    for f in (PWP_FILE, ROS_FILE, YTD_FILE):
        if not f.exists():
            print(f"[staff-off] missing input {f}; keeping previous output", file=sys.stderr)
            return 0 if OUTPUT.exists() else 1
    pwp_raw = json.loads(PWP_FILE.read_text())
    ros = json.loads(ROS_FILE.read_text())
    ytd = json.loads(YTD_FILE.read_text())
    gl  = json.loads(GL_FILE.read_text()) if GL_FILE.exists() else {}
    savant = json.loads(SAVANT_FILE.read_text()) if SAVANT_FILE.exists() else {}

    # write-through freeze snapshot (int-keyed in memory, str-keyed on disk)
    frozen = {}
    if FROZEN_FILE.exists():
        try:
            fj = json.loads(FROZEN_FILE.read_text())
            frozen = {int(k): float(v) for k, v in (fj.get("woba") or {}).items()}
        except Exception as e:
            print(f"[staff-off] could not read freeze snapshot: {e}", file=sys.stderr)

    pwp   = build_pwp(pwp_raw)
    prior = build_prior(ros)
    yt    = build_ytd(ytd)
    xw    = build_xwoba(savant, gl)
    players, counts = blend(prior, yt, xw, pwp, frozen)
    teams = aggregate_teams(players)

    # persist the refreshed freeze snapshot (last-good talent rate per hitter)
    FROZEN_FILE.write_text(json.dumps({
        "generated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds"),
        "source": "player_war_projections ros.blend.woba (write-through)",
        "woba": {str(mid): v for mid, v in sorted(frozen.items())},
    }, separators=(",", ":")))

    # strip internal weight from the published lookup
    pub_players = {str(mid): {k: v for k, v in p.items() if k != "_ptwt"}
                   for mid, p in players.items() if p["team"] in REAL_TEAMS}
    lg = round(sum(t["proj_woba"] for t in teams.values()) / len(teams), 4) if teams else None

    payload = {
        "generated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds"),
        "method": "STAFF_OFF_v3",
        "input": "ros.blend.woba (projection-only, 5-system, write-through frozen for October)",
        "prior_systems": list(PRIOR_SYS),
        "fallback": {"source": "marcel_2026", "default": MARCEL_DEFAULT, "n_priors": len(MARCEL_PRIORS)},
        "counts": counts,
        "league_avg": lg,
        "teams": dict(sorted(teams.items(), key=lambda kv: -kv[1]["proj_woba"])),
        "players": pub_players,
    }
    OUTPUT.write_text(json.dumps(payload, separators=(",", ":")))
    print(f"[staff-off] wrote {OUTPUT} ({len(teams)} teams, {len(pub_players)} hitters, "
          f"lgAvg {lg}; sources proj={counts['proj']} frozen={counts['frozen']} marcel={counts['marcel']})",
          file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
