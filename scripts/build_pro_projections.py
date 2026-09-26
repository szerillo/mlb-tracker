#!/usr/bin/env python3
"""
Internal projection engine DRIVER (the "Excel-free" pipeline).

Assembles each game's inputs from the repo feeds + bridge feeds and runs
compute_pro_projections.project_matchup() to produce data/pro_projections.json —
the same R2/S2 (F5) / R9/S9 (full) / T9 (total) / S10 (WP) the GAME UPLOADER
publishes, with NO spreadsheet in the request path.

Feeds:
  batter_projected.json   offense inputs (xR, platoon, FLD, BSR)   [bridge]
  pitcher_projected.json  SP stamina + K-BB                        [bridge]
  sheet_tables.json       Team Modifiers (park PF + offense divisor)[bridge]
  pitcher_stats.json      SP RA (unified_adj) + pen RA (unified_score) + hand
  bullpens_rr.json        available arms per team (+ fatigue state)
  weather.json            v8 run_adj_pct -> J4
  umps.json               HP ump favor -> L6 (0 when unassigned/forward)
  pitcher_split_tilt.json per-game tilt
  lineups.json            batting order + positions

Output row per gamePk: f5/full runs, total, WP + the intermediates, so it drops
into the site exactly where sheet_projections.json does.
"""
from __future__ import annotations
import json, os, re, sys, urllib.request, datetime
import unicodedata

HERE = os.path.dirname(__file__)
REPO = os.path.abspath(os.path.join(HERE, ".."))
sys.path.insert(0, HERE)
import compute_pro_projections as E

def _load(name):
    try:
        return json.loads(open(os.path.join(REPO, "data", name)).read())
    except Exception:
        return {}

def _norm(s):
    s = (s or "").lower()
    s = "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))  # strip accents (Diaz/Diaz, Pena/Pena)
    s = s.replace(".", "").replace("'", "")
    s = re.sub(r"\s+(jr|sr|ii|iii|iv)$", "", s)
    return re.sub(r"\s+", " ", s).strip()

FATIGUE_STATES = {"TIRED"}          # +0.4 RA / -0.04 K-BB (sheet fatigue flag)
EXCLUDE_STATES = {"LIKELY_OUT"}     # unavailable arms — drop from the pen

def _team_key(full, keyed):
    """Match a statsapi full team name ('Chicago Cubs') to a nickname-keyed dict
    ('cubs')/('red sox') by longest-suffix match."""
    fn = _norm(full)
    best = None
    for k in keyed:
        if fn.endswith(_norm(k)) and (best is None or len(k) > len(best)):
            best = k
    return keyed.get(best) if best else None

def _sp_gs_ip(mlbam_id, season):
    if not mlbam_id:
        return (None, None)
    try:
        url = f"https://statsapi.mlb.com/api/v1/people/{mlbam_id}/stats?stats=season&group=pitching&season={season}"
        req = urllib.request.Request(url, headers={"User-Agent": "mlb-tracker/1.0"})
        d = json.load(urllib.request.urlopen(req, timeout=15))
        splits = (d.get("stats") or [{}])[0].get("splits") or []
        agg = max(splits, key=lambda s: (s.get("stat", {}).get("battersFaced") or 0), default=None)
        if agg:
            st = agg["stat"]
            return (st.get("gamesStarted"), float(st.get("inningsPitched") or 0) or None)
    except Exception:
        pass
    return (None, None)

def _hp_ump_favor(game_pk, umps, baseline):
    """HP-ump favor for J2 (sheet L6): clamp(off_adj_shrunk - baseline, +/-0.012).
    0 when the plate ump is unassigned (forward games)."""
    try:
        d = json.load(urllib.request.urlopen(
            f"https://statsapi.mlb.com/api/v1/game/{game_pk}/feed/live", timeout=12))
        offs = (d.get("liveData", {}).get("boxscore", {}) or {}).get("officials") or []
        hp = next((o.get("official", {}).get("fullName") for o in offs
                   if str(o.get("officialType", "")).lower().startswith("home")), None)
        if not hp:
            return None
        u = umps.get(hp) or {}
        adj = u.get("off_adj_shrunk")
        if adj is None:
            return None
        return max(-0.012, min(0.012, adj - (baseline or 0.0)))
    except Exception:
        return None


def main():
    bp   = _load("batter_projected.json").get("players", {})
    pp   = _load("pitcher_projected.json").get("pitchers", {})
    tabl = _load("sheet_tables.json").get("teams", {})
    ps   = {_norm(k): v for k, v in _load("pitcher_stats.json").get("pitchers", {}).items()}
    pens = _load("bullpens_rr.json").get("teams", {})
    wx   = {str(g.get("game_pk")): g for g in _load("weather.json").get("games", [])}
    tilt_names = _load("pitcher_split_tilt.json").get("games", {})   # SP names per game
    _st = _load("sheet_tables.json")
    tilt_v3 = _st.get("tilt", {})                                     # Tilt By Game v3 (an_event_id -> {away,home})
    _umpd = _load("umps.json"); umps = _umpd.get("umpires", {}); ump_base = _umpd.get("baseline")
    sp_map = _load("sheet_projections.json").get("games", {})         # pk -> {an_event_id}
    lus  = {str(g.get("game_pk")): g for g in _load("lineups.json").get("games", [])}
    const = _load("sheet_projections.json").get("constants") or E.DEFAULT_CONST
    season = datetime.date.today().year

    def sp_info(name):
        p = ps.get(_norm(name)) or {}
        ra = p.get("unified_adj") if p.get("unified_adj") is not None else p.get("unified_score")
        if ra is None:
            ra = p.get("fip_proj")
        st = (pp.get(_norm(name)) or {}).get("stamina")
        hand = "LHP" if (p.get("hand") == "L") else "RHP"
        gs, ip = _sp_gs_ip(p.get("mlbam_id"), season)
        if ip is None:
            ip = p.get("ip")
        return {"ra": ra, "stamina": st, "gs": gs, "ip": ip, "hand": hand, "mlbam_id": p.get("mlbam_id")}

    def arms_for(team_full, sp_name):
        row = _team_key(team_full, pens)
        if not row:
            return []
        out = []
        for a in row:
            nm = a.get("player")
            if not nm or _norm(nm) == _norm(sp_name):    # exclude tonight's SP
                continue
            if str(a.get("state", "")).upper() in EXCLUDE_STATES:   # LIKELY_OUT -> unavailable
                continue
            praw = ps.get(_norm(nm)) or {}
            ra = praw.get("unified_score")
            kbb = (pp.get(_norm(nm)) or {}).get("kbb")
            if ra is None or kbb is None:
                continue
            fat = str(a.get("state", "")).upper() in FATIGUE_STATES
            out.append({"ra": ra, "kbb": kbb, "fatigued": fat})
        return out

    out_games = {}
    for pk, lg in lus.items():
        try:
            au = (lg.get("lineups", {}).get("away") or {}).get("players") or []
            hu = (lg.get("lineups", {}).get("home") or {}).get("players") or []
            if len(au) < 9 or len(hu) < 9:
                continue
            away_full, home_full = lg.get("away"), lg.get("home")
            # SPs from the tilt feed (has away_sp/home_sp) — falls back gracefully
            t = tilt_names.get(pk, {})
            asp_name, hsp_name = t.get("away_sp"), t.get("home_sp")
            if not asp_name or not hsp_name:
                continue
            asp, hsp = sp_info(asp_name), sp_info(hsp_name)
            if asp["ra"] is None or hsp["ra"] is None or asp["stamina"] is None or hsp["stamina"] is None:
                continue
            atm, htm = _team_key(away_full, tabl), _team_key(home_full, tabl)
            if not atm or not htm:
                continue
            w = wx.get(pk, {})
            wadj = ((w.get("v8") or {}).get("run_adj_pct"))
            wadj = (wadj / 100.0) if wadj is not None else 0.0
            eid = str((sp_map.get(pk) or {}).get("an_event_id") or "").split(".")[0]
            tv = tilt_v3.get(eid, {})
            ump_favor = _hp_ump_favor(pk, umps, ump_base)
            mu = {
                "away_lineup": [(p["name"], p.get("pos")) for p in au[:9]],
                "home_lineup": [(p["name"], p.get("pos")) for p in hu[:9]],
                "bp_players": bp,
                "away_sp_hand": asp["hand"], "home_sp_hand": hsp["hand"],
                "away_park_off": atm.get("pf_adj_away"), "home_park_off": htm.get("pf_adj_home"),
                "home_park_factor": htm.get("runs_pf"),
                "weather_adj": wadj, "ump_favor": ump_favor,
                "away_sp": asp, "home_sp": hsp,
                "away_arms": arms_for(away_full, asp_name),
                "home_arms": arms_for(home_full, hsp_name),
                "tilt_away": (tv.get("away") if tv.get("away") is not None else 0.0),
                "tilt_home": (tv.get("home") if tv.get("home") is not None else 0.0),
            }
            o = E.project_matchup(mu, const)
            out_games[pk] = {
                "away_team": away_full, "home_team": home_full,
                "away_runs": round(o["away_runs"], 3), "home_runs": round(o["home_runs"], 3),
                "total": round(o["total"], 3),
                "away_wp": round(o["away_wp"], 4), "home_wp": round(o["home_wp"], 4),
                "f5_away_runs": round(o["f5_away_runs"], 3), "f5_home_runs": round(o["f5_home_runs"], 3),
                "f5_total": round(o["f5_total"], 3), "f5_home_wp": round(o["f5_home_wp"], 4),
                "away_sp": asp_name, "home_sp": hsp_name,
            }
        except Exception as e:
            print(f"  [pro] skip {pk}: {e}", file=sys.stderr)
    payload = {"generated_at": datetime.datetime.utcnow().isoformat(timespec="seconds") + "Z",
               "source": "compute_pro_projections engine (internal, no spreadsheet)",
               "constants": const, "n_games": len(out_games), "games": out_games}
    outp = os.path.join(REPO, "data", "pro_projections.json")
    with open(outp, "w") as fh:
        json.dump(payload, fh, indent=1)
    print(f"[pro] wrote {len(out_games)} game projections -> {outp}")
    return 0

if __name__ == "__main__":
    sys.exit(main())
