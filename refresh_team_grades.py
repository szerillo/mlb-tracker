#!/usr/bin/env python3
"""
Refresh league-wide team-grade reference stats.

The frontend's Statcast team-avg view z-scores team grades against whatever
teams are visible in the current slate, which makes a 2-game slate produce
wildly different grades than a 14-game slate for the same team. This script
walks all 30 teams once per day, computes each team's average percentile per
category over its full active roster, then stores both:

  • teams[abbr][cat] = team's avg percentile (raw, 0-100 scale)
  • stats[cat] = { mean, sd } across the 30 teams

The frontend reads these stats to z-score deterministically — same team
always gets the same grade regardless of slate size.

Categories: pwr, eye, bsr, def — same set the lineup-row letter grades use.

Output: data/team_grades.json
Runs nightly so the grades stay aligned with the latest hitter percentiles
and projection refreshes.
"""
from __future__ import annotations
import datetime, json, sys, unicodedata, urllib.request
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = REPO_ROOT / "data"
OUTPUT = DATA_DIR / "team_grades.json"

# MLB team IDs (mirrors the frontend's teamLogo map). Stable across seasons.
MLB_TEAMS = [
    (109, "ARI"), (144, "ATL"), (110, "BAL"), (111, "BOS"), (112, "CHC"),
    (145, "CWS"), (113, "CIN"), (114, "CLE"), (115, "COL"), (116, "DET"),
    (117, "HOU"), (118, "KC"),  (108, "LAA"), (119, "LAD"), (146, "MIA"),
    (158, "MIL"), (142, "MIN"), (121, "NYM"), (147, "NYY"), (133, "OAK"),
    (143, "PHI"), (134, "PIT"), (135, "SD"),  (137, "SF"),  (136, "SEA"),
    (138, "STL"), (139, "TB"),  (140, "TEX"), (141, "TOR"), (120, "WSH"),
]


def _norm_name(s):
    if not s:
        return ""
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c)).lower()
    for suf in (" jr.", " jr", " sr.", " sr", " iii", " ii"):
        if s.endswith(suf):
            s = s[: -len(suf)]
    return s.replace(".", "").strip()


def _load_json(path):
    p = DATA_DIR / path
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text())
    except Exception as e:
        print(f"[team-grades] load failed {path}: {e}", file=sys.stderr)
        return None


def _fetch_roster(team_id):
    """26-man active roster from MLB Stats API. Cached at the OS level by
    the script's once-per-day invocation, so no need for a local cache here."""
    url = f"https://statsapi.mlb.com/api/v1/teams/{team_id}/roster?rosterType=active"
    req = urllib.request.Request(url, headers={"User-Agent": "u/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            d = json.load(r)
        return [(p["person"]["id"], p["person"]["fullName"]) for p in d.get("roster", [])]
    except Exception as e:
        print(f"[team-grades] roster fetch failed for team {team_id}: {e}", file=sys.stderr)
        return []


# ── grade pieces, mirroring index.html ────────────────────────────────────
def _avg_pct(vals):
    vs = [v for v in vals if v is not None]
    return sum(vs) / len(vs) if vs else None


def _power_pct(hp):
    """Mirror frontend powerPct: barrel / hard_hit / xslg / xiso / bat_speed
    (need at least 2 non-null), with max_ev fallback for sparse rookies."""
    if not hp:
        return None
    core = [hp.get("barrel"), hp.get("hard_hit"), hp.get("xslg"),
            hp.get("xiso"), hp.get("bat_speed")]
    present = [v for v in core if v is not None]
    if len(present) >= 2:
        return _avg_pct(present)
    if hp.get("max_ev") is not None:
        return hp["max_ev"]
    return None


def _eye_pct(hp):
    return _avg_pct([hp.get("bb_pct"), hp.get("chase")]) if hp else None


# BSR and DEF use projected runs vs a known population (the regulars). We
# can approximate by reading the hitter's bsr/fld and normalizing within
# this script's own population over all 30 teams' rosters — same shape as
# the existing index.html _BSR_POP/_FLD_POP at runtime.
def _percentile_of(v, pop_sorted):
    if v is None or not pop_sorted:
        return None
    lo, hi = 0, len(pop_sorted)
    while lo < hi:
        mid = (lo + hi) // 2
        if pop_sorted[mid] <= v:
            lo = mid + 1
        else:
            hi = mid
    return round(100 * lo / len(pop_sorted))


def _stdev(vals):
    n = len(vals)
    if n < 2:
        return 0.0
    m = sum(vals) / n
    var = sum((x - m) ** 2 for x in vals) / n
    return var ** 0.5


def main():
    hitters = _load_json("hitters.json") or {}
    hp_data = _load_json("hitter_percentiles.json") or {}
    if not hitters.get("hitters") or not hp_data.get("hitters"):
        print("[team-grades] missing hitters or hitter_percentiles; skipping",
              file=sys.stderr)
        return 0

    hp_map = hp_data["hitters"]
    h_map = hitters["hitters"]

    # Build BSR / DEF populations from anyone with valid projection — same
    # qualifier pattern index.html uses for _BSR_POP/_FLD_POP.
    bsr_pop = sorted([h.get("bsr") for h in h_map.values()
                      if isinstance(h.get("bsr"), (int, float))])
    fld_pop = sorted([h.get("fld") for h in h_map.values()
                      if isinstance(h.get("fld"), (int, float))])

    teams_out = {}
    cat_collect = {"pwr": [], "eye": [], "bsr": [], "def": []}
    for team_id, abbr in MLB_TEAMS:
        roster = _fetch_roster(team_id)
        if not roster:
            print(f"[team-grades] {abbr}: empty roster", file=sys.stderr)
            continue
        # Each per-roster category mean (skipping hitters with no data)
        pwr_vals, eye_vals, bsr_vals, def_vals = [], [], [], []
        for _, name in roster:
            nm = _norm_name(name)
            hp = hp_map.get(nm)
            h = h_map.get(nm)
            if hp:
                p_pwr = _power_pct(hp)
                p_eye = _eye_pct(hp)
                if p_pwr is not None: pwr_vals.append(p_pwr)
                if p_eye is not None: eye_vals.append(p_eye)
            if h:
                bsr = h.get("bsr")
                fld = h.get("fld")
                if isinstance(bsr, (int, float)):
                    pct = _percentile_of(bsr, bsr_pop)
                    if pct is not None: bsr_vals.append(pct)
                if isinstance(fld, (int, float)):
                    pct = _percentile_of(fld, fld_pop)
                    if pct is not None: def_vals.append(pct)
        # Average each category's percentiles over the roster
        team_row = {}
        if pwr_vals:
            team_row["pwr"] = round(sum(pwr_vals) / len(pwr_vals), 2)
            cat_collect["pwr"].append(team_row["pwr"])
        if eye_vals:
            team_row["eye"] = round(sum(eye_vals) / len(eye_vals), 2)
            cat_collect["eye"].append(team_row["eye"])
        if bsr_vals:
            team_row["bsr"] = round(sum(bsr_vals) / len(bsr_vals), 2)
            cat_collect["bsr"].append(team_row["bsr"])
        if def_vals:
            team_row["def"] = round(sum(def_vals) / len(def_vals), 2)
            cat_collect["def"].append(team_row["def"])
        team_row["n_roster"] = len(roster)
        teams_out[abbr] = team_row

    # League-wide mean + SD per category — these are the fixed reference
    # stats the frontend will z-score against (replacing the slate-dependent
    # _colStat in renderStatcastTeamAvgTable).
    stats = {}
    for cat, vals in cat_collect.items():
        if len(vals) >= 4:
            m = sum(vals) / len(vals)
            sd = _stdev(vals)
            stats[cat] = {"mean": round(m, 3), "sd": round(sd, 3), "n": len(vals)}
        else:
            stats[cat] = {"mean": None, "sd": None, "n": len(vals)}

    payload = {
        "generated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds"),
        "n_teams": len(teams_out),
        "categories": ["pwr", "eye", "bsr", "def"],
        "teams": teams_out,
        "stats": stats,
        "note": "Per-team category means computed over each team's active roster, "
                "then stats = league-wide mean+SD across all 30 teams. Frontend "
                "z-scores each team's raw pct against stats[cat] for a fixed "
                "letter grade independent of which games are in today's slate.",
    }
    OUTPUT.write_text(json.dumps(payload, separators=(",", ":")))
    print(f"[team-grades] wrote {len(teams_out)} teams to {OUTPUT}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
