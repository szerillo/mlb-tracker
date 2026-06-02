#!/usr/bin/env python3
"""
Pull YTD + ROS WAR / OPS / FIP projections from FanGraphs for every hitter
and pitcher, blend the 5-system ROS projections, derive an end-of-season
projection (EOS = YTD + ROS blend), and emit the input file the Player
Futures awards model uses.

Sources (mirrors refresh_projections.py for endpoint pattern):
  Hitters
    ratcdc   → ATC DC ROS         (also: Vol / Skew / Dim — ATC modifiers)
    rthebatx → The BAT X ROS
    roopsydc → OOPSY DC ROS
    rzipsdc  → ZiPS DC ROS
    rsteamer → Steamer ROS
  Pitchers — same five `r*` ROS endpoints, stats=pit.

  YTD season-to-date stats: FG leaders endpoint with type=8 (Dashboard view),
  qual=0 so we capture every player who's appeared. We pull WAR / OPS / R / HR
  / RBI / SB / wRC+ / PA for hitters, and WAR / IP / K / BB / W / SV / ERA /
  WHIP / K-BB% / FIP for pitchers.

Output: data/player_war_projections.json keyed by mlbam_id (when known) or
        normalized full name as fallback.

  {
    "generated_at": ISO, "season": 2026,
    "hitters": {
      "<key>": {
        "name": "Aaron Judge", "team_abbr": "NYY", "league": "AL",
        "ytd": {"war": 4.1, "ops": .991, "r": 56, "hr": 18, "rbi": 49, "sb": 4, "wrc_plus": 178, "pa": 247},
        "ros": {
          "war_blend": 4.8, "ops_blend": .980, "pa_blend": 350,
          "by_system": {"atc": {war,ops,pa,...}, "batx": {...}, ...},
          "vol": 0.40, "skew": 0.10, "dim": 0.55
        },
        "eos": {"war": 8.9, "ops": .985, "r": 110, "hr": 38, ...}
      }, ...
    },
    "pitchers": { ... same shape, pitcher stats ... }
  }

USAGE:
    python scripts/refresh_player_war_projections.py
"""
from __future__ import annotations
import datetime, json, math, sys, time, unicodedata, urllib.request
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
OUTPUT    = REPO_ROOT / "data" / "player_war_projections.json"

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

SEASON = datetime.date.today().year

# 5-system ROS blend.  ATC is special — it also carries Vol/Skew/Dim modifiers.
ROS_SYSTEMS = ["ratcdc", "rthebatx", "roopsydc", "rzipsdc", "rsteamer"]
ROS_SHORT   = {"ratcdc": "atc", "rthebatx": "batx", "roopsydc": "oopsy",
               "rzipsdc": "zips", "rsteamer": "steamer"}

# FG team-name → our internal abbr (matches refresh_team_projections.py + the
# team_futures keyspace).
TEAM_ALIAS = {
    "OAK": "ATH", "WAS": "WSH", "CHW": "CWS",
    "SDP": "SD",  "SFG": "SF",  "KCR": "KC", "TBR": "TB",
}
# League map by abbr (kept inline so we don't need a separate fetch for league
# of every player). Source: 2026 MLB orgs.
AL_TEAMS = {"BAL","BOS","NYY","TB","TOR","CWS","CLE","DET","KC","MIN",
            "HOU","LAA","ATH","SEA","TEX"}
NL_TEAMS = {"ATL","MIA","NYM","PHI","WSH","CHC","CIN","MIL","PIT","STL",
            "ARI","COL","LAD","SD","SF"}


# ─── HTTP helpers ──────────────────────────────────────────────────────────
def _http_json(url, timeout=30, retries=2):
    for attempt in range(retries + 1):
        try:
            req = urllib.request.Request(url, headers={
                "User-Agent": UA,
                "Accept":     "application/json,text/javascript,*/*;q=0.01",
                "Referer":    "https://www.fangraphs.com/",
            })
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return json.loads(r.read().decode("utf-8", errors="replace"))
        except Exception as e:
            if attempt < retries:
                time.sleep(1.5 * (attempt + 1))
                continue
            print(f"  http err ({url[:80]}…): {e}", file=sys.stderr)
            return None


def _norm_name(s):
    if not isinstance(s, str): return ""
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = s.lower()
    for suf in (" jr.", " jr", " sr.", " sr", " iii", " ii"):
        if s.endswith(suf): s = s[:-len(suf)]
    return s.replace(".", "").strip()


def _team_abbr(raw):
    if not raw: return None
    a = (raw or "").strip().upper()
    return TEAM_ALIAS.get(a, a)


def _league(abbr):
    if not abbr: return None
    return "AL" if abbr in AL_TEAMS else "NL" if abbr in NL_TEAMS else None


def _key(name, mlbam_id):
    if mlbam_id: return f"mlb:{mlbam_id}"
    n = _norm_name(name)
    return f"nm:{n}" if n else None


def _f(v):
    """Float-coerce. FG returns numbers as strings sometimes."""
    if v is None or v == "" or v == "--": return None
    try: return float(v)
    except (TypeError, ValueError): return None


def _i(v):
    f = _f(v)
    return int(f) if f is not None else None


# ─── FG endpoints ──────────────────────────────────────────────────────────
def fetch_projection(proj_type, side):
    """side ∈ {'bat','pit'}; returns list[dict] or []."""
    url = (f"https://www.fangraphs.com/api/projections"
           f"?pos=all&type={proj_type}&stats={side}&season={SEASON}&players=0")
    j = _http_json(url) or []
    if not isinstance(j, list): return []
    return j


def fetch_leaders(side):
    """YTD season stats from FG leaders.  type=8 is the Dashboard view (rich
    stat set); qual=0 to keep every player who's appeared."""
    url = (f"https://www.fangraphs.com/api/leaders/major-league/data"
           f"?age=&pos=all&stats={side}&lg=all&qual=0&season={SEASON}"
           f"&season1={SEASON}&type=8&month=0&team=0&pageitems=2000")
    j = _http_json(url, timeout=45) or {}
    # FG returns {"data":[…]} or sometimes just a bare list — handle both.
    if isinstance(j, dict):
        rows = j.get("data") or j.get("leaders") or j.get("rows") or []
    elif isinstance(j, list):
        rows = j
    else:
        rows = []
    return rows


# ─── Hitter parsing ────────────────────────────────────────────────────────
HIT_STAT_MAP = {
    "war":     ("WAR",  _f),
    "ops":     ("OPS",  _f),
    "obp":     ("OBP",  _f),
    "slg":     ("SLG",  _f),
    "avg":     ("AVG",  _f),
    "woba":    ("wOBA", _f),
    "wrc_plus":("wRC+", _f),
    "r":       ("R",    _i),
    "hr":      ("HR",   _i),
    "rbi":     ("RBI",  _i),
    "sb":      ("SB",   _i),
    "pa":      ("PA",   _i),
    "h":       ("H",    _i),
}
PIT_STAT_MAP = {
    "war":  ("WAR",  _f),
    "ip":   ("IP",   _f),
    "k":    ("SO",   _i),
    "bb":   ("BB",   _i),
    "w":    ("W",    _i),
    "sv":   ("SV",   _i),
    "era":  ("ERA",  _f),
    "whip": ("WHIP", _f),
    "fip":  ("FIP",  _f),
    "xfip": ("xFIP", _f),
    "k_bb_pct": ("K-BB%", _f),   # FG sometimes returns "K-BB%" or "kbbpct"
    "qs":   ("QS",   _i),
}


def _pull(row, keymap):
    """Walk a FG row dict pulling stat keys. FG keys are inconsistent across
    endpoints (sometimes 'WAR', sometimes 'war', sometimes 'projWAR'), so try
    several variants for each."""
    out = {}
    for k, (preferred, cast) in keymap.items():
        v = None
        for variant in (preferred, preferred.lower(),
                        preferred.replace("-", "_").lower(),
                        preferred.replace("+", "_plus").lower(),
                        f"proj{preferred}", "p" + preferred.lower()):
            if variant in row:
                v = row[variant]; break
        out[k] = cast(v)
    # ATC modifiers — only present on the ratcdc / Steamer 600 rows
    for atc_k, raw in (("vol", "Vol"), ("skew", "Skew"), ("dim", "Dim")):
        if atc_k not in out and raw in row:
            out[atc_k] = _f(row[raw])
    return out


def _player_meta(row):
    name = row.get("PlayerName") or row.get("playerName") or row.get("name") or ""
    team_abbr = _team_abbr(row.get("Team") or row.get("team") or row.get("AbbName"))
    league    = _league(team_abbr)
    mlbam_id  = row.get("xMLBAMID") or row.get("xmlbamid") or row.get("mlbamid")
    fg_id     = row.get("playerids") or row.get("playerid") or row.get("PlayerId")
    pos       = row.get("Pos") or row.get("pos") or row.get("MinPos")
    try: mlbam_id = int(mlbam_id) if mlbam_id else None
    except (TypeError, ValueError): mlbam_id = None
    return name, team_abbr, league, mlbam_id, fg_id, pos


def _blend(values):
    """Mean of non-None values; None if nothing to blend."""
    xs = [v for v in values if v is not None]
    return (sum(xs) / len(xs)) if xs else None


# ─── Main ─────────────────────────────────────────────────────────────────
def main():
    out = {"generated_at": datetime.datetime.utcnow().isoformat() + "Z",
           "season": SEASON, "hitters": {}, "pitchers": {}}

    for side, label, stat_map, dest in (
        ("bat", "hitters",  HIT_STAT_MAP, "hitters"),
        ("pit", "pitchers", PIT_STAT_MAP, "pitchers")):

        print(f"[player-war] === {label} ===", file=sys.stderr)
        # 1. YTD season-to-date
        print(f"[player-war] fetch YTD leaders ({side})…", file=sys.stderr)
        ytd_rows = fetch_leaders(side)
        ytd_by_key = {}
        for row in ytd_rows:
            name, team, league, mlbam, fg_id, pos = _player_meta(row)
            k = _key(name, mlbam)
            if not k: continue
            ytd_by_key[k] = {
                "name":      name,
                "team_abbr": team,
                "league":    league,
                "mlbam_id":  mlbam,
                "fg_id":     fg_id,
                "pos":       pos,
                "stats":     _pull(row, stat_map),
            }
        print(f"  YTD rows: {len(ytd_rows)} → {len(ytd_by_key)} unique players",
              file=sys.stderr)

        # 2. Each ROS system
        ros_by_key = {}   # key -> {system_short -> stats}
        for proj in ROS_SYSTEMS:
            print(f"[player-war] fetch ROS {proj} ({side})…", file=sys.stderr)
            rows = fetch_projection(proj, side)
            short = ROS_SHORT[proj]
            n_added = 0
            for row in rows:
                name, team, league, mlbam, fg_id, pos = _player_meta(row)
                k = _key(name, mlbam)
                if not k: continue
                stats = _pull(row, stat_map)
                ent = ros_by_key.setdefault(k, {
                    "name": name, "team_abbr": team, "league": league,
                    "mlbam_id": mlbam, "fg_id": fg_id, "pos": pos,
                    "by_system": {}, "vol": None, "skew": None, "dim": None})
                ent["by_system"][short] = stats
                # Capture ATC modifiers from ratcdc only
                if proj == "ratcdc":
                    for atc_k in ("vol", "skew", "dim"):
                        if stats.get(atc_k) is not None:
                            ent[atc_k] = stats[atc_k]
                # Fill metadata from any ROS system if YTD missed the player
                ent["team_abbr"] = ent["team_abbr"] or team
                ent["league"]    = ent["league"]    or league
                ent["mlbam_id"]  = ent["mlbam_id"]  or mlbam
                ent["pos"]       = ent["pos"]       or pos
                n_added += 1
            print(f"  {proj}: {n_added} rows", file=sys.stderr)
            time.sleep(0.6)   # be polite

        # 3. Merge YTD + ROS → EOS per player
        all_keys = set(ytd_by_key) | set(ros_by_key)
        merged = {}
        for k in all_keys:
            ytd_e = ytd_by_key.get(k, {})
            ros_e = ros_by_key.get(k, {})
            name      = ros_e.get("name")      or ytd_e.get("name")
            team_abbr = ros_e.get("team_abbr") or ytd_e.get("team_abbr")
            league    = ros_e.get("league")    or ytd_e.get("league")
            mlbam_id  = ros_e.get("mlbam_id")  or ytd_e.get("mlbam_id")
            fg_id     = ros_e.get("fg_id")     or ytd_e.get("fg_id")
            pos       = ros_e.get("pos")       or ytd_e.get("pos")

            # ROS blends
            by_sys = ros_e.get("by_system", {})
            ros_blend = {}
            for stat in stat_map.keys():
                ros_blend[stat] = _blend([s.get(stat) for s in by_sys.values()])
            # Add ATC modifiers
            ros_blend["vol"]  = ros_e.get("vol")
            ros_blend["skew"] = ros_e.get("skew")
            ros_blend["dim"]  = ros_e.get("dim")

            ytd_stats = ytd_e.get("stats", {})

            # EOS — additive for counting stats, weighted avg for rate stats
            #       (weight = playing-time proxy = PA for hitters, IP for pitchers).
            eos = {}
            pt_key = "pa" if side == "bat" else "ip"
            ytd_pt = ytd_stats.get(pt_key) or 0
            ros_pt = ros_blend.get(pt_key) or 0
            tot_pt = (ytd_pt or 0) + (ros_pt or 0)
            counting = {"r","hr","rbi","sb","pa","h","k","bb","w","sv","ip","qs","war"}
            for stat in stat_map.keys():
                y = ytd_stats.get(stat); r = ros_blend.get(stat)
                if stat in counting:
                    s = (y or 0) + (r or 0) if (y is not None or r is not None) else None
                    eos[stat] = round(s, 3) if isinstance(s, float) else s
                else:
                    # PT-weighted rate
                    if y is None and r is None:
                        eos[stat] = None
                    elif y is None:
                        eos[stat] = r
                    elif r is None:
                        eos[stat] = y
                    elif tot_pt:
                        eos[stat] = round((y * (ytd_pt or 0) + r * (ros_pt or 0)) / tot_pt, 4)
                    else:
                        eos[stat] = round((y + r) / 2, 4)

            merged[k] = {
                "name": name, "team_abbr": team_abbr, "league": league,
                "mlbam_id": mlbam_id, "fg_id": fg_id, "pos": pos,
                "ytd": ytd_stats or None,
                "ros": {
                    "by_system": by_sys,
                    "blend": ros_blend,
                },
                "eos": eos,
            }

        out[dest] = merged
        print(f"  merged: {len(merged)} {label}", file=sys.stderr)

    # PRESERVE-ON-EMPTY guard (FG occasionally 503s the projections endpoint)
    if not out["hitters"] and not out["pitchers"]:
        if OUTPUT.exists():
            print("[player-war] both buckets empty — keeping prior file",
                  file=sys.stderr)
            return 0

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(json.dumps(out, indent=2))
    print(f"[player-war] wrote {len(out['hitters'])} hitters + "
          f"{len(out['pitchers'])} pitchers → {OUTPUT}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
