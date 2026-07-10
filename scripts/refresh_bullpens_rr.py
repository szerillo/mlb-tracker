#!/usr/bin/env python3
"""
Official bullpens from FanGraphs Roster Resource -> data/bullpens_rr.csv (+ .json)

Replaces the Action-Network-pushed "Bullpen Dump" with a clean, official feed:
  - who's in each team's active bullpen + their role order (CL/SU/MID/LR) comes
    from FanGraphs Roster Resource depth charts (type == 'mlb-bp');
  - the CORRECT team for each reliever comes from MLB StatsAPI via mlbamid
    (Roster Resource's own team label is unreliable);
  - formal player names (no informal-name cleanup);
  - Workload flag = TRUE when Bartolo's fatigue model flags the reliever as
    FATIGUED or LIKELY OUT.

CSV columns match the existing Bullpen Dump layout so it's a drop-in via
=IMPORTDATA() into a dedicated tab:
  A Team(nickname) | B Position | C PlayerID(mlbam) | D Player |
  E (const 8) | F number 1..N in role order | G "Team N" concat | H Workload
"""
import json, os, sys, time, urllib.request, collections

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
OUT_CSV = os.path.join(REPO_ROOT, "data", "bullpens_rr.csv")
OUT_JSON = os.path.join(REPO_ROOT, "data", "bullpens_rr.json")
FATIGUE = os.path.join(REPO_ROOT, "data", "fatigue.json")

RR_URL = "https://www.fangraphs.com/api/depth-charts/roster?teamid={tid}"
PEOPLE_URL = "https://statsapi.mlb.com/api/v1/people?personIds={ids}&hydrate=currentTeam"
UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120 Safari/537.36")

# MLB full name -> the nickname Sean's model/sheet uses (matches Bullpen Dump).
NICK = {
    "Arizona Diamondbacks": "Diamondbacks", "Athletics": "Athletics",
    "Atlanta Braves": "Braves", "Baltimore Orioles": "Orioles",
    "Boston Red Sox": "Red Sox", "Chicago Cubs": "Cubs",
    "Chicago White Sox": "White Sox", "Cincinnati Reds": "Reds",
    "Cleveland Guardians": "Guardians", "Colorado Rockies": "Rockies",
    "Detroit Tigers": "Tigers", "Houston Astros": "Astros",
    "Kansas City Royals": "Royals", "Los Angeles Angels": "Angels",
    "Los Angeles Dodgers": "Dodgers", "Miami Marlins": "Marlins",
    "Milwaukee Brewers": "Brewers", "Minnesota Twins": "Twins",
    "New York Mets": "Mets", "New York Yankees": "Yankees",
    "Philadelphia Phillies": "Phillies", "Pittsburgh Pirates": "Pirates",
    "San Diego Padres": "Padres", "San Francisco Giants": "Giants",
    "Seattle Mariners": "Mariners", "St. Louis Cardinals": "Cardinals",
    "Tampa Bay Rays": "Rays", "Texas Rangers": "Rangers",
    "Toronto Blue Jays": "Blue Jays", "Washington Nationals": "Nationals",
}

# Role -> sort priority so the bullpen numbers 1..N read CL -> setup -> mid -> long.
def _role_pri(role):
    r = (role or "").upper()
    if r == "CL": return 0
    if r.startswith("SU"):
        # SU8 (8th) ahead of SU7 (7th); bare SU in between
        try: return 10 - int(r[2:])
        except Exception: return 5
    if r in ("MID", "MR"): return 20
    if r in ("LR", "LONG"): return 30
    return 40


def _get(url, tries=3, timeout=30):
    for _ in range(tries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA})
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return json.load(r)
        except Exception:
            time.sleep(1.0)
    return None


import re as _re
import unicodedata as _ud
def _deaccent(s):
    """Strip diacritics to plain ASCII (José -> Jose, Muñoz -> Munoz)."""
    return _ud.normalize("NFKD", s or "").encode("ascii", "ignore").decode("ascii")


def _norm(n):
    return _deaccent(n).lower().replace(".", "").replace("'", "").replace("-", " ").strip()


def _clean_name(n):
    """Canonical display name. Roster Resource's `player` field carries quirks
    (team tags 'Abner Uribe (mil)', middle initials 'Fernando E. Cruz', formal
    first names 'Peter Fairbanks'); we prefer StatsAPI fullName upstream, then
    here: drop trailing generational suffixes (Jr./Sr./II/III/IV) and strip
    Spanish accents incl. ñ, to match the name convention in Sean's model."""
    n = (n or "").strip()
    n = _re.sub(r"\s*\([^)]*\)\s*$", "", n)                       # drop trailing (TAG)
    n = _re.sub(r"\s+(Jr\.?|Sr\.?|IV|III|II)$", "", n, flags=_re.I)  # drop Jr/Sr/II/III/IV
    n = _deaccent(n)                                              # José -> Jose, ñ -> n
    return n.strip()


def _slate_date():
    """The slate the bullpen supports — match it so e.g. 7/10 games use 7/10
    rest/fatigue. Prefer SLATE_DATE env, else the date on the live sheet
    projections (same slate Sean is projecting), else ET business day."""
    import datetime as _dt
    env = os.environ.get("SLATE_DATE", "").strip()
    if env:
        return env
    try:
        sp = json.load(open(os.path.join(REPO_ROOT, "data", "sheet_projections.json")))
        if sp.get("date"):
            return sp["date"]
    except Exception:
        pass
    return (_dt.datetime.utcnow() - _dt.timedelta(hours=4)).date().isoformat()


def load_fatigue(slate):
    """(team_fullname, normalized_name) set flagged not-available FOR the slate
    date (bullpen rest is date-specific)."""
    flagged = set()
    try:
        d = json.load(open(FATIGUE))
        dates = d.get("dates", {})
        if not dates:
            return flagged, None
        if slate in dates:
            day = slate
        else:
            # nearest available date to the slate (fatigue only carries a few days)
            day = min(dates.keys(), key=lambda k: abs(
                (__import__("datetime").date.fromisoformat(k)
                 - __import__("datetime").date.fromisoformat(slate)).days)
                if _isdate(k) and _isdate(slate) else 9999)
        for team, rows in (dates[day].get("teams", {}) or {}).items():
            for r in rows:
                if r.get("tier") in ("FATIGUED", "LIKELY OUT"):
                    flagged.add((team, _norm(r.get("name"))))
        return flagged, day
    except Exception as e:
        print(f"[bullpens] fatigue load failed ({e}); workload all FALSE")
        return flagged, None


def _isdate(s):
    try:
        __import__("datetime").date.fromisoformat(s); return True
    except Exception:
        return False


def main():
    # 1) Collect every mlb-bp reliever across all 30 RR team pages (dedup by id).
    pen = collections.OrderedDict()   # mlbamid -> {player, role, ord}
    for tid in range(1, 31):
        d = _get(RR_URL.format(tid=tid))
        if not d:
            print(f"[bullpens] teamid {tid} fetch failed; skipping")
            continue
        bp = [r for r in d if r.get("type") == "mlb-bp"]
        for i, r in enumerate(bp):
            mid = r.get("mlbamid")
            if mid and mid not in pen:
                pen[mid] = {"player": r.get("player"), "role": r.get("role"), "ord": i}
        time.sleep(0.4)
    if not pen:
        print("[bullpens] no relievers collected; leaving previous feed intact", file=sys.stderr)
        return 1
    print(f"[bullpens] collected {len(pen)} unique relievers from Roster Resource")

    # 2) Resolve each reliever's real team from StatsAPI (mlbamid -> currentTeam).
    ids = list(pen.keys())
    team_of = {}
    name_of = {}   # mlbamid -> canonical StatsAPI fullName (clean common name)
    for j in range(0, len(ids), 100):
        chunk = ids[j:j + 100]
        d = _get(PEOPLE_URL.format(ids=",".join(map(str, chunk))))
        for p in (d or {}).get("people", []):
            team_of[p["id"]] = (p.get("currentTeam", {}) or {}).get("name")
            name_of[p["id"]] = p.get("fullName")
        time.sleep(0.3)

    # 3) Group by team, order by role, number 1..N.
    byteam = collections.defaultdict(list)
    for mid, info in pen.items():
        full = team_of.get(mid)
        if not full:
            continue
        byteam[full].append((mid, info["player"], info["role"], info["ord"]))

    slate = _slate_date()
    flagged, fdate = load_fatigue(slate)
    print(f"[bullpens] slate {slate}; using fatigue for {fdate} ({len(flagged)} relievers flagged)")
    rows = []       # CSV rows (list of 8)
    games = {}      # json: nickname -> [ {num, player, mlbamid, role, workload} ]
    for full in sorted(byteam):
        nick = NICK.get(full) or full.split()[-1]
        pen_sorted = sorted(byteam[full], key=lambda x: (_role_pri(x[2]), x[3]))
        arr = []
        for num, (mid, rr_player, role, _o) in enumerate(pen_sorted, start=1):
            # canonical name: StatsAPI fullName if we have it, else cleaned RR name
            player = _clean_name(name_of.get(mid) or rr_player)
            workload = (full, _norm(player)) in flagged
            rows.append([nick, "RP", mid, player, 8, num, f"{nick} {num}",
                         "TRUE" if workload else "FALSE"])
            arr.append({"num": num, "player": player, "mlbamid": mid,
                        "role": role, "workload": workload})
        games[nick] = arr

    # 4) Write CSV (header matches Bullpen Dump) + JSON.
    os.makedirs(os.path.dirname(OUT_CSV), exist_ok=True)
    import csv
    with open(OUT_CSV, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["Team", "Position", "PlayerID", "Player", "", "", "", "Workload"])
        for r in rows:
            w.writerow(r)
    payload = {
        "generated_at": __import__("datetime").datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "source": "FanGraphs Roster Resource (mlb-bp) + MLB StatsAPI team + Bartolo fatigue",
        "slate_date": slate, "fatigue_date": fdate,
        "n_teams": len(games), "n_relievers": len(rows),
        "teams": games,
    }
    with open(OUT_JSON, "w") as f:
        json.dump(payload, f, indent=2)
    flagged_ct = sum(1 for r in rows if r[7] == "TRUE")
    print(f"[bullpens] wrote {len(rows)} relievers across {len(games)} teams "
          f"({flagged_ct} flagged workload) -> {OUT_CSV}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
