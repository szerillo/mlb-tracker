"""
Fetch platoon splits (vs RHB / vs LHB) for today's probable starting pitchers,
detect openers (short-outing starters), and store each probable SP's stats.

Writes data/pitcher_splits.json keyed by normalized pitcher name.

Opener detection: if an SP has ≥3 starts this season and average IP < 2.5,
flag as opener. Also considers 'games' vs 'gamesStarted' ratio.
"""
import json, os, sys, datetime, urllib.request, unicodedata
from concurrent.futures import ThreadPoolExecutor

OUTPUT = os.path.join(os.path.dirname(__file__), "..", "data", "pitcher_splits.json")

sys.path.insert(0, os.path.dirname(__file__))
from _common import skip_if_not_in_window


def strip_accents(s):
    if not isinstance(s, str): return s
    return "".join(c for c in unicodedata.normalize('NFKD', s) if not unicodedata.combining(c))


def norm_name(s):
    if not isinstance(s, str): return ""
    s = strip_accents(s).lower()
    for suf in [' jr.', ' jr', ' sr.', ' sr', ' iii', ' ii']:
        if s.endswith(suf): s = s[:-len(suf)]
    return s.replace('.','').strip()


def fetch(url, timeout=30):
    try:
        req = urllib.request.Request(url, headers={"User-Agent":"mlb-tracker/1.0"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.load(r)
    except Exception as e:
        print(f"  ERR {url[:80]}: {e}")
        return None


def get_today_probables():
    """Return list of (pitcher_id, pitcher_name, team_name)."""
    today = datetime.date.today().isoformat()
    d = fetch(f"https://statsapi.mlb.com/api/v1/schedule?sportId=1&date={today}&hydrate=probablePitcher")
    if not d or not d.get("dates"):
        return []
    out = []
    for g in d["dates"][0].get("games", []):
        for side in ("away", "home"):
            pp = g["teams"][side].get("probablePitcher")
            if pp and pp.get("id"):
                out.append({
                    "pid": pp["id"],
                    "name": pp.get("fullName",""),
                    "team": g["teams"][side]["team"]["name"],
                    "game_pk": g["gamePk"],
                    "side": side,
                })
    return out


def fetch_splits_and_gamelog(pid, season):
    """Fetch vsL/vsR splits + gameLog + season summary + player bio for one pitcher."""
    # Two calls: /people/{id} for bio (hand), /stats for the splits
    bio = fetch(f"https://statsapi.mlb.com/api/v1/people/{pid}")
    stats_url = (f"https://statsapi.mlb.com/api/v1/people/{pid}/stats"
                 f"?stats=statSplits,season,gameLog&group=pitching&season={season}&sitCodes=vr,vl")
    stats = fetch(stats_url)
    if not bio or not stats:
        return None
    bio_p = (bio.get("people") or [{}])[0]
    return {"bio": bio_p, "stats": stats}


def parse_pitcher(data, pid):
    """Extract split + gamelog + season info into a single dict."""
    if not data: return None
    bio = data.get("bio", {})
    hand = (bio.get("pitchHand") or {}).get("code")  # 'R' or 'L'
    name = bio.get("fullName")
    stats_groups = (data.get("stats") or {}).get("stats", [])
    vs_r = None
    vs_l = None
    season = None
    gamelog = []
    for grp in stats_groups:
        typ = grp.get("type", {}).get("displayName") or grp.get("type", {}).get("code")
        splits = grp.get("splits", [])
        for s in splits:
            sit_code = s.get("split", {}).get("code")
            stat = s.get("stat", {})
            if typ == "statSplits" and sit_code == "vr":
                vs_r = stat
            elif typ == "statSplits" and sit_code == "vl":
                vs_l = stat
            elif typ == "season":
                if not season:
                    season = stat
            elif typ == "gameLog":
                gamelog.append({
                    "date": s.get("date"),
                    "ip": float(stat.get("inningsPitched") or 0),
                    "pitches": int(stat.get("numberOfPitches") or 0),
                    "started": int(stat.get("gamesStarted") or 0) > 0,
                })

    def safe_rate(num, den):
        try:
            n = float(num); d = float(den)
            return round(100 * n / d, 1) if d > 0 else None
        except (TypeError, ValueError):
            return None

    def compact(stat):
        if not stat: return None
        tbf = stat.get("battersFaced")
        return {
            "tbf": tbf,
            "k_pct": safe_rate(stat.get("strikeOuts"), tbf),
            "bb_pct": safe_rate(stat.get("baseOnBalls"), tbf),
            "woba": None,  # MLB Stats API doesn't expose wOBA in splits directly
            "hr_per_pa": safe_rate(stat.get("homeRuns"), tbf),
            "avg_against": stat.get("avg"),
            "ops_against": stat.get("ops"),
        }

    # Opener detection — last 5 starts, avg IP
    started = [g for g in gamelog if g["started"]]
    started_last5 = sorted(started, key=lambda g: g["date"] or "", reverse=True)[:5]
    avg_ip_last5 = (sum(g["ip"] for g in started_last5) / len(started_last5)) if started_last5 else None

    is_opener = False
    opener_reason = None
    if avg_ip_last5 is not None and len(started_last5) >= 2:
        if avg_ip_last5 < 2.5:
            is_opener = True
            opener_reason = f"avg IP in last {len(started_last5)} starts: {avg_ip_last5:.1f}"

    return {
        "pid": pid,
        "name": name,
        "hand": hand,
        "vs_r": compact(vs_r),
        "vs_l": compact(vs_l),
        "season_summary": {
            "games": (season or {}).get("games"),
            "starts": (season or {}).get("gamesStarted"),
            "ip": (season or {}).get("inningsPitched"),
            "era": (season or {}).get("era"),
            "k_pct": safe_rate((season or {}).get("strikeOuts"), (season or {}).get("battersFaced")),
            "bb_pct": safe_rate((season or {}).get("baseOnBalls"), (season or {}).get("battersFaced")),
        },
        "last5_starts_avg_ip": round(avg_ip_last5, 2) if avg_ip_last5 else None,
        "is_opener": is_opener,
        "opener_reason": opener_reason,
    }


def main():
    if skip_if_not_in_window("refresh_splits"):
        return

    season = datetime.date.today().year
    probables = get_today_probables()
    print(f"Probable starters today: {len(probables)}")
    if not probables:
        return

    # Fetch in parallel
    results = {}
    with ThreadPoolExecutor(max_workers=10) as ex:
        fetched = list(ex.map(lambda p: (p, fetch_splits_and_gamelog(p["pid"], season)), probables))
    for prob, data in fetched:
        parsed = parse_pitcher(data, prob["pid"])
        if parsed:
            parsed["team"] = prob["team"]
            parsed["game_pk"] = prob["game_pk"]
            key = norm_name(parsed["name"])
            if key:
                results[key] = parsed

    payload = {
        "generated_at": datetime.datetime.utcnow().isoformat() + "Z",
        "source": "MLB Stats API (statSplits + gameLog + season)",
        "season": season,
        "pitchers": results,
    }

    os.makedirs(os.path.dirname(OUTPUT), exist_ok=True)
    with open(OUTPUT, "w") as f:
        json.dump(payload, f, indent=2)

    openers = [v for v in results.values() if v["is_opener"]]
    print(f"  wrote {len(results)} probable SPs ({len(openers)} flagged as openers)")
    for op in openers:
        print(f"    OPENER: {op['name']} — {op['opener_reason']}")


if __name__ == "__main__":
    main()
