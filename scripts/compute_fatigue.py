"""
Compute bullpen fatigue across the last 5 days of MLB games.
Outputs data/fatigue.json with per-team, per-pitcher tier (LIKELY OUT, FATIGUED, MONITOR).
"""
import json, os, datetime, urllib.request, unicodedata, concurrent.futures
from collections import defaultdict

OUTPUT = os.path.join(os.path.dirname(__file__), "..", "data", "fatigue.json")

NAME_MAP = {
    "Emilio Pagán": "Emilio Pagan", "Yoendrys Gómez": "Yoendrys Gomez",
    "Yohan Ramírez": "Yohan Ramirez", "Seranthony Domínguez": "Seranthony Dominguez",
    "Jonathan Loáisiga": "Jonathan Loaisiga", "José Alvarado": "Jose Alvarado",
    "Daniel Lynch IV": "Daniel Lynch", "Edwin Díaz": "Edwin Diaz",
    "Cionel Pérez": "Cionel Perez", "Huascar Brazobán": "Huascar Brazoban",
    "José Urquidy": "Jose Urquidy", "Jose A. Ferrer": "Jose Ferrer",
    "Andrés Muñoz": "Andres Munoz", "Mark Leiter Jr.": "Mark Leiter",
    "Jovani Morán": "Jovani Moran", "Luis García": "Luis Garcia",
    "José Suarez": "Jose Suarez", "Albert Suárez": "Albert Suarez",
    "P.J. Higgins": "PJ Higgins", "J.P. France": "JP France",
    "Rolddy Muñoz": "Rolddy Munoz",
}


def norm(name):
    if name in NAME_MAP:
        return NAME_MAP[name]
    nfkd = unicodedata.normalize('NFKD', name)
    return "".join(c for c in nfkd if not unicodedata.combining(c))


def fetch(url):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "fatigue/1.0"})
        with urllib.request.urlopen(req, timeout=30) as r:
            return json.loads(r.read())
    except Exception as e:
        print(f"ERR {url}: {e}")
        return None


def get_pks(date):
    d = fetch(f"https://statsapi.mlb.com/api/v1/schedule?sportId=1&date={date}")
    if not d or not d.get("dates"):
        return []
    return [(g["gamePk"], g["teams"]["away"]["team"]["name"],
             g["teams"]["home"]["team"]["name"], g["status"]["detailedState"])
            for g in d["dates"][0].get("games", [])]


def get_box(pk):
    return fetch(f"https://statsapi.mlb.com/api/v1/game/{pk}/boxscore")


def extract_relievers(box):
    """Return team_name -> pitcher -> pitch_count, filtering out true starters only."""
    res = {}
    if not box or "teams" not in box:
        return res
    for side in ("away", "home"):
        t = box["teams"].get(side, {})
        tn = t.get("team", {}).get("name", "?")
        rps = {}
        for pid in t.get("pitchers", []):
            p = t.get("players", {}).get(f"ID{pid}")
            if not p:
                continue
            s = (p.get("stats") or {}).get("pitching") or {}
            if not s:
                continue
            gs = int(s.get("gamesStarted") or 0)
            pitches = int(s.get("numberOfPitches") or 0)
            pos = ((p.get("position") or {}).get("abbreviation") or "P")
            if pos != "P":
                continue
            if gs > 0:
                continue  # exclude true starters; keep bulk relief
            name = norm(p.get("person", {}).get("fullName", "?"))
            rps[name] = pitches
        res[tn] = rps
    return res


def classify(days):
    d1, d2, d3, d4, d5 = days
    total = sum(days)
    apps_last4 = sum(1 for x in (d2, d3, d4, d5) if x > 0)
    apps_5 = sum(1 for x in days if x > 0)
    b2b = (d4 > 0 and d5 > 0)
    reasons = []
    tier = "AVAILABLE"
    if apps_last4 >= 3: reasons.append(f"{apps_last4}-in-4"); tier = "LIKELY OUT"
    if b2b and (d4 + d5) >= 30: reasons.append(f"B2B {d4}+{d5}"); tier = "LIKELY OUT"
    if d5 > 30: reasons.append(f"{d5}p yesterday"); tier = "LIKELY OUT"
    if total >= 60: reasons.append(f"{total}p/5d"); tier = "LIKELY OUT"
    if tier != "LIKELY OUT":
        if b2b: reasons.append(f"B2B {d4}+{d5}"); tier = "FATIGUED"
        elif apps_5 >= 3: reasons.append(f"{apps_5} apps/5d"); tier = "FATIGUED"
        elif total >= 45: reasons.append(f"{total}p/5d"); tier = "FATIGUED"
    return tier, reasons


def main():
    import sys
    sys.path.insert(0, os.path.dirname(__file__))
    from _common import skip_if_not_in_window
    if skip_if_not_in_window("compute_fatigue"):
        return
    # Treat "today" as the MLB business day in ET, not UTC — matters around
    # midnight ET when UTC has already rolled to the next day but late West
    # Coast games are still finishing.
    et_now = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=4)
    today = et_now.date()
    # 5-day rolling window — today back 4 days (so completed games earlier
    # today are captured on the same evening, not waiting for midnight ET).
    # Games still in progress / scheduled are skipped by the extract step below.
    dates = [(today - datetime.timedelta(days=d)).isoformat() for d in range(4, -1, -1)]
    print(f"Building fatigue for window: {dates[0]} .. {dates[-1]}")

    # Track per-team -> pitcher -> 5-day array
    usage = defaultdict(lambda: defaultdict(lambda: [0, 0, 0, 0, 0]))
    # Track per-team freshness metadata so the UI can show "Thru Wed" markers.
    # last_final = latest date (ISO) that has a Final game counted into usage
    # pending    = True if any scheduled/in-progress game today hasn't Final'd yet
    team_meta = defaultdict(lambda: {"last_final": None, "pending_today": False})
    today_iso = dates[-1]
    FINAL_STATUSES = ("Final", "Game Over", "Completed Early")
    PENDING_STATUSES = ("Scheduled", "Pre-Game", "Warmup", "Delayed Start",
                        "In Progress", "Manager challenge")
    for idx, date in enumerate(dates):
        games = get_pks(date)
        if not games:
            continue
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as ex:
            boxes = list(ex.map(get_box, [g[0] for g in games]))
        for (_, a, h, status), b in zip(games, boxes):
            # Note freshness per team before filtering.
            if status in FINAL_STATUSES:
                for team in (a, h):
                    cur = team_meta[team]["last_final"]
                    if cur is None or date > cur:
                        team_meta[team]["last_final"] = date
            if date == today_iso and status in PENDING_STATUSES:
                team_meta[a]["pending_today"] = True
                team_meta[h]["pending_today"] = True
            # Skip any game that hasn't reached a reliable box-score state.
            if status in ("Postponed", "Cancelled") or status in PENDING_STATUSES:
                continue
            for team, pitchers in extract_relievers(b).items():
                for n, p in pitchers.items():
                    if p > 0:
                        usage[team][n][idx] += p

    # Classify per team/pitcher — emit a row for EVERY pitcher with any usage
    # (frontend shows L5 Days for rested arms too; tier is "OK" when no flag)
    out_teams = {}
    for team, pmap in usage.items():
        rows = []
        for p, days in pmap.items():
            tier, reasons = classify(days)
            if tier == "AVAILABLE":
                # Emit with tier=null so the frontend can show usage but not
                # bucket the pitcher into RESTED-vs-flagged incorrectly.
                rows.append({
                    "name": p,
                    "days": days,
                    "total": sum(days),
                    "tier": None,
                    "reasons": "",
                })
            else:
                rows.append({
                    "name": p,
                    "days": days,
                    "total": sum(days),
                    "tier": tier,
                    "reasons": "; ".join(reasons),
                })
        # Sort: LIKELY OUT first, FATIGUED next, then rested — within each by recency
        def sort_key(r):
            tier_rank = {"LIKELY OUT": 0, "FATIGUED": 1}.get(r["tier"], 2)
            return (tier_rank, -r["days"][4], -r["total"])
        rows.sort(key=sort_key)
        if rows:
            out_teams[team] = rows

    # Serialize team_meta for teams present in out_teams (plus any team that
    # had a Final / pending game even if they had no bullpen usage recorded).
    team_meta_out = {}
    for team in set(list(out_teams.keys()) + list(team_meta.keys())):
        m = team_meta.get(team, {})
        team_meta_out[team] = {
            "last_final": m.get("last_final"),
            "pending_today": bool(m.get("pending_today", False)),
        }

    payload = {
        "generated_at": today.isoformat(),
        "window": {"start": dates[0], "end": dates[-1]},
        "day_labels": dates,
        "source": "MLB Stats API (box scores)",
        "teams": out_teams,
        "team_meta": team_meta_out,
    }

    os.makedirs(os.path.dirname(OUTPUT), exist_ok=True)
    with open(OUTPUT, "w") as f:
        json.dump(payload, f, indent=2)
    print(f"  wrote {len(out_teams)} teams to {OUTPUT}")


if __name__ == "__main__":
    main()
