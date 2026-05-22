"""
Compute bullpen fatigue, keyed by VIEWED DATE.

For each target date D in [today .. today+LOOKAHEAD], the fatigue window is the
5 COMPLETED calendar days BEFORE D (never D itself). So:

  • Viewing today  → window = [today-5 .. today-1]; today's own usage is NOT shown.
  • Viewing tomorrow → window = [tomorrow-5 .. tomorrow-1] = [today-4 .. today];
    "yesterday" relative to tomorrow is today, which fills in once today's games
    final (0 until then).

This removes the old "everyone is 0 for the most recent day" confusion — the
target date is never in its own window, so there's no phantom 0 column and no
"already pitched today" ambiguity.

Output: data/fatigue.json
{
  "generated_at": "<ISO>",
  "source": "MLB Stats API (box scores)",
  "lookahead_days": 5,
  "dates": {
    "2026-05-22": {
       "window": {"start": "2026-05-17", "end": "2026-05-21"},
       "day_labels": ["2026-05-17", ... "2026-05-21"],   # 5 days BEFORE the key
       "teams": { "<team>": [ {name, days, total, tier, reasons}, ... ] },
       "team_meta": { "<team>": {"last_final": iso|null, "pending_today": bool} }
    },
    ...
  }
}
"""
import json, os, datetime, urllib.request, unicodedata, concurrent.futures
from collections import defaultdict

OUTPUT = os.path.join(os.path.dirname(__file__), "..", "data", "fatigue.json")

LOOKAHEAD_DAYS = 5   # emit fatigue for today + next 5 days

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
    # days = [D-5, D-4, D-3, D-2, D-1] — the 5 COMPLETED days before target date D.
    # d5 = D-1 = "yesterday" relative to D. D itself is NOT in the window, so
    # there's no "pitched today already" case to worry about.
    #
    # Tier ladder:
    #   LIKELY OUT — high-confidence unavailable
    #   FATIGUED   — likely available but worked recently
    #   AVAILABLE  — fresh
    #
    # Tuned so a single <=30-pitch outing yesterday with no other work stays
    # AVAILABLE (a normal day's work; rested the next day). Only flag genuinely
    # heavy / stacked usage.
    d1, d2, d3, d4, d5 = days
    total = sum(days)
    apps = sum(1 for x in days if x > 0)
    apps_last4 = sum(1 for x in (d2, d3, d4, d5) if x > 0)
    b2b = (d4 > 0 and d5 > 0)  # pitched the 2 days immediately before D
    reasons = []
    tier = "AVAILABLE"
    # LIKELY OUT
    if apps_last4 >= 3: reasons.append(f"{apps_last4}-in-4"); tier = "LIKELY OUT"
    if b2b and (d4 + d5) >= 40: reasons.append(f"B2B {d4}+{d5}"); tier = "LIKELY OUT"
    if d5 > 30: reasons.append(f"{d5}p yesterday"); tier = "LIKELY OUT"
    if total >= 60: reasons.append(f"{total}p/5d"); tier = "LIKELY OUT"
    # FATIGUED
    if tier != "LIKELY OUT":
        if b2b and (d4 + d5) >= 25:
            reasons.append(f"B2B {d4}+{d5}"); tier = "FATIGUED"
        elif apps >= 4:
            reasons.append(f"{apps} apps/5d"); tier = "FATIGUED"
        elif total >= 55:
            reasons.append(f"{total}p/5d"); tier = "FATIGUED"
    return tier, reasons


def main():
    import sys
    sys.path.insert(0, os.path.dirname(__file__))
    from _common import skip_if_not_in_window
    if skip_if_not_in_window("compute_fatigue"):
        return

    # ET business day
    et_now = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=4)
    today = et_now.date()

    # Calendar dates we need usage for: the union of every target date's window.
    # Target dates run today .. today+LOOKAHEAD. The earliest window day is
    # today-5; the latest meaningful one is today (future days have no games yet
    # -> 0). So pull box scores for [today-5 .. today].
    earliest = today - datetime.timedelta(days=5)
    pull_dates = [(earliest + datetime.timedelta(days=i)).isoformat()
                  for i in range((today - earliest).days + 1)]  # today-5 .. today

    # usage[date_iso][team][pitcher] = pitches
    usage = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
    last_final = {}        # team -> iso (latest Final date counted)
    pending_today = set()  # teams with a non-final game today
    today_iso = today.isoformat()
    FINAL_STATUSES = ("Final", "Game Over", "Completed Early")
    PENDING_STATUSES = ("Scheduled", "Pre-Game", "Warmup", "Delayed Start",
                        "In Progress", "Manager challenge")

    for date in pull_dates:
        games = get_pks(date)
        if not games:
            continue
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as ex:
            boxes = list(ex.map(get_box, [g[0] for g in games]))
        for (_, a, h, status), b in zip(games, boxes):
            if status in FINAL_STATUSES:
                for team in (a, h):
                    cur = last_final.get(team)
                    if cur is None or date > cur:
                        last_final[team] = date
            if date == today_iso and status in PENDING_STATUSES:
                pending_today.add(a); pending_today.add(h)
            if status in ("Postponed", "Cancelled") or status in PENDING_STATUSES:
                continue
            for team, pitchers in extract_relievers(b).items():
                for n, p in pitchers.items():
                    if p > 0:
                        usage[date][team][n] += p

    # All teams we've seen usage / a game for.
    all_teams = set()
    for date in usage:
        all_teams.update(usage[date].keys())
    all_teams.update(last_final.keys())
    all_teams.update(pending_today)

    # Build per-target-date fatigue.
    out_dates = {}
    target_dates = [(today + datetime.timedelta(days=i)).isoformat()
                    for i in range(LOOKAHEAD_DAYS + 1)]
    for D_iso in target_dates:
        D = datetime.date.fromisoformat(D_iso)
        window = [(D - datetime.timedelta(days=k)).isoformat() for k in range(5, 0, -1)]
        # window = [D-5, D-4, D-3, D-2, D-1]

        teams_out = {}
        for team in all_teams:
            names = set()
            for wd in window:
                names.update(usage.get(wd, {}).get(team, {}).keys())
            rows = []
            for p in names:
                days = [usage.get(wd, {}).get(team, {}).get(p, 0) for wd in window]
                if sum(days) == 0:
                    continue
                tier, reasons = classify(days)
                rows.append({
                    "name": p,
                    "days": days,
                    "total": sum(days),
                    "tier": None if tier == "AVAILABLE" else tier,
                    "reasons": "" if tier == "AVAILABLE" else "; ".join(reasons),
                })
            if not rows:
                continue

            def sort_key(r):
                rank = {"LIKELY OUT": 0, "FATIGUED": 1}.get(r["tier"], 2)
                return (rank, -r["days"][4], -r["total"])
            rows.sort(key=sort_key)
            teams_out[team] = rows

        meta = {}
        for team in all_teams:
            meta[team] = {
                "last_final": last_final.get(team),
                "pending_today": (D_iso == today_iso and team in pending_today),
            }

        out_dates[D_iso] = {
            "window": {"start": window[0], "end": window[-1]},
            "day_labels": window,
            "teams": teams_out,
            "team_meta": meta,
        }

    payload = {
        "generated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "source": "MLB Stats API (box scores)",
        "lookahead_days": LOOKAHEAD_DAYS,
        "dates": out_dates,
    }

    os.makedirs(os.path.dirname(OUTPUT), exist_ok=True)
    with open(OUTPUT, "w") as f:
        json.dump(payload, f, indent=2)
    n_today = len(out_dates.get(today_iso, {}).get("teams", {}))
    print(f"  wrote fatigue for {len(out_dates)} dates "
          f"({today_iso} -> {target_dates[-1]}); {n_today} teams on today's view")


if __name__ == "__main__":
    main()
