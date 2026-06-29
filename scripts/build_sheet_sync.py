#!/usr/bin/env python3
"""
Emit data/sheet_sync.json — a tiny, pre-joined feed the projection-model Google
Sheet pulls (via an Apps Script checkbox) to overwrite each matchup block's
weather % (J, header+3) and HP-umpire name (J, header+5).

KEY
---
Keyed by Action Network event id (the "GameID" the model already stores in
column L of every "Matchup N" block), so the sheet-side join is an exact id
lookup — no fuzzy team/date matching.

PER GAME
--------
  weather_pct : weather.json -> v8.run_adj_pct (headline blended runs %; the
                "Weather" number the tool shows). Domes / missing model -> 0.0.
  ump_name    : posted HP umpire (MLB Stats API officials) when the lineup card
                is up; otherwise the crew-rotation PROJECTION the tool uses
                (today's HP = the 1B/2B/3B ump of the same series 1/2/3 days
                back). De-accented, single-spaced. "" if neither resolves.
  ump_status  : "posted" | "projected" | "".

The model holds today AND the next day(s), so we resolve a multi-day slate and
the sheet matches whatever GameIDs are present.

USAGE
-----
    python scripts/build_sheet_sync.py
    SLATE_DATES=2026-06-29 WEATHER_PATH=/tmp/weather.json \
        python scripts/build_sheet_sync.py     # test against one date/file
"""
from __future__ import annotations
import os, sys, json, datetime, unicodedata, urllib.request

REPO_ROOT    = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
WEATHER_PATH = os.environ.get("WEATHER_PATH", os.path.join(REPO_ROOT, "data", "weather.json"))
OUT_PATH     = os.environ.get("OUT_PATH", os.path.join(REPO_ROOT, "data", "sheet_sync.json"))

AN_API  = ("https://api.actionnetwork.com/web/v2/scoreboard/gameprojections/mlb"
           "?bookIds=15,30&date={yyyymmdd}&periods=event")
MLB_API = "https://statsapi.mlb.com/api/v1/schedule?sportId=1&date={iso}&hydrate=officials,team"
UA = "mlb-tracker/1.0 (+github.com/szerillo/mlb-tracker)"

ROT_POS = ["First Base", "Second Base", "Third Base"]   # daysBack 1/2/3 -> today's HP

NICKS = [
    "blue jays", "red sox", "white sox",
    "diamondbacks", "athletics", "guardians", "mariners", "rangers", "astros",
    "angels", "dodgers", "padres", "giants", "rockies", "cardinals", "cubs",
    "brewers", "pirates", "reds", "braves", "marlins", "mets", "nationals",
    "phillies", "orioles", "yankees", "rays", "tigers", "royals", "twins",
]

# --------------------------------------------------------------------------- helpers
def http_json(url, timeout=30):
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.load(r)

def clean_name(s: str) -> str:
    """De-accent + collapse whitespace. 'Alfonso Márquez' -> 'Alfonso Marquez'."""
    s = unicodedata.normalize("NFKD", s or "")
    s = "".join(c for c in s if not unicodedata.combining(c))
    return " ".join(s.split())

def canon_team(name: str) -> str:
    n = (name or "").lower()
    for nk in NICKS:
        if nk in n:
            return nk
    return n.strip()

def team_key(a: str, h: str) -> frozenset:
    return frozenset((canon_team(a), canon_team(h)))

def et_today() -> datetime.date:
    return (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=4)).date()

def slate_dates() -> list[str]:
    env = os.environ.get("SLATE_DATES", "").strip()
    if env:
        return [d.strip() for d in env.split(",") if d.strip()]
    t = et_today()
    return [(t + datetime.timedelta(days=i)).isoformat() for i in (0, 1, 2, 3)]

# --------------------------------------------------------------------------- sources
def load_weather() -> dict[int, float]:
    """game_pk -> headline weather run %. Domes / missing model -> 0.0."""
    try:
        w = json.load(open(WEATHER_PATH))
    except Exception as e:
        print(f"  [warn] weather read failed ({WEATHER_PATH}): {e}", file=sys.stderr)
        return {}
    out = {}
    for g in w.get("games", []):
        pk = g.get("game_pk")
        if pk is None:
            continue
        v8 = g.get("v8") or {}
        pct = v8.get("run_adj_pct")
        out[int(pk)] = float(pct) if isinstance(pct, (int, float)) else 0.0
    return out

_SCHED_CACHE: dict[str, dict] = {}
def mlb_schedule(iso: str) -> dict:
    """{team_key -> {pk, officials:[(type,name)], hp}} for a date, cached."""
    if iso in _SCHED_CACHE:
        return _SCHED_CACHE[iso]
    out = {}
    try:
        sched = http_json(MLB_API.format(iso=iso))
        for d in sched.get("dates", []) or []:
            for g in d.get("games", []) or []:
                a = g["teams"]["away"]["team"]["name"]; h = g["teams"]["home"]["team"]["name"]
                offs = [(o.get("officialType"), clean_name(o.get("official", {}).get("fullName", "")))
                        for o in g.get("officials", []) or []]
                hp = next((nm for ty, nm in offs if ty == "Home Plate" and nm), "")
                out[team_key(a, h)] = {"pk": g.get("gamePk"), "officials": offs, "hp": hp,
                                       "away": a, "home": h}
    except Exception as e:
        print(f"  [warn] MLB schedule fetch failed for {iso}: {e}", file=sys.stderr)
    _SCHED_CACHE[iso] = out
    return out

def an_games(iso: str) -> dict[str, dict]:
    """an_event_id(str) -> {away_full, home_full} for a date."""
    out = {}
    try:
        an = http_json(AN_API.format(yyyymmdd=iso.replace("-", "")))
        for g in an.get("games", []) or []:
            tm = {t.get("id"): (t.get("full_name") or t.get("display_name")) for t in g.get("teams", [])}
            aw = tm.get(g.get("away_team_id")); hm = tm.get(g.get("home_team_id"))
            if aw and hm:
                out[str(g.get("id"))] = {"away": aw, "home": hm}
    except Exception as e:
        print(f"  [warn] AN fetch failed for {iso}: {e}", file=sys.stderr)
    return out

def project_hp(iso: str, key: frozenset) -> str:
    """Crew-rotation projection: walk back 1-3 days; for the first same-series
    game with posted officials, today's HP = its 1B/2B/3B ump (db 1/2/3)."""
    base = datetime.date.fromisoformat(iso)
    for db in (1, 2, 3):
        prior = (base - datetime.timedelta(days=db)).isoformat()
        rec = mlb_schedule(prior).get(key)
        if not rec or not rec["officials"]:
            continue
        want = ROT_POS[db - 1]
        nm = next((nm for ty, nm in rec["officials"] if ty == want and nm), "")
        if nm:
            return nm
    return ""

# --------------------------------------------------------------------------- main
def main():
    dates = slate_dates()
    print(f"[sheet_sync] slate dates: {dates}")
    weather = load_weather()
    print(f"[sheet_sync] weather games: {len(weather)}")

    games = {}
    n_posted = n_proj = n_nomatch = 0
    for iso in dates:
        an = an_games(iso)
        mlb = mlb_schedule(iso)
        for anid, t in an.items():
            key = team_key(t["away"], t["home"])
            rec = mlb.get(key)
            if not rec:
                n_nomatch += 1
                continue
            pk = rec["pk"]
            hp, status = rec["hp"], "posted"
            if not hp:
                hp = project_hp(iso, key)
                status = "projected" if hp else ""
            if status == "posted":   n_posted += 1
            elif status == "projected": n_proj += 1
            # weather present in model -> number (domes are a real 0.0); absent
            # (no forecast yet, e.g. 3-4 days out) -> null so the sheet SKIPS it
            # rather than overwriting a real value with a misleading 0.0.
            wp = weather.get(int(pk)) if pk is not None else None
            games[str(anid)] = {
                "game_pk": pk,
                "date": iso,
                "away": rec["away"], "home": rec["home"],
                "weather_pct": (round(wp, 1) if isinstance(wp, (int, float)) else None),
                "ump_name": clean_name(hp),
                "ump_status": status,
            }

    payload = {
        "generated_at": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "source": "weather.json (v8.run_adj_pct) + MLB Stats API officials (posted/crew-rotation projected); keyed by Action Network event id",
        "dates": dates,
        "n_games": len(games),
        "games": games,
    }
    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    with open(OUT_PATH, "w") as f:
        json.dump(payload, f, indent=2)
    print(f"[sheet_sync] wrote {len(games)} games -> {OUT_PATH}")
    print(f"[sheet_sync]   ump posted:{n_posted} projected:{n_proj} | unmatched AN games:{n_nomatch}")

if __name__ == "__main__":
    main()
