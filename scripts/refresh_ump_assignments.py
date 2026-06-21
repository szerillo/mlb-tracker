#!/usr/bin/env python3
"""
Wire the slate's Home-Plate umpire assignments INTO the projection model
(the Google Sheet), matched by game.

WHY THIS EXISTS
---------------
The model's per-game blocks ("Matchup N") carry the Action Network event id
(the same `game_id` the GAME UPLOADER tab / sheet_projections.json use). This
script:
  1. reads the model tab and locates every "Matchup N" header,
  2. for each block, finds the AN event id and the target ump cell,
  3. resolves AN event id -> teams -> MLB gamePk -> Home-Plate umpire,
  4. writes the (de-accented, single-spaced) ump name into the cell.

CELL RULE (verified against the live sheet, 2026-06-20)
-------------------------------------------------------
The ump cell is column J on the row 5 below each "Matchup N" header (the
"xERA" row of that block). This is NOT a fixed +63 stride — block spacing
varies (52/53/47/...), so we ANCHOR to the header text, never arithmetic.
For Matchup 1 that resolves to J6, matching Sean's spec; the rest follow the
same header-anchored rule.

UMP SOURCE
----------
MLB Stats API schedule, hydrated with officials. The HP ump populates once a
game's lineup card posts (a few hours before first pitch). Games without a
posted ump are skipped (cell left untouched), so the script is safe to run
repeatedly through the day — it fills cells in as assignments post.

MATCHING
--------
Keyed on AN event id -> gamePk so doubleheaders and multi-day slates (the
model holds today AND tomorrow) resolve to the exact game, not just a team.

USAGE
-----
    GOOGLE_SERVICE_ACCOUNT_JSON='{...}'  \
    SHEET_ID=1Dq9ma3W_YPOJJzq6ZnqivfaniEk8wZJw3gZvuDrH6DE \
        python scripts/refresh_ump_assignments.py

    # preview without writing:
    DRY_RUN=1 ... python scripts/refresh_ump_assignments.py

ENV
---
    GOOGLE_SERVICE_ACCOUNT_JSON  service-account creds JSON (required unless DRY_RUN)
    SHEET_ID                     model spreadsheet id (default: the model)
    MODEL_TAB_GID                tab gid (default 121298510)
    MODEL_TAB_NAME               tab title (overrides gid lookup if set)
    SLATE_DATES                  comma ISO dates to resolve (default: ET today + tomorrow)
    DRY_RUN                      "1" -> print planned writes, do not touch the sheet
    WRITE_JSON                   "1" -> also dump data/ump_assignments.json
"""
from __future__ import annotations
import os, re, sys, json, datetime, unicodedata, urllib.request

# ----------------------------------------------------------------------------- config
SHEET_ID      = os.environ.get("SHEET_ID", "1Dq9ma3W_YPOJJzq6ZnqivfaniEk8wZJw3gZvuDrH6DE").strip()
MODEL_TAB_GID = int(os.environ.get("MODEL_TAB_GID", "121298510"))
MODEL_TAB_NAME = os.environ.get("MODEL_TAB_NAME", "").strip()
DRY_RUN       = os.environ.get("DRY_RUN", "") in ("1", "true", "True")
WRITE_JSON    = os.environ.get("WRITE_JSON", "") in ("1", "true", "True")

UMP_COL_LETTER = "J"      # ump name lands in column J
UMP_ROW_OFFSET = 5        # ... 5 rows below the "Matchup N" header
HEADER_COL     = 5        # "Matchup N" lives in column E (1-based)

AN_API  = ("https://api.actionnetwork.com/web/v2/scoreboard/gameprojections/mlb"
           "?bookIds=15,30&date={yyyymmdd}&periods=event")
MLB_API = "https://statsapi.mlb.com/api/v1/schedule?sportId=1&date={iso}&hydrate=officials,team"
UA = "mlb-tracker/1.0 (+github.com/szerillo/mlb-tracker)"

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
JSON_OUT  = os.path.join(REPO_ROOT, "data", "ump_assignments.json")

# Canonical nickname map so AN full_name, MLB name, and the model's team label
# all collapse to one token. Multi-word nicknames first to avoid "sox" collision.
NICKS = [
    "blue jays", "red sox", "white sox",
    "diamondbacks", "athletics", "guardians", "mariners", "rangers", "astros",
    "angels", "dodgers", "padres", "giants", "rockies", "cardinals", "cubs",
    "brewers", "pirates", "reds", "braves", "marlins", "mets", "nationals",
    "phillies", "orioles", "yankees", "rays", "tigers", "royals", "twins",
]

# ----------------------------------------------------------------------------- helpers
def _http_json(url, timeout=30):
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.load(r)

def clean_name(s: str) -> str:
    """De-accent + collapse internal whitespace + strip. 'Alfonso Márquez' -> 'Alfonso Marquez'."""
    s = unicodedata.normalize("NFKD", s or "")
    s = "".join(c for c in s if not unicodedata.combining(c))
    return " ".join(s.split())

def canon_team(name: str) -> str:
    n = (name or "").lower()
    for nk in NICKS:
        if nk in n:
            return nk
    return n.strip()

def et_today() -> datetime.date:
    return (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=4)).date()

def slate_dates() -> list[str]:
    env = os.environ.get("SLATE_DATES", "").strip()
    if env:
        return [d.strip() for d in env.split(",") if d.strip()]
    t = et_today()
    return [t.isoformat(), (t + datetime.timedelta(days=1)).isoformat()]

# ----------------------------------------------------------------------------- ump map
def build_anid_to_ump(dates: list[str]) -> dict[str, dict]:
    """an_event_id (str) -> {ump, gamePk, date, away, home}. Only games with a
    posted HP ump are included."""
    out: dict[str, dict] = {}
    for iso in dates:
        # AN: an_event_id -> (away_full, home_full)
        an_teams = {}
        try:
            an = _http_json(AN_API.format(yyyymmdd=iso.replace("-", "")))
            for g in an.get("games", []) or []:
                tm = {t.get("id"): (t.get("full_name") or t.get("display_name")) for t in g.get("teams", [])}
                an_teams[str(g.get("id"))] = (tm.get(g.get("away_team_id")), tm.get(g.get("home_team_id")))
        except Exception as e:
            print(f"  [warn] AN fetch failed for {iso}: {e}", file=sys.stderr)
        # MLB: {frozenset(canon away, canon home)} -> (gamePk, hp_ump)
        mlb = {}
        try:
            sched = _http_json(MLB_API.format(iso=iso))
            for d in sched.get("dates", []) or []:
                for g in d.get("games", []) or []:
                    a = g["teams"]["away"]["team"]["name"]; h = g["teams"]["home"]["team"]["name"]
                    hp = [o["official"]["fullName"] for o in g.get("officials", [])
                          if o.get("officialType") == "Home Plate"]
                    key = frozenset((canon_team(a), canon_team(h)))
                    mlb[key] = {"pk": g.get("gamePk"), "ump": clean_name(hp[0]) if hp else "",
                                "away": a, "home": h}
        except Exception as e:
            print(f"  [warn] MLB fetch failed for {iso}: {e}", file=sys.stderr)
        # join
        for anid, (aw, hm) in an_teams.items():
            if not aw or not hm:
                continue
            rec = mlb.get(frozenset((canon_team(aw), canon_team(hm))))
            if rec and rec["ump"]:
                out[anid] = {"ump": rec["ump"], "gamePk": rec["pk"], "date": iso,
                             "away": rec["away"], "home": rec["home"]}
    return out

# ----------------------------------------------------------------------------- sheets io
def _sheets_service():
    from google.oauth2.service_account import Credentials
    from googleapiclient.discovery import build
    raw = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
    if not raw:
        raise SystemExit("GOOGLE_SERVICE_ACCOUNT_JSON not set (required unless DRY_RUN).")
    info = json.loads(raw)
    creds = Credentials.from_service_account_info(
        info, scopes=["https://www.googleapis.com/auth/spreadsheets"])
    return build("sheets", "v4", credentials=creds, cache_discovery=False)

def resolve_tab_title(svc) -> str:
    if MODEL_TAB_NAME:
        return MODEL_TAB_NAME
    meta = svc.spreadsheets().get(
        spreadsheetId=SHEET_ID, fields="sheets(properties(sheetId,title))").execute()
    for s in meta.get("sheets", []):
        if s["properties"]["sheetId"] == MODEL_TAB_GID:
            return s["properties"]["title"]
    raise SystemExit(f"Tab gid {MODEL_TAB_GID} not found in spreadsheet.")

def read_grid_via_api(svc, title):
    resp = svc.spreadsheets().values().get(
        spreadsheetId=SHEET_ID, range=f"'{title}'!A1:AZ4000",
        majorDimension="ROWS").execute()
    return resp.get("values", [])

def read_grid_via_gviz():
    """No-auth read for DRY_RUN previews (works when the sheet is link-readable)."""
    import csv, io
    url = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/gviz/tq?tqx=out:csv&gid={MODEL_TAB_GID}"
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=30) as r:
        text = r.read().decode("utf-8", "replace")
    return list(csv.reader(io.StringIO(text)))

# ----------------------------------------------------------------------------- parse
ANID_IN_CELL = re.compile(r"^(.+?)\s+\d+\s+(29\d{4}|\d{5,7})$")  # "Braves 10 291169"

def parse_blocks(grid):
    """Return [{matchup, header_row(1-based), ump_row, an_id, team_label}]."""
    def cell(r, c):  # 1-based
        return grid[r-1][c-1] if (r-1 < len(grid) and c-1 < len(grid[r-1])) else ""
    nrows = len(grid)
    blocks = []
    for r in range(1, nrows + 1):
        m = re.match(r"^Matchup (\d+)$", (cell(r, HEADER_COL) or "").strip())
        if not m:
            continue
        an_id, team = "", ""
        for rr in range(r, min(r + 9, nrows + 1)):
            for cc in range(1, 15):
                mm = ANID_IN_CELL.match((cell(rr, cc) or "").strip())
                if mm and not an_id:
                    team, an_id = mm.group(1), mm.group(2)
        blocks.append({"matchup": int(m.group(1)), "header_row": r,
                       "ump_row": r + UMP_ROW_OFFSET, "an_id": an_id, "team_label": team})
    return blocks

# ----------------------------------------------------------------------------- main
def main():
    dates = slate_dates()
    print(f"[umps] slate dates: {dates}")
    anid_ump = build_anid_to_ump(dates)
    print(f"[umps] resolved {len(anid_ump)} games with a posted HP ump")

    svc = None
    if DRY_RUN:
        grid = read_grid_via_gviz()
        title = MODEL_TAB_NAME or f"gid:{MODEL_TAB_GID}"
    else:
        svc = _sheets_service()
        title = resolve_tab_title(svc)
        grid = read_grid_via_api(svc, title)
    blocks = parse_blocks(grid)
    print(f"[umps] found {len(blocks)} matchup blocks in tab '{title}'")

    writes, planned, json_rows = [], [], {}
    for b in blocks:
        rec = anid_ump.get(b["an_id"])
        if not rec:
            continue
        cell_a1 = f"{UMP_COL_LETTER}{b['ump_row']}"
        planned.append((b["matchup"], b["an_id"], cell_a1, b["team_label"], rec["ump"]))
        writes.append({"range": f"'{title}'!{cell_a1}", "values": [[rec["ump"]]]})
        json_rows[b["an_id"]] = {**rec, "matchup": b["matchup"], "cell": cell_a1}

    planned.sort(key=lambda x: x[0])
    print(f"\n{'M#':>3} {'AN_id':>7} {'cell':>7}  {'team':<14} -> HP ump")
    for mno, anid, cell_a1, team, ump in planned:
        print(f"{mno:>3} {anid:>7} {cell_a1:>7}  {team:<14} -> {ump}")
    print(f"\n[umps] {len(writes)} cells to write "
          f"({len(blocks) - len(writes)} blocks unresolved / no ump posted yet)")

    if WRITE_JSON:
        os.makedirs(os.path.dirname(JSON_OUT), exist_ok=True)
        with open(JSON_OUT, "w") as f:
            json.dump({"generated_at": datetime.datetime.utcnow().isoformat(timespec="seconds") + "Z",
                       "dates": dates, "source": "MLB Stats API officials (HP ump)",
                       "n": len(json_rows), "assignments": json_rows}, f, indent=2)
        print(f"[umps] wrote {JSON_OUT}")

    if DRY_RUN:
        print("[umps] DRY_RUN — no cells written.")
        return 0
    if writes:
        svc.spreadsheets().values().batchUpdate(
            spreadsheetId=SHEET_ID,
            body={"valueInputOption": "RAW", "data": writes}).execute()
        print(f"[umps] wrote {len(writes)} ump names to '{title}'.")
    else:
        print("[umps] nothing to write.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
