#!/usr/bin/env python3
"""
Import Sean's own Action Network expert projections from the Google Sheet's
GAME UPLOADER tab into the site as data/sheet_projections.json.

SOURCE: the GAME UPLOADER tab, published to the web as CSV. Set the URL via the
SHEET_CSV_URL env var (File -> Share -> Publish to web -> GAME UPLOADER -> CSV).
The tab columns are the Action Network upload format:
    user_id | expert_id | game_id | away_score | home_score | away_win_p |
    home_win_p | ml_away | ml_home | spread_away | spread_home | total | ... | date

game_id == Action Network event id (same id the site's odds archive uses), so we
map game_id -> teams via the AN gameprojections API, then teams -> MLB gamePk via
the MLB schedule API. Output is keyed by gamePk so the front-end can join to
odds.json / the live slate directly.

USAGE:
    SHEET_CSV_URL="https://docs.google.com/.../pub?gid=...&single=true&output=csv" \
        python scripts/refresh_sheet_projections.py
"""
from __future__ import annotations
import csv
import re, io, json, os, sys, datetime, urllib.request, urllib.parse, time

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
OUTPUT = os.path.join(REPO_ROOT, "data", "sheet_projections.json")
SHEET_CSV_URL = os.environ.get("SHEET_CSV_URL", "").strip()

# The model doc. gviz (below) reads it by TAB NAME, so no gid to go stale.
SHEET_ID = "1Dq9ma3W_YPOJJzq6ZnqivfaniEk8wZJw3gZvuDrH6DE"
GVIZ = "https://docs.google.com/spreadsheets/d/{sid}/gviz/tq?tqx=out:csv&sheet={tab}&_cb={cb}"


def sheet_csv_url(env_url: str, tab: str) -> str:
    """Always read the LIVE sheet.

    Google's /pub ("Publish to web") and /export CSV endpoints serve a CACHED
    snapshot that can lag the real sheet by hours. That is exactly how a fully
    filled 15-game slate arrived in CI as "0 projection rows": the importer
    fetched the publish URL, got the pre-upload blank cache, joined nothing, and
    the no-clobber guard then froze a stale feed all night.

    The gviz endpoint evaluates live cell values, so we read gviz unconditionally
    and only honour an env/secret URL if it is itself a gviz URL. A cached
    publish/export URL in the secret is ignored (loudly) rather than trusted."""
    if env_url and "gviz/tq" in env_url:
        return env_url
    if env_url:
        print(f"  [sheet] ignoring cached publish/export URL for '{tab}' "
              f"(serves a stale snapshot); reading the live gviz feed instead")
    return GVIZ.format(sid=SHEET_ID, tab=urllib.parse.quote(tab), cb=int(time.time()))
AN_API = ("https://api.actionnetwork.com/web/v2/scoreboard/gameprojections/mlb"
          "?bookIds=15,30&date={yyyymmdd}&periods=event")
MLB_API = "https://statsapi.mlb.com/api/v1/schedule?sportId=1&date={iso}"
# Google's gviz endpoint returns an EMPTY body to non-browser user agents, which
# parsed as "0 projection rows" and silently froze the feed (2026-07-18 incident:
# sheet had all 16 rows, CI saw none). Present a browser UA for every fetch.
UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0 Safari/537.36")

NAMED = ["user_id","expert_id","game_id","away_score","home_score","away_win_p",
         "home_win_p","ml_away","ml_home","spread_away","spread_home","total"]


def _http_get_json(url, timeout=25):
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.load(r)


def _http_get_text(url, timeout=25):
    hdrs = {"User-Agent": UA,
            "Accept": "text/csv,text/plain,*/*",
            "Accept-Language": "en-US,en;q=0.9"}
    req = urllib.request.Request(url, headers=hdrs)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read().decode("utf-8", "replace")


def fetch_sheet_text(url, label="sheet"):
    """Fetch the sheet CSV, retry once without the cache-buster, and NEVER return
    an empty body silently — an empty gviz response is what produced a phantom
    '0 projection rows' run while the sheet was fully populated."""
    txt = ""
    try:
        txt = _http_get_text(url)
    except Exception as e:
        print(f"  [{label}] fetch error: {e}", file=sys.stderr)
    if not (txt or "").strip():
        alt = re.sub(r"[&?]_cb=\d+", "", url)
        print(f"  [{label}] EMPTY response from sheet; retrying without cache-buster",
              file=sys.stderr)
        try:
            txt = _http_get_text(alt)
        except Exception as e:
            print(f"  [{label}] retry failed: {e}", file=sys.stderr)
    print(f"  [{label}] sheet CSV bytes={len(txt or '')}", file=sys.stderr)
    return txt


def _tkey(s): return (s or "").lower().replace(" ", "").replace(".", "")
def _nick(name): return _tkey(name.split()[-1]) if name else ""

def _parse_dt(ts):
    if not ts:
        return None
    try:
        return datetime.datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
    except Exception:
        return None


def _et_today() -> datetime.date:
    return (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=4)).date()


def _parse_float(v):
    if v is None: return None
    v = str(v).strip().replace(",", "")
    if v == "" or v.lower() in ("na", "n/a", "#n/a"): return None
    try: return float(v)
    except ValueError: return None


def _parse_date_cell(v):
    """Accept M/D/YYYY, MM/DD/YYYY, or YYYY-MM-DD -> iso date string."""
    v = (v or "").strip()
    for fmt in ("%m/%d/%Y", "%-m/%-d/%Y", "%Y-%m-%d"):
        try: return datetime.datetime.strptime(v, fmt).date().isoformat()
        except ValueError: continue
    # fallback: split on / manually
    if "/" in v:
        try:
            m, d, y = v.split()[0].split("/")
            return datetime.date(int(y), int(m), int(d)).isoformat()
        except Exception: return None
    return None


def parse_sheet_csv(text: str, target_iso: str, require_col: str = "away_score"):
    """Return list of dicts for rows whose date == target_iso AND that carry a
    projection. Full-game keys off away_score; F5 keys off 'total' because the
    F5 tab carries total/win%/ML but no team-run split (away_score blank)."""
    rows = list(csv.reader(io.StringIO(text)))
    if not rows: return []
    header = [h.strip().lower() for h in rows[0]]
    idx = {n: header.index(n) for n in NAMED if n in header}
    if "game_id" not in idx:
        raise RuntimeError(f"game_id column not found. header={header}")
    # date column: first column (beyond the named ones) whose values look like dates
    date_col = None
    named_max = max(idx.values()) if idx else 11
    for c in range(named_max + 1, len(header) + 5):
        hits = sum(1 for r in rows[1:30] if c < len(r) and _parse_date_cell(r[c]))
        if hits >= 3:
            date_col = c; break
    out = []
    for r in rows[1:]:
        def cell(name):
            i = idx.get(name)
            return r[i] if (i is not None and i < len(r)) else ""
        gid = _parse_float(cell("game_id"))
        req = _parse_float(cell(require_col))
        if gid is None or req is None:
            continue
        row_iso = None
        if date_col is not None and date_col < len(r):
            row_iso = _parse_date_cell(r[date_col])
        if target_iso and row_iso and row_iso != target_iso:
            continue
        out.append({
            "game_id": int(gid),
            "away_runs": _parse_float(cell("away_score")),
            "home_runs": _parse_float(cell("home_score")),
            "total":     _parse_float(cell("total")),
            "away_wp":   _parse_float(cell("away_win_p")),
            "home_wp":   _parse_float(cell("home_win_p")),
            "ml_away":   _parse_float(cell("ml_away")),
            "ml_home":   _parse_float(cell("ml_home")),
            "date": row_iso,
        })
    return out


def pick_slate_date(all_rows):
    """Choose which date's slate to publish.

    We used to take max(date) — but ONE stray future-dated row (e.g. an early
    projection for a game 2 days out) then hijacked the entire slate: the
    importer targeted that date, found 1 row, joined 0 games, and the no-clobber
    guard froze a stale feed. The uploader tab only ever holds one real slate, so
    the right answer is the date carrying the MOST projection rows (ties -> the
    later date, preserving 'tomorrow's slate uploaded tonight'), ignoring dates
    already in the past."""
    from collections import Counter
    today = _et_today().isoformat()
    counts = Counter(r["date"] for r in all_rows if r.get("date"))
    if not counts:
        return today
    live = {d: n for d, n in counts.items() if d >= today} or dict(counts)
    return max(live, key=lambda d: (live[d], d))


def keep_previous(output_path, iso, n_join, n_sched):
    """True when the new snapshot is a mid-fill partial and we already hold a
    fuller one. Guards both the empty case (n_join == 0) and the thin case (the
    tab caught mid-upload: 1 of 15 games), which used to clobber a good feed."""
    try:
        prev = json.load(open(output_path))
    except Exception:
        return False
    prev_n = prev.get("n_games") or len(prev.get("games") or {})
    if prev_n <= 0:
        return False
    # Only protect a feed for THIS slate. A previous snapshot from an earlier
    # date is stale — keeping it makes (e.g.) the F5 view show yesterday's/last
    # week's games for today when today's tab is empty. Let the empty feed
    # through so today's games render with no projection instead of wrong ones.
    if prev.get("date") != iso:
        return False
    if n_join == 0:
        return True
    # same slate, materially thinner than both the previous snapshot and the
    # actual MLB schedule -> it's a partial fill, not a real slate reduction
    if prev.get("date") == iso and n_join < prev_n and n_sched and n_join < 0.6 * n_sched:
        return True
    return False


def build_id_maps(iso: str):
    """an_event_id -> (away_name, home_name, start_time)  and
       (nick, nick) -> sorted list of {pk, dt, num} (keeps BOTH games of a DH,
       so we can resolve each AN event to its own gamePk by start time instead
       of collapsing the doubleheader onto a single pk)."""
    an = _http_get_json(AN_API.format(yyyymmdd=iso.replace("-", "")))
    an_teams = {}
    for g in an.get("games", []) or []:
        tm = {t.get("id"): (t.get("full_name") or t.get("display_name")) for t in g.get("teams", [])}
        an_teams[g.get("id")] = (tm.get(g.get("away_team_id")),
                                 tm.get(g.get("home_team_id")),
                                 g.get("start_time"))
    sched = _http_get_json(MLB_API.format(iso=iso))
    pk = {}
    for day in sched.get("dates", []) or []:
        for g in day.get("games", []) or []:
            a = g["teams"]["away"]["team"]["name"]; h = g["teams"]["home"]["team"]["name"]
            pk.setdefault((_nick(a), _nick(h)), []).append({
                "pk": g.get("gamePk"),
                "dt": _parse_dt(g.get("gameDate")),
                "num": g.get("gameNumber") or 1,
            })
    for lst in pk.values():
        lst.sort(key=lambda x: (x["num"], x["dt"] or datetime.datetime.max))
    return an_teams, pk


def _game_states(iso):
    """Per-gamePk state for slate `iso`: locked once BOTH lineups are posted,
    plus whether the game is final. Drives the lineup-lock freeze and keeps the
    live slate pinned to today until today's games are all over."""
    url = ("https://statsapi.mlb.com/api/v1/schedule?sportId=1&date="
           + iso + "&hydrate=lineups,probablePitcher")
    out = {}
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "mlb-tracker/1.0"})
        with urllib.request.urlopen(req, timeout=20) as r:
            d = json.load(r)
    except Exception as e:
        print(f"[sheet_projections] lineup fetch failed: {e}", file=sys.stderr)
        return None
    final_states = ("Final", "Game Over", "Completed Early", "Postponed", "Cancelled")
    for day in d.get("dates", []) or []:
        for g in day.get("games", []) or []:
            lu = g.get("lineups") or {}
            home = lu.get("homePlayers") or []
            away = lu.get("awayPlayers") or []
            locked = len(home) >= 9 and len(away) >= 9
            final = (g.get("status", {}) or {}).get("detailedState", "") in final_states
            tms = g.get("teams", {}) or {}
            sp_a = ((tms.get("away", {}) or {}).get("probablePitcher", {}) or {}).get("id")
            sp_h = ((tms.get("home", {}) or {}).get("probablePitcher", {}) or {}).get("id")
            ids_a = ",".join(str(p.get("id")) for p in sorted(away, key=lambda p: p.get("id") or 0))
            ids_h = ",".join(str(p.get("id")) for p in sorted(home, key=lambda p: p.get("id") or 0))
            sig = f"{sp_a}|{sp_h}|{ids_a}|{ids_h}"
            out[str(g.get("gamePk"))] = {"locked": locked, "final": final, "sig": sig}
    return out


def main():
    url = sheet_csv_url(SHEET_CSV_URL, "GAME UPLOADER")
    try:
        text = fetch_sheet_text(url, "sheet_projections")
    except Exception as e:
        print(f"ERR: could not fetch sheet CSV: {e}", file=sys.stderr)
        return 1
    # Auto-detect the slate date from the sheet itself (the user uploads the
    # NEXT slate the night before — e.g. Monday's games on Sunday evening).
    # Use the latest dated projection rows; fall back to ET-today when the
    # sheet has no parseable dates.
    all_rows = parse_sheet_csv(text, None)
    iso = pick_slate_date(all_rows)

    # Pin the live slate to today while today still has games to play, so
    # prepping tomorrow's rows in the sheet doesn't yank the slate onto tomorrow.
    # (Advances to the sheet's latest date once today's games are all final.)
    _today = _et_today().isoformat()
    if iso > _today:
        _t = _game_states(_today)
        if _t is None:
            # Lineup/status fetch hiccup: don't advance onto tomorrow on a maybe;
            # hold the live slate on today so the scoreboard can't roll early.
            print(f"[sheet_projections] holding slate on {_today} (lineup fetch unavailable; sheet latest {iso})")
            iso = _today
        elif _t and not all(s.get("final") for s in _t.values()):
            print(f"[sheet_projections] pinning slate to {_today} (today still live; sheet latest {iso})")
            iso = _today
        # else: no games today OR all final -> allow advance to the sheet's latest
    states = _game_states(iso)
    if states is None:
        # Can't read lineup locks this run; keep the existing feed rather than risk
        # un-freezing a locked game or rolling early. The 5-min loop retries.
        print("[sheet_projections] lineup states unavailable; keeping existing feed", file=sys.stderr)
        return 0
    rows = [r for r in all_rows if (r.get("date") or iso) == iso]
    print(f"[sheet_projections] slate date {iso} ({len(rows)} projection rows)")
    an_teams, pk_map = build_id_maps(iso)

    used_pks = set()
    def _resolve_pk(a, h, an_start):
        cand = [c for c in (pk_map.get((_nick(a), _nick(h))) or []) if c["pk"] not in used_pks]
        if not cand:
            return None
        if len(cand) == 1:
            gpk = cand[0]["pk"]
        else:
            ant = _parse_dt(an_start)
            if ant is None:
                gpk = cand[0]["pk"]
            else:
                gpk = min(cand, key=lambda c: abs((c["dt"] - ant).total_seconds())
                          if c["dt"] else 9e18)["pk"]
        used_pks.add(gpk)
        return gpk

    games = {}
    n_join = n_miss = 0
    # process in AN start-time order so DH nearest-time matching is stable
    for row in sorted(rows, key=lambda r: str((an_teams.get(r["game_id"]) or (None, None, ""))[2] or "")):
        nm = an_teams.get(row["game_id"])
        if not nm or not nm[0] or not nm[1]:
            n_miss += 1; continue
        a, h, an_start = nm
        gpk = _resolve_pk(a, h, an_start)
        if gpk is None:
            n_miss += 1; continue
        games[str(gpk)] = {
            "an_event_id": row["game_id"],
            "away_team": a, "home_team": h,
            "away_runs": row["away_runs"], "home_runs": row["home_runs"],
            "total": row["total"],
            "away_wp": row["away_wp"], "home_wp": row["home_wp"],
            "ml_away": row["ml_away"], "ml_home": row["ml_home"],
        }
        n_join += 1

    # --- Freeze projections at lineup lock -----------------------------------
    # Once BOTH lineups for a game post, lock its projection to the last snapshot
    # so later sheet edits (e.g. prepping tomorrow) can't change it. Locked games
    # are carried forward even if their rows later leave the sheet.
    _prev = {}
    try:
        if os.path.exists(OUTPUT):
            _pj = json.load(open(OUTPUT))
            if _pj.get("date") == iso:
                _prev = _pj.get("games", {}) or {}
    except Exception:
        _prev = {}
    _nfrozen = 0
    _nreopen = 0
    for _pk, _st in states.items():
        if not _st.get("locked"):
            continue
        _pg = _prev.get(_pk)
        _cur_sig = _st.get("sig")
        _stored_sig = (_pg or {}).get("lock_sig")
        _reopened = bool((_pg or {}).get("scratch_reopened"))
        # A lineup change after lock (late scratch / SP swap) reopens the game so
        # your updated projection is pulled; it then stays live for the day.
        if _pg and _stored_sig and _cur_sig and _cur_sig != _stored_sig:
            _reopened = True
        if _reopened:
            if _pk in games:
                games[_pk]["locked"] = False
                games[_pk]["scratch_reopened"] = True
                games[_pk]["lock_sig"] = _cur_sig
            elif _pg is not None:
                _g = dict(_pg); _g["locked"] = False; _g["scratch_reopened"] = True
                _g["lock_sig"] = _cur_sig; games[_pk] = _g
            _nreopen += 1
            continue
        if _pg is not None:
            _g = dict(_pg); _g["locked"] = True; _g["lock_sig"] = _stored_sig or _cur_sig
            games[_pk] = _g; _nfrozen += 1
        elif _pk in games:
            games[_pk]["locked"] = True; games[_pk]["lock_sig"] = _cur_sig; _nfrozen += 1
    for _pk in games:
        if "locked" not in games[_pk]:
            games[_pk]["locked"] = bool(states.get(_pk, {}).get("locked"))
    if _nreopen:
        print(f"[sheet_projections] {_nreopen} game(s) reopened after a lineup change (scratch)")
    if _nfrozen:
        print(f"[sheet_projections] froze {_nfrozen} locked game(s) at lineup lock")

    # Don't let an empty run (tab still being filled / rolled over) wipe a good
    # live feed — keep the previous non-empty snapshot instead.
    n_sched = sum(len(v) for v in pk_map.values())
    if keep_previous(OUTPUT, iso, n_join, n_sched):
        print(f"[sheet_projections] only {n_join}/{n_sched} games joined for {iso}; "
              f"keeping previous fuller feed (won't clobber)")
        return 0

    payload = {
        "generated_at": datetime.datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "date": iso,
        "source": "Google Sheet GAME UPLOADER tab (Action Network expert upload format)",
        "n_games": len(games),
        "games": games,
    }
    os.makedirs(os.path.dirname(OUTPUT), exist_ok=True)
    with open(OUTPUT, "w") as f:
        json.dump(payload, f, indent=2)
    print(f"[sheet_projections] {n_join} games joined, {n_miss} unmatched -> data/sheet_projections.json")
    return 0


if __name__ == "__main__":
    sys.exit(main())
