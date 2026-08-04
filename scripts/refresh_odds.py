"""
Scrape MLB odds from ActionNetwork's gameprojections API (v2 JSON endpoint).

Previously this scraped https://www.actionnetwork.com/mlb/odds, which returns
"unsettled markets" — a mix of today's pre-game lines + yesterday's in-progress
games that haven't fully settled yet. From a GH Actions runner that meant the
page often returned mostly yesterday's games, so data/odds.json got stuck on
stale content for hours at a time.

The v2 gameprojections API takes an explicit ?date=YYYYMMDD param and returns
exactly today's slate with all major books in one shot. Public, no auth needed
for market data (PRO/edge projections require auth, but we only need ML/RL/total
here — PRO lives in the separate odds_archive via scrape_action_archive.py).

Extracts best-price moneyline, run line, and total across major US books
(DraftKings, FanDuel, BetMGM, Caesars, BetRivers, bet365, Fanatics).

Writes data/odds.json with best prices per game joined to MLB gamePk.
"""
import json, os, sys, datetime, urllib.request

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
OUTPUT = os.path.join(REPO_ROOT, "data", "odds.json")
AN_API = ("https://api.actionnetwork.com/web/v2/scoreboard/gameprojections/mlb"
          "?bookIds=15,30,68,69,71,75,79,123,2988&date={yyyymmdd}&periods=event")
MLB_API = "https://statsapi.mlb.com/api/v1/schedule?sportId=1&date={iso}"

# Real sportsbooks (exclude Consensus=15 / Open=30 aggregators)
REAL_BOOKS = {68, 69, 71, 75, 79, 123, 2988}  # DK, FanDuel, BetRivers, BetMGM, bet365, Caesars, Fanatics

# Friendly book names — AN's allBooks map isn't returned by this endpoint so we
# hardcode. These are the display_name values they use elsewhere in the UI.
BOOK_NAMES = {
    68:   "DraftKings",
    69:   "FanDuel",
    71:   "BetRivers",
    75:   "BetMGM",
    79:   "bet365",
    123:  "Caesars",
    2988: "Fanatics",
}

sys.path.insert(0, os.path.dirname(__file__))
from _common import skip_if_not_in_window, within_game_window


def _et_today() -> datetime.date:
    """Return the MLB business day in ET — matters around midnight when late
    West-Coast games are still finishing under yesterday's date."""
    return (datetime.datetime.now(datetime.timezone.utc)
            - datetime.timedelta(hours=4)).date()


def _http_get(url: str, timeout: int = 25) -> dict | None:
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": "mlb-tracker/1.0 (+github.com/szerillo/mlb-tracker)",
            "Accept": "application/json,*/*",
        })
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.load(r)
    except Exception as e:
        print(f"  http err {url}: {e}")
        return None


def is_better(cand: int, best: int) -> bool:
    """American-odds comparison: higher payout = better for the bettor."""
    def payout(o): return o / 100 if o > 0 else 100 / abs(o)
    return payout(cand) > payout(best)


def _tt_amer_prob(o):
    if o is None: return None
    return 100.0 / (o + 100.0) if o > 0 else (-o) / ((-o) + 100.0)


def implied_team_totals(ml_away, ml_home, total_line):
    """Fair per-team totals implied by closing ML + game total. Devig the
    two-way ML to a home win prob, map win prob -> expected run margin, split
    the total. PROVISIONAL: the win%->margin slope (8.5) is a placeholder
    pending Fable calibration (see TEAM_TOTAL_EDGE_CALIBRATION_HANDOFF); the
    single-team over% dispersion (frontend k=1.54) is likewise provisional.
    A posted best-price team_total market can override this later."""
    if not ml_away or not ml_home or total_line is None:
        return None
    pa = _tt_amer_prob(ml_away.get("odds")); ph = _tt_amer_prob(ml_home.get("odds"))
    if pa is None or ph is None or (pa + ph) <= 0:
        return None
    ph_dv = ph / (pa + ph)
    margin = (ph_dv - 0.5) * 8.5
    home_tt = round((total_line + margin) / 2.0, 2)
    away_tt = round((total_line - margin) / 2.0, 2)
    return {
        "away": {"line": away_tt, "over": None, "under": None, "source": "implied"},
        "home": {"line": home_tt, "over": None, "under": None, "source": "implied"},
    }


def best_market(game: dict, market_type: str, side: str | None = None) -> dict | None:
    """Find best odds for a market across REAL_BOOKS.
       market_type: 'moneyline' | 'spread' | 'total'
       side: 'home'/'away' for ML/RL, 'over'/'under' for total"""
    best = None
    for mkt_id, mkt in (game.get("markets") or {}).items():
        try:
            bid = int(mkt_id)
        except (TypeError, ValueError):
            continue
        if bid not in REAL_BOOKS:
            continue
        entries = (mkt.get("event") or {}).get(market_type) or []
        for e in entries:
            if side and e.get("side") != side:
                continue
            odds = e.get("odds")
            if odds is None:
                continue
            if best is None or is_better(odds, best["odds"]):
                best = {"odds": odds, "value": e.get("value"), "book_id": bid}
    return best

def best_total(game: dict):
    """Best over/under for the totals market, PINNED TO ONE (consensus) line so
    the juice pair is always internally consistent. Shopping over and under
    independently could pull the best over at 7.5 and the best under at 7.0 on a
    middled game -> impossible pairs like +105/+103 and miscalculated edges.
    Pick the line the most books offer (both sides), then best price per side AT
    THAT line."""
    from collections import defaultdict
    rows = []
    for mkt_id, mkt in (game.get("markets") or {}).items():
        try:
            bid = int(mkt_id)
        except (TypeError, ValueError):
            continue
        if bid not in REAL_BOOKS:
            continue
        for e in (mkt.get("event") or {}).get("total") or []:
            s = e.get("side"); ln = e.get("value"); od = e.get("odds")
            if s in ("over", "under") and ln is not None and od is not None:
                rows.append((bid, s, ln, od))
    if not rows:
        return None, None
    both = defaultdict(set); allc = defaultdict(int); perbook = defaultdict(set)
    for bid, s, ln, od in rows:
        perbook[(bid, ln)].add(s); allc[ln] += 1
    for (bid, ln), sides in perbook.items():
        if "over" in sides and "under" in sides:
            both[ln].add(bid)
    L = max(both, key=lambda k: (len(both[k]), allc[k])) if both else max(allc, key=lambda k: allc[k])
    best_o = best_u = None
    for bid, s, ln, od in rows:
        if ln != L:
            continue
        if s == "over" and (best_o is None or is_better(od, best_o["odds"])):
            best_o = {"odds": od, "value": L, "book_id": bid}
        elif s == "under" and (best_u is None or is_better(od, best_u["odds"])):
            best_u = {"odds": od, "value": L, "book_id": bid}
    return best_o, best_u



def _odds_should_run():
    """Odds refresh schedule. Runs at three fixed ET anchors — 8:00 PM,
    11:30 PM, 8:00 AM — to capture opening / late / morning line snapshots
    (feeds CLV open->close tracking), PLUS any time a game is within the
    approach window so live odds stay fresh as first pitch nears. FORCE_RUN
    (manual dispatch) always runs."""
    if os.environ.get("FORCE_RUN"):
        return True
    now = datetime.datetime.now(datetime.timezone.utc)
    et = now - datetime.timedelta(hours=4)          # EDT (baseball season)
    mins = et.hour * 60 + et.minute
    for tgt in (20 * 60, 23 * 60 + 30, 8 * 60):     # 8PM, 11:30PM, 8AM ET
        if abs(mins - tgt) <= 20:
            return True
    try:
        return bool(within_game_window())
    except Exception:
        return False


def _pull_slate(date):
    """Pull one date's AN slate joined to MLB gamePks. Returns a list
    (possibly empty) or None when the API fetch itself failed."""
    yyyymmdd = date.strftime("%Y%m%d")
    data = _http_get(AN_API.format(yyyymmdd=yyyymmdd))
    if not data:
        return None
    games = data.get("games", []) or []

    # Today's MLB games for gamePk join.
    # NOTE: a naive (away, home) -> gamePk map collapses doubleheaders onto a
    # single pk (the last-written game wins), so both games of a DH resolve to
    # the same pk. Instead, keep ALL games per matchup and match each AN event
    # to the MLB game with the nearest start time, consuming each pk once.
    sched = _http_get(MLB_API.format(iso=date.isoformat())) or {}
    mlb_games = (sched.get("dates") or [{}])[0].get("games", []) or []

    def _parse_dt(ts):
        if not ts:
            return None
        try:
            return datetime.datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
        except Exception:
            return None

    mlb_by_teams = {}   # (away, home) -> list of {"pk", "dt", "num"}
    for g in mlb_games:
        try:
            a = g["teams"]["away"]["team"]["name"]
            h = g["teams"]["home"]["team"]["name"]
        except Exception:
            continue
        mlb_by_teams.setdefault((a, h), []).append({
            "pk": g.get("gamePk"),
            "dt": _parse_dt(g.get("gameDate")),
            "num": g.get("gameNumber") or 1,
        })
    for lst in mlb_by_teams.values():
        lst.sort(key=lambda x: (x["num"], x["dt"] or datetime.datetime.max))

    used_pks = set()

    def _resolve_pk(away_nm, home_nm, an_start):
        cand = mlb_by_teams.get((away_nm, home_nm)) or []
        cand = [c for c in cand if c["pk"] not in used_pks]
        if not cand:
            return None
        if len(cand) == 1:
            pk = cand[0]["pk"]
        else:
            ant = _parse_dt(an_start)
            if ant is None:
                pk = cand[0]["pk"]
            else:
                pk = min(
                    cand,
                    key=lambda c: abs((c["dt"] - ant).total_seconds()) if c["dt"] else 9e18,
                )["pk"]
        used_pks.add(pk)
        return pk

    games_out = []
    # process in start-time order so nearest-time DH matching is stable
    for g in sorted(games, key=lambda x: str(x.get("start_time") or "")):
        teams = g.get("teams") or []
        away = next((t for t in teams if t.get("id") == g.get("away_team_id")), {})
        home = next((t for t in teams if t.get("id") == g.get("home_team_id")), {})
        away_nm = _asg_alias(away.get("full_name") or away.get("display_name") or "")
        home_nm = _asg_alias(home.get("full_name") or home.get("display_name") or "")
        pk = _resolve_pk(away_nm, home_nm, g.get("start_time"))

        ml_away = best_market(g, "moneyline", side="away")
        ml_home = best_market(g, "moneyline", side="home")
        sp_away = best_market(g, "spread",    side="away")
        sp_home = best_market(g, "spread",    side="home")
        tot_over, tot_under = best_total(g)
        _tt_line = (tot_over or tot_under or {}).get("value")
        _team_total = implied_team_totals(ml_away, ml_home, _tt_line)

        def _fmt(m):
            if not m: return None
            return {
                "odds": m["odds"],
                "line": m.get("value"),
                "book": BOOK_NAMES.get(m["book_id"], f"Book {m['book_id']}"),
            }

        games_out.append({
            "game_pk": pk,
            "an_event_id": g.get("id"),
            "matchup": f"{away_nm} @ {home_nm}",
            "start_time": g.get("start_time"),
            "status": g.get("status_display") or g.get("status"),
            "moneyline": {"away": _fmt(ml_away), "home": _fmt(ml_home)},
            "run_line":  {"away": _fmt(sp_away), "home": _fmt(sp_home)},
            "total":     {"over": _fmt(tot_over), "under": _fmt(tot_under)},
            "team_total": _team_total,
        })
    return games_out


# Action Network calls the All-Star squads "American League" / "National League";
# MLB's schedule calls them "American League All-Stars" / "National League
# All-Stars". The odds join is an exact (away_name, home_name) match against the
# MLB schedule, so the ASG landed with game_pk: null and its odds/edges never
# reached the front end. Normalize AN's names to the MLB form.
def _asg_alias(n):
    s = (n or "").strip()
    if s in ("American League", "National League"):
        return s + " All-Stars"
    return s


def main():
    if not _odds_should_run():
        print("[refresh_odds] skip: not a scheduled anchor (8PM/11:30PM/8AM ET) "
              "and no game within the approach window.")
        return
    date = _et_today()
    print(f"[refresh_odds] fetching API for date={date.isoformat()}")

    games_out = _pull_slate(date)
    if games_out is None:
        print("  ERR: API fetch failed; leaving data/odds.json unchanged")
        return
    print(f"  API returned {len(games_out)} games")
    # Also pull TOMORROW's slate so next-day lines (e.g. Monday's, posted
    # Sunday evening) flow into the tool as soon as books hang them.
    _tom = date + datetime.timedelta(days=1)
    _tom_games = _pull_slate(_tom) or []
    if _tom_games:
        print(f"  +{len(_tom_games)} next-day games for {_tom.isoformat()}")
        games_out += _tom_games

    # --- Freeze odds at first pitch -------------------------------------------
    # Once a game starts, lock its market to the last pre-start snapshot so the
    # scoreboard + Projections tab show the closing pregame line (not live/in-game
    # odds). Each refresh carries forward the existing odds for any game whose
    # start time has passed, as long as a valid pregame price was already captured.
    now_utc = datetime.datetime.now(datetime.timezone.utc)
    prev_by_pk = {}
    try:
        if os.path.exists(OUTPUT):
            prev = json.load(open(OUTPUT))
            if prev.get("date") == date.isoformat():
                for pg in prev.get("games", []):
                    if pg.get("game_pk") is not None:
                        prev_by_pk[pg["game_pk"]] = pg
    except Exception as e:
        print(f"  WARN: could not read existing odds for freeze: {e}")

    def _has_started(g):
        st = g.get("start_time")
        if not st:
            return False
        try:
            t = datetime.datetime.fromisoformat(st.replace("Z", "+00:00"))
            return now_utc >= t
        except Exception:
            return False

    def _has_pregame_price(pg):
        ml = (pg or {}).get("moneyline") or {}
        return bool(ml.get("away") and ml.get("home"))

    frozen = 0
    for i, g in enumerate(games_out):
        prev_g = prev_by_pk.get(g.get("game_pk"))
        if _has_started(g) and _has_pregame_price(prev_g):
            games_out[i] = {
                **prev_g,
                "status": g.get("status") or prev_g.get("status"),  # status may update
                "odds_locked": True,
            }
            frozen += 1
    if frozen:
        print(f"  froze pregame odds for {frozen} started game(s)")

    payload = {
        "generated_at": datetime.datetime.utcnow().isoformat() + "Z",
        "source": "Action Network gameprojections API (v2) · best across DK/FD/BM/Caesars/BetRivers/bet365/Fanatics",
        "source_url": AN_API.format(yyyymmdd=date.strftime("%Y%m%d")),
        "date": date.isoformat(),
        "n_games": len(games_out),
        "games": games_out,
    }
    # Guard: never clobber a populated odds.json with an empty fetch (a
    # transient AN API blip at 09:58Z on 6/7 wiped the file for a whole day).
    if not games_out and os.path.exists(OUTPUT):
        try:
            _prev = json.load(open(OUTPUT))
            if (_prev.get("games") or []):
                print("  API returned 0 games but existing odds.json has "
                      f"{len(_prev['games'])} — keeping existing file (no overwrite)")
                return 0
        except Exception:
            pass
    os.makedirs(os.path.dirname(OUTPUT), exist_ok=True)
    with open(OUTPUT, "w") as f:
        json.dump(payload, f, indent=2)
    pk_ct = sum(1 for g in games_out if g["game_pk"])
    print(f"  wrote {len(games_out)} games ({pk_ct} matched to MLB pk) → {OUTPUT}")

    # ── Archive locked closing odds for past-date scroll ────────────────────
    # Every game that's started (and thus has its closing pregame line locked)
    # gets snapshotted to data/archive/{date}/closing_odds.json. Re-runs
    # overwrite, but locked odds don't change once frozen so that's safe. The
    # frontend's day-scroll reads from here for any date < today.
    started_games = [g for g in games_out if _has_started(g)]
    if started_games:
        archive_dir = os.path.join(REPO_ROOT, "data", "archive", date.isoformat())
        os.makedirs(archive_dir, exist_ok=True)
        archive_path = os.path.join(archive_dir, "closing_odds.json")
        archive_payload = {
            "generated_at": payload["generated_at"],
            "source": payload["source"],
            "date": date.isoformat(),
            "n_games": len(started_games),
            "games": started_games,
            "note": "Closing (last pre-first-pitch) odds for each started game on "
                    "this date. Used by the scoreboard when scrolling to past dates.",
        }
        with open(archive_path, "w") as f:
            json.dump(archive_payload, f)
        print(f"  archived closing odds for {len(started_games)} games → {archive_path}")


if __name__ == "__main__":
    main()
