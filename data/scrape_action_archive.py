#!/usr/bin/env python3
"""
Scrape Action Network historical odds + PRO projections archive.

Endpoint: https://api.actionnetwork.com/web/v2/scoreboard/gameprojections/mlb
    Query: ?bookIds=15,30,1006,1,972,1005,939,1548,1929,1903,2789&date=YYYYMMDD&periods=event
    - Works without auth (PRO projections are in the public response)
    - Book 30  = Opener
    - Book 15  = Consensus
    - edge_projections.game  = Action's PRO model (devigged projection odds,
      edge %, letter grade) for the full game period
    - edge_projections.firstfiveinnings  = F5 period PRO

Output per date: data/odds_archive/YYYY-MM-DD.json
{
  "games": [{
    "game_pk": 824776, "an_event_id": 287145,
    "away_team": "...", "home_team": "...", "start_time": "...",
    "status": "Final", "actual_away_runs": 4, "actual_home_runs": 7,
    "open":      { "ml_away": +150, "ml_home": -170, "total": {"line": 8.5, "over": -110, "under": -105} },
    "consensus": { "ml_away": +155, "ml_home": -175, "total": {"line": 8.5, "over": -108, "under": -110} },
    "pro": {
      "ml_away_proj": -158, "ml_home_proj": +158,
      "ml_away_edge_pct": 4.2, "ml_home_edge_pct": -8.5,
      "ml_away_grade": "B-", "ml_home_grade": "F",
      "over_proj": 7.7, "under_proj": 7.7,
      "over_edge_pct": 0.4, "under_edge_pct": -5.2,
      "over_grade": "C-", "under_grade": "D-"
    }
  }]
}

USAGE:
    # full backfill:
    python scripts/scrape_action_archive.py --start 2026-03-25 --end 2026-04-22
    # daily incremental:
    python scripts/scrape_action_archive.py --start yesterday --end yesterday
"""
from __future__ import annotations
import argparse
import datetime
import json
import os
import sys
import time
import urllib.request

HERE = os.path.dirname(__file__)
REPO_ROOT = os.path.dirname(HERE) if HERE.endswith("scripts") else HERE
OUTPUT_DIR = os.path.join(REPO_ROOT, "data", "odds_archive")
AN_API = ("https://api.actionnetwork.com/web/v2/scoreboard/gameprojections/mlb"
          "?bookIds=15,30,1006,1,972,1005,939,1548,1929,1903,2789&date={yyyymmdd}&periods=event")
MLB_API = "https://statsapi.mlb.com/api/v1/schedule?sportId=1&date={iso}"
BOOK_OPEN = "30"
BOOK_CONSENSUS = "15"
UA = "mlb-tracker/1.0 (+github.com/szerillo/mlb-tracker)"


def _http_get(url: str, timeout: int = 30, retries: int = 3):
    last = None
    for i in range(retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA})
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return json.load(r)
        except Exception as e:
            last = e
            time.sleep(0.6 * (i + 1))
    raise last


def _extract_market(markets: dict, book_id: str) -> dict:
    book = markets.get(book_id)
    if not book:
        return {}
    ev = book.get("event") or {}
    out = {}
    ml = ev.get("moneyline") or []
    for e in ml:
        if e.get("side") == "away": out["ml_away"] = e.get("odds")
        if e.get("side") == "home": out["ml_home"] = e.get("odds")
    tot = ev.get("total") or []
    over = next((e for e in tot if e.get("side") == "over"), None)
    under = next((e for e in tot if e.get("side") == "under"), None)
    if over or under:
        out["total"] = {
            "line": (over or under).get("value"),
            "over": over.get("odds") if over else None,
            "under": under.get("odds") if under else None,
        }
    return out


def _extract_pro(game: dict) -> dict:
    """Pull PRO projections from edge_projections.game. Returns empty dict
    if PRO is unavailable (e.g. games with no model output)."""
    ep = (game.get("edge_projections") or {}).get("game") or game.get("game_projections") or {}
    if not ep:
        return {}
    def _f(v):
        if v is None: return None
        try: return float(v) if isinstance(v, (int, float)) else float(v)
        except (ValueError, TypeError): return None
    return {
        "ml_away_proj":   _f(ep.get("ml_away_proj")),
        "ml_home_proj":   _f(ep.get("ml_home_proj")),
        "ml_away_edge_pct": _f(ep.get("ml_away_edge_pct")),
        "ml_home_edge_pct": _f(ep.get("ml_home_edge_pct")),
        "ml_away_grade": ep.get("ml_away_edge_grade"),
        "ml_home_grade": ep.get("ml_home_edge_grade"),
        "over_proj":  _f(ep.get("over_proj")),
        "under_proj": _f(ep.get("under_proj")),
        "over_edge_pct":  _f(ep.get("over_edge_pct")),
        "under_edge_pct": _f(ep.get("under_edge_pct")),
        "over_grade":  ep.get("over_edge_grade"),
        "under_grade": ep.get("under_edge_grade"),
    }


def _team_key(name: str) -> str:
    return (name or "").lower().replace(" ", "").replace(".", "")


def _mlb_game_pk_map(iso_date: str) -> dict:
    try:
        d = _http_get(MLB_API.format(iso=iso_date))
    except Exception as e:
        print(f"  [{iso_date}] MLB schedule fetch failed: {e}", file=sys.stderr)
        return {}
    out = {}
    for day in d.get("dates", []) or []:
        for g in day.get("games", []) or []:
            a_full = g["teams"]["away"]["team"]["name"]
            h_full = g["teams"]["home"]["team"]["name"]
            def short(nm):
                parts = nm.split()
                return " ".join(parts[-2:]) if len(parts) >= 3 else nm
            out[(_team_key(short(a_full)), _team_key(short(h_full)))] = g.get("gamePk")
            out[(_team_key(a_full.split()[-1]), _team_key(h_full.split()[-1]))] = g.get("gamePk")
    return out


def scrape_date(iso_date: str) -> dict:
    yyyymmdd = iso_date.replace("-", "")
    data = _http_get(AN_API.format(yyyymmdd=yyyymmdd))
    games = data.get("games", []) or []
    pk_map = _mlb_game_pk_map(iso_date)
    out_games = []
    for g in games:
        teams = g.get("teams", []) or []
        away = next((t for t in teams if t.get("id") == g.get("away_team_id")), {})
        home = next((t for t in teams if t.get("id") == g.get("home_team_id")), {})
        away_nm = away.get("display_name") or away.get("full_name") or ""
        home_nm = home.get("display_name") or home.get("full_name") or ""
        game_pk = (pk_map.get((_team_key(away_nm), _team_key(home_nm)))
                   or pk_map.get((_team_key(away_nm.split()[-1] if away_nm else ""), _team_key(home_nm.split()[-1] if home_nm else ""))))
        markets = g.get("markets") or {}
        box = g.get("boxscore") or {}
        actual_away = box.get("total_away_points") or box.get("away_score")
        actual_home = box.get("total_home_points") or box.get("home_score")
        out_games.append({
            "an_event_id": g.get("id"),
            "game_pk": game_pk,
            "away_team": away_nm,
            "home_team": home_nm,
            "start_time": g.get("start_time"),
            "status": g.get("status_display") or g.get("status"),
            "actual_away_runs": actual_away,
            "actual_home_runs": actual_home,
            "open":      _extract_market(markets, BOOK_OPEN),
            "consensus": _extract_market(markets, BOOK_CONSENSUS),
            "pro": _extract_pro(g),
        })
    return {
        "generated_at": datetime.datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "date": iso_date,
        "source": "Action Network gameprojections API",
        "source_url": AN_API.format(yyyymmdd=yyyymmdd),
        "note": "Books 30=Open, 15=Consensus. PRO = Action edge_projections.game (devigged model odds, edge%, letter grades).",
        "n_games": len(out_games),
        "games": out_games,
    }


def _iter_dates(start: str, end: str):
    d = datetime.date.fromisoformat(start)
    last = datetime.date.fromisoformat(end)
    while d <= last:
        yield d.isoformat()
        d += datetime.timedelta(days=1)


def _resolve_alias(s: str) -> str:
    et_now = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=4)
    if s == "today": return et_now.date().isoformat()
    if s == "yesterday": return (et_now.date() - datetime.timedelta(days=1)).isoformat()
    return s


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True)
    ap.add_argument("--end",   required=True)
    ap.add_argument("--force", action="store_true")
    args = ap.parse_args()

    start = _resolve_alias(args.start)
    end   = _resolve_alias(args.end)
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    n_new = n_skip = n_err = 0
    for iso in _iter_dates(start, end):
        out_path = os.path.join(OUTPUT_DIR, f"{iso}.json")
        if os.path.exists(out_path) and not args.force:
            n_skip += 1
            continue
        try:
            payload = scrape_date(iso)
        except Exception as e:
            print(f"[{iso}] ERR: {e}", file=sys.stderr)
            n_err += 1
            continue
        with open(out_path, "w") as f:
            json.dump(payload, f, indent=2)
        n_pro = sum(1 for g in payload["games"] if g["pro"].get("ml_away_proj") is not None)
        print(f"[{iso}] {payload['n_games']} games ({n_pro} with PRO) → {os.path.relpath(out_path, REPO_ROOT)}")
        n_new += 1
        time.sleep(0.35)
    print(f"[done] new={n_new}  skipped={n_skip}  errors={n_err}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
