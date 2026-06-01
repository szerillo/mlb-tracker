#!/usr/bin/env python3
"""
Refresh team-level futures odds (best-available across sportsbooks) from
the BettingPros v3 API. Mirrors the format Sean uses in his win-totals
article: per team, capture the best price we can find for:

  • Season win total O/U  (market_id 192)
  • Win division           (market_id 190)
  • Make playoffs          (market_id 191)
  • Win World Series       (market_id 188)

For each market we walk all selections, then within each selection we walk
every book's "lines" array and find the BEST American-odds price (highest
payout for the bettor). That matches the "best available" framing — same
philosophy we use for scoreboard moneylines.

Output: data/team_futures_odds.json
  {
    "generated_at": ISO,
    "season": 2026,
    "books_seen": [...],
    "teams": {
      "NYY": {
        "abbr": "NYY",
        "win_total": { "line": 90.5, "over_odds": -110, "over_book": "DK",
                       "under_odds": -110, "under_book": "FD" },
        "division":  { "odds": 210, "book": "DK" },
        "playoffs":  { "odds": -237, "book": "BetMGM" },
        "world_series": { "odds": 1801, "book": "BetMGM" }
      }, ...
    }
  }

USAGE:
    python scripts/refresh_team_futures_odds.py
"""
from __future__ import annotations
import datetime, json, os, sys, urllib.request
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
OUTPUT = REPO_ROOT / "data" / "team_futures_odds.json"

# Public key BettingPros uses for their own web UI — exposed in the page's JS
# bundle. They allow it from any origin so no auth flow needed. If they ever
# rotate it we can pull it back out of /dist/assets/api-*.js.
API_KEY = os.environ.get("BETTINGPROS_KEY", "CHi8Hy5CEE4khd46XNYL23dCFX96oUdw6qOt1Dnh")
BASE = "https://api.bettingpros.com/v3/offers"

MARKETS = {
    "win_total":    192,  # Season Win Total (O/U)
    "division":     190,  # Division Winner
    "playoffs":     191,  # Make Playoffs
    "world_series": 188,  # World Series Winner
}

# BettingPros book IDs → friendly names (display only). Their API returns
# numeric ids; we look up the name from this map. ID 0 = Consensus (skip).
BOOK_NAMES = {
    0: "Consensus", 2: "Pinnacle",
    10: "FanDuel", 12: "DraftKings", 13: "Caesars", 14: "Fanatics",
    15: "SugarHouse", 18: "BetRivers", 19: "BetMGM",
    24: "bet365", 33: "thescore Bet", 36: "Underdog", 37: "PrizePicks",
    38: "ProphetX", 39: "Fliff", 45: "Betr", 49: "Hard Rock",
    60: "Novig", 63: "Sleeper", 68: "Kalshi", 70: "DraftKings Pick6",
    73: "Polymarket", 74: "DraftKings Predictions",
}

# BettingPros team-abbr quirks → match our standard 30
BP_ABBR_FIXUP = {"SAC": "ATH", "OAK": "ATH"}


def _http_get(url: str, timeout: int = 30) -> dict | None:
    try:
        req = urllib.request.Request(url, headers={
            "x-api-key": API_KEY,
            "User-Agent": "mlb-tracker/1.0 (+github.com/szerillo/mlb-tracker)",
            "Accept": "application/json",
        })
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.load(r)
    except Exception as e:
        print(f"  http err {url}: {e}", file=sys.stderr)
        return None


def best_american(books: list, want_best_payout: bool = True) -> tuple[int | None, int | None]:
    """Find best (American odds, book id) across an offer's books list.
    BettingPros returns one book per books[] item with a `lines` array.
    `best: true` flag is sometimes set but unreliable, so we re-derive."""
    best_cost = None
    best_book = None
    for b in books or []:
        bid = b.get("id", 0)
        if bid == 0:
            continue  # skip consensus
        for line in b.get("lines") or []:
            if line.get("is_off") or not line.get("active"):
                continue
            cost = line.get("cost")
            if cost is None:
                continue
            # American odds: higher number = better for bettor BUT positive
            # values pay more than -100, and negative values pay LESS as they
            # get more negative. Convert to implied payout to compare.
            payout = cost / 100 if cost > 0 else 100 / abs(cost)
            if best_cost is None or (want_best_payout and payout > _payout(best_cost)):
                best_cost = cost
                best_book = bid
    return best_cost, best_book


def _payout(cost: int) -> float:
    return cost / 100 if cost > 0 else 100 / abs(cost)


def fetch_win_totals(season: int) -> dict[str, dict]:
    """Win totals are different — each offer is per-team with two selections
    (Over / Under). We capture the best over-odds and best under-odds for
    each team along with the line (typically the most common one)."""
    out = {}
    page = 1
    while True:
        url = f"{BASE}?sport=MLB&market_id=192&season={season}&location=ALL&limit=10&page={page}"
        d = _http_get(url)
        if not d:
            break
        offers = d.get("offers") or []
        for o in offers:
            team_abbr = None
            for p in o.get("participants") or []:
                abbr = (p.get("team") or {}).get("abbreviation") or p.get("id")
                if abbr:
                    team_abbr = BP_ABBR_FIXUP.get(abbr, abbr)
                    break
            if not team_abbr:
                continue
            sels = o.get("selections") or []
            # Find Over and Under sides
            row = {"line": None, "over_odds": None, "over_book": None,
                   "under_odds": None, "under_book": None}
            for s in sels:
                side = (s.get("label") or s.get("selection") or "").lower()
                # Line is the same for both sides
                cost, book = best_american(s.get("books") or [])
                if cost is None:
                    continue
                # Grab the line value from any line entry
                for b in s.get("books") or []:
                    for ln in b.get("lines") or []:
                        if ln.get("line") is not None and not ln.get("is_off"):
                            row["line"] = ln["line"]
                            break
                    if row["line"] is not None:
                        break
                if "over" in side:
                    row["over_odds"] = cost
                    row["over_book"] = BOOK_NAMES.get(book, f"Book {book}")
                elif "under" in side:
                    row["under_odds"] = cost
                    row["under_book"] = BOOK_NAMES.get(book, f"Book {book}")
            if row["line"] is not None:
                out[team_abbr] = row
        pg = d.get("_pagination") or {}
        if page >= pg.get("total_pages", 1):
            break
        page += 1
    return out


def fetch_winner_market(market_id: int, season: int) -> dict[str, dict]:
    """Division / Playoffs / World Series — one offer with N selections, each
    selection = one team's odds to win. Returns {team_abbr: {odds, book}}."""
    out = {}
    page = 1
    while True:
        url = f"{BASE}?sport=MLB&market_id={market_id}&season={season}&location=ALL&limit=10&page={page}"
        d = _http_get(url)
        if not d:
            break
        for o in d.get("offers") or []:
            for s in o.get("selections") or []:
                abbr = s.get("participant") or s.get("short_label")
                if not abbr:
                    continue
                abbr = BP_ABBR_FIXUP.get(abbr, abbr)
                cost, book = best_american(s.get("books") or [])
                if cost is None:
                    continue
                out[abbr] = {
                    "odds": cost,
                    "book": BOOK_NAMES.get(book, f"Book {book}"),
                }
        pg = d.get("_pagination") or {}
        if page >= pg.get("total_pages", 1):
            break
        page += 1
    return out


def main():
    season = datetime.date.today().year
    print(f"[futures-odds] season={season}", file=sys.stderr)

    print(f"  fetching win totals…", file=sys.stderr)
    win_totals = fetch_win_totals(season)
    print(f"    {len(win_totals)} teams", file=sys.stderr)

    print(f"  fetching division winners…", file=sys.stderr)
    division = fetch_winner_market(190, season)
    print(f"    {len(division)} teams", file=sys.stderr)

    print(f"  fetching playoffs…", file=sys.stderr)
    playoffs = fetch_winner_market(191, season)
    print(f"    {len(playoffs)} teams", file=sys.stderr)

    print(f"  fetching world series…", file=sys.stderr)
    world_series = fetch_winner_market(188, season)
    print(f"    {len(world_series)} teams", file=sys.stderr)

    # Stitch by abbr
    all_teams = (set(win_totals) | set(division) | set(playoffs) | set(world_series))

    # PRESERVE-ON-EMPTY: BettingPros' Cloudflare blocks GitHub Actions runner
    # IPs intermittently — when blocked, all 4 fetch_* calls return {} and
    # all_teams is empty. Overwriting with an empty file would wipe the last
    # known good odds (which the frontend Futures tab depends on). If we got
    # nothing, exit 0 without writing so the previous file survives until the
    # next pass (or a manual run from an unblocked IP) succeeds.
    if not all_teams:
        print("[futures-odds] all markets returned 0 teams (likely IP block) — "
              "leaving existing data/team_futures_odds.json unchanged",
              file=sys.stderr)
        return 0

    teams_out = {}
    for abbr in sorted(all_teams):
        teams_out[abbr] = {
            "abbr":         abbr,
            "win_total":    win_totals.get(abbr),
            "division":     division.get(abbr),
            "playoffs":     playoffs.get(abbr),
            "world_series": world_series.get(abbr),
        }

    # Track which books are showing up (debug aid)
    books_seen = set()
    for abbr in teams_out:
        for k in ("division", "playoffs", "world_series"):
            v = teams_out[abbr].get(k)
            if v and v.get("book"):
                books_seen.add(v["book"])
        wt = teams_out[abbr].get("win_total")
        if wt:
            for k in ("over_book", "under_book"):
                if wt.get(k):
                    books_seen.add(wt[k])

    payload = {
        "generated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds"),
        "season": season,
        "source": "BettingPros v3 API · best-available American odds per market",
        "n_teams": len(teams_out),
        "markets": MARKETS,
        "books_seen": sorted(books_seen),
        "teams": teams_out,
    }
    OUTPUT.write_text(json.dumps(payload, separators=(",", ":")))
    print(f"[futures-odds] wrote {OUTPUT}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
