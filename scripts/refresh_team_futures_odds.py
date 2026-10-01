#!/usr/bin/env python3
"""
Refresh team-level futures odds by scraping VegasInsider.

Why VegasInsider: BettingPros' v3 API (the previous source) is fronted by
Cloudflare and blocks GitHub Actions runners, so the script returned empty
markets and ran "preserve-on-empty" indefinitely. VI is a public HTML page,
not Cloudflare-fronted, and exposes per-book pricing for the markets we care
about. We scrape best-available American odds across whichever books they
show (BetMGM / DraftKings / Caesars / RiversCasino / Hard Rock / Polymarket
depending on market).

Markets pulled:
  • World Series winner    → /mlb/odds/futures/                  (table-world-series-winner)
  • Make playoffs Y/N      → /mlb/odds/playoff-prop/              (table-to-make-the-playoffs)
  • Division winner (×6)   → /mlb/odds/{league}-{div}/            (table-{league}-{div}-winner)

Win totals are JS-rendered on VI's win-totals page and not available in the
returned HTML, so this script PRESERVES the win_total block from the
existing data/team_futures_odds.json file (which can be manually seeded or
filled by an alternate source).

Output schema (matches what compute_team_futures.py expects):
  data/team_futures_odds.json
  {
    "generated_at": ISO, "season": 2026,
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
import datetime, json, os, re, sys, time, urllib.request
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
OUTPUT = REPO_ROOT / "data" / "team_futures_odds.json"

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

# VegasInsider's team-abbr → our standard abbr. VI mostly matches MLB
# Stats API conventions, but has a few historical aliases. Anything not in
# this dict is assumed to be already correct.
VI_ABBR_MAP = {
    "OAK": "ATH",   # Athletics renamed
    "CHW": "CWS",   # White Sox shorthand
    "WAS": "WSH",   # Nationals shorthand
    "SDP": "SD",
    "SFG": "SF",
    "KCR": "KC",
    "TBR": "TB",
}

WS_URL          = "https://www.vegasinsider.com/mlb/odds/futures/"
PLAYOFFS_URL    = "https://www.vegasinsider.com/mlb/odds/playoff-prop/"
DIVISIONS = [
    ("/mlb/odds/american-league-east/",     "table-al-east-winner",     "AL East"),
    ("/mlb/odds/american-league-central/",  "table-al-central-winner",  "AL Central"),
    ("/mlb/odds/american-league-west/",     "table-al-west-winner",     "AL West"),
    ("/mlb/odds/national-league-east/",     "table-nl-east-winner",     "NL East"),
    ("/mlb/odds/national-league-central/",  "table-nl-central-winner",  "NL Central"),
    ("/mlb/odds/national-league-west/",     "table-nl-west-winner",     "NL West"),
]

PENNANTS = [
    ("/mlb/odds/american-league/", "table-al-winner", "AL Pennant"),
    ("/mlb/odds/national-league/", "table-nl-winner", "NL Pennant"),
]

# Map of VI's header column logo alt text → friendly book name.
BOOK_NAMES = {
    "BetMGM": "BetMGM", "DraftKings": "DraftKings", "Caesars": "Caesars",
    "RiversCasino": "RiversCasino", "FanDuel": "FanDuel",
    "Hard Rock": "Hard Rock", "Hard Rock Bet": "Hard Rock", "BetRivers": "BetRivers",
    "ESPN BET": "ESPN BET", "Bet365": "bet365",
    "Polymarket": "Polymarket", "Fanatics": "Fanatics",
}


def _http_get(url, timeout=25):
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": UA,
            "Accept": "text/html,application/xhtml+xml,*/*",
        })
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.read().decode("utf-8", errors="replace")
    except Exception as e:
        print(f"  http err {url}: {e}", file=sys.stderr)
        return None


def _is_better(cand: int, best: int) -> bool:
    """American-odds best-price comparison: higher payout for bettor."""
    def payout(o): return o / 100 if o > 0 else 100 / abs(o)
    return payout(cand) > payout(best)


def _normalize_book(alt: str) -> str:
    """Normalize an <img alt> book name into one of our friendly labels."""
    alt = (alt or "").strip()
    for needle, friendly in BOOK_NAMES.items():
        if needle.lower() in alt.lower():
            return friendly
    return alt or "Book"


def _extract_table(html: str, table_id: str) -> str | None:
    m = re.search(
        rf'<table id="{re.escape(table_id)}"[^>]*>(.*?)</table>',
        html, re.DOTALL)
    return m.group(1) if m else None


def _parse_book_columns(table_html: str) -> list[str]:
    """Read the header row to learn which book each column belongs to.
    Returns a list of book names. Skips the first column (team) and any
    "Time" column. Empty entries become "Book N"."""
    # Header row is the first <tr> in the table
    rows = re.findall(r"<tr[^>]*>(.*?)</tr>", table_html, re.DOTALL)
    if not rows: return []
    hdr_cells = re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", rows[0], re.DOTALL)
    books = []
    # First cell is team — drop it. Then read each header cell.
    for i, cell in enumerate(hdr_cells[1:], start=1):
        alt_match = re.search(r'alt="([^"]+)"', cell)
        if alt_match:
            books.append(_normalize_book(alt_match.group(1)))
            continue
        # Plaintext header
        txt = re.sub(r"<[^>]+>", " ", cell)
        txt = re.sub(r"\s+", " ", txt).strip()
        if not txt or txt.lower() in ("time",):
            books.append(None)   # skip column when reading rows
        else:
            books.append(txt)
    return books


def _parse_data_rows(table_html: str, books: list[str]) -> dict[str, dict]:
    """Return { our_abbr: { "best_odds": int, "best_book": str } } for the
    table, picking best-available across whichever books posted a price."""
    rows = re.findall(r"<tr[^>]*>(.*?)</tr>", table_html, re.DOTALL)
    out = {}
    for row in rows[1:]:  # skip header
        # Team abbr from the data-abbr attr inside the first cell's link.
        abbr_match = re.search(r'data-abbr="([A-Z]{2,4})"', row)
        if not abbr_match:
            continue
        vi_abbr = abbr_match.group(1)
        abbr = VI_ABBR_MAP.get(vi_abbr, vi_abbr)
        # Pull all td cells; index 0 is team, rest are odds columns aligned
        # with `books`.
        cells = re.findall(r"<td[^>]*>(.*?)</td>", row, re.DOTALL)
        if len(cells) < 2: continue
        priced = []   # (odds, book) for every book that posted a price
        # Walk odds cells against books. cells[1:] may be ordered different
        # than `books[1:]` depending on rendering; we assume same order.
        for cell, book in zip(cells[1:], books):
            if book is None:  # column we skip (e.g., "Time")
                continue
            # Look for `<span class="data-value"> -110 </span>`
            val = re.search(
                r'<span class="data-value"[^>]*>\s*([+\-]?\d+)\s*</span>',
                cell)
            if not val: continue
            try:
                priced.append((int(val.group(1)), book))
            except ValueError:
                continue
        if priced:
            # Use the SECOND-best price, not the best. A single book with a
            # stale / mispriced number (e.g. CLE pennant +2200 while the field
            # is ~+600) would otherwise win "best available" and distort the
            # implied % on the vs-Market board. Dropping the single most
            # generous price gives a consensus-representative number. With only
            # one book, fall back to it. (`top_odds`/`n_books` kept for audit.)
            def _payout(o): return o / 100 if o > 0 else 100 / abs(o)
            priced.sort(key=lambda x: _payout(x[0]), reverse=True)
            pick = priced[1] if len(priced) >= 2 else priced[0]
            out[abbr] = {"best_odds": pick[0], "best_book": pick[1],
                         "n_books": len(priced), "top_odds": priced[0][0]}
    return out


def fetch_market(url: str, table_id: str, label: str) -> dict[str, dict]:
    print(f"[futures-odds] fetch {label}: {url}", file=sys.stderr)
    html = _http_get(url)
    if not html:
        return {}
    tbl = _extract_table(html, table_id)
    if not tbl:
        print(f"  WARN: table '{table_id}' not found on {url}", file=sys.stderr)
        return {}
    books = _parse_book_columns(tbl)
    rows = _parse_data_rows(tbl, books)
    print(f"  {len(rows)} teams via {sum(1 for b in books if b)} books",
          file=sys.stderr)
    return rows


# ── Action Network futures (fresh, multi-book). VegasInsider's pennant/WS tables went stale in
#    October (e.g. NYY pennant +300 while every live book had +175/+180), so AN overrides them. ──
AN_LIST = "https://api.actionnetwork.com/web/v1/leagues/8/futures/available"
AN_FUT  = "https://api.actionnetwork.com/web/v1/leagues/8/futures/{}?bookIds={}"
AN_BOOKS = {68: "DraftKings", 1548: "DraftKings", 3118: "DraftKings", 69: "FanDuel", 1006: "FanDuel",
            123: "Caesars", 3120: "Caesars", 75: "BetMGM", 283: "BetMGM", 79: "bet365", 71: "BetRivers",
            972: "BetRivers", 2988: "Fanatics"}
AN_ABBR = {"CHW": "CWS", "SDP": "SD", "TBR": "TB", "AZ": "ARI", "WAS": "WSH", "KCR": "KC", "SFG": "SF", "OAK": "ATH"}

def _an_json(url):
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0", "Referer": "https://www.actionnetwork.com/"})
    with urllib.request.urlopen(req, timeout=25) as r:
        return json.load(r)

def _imp(o):
    return 100.0 / (o + 100) if o > 0 else -o / (-o + 100.0)

def fetch_an_market(name_part: str) -> dict[str, dict]:
    """Best price per team across books for the AN futures market whose type contains name_part.
    Drops a book's price when it sits far off the other books (stale / suspended line)."""
    try:
        types = [f["type"] for f in _an_json(AN_LIST).get("futures", []) if name_part in f.get("type", "")]
        if not types:
            return {}
        d = _an_json(AN_FUT.format(types[0], ",".join(str(i) for i in AN_BOOKS)))
    except Exception as e:
        print(f"[futures-odds] AN {name_part} failed: {e}", file=sys.stderr)
        return {}
    tm = {t["id"]: AN_ABBR.get(t.get("abbr"), t.get("abbr")) for t in d.get("teams", [])}
    by = {}
    for bk in d.get("books", []):
        brand = AN_BOOKS.get(bk.get("book_id"))
        if not brand:
            continue
        for o in bk.get("odds", []):
            ab, m = tm.get(o.get("team_id")), o.get("money")
            if ab and m is not None:
                cur = by.setdefault(ab, {}).get(brand)
                if cur is None or m > cur:
                    by[ab][brand] = m
    out = {}
    for ab, books in by.items():
        ps = sorted(_imp(o) for o in books.values())
        med = ps[len(ps) // 2] if len(ps) % 2 else (ps[len(ps) // 2 - 1] + ps[len(ps) // 2]) / 2
        ok = {b: o for b, o in books.items() if len(ps) < 3 or abs(_imp(o) - med) <= max(0.04, 0.25 * med)}
        if not ok:
            continue
        b, o = max(ok.items(), key=lambda kv: kv[1] / 100 if kv[1] > 0 else 100 / -kv[1])
        out[ab] = {"best_odds": o, "best_book": b, "n_books": len(ok)}
    print(f"[futures-odds] AN {name_part}: {len(out)} teams", file=sys.stderr)
    return out


def main():
    teams_out: dict[str, dict] = {}

    # 1) World Series
    for abbr, info in fetch_market(WS_URL, "table-world-series-winner",
                                   "World Series").items():
        teams_out.setdefault(abbr, {"abbr": abbr})
        teams_out[abbr]["world_series"] = {
            "odds": info["best_odds"], "book": info["best_book"],
        }

    # 2) Playoff Y/N
    for abbr, info in fetch_market(PLAYOFFS_URL, "table-to-make-the-playoffs",
                                   "Playoffs Y/N").items():
        teams_out.setdefault(abbr, {"abbr": abbr})
        teams_out[abbr]["playoffs"] = {
            "odds": info["best_odds"], "book": info["best_book"],
        }

    # 3) Division winner (×6)
    for path, table_id, label in DIVISIONS:
        url = f"https://www.vegasinsider.com{path}"
        for abbr, info in fetch_market(url, table_id, label).items():
            teams_out.setdefault(abbr, {"abbr": abbr})
            teams_out[abbr]["division"] = {
                "odds": info["best_odds"], "book": info["best_book"],
            }
        time.sleep(0.4)  # pace polite

    # 3b) Pennant / league championship winner (x2)
    for path, table_id, label in PENNANTS:
        url = f"https://www.vegasinsider.com{path}"
        for abbr, info in fetch_market(url, table_id, label).items():
            teams_out.setdefault(abbr, {"abbr": abbr})
            teams_out[abbr]["pennant"] = {
                "odds": info["best_odds"], "book": info["best_book"],
            }
        time.sleep(0.4)  # pace polite

    # 3c) Action Network overrides for pennant + World Series (fresh multi-book; VI kept as fallback)
    for part, field in (("american_league_to_win", "pennant"), ("national_league_to_win", "pennant"),
                        ("world_series_to_win", "world_series")):
        for abbr, info in fetch_an_market(part).items():
            teams_out.setdefault(abbr, {"abbr": abbr})
            teams_out[abbr][field] = {"odds": info["best_odds"], "book": info["best_book"], "src": "actionnetwork"}

    # 4) PRESERVE-ON-EMPTY: if our scrape missed every market (e.g. VI
    #    rotates structure), do NOT overwrite the existing file. Keeps
    #    whatever the last good run wrote so the Futures tab stays populated.
    has_any = any(
        ("world_series" in t and t["world_series"].get("odds") is not None)
        or ("playoffs" in t and t["playoffs"].get("odds") is not None)
        or ("division" in t and t["division"].get("odds") is not None)
        for t in teams_out.values()
    )
    if not has_any:
        if OUTPUT.exists():
            print("[futures-odds] scrape returned nothing — leaving existing "
                  "team_futures_odds.json unchanged", file=sys.stderr)
            return 0

    # 5) Merge with existing file so we preserve any win_total block that
    #    came from another source (e.g. manual seed; VI doesn't surface win
    #    totals in scrapeable HTML).
    if OUTPUT.exists():
        try:
            prev = json.loads(OUTPUT.read_text())
            for abbr, prev_team in (prev.get("teams") or {}).items():
                wt = prev_team.get("win_total")
                if wt and isinstance(wt, dict) and wt.get("line") is not None:
                    teams_out.setdefault(abbr, {"abbr": abbr})
                    teams_out[abbr]["win_total"] = wt
        except Exception as e:
            print(f"[futures-odds] couldn't merge prior file: {e}",
                  file=sys.stderr)

    # 6) Collect distinct books seen for the payload header
    books_seen = sorted({
        t[mkt]["book"]
        for t in teams_out.values()
        for mkt in ("world_series", "playoffs", "division", "pennant", "win_total")
        if isinstance(t.get(mkt), dict) and t[mkt].get("book")
    })

    payload = {
        "generated_at": datetime.datetime.utcnow().isoformat() + "Z",
        "season":       datetime.date.today().year,
        "source":       ("VegasInsider — World Series + Playoff Y/N + 6 "
                         "Division pages; win totals preserved from prior file"),
        "books_seen":   books_seen,
        "n_teams":      len(teams_out),
        "teams":        teams_out,
    }
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(json.dumps(payload, indent=2))
    print(f"[futures-odds] wrote {len(teams_out)} teams → {OUTPUT}",
          file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
