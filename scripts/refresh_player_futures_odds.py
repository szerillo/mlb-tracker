#!/usr/bin/env python3
"""
Scrape player futures odds from VegasInsider for the 6 awards markets:
  AL MVP / NL MVP / AL Cy Young / NL Cy Young / AL ROY / NL ROY.

Mirrors refresh_team_futures_odds.py — public HTML, no Cloudflare block from
GH Actions runners. Each VI page has two <table id="table-{league}-{award}">
blocks, one per league, with one row per player and one column per book.

Player rows differ from team rows: VI shows a player-avatar image + a
<span>Name</span> inside <td class="game-team">, but does NOT expose
data-abbr or team text. To get team / league assignments we hit the MLB Stats
API people-search by name (cached). Team-to-league mapping comes from the
schedule.teams record (AL = leagueId 103, NL = leagueId 104).

Output schema (matches what compute_player_futures.py expects):

  data/player_futures_odds.json
  {
    "generated_at": ISO, "season": 2026,
    "books_seen": [...],
    "markets": {
      "AL_MVP": {
        "label": "AL MVP",
        "n_players": 30,
        "players": [
          {
            "name": "Aaron Judge",
            "mlbam_id": 592450, "team_abbr": "NYY", "league": "AL",
            "best_odds": -130, "best_book": "BetMGM",
            "all_book_odds": {"BetMGM": -130, "DraftKings": -135, ...}
          }, ...
        ]
      },
      "NL_MVP": { ... }, "AL_CY": { ... }, "NL_CY": { ... },
      "AL_ROY": { ... }, "NL_ROY": { ... }
    }
  }

USAGE:
    python scripts/refresh_player_futures_odds.py
"""
from __future__ import annotations
import datetime, json, os, re, sys, time, unicodedata, urllib.request
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
OUTPUT = REPO_ROOT / "data" / "player_futures_odds.json"
CACHE  = REPO_ROOT / "data" / "_player_team_cache.json"

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

# VI exposes both leagues on a single page, so we just hit 3 URLs total.
MARKET_URLS = [
    ("https://www.vegasinsider.com/mlb/odds/mvp/",                 "MVP",  ("AL_MVP", "NL_MVP")),
    ("https://www.vegasinsider.com/mlb/odds/cy-young/",            "CY",   ("AL_CY",  "NL_CY")),
    ("https://www.vegasinsider.com/mlb/odds/rookie-of-the-year/",  "ROY",  ("AL_ROY", "NL_ROY")),
]
MARKET_TABLE_IDS = {
    "AL_MVP": "table-al-mvp", "NL_MVP": "table-nl-mvp",
    "AL_CY":  "table-al-cy-young", "NL_CY":  "table-nl-cy-young",
    "AL_ROY": "table-al-rookie-of-the-year", "NL_ROY": "table-nl-rookie-of-the-year",
}
MARKET_LABELS = {
    "AL_MVP": "AL MVP", "NL_MVP": "NL MVP",
    "AL_CY":  "AL Cy Young",  "NL_CY":  "NL Cy Young",
    "AL_ROY": "AL Rookie of the Year", "NL_ROY": "NL Rookie of the Year",
}

# Translate VI / MLB API team abbrevs into our internal codes (matches the
# team_projections.json keyspace).
TEAM_ALIAS = {
    "OAK": "ATH", "WAS": "WSH", "CHW": "CWS",
    "SDP": "SD",  "SFG": "SF",  "KCR": "KC", "TBR": "TB",
}

MLB_LEAGUE_ID_AL = 103
MLB_LEAGUE_ID_NL = 104


def _http_get(url, timeout=25, headers=None):
    try:
        req = urllib.request.Request(url, headers=headers or {"User-Agent": UA})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.read().decode("utf-8", errors="replace")
    except Exception as e:
        print(f"  http err {url}: {e}", file=sys.stderr)
        return None


def _http_json(url, timeout=20):
    raw = _http_get(url, timeout=timeout, headers={
        "User-Agent": UA, "Accept": "application/json"})
    if not raw: return None
    try: return json.loads(raw)
    except Exception as e:
        print(f"  json err {url}: {e}", file=sys.stderr)
        return None


def _norm_name(s):
    """Diacritic-stripped lowercase for cache lookup."""
    if not s: return ""
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    return s.lower().strip()


def _better(cand, best):
    """American-odds best-price comparison."""
    def payout(o): return o / 100 if o > 0 else 100 / abs(o)
    return payout(cand) > payout(best)


def _extract_table(html, table_id):
    m = re.search(
        rf'<table id="{re.escape(table_id)}"[^>]*>(.*?)</table>',
        html, re.DOTALL)
    return m.group(1) if m else None


def _parse_book_columns(table_html):
    rows = re.findall(r"<tr[^>]*>(.*?)</tr>", table_html, re.DOTALL)
    if not rows: return []
    hdr_cells = re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", rows[0], re.DOTALL)
    books = []
    for cell in hdr_cells[1:]:   # skip team/player col
        alt = re.search(r'alt="([^"]+)"', cell)
        if alt:
            books.append(alt.group(1).strip()); continue
        txt = re.sub(r"<[^>]+>", " ", cell)
        txt = re.sub(r"\s+", " ", txt).strip()
        books.append(None if (not txt or txt.lower() == "time") else txt)
    return books


def _parse_player_row(row, books):
    """Pull player name from <span>NAME</span> inside the game-team cell, then
    best-available odds across whichever books posted a price."""
    cells = re.findall(r"<td[^>]*>(.*?)</td>", row, re.DOTALL)
    if len(cells) < 2: return None
    # Player name lives in <span>...</span> inside the first cell.
    name_match = re.search(r'<span>\s*([^<]+?)\s*</span>', cells[0])
    if not name_match: return None
    name = re.sub(r"\s+", " ", name_match.group(1)).strip()
    best = None; best_book = None
    all_odds = {}
    for cell, book in zip(cells[1:], books):
        if book is None: continue
        val = re.search(
            r'<span class="data-value"[^>]*>\s*([+\-]?\d+)\s*</span>', cell)
        if not val: continue
        try: odds = int(val.group(1))
        except ValueError: continue
        all_odds[book] = odds
        if best is None or _better(odds, best):
            best, best_book = odds, book
    if best is None: return None
    return {"name": name, "best_odds": best, "best_book": best_book,
            "all_book_odds": all_odds}


# ── Team / League resolution via MLB Stats API ──────────────────────────────
def _load_cache():
    if CACHE.exists():
        try: return json.loads(CACHE.read_text())
        except Exception: pass
    return {}


def _save_cache(cache):
    CACHE.parent.mkdir(parents=True, exist_ok=True)
    CACHE.write_text(json.dumps(cache, indent=2))


def _resolve_player_team(name, cache, season=None):
    """Return {abbr, mlbam_id, league} or None. Uses MLB Stats API people
    search; caches by normalized name."""
    key = _norm_name(name)
    if key in cache:
        return cache[key]
    # Query MLB people-search
    season = season or datetime.date.today().year
    url = (f"https://statsapi.mlb.com/api/v1/people/search"
           f"?names={urllib.request.quote(name)}&active=true")
    j = _http_json(url, timeout=15)
    if not j or not j.get("people"):
        cache[key] = None
        return None
    # Take the most recent active player matching name; tie-break by debut year
    candidates = []
    for p in j["people"]:
        if not p.get("active"): continue
        team = p.get("currentTeam") or {}
        team_id = team.get("id")
        if not team_id: continue
        # Need team's league
        info = _http_json(f"https://statsapi.mlb.com/api/v1/teams/{team_id}", timeout=10)
        try:
            t_info = (info or {}).get("teams", [{}])[0]
            league_id = (t_info.get("league") or {}).get("id")
            abbr = t_info.get("abbreviation") or t_info.get("teamCode") or ""
        except Exception:
            league_id, abbr = None, None
        league = ("AL" if league_id == MLB_LEAGUE_ID_AL
                  else "NL" if league_id == MLB_LEAGUE_ID_NL else None)
        candidates.append({
            "name": name, "mlbam_id": p.get("id"),
            "team_abbr": TEAM_ALIAS.get(abbr, abbr) if abbr else None,
            "league": league,
        })
        time.sleep(0.1)
    if not candidates:
        cache[key] = None
        return None
    # Prefer one with a real league assignment
    chosen = next((c for c in candidates if c["league"]), candidates[0])
    cache[key] = chosen
    return chosen


# ── Main ────────────────────────────────────────────────────────────────────
def _dedupe_market(players):
    """Collapse duplicate rows for the same normalized name, keeping the row with
    the most book prices (best consensus). CY markets legitimately carry a
    consensus row + a stray single-book row per pitcher; MVP/ROY do not.
    Returns (deduped_list, n_removed)."""
    best = {}; order = []
    for p in players:
        k = _norm_name(p.get("name"))
        if not k:
            continue
        if k not in best:
            best[k] = p; order.append(k)
        elif len(p.get("all_book_odds") or {}) > len(best[k].get("all_book_odds") or {}):
            best[k] = p
    deduped = [best[k] for k in order]
    return deduped, len(players) - len(deduped)


def main():
    cache = _load_cache()
    markets_out = {}
    for url, label_short, (al_key, nl_key) in MARKET_URLS:
        print(f"[player-futures-odds] fetch {label_short}: {url}", file=sys.stderr)
        html = _http_get(url)
        if not html:
            continue
        for key in (al_key, nl_key):
            tbl_id = MARKET_TABLE_IDS[key]
            tbl = _extract_table(html, tbl_id)
            if not tbl:
                print(f"  WARN: table '{tbl_id}' not on page", file=sys.stderr)
                continue
            books = _parse_book_columns(tbl)
            rows = re.findall(r"<tr[^>]*>(.*?)</tr>", tbl, re.DOTALL)
            players = []
            for row in rows[1:]:
                p = _parse_player_row(row, books)
                if not p: continue
                resolved = _resolve_player_team(p["name"], cache)
                if resolved:
                    p.update({
                        "mlbam_id": resolved.get("mlbam_id"),
                        "team_abbr": resolved.get("team_abbr"),
                        "league": resolved.get("league"),
                    })
                else:
                    p.update({"mlbam_id": None, "team_abbr": None, "league": None})
                players.append(p)
            markets_out[key] = {
                "label": MARKET_LABELS[key],
                "n_players": len(players),
                "books_seen": [b for b in books if b],
                "players": players,
            }
            print(f"  {MARKET_LABELS[key]}: {len(players)} players via "
                  f"{sum(1 for b in books if b)} books", file=sys.stderr)
        time.sleep(0.4)

    _save_cache(cache)

    # ── GUARD 1: de-dupe every market (keep the most-books row per player). ──
    WIN_MARKETS = ("AL_MVP", "NL_MVP", "AL_ROY", "NL_ROY")
    win_raw = win_removed = 0
    for key, m in markets_out.items():
        raw = m.get("players") or []
        deduped, removed = _dedupe_market(raw)
        m["players"] = deduped
        m["n_players"] = len(deduped)
        if key in WIN_MARKETS:
            win_raw += len(raw); win_removed += removed

    # ── GUARD 2: reject-on-corrupt. MVP/ROY list each player exactly once on a
    #    healthy page; duplicates there mean a doubled/cached page carrying stale
    #    lines (CY's consensus+single-book rows are excluded from this test).
    #    Don't overwrite a good file — leave the prior file untouched. ──
    win_dup_ratio = (win_removed / win_raw) if win_raw else 0.0
    reject = None
    if not any(m.get("n_players", 0) for m in markets_out.values()):
        reject = "scrape empty"
    elif win_dup_ratio > 0.10:
        reject = (f"{win_removed} duplicate MVP/ROY rows "
                  f"({win_dup_ratio:.0%}) — doubled/cached page")
    if reject:
        if OUTPUT.exists():
            print(f"[player-futures-odds] REJECT ({reject}) — leaving prior file untouched",
                  file=sys.stderr)
            return 0
        print(f"[player-futures-odds] WARN ({reject}) but no prior file — writing anyway",
              file=sys.stderr)

    # Distinct books across the whole pull
    books_seen = sorted({
        b for m in markets_out.values()
        for b in (m.get("books_seen") or [])
    })
    payload = {
        "generated_at": datetime.datetime.utcnow().isoformat() + "Z",
        "season":       datetime.date.today().year,
        "source":       "VegasInsider — MVP / Cy Young / Rookie of the Year pages",
        "books_seen":   books_seen,
        "markets":      markets_out,
    }
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(json.dumps(payload, indent=2))
    print(f"[player-futures-odds] wrote {len(markets_out)} markets → {OUTPUT}",
          file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
