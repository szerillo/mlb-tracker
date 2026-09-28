#!/usr/bin/env python3
"""
refresh_series_odds.py  ---  postseason SERIES market odds for the futures board.

Writes data/series_odds.json in a shape the board merges onto data/lane2_series.json
(the model feed) by (league, a, b) for series and (al, nl) for the WS exacta.

FOUR market blocks, each independently gated by an ENABLE_* flag:
  - series_ml     : per-round series moneyline (Team A / Team B to win the series)   [LIVE]
  - correct_score : exact series result (2-0/2-1 bo3; 3-0/3-1/3-2 bo5; 4-x bo7)      [framework only]
  - spread        : series game spread (-1.5 / +1.5, -2.5 / +2.5, ...)               [framework only]
  - ws_exacta     : World Series pairing / matchup market                            [framework only]

Only series_ml is enabled by default; the other three are wired end-to-end (schema,
merge keys, fetch stubs) but return nothing until their books are confirmed and the
ENABLE_* flag is flipped. That is the "framework built, not out yet" state Sean asked
for: the board renders those columns blank rather than breaking, and lighting them up
later is one flag + one fetch function each.

Merge contract (frontend):
  series_odds.series[i] matches lane2_series.series by (league,a,b); each carries
  {ml_a, ml_b, exact:{...}, spread:{...}} with null where a market is absent.
  series_odds.ws_exacta.pairings matches by (al,nl) with {market} price or null.
"""
from __future__ import annotations
import json, os, sys, datetime, urllib.request

HERE = os.path.dirname(os.path.abspath(__file__)); REPO = os.path.abspath(os.path.join(HERE, ".."))
DATA = os.path.join(REPO, "data")
MODEL_FEED = os.path.join(DATA, "lane2_series.json")   # produced by run_lane2_bracket
OUT        = os.path.join(DATA, "series_odds.json")

# ── enable flags: flip on as each book/source is confirmed ──────────────────
ENABLE_SERIES_ML     = True
ENABLE_CORRECT_SCORE = False
ENABLE_SPREAD        = False
ENABLE_WS_EXACTA     = False

# Action Network futures/series endpoint (series ML posts here per round).
AN_HEADERS = {"User-Agent": "Mozilla/5.0", "Referer": "https://www.actionnetwork.com/"}


def _get(url, timeout=25):
    return json.load(urllib.request.urlopen(urllib.request.Request(url, headers=AN_HEADERS), timeout=timeout))


def _load_model():
    if not os.path.exists(MODEL_FEED):
        print(f"[series_odds] no model feed at {MODEL_FEED}; nothing to key against", file=sys.stderr)
        return None
    return json.load(open(MODEL_FEED))


# ── market fetchers ─────────────────────────────────────────────────────────
def _dec(o):
    """American -> decimal payout (bigger = better price for the bettor)."""
    if o is None: return None
    return 1 + (o/100.0 if o > 0 else 100.0/(-o))

def _best(cands):
    """cands = [(american, book)]; return (american, book) with the best payout."""
    cands = [(o, b) for o, b in cands if o is not None]
    if not cands: return (None, None)
    return max(cands, key=lambda x: _dec(x[0]))

# ── per-book series-ML fetchers ─────────────────────────────────────────────
# Each returns {(league,a,b): {'a': american, 'b': american}} for the given series.
# All are best-effort and wrapped so a failure (endpoint moved, geo-block, market
# not posted yet) yields {} rather than crashing the run. Confirm each endpoint's
# exact path + JSON shape against a live posted market, then it feeds automatically.
def _bk_actionnetwork(series):
    # AN aggregates books; series/futures live under a scoreboard-style endpoint.
    # TODO(confirm): map AN postseason series market ids -> teams once WC posts.
    return {}
def _bk_fanduel(series):
    # FanDuel Sportsbook API (sbapi.<region>.sportsbook.fanduel.com). Geo-gated;
    # TODO(confirm): market catalog id for "<Team> to win the series".
    return {}
def _bk_draftkings(series):
    # DraftKings eventgroup API (sportsbook-nash.draftkings.com/...).
    # TODO(confirm): subcategory "Series Winner" offer ids.
    return {}
def _bk_betonline(series):
    # BetOnline (BOL) sports API. TODO(confirm): postseason series market path.
    return {}

BOOKS = [("AN", _bk_actionnetwork), ("FanDuel", _bk_fanduel),
         ("DraftKings", _bk_draftkings), ("BetOnline", _bk_betonline)]

def fetch_series_ml(series):
    """Best price across books per side -> {(league,a,b): {ml_a,ml_b,book}}.
    LIVE block: polls every book in BOOKS, keeps the best payout for each side and
    records which book. Books that return nothing (unmapped / market not posted) are
    simply skipped, so cells stay blank until a real price appears."""
    if not ENABLE_SERIES_ML:
        return {}
    per_book = {}
    for name, fn in BOOKS:
        try:
            per_book[name] = fn(series) or {}
        except Exception as e:
            print(f"[series_odds] {name} series-ML fetch failed: {e}", file=sys.stderr)
            per_book[name] = {}
    out = {}
    for s in series:
        key = (s["league"], s["a"], s["b"])
        a_cands = [(per_book[nm].get(key, {}).get("a"), nm) for nm, _ in BOOKS]
        b_cands = [(per_book[nm].get(key, {}).get("b"), nm) for nm, _ in BOOKS]
        ba, bka = _best(a_cands); bb, bkb = _best(b_cands)
        if ba is None and bb is None:
            continue
        out[key] = {"ml_a": ba, "ml_b": bb, "book": bka or bkb}
    return out


def fetch_correct_score(series):
    """{(league,a,b): {'a-2-0':odds, 'a-2-1':odds, 'b-2-1':odds, 'b-2-0':odds, ...}}"""
    if not ENABLE_CORRECT_SCORE:
        return {}
    return {}


def fetch_spread(series):
    """{(league,a,b): {'a_minus_1_5':odds,'b_minus_1_5':odds,'a_minus_2_5':odds,...}}"""
    if not ENABLE_SPREAD:
        return {}
    return {}


def fetch_ws_exacta(pairings):
    """{(al,nl): odds} WS pairing / matchup market."""
    if not ENABLE_WS_EXACTA:
        return {}
    return {}


# ── assemble ────────────────────────────────────────────────────────────────
def main():
    model = _load_model()
    series_in  = (model or {}).get("series", [])
    pairings_in = (model or {}).get("ws_exacta", {}).get("pairings", [])

    ml  = fetch_series_ml(series_in)
    cs  = fetch_correct_score(series_in)
    sp  = fetch_spread(series_in)
    ex  = fetch_ws_exacta(pairings_in)

    series_out = []
    for s in series_in:
        key = (s["league"], s["a"], s["b"])
        m = ml.get(key, {})
        series_out.append({
            "league": s["league"], "a": s["a"], "b": s["b"],
            "ml_a": m.get("ml_a"), "ml_b": m.get("ml_b"), "book": m.get("book"),
            "exact":  cs.get(key),      # dict or None
            "spread": sp.get(key),      # dict or None
        })

    pairings_out = [{"al": p["al"], "nl": p["nl"], "market": ex.get((p["al"], p["nl"]))}
                    for p in pairings_in]

    out = {
        "generated_at": datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "+00:00",
        "source": "Action Network series/futures (series ML); correct-score / spread / exacta pending",
        "enabled": {"series_ml": ENABLE_SERIES_ML, "correct_score": ENABLE_CORRECT_SCORE,
                    "spread": ENABLE_SPREAD, "ws_exacta": ENABLE_WS_EXACTA},
        "round": (model or {}).get("round"),
        "series": series_out,
        "ws_exacta": {"pairings": pairings_out},
    }
    os.makedirs(DATA, exist_ok=True)
    open(OUT, "w").write(json.dumps(out, indent=1))
    n_ml = sum(1 for s in series_out if s["ml_a"] is not None)
    print(f"[series_odds] wrote series_odds.json | {len(series_out)} series ({n_ml} with live ML), "
          f"{len(pairings_out)} pairings | enabled={out['enabled']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
