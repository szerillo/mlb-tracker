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
ENABLE_CORRECT_SCORE = True    # Kalshi KXMLBSERIESSCORE (exact series score)
ENABLE_SPREAD        = True    # derived from the same exact-score markets (-1.5g = win by 2+)
ENABLE_WS_EXACTA     = True    # Kalshi KXTEAMSINWS (WS pairing = both teams reach the WS)
ENABLE_GAMES_OU      = True    # Kalshi KXMLBSERIESGAMES (series total games over/under)

# Action Network futures/series endpoint (series ML posts here per round).
AN_HEADERS = {"User-Agent": "Mozilla/5.0", "Referer": "https://www.actionnetwork.com/"}

# Kalshi public markets (no auth) — postseason SERIES winner markets live here:
#   KXMLBSERIES-<YY><AWAY><HOME><ROUND>-<TEAM>  (one binary market per team, yes = that team wins the series)
# yes_bid/yes_ask are in dollars = implied probability; mid is the market's series win prob.
KALSHI_SERIES_URL = "https://api.elections.kalshi.com/trade-api/v2/markets?series_ticker=KXMLBSERIES&status=open&limit=1000"
KALSHI_MKT = "https://api.elections.kalshi.com/trade-api/v2/markets?series_ticker={}&status=open&limit=1000"
KALSHI_AB = {"AZ": "ARI"}   # Kalshi team code -> board abbreviation


def _kalshi(series_ticker):
    """All open markets for a Kalshi series ticker, as (market, mid_prob) pairs. Mid of
    yes bid/ask (dollars = implied prob); one-sided books fall back to whichever side exists."""
    try:
        data = _get(KALSHI_MKT.format(series_ticker))
    except Exception as e:
        print(f"[series_odds] Kalshi {series_ticker} fetch failed: {e}", file=sys.stderr)
        return []
    out = []
    for m in data.get("markets", []):
        try:
            bid = float(m.get("yes_bid_dollars") or 0); ask = float(m.get("yes_ask_dollars") or 0)
        except (TypeError, ValueError):
            continue
        mid = (bid + ask) / 2 if (bid > 0 and ask > 0) else (bid or ask)
        if 0 < mid < 1:
            out.append((m, mid))
    return out


def _event_code(m):
    """KXMLBSERIESSCORE-26CWSHOUWC-HOU21 -> '26CWSHOUWC'."""
    parts = (m.get("event_ticker") or m.get("ticker", "")).split("-")
    return parts[1] if len(parts) > 1 else ""


def _series_for_event(code, series):
    """Match a Kalshi event code (contains both team codes) to one of our series."""
    for s_ in series:
        ka = {v: k for k, v in KALSHI_AB.items()}
        a, b = ka.get(s_["a"], s_["a"]), ka.get(s_["b"], s_["b"])
        body = code[2:]   # drop the 2-digit year
        if (body.startswith(a + b) or body.startswith(b + a)):
            return s_
    return None


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

def _american(p):
    """implied prob (0..1) -> American odds."""
    if p is None or p <= 0 or p >= 1: return None
    return round(-100*p/(1-p)) if p >= 0.5 else round(100*(1-p)/p)

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
    """Series moneyline from Kalshi's public KXMLBSERIES winner markets (real market prices).
    Each active series posts two binary markets (one per team); yes bid/ask in dollars = implied
    prob, mid = market series win prob. Matched to our (league,a,b) by team abbreviation. Any
    series whose Kalshi market is not posted yet stays blank."""
    if not ENABLE_SERIES_ML:
        return {}
    try:
        data = _get(KALSHI_SERIES_URL)
    except Exception as e:
        print(f"[series_odds] Kalshi series-ML fetch failed: {e}", file=sys.stderr)
        return {}
    # event_ticker -> {team_abbr: mid_prob}
    ev = {}
    for m in data.get("markets", []):
        tk = m.get("ticker", "")
        team = tk.split("-")[-1]
        try:
            bid = float(m.get("yes_bid_dollars") or 0); ask = float(m.get("yes_ask_dollars") or 0)
        except (TypeError, ValueError):
            continue
        mid = (bid + ask) / 2 if (bid > 0 and ask > 0) else (bid or ask)
        if not (0 < mid < 1):
            continue
        ev.setdefault(m.get("event_ticker", tk[:-len(team)-1] if team else tk), {})[team] = mid
    out = {}
    for s_ in series:
        a, b = s_["a"], s_["b"]
        for teams in ev.values():
            if a in teams and b in teams:
                out[(s_["league"], a, b)] = {"ml_a": _american(teams[a]),
                                             "ml_b": _american(teams[b]), "book": "Kalshi"}
                break
    return out

_EXACT_PROB = {}   # (league,a,b) -> {'a-2-0': mid_prob, ...}; shared by exact + spread

def fetch_correct_score(series):
    """{(league,a,b): {'a-2-0':odds, 'a-2-1':odds, 'b-2-1':odds, 'b-2-0':odds, ...}} from Kalshi
    KXMLBSERIESSCORE (one binary per exact result; ticker suffix = TEAM + wins + losses)."""
    if not ENABLE_CORRECT_SCORE:
        return {}
    for m, mid in _kalshi("KXMLBSERIESSCORE"):
        s_ = _series_for_event(_event_code(m), series)
        if not s_:
            continue
        suf = m.get("ticker", "").split("-")[-1]
        team, w, l = KALSHI_AB.get(suf[:-2], suf[:-2]), suf[-2], suf[-1]
        side = "a" if team == s_["a"] else ("b" if team == s_["b"] else None)
        if side and w.isdigit() and l.isdigit():
            _EXACT_PROB.setdefault((s_["league"], s_["a"], s_["b"]), {})[f"{side}-{w}-{l}"] = mid
    return {k: {kk: _american(p) for kk, p in v.items()} for k, v in _EXACT_PROB.items()}


def fetch_spread(series):
    """{(league,a,b): {'a_minus_1_5':odds,'b_minus_1_5':odds}}  -1.5 games = win the series by 2+,
    i.e. the sum of that side's exact-score markets with (wins - losses) >= 2. Needs fetch_correct_score first."""
    if not ENABLE_SPREAD:
        return {}
    out = {}
    for key, ex in _EXACT_PROB.items():
        d = {}
        for side in ("a", "b"):
            p = sum(v for k, v in ex.items() if k.startswith(side + "-") and int(k.split("-")[1]) - int(k.split("-")[2]) >= 2)
            if p > 0:
                d[f"{side}_minus_1_5"] = _american(min(p, 0.99))
        if d:
            out[key] = d
    return out


def fetch_games_ou(series):
    """{(league,a,b): {'line': 2.5, 'over': odds, 'under': odds}} from Kalshi KXMLBSERIESGAMES."""
    if not ENABLE_GAMES_OU:
        return {}
    out = {}
    for m, mid in _kalshi("KXMLBSERIESGAMES"):
        s_ = _series_for_event(_event_code(m), series)
        if not s_:
            continue
        n = m.get("ticker", "").split("-")[-1]
        line = (int(n) - 0.5) if n.isdigit() else m.get("floor_strike")
        out[(s_["league"], s_["a"], s_["b"])] = {"line": line, "over": _american(mid), "under": _american(1 - mid)}
    return out


def fetch_ws_exacta(pairings):
    """{(al,nl): odds} from Kalshi KXTEAMSINWS (the WS matchup = both teams win their pennant).
    Ticker suffix is the two team codes concatenated, e.g. KXTEAMSINWS-26-TBLAD."""
    if not ENABLE_WS_EXACTA:
        return {}
    ka = {v: k for k, v in KALSHI_AB.items()}
    want = {}
    for p in pairings:
        al, nl = ka.get(p["al"], p["al"]), ka.get(p["nl"], p["nl"])
        want[al + nl] = (p["al"], p["nl"]); want[nl + al] = (p["al"], p["nl"])
    out = {}
    for m, mid in _kalshi("KXTEAMSINWS"):
        suf = m.get("ticker", "").split("-")[-1]
        if suf in want:
            out[want[suf]] = _american(mid)
    return out


# ── assemble ────────────────────────────────────────────────────────────────
def main():
    model = _load_model()
    series_in  = (model or {}).get("series", [])
    pairings_in = (model or {}).get("ws_exacta", {}).get("pairings", [])

    ml  = fetch_series_ml(series_in)
    cs  = fetch_correct_score(series_in)
    sp  = fetch_spread(series_in)
    ou  = fetch_games_ou(series_in)
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
            "games":  ou.get(key),      # {'line','over','under'} or None
        })

    pairings_out = [{"al": p["al"], "nl": p["nl"], "market": ex.get((p["al"], p["nl"]))}
                    for p in pairings_in]

    out = {
        "generated_at": datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "+00:00",
        "source": "Kalshi: KXMLBSERIES (series ML), KXMLBSERIESSCORE (exact score + derived -1.5g), KXMLBSERIESGAMES (total games), KXTEAMSINWS (WS matchup)",
        "enabled": {"series_ml": ENABLE_SERIES_ML, "correct_score": ENABLE_CORRECT_SCORE,
                    "spread": ENABLE_SPREAD, "ws_exacta": ENABLE_WS_EXACTA, "games_ou": ENABLE_GAMES_OU},
        "round": (model or {}).get("round"),
        "series": series_out,
        "ws_exacta": {"pairings": pairings_out},
    }
    os.makedirs(DATA, exist_ok=True)
    open(OUT, "w").write(json.dumps(out, indent=1))
    n_ml = sum(1 for s in series_out if s["ml_a"] is not None)
    n_ex = sum(1 for s in series_out if s["exact"]); n_ou = sum(1 for s in series_out if s["games"])
    n_px = sum(1 for p in pairings_out if p["market"] is not None)
    print(f"[series_odds] exact {n_ex} · games o/u {n_ou} · WS pairings priced {n_px}")
    print(f"[series_odds] wrote series_odds.json | {len(series_out)} series ({n_ml} with live ML), "
          f"{len(pairings_out)} pairings | enabled={out['enabled']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
