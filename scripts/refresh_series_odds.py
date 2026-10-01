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
        m["_bid"], m["_ask"] = bid, ask
        mid = (bid + ask) / 2 if (bid > 0 and ask > 0) else (bid or ask)
        if 0 < mid < 1:
            out.append((m, mid))
    return out


FEE = 0.07   # Kalshi taker fee coefficient: fee per contract = 0.07 * p * (1 - p)

def _cost(p):
    """What it actually costs to BUY a contract at price p (dollars), fee included, as a prob."""
    if p is None or p <= 0 or p >= 1:
        return None
    return min(p + FEE * p * (1 - p), 0.995)

MAX_SPREAD = 0.30   # bid/ask wider than this = no real market yet (empty book shows 0.00/0.99)

def _dead(m):
    a = m.get("_ask") or 0; b = m.get("_bid") or 0
    return (a - b) > MAX_SPREAD

def _buy_yes(m):
    """Cost to back YES = the yes ask (+fee). None when nobody is offering."""
    if _dead(m): return None
    a = m.get("_ask") or 0
    return _cost(a) if 0 < a < 1 else None

def _buy_no(m):
    """Cost to back NO = 1 - yes bid (+fee)."""
    if _dead(m): return None
    b = m.get("_bid") or 0
    return _cost(1 - b) if 0 < b < 1 else None

def _best_cost(*cs):
    cs = [c for c in cs if c is not None]
    return min(cs) if cs else None


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
# Action Network futures API: one call returns the "Playoff Series - <round> - To Win" market for
# every US book it tracks (DraftKings, FanDuel, Caesars, BetMGM, bet365, Fanatics, BetRivers ...).
AN_FUT_LIST = "https://api.actionnetwork.com/web/v1/leagues/8/futures/available"
AN_FUT      = "https://api.actionnetwork.com/web/v1/leagues/8/futures/{}?bookIds={}"
# AN book id -> brand (state variants collapse to one brand; best price per brand kept)
AN_BOOKS = {68: "DraftKings", 1548: "DraftKings", 3118: "DraftKings", 69: "FanDuel", 1006: "FanDuel",
            123: "Caesars", 3120: "Caesars", 75: "BetMGM", 283: "BetMGM", 79: "bet365", 71: "BetRivers",
            972: "BetRivers", 2988: "Fanatics", 247: "Unibet", 1902: "Resorts World", 1903: "Bally Bet"}
AN_AB = {"CHW": "CWS", "SDP": "SD", "TBR": "TB", "AZ": "ARI", "ARI": "ARI"}

def _imp(o):
    return None if o is None else (100.0/(o+100) if o > 0 else -o/(-o+100.0))

def _an_series_odds():
    """[ {team: {brand: american}} per open playoff-series 'To Win' market ] (one dict per AN market,
    so a Wild Card price can never leak into a Division Series price for the same team)."""
    try:
        types = [f["type"] for f in _get(AN_FUT_LIST).get("futures", [])
                 if "playoff_series" in f.get("type", "") or "championship_series" in f.get("type", "")]
    except Exception as e:
        print(f"[series_odds] AN futures list failed: {e}", file=sys.stderr); return []
    ids = ",".join(str(i) for i in AN_BOOKS)
    mkts = []
    for t in types:
        try:
            d = _get(AN_FUT.format(t, ids))
        except Exception as e:
            print(f"[series_odds] AN {t} failed: {e}", file=sys.stderr); continue
        tm = {x["id"]: AN_AB.get(x.get("abbr"), x.get("abbr")) for x in d.get("teams", [])}
        by_team = {}
        for bk in d.get("books", []):
            brand = AN_BOOKS.get(bk.get("book_id"))
            if not brand: continue
            for o in bk.get("odds", []):
                ab, money = tm.get(o.get("team_id")), o.get("money")
                if not ab or money is None: continue
                cur = by_team.setdefault(ab, {}).get(brand)
                if cur is None or _dec(money) > _dec(cur):
                    by_team[ab][brand] = money
        mkts.append(by_team)
    return mkts

def _bk_actionnetwork(series):
    mkts = _an_series_odds()
    out = {}
    for s_ in series:
        a, b = s_["a"], s_["b"]
        for bt in mkts:
            if a not in bt or b not in bt: continue
            A, B = {}, {}
            for brand in set(bt[a]) & set(bt[b]):
                # a book's two sides must form a sane two-way market (overround 0-15%); drops stale/settling lines
                tot = _imp(bt[a][brand]) + _imp(bt[b][brand])
                if 1.0 <= tot <= 1.15:
                    A[brand], B[brand] = bt[a][brand], bt[b][brand]
            # cross-book sanity: drop a book whose no-vig A% sits >8 pts off the median of the others
            # (stale or mid-settlement line; e.g. a book still showing pre-Game-2 prices)
            if len(A) >= 3:
                nv = {k: _imp(A[k]) / (_imp(A[k]) + _imp(B[k])) for k in A}
                for k in list(A):
                    others = sorted(v for kk, v in nv.items() if kk != k)
                    med = others[len(others)//2] if len(others) % 2 else (others[len(others)//2-1] + others[len(others)//2]) / 2
                    if abs(nv[k] - med) > 0.08:
                        print(f"[series_odds] drop stale {k} {a}-{b}: {nv[k]:.3f} vs median {med:.3f}", file=sys.stderr)
                        A.pop(k); B.pop(k)
            if A:
                out[(s_["league"], a, b)] = {"a": A, "b": B}
            break
    if out: print(f"[series_odds] Action Network series ML: {len(out)} series, books={sorted({k for v in out.values() for k in v['a']})}")
    return out


FULLNAME = {'Tampa Bay Rays':'TB','Cleveland Guardians':'CLE','Chicago White Sox':'CWS','Houston Astros':'HOU',
  'New York Yankees':'NYY','Boston Red Sox':'BOS','Milwaukee Brewers':'MIL','Los Angeles Dodgers':'LAD',
  'Atlanta Braves':'ATL','San Diego Padres':'SD','Chicago Cubs':'CHC','Philadelphia Phillies':'PHI'}
def _game_ml_today():
    """{frozenset({away,home}): {team: (best ML, book)}} for not-yet-started games in data/odds.json."""
    out = {}
    try:
        d = json.load(open(os.path.join(DATA, "odds.json")))
    except Exception:
        return out
    games = d.get("games") or []
    for g in (games.values() if isinstance(games, dict) else games):
        if str(g.get("status", "")).lower() not in ("scheduled", "pre", "pregame"): continue
        try: aw, hm = [FULLNAME.get(x.strip()) for x in g["matchup"].split(" @ ")]
        except Exception: continue
        ml = g.get("moneyline") or {}
        if not (aw and hm and ml.get("away") and ml.get("home")): continue
        out[frozenset({aw, hm})] = {aw: (ml["away"]["odds"], ml["away"].get("book") or "best"),
                                    hm: (ml["home"]["odds"], ml["home"].get("book") or "best")}
    return out


# ── Pinnacle (guest API, no auth): sharp series ML for every open series ──
PIN_MATCHUPS = "https://guest.api.arcadia.pinnacle.com/0.1/leagues/246/matchups"
PIN_MARKETS  = "https://guest.api.arcadia.pinnacle.com/0.1/leagues/246/markets/straight"
def _pinnacle_series():
    """{frozenset({a,b}): {team: american}} from Pinnacle 'Series Prices' specials."""
    out = {}
    try:
        ms = _get(PIN_MATCHUPS); mk = _get(PIN_MARKETS)
    except Exception as e:
        print(f"[series_odds] Pinnacle fetch failed: {e}", file=sys.stderr); return out
    sp = {m["id"]: {p["id"]: FULLNAME.get(p.get("name")) for p in m.get("participants", [])}
          for m in ms if m.get("type") == "special" and (m.get("special") or {}).get("category") == "Series Prices"}
    for x in mk:
        parts = sp.get(x.get("matchupId"))
        if not parts or x.get("type") != "moneyline" or x.get("isAlternate"): continue
        pr = {parts.get(p["participantId"]): p["price"] for p in x.get("prices", []) if parts.get(p["participantId"])}
        if len(pr) == 2: out[frozenset(pr)] = pr
    return out

# ── Manually pasted book prices (DK / Caesars series props): data/series_odds_manual.json ──
MANUAL = os.path.join(DATA, "series_odds_manual.json")
def _manual_props(series):
    """{(league,a,b): {'exact':{k:[(odds,book)]}, 'spread':{...}, 'games':{line:{'over':[..],'under':[..]}}}}.
    A pasted series is dropped once its score has moved since the paste (prices are stale)."""
    out = {}
    try: d = json.load(open(MANUAL))
    except Exception: return out
    for book, ser in (d.get("books") or {}).items():
        for e in ser.values():
            s_ = next((x for x in series if {x["a"], x["b"]} == {e["a"], e["b"]}), None)
            if not s_: continue
            w = s_.get("wins") or {"a": 0, "b": 0}
            flip = s_["a"] != e["a"]
            wa, wb = (w.get("b", 0), w.get("a", 0)) if flip else (w.get("a", 0), w.get("b", 0))
            if (wa, wb) != (e["wins_at_paste"]["a"], e["wins_at_paste"]["b"]): continue
            sw = (lambda k: k.replace("a", "\0").replace("b", "a").replace("\0", "b")) if flip else (lambda k: k)
            o = out.setdefault((s_["league"], s_["a"], s_["b"]), {"exact": {}, "spread": {}, "games": {}})
            for k, v in (e.get("exact") or {}).items(): o["exact"].setdefault(sw(k[0]) + k[1:], []).append((v, book))
            for k, v in (e.get("spread") or {}).items(): o["spread"].setdefault(sw(k[0]) + k[1:], []).append((v, book))
            for ln, ou in (e.get("games") or {}).items():
                g = o["games"].setdefault(ln, {"over": [], "under": []})
                for side in ("over", "under"):
                    if ou.get(side) is not None: g[side].append((ou[side], book))
    return out


# ── Bovada (open JSON feed): full series markets -- winner, correct score, +/-1.5/2.5 games, total games ──
BOV_LIST  = "https://www.bovada.lv/services/sports/event/v2/events/A/description/baseball?marketFilterId=def&lang=en"
BOV_EVENT = "https://www.bovada.lv/services/sports/event/coupon/events/A/description{}?lang=en"
NICK = {"Rays":"TB","Yankees":"NYY","Guardians":"CLE","White Sox":"CWS","Padres":"SD","Brewers":"MIL","Dodgers":"LAD",
        "Braves":"ATL","Phillies":"PHI","Cubs":"CHC","Red Sox":"BOS","Astros":"HOU","Mariners":"SEA","Tigers":"DET"}
def _nick(txt):
    for k, v in NICK.items():
        if k in txt: return v
    return None
def _am(x):
    try: return int(str(x).replace("+", "")) if str(x).upper() != "EVEN" else 100
    except Exception: return None
def _bovada_series():
    """{frozenset({a,b}): {'ml':{team:odds}, 'exact':{(team,w,l):odds}, 'spread':{(team,±x.5):odds}, 'games':{line:{'over','under'}}}}"""
    out = {}
    try: groups = _get(BOV_LIST)
    except Exception as e:
        print(f"[series_odds] Bovada list failed: {e}", file=sys.stderr); return out
    links = [e["link"] for g in groups for e in g.get("events", []) if "playoff-series" in e.get("link", "")]
    for ln in links:
        try: d = _get(BOV_EVENT.format(ln))
        except Exception as e:
            print(f"[series_odds] Bovada {ln} failed: {e}", file=sys.stderr); continue
        for g in d:
            for ev in g.get("events", []):
                rec = {"ml": {}, "exact": {}, "spread": {}, "games": {}}
                for dg in ev.get("displayGroups", []):
                    for m in dg.get("markets", []):
                        md = m.get("description", "")
                        for o in m.get("outcomes", []):
                            od = o.get("description", ""); pr = o.get("price") or {}; a = _am(pr.get("american"))
                            if a is None: continue
                            if md == "Series Winner":
                                t = _nick(od); t and rec["ml"].__setitem__(t, a)
                            elif md == "Series Correct Score":
                                t = _nick(od); sc = od.split()[-1]
                                if t and "-" in sc: w, l = sc.split("-"); rec["exact"][(t, int(w), int(l))] = a
                            elif "Series Handicap" in md:
                                t = _nick(od); hc = [x for x in od.split() if x[:1] in "+-"]
                                if t and hc: rec["spread"][(t, float(hc[0]))] = a
                            elif md in ("Total", "Alternate Series Total Games"):
                                side = "over" if od.lower().startswith("over") else "under"
                                line = pr.get("handicap") or od.split()[-1]
                                rec["games"].setdefault(str(float(line)), {})[side] = a
                teams = set(rec["ml"]) or {t for (t, _, _) in rec["exact"]}
                if len(teams) == 2: out[frozenset(teams)] = rec
    if out: print(f"[series_odds] Bovada series markets: {len(out)} series")
    return out

def fetch_series_ml(series):
    """Series moneyline from Kalshi KXMLBSERIES. Each series posts TWO binaries (one per team).
    Price shown for each side = the cheapest way to actually back it, fee included:
        back A = min( A yes-ask , 1 - B yes-bid )   (buy A yes, or buy B no)
    so each side carries its own real price (and the spread/fee), not a mirrored midpoint."""
    if not ENABLE_SERIES_ML:
        return {}
    ev = {}
    for m, mid in _kalshi("KXMLBSERIES"):
        team = KALSHI_AB.get(m.get("ticker", "").split("-")[-1], m.get("ticker", "").split("-")[-1])
        ev.setdefault(_event_code(m), {})[team] = m
    out = {}
    for s_ in series:
        a, b = s_["a"], s_["b"]
        for teams in ev.values():
            if a in teams and b in teams:
                ma, mb = teams[a], teams[b]
                ca = _best_cost(_buy_yes(ma), _buy_no(mb))
                cb = _best_cost(_buy_yes(mb), _buy_no(ma))
                out[(s_["league"], a, b)] = {"ml_a": _american(ca), "ml_b": _american(cb), "book": "Kalshi"}
                break
    # sportsbooks via Action Network: best available price per side across Kalshi + books
    try:
        bk = _bk_actionnetwork(series)
    except Exception as e:
        print(f"[series_odds] book merge skipped: {e}", file=sys.stderr); bk = {}
    try:
        pin = _pinnacle_series()
        for s_ in series:
            pr = pin.get(frozenset({s_["a"], s_["b"]}))
            if not pr: continue
            e = bk.setdefault((s_["league"], s_["a"], s_["b"]), {"a": {}, "b": {}})
            e["a"]["Pinnacle"] = pr[s_["a"]]; e["b"]["Pinnacle"] = pr[s_["b"]]
        if pin: print(f"[series_odds] Pinnacle series ML: {len(pin)} series")
        global _BOV
        _BOV = _bovada_series()
        for s_ in series:
            r = _BOV.get(frozenset({s_["a"], s_["b"]}))
            if not r or len(r["ml"]) != 2: continue
            e = bk.setdefault((s_["league"], s_["a"], s_["b"]), {"a": {}, "b": {}})
            e["a"]["Bovada"] = r["ml"][s_["a"]]; e["b"]["Bovada"] = r["ml"][s_["b"]]
    except Exception as ex:
        print(f"[series_odds] Pinnacle merge skipped: {ex}", file=sys.stderr)
    # Deciding game (WC 1-1, LDS 2-2, LCS/WS 3-3): the series IS today's game, so the book price is that
    # game's moneyline (data/odds.json, best across books). Books' series-futures boards lag badly here
    # (FanDuel still had PHI +116 on the series with PHI -112 on the Game 3 ML).
    try:
        gml = _game_ml_today()
        for s_ in series:
            w = s_.get("wins") or {}; need = (s_.get("best_of") or 3) // 2 + 1
            if (w.get("a") or 0) == need - 1 and (w.get("b") or 0) == need - 1:
                m = gml.get(frozenset({s_["a"], s_["b"]}))
                key = (s_["league"], s_["a"], s_["b"])
                if m:
                    bk[key] = {"a": {f"{m[s_['a']][1]} G{2*need-1} ML": m[s_["a"]][0]},
                               "b": {f"{m[s_['b']][1]} G{2*need-1} ML": m[s_["b"]][0]}}
                else:
                    bk.pop(key, None)
    except Exception as e:
        print(f"[series_odds] decider ML swap skipped: {e}", file=sys.stderr)
    for key, v in bk.items():
        cur = out.setdefault(key, {"ml_a": None, "ml_b": None, "book": None})
        books = {"a": dict(v["a"]), "b": dict(v["b"])}
        if cur.get("ml_a") is not None: books["a"]["Kalshi"] = cur["ml_a"]
        if cur.get("ml_b") is not None: books["b"]["Kalshi"] = cur["ml_b"]
        for side in ("a", "b"):
            o, name = _best([(o, n) for n, o in books[side].items()])
            cur[f"ml_{side}"] = o; cur[f"book_{side}"] = name
        cur["book"] = cur.get("book_a") if cur.get("book_a") == cur.get("book_b") else "best"
        cur["books"] = books
    return out

_BOV = {}
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
        c = _buy_yes(m)
        if side and w.isdigit() and l.isdigit() and c is not None:
            _EXACT_PROB.setdefault((s_["league"], s_["a"], s_["b"]), {})[f"{side}-{w}-{l}"] = c
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
            if p > 0:   # bo3: exactly the 2-0 contract's cost; bo5+: sum of the qualifying contracts' costs
                d[f"{side}_minus_1_5"] = _american(min(p, 0.99))
        if d:
            out[key] = d
    # direct series-spread markets (KXMLBSERIESSPREAD-<event>-<TEAM><n>: TEAM wins by n+ = -(n-0.5)g).
    # Back "TEAM -x.5" = buy YES; back "OPP +x.5" = buy NO on the same contract. Overrides the exact-sum proxy.
    for m, mid in _kalshi("KXMLBSERIESSPREAD"):
        s_ = _series_for_event(_event_code(m), series)
        if not s_:
            continue
        suf = m.get("ticker", "").split("-")[-1]
        team, n = KALSHI_AB.get(suf[:-1], suf[:-1]), suf[-1]
        if not n.isdigit() or team not in (s_["a"], s_["b"]):
            continue
        side = "a" if team == s_["a"] else "b"; opp = "b" if side == "a" else "a"
        lab = f"{int(n)-1}_5"
        d = out.setdefault((s_["league"], s_["a"], s_["b"]), {})
        cy, cn = _buy_yes(m), _buy_no(m)
        if cy is not None: d[f"{side}_minus_{lab}"] = _american(cy)
        if cn is not None: d[f"{opp}_plus_{lab}"] = _american(cn)
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
        ov, un = _american(_buy_yes(m)), _american(_buy_no(m))
        if ov is None and un is None:
            continue
        o = out.setdefault((s_["league"], s_["a"], s_["b"]), {"ladder": {}})
        o["ladder"][str(line)] = {"over": ov, "under": un}
    # keep the legacy single-line fields (lowest line) for older board builds
    for o in out.values():
        ln = sorted(o["ladder"], key=float)[0]
        o.update({"line": float(ln), **o["ladder"][ln]})
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
            out[want[suf]] = _american(_buy_yes(m))
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

    man = _manual_props(series_in)
    for s_ in series_in:   # live Bovada series props join the pasted books
        r = _BOV.get(frozenset({s_["a"], s_["b"]}))
        if not r: continue
        o = man.setdefault((s_["league"], s_["a"], s_["b"]), {"exact": {}, "spread": {}, "games": {}})
        side = lambda t: "a" if t == s_["a"] else "b"
        for (t, w, l), v in r["exact"].items(): o["exact"].setdefault(f"{side(t)}-{w}-{l}", []).append((v, "Bovada"))
        for (t, hc), v in r["spread"].items():
            o["spread"].setdefault(f"{side(t)}_{'minus' if hc < 0 else 'plus'}_{int(abs(hc))}_5", []).append((v, "Bovada"))
        for ln, ou in r["games"].items():
            g = o["games"].setdefault(ln, {"over": [], "under": []})
            for sd in ("over", "under"):
                if ou.get(sd) is not None: g[sd].append((ou[sd], "Bovada"))
    def _merge(kal, extra):
        """best price per key across Kalshi (already a buy price) and pasted books -> (odds, books)"""
        best, src = dict(kal or {}), {k: "Kalshi" for k in (kal or {})}
        for k, lst in extra.items():
            for o, b in lst:
                if best.get(k) is None or _dec(o) > _dec(best[k]): best[k], src[k] = o, b
        return (best or None), src
    series_out = []
    for s in series_in:
        key = (s["league"], s["a"], s["b"])
        m = ml.get(key, {})
        series_out.append({
            "league": s["league"], "a": s["a"], "b": s["b"],
            "ml_a": m.get("ml_a"), "ml_b": m.get("ml_b"), "book": m.get("book"),
            "book_a": m.get("book_a", m.get("book")), "book_b": m.get("book_b", m.get("book")),
            "books": m.get("books"),   # {'a': {brand: odds}, 'b': {...}} incl. Kalshi
            "exact":  cs.get(key),      # dict or None
            "spread": sp.get(key),      # dict or None
            "games":  ou.get(key),      # {'line','over','under'} or None
        })
        mm = man.get(key)
        if mm:
            row = series_out[-1]
            row["exact"], row["exact_book"] = _merge(row["exact"], mm["exact"])
            row["spread"], row["spread_book"] = _merge(row["spread"], mm["spread"])
            g = row["games"] or {"ladder": {}}
            lad = {ln: dict(v) for ln, v in (g.get("ladder") or {}).items()}
            gbook = {}
            for ln, sides in mm["games"].items():
                cur = lad.setdefault(ln, {"over": None, "under": None})
                for side, lst in sides.items():
                    for o, b in lst:
                        if cur.get(side) is None or _dec(o) > _dec(cur[side]):
                            cur[side] = o; gbook[f"{ln} {side}"] = b
            if lad:
                ln0 = sorted(lad, key=float)[0]
                row["games"] = {"ladder": lad, "line": float(ln0), **lad[ln0], "book": gbook}

    pairings_out = [{"al": p["al"], "nl": p["nl"], "market": ex.get((p["al"], p["nl"]))}
                    for p in pairings_in]

    out = {
        "generated_at": datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "+00:00",
        "source": "Series ML: best of Kalshi + Pinnacle + Bovada + sportsbooks via Action Network; series props: best of Kalshi + Bovada + pasted DK/Caesars (data/series_odds_manual.json); Kalshi + sportsbooks via Action Network (DK/FD/Caesars/MGM/bet365/Fanatics/BetRivers); Kalshi: KXMLBSERIES (series ML), KXMLBSERIESSCORE (exact score + derived -1.5g), KXMLBSERIESGAMES (total games ladder), KXMLBSERIESSPREAD (series -1.5/-2.5), KXTEAMSINWS (WS matchup)",
        "price_type": "buy",   # every market price = real cost to back that side (ask / 1-bid, Kalshi fee included); do NOT de-vig
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
