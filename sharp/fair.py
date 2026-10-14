"""The ruler: a weighted, devigged, cross-venue fair probability for one side of one market at one timestamp (spec §5-§7).

No projection enters here. Weights are the PRIOR vector from constants.py until fit_weights() has replaced it; every result carries
`status` ('prior' | 'fitted') and the venue detail so the dashboard can tag it.
"""
import os, sys, json, bisect, datetime as dt, collections
HERE = os.path.dirname(os.path.abspath(__file__)); sys.path.insert(0, os.path.dirname(HERE))
from sharp import constants as C, norm as N, devig as DV

OTHER = {'home': 'away', 'away': 'home', 'over': 'under', 'under': 'over'}
EXCHANGES = {v for v, c in C.VENUE_CLASS.items() if c == 'exchange'}
WEIGHTS_FILE = os.path.join(os.path.dirname(HERE), 'data', 'sharp', 'weights_fitted.json')


def weights_for(sport, mclass):
    """Fitted vector when one exists for this sport/class and passed its gate, else the prior. Returns (dict, status, meta)."""
    if os.path.exists(WEIGHTS_FILE):
        w = json.load(open(WEIGHTS_FILE)).get(f"{sport}:{mclass}")
        if w and w.get('status') == 'fitted': return w['value'], 'fitted', w
    c = C.WEIGHTS[(sport, mclass)]
    return c['value'], 'prior', c


def capped_weights(w):
    """Normalise and cap any venue at WEIGHT_CAP of the total (spec §7); redistribute the excess to the others."""
    w = {k: float(v) for k, v in w.items() if v and v > 0}
    cap = C.WEIGHT_CAP['value']
    for _ in range(10):
        s = sum(w.values())
        if not s: return {}
        n = {k: v / s for k, v in w.items()}
        over = {k: v for k, v in n.items() if v > cap + 1e-9}
        if not over or len(n) == 1: return n
        excess = sum(v - cap for v in over.values()); rest = {k: v for k, v in n.items() if k not in over}
        rs = sum(rest.values())
        w = {**{k: cap for k in over}, **{k: v + excess * (v / rs if rs else 1 / len(rest)) for k, v in rest.items()}}
    return n


class EventQuotes:
    """All ticks of one event in memory, indexed by (venue, market, line, side) and sorted by time."""
    def __init__(self, conn, event_id):
        self.idx = collections.defaultdict(list)
        self.event = conn.execute("SELECT event_id, et_date, start_utc, away, home FROM events WHERE event_id=?", (event_id,)).fetchone()
        for r in conn.execute("""SELECT ts_utc, venue, market, line, side, implied, bid, ask, mid, flags, price_american, "limit", participant, player_name FROM ticks
                                 WHERE event_id=? ORDER BY ts_utc""", (event_id,)):
            self.idx[(r[1], r[2], r[3], r[4], r[12])].append(r)
        self.keys = collections.defaultdict(set)      # (market, line, participant) -> venues
        self.names = {}
        for (v, m, l, s, p) in self.idx:
            self.keys[(m, l, p)].add(v)
            if p and self.idx[(v, m, l, s, p)][0][13]: self.names[p] = self.idx[(v, m, l, s, p)][0][13]

    def latest(self, venue, market, line, side, ts_iso, max_age_min, participant=None):
        L = self.idx.get((venue, market, line, side, participant))
        if not L: return None
        i = bisect.bisect_right([r[0] for r in L], ts_iso) - 1
        if i < 0: return None
        r = L[i]
        age = (N.parse_ts(ts_iso) - N.parse_ts(r[0])).total_seconds() / 60
        if age > max_age_min: return None
        return r

    def venue_prob(self, venue, market, line, side, ts_iso, method, participant=None, max_age=None):
        """Devigged probability of `side` at `venue` as of ts, or (None, reason)."""
        is_x = venue in EXCHANGES
        if max_age is None: max_age = C.STALE_EXCHANGE_MIN['value'] if is_x else C.STALE_BOOK_MIN['value']
        r = self.latest(venue, market, line, side, ts_iso, max_age, participant)
        if not r: return None, 'no_quote'
        other_line = line if market not in ('rl', 'f5_rl') or line is None else -line
        other_part = participant
        if market in ('ml', 'rl', 'f5_ml', 'f5_rl') and self.event: other_part = self.event[4] if side == 'away' else self.event[3]
        o = self.latest(venue, market, other_line, OTHER.get(side), ts_iso, max_age, other_part)
        flags = set((r[9] or '').split(','))
        if is_x:
            if r[6] is None or r[7] is None: return None, 'oneside'
            if 'wide' in flags or (r[7] - r[6]) > C.EXCHANGE_SPREAD_MAX['value']: return None, 'wide'
            p = r[8]
            if market == 'f5_ml' and venue == 'Kalshi':     # three-way (tie is its own ticker): condition on no tie
                if not o or o[8] is None: return None, 'no_other'
                p = p / (p + o[8])
            return p, 'ok'
        if not o: return None, 'no_other'
        return DV.devig_pair(r[5], o[5], method), 'ok'

    def fair(self, market, line, side, ts_iso, participant=None, sport='mlb'):
        mclass = N.MARKET_CLASS(market)
        w, status, meta = weights_for(sport, mclass)
        method = C.DEVIG_METHOD[(sport, mclass)]['value']
        detail = {}; probs = {}
        for v in self.keys.get((market, line, participant), set()):
            if v not in C.RULER_ELIGIBLE: continue
            p, why = self.venue_prob(v, market, line, side, ts_iso, method, participant)
            detail[v] = {'p': p, 'why': why}
            if p is not None and w.get(v, 0) > 0: probs[v] = p
        if not probs: return dict(fair=None, status=status, n=0, detail=detail, weights={})
        cw = capped_weights({v: w[v] for v in probs})
        fair = sum(cw[v] * probs[v] for v in probs)
        return dict(fair=fair, status=status, n=len(probs), detail=detail, weights=cw, devig=method)


def best_price(eq, market, line, side, ts_iso, participant=None, exclude=()):
    """Best bettable price (highest decimal / lowest implied) across venues at ts, excluding unbettable venues. Returns list sorted best-first."""
    rows = []
    for (v, m, l, s, p), L in eq.idx.items():
        if m != market or l != line or s != side or p != participant or v in C.UNBETTABLE_VENUES['value'] or v in exclude: continue
        if C.VENUE_CLASS.get(v) == 'reference': continue
        r = eq.latest(v, m, l, s, ts_iso, 90, p)
        if not r or r[5] is None: continue
        cost = r[5]
        if v == 'Kalshi' and r[7] is not None:
            import math
            cost = r[7] + math.ceil(C.KALSHI_TAKER_FEE['value'] * r[7] * (1 - r[7]) * 100) / 100
        rows.append(dict(venue=v, implied=r[5], cost=cost, american=r[10], cost_american=N.prob_to_american(cost), ts=r[0], limit=r[11], bid=r[6], ask=r[7], flags=r[9]))
    return sorted(rows, key=lambda x: x['cost'])
