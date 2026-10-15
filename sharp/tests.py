#!/usr/bin/env python3
"""Pre-registered venue tests (spec §6), written against the `closes` table before the data exists.

Every test returns {name, sport, gate, n, published, ...}. Below the gate the numbers are NOT reported (published=False, value=None);
`--preview` prints them locally, labelled PREVIEW, and the export never carries them. Gates live in constants.py with provenance.

  A  accuracy: devigged close vs outcome, by decile + Brier, per venue
  B  speed: gap closure at 24h/6h/1h, per venue, with the reaction-speed control (feed-lag control waits on measured lags)
  C  head-to-head edge conversion on props, per venue
  D  the over tax: overs vs unders priced-vs-hit per venue, and C split by side
  opener: side moved toward by 3+ points open->close: open, close, hit rate
"""
import os, sys, json, sqlite3, random, statistics, collections, argparse
HERE = os.path.dirname(os.path.abspath(__file__)); sys.path.insert(0, os.path.dirname(HERE))
from sharp import constants as C

ROOT = os.path.abspath(os.path.join(HERE, '..')); SD = os.path.join(ROOT, 'data', 'sharp'); DB = os.path.join(SD, 'sharp.sqlite')
OTHER = {'home': 'away', 'away': 'home', 'over': 'under', 'under': 'over'}


def rows(conn, where='', args=()):
    conn.row_factory = sqlite3.Row
    return [dict(r) for r in conn.execute(f"SELECT * FROM closes WHERE outcome IS NOT NULL AND p_close IS NOT NULL AND vclass != 'reference' {where}", args)]


def gate_for(mclass):
    return C.MIN_EVENTS_GAME_LINES['value'] if mclass == 'game' else C.MIN_PROP_ROWS['value']


def n_events(rs): return len({r['event_id'] for r in rs})


def boot_ci(vals, fn=statistics.mean, k=400, seed=7):
    if len(vals) < 5: return None
    rnd = random.Random(seed); out = []
    for _ in range(k): out.append(fn([vals[rnd.randrange(len(vals))] for _ in range(len(vals))]))
    out.sort(); return [round(out[int(.025 * k)], 4), round(out[int(.975 * k)], 4)]


def publish(res, gate, n, preview):
    res['gate'] = gate; res['n'] = n; res['published'] = n >= gate
    if not res['published'] and not preview:
        res = {k: (None if k in ('value', 'venues', 'table') else v) for k, v in res.items()}
        res['note'] = f"gated: {n} of {gate} required"
    elif not res['published']: res['note'] = f"PREVIEW below gate ({n} of {gate}); not publishable"
    return res


# ------------------------------------------------------------------ A ----
def test_a(conn, mclass, preview=False):
    rs = rows(conn, "AND mclass=? AND close_age_min <= ?", (mclass, C.STALE_BOOK_MIN['value']))
    n = n_events(rs) if mclass == 'game' else len(rs)
    venues = {}
    for v, L in _group(rs, 'venue').items():
        dec = collections.defaultdict(list)
        for r in L: dec[min(9, int(r['p_close'] * 10))].append(r)
        venues[v] = dict(n=len(L), n_events=n_events(L), brier=round(statistics.mean((r['p_close'] - r['outcome']) ** 2 for r in L), 5),
                         priced=round(statistics.mean(r['p_close'] for r in L), 4), hit=round(statistics.mean(r['outcome'] for r in L), 4),
                         deciles=[dict(bucket=f"{d * 10}-{d * 10 + 10}", n=len(x), priced=round(statistics.mean(r['p_close'] for r in x), 3),
                                       hit=round(statistics.mean(r['outcome'] for r in x), 3)) for d, x in sorted(dec.items())])
    return publish(dict(test='A', name='accuracy (calibration)', mclass=mclass, venues=venues), gate_for(mclass), n, preview)


def _group(rs, key):
    g = collections.defaultdict(list)
    for r in rs: g[r[key]].append(r)
    return g


# ------------------------------------------------------------------ B ----
def test_b(conn, mclass, preview=False):
    rs = rows(conn, "AND mclass=?", (mclass,))
    n = n_events(rs) if mclass == 'game' else len(rs)
    thr = C.DISAGREEMENT_THRESHOLD_ML['value']
    by_mkt = _group(rs, 'event_id')
    out = {}
    for hz in ('24h', '6h', '1h'):
        per_venue = collections.defaultdict(list); per_venue_ctrl = collections.defaultdict(list)
        for ev, L in by_mkt.items():
            cells = collections.defaultdict(dict)
            for r in L: cells[(r['market'], r['line'], r['side'], r['participant'])][r['venue']] = r
            for key, vs in cells.items():
                for v, r in vs.items():
                    pv = r[f'p_{hz}']
                    others = [u for u in vs.values() if u['venue'] != v and u[f'p_{hz}'] is not None and u['p_close'] is not None]
                    if pv is None or len(others) < 2: continue
                    med_t = statistics.median(u[f'p_{hz}'] for u in others); med_c = statistics.median(u['p_close'] for u in others)
                    gap = pv - med_t
                    if abs(gap) < thr: continue
                    closure = max(-1.0, min(2.0, (med_c - med_t) / gap))
                    per_venue[v].append(closure)
                    # reaction-speed control: the disagreement had to exist at the earlier horizon too (same sign, above threshold)
                    prev = {'1h': '6h', '6h': '24h'}.get(hz)
                    if prev and r[f'p_{prev}'] is not None:
                        others_prev = [u[f'p_{prev}'] for u in others if u[f'p_{prev}'] is not None]
                        if len(others_prev) >= 2:
                            gap_prev = r[f'p_{prev}'] - statistics.median(others_prev)
                            if abs(gap_prev) >= thr and (gap_prev > 0) == (gap > 0): per_venue_ctrl[v].append(closure)
        out[hz] = {v: dict(n=len(x), closure_pct=round(100 * statistics.mean(x), 1), ci=boot_ci(x),
                           n_ctrl=len(per_venue_ctrl.get(v, [])),
                           closure_ctrl_pct=(round(100 * statistics.mean(per_venue_ctrl[v]), 1) if per_venue_ctrl.get(v) else None))
                   for v, x in per_venue.items() if x}
    return publish(dict(test='B', name='speed (gap closure)', mclass=mclass, threshold=thr, table=out,
                        note_feed_lag='feed-lag control not run: no venue lag measured yet (constants.FEED_LAG_S)'), gate_for(mclass), n, preview)


# ------------------------------------------------------------------ C ----
def test_c(conn, preview=False, side_filter=None):
    rs = rows(conn, "AND mclass='prop' AND close_age_min <= ?", (C.STALE_BOOK_MIN['value'],))
    thr = C.DISAGREEMENT_THRESHOLD_ML['value']
    cells = collections.defaultdict(dict)
    for r in rs:
        if side_filter and r['side'] != side_filter: continue
        cells[(r['event_id'], r['market'], r['line'], r['side'], r['participant'])][r['venue']] = r
    bets = collections.defaultdict(list); pairs = collections.Counter()
    for key, vs in cells.items():
        names = sorted(vs)
        for i, v in enumerate(names):
            for u in names:
                if u == v: continue
                rv, ru = vs[v], vs[u]
                if abs(rv['p_close'] - ru['p_close']) < thr: continue
                pairs[v] += 1
                paid = ru['close_implied']                      # raw cost of taking this side at u
                if rv['p_close'] - paid >= thr:                  # a bet at u that beats v's close
                    bets[v].append(dict(paid=paid, exp=rv['p_close'], hit=rv['outcome']))
    venues = {}
    for v, B in bets.items():
        hit = statistics.mean(b['hit'] for b in B); paid = statistics.mean(b['paid'] for b in B); exp = statistics.mean(b['exp'] for b in B)
        conv = (hit - paid) / (exp - paid) if exp - paid > 1e-6 else None
        venues[v] = dict(n_pairs=pairs[v], n_bets=len(B), hit=round(hit, 4), paid=round(paid, 4), expected=round(exp, 4),
                         edge_real_pct=(round(100 * conv, 1) if conv is not None else None),
                         ci=boot_ci([(b['hit'] - b['paid']) / max(1e-6, (b['exp'] - b['paid'])) for b in B], k=200),
                         published=pairs[v] >= C.MIN_PAIRS_PROPS['value'])
    n = min(pairs.values()) if pairs else 0
    res = dict(test='C', name='head-to-head edge conversion (props)' + (f', {side_filter}s only' if side_filter else ''), threshold=thr, venues=venues,
               pairs_per_venue=dict(pairs))
    return publish(res, C.MIN_PAIRS_PROPS['value'], max(pairs.values()) if pairs else 0, preview)


# ------------------------------------------------------------------ D ----
def test_d(conn, preview=False):
    rs = rows(conn, "AND mclass='prop' AND close_age_min <= ?", (C.STALE_BOOK_MIN['value'],))
    venues = {}
    for v, L in _group(rs, 'venue').items():
        d = {}
        for side in ('over', 'under'):
            x = [r for r in L if r['side'] == side]
            if len(x) < 5: continue
            d[side] = dict(n=len(x), priced=round(statistics.mean(r['p_close'] for r in x), 4), hit=round(statistics.mean(r['outcome'] for r in x), 4),
                           tax_pts=round(100 * (statistics.mean(r['p_close'] for r in x) - statistics.mean(r['outcome'] for r in x)), 2),
                           ci_hit=boot_ci([r['outcome'] for r in x], k=200))
        venues[v] = d
    res = dict(test='D', name='the over tax', venues=venues)
    res = publish(res, C.MIN_PROP_ROWS['value'], len(rs), preview)
    res['c_by_side'] = {s: test_c(conn, preview, side_filter=s) for s in ('over', 'under')}
    return res


# ------------------------------------------------------------------ opener ----
def test_opener(conn, mclass, preview=False):
    rs = rows(conn, "AND mclass=? AND p_open IS NOT NULL AND close_age_min <= ?", (mclass, C.STALE_BOOK_MIN['value']))
    thr = C.OPENER_MOVE_PTS['value']
    venues = {}
    for v, L in _group(rs, 'venue').items():
        x = [r for r in L if r['p_close'] - r['p_open'] >= thr]
        if len(x) < 5: continue
        venues[v] = dict(n=len(x), open=round(statistics.mean(r['p_open'] for r in x), 4), close=round(statistics.mean(r['p_close'] for r in x), 4),
                         hit=round(statistics.mean(r['outcome'] for r in x), 4), ci_hit=boot_ci([r['outcome'] for r in x], k=200))
    return publish(dict(test='opener', name='side moved toward by 3+ points: open, close, hit', mclass=mclass, venues=venues),
                   gate_for(mclass), n_events(rs) if mclass == 'game' else len(rs), preview)


def run_all(preview=False):
    conn = sqlite3.connect(DB)
    out = dict(A={m: test_a(conn, m, preview) for m in ('game', 'prop')},
               B={m: test_b(conn, m, preview) for m in ('game', 'prop')},
               C=test_c(conn, preview), D=test_d(conn, preview),
               opener={m: test_opener(conn, m, preview) for m in ('game', 'prop')},
               steam=dict(test='steam', published=False, note='not implemented yet: needs a tick-level pass (3+ point hourly moves, continuation to close and to result)'),
               E={'novig_prop': test_e(conn, 'Novig', 60, 'prop', preview), 'novig_game': test_e(conn, 'Novig', 60, 'game', preview),
                  'kalshi_prop': test_e(conn, 'Kalshi', 60, 'prop', preview), 'pinnacle_prop': test_e(conn, 'Pinnacle', 60, 'prop', preview)})
    conn.close(); return out


if __name__ == '__main__':
    ap = argparse.ArgumentParser(); ap.add_argument('--preview', action='store_true', help='print below-gate numbers, labelled PREVIEW (local only)')
    a = ap.parse_args()
    print(json.dumps(run_all(a.preview), indent=1, default=str))


# ------------------------------------------------------------------ E: EV vs Novig at T-1h ----
def test_e(conn, ruler='Novig', horizon_min=60, mclass='prop', preview=False):
    """Sean 10/14 (table from another account): bets at a book whose price beats the ruler's devigged price ~1h before start, bucketed by
    EV, graded at the book price. Same line, same side, same player; ruler must be a two-sided quote inside the spread rule.
    ROI per bucket with a t-stat that treats bets as independent (it overstates: both sides and same-game props are correlated)."""
    import math, datetime as dt
    from sharp import fair as F, norm as N, settle as S
    res = {r[0]: dict(status=r[1], home_score=r[3], away_score=r[4], f5_home=r[5], f5_away=r[6], rfi=r[7], players=json.loads(r[8] or '{}'))
           for r in conn.execute("SELECT * FROM results")}
    bets = []
    for ev, start, away, home in conn.execute("SELECT event_id, start_utc, away, home FROM events").fetchall():
        r = res.get(ev)
        if not r or r['status'] != 'final': continue
        eq = F.EventQuotes(conn, ev); st = N.parse_ts(start); ts = N.iso(st - dt.timedelta(minutes=horizon_min))
        meth = C.DEVIG_METHOD[('mlb', mclass)]['value']
        for (v, m, l, s, p), L in eq.idx.items():
            if N.MARKET_CLASS(m) != mclass or v == ruler or C.VENUE_CLASS.get(v) in ('reference', 'exchange') or v in C.UNBETTABLE_VENUES['value']: continue
            q = eq.latest(v, m, l, s, ts, C.HORIZON_TOLERANCE_MIN['value'], p)
            if not q or q[5] is None: continue
            pr, why = eq.venue_prob(ruler, m, l, s, ts, meth, p, max_age=C.HORIZON_TOLERANCE_MIN['value'])
            if pr is None: continue
            out = S.outcome(m, l, s, p, r, v, (away, home))
            if out not in (0, 1): continue
            cost = q[5]; ev_pct = pr / cost - 1
            bets.append(dict(ev=ev_pct, pnl=S.pnl(out, cost), venue=v, side=s))
    table = []
    for lo in (0.0, 0.02, 0.04, 0.06, 0.08):
        x = [b['pnl'] for b in bets if b['ev'] >= lo]
        if len(x) < 2: table.append(dict(filter=f"EV>={int(lo*100)}%", bets=len(x))); continue
        mu = statistics.mean(x); sd = statistics.pstdev(x) or 1e-9
        table.append(dict(filter=f"EV>={int(lo*100)}%", bets=len(x), roi_pct=round(100 * mu, 2), t=round(mu / (sd / math.sqrt(len(x))), 2)))
    by_side = {s: dict(n=len([b for b in bets if b['side'] == s and b['ev'] >= 0.02]),
                       roi_pct=(round(100 * statistics.mean([b['pnl'] for b in bets if b['side'] == s and b['ev'] >= 0.02]), 2)
                                if [b for b in bets if b['side'] == s and b['ev'] >= 0.02] else None)) for s in ('over', 'under')}
    out = dict(test='E', name=f'EV vs {ruler} at T-{horizon_min}m ({mclass}), graded at the book price', ruler=ruler, horizon_min=horizon_min,
               table=table, by_side_ev2=by_side, note_t='t treats bets as independent; both sides and same-game props are correlated, so it overstates')
    return publish(out, C.MIN_PROP_ROWS['value'] if mclass == 'prop' else C.MIN_EVENTS_GAME_LINES['value'], len(bets), preview)
