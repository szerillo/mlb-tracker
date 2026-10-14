#!/usr/bin/env python3
"""Build data/sharp/dashboard.json for sharp_dashboard.html.

Sections: board (upcoming games: every market x side with the prior-weighted fair, the venue ladder and the best bettable price),
props (same, per player x stat x line), venues (Tests A-D, gated), ledger (fills), health (log cadence per venue per day), constants.
"""
import os, sys, json, sqlite3, datetime as dt, collections
HERE = os.path.dirname(os.path.abspath(__file__)); sys.path.insert(0, os.path.dirname(HERE))
from sharp import constants as C, norm as N, fair as F, tests as T

ROOT = os.path.abspath(os.path.join(HERE, '..')); SD = os.path.join(ROOT, 'data', 'sharp'); DB = os.path.join(SD, 'sharp.sqlite')
NOW = dt.datetime.now(dt.timezone.utc); NOW_S = N.iso(NOW)
GAME_ORDER = ['ml', 'rl', 'total', 'tt_away', 'tt_home', 'f5_ml', 'f5_rl', 'f5_total', 'f5_tt_away', 'f5_tt_home', 'rfi']
MIN_VENUES_FOR_EDGE = 3   # fair shown only when at least this many ruler venues quote the cell (Fable 10/14; rule, revisit)


def r4(x): return None if x is None else round(x, 4)


def cell(eq, market, line, side, part, ts):
    fr = eq.fair(market, line, side, ts, part)
    ladder = []
    for v, d in sorted(fr['detail'].items(), key=lambda kv: (kv[1]['p'] is None, -(kv[1]['p'] or 0))):
        q = eq.latest(v, market, line, side, ts, 90, part)
        ladder.append(dict(venue=v, p=r4(d['p']), why=d['why'] if d['p'] is None else None, w=round(fr['weights'].get(v, 0), 3),
                           american=q[10] if q else None, bid=r4(q[6]) if q else None, ask=r4(q[7]) if q else None, limit=q[11] if q else None,
                           age_min=round((N.parse_ts(ts) - N.parse_ts(q[0])).total_seconds() / 60) if q else None,
                           flags=q[9] if q else None))
    best = F.best_price(eq, market, line, side, ts, part)
    for b in best:
        for k in ('implied', 'cost', 'bid', 'ask'): b[k] = r4(b[k])
    fair = fr['fair'] if fr['n'] >= MIN_VENUES_FOR_EDGE else None
    edge = (fair - best[0]['cost']) if (fair is not None and best) else None
    # reference quotes: AN consensus public %, AN open
    cons = eq.latest('AN_Consensus', market, line, side, ts, 10 ** 6, part); opn = eq.latest('AN_Open', market, line, side, ts, 10 ** 6, part)
    return dict(market=market, line=line, side=side, participant=part, player=eq.names.get(part) if part else None,
                fair=r4(fair), fair_n=fr['n'], fair_status=fr['status'], fair_american=N.prob_to_american(fair) if fair else None,
                best=best[:4], edge_pts=(round(100 * edge, 2) if edge is not None else None), ladder=ladder,
                open_american=opn[10] if opn else None, open_p=r4(N.implied_from_american(opn[10])) if opn else None, public=None)


def public_pct(conn, ev, market, side):
    r = conn.execute("SELECT extra FROM ticks WHERE event_id=? AND venue='AN_Consensus' AND market=? AND side=? AND extra IS NOT NULL ORDER BY ts_utc DESC LIMIT 1",
                     (ev, market, side)).fetchone()
    return json.loads(r[0]) if r else None


def main_line(eq, market, part=None):
    """The 'main' line for a totals-type market: the line with the most venues, ties to the one nearest 50%."""
    cands = [(k[1], len(v)) for k, v in eq.keys.items() if k[0] == market and k[2] == part and k[1] is not None]
    if not cands: return []
    mx = max(n for _, n in cands)
    return sorted({l for l, n in cands if n == mx})


def board(conn):
    out = []
    for ev, et_date, start, away, home, status in conn.execute("SELECT event_id, et_date, start_utc, away, home, status FROM events ORDER BY start_utc").fetchall():
        st = N.parse_ts(start)
        if st < NOW - dt.timedelta(hours=1) or st > NOW + dt.timedelta(hours=40): continue
        eq = F.EventQuotes(conn, ev); ts = NOW_S
        last_tick = max((L[-1][0] for L in eq.idx.values()), default=None)
        if last_tick and (NOW - N.parse_ts(last_tick)).total_seconds() > 90 * 60:
            ts = last_tick   # the log is stale (job not running): price the board at the last observation and say so
        g = dict(event_id=ev, et_date=et_date, start_utc=start, away=away, home=home, status=status, priced_at=ts,
                 stale_min=round((NOW - N.parse_ts(ts)).total_seconds() / 60), minutes_to_start=round((st - N.parse_ts(ts)).total_seconds() / 60), markets=[])
        for m in GAME_ORDER:
            if m in ('ml', 'f5_ml'):
                for side, part in (('away', away), ('home', home)):
                    c = cell(eq, m, None, side, part, ts)
                    if c['ladder']: c['public'] = public_pct(conn, ev, m, side); g['markets'].append(c)
            elif m in ('rl', 'f5_rl'):
                lines = sorted({k[1] for k in eq.keys if k[0] == m and k[1] is not None}, key=lambda l: abs(abs(l) - (1.5 if m == 'rl' else 0.5)))
                for line in lines[:2]:
                    for side, part in (('away', away), ('home', home)):
                        c = cell(eq, m, line if side == 'home' else -line, side, part, ts)
                        if c['ladder']: c['public'] = public_pct(conn, ev, m, side); g['markets'].append(c)
            else:
                part = home if m.endswith('home') else (away if m.endswith('away') else None)
                for line in main_line(eq, m, part)[:1] if m != 'rfi' else [0.5]:
                    for side in ('over', 'under'):
                        c = cell(eq, m, line, side, part, ts)
                        if c['ladder']: c['public'] = public_pct(conn, ev, m, side) if m in ('total',) else None; g['markets'].append(c)
        # Pinnacle limit growth (first post vs now) per market
        g['pinnacle_limits'] = {}
        for (v, m, l, s, p), L in eq.idx.items():
            if v == 'Pinnacle' and N.MARKET_CLASS(m) == 'game' and L[0][11]:
                g['pinnacle_limits'][m] = dict(first=L[0][11], now=L[-1][11], first_ts=L[0][0])
        # props: every player x stat x line with 2+ venues; keep the main line per player-stat (most venues)
        props = []
        groups = collections.defaultdict(list)
        for (m, l, p), vs in eq.keys.items():
            if m.startswith('prop_') and p: groups[(m, p)].append((l, len(vs)))
        for (m, p), ls in groups.items():
            ls.sort(key=lambda x: (-x[1], x[0]))
            for l, nv in ls[:2]:
                if nv < 2: continue
                for side in ('over', 'under'):
                    c = cell(eq, m, l, side, p, ts)
                    if c['ladder']: c['stat'] = m[5:]; props.append(c)
        props.sort(key=lambda c: -(c['edge_pts'] if c['edge_pts'] is not None else -99))
        g['props'] = props
        out.append(g)
    return out


def health(conn):
    rows = [dict(zip(('et_date', 'venue', 'mclass', 'n_ticks', 'n_events', 'first_ts', 'last_ts'), r)) for r in conn.execute("SELECT * FROM log_health ORDER BY 1,2,3")]
    # snapshots per game per venue (how many distinct observation times) -> the cadence check
    cad = [dict(venue=r[0], mclass=r[1], games=r[2], snaps_per_game=round(r[3], 1)) for r in conn.execute("""
        SELECT venue, mclass, COUNT(DISTINCT event_id), AVG(n) FROM (
          SELECT venue, mclass, event_id, COUNT(DISTINCT substr(ts_utc,1,16)) n FROM ticks GROUP BY 1,2,3) GROUP BY 1,2""")]
    unmatched = {}
    return dict(by_day=rows, cadence=cad, feed_lag={v: c['value'] for v, c in C.FEED_LAG_S.items()},
                note="expected ~40 snapshots per game-day at the 20-minute job cadence (10 AM to midnight ET); far fewer means the job is not landing")


def ledger(conn):
    conn.row_factory = sqlite3.Row
    rows = [dict(r) for r in conn.execute("SELECT * FROM fills ORDER BY ts_utc DESC")]
    for r in rows:
        r['clv_fill_pts'] = round(100 * (r['fair_at_fill'] - r['implied']), 2) if r.get('fair_at_fill') is not None and not r['late'] else None
        r['clv_close_pts'] = round(100 * (r['fair_at_close'] - r['implied']), 2) if r.get('fair_at_close') is not None and not r['late'] else None
        try: r['fair_at_fill_detail'] = json.loads(r['fair_at_fill_detail'] or 'null')
        except Exception: pass
    graded = [r for r in rows if r.get('result') in ('W', 'L', 'P')]
    summ = collections.defaultdict(lambda: dict(n=0, units=0.0, pnl=0.0, clv=[], w=0, l=0, p=0))
    for r in graded:
        for key in (('all', 'all'), ('venue', r['venue']), ('market', r['market']), ('side', r['side']), ('tag', r['rule_tag']), ('mclass', r['mclass'])):
            s = summ[f"{key[0]}:{key[1]}"]; s['n'] += 1; s['units'] += r['stake']; s['pnl'] += r['pnl_units'] or 0
            s[{'W': 'w', 'L': 'l', 'P': 'p'}[r['result']]] += 1
            if r['clv_close_pts'] is not None: s['clv'].append(r['clv_close_pts'])
    for s in summ.values():
        s['clv_avg'] = round(sum(s['clv']) / len(s['clv']), 2) if s['clv'] else None; s['n_clv'] = len(s['clv']); del s['clv']
        s['pnl'] = round(s['pnl'], 3); s['roi'] = round(100 * s['pnl'] / s['units'], 1) if s['units'] else None
    conn.row_factory = None
    return dict(fills=rows, summary=dict(summ))


def main():
    conn = sqlite3.connect(DB)
    tests = T.run_all(preview=False)
    wf = json.load(open(F.WEIGHTS_FILE)) if os.path.exists(F.WEIGHTS_FILE) else {}
    nev = conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]; nset = conn.execute("SELECT COUNT(*) FROM results WHERE status='final'").fetchone()[0]
    nt = conn.execute("SELECT COUNT(*) FROM ticks").fetchone()[0]
    out = dict(built=NOW_S, sport='mlb', counts=dict(events=nev, settled=nset, ticks=nt,
                                                    prop_rows_settled=conn.execute("SELECT COUNT(*) FROM closes WHERE mclass='prop' AND outcome IS NOT NULL").fetchone()[0]),
               weights=dict(game=F.weights_for('mlb', 'game')[0], game_status=F.weights_for('mlb', 'game')[1],
                            prop=F.weights_for('mlb', 'prop')[0], prop_status=F.weights_for('mlb', 'prop')[1], fit_file=wf),
               board=board(conn), venues=tests, ledger=ledger(conn), health=health(conn), constants=C.dump(),
               spec='SHARP_TRACKER_BUILD_SPEC_2026-10-14.md')
    def strip(x):   # drop null fields to keep the committed file small
        if isinstance(x, dict): return {k: strip(v) for k, v in x.items() if v is not None}
        if isinstance(x, list): return [strip(v) for v in x]
        return x
    json.dump(strip(out), open(os.path.join(SD, 'dashboard.json'), 'w'), default=str, separators=(',', ':'))
    print(f"[export] dashboard.json: {len(out['board'])} games on the board, {sum(len(g['props']) for g in out['board'])} prop cells, {len(out['ledger']['fills'])} fills, "
          f"{os.path.getsize(os.path.join(SD, 'dashboard.json')) // 1024} KB")
    conn.close()


if __name__ == '__main__':
    main()
