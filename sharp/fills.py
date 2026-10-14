#!/usr/bin/env python3
"""The bet ledger (spec §8). A fill is written BEFORE first pitch with the price taken, the stake, the rule tag, and the fair at that moment.
Anything written after the start is kept but tagged late and excluded from CLV. 'Discussed' and 'would have' rows are not fills.

  python3 -m sharp.fills add --game MIL@LAD --market ml --side home --venue FD --price -174 --stake 1 --tag "rule:open_gap"
  python3 -m sharp.fills add --game MIL@LAD --market prop_K --player "Blake Snell" --line 6.5 --side over --venue Novig --price 117 --stake 0.5 --tag "rule:book_vs_exchange"
  python3 -m sharp.fills grade          # fair at close + result for every settled fill (run by the workflow)
  python3 -m sharp.fills list

Prices: American for books; for exchanges give the ask you paid as American (Kalshi: include the fee, i.e. what the ticket actually cost).
"""
import os, sys, json, uuid, argparse, sqlite3, datetime as dt
HERE = os.path.dirname(os.path.abspath(__file__)); sys.path.insert(0, os.path.dirname(HERE))
from sharp import norm as N, fair as F, settle as S

ROOT = os.path.abspath(os.path.join(HERE, '..')); SD = os.path.join(ROOT, 'data', 'sharp')
DB = os.path.join(SD, 'sharp.sqlite'); FILLS = os.path.join(SD, 'fills.jsonl')


def load():
    out = []
    if os.path.exists(FILLS):
        for l in open(FILLS):
            try: out.append(json.loads(l))
            except Exception: pass
    return out


def save(rows):
    with open(FILLS, 'w') as f:
        for r in rows: f.write(json.dumps(r) + '\n')


def find_event(conn, game, date=None):
    a, h = game.upper().split('@'); a, h = N.AB_FIX.get(a, a), N.AB_FIX.get(h, h)
    q = "SELECT event_id, start_utc, et_date FROM events WHERE away=? AND home=?" + (" AND et_date=?" if date else "") + " ORDER BY start_utc DESC"
    rows = conn.execute(q, (a, h, date) if date else (a, h)).fetchall()
    if not rows: sys.exit(f"no event {game} in the log" + (f" on {date}" if date else ""))
    now = dt.datetime.now(dt.timezone.utc)
    upcoming = sorted([r for r in rows if N.parse_ts(r[1]) > now - dt.timedelta(hours=1)], key=lambda r: r[1])   # nearest first pitch
    return (upcoming or rows)[0]


def add(args):
    conn = sqlite3.connect(DB)
    if args.event: ev, start, _ = conn.execute("SELECT event_id, start_utc, et_date FROM events WHERE event_id=?", (args.event,)).fetchone()
    else: ev, start, _ = find_event(conn, args.game, args.date)
    now = dt.datetime.now(dt.timezone.utc); ts = N.iso(now)
    part = N.player_key(args.player) if args.player else None
    if args.market in ('ml', 'rl', 'f5_ml', 'f5_rl', 'tt_home', 'tt_away', 'f5_tt_home', 'f5_tt_away'):
        away, home = conn.execute("SELECT away, home FROM events WHERE event_id=?", (ev,)).fetchone()
        part = home if (args.side == 'home' or args.market.endswith('home')) else (away if (args.side == 'away' or args.market.endswith('away')) else None)
    eq = F.EventQuotes(conn, ev)
    fr = eq.fair(args.market, args.line, args.side, ts, part)
    imp = N.implied_from_american(args.price)
    late = N.parse_ts(start) <= now
    row = dict(fill_id=uuid.uuid4().hex[:12], ts_utc=ts, event_id=ev, start_utc=start, market=args.market, mclass=N.MARKET_CLASS(args.market),
               participant=part, player_name=args.player, line=args.line, side=args.side, venue=args.venue, price_american=int(args.price),
               price_decimal=round(N.american_to_decimal(args.price), 4), implied=round(imp, 5), stake=float(args.stake), rule_tag=args.tag,
               fair_at_fill=round(fr['fair'], 5) if fr['fair'] is not None else None, fair_at_fill_status=fr['status'],
               fair_at_fill_detail={k: (round(v['p'], 4) if v['p'] is not None else v['why']) for k, v in fr['detail'].items()},
               fair_weights={k: round(v, 3) for k, v in fr['weights'].items()}, late=int(late), note=args.note)
    rows = load(); rows.append(row); save(rows)
    clv = (row['fair_at_fill'] - imp) if row['fair_at_fill'] is not None else None
    print(f"fill {row['fill_id']}: {args.market} {args.side} {args.line or ''} {args.player or part} @ {args.venue} {args.price:+d} x{args.stake}u "
          f"| fair now {row['fair_at_fill']} ({fr['status']}, {fr['n']} venues) | edge vs fair {clv * 100:+.1f} pts" if clv is not None else
          f"fill {row['fill_id']} written; NO fair at fill ({fr['n']} venues quoting) -> excluded from CLV", ' | LATE (after start)' if late else '')


def grade(_=None):
    conn = sqlite3.connect(DB); rows = load(); n = 0
    res = {r[0]: dict(status=r[1], home_score=r[3], away_score=r[4], f5_home=r[5], f5_away=r[6], rfi=r[7], players=json.loads(r[8] or '{}'))
           for r in conn.execute("SELECT * FROM results")}
    for r in rows:
        if r.get('result') is not None: continue
        ev = r['event_id']; e = conn.execute("SELECT away, home, start_utc FROM events WHERE event_id=?", (ev,)).fetchone()
        if not e: continue
        if r.get('fair_at_close') is None and N.parse_ts(e[2]) < dt.datetime.now(dt.timezone.utc):
            eq = F.EventQuotes(conn, ev); fr = eq.fair(r['market'], r['line'], r['side'], e[2], r['participant'])
            r['fair_at_close'] = round(fr['fair'], 5) if fr['fair'] is not None else None; r['fair_at_close_status'] = fr['status']
        rr = res.get(ev)
        if rr and rr['status'] in ('final', 'postponed'):
            out = S.outcome(r['market'], r['line'], r['side'], r['participant'], rr, r['venue'], (e[0], e[1]))
            r['result'] = {1: 'W', 0: 'L', None: 'P'}.get(out, 'void'); r['pnl_units'] = round(S.pnl(out, r['implied'], r['stake']), 4); n += 1
    save(rows)
    conn.execute("DELETE FROM fills")
    conn.executemany("INSERT INTO fills VALUES (" + ','.join('?' * 25) + ")", [
        (r['fill_id'], r['ts_utc'], r['event_id'], r['start_utc'], r['market'], r['mclass'], r.get('participant'), r.get('player_name'), r.get('line'), r['side'],
         r['venue'], r['price_american'], r['price_decimal'], r['implied'], r['stake'], r['rule_tag'], r.get('fair_at_fill'), r.get('fair_at_fill_status'),
         json.dumps(r.get('fair_at_fill_detail')), r.get('fair_at_close'), r.get('fair_at_close_status'), r.get('late', 0), r.get('result'), r.get('pnl_units'), r.get('note')) for r in rows])
    conn.commit(); conn.close()
    print(f"[fills] {len(rows)} fills on file, {n} newly graded")


def list_(_=None):
    for r in load():
        print(f"{r['ts_utc']} {r['fill_id']} {r['event_id']} {r['market']} {r['side']} {r.get('line') or ''} {r.get('player_name') or r.get('participant') or ''} "
              f"@{r['venue']} {r['price_american']:+d} {r['stake']}u tag={r['rule_tag']} fair_fill={r.get('fair_at_fill')} fair_close={r.get('fair_at_close')} "
              f"result={r.get('result')} pnl={r.get('pnl_units')}{' LATE' if r.get('late') else ''}")


if __name__ == '__main__':
    ap = argparse.ArgumentParser(); sub = ap.add_subparsers(dest='cmd', required=True)
    a = sub.add_parser('add')
    a.add_argument('--game', help='AWAY@HOME, e.g. MIL@LAD'); a.add_argument('--event', help='gamePk instead of --game'); a.add_argument('--date', help='ET date YYYY-MM-DD for --game')
    a.add_argument('--market', required=True, help='ml rl total tt_home tt_away f5_ml f5_rl f5_total rfi prop_K prop_TB ...')
    a.add_argument('--side', required=True, choices=['home', 'away', 'over', 'under'])
    a.add_argument('--line', type=float); a.add_argument('--player'); a.add_argument('--venue', required=True)
    a.add_argument('--price', type=int, required=True, help='American odds actually taken'); a.add_argument('--stake', type=float, default=1.0, help='units risked')
    a.add_argument('--tag', required=True, help='rule / reason tag, written now, never later'); a.add_argument('--note')
    sub.add_parser('grade'); sub.add_parser('list')
    args = ap.parse_args()
    {'add': add, 'grade': grade, 'list': list_}[args.cmd](args)
