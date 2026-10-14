#!/usr/bin/env python3
"""Grade the archived Sharp Props board and Sean's tickets once markets settle.

Inputs:
  data/prop_scan/board/*.jsonl         (props_board_archive.py: every board row per run)
  data/prop_scan/board_close/*.json    (frozen Kalshi + Pinnacle closes, last 30 min before first pitch)
  data/fills.csv                       (Sean's tickets, written by fill_log.py add / close)
Outputs:
  data/prop_scan/board_graded.json     one row per board item (first actionable call): entry, closes, result, P/L, CLV
  data/prop_scan/tickets_graded.json   one row per fill: cost, CLV vs Pinnacle / Novig / Kalshi close, edge at fill, P/L
  data/prop_scan/tickets_summary.json  fills CLV / beat-close % / ROI by rule, side (over/under) and market
  data/prop_scan/board_summary.json    CLV / P/L by call level and by rule

Fill columns: see fill_log.py (COLS). Prices at fill and close are already oriented to the ticket's side.
"""
import json, os, glob, csv, time, re, collections, statistics as st, urllib.request, datetime as dt

HERE = os.path.dirname(os.path.abspath(__file__))
SCAN = os.environ.get("SCAN_DIR") or os.path.join(HERE, "..", "data", "prop_scan")
API = "https://api.elections.kalshi.com/trade-api/v2"
fee = lambda p: 0.07 * p * (1 - p)
imp = lambda o: 100 / (o + 100) if o > 0 else -o / (-o + 100)
_mk = {}
def market(t):
    if t in _mk: return _mk[t]
    for i in range(4):
        try:
            _mk[t] = json.load(urllib.request.urlopen(urllib.request.Request(f"{API}/markets/{t}", headers={'User-Agent': 'Mozilla/5.0'}), timeout=30)).get('market', {}); return _mk[t]
        except Exception: time.sleep(1.5 * (i + 1))
    _mk[t] = {}; return _mk[t]

def closes():
    out = {}
    for p in glob.glob(os.path.join(SCAN, 'board_close', '*.json')):
        try: out.update(json.load(open(p)))
        except Exception: pass
    return out

def grade_board():
    C = closes(); first = {}
    for p in sorted(glob.glob(os.path.join(SCAN, 'board', '*.jsonl'))):
        for l in open(p):
            try: r = json.loads(l)
            except Exception: continue
            if r.get('level') in ('info',) or r.get('cat') == 'HIT' or not r.get('ticker'): continue
            fp = r.get('first_pitch')
            if fp and r.get('ts') and r['ts'] >= dt.datetime.fromisoformat(fp).astimezone(dt.timezone.utc).isoformat(): continue   # logged after first pitch
            if r['key'] not in first: first[r['key']] = r   # first time the call appeared (the call we would have acted on)
    rows = []
    for k, r in first.items():
        m = market(r['ticker'])
        if m.get('result') not in ('yes', 'no'): continue
        y = 1 if m['result'] == 'yes' else 0; win = y if r['side'] == 'YES' else 1 - y
        px = r.get('kalshi_px')
        c = C.get(k, {}); kc = (c.get('kalshi_close') or {}).get('mid_side'); pc = (c.get('pinnacle_close') or {}).get('p_side')
        cost = px + fee(px) if px is not None else None
        rows.append({**{f: r.get(f) for f in ('key', 'game', 'first_pitch', 'cat', 'rules', 'label', 'side', 'ticker', 'level', 'size', 'model', 'market', 'edge', 'book', 'book_odds', 'best_venue', 'ts', 'f5_share')},
                     'entry_kalshi_px': px, 'entry_cost': cost, 'result': m['result'], 'win': win,
                     'pl_kalshi': (win - cost) if cost is not None else None,
                     'pl_book': ((100 / abs(r['book_odds']) if r['book_odds'] < 0 else r['book_odds'] / 100) if win else -1) if r.get('book_odds') is not None else None,
                     'kalshi_close_side': kc, 'pinnacle_close_side': pc,
                     'clv_kalshi': (kc - px) if (kc is not None and px is not None) else None,
                     'clv_pinnacle': (pc - cost) if (pc is not None and cost is not None) else None})
    json.dump(rows, open(os.path.join(SCAN, 'board_graded.json'), 'w'), indent=0)
    def summ(group):
        S = {}
        for g, v in group.items():
            ck = [x['clv_kalshi'] for x in v if x['clv_kalshi'] is not None]; cp = [x['clv_pinnacle'] for x in v if x['clv_pinnacle'] is not None]
            pl = [x['pl_kalshi'] for x in v if x['pl_kalshi'] is not None]
            S[g] = {'n': len(v), 'win_pct': round(st.mean(x['win'] for x in v), 3), 'pl_per_contract': round(st.mean(pl), 4) if pl else None,
                    'clv_kalshi_pts': round(100 * st.mean(ck), 2) if ck else None, 'clv_pinnacle_pts': round(100 * st.mean(cp), 2) if cp else None, 'n_pin': len(cp)}
        return S
    by_level = collections.defaultdict(list); by_rule = collections.defaultdict(list)
    for x in rows:
        by_level[x['level']].append(x)
        for ru in x['rules'] or []: by_rule[ru].append(x)
    # Fable 10/14: grade live F5 OVER by Kalshi F5-share quartile (Q4 = share >= .545); revisit sizing at 100 fills
    by_f5q = collections.defaultdict(list)
    for x in rows:
        if 'F5 OVER' in (x['rules'] or []):
            sh = x.get('f5_share'); by_f5q['no share' if sh is None else ('Q4 (>= .545)' if sh >= 0.545 else 'Q1-Q3 (< .545)')].append(x)
    json.dump({'by_level': summ(by_level), 'by_rule': summ(by_rule), 'f5_over_by_share': summ(by_f5q), 'n': len(rows)},
              open(os.path.join(SCAN, 'board_summary.json'), 'w'), indent=1)
    print(f"[board_grade] {len(rows)} settled board calls graded")

FILLS = os.path.join(SCAN, '..', 'fills.csv')   # data/fills.csv, written by fill_log.py (add / close)

def _f(x):
    try: return float(x)
    except (TypeError, ValueError): return None

def _cost(r):
    """Price paid as a probability. Kalshi: cents + taker fee. Novig: cents (no pregame fee). Books: American, vig included."""
    pr = (r.get('book_price') or '').strip(); bk = (r.get('book') or '').strip().lower()
    if re.match(r'^[+-]\d+$', pr): return imp(int(pr))
    v = _f(pr.rstrip('c'))
    if v is None: return None
    if v > 1: v /= 100
    return v + fee(v) if bk == 'kalshi' else v

def _payout(r, cost):
    """Profit per $1 staked on a win, at the actual fill price."""
    pr = (r.get('book_price') or '').strip()
    if re.match(r'^[+-]\d+$', pr): o = int(pr); return o / 100 if o > 0 else 100 / -o
    return (1 / cost - 1) if cost else None

def grade_tickets():
    """CLV grader for Sean's tickets: data/fills.csv (fill_log.py). Close ruler = Pinnacle no-vig close, else Novig no-vig close,
    else Kalshi close mid; same line and side as the ticket (fill_log.py already orients every price to the ticket's side)."""
    if not os.path.exists(FILLS): print('[board_grade] no data/fills.csv yet'); return
    out = []
    for r in csv.DictReader(open(FILLS)):
        if not (r.get('id') or '').strip(): continue
        cost = _cost(r)
        kb, ka = _f(r.get('k_bid_close')), _f(r.get('k_ask_close'))
        kmid = (kb + ka) / 2 if kb is not None and ka is not None else None
        kb0, ka0 = _f(r.get('k_bid_fill')), _f(r.get('k_ask_fill'))
        pin_c, nov_c = _f(r.get('pin_fair_close')), _f(r.get('novig_fair_close'))
        pin_f, nov_f = _f(r.get('pin_fair_fill')), _f(r.get('novig_fair_fill'))
        close = next(((v, n) for v, n in ((pin_c, 'Pinnacle'), (nov_c, 'Novig'), (kmid, 'Kalshi mid')) if v is not None), (None, None))
        fill_fair = next((v for v in (pin_f, nov_f, (kb0 + ka0) / 2 if kb0 is not None and ka0 is not None else None) if v is not None), None)
        res = (r.get('result') or '').upper(); win = {'WIN': 1, 'LOSS': 0}.get(res)
        pay = _payout(r, cost)
        row = {'id': r['id'], 'date': r.get('date'), 'game': r.get('game'), 'market': r.get('market'), 'player': r.get('player'),
               'side': r.get('side'), 'line': r.get('line'), 'book': r.get('book'), 'price': r.get('book_price'), 'size': r.get('stake'),
               'rule': r.get('rule'), 'rule_tier': r.get('rule'), 'bet': f"{r.get('player')} {r.get('market')} {r.get('side')} {r.get('line')}".replace('- ', ''),
               'ticker': r.get('kalshi_ticker'), 'cost': cost, 'win': win, 'result': res or None,
               'pl': None if win is None or pay is None else (pay if win else -1.0),
               'clv_close': None if close[0] is None or cost is None else close[0] - cost, 'clv_src': close[1],
               'clv_pinnacle': None if pin_c is None or cost is None else pin_c - cost,
               'clv_novig': None if nov_c is None or cost is None else nov_c - cost,
               'clv_kalshi': None if kmid is None or cost is None else kmid - cost,
               'edge_at_fill': None if fill_fair is None or cost is None else fill_fair - cost,
               'move_fill_to_close': None if fill_fair is None or close[0] is None else close[0] - fill_fair}
        out.append(row)
    json.dump(out, open(os.path.join(SCAN, 'tickets_graded.json'), 'w'), indent=0)
    def summ(key):
        g = collections.defaultdict(list)
        for x in out:
            if x['result'] != 'VOID': g[key(x)].append(x)
        res = {}
        for k, v in g.items():
            c = [x['clv_close'] for x in v if x['clv_close'] is not None]; pl = [x['pl'] for x in v if x['pl'] is not None]
            res[k] = {'n': len(v), 'n_clv': len(c), 'clv_pts': round(100 * st.mean(c), 2) if c else None,
                      'beat_close_pct': round(sum(x > 0 for x in c) / len(c), 3) if c else None,
                      'n_settled': len(pl), 'roi': round(st.mean(pl), 4) if pl else None}
        return res
    json.dump({'source': 'data/fills.csv', 'n': len(out), 'all': summ(lambda x: 'all').get('all'), 'by_rule': summ(lambda x: x['rule'] or '?'),
               'by_side': summ(lambda x: x['side'] or '?'), 'by_market': summ(lambda x: x['market'] or '?')},
              open(os.path.join(SCAN, 'tickets_summary.json'), 'w'), indent=1)
    print(f"[board_grade] {len(out)} fills graded from data/fills.csv")

if __name__ == '__main__':
    grade_board(); grade_tickets()
