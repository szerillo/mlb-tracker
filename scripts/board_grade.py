#!/usr/bin/env python3
"""Grade the archived Sharp Props board and Sean's tickets once markets settle.

Inputs:
  data/prop_scan/board/*.jsonl         (props_board_archive.py: every board row per run)
  data/prop_scan/board_close/*.json    (frozen Kalshi + Pinnacle closes, last 30 min before first pitch)
  data/prop_scan/tickets.csv           (Sean's tickets; optional. Columns below)
Outputs:
  data/prop_scan/board_graded.json     one row per board item (first actionable call): entry, closes, result, P/L, CLV
  data/prop_scan/tickets_graded.json   one row per ticket with the same fields
  data/prop_scan/board_summary.json    CLV / P/L by call level and by rule

tickets.csv columns (header row required):
  date,game,bet,book,price,size,rule_tier,kalshi_mid_at_bet,pinnacle_novig_at_bet,ticker,side
  price: American odds (e.g. +124) or Kalshi cents (e.g. 45c). ticker/side (YES|NO) optional but needed for auto-grading.
"""
import json, os, glob, csv, time, re, collections, statistics as st, urllib.request

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
            if r['key'] not in first: first[r['key']] = r   # first time the call appeared (the call we would have acted on)
    rows = []
    for k, r in first.items():
        m = market(r['ticker'])
        if m.get('result') not in ('yes', 'no'): continue
        y = 1 if m['result'] == 'yes' else 0; win = y if r['side'] == 'YES' else 1 - y
        px = r.get('kalshi_px')
        c = C.get(k, {}); kc = (c.get('kalshi_close') or {}).get('mid_side'); pc = (c.get('pinnacle_close') or {}).get('p_side')
        cost = px + fee(px) if px is not None else None
        rows.append({**{f: r.get(f) for f in ('key', 'game', 'first_pitch', 'cat', 'rules', 'label', 'side', 'ticker', 'level', 'size', 'model', 'market', 'edge', 'book', 'book_odds', 'best_venue', 'ts')},
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
    json.dump({'by_level': summ(by_level), 'by_rule': summ(by_rule), 'n': len(rows)}, open(os.path.join(SCAN, 'board_summary.json'), 'w'), indent=1)
    print(f"[board_grade] {len(rows)} settled board calls graded")

def grade_tickets():
    p = os.path.join(SCAN, 'tickets.csv')
    if not os.path.exists(p): return
    C = closes(); by_ticker = {}
    for k, c in C.items():
        t = (c.get('board') or {}).get('ticker')
        if t: by_ticker[t] = c
    out = []
    for r in csv.DictReader(open(p)):
        t = (r.get('ticker') or '').strip(); side = (r.get('side') or '').strip().upper()
        pr = (r.get('price') or '').strip()
        if pr.endswith('c'): cost = float(pr[:-1]) / 100; cost += fee(cost)
        elif re.match(r'^[+-]\d+$', pr): cost = imp(int(pr))
        else: cost = None
        row = dict(r, cost=cost)
        if t:
            m = market(t); c = by_ticker.get(t, {})
            if m.get('result') in ('yes', 'no') and side in ('YES', 'NO'):
                y = 1 if m['result'] == 'yes' else 0; row['win'] = y if side == 'YES' else 1 - y
            kc = (c.get('kalshi_close') or {}).get('mid_yes'); pc = (c.get('pinnacle_close') or {}).get('p_side')
            if kc is not None and side in ('YES', 'NO'): row['kalshi_close_side'] = kc if side == 'YES' else 1 - kc
            if pc is not None: row['pinnacle_close_side'] = pc
            if cost is not None and row.get('pinnacle_close_side') is not None: row['clv_pinnacle'] = row['pinnacle_close_side'] - cost
            if cost is not None and row.get('kalshi_close_side') is not None: row['clv_kalshi'] = row['kalshi_close_side'] - cost
        out.append(row)
    json.dump(out, open(os.path.join(SCAN, 'tickets_graded.json'), 'w'), indent=0)
    print(f"[board_grade] {len(out)} tickets graded")

if __name__ == '__main__':
    grade_board(); grade_tickets()
