#!/usr/bin/env python3
"""Grade the Sharp Sides & Totals board once games settle.

Reads data/game_lines/board/*.jsonl (first appearance of each position = the call you would have acted on) and
data/game_lines/board_close/*.json (Pinnacle / consensus / best price frozen in the last 30 min before first pitch).
Results from MLB StatsAPI linescores (full game + first five).
Writes data/game_lines/board_graded.json and data/game_lines/board_summary.json (by market, by level, by signal).
CLV first: Pinnacle close no-vig minus the entry cost, same line only. P/L per $1 staked at the entry price.
"""
import json, os, glob, collections, statistics as st, urllib.request, datetime as dt

HERE = os.path.dirname(os.path.abspath(__file__))
GL = os.environ.get('LINES_DIR') or os.path.join(HERE, '..', 'data', 'game_lines')
MLB = 'https://statsapi.mlb.com/api/v1'
NORM = {'CHW': 'CWS', 'AZ': 'ARI', 'WAS': 'WSH', 'OAK': 'ATH'}

def g(u):
    try: return json.load(urllib.request.urlopen(urllib.request.Request(u, headers={'User-Agent': 'Mozilla/5.0'}), timeout=30))
    except Exception: return {}

_res = {}
def results(date):
    if date in _res: return _res[date]
    out = {}
    x = g(f"{MLB}/schedule?sportId=1&date={date}&hydrate=linescore,team")
    for d in x.get('dates', []):
        for gm in d['games']:
            if gm.get('status', {}).get('abstractGameState') != 'Final': continue
            a = NORM.get(gm['teams']['away']['team'].get('abbreviation'), gm['teams']['away']['team'].get('abbreviation'))
            h = NORM.get(gm['teams']['home']['team'].get('abbreviation'), gm['teams']['home']['team'].get('abbreviation'))
            inn = (gm.get('linescore') or {}).get('innings', [])
            f5a = sum((i.get('away') or {}).get('runs', 0) or 0 for i in inn[:5]); f5h = sum((i.get('home') or {}).get('runs', 0) or 0 for i in inn[:5])
            out[(a, h, gm['gameDate'][:13])] = {'a': gm['teams']['away'].get('score'), 'h': gm['teams']['home'].get('score'), 'f5a': f5a, 'f5h': f5h}
    _res[date] = out; return out

def outcome(p, r):
    """1 win, 0 loss, None push"""
    m, side, line = p['mkt'], p['side'], p.get('line')
    if m in ('ml', 'lag_ml'): return int((r['a'] > r['h']) == (side == 'away'))
    if m == 'f5_ml':
        if r['f5a'] == r['f5h']: return 0 if 'Kalshi' in (p.get('venue') or '') else None   # Kalshi 3-way loses on a tie; books push
        return int((r['f5a'] > r['f5h']) == (side == 'away'))
    tot = {'total': r['a'] + r['h'], 'lag_total': r['a'] + r['h'], 'f5_total': r['f5a'] + r['f5h'], 'tt_away': r['a'], 'tt_home': r['h']}.get(m)
    if tot is None or line is None: return None
    if tot == line: return None
    return int((tot > line) == (side == 'over'))

def main():
    C = {}
    for f in glob.glob(os.path.join(GL, 'board_close', '*.json')):
        try: C.update(json.load(open(f)))
        except Exception: pass
    first = {}
    for f in sorted(glob.glob(os.path.join(GL, 'board', '*.jsonl'))):
        for l in open(f):
            try: p = json.loads(l)
            except Exception: continue
            if p['key'] not in first: first[p['key']] = p
    rows = []
    for k, p in first.items():
        fp = p['first_pitch']; date = dt.datetime.fromisoformat(fp).astimezone(dt.timezone(dt.timedelta(hours=-4))).date().isoformat()
        R = results(date); utc = dt.datetime.fromisoformat(fp).astimezone(dt.timezone.utc).strftime('%Y-%m-%dT%H')
        r = R.get((p['away'], p['home'], utc)) or next((v for (a, h, t), v in R.items() if a == p['away'] and h == p['home']), None)
        if not r: continue
        w = outcome(p, r); cost = p.get('cost')
        c = C.get(k) or {}; pin = c.get('pinnacle') or {}
        clv_pin = (pin['p'] - cost) * 100 if pin.get('p') is not None and cost and pin.get('line') in (None, p.get('line')) else None
        clv_mkt = (c['market_fair'] - cost) * 100 if c.get('market_fair') is not None and cost else None
        pl = None if w is None or not cost else ((1 / cost - 1) if w else -1.0)
        sig = p.get('signals') or {}; mv = (sig.get('move') or {}).get('pts')
        rows.append({**{x: p.get(x) for x in ('key', 'game', 'date', 'mkt', 'label', 'side', 'line', 'venue', 'odds', 'cost', 'model', 'market_fair', 'edge', 'edge_mkt', 'level', 'threshold', 'ts')},
                     'result': w, 'pl': pl, 'clv_pinnacle_pts': clv_pin, 'clv_market_pts': clv_mkt,
                     'move_pts_at_call': mv, 'moving_toward': (mv > 0) if mv is not None else None,
                     'public_money_minus_tickets': ((p.get('public') or {}).get('money_pct') or 0) - ((p.get('public') or {}).get('tickets_pct') or 0) if p.get('public') else None})
    json.dump(rows, open(os.path.join(GL, 'board_graded.json'), 'w'), indent=0)
    def summ(groups):
        S = {}
        for gk, v in groups.items():
            pl = [x['pl'] for x in v if x['pl'] is not None]; cp = [x['clv_pinnacle_pts'] for x in v if x['clv_pinnacle_pts'] is not None]
            S[str(gk)] = {'n': len(v), 'n_decided': len(pl), 'win_pct': round(sum(1 for x in v if x['result'] == 1) / max(1, len(pl)), 3),
                          'roi': round(st.mean(pl), 4) if pl else None, 'clv_pinnacle_pts': round(st.mean(cp), 2) if cp else None, 'n_clv': len(cp)}
        return S
    G = lambda f: collections.defaultdict(list, {})
    by_m, by_l, by_mv = collections.defaultdict(list), collections.defaultdict(list), collections.defaultdict(list)
    for x in rows:
        by_m['tt' if (x['mkt'] or '').startswith('tt') else x['mkt']].append(x); by_l[x['level']].append(x)
        by_mv['toward' if x['moving_toward'] else ('against' if x['moving_toward'] is False else 'no move data')].append(x)
    json.dump({'n': len(rows), 'by_market': summ(by_m), 'by_level': summ(by_l), 'by_move_at_call': summ(by_mv)}, open(os.path.join(GL, 'board_summary.json'), 'w'), indent=1)
    print(f"[sides_grade] {len(rows)} positions graded")

if __name__ == '__main__':
    main()
