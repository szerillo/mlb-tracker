#!/usr/bin/env python3
"""Paper bet tracker: every model's calls in one ledger, graded open -> close -> result.

Sources (all already produced by the pipeline; this script only reads and normalises):
  data/clv_archive/YYYY-MM-DD.json        Sheet (Action PRO) number at the open vs the close: ML, total, F5 ML, F5 total (compute_clv.py)
  data/game_lines/board_graded.json       Sides board: sheet vs best current price (books + Kalshi), plus the Kalshi lag rule (sides_grade.py)
  data/prop_scan/board_graded.json        Sharp Props board calls (board_grade.py)
  data/prop_scan/scan_graded.json         Every prop-scan flag, original grader (scan_grade.py)
  data/prop_scan/tickets_graded.json      Sean's logged tickets (board_grade.py)
Writes data/bet_tracker.json (one row per paper bet) for bet_tracker.html.

Units: 1 unit risked per bet. P/L uses the entry price (American odds, or Kalshi cost incl. fee).
CLV is reported in the source's own unit (pts of probability for prices, runs for totals lines); `beat_close` is the sign.
"""
import json, os, glob, re, datetime as dt

HERE = os.path.dirname(os.path.abspath(__file__))
D = os.path.abspath(os.path.join(HERE, '..', 'data'))
THRESH = {'ml': 3.0, 'total': 2.5, 'f5_ml': 3.5, 'f5_total': 3.5}
MKT = {'ml': 'ML', 'total': 'Total', 'f5_ml': 'F5 ML', 'f5_total': 'F5 total', 'tt_away': 'Team total', 'tt_home': 'Team total',
       'lag_ml': 'ML', 'lag_total': 'Total', 'K': 'K prop', 'EARLY': 'YRFI / F5 over', 'RFI': 'YRFI', 'F5': 'F5 over', 'WALK': 'Walks prop',
       'HIT': 'Hitter prop', 'HSP': 'Hitter prop (vs SP)', 'OUTS': 'Outs prop', 'KOP': 'Prop (Kalshi vs Pinnacle)'}
imp = lambda o: 100 / (o + 100) if o > 0 else -o / (-o + 100)
def to_am(p):
    if p is None or p <= 0 or p >= 1: return None
    return -round(100 * p / (1 - p)) if p >= .5 else round(100 * (1 - p) / p)
def payout(cost):   # units won per 1 unit risked at an implied cost
    return (1 - cost) / cost if cost else None
def load(p, default):
    try: return json.load(open(p))
    except Exception: return default
def res_word(w):
    return {1: 'W', 0: 'L', None: 'P'}.get(w, 'P')

rows = []

# ---------- A. Sheet number at the open vs the close (clv_archive) ----------
for f in sorted(glob.glob(os.path.join(D, 'clv_archive', '*.json'))):
    day = load(f, {}); date = day.get('date') or os.path.basename(f)[:10]
    for g in day.get('games', []):
        game = f"{g.get('away')} @ {g.get('home')}"
        for mk in ('ml', 'total', 'f5_ml', 'f5_total'):
            x = g.get(mk)
            if not x or not x.get('lean'): continue
            r = {'date': date, 'model': 'Sheet at the open', 'source': 'clv_archive', 'market': MKT[mk], 'mkt': mk, 'game': game,
                 'edge': x.get('edge_pct'), 'threshold': THRESH[mk], 'final': g.get('final')}
            r['qualified'] = r['edge'] is not None and r['edge'] >= THRESH[mk]
            if mk in ('ml', 'f5_ml'):
                team = g.get('away') if x['lean'] == 'away' else g.get('home')
                o = x.get('open_odds'); c = x.get('close_odds')
                r.update(pick=f"{team} {'F5 ' if mk == 'f5_ml' else ''}ML", side=x['lean'], open=o, close=c, entry_odds=o,
                         clv=x.get('clv_pct'), clv_unit='%')
                cost = imp(o) if o is not None else None
            else:
                r.update(pick=f"{'F5 ' if mk == 'f5_total' else ''}{x['lean']} {x.get('open_line')}", side=x['lean'].lower(), line=x.get('open_line'),
                         open=x.get('open_line'), close=x.get('close_line'), entry_odds=-110, price_note='totals priced at -110 (open price not archived)',
                         clv=x.get('clv_pts'), clv_unit='runs', proj=x.get('proj'))
                cost = imp(-110)
            res = x.get('result'); w = {'win': 1, 'loss': 0}.get(res)
            r['direction'] = x.get('direction')
            r['beat_close'] = None if r.get('clv') is None else (r['clv'] > 0)
            r['result'] = {'win': 'W', 'loss': 'L', 'push': 'P'}.get(res, 'pending')
            r['pl'] = None if res not in ('win', 'loss', 'push') or cost is None else (payout(cost) if res == 'win' else (-1.0 if res == 'loss' else 0.0))
            rows.append(r)

# ---------- B. Sides board (sheet vs best current price; Kalshi lag rule) ----------
for x in load(os.path.join(D, 'game_lines', 'board_graded.json'), []):
    mk = x.get('mkt') or ''
    lag = mk.startswith('lag')
    cost = x.get('cost')
    r = {'date': x.get('date'), 'model': 'Kalshi lag rule (Fable test)' if lag else 'Sides board (best price)', 'source': 'game_lines',
         'market': MKT.get(mk, mk), 'mkt': mk, 'game': x.get('game'), 'pick': x.get('label'), 'side': x.get('side'), 'line': x.get('line'),
         'venue': x.get('venue'), 'entry_odds': x.get('odds') if x.get('odds') is not None else to_am(cost), 'edge': x.get('edge'),
         'threshold': x.get('threshold'), 'level': x.get('level'), 'qualified': x.get('level') in ('bet', 'lead'),
         'clv': x.get('clv_pinnacle_pts') if x.get('clv_pinnacle_pts') is not None else x.get('clv_market_pts'), 'clv_unit': 'pts',
         'clv_src': 'Pinnacle close' if x.get('clv_pinnacle_pts') is not None else 'market close'}
    r['beat_close'] = None if r['clv'] is None else r['clv'] > 0
    r['direction'] = None if r['clv'] is None else ('correct' if r['clv'] > 0 else ('wrong' if r['clv'] < 0 else 'push'))
    w = x.get('result'); r['result'] = 'pending' if w is None and x.get('pl') is None else res_word(w)
    r['pl'] = x.get('pl')
    rows.append(r)

# ---------- C. Sharp Props board ----------
for x in load(os.path.join(D, 'prop_scan', 'board_graded.json'), []):
    cost = x.get('entry_cost'); bo = x.get('book_odds')
    venue = 'Kalshi' if (x.get('best_venue') != 'book' or bo is None) else x.get('book')
    entry = to_am(cost) if venue == 'Kalshi' else bo
    pl = (x.get('pl_kalshi') / cost) if (venue == 'Kalshi' and cost and x.get('pl_kalshi') is not None) else x.get('pl_book')
    clv = x.get('clv_pinnacle') if x.get('clv_pinnacle') is not None else x.get('clv_kalshi')
    fp = x.get('first_pitch') or ''
    r = {'date': fp[:10], 'model': 'Sharp Props board', 'source': 'prop_scan', 'market': MKT.get(x.get('cat'), x.get('cat')), 'mkt': x.get('cat'),
         'game': x.get('game'), 'pick': x.get('label'), 'side': x.get('side'), 'venue': venue, 'entry_odds': entry, 'edge': x.get('edge'),
         'level': x.get('level'), 'rules': ', '.join(x.get('rules') or []), 'qualified': x.get('level') in ('bet', 'lead', 'lean'),
         'clv': None if clv is None else round(clv * 100, 2), 'clv_unit': 'pts', 'clv_src': 'Pinnacle close' if x.get('clv_pinnacle') is not None else 'Kalshi close',
         'result': res_word(x.get('win')), 'pl': None if pl is None else round(pl, 4)}
    r['beat_close'] = None if r['clv'] is None else r['clv'] > 0
    r['direction'] = None if r['clv'] is None else ('correct' if r['clv'] > 0 else 'wrong')
    rows.append(r)

# ---------- D. Every prop-scan flag (original grader) ----------
_seen = set()
TEAMS = ['ARI','AZ','ATL','BAL','BOS','CHC','CWS','CHW','CIN','CLE','COL','DET','HOU','KC','LAA','LAD','MIA','MIL','MIN','NYM','NYY','ATH','OAK','PHI','PIT','SD','SF','SEA','STL','TB','TEX','TOR','WSH','WAS']
def game_of(ev):
    """Kalshi game code (26OCT071600CLECWS) -> 'CLE @ CWS'"""
    c = str(ev or '').split('-', 1)[-1]; t = re.sub(r'G\d$', '', c[11:])
    for a in TEAMS:
        if t.startswith(a) and t[len(a):] in TEAMS: return f"{a} @ {t[len(a):]}"
    return c
def _flag_edge(x):
    try: return (x.get('fair') or 0) - (x.get('price') or 1)
    except Exception: return -9
MON = {m: i + 1 for i, m in enumerate(['JAN','FEB','MAR','APR','MAY','JUN','JUL','AUG','SEP','OCT','NOV','DEC'])}
def first_pitch_utc(ev):
    """Kalshi game code 26OCT071600CLECWS -> first pitch (ET in the code) as UTC ISO, else None"""
    m = re.match(r'^(\d\d)([A-Z]{3})(\d\d)(\d\d)(\d\d)', str(ev or '').split('-', 1)[-1])
    if not m or m.group(2) not in MON: return None
    t = dt.datetime(2000 + int(m.group(1)), MON[m.group(2)], int(m.group(3)), int(m.group(4)), int(m.group(5)), tzinfo=dt.timezone(dt.timedelta(hours=-4)))
    return t.astimezone(dt.timezone.utc).isoformat()
def pregame(x):
    fp = first_pitch_utc(x.get('event'))
    try: return not (fp and x.get('ts')) or dt.datetime.fromisoformat(x['ts']) < dt.datetime.fromisoformat(fp)
    except Exception: return True
_scan = [x for x in load(os.path.join(D, 'prop_scan', 'scan_graded.json'), []) if pregame(x)]   # in-game rows from before the fix are dropped
_scan.sort(key=lambda x: (x.get('ts') or '', -_flag_edge(x)))   # earliest scan first; within a scan, the best-edge rung first
for x in _scan:
    base = re.sub(r'\s[ou]\d+(\.\d+)?(\s*/.*)?$', '', str(x.get('what') or ''))
    k = (x.get('rule'), str(x.get('event') or '').split('-', 1)[-1], base)
    if k in _seen: continue          # one position per rule x game x player: later scans and other rungs of the same ladder are the same bet
    _seen.add(k)
    px = x.get('price')
    cost = px + 0.07 * px * (1 - px) if px is not None else None
    win = None
    if x.get('result') in ('yes', 'no'): win = int((x['result'] == 'yes') == (x.get('side') == 'YES'))
    clv = x.get('clv')
    r = {'date': (x.get('ts') or '')[:10], 'model': f"Prop scan: {x.get('rule')}", 'source': 'prop_scan_flags', 'market': 'Prop flag',
         'mkt': 'flag', 'game': game_of(x.get('event')), 'pick': re.sub(r'^\d\d[A-Z]{3}\d{6}[A-Z]+\s', '', str(x.get('what') or '')), 'side': x.get('side'), 'venue': 'Kalshi', 'entry_odds': to_am(cost),
         'level': f"tier {x.get('tier')}", 'qualified': x.get('tier') in (1, 2), 'rules': x.get('rule'),
         'clv': None if clv is None else round(clv * 100, 2), 'clv_unit': 'pts', 'clv_src': 'Kalshi close',
         'result': 'pending' if win is None else res_word(win), 'pl': None if (x.get('pl') is None or not cost) else round(x['pl'] / cost, 4)}
    r['beat_close'] = None if r['clv'] is None else r['clv'] > 0
    r['direction'] = None if r['clv'] is None else ('correct' if r['clv'] > 0 else 'wrong')
    rows.append(r)

# ---------- E. Sean's tickets ----------
for x in load(os.path.join(D, 'prop_scan', 'tickets_graded.json'), []):
    cost = x.get('cost'); w = x.get('win')
    clv = x.get('clv_pinnacle') if x.get('clv_pinnacle') is not None else x.get('clv_kalshi')
    r = {'date': x.get('date'), 'model': 'My tickets', 'source': 'tickets', 'market': x.get('rule_tier') or 'Ticket', 'mkt': 'ticket',
         'game': x.get('game'), 'pick': x.get('bet'), 'side': x.get('side'), 'venue': x.get('book'), 'entry_odds': x.get('price'),
         'size': x.get('size'), 'qualified': True, 'clv': None if clv is None else round(clv * 100, 2), 'clv_unit': 'pts',
         'result': 'pending' if w is None else res_word(w), 'pl': None if (w is None or not cost) else ((1 / cost - 1) if w else -1.0)}
    r['beat_close'] = None if r['clv'] is None else r['clv'] > 0
    rows.append(r)

for r in rows:   # beat_close: True / False, None when the close did not move (or no close)
    if r.get('clv') is not None and r['clv'] == 0: r['beat_close'] = None
    for k, v in list(r.items()):
        if isinstance(v, float): r[k] = round(v, 4)
rows.sort(key=lambda r: (r.get('date') or '', r.get('model') or ''), reverse=True)
out = {'generated_at': dt.datetime.now(dt.timezone.utc).isoformat(), 'n': len(rows),
       'sources': sorted({r['model'] for r in rows}), 'rows': rows}
json.dump(out, open(os.path.join(D, 'bet_tracker.json'), 'w'), separators=(',', ':'))
print(f"[bet_tracker] {len(rows)} paper bets from {len(out['sources'])} models")
