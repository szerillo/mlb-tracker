#!/usr/bin/env python3
"""Sharp Sides & Totals board: Sean's model vs every venue, plus market movement, Pinnacle, public splits and Kalshi flow.

Runs right after scripts/game_lines_log.py (every 20 min + after each prop scan). Reads:
  data/game_lines/latest.json + pinnacle_log.jsonl + an_log.jsonl + kalshi_prints.jsonl   (game_lines_log.py)
  Sean's GAME UPLOADER (published CSV, any upcoming date; falls back to data/sheet_projections.json)
  data/f5_projections.json  (F5 UPLOADER)
Writes:
  data/game_lines/board_latest.json        what the Sharp Sides & Totals tab renders (one card per game x market)
  data/game_lines/board/YYYY-MM-DD.jsonl   append-only positions at each run (bet / lean / lag-rule), for grading
  data/game_lines/board_close/YYYY-MM-DD.json   closes frozen in the last FREEZE_MIN min before first pitch

Edge math mirrors the app (index.html): ML = wp - implied; totals = empirical CDF lookup (pgPOver / pgPOverF5);
team totals = NB(mu+0.20, r=8) then outcome-centred logit x0.68. Thresholds = OCTOBER_RULES (total +2.5, F5 and TT +3.5)
and the app's ML highlight (+3). Flow signals (public %, Kalshi prints) are shown as ungraded context, never as calls.
Read-only. Places nothing.
"""
import json, os, re, csv, io, math, statistics as st, datetime as dt, urllib.request, collections

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, '..'))
GL = os.environ.get('LINES_DIR') or os.path.join(ROOT, 'data', 'game_lines')
ET = dt.timezone(dt.timedelta(hours=-4))
NOW = dt.datetime.now(dt.timezone.utc); NOW_S = NOW.isoformat()
FREEZE_MIN = float(os.environ.get('FREEZE_MIN', 30))
SHEET_CSV = ("https://docs.google.com/spreadsheets/d/e/2PACX-1vR8rC-5ro6T19a3W6mQDpwDrr5nK6supT0TVYATBk305OgcrlQqeCOlz8mPydvfEZ_XqYR96g7s816P"
             "/pub?gid=580753288&single=true&output=csv")
BOOKS = ['DK', 'FD', 'CZR', 'BetRivers', 'Fanatics', 'theScore']          # NY books (BetMGM excluded by rule)
THRESH = {'ml': 3.0, 'total': 2.5, 'tt': 5.0, 'f5_ml': 3.5, 'f5_total': 3.5}   # Fable 10/7: TT floor 5 while the sheet level is off
# Fable 10/7 (REPLY_sharp_sides_totals): October scoring factor goes ON TOP of the sheet's runs (logged postseason assumption)
SEASON_FACTOR = {'F': 1.04, 'D': 1.04, 'L': 1.04, 'W': 0.95}
NO_SHEET_UNDERS = True   # Fable 10/7: no under leans off the sheet's total, F5 total or TT until the sheet is recentred (level bias, not edge)
LAG = {'ml': 0.02, 'total': 0.3}
SLOPE = {'total': 0.113, 'f5_total': 0.19, 'tt': 0.19}   # prob per run for line shifts (full total 0.113 = house constant)                                           # Fable cross-venue live test
MKT_NAME = {'ml': 'Moneyline', 'total': 'Total', 'tt': 'Team total', 'f5_ml': 'F5 moneyline', 'f5_total': 'F5 total'}
KFEE = lambda p: 0.07 * p * (1 - p)
imp = lambda o: 100 / (o + 100) if o > 0 else -o / (-o + 100)
def to_am(p):
    if p is None or p <= 0 or p >= 1: return None
    return -round(100 * p / (1 - p)) if p >= .5 else round(100 * (1 - p) / p)
def max_kalshi(t):
    if t is None: return None
    c = None
    for x in range(1, 100):
        if x / 100 + KFEE(x / 100) <= t: c = x
    return c
def novig(a, b):
    """power devig: probability of side a"""
    ia, ib = imp(a), imp(b); lo, hi = 0.5, 3.0
    for _ in range(60):
        k = (lo + hi) / 2
        if ia ** k + ib ** k > 1: lo = k
        else: hi = k
    return ia ** k
r4 = lambda x: None if x is None else round(x, 4)

# ---------- app edge math (index.html) ----------
TOT_LOW = [0.859,0.815,0.784,0.709,0.667,0.617,0.565,0.500,0.447,0.403,0.349,0.307,0.268,0.245,0.205]
TOT_MID = [0.833,0.785,0.741,0.702,0.644,0.591,0.542,0.500,0.451,0.401,0.364,0.325,0.290,0.255,0.231]
TOT_HIGH = [0.816,0.777,0.739,0.681,0.644,0.596,0.549,0.500,0.460,0.425,0.375,0.337,0.303,0.275,0.235]
F5L = [0.829,0.778,0.744,0.667,0.591,0.500,0.404,0.321,0.266,0.218,0.163]
F5M = [0.847,0.753,0.720,0.626,0.576,0.500,0.430,0.373,0.307,0.275,0.218]
F5H = [0.830,0.737,0.690,0.614,0.554,0.500,0.445,0.403,0.338,0.295,0.232]
def p_over_full(line, proj):
    col = TOT_LOW if line <= 7.5 else (TOT_MID if line < 9.5 else TOT_HIGH)
    kk = max(-3.5, min(3.5, line - proj)); pos = (kk + 3.5) / 0.5
    i = min(14, math.floor(pos)); j = min(14, i + 1)
    return max(.02, min(.98, col[i] + (pos - i) * (col[j] - col[i])))
def p_over_f5(line, proj):
    col = F5L if line <= 4 else (F5M if line < 5 else F5H)
    kk = max(-2.5, min(2.5, line - proj)); pos = (kk + 2.5) / 0.5
    i = min(10, math.floor(pos)); j = min(10, i + 1)
    return max(.02, min(.98, col[i] + (pos - i) * (col[j] - col[i])))
def p_over_tt(line, mu, r=8.0):
    mu = mu + 0.20; p = r / (r + mu); cdf = p ** r; lg = 0
    for x in range(1, math.floor(line) + 1):
        lg += math.log((x - 1 + r) / x); cdf += math.exp(lg + r * math.log(p) + x * math.log(1 - p))
    pv = min(max(1 - cdf, 1e-4), 1 - 1e-4)
    return 1 / (1 + math.exp(-0.68 * math.log(pv / (1 - pv))))   # TT=empctr_v1

def jl(p):
    if not os.path.exists(p): return []
    out = []
    for l in open(p):
        try: out.append(json.loads(l))
        except Exception: pass
    return out

def g(u):
    try: return urllib.request.urlopen(urllib.request.Request(u, headers={'User-Agent': 'Mozilla/5.0'}), timeout=25).read().decode()
    except Exception: return ''

_GT = {}
def game_type(date, away, home):
    """StatsAPI gameType (R / F / D / L / W) for the game, cached per date."""
    if date not in _GT:
        _GT[date] = {}
        try:
            x = json.loads(g(f"https://statsapi.mlb.com/api/v1/schedule?sportId=1&date={date}&hydrate=team") or '{}')
            for d in x.get('dates', []):
                for gm in d['games']:
                    ab = lambda t: {'CHW': 'CWS', 'AZ': 'ARI', 'WAS': 'WSH', 'OAK': 'ATH'}.get(t['team'].get('abbreviation'), t['team'].get('abbreviation'))
                    _GT[date][(ab(gm['teams']['away']), ab(gm['teams']['home']))] = gm.get('gameType')
        except Exception: pass
    return _GT[date].get((away, home), 'R')

# ---------- Sean's projections ----------
def projections():
    full, f5 = {}, {}
    txt = g(SHEET_CSV)
    for r in csv.reader(io.StringIO(txt)):
        if len(r) < 12 or not r[2].strip().isdigit(): continue
        try: full[int(r[2])] = {'away_runs': float(r[3]), 'home_runs': float(r[4]), 'away_wp': float(r[5]), 'home_wp': float(r[6]), 'total': float(r[11]), 'src': 'GAME UPLOADER (live)'}
        except ValueError: pass
    try:
        sp = json.load(open(os.path.join(ROOT, 'data', 'sheet_projections.json')))
        for v in sp.get('games', {}).values():
            if v.get('an_event_id') and int(v['an_event_id']) not in full and v.get('total') is not None:
                full[int(v['an_event_id'])] = {**{k: v.get(k) for k in ('away_runs', 'home_runs', 'away_wp', 'home_wp', 'total')}, 'src': 'sheet_projections.json'}
    except Exception: pass
    try:
        fp = json.load(open(os.path.join(ROOT, 'data', 'f5_projections.json')))
        for v in fp.get('games', {}).values():
            if v.get('an_event_id') and v.get('total') is not None: f5[int(v['an_event_id'])] = v
    except Exception: pass
    return full, f5

# ---------- candidate prices for a side ----------
OTHER = {'away': 'home', 'home': 'away', 'over': 'under', 'under': 'over'}
def book_cands(books, mkt, side, line_key='line'):
    """[(venue, line, odds)] across NY books for one side of one market. Coherence gate (app, Fable 2026-09-07): the book's two
    sides must be at the same line and their implied probabilities must sum to 1.00-1.12, else the quote is a mis-scrape."""
    out = []
    for b in BOOKS:
        x = (books.get(b) or {}).get(mkt) or {}
        o, q = x.get(side), x.get(OTHER[side])
        if not (o and q) or o.get('odds') is None or q.get('odds') is None: continue
        if o.get(line_key) is not None and q.get(line_key) is not None and abs(abs(o[line_key]) - abs(q[line_key])) > 1e-9: continue
        if not (1.00 <= imp(o['odds']) + imp(q['odds']) <= 1.12): continue
        out.append((b, o.get(line_key), o['odds']))
    return out
def near(cands, main, width=1.0):
    """keep lines within +-width runs of the main line (the model's lookup is only trusted near the market line)"""
    if main is None:
        ls = [c['line'] for c in cands if c['venue'] in BOOKS and c['line'] is not None]
        main = st.median(ls) if ls else None
    if main is None: return cands
    return [c for c in cands if c['line'] is None or abs(c['line'] - main) <= width + 1e-9]

def consensus_novig(books, mkt, a, b, book='Consensus'):
    x = (books.get(book) or {}).get(mkt) or {}
    if x.get(a) and x.get(b) and x[a].get('odds') is not None and x[b].get('odds') is not None:
        return novig(x[a]['odds'], x[b]['odds']), x[a].get('line'), x
    return None, None, x

def pin_side(pin, mkt, side):
    """Pinnacle no-vig prob for side, its line, price, limit"""
    x = pin.get(mkt)
    if not x: return None
    if mkt.endswith('ml'):
        other = 'home' if side == 'away' else 'away'
        return {'p': novig(x[side], x[other]), 'line': None, 'odds': x[side], 'limit': x.get('limit')}
    other = 'under' if side == 'over' else 'over'
    return {'p': novig(x[side], x[other]), 'line': x.get('line'), 'odds': x[side], 'limit': x.get('limit')}

def first_pin(PL, gkey, mkt, side):
    rows = [r for r in PL if r.get('game') == gkey and r.get('mkt') == mkt]
    if not rows: return None
    r = min(rows, key=lambda r: r['ts']); other = {'away': 'home', 'home': 'away', 'over': 'under', 'under': 'over'}[side]
    if side not in r or other not in r: return None
    return {'ts': r['ts'], 'p': novig(r[side], r[other]), 'line': r.get('line'), 'odds': r[side]}

def build():
    L = json.load(open(os.path.join(GL, 'latest.json')))
    PL = jl(os.path.join(GL, 'pinnacle_log.jsonl'))
    PR = jl(os.path.join(GL, 'kalshi_prints.jsonl'))
    full, f5 = projections()
    cards = []
    for gkey, an in L['action'].items():
        a, h = an['away'], an['home']; start = an['start'].replace('.000Z', '+00:00').replace('Z', '+00:00')
        sdt = dt.datetime.fromisoformat(start); started = sdt <= NOW or an.get('status') not in (None, 'scheduled')
        if started: continue   # in-game prices are not pregame prices: no cards once a game starts
        pin = L['pinnacle'].get(gkey, {}) or next((v for k, v in L['pinnacle'].items() if v['away'] == a and v['home'] == h and abs((dt.datetime.fromisoformat(v['start'].replace('Z', '+00:00')) - sdt).total_seconds()) < 4 * 3600), {})
        kcode = sdt.astimezone(ET).strftime('%y') + sdt.astimezone(ET).strftime('%b').upper() + sdt.astimezone(ET).strftime('%d%H%M') + a + h
        krs = L['kalshi'].get(kcode) or next((v for k, v in L['kalshi'].items() if k.endswith(a + h) and k[:7] == kcode[:7]), [])
        model, model5 = full.get(int(an['an_id'])), f5.get(int(an['an_id']))
        gt = game_type(sdt.astimezone(ET).date().isoformat(), a, h); fac = SEASON_FACTOR.get(gt, 1.0)
        if model and fac != 1.0:   # 1.04 postseason / 0.95 World Series on the sheet's runs; WP unchanged
            model = {**model, **{k: model[k] * fac for k in ('away_runs', 'home_runs', 'total') if model.get(k) is not None}}
        books = an['books']
        base = {'game': f"{a} @ {h}", 'away': a, 'home': h, 'first_pitch': sdt.isoformat(), 'date': sdt.astimezone(ET).date().isoformat(),
                'started': started, 'an_id': an['an_id'], 'model_src': (model or {}).get('src'), 'kalshi_event': kcode if krs else None,
                'game_type': gt, 'scoring_factor': fac}
        prints_g = [p for p in PR if kcode in p['ticker']]
        pkey = next((k for k, v in L['pinnacle'].items() if v is pin), None)

        def kalshi_cands(series, side_fn, prob_fn):
            """Kalshi rungs as (venue, line, cost_incl_fee, ticker, kside, mid) for a side; prob_fn(line) gives model prob of the YES outcome"""
            out = []
            for r in krs:
                if r['series'] != series or not (r['bid'] > 0 and 0 < r['ask'] < 1): continue
                if series != 'KXMLBGAME' and series != 'KXMLBF5' and not (.15 <= (r['bid'] + r['ask']) / 2 <= .85): continue
                for kside in ('YES', 'NO'):
                    s = side_fn(r, kside)
                    if s is None: continue
                    px = r['ask'] if kside == 'YES' else 1 - r['bid']
                    out.append({'venue': 'Kalshi', 'line': r.get('strike'), 'side': s, 'cost': px + KFEE(px), 'px': px, 'ticker': r['ticker'], 'kside': kside,
                                'mid': (r['bid'] + r['ask']) / 2 if kside == 'YES' else 1 - (r['bid'] + r['ask']) / 2})
            return out

        def card(mkt, side_label, side, model_p_fn, cands, pin_info, cons_p, cons_line, open_p, open_line, public, extra=None):
            """pick the best venue for this side (max model - cost), and attach context"""
            best = None
            for c in cands:
                p = model_p_fn(c['line'])
                if p is None: continue
                e = (p - c['cost']) * 100
                if best is None or e > best['edge']: best = {**c, 'edge': round(e, 2), 'model': p}
            if best is None: return None
            mk = pin_info['p'] if (pin_info and (pin_info.get('line') in (None, best['line']))) else (cons_p if cons_line in (None, best['line']) else None)
            mk_src = 'Pinnacle no-vig' if (pin_info and pin_info.get('line') in (None, best['line'])) else ('consensus no-vig' if mk is not None else None)
            if mk is None and best['line'] is not None and cons_p is not None and cons_line is not None:
                slope = SLOPE['tt' if mkt.startswith('tt') else mkt]   # prob per run, approximate
                sgn = 1 if side == 'over' else -1
                mk = min(.98, max(.02, cons_p + sgn * slope * (cons_line - best['line']))); mk_src = f'consensus no-vig shifted {cons_line} -> {best["line"]} (approx)'
            th = THRESH['tt'] if mkt.startswith('tt') else THRESH[mkt]
            c = {**base, 'key': f"{base['date']}|{a}@{h}|{mkt}|{side}", 'mkt': mkt, 'market': MKT_NAME.get('tt' if mkt.startswith('tt') else mkt, mkt), 'side': side, 'label': side_label,
                 'venue': best['venue'], 'line': best['line'], 'odds': best.get('odds'), 'cost': r4(best['cost']), 'cost_am': to_am(best['cost']), 'model': r4(best['model']),
                 'edge': best['edge'], 'threshold': th, 'market_fair': r4(mk), 'market_fair_src': mk_src,
                 'edge_mkt': round((mk - best['cost']) * 100, 2) if mk is not None else None,
                 'target_am': to_am(best['model'] - .02), 'ticker': best.get('ticker'), 'kalshi_side': best.get('kside'),
                 'target_kalshi_cents': max_kalshi(best['model'] - .02) if best['venue'] == 'Kalshi' else None,
                 'pinnacle': {k: (r4(v) if isinstance(v, float) else v) for k, v in (pin_info or {}).items()} or None,
                 'all_prices': sorted([{'venue': x['venue'], 'line': x['line'], 'odds': x.get('odds'), 'cost': r4(x['cost']), 'edge': round((model_p_fn(x['line']) - x['cost']) * 100, 2) if model_p_fn(x['line']) is not None else None}
                                       for x in cands], key=lambda x: -(x['edge'] if x['edge'] is not None else -99))[:8],
                 'public': public, 'chips': [], 'signals': {}}
            # movement: Pinnacle first-of-day -> now, else AN open -> consensus (same line only; otherwise report the line move)
            if pin_info and pin_info.get('first') and pin_info['first'].get('line') == pin_info.get('line'):
                mv = (pin_info['p'] - pin_info['first']['p']) * 100; c['signals']['move'] = {'src': 'Pinnacle since ' + pin_info['first']['ts'][11:16] + ' UTC', 'pts': round(mv, 1)}
            elif open_p is not None and cons_p is not None and open_line == cons_line:
                mv = (cons_p - open_p) * 100; c['signals']['move'] = {'src': 'consensus vs open', 'pts': round(mv, 1)}
            elif open_line is not None and cons_line is not None and open_line != cons_line:
                c['signals']['move'] = {'src': 'consensus vs open', 'line_from': open_line, 'line_to': cons_line}
            mvp = (c['signals'].get('move') or {}).get('pts')
            if mvp is not None and abs(mvp) >= 1.0: c['chips'].append(f"market moved {mvp:+.1f} pts on this side since open (display only, Fable 10/7)")
            if 'line_from' in (c['signals'].get('move') or {}): c['chips'].append(f"line moved {c['signals']['move']['line_from']} → {c['signals']['move']['line_to']}")
            if mk is not None:
                gap = (best['model'] - mk) * 100; c['signals']['model_vs_market'] = round(gap, 1)
            # public money / tickets: logged only (Fable 10/7: revisit at 500 games); shown as plain text on the card, never a chip
            if extra: c.update(extra)
            lvl = 'bet' if c['edge'] >= th else ('lean' if c['edge'] >= th - 1.5 else ('watch' if c['edge'] > 0 else 'info'))
            # timing (Fable 10/7): ML / F5 ML keep ~1/3 of the edge in play at mid-day and ~1/5 in the last 3 h; totals / TT / F5 totals are done by 6 h out
            mins = (sdt - NOW).total_seconds() / 60
            if mkt in ('ml', 'f5_ml'):
                c['timing'] = 'early: best CLV' if mins > 720 else ('mid-day: about a third of the edge still to come' if mins > 180 else 'late: about a fifth of the edge left to come')
            else:
                c['timing'] = 'open window: totals keep moving toward the sheet until about 6 h out' if mins > 360 else 'under 6 h out: effectively the close for totals'
            if mkt in ('f5_ml', 'f5_total') or mkt.startswith('tt'):
                c['chips'].append('thinner market than ML: CLV-graded, small stake')
            if NO_SHEET_UNDERS and side == 'under' and (mkt in ('total', 'f5_total') or mkt.startswith('tt')) and lvl != 'info':
                c['blocked'] = 'No sheet unders in October until the sheet is recentred (Fable 10/7): the sheet ran about 1 run a game under all summer, so under edges are level bias'
                c['chips'].append('blocked: sheet-under level bias (Fable 10/7)'); lvl = 'info'
            c['level'] = lvl
            return c

        # ---------- full-game ML ----------
        if model and model.get('away_wp') is not None:
            cons_a, _, cx = consensus_novig(books, 'ml', 'away', 'home'); op_a, _, ox = consensus_novig(books, 'ml', 'away', 'home', 'Open')
            for side, team in (('away', a), ('home', h)):
                wp = model[f'{side}_wp']
                cands = [{'venue': b, 'line': None, 'odds': o, 'cost': imp(o)} for b, _, o in book_cands(books, 'ml', side)]
                cands += kalshi_cands('KXMLBGAME', lambda r, ks, t=team: (t if r['ticker'].endswith('-' + t) else None) if ks == 'YES' else None, None)
                pi = pin_side(pin, 'ml', side)
                if pi: pi['first'] = first_pin(PL, pkey, 'ml', side)
                cp = cons_a if side == 'away' else (1 - cons_a if cons_a is not None else None)
                opn = op_a if side == 'away' else (1 - op_a if op_a is not None else None)
                pub = {k: (cx.get(side) or {}).get(k) for k in ('tickets_pct', 'money_pct')} if cx.get(side) else None
                cd = card('ml', f"{team} ML", side, lambda _l, wp=wp: wp, cands, pi, cp, None, opn, None, pub, {'model_proj': f"{team} {wp:.3f}"})
                if cd: cards.append(cd)
        # ---------- full-game total ----------
        if model and model.get('total') is not None:
            T = model['total']
            cons_o, cons_line, cx = consensus_novig(books, 'total', 'over', 'under'); op_o, op_line, _ = consensus_novig(books, 'total', 'over', 'under', 'Open')
            for side in ('over', 'under'):
                pfn = (lambda l, T=T: p_over_full(l, T)) if side == 'over' else (lambda l, T=T: 1 - p_over_full(l, T))
                cands = [{'venue': b, 'line': l, 'odds': o, 'cost': imp(o)} for b, l, o in book_cands(books, 'total', side) if l is not None]
                cands += kalshi_cands('KXMLBTOTAL', lambda r, ks, s=side: s if (ks == 'YES') == (s == 'over') else None, None)
                pi = pin_side(pin, 'total', side)
                if pi: pi['first'] = first_pin(PL, pkey, 'total', side)
                cp = cons_o if side == 'over' else (1 - cons_o if cons_o is not None else None)
                opn = op_o if side == 'over' else (1 - op_o if op_o is not None else None)
                pub = {k: (cx.get(side) or {}).get(k) for k in ('tickets_pct', 'money_pct')} if cx.get(side) else None
                cands = near(cands, cons_line, 0.5)
                cd = card('total', f"{side.title()}", side, pfn, cands, pi, cp, cons_line, opn, op_line, pub, {'model_proj': f"total {T:.2f}"})
                if cd: cd['label'] = f"{side.title()} {cd['line']}"; cards.append(cd)
        # ---------- team totals ----------
        if model and model.get('away_runs') is not None:
            for tside, team in (('away', a), ('home', h)):
                mu = model[f'{tside}_runs']; mkt = f'tt_{tside}'
                cons_o, cons_line, cx = consensus_novig(books, mkt, 'over', 'under'); op_o, op_line, _ = consensus_novig(books, mkt, 'over', 'under', 'Open')
                for side in ('over', 'under'):
                    pfn = (lambda l, mu=mu: p_over_tt(l, mu)) if side == 'over' else (lambda l, mu=mu: 1 - p_over_tt(l, mu))
                    cands = [{'venue': b, 'line': l, 'odds': o, 'cost': imp(o)} for b, l, o in book_cands(books, mkt, side) if l is not None]
                    cands += kalshi_cands('KXMLBTEAMTOTAL', lambda r, ks, s=side, t=team: (s if (ks == 'YES') == (s == 'over') else None) if re.search(rf'-{t}\d+$', r['ticker']) else None, None)
                    pi = pin_side(pin, mkt, side)
                    if pi: pi['first'] = first_pin(PL, pkey, mkt, side)
                    cp = cons_o if side == 'over' else (1 - cons_o if cons_o is not None else None)
                    opn = op_o if side == 'over' else (1 - op_o if op_o is not None else None)
                    cands = near(cands, cons_line, 0.0)   # TT model (outcome-centred x0.68) is only calibrated at the posted line
                    cd = card(mkt, f"{team} {side}", side, pfn, cands, pi, cp, cons_line, opn, op_line, None, {'model_proj': f"{team} {mu:.2f} runs", 'team': team})
                    if cd: cd['market'] = 'Team total'; cd['label'] = f"{team} {side} {cd['line']}"; cards.append(cd)
        # ---------- F5 ML (2-way, push on tie; Kalshi F5 is 3-way: team YES loses on a tie) ----------
        if model5 and model5.get('away_wp') is not None:
            tie = next(((r['bid'] + r['ask']) / 2 for r in krs if r['series'] == 'KXMLBF5' and r['ticker'].endswith('-TIE') and r['bid'] > 0), None)
            cons_a, _, cx = consensus_novig(books, 'f5_ml', 'away', 'home'); op_a, _, _ = consensus_novig(books, 'f5_ml', 'away', 'home', 'Open')
            for side, team in (('away', a), ('home', h)):
                wp = model5[f'{side}_wp']
                cands = [{'venue': b, 'line': None, 'odds': o, 'cost': imp(o)} for b, _, o in book_cands(books, 'f5_ml', side)]
                kc = kalshi_cands('KXMLBF5', lambda r, ks, t=team: (t if r['ticker'].endswith('-' + t) else None) if ks == 'YES' else None, None)
                if tie is not None:   # compare a 3-way Kalshi price on the same footing: model P(win outright) = wp x (1 - tie)
                    for x in kc: x['cost'] = x['cost'] / (1 - tie); x['venue'] = 'Kalshi (3-way, tie-adjusted)'
                    cands += kc
                pi = pin_side(pin, 'f5_ml', side)
                cp = cons_a if side == 'away' else (1 - cons_a if cons_a is not None else None)
                opn = op_a if side == 'away' else (1 - op_a if op_a is not None else None)
                cd = card('f5_ml', f"{team} F5 ML", side, lambda _l, wp=wp: wp, cands, pi, cp, None, opn, None, None, {'model_proj': f"{team} F5 {wp:.3f}", 'kalshi_tie_mid': r4(tie)})
                if cd: cards.append(cd)
        # ---------- F5 total ----------
        if model5 and model5.get('total') is not None:
            T5 = model5['total']
            cons_o, cons_line, cx = consensus_novig(books, 'f5_total', 'over', 'under'); op_o, op_line, _ = consensus_novig(books, 'f5_total', 'over', 'under', 'Open')
            for side in ('over', 'under'):
                pfn = (lambda l, T=T5: p_over_f5(l, T)) if side == 'over' else (lambda l, T=T5: 1 - p_over_f5(l, T))
                cands = [{'venue': b, 'line': l, 'odds': o, 'cost': imp(o)} for b, l, o in book_cands(books, 'f5_total', side) if l is not None]
                cands += kalshi_cands('KXMLBF5TOTAL', lambda r, ks, s=side: s if (ks == 'YES') == (s == 'over') else None, None)
                pi = pin_side(pin, 'f5_total', side)
                cp = cons_o if side == 'over' else (1 - cons_o if cons_o is not None else None)
                opn = op_o if side == 'over' else (1 - op_o if op_o is not None else None)
                cands = near(cands, cons_line, 0.5)
                cd = card('f5_total', f"F5 {side}", side, pfn, cands, pi, cp, cons_line, opn, op_line, None, {'model_proj': f"F5 total {T5:.2f}"})
                if cd: cd['label'] = f"F5 {side} {cd['line']}"; cards.append(cd)

        # ---------- cross-venue lag (Fable live test): books' no-vig median vs Kalshi, at the same moment ----------
        lag = []
        for side, team in (('away', a), ('home', h)):
            ps = []
            for b in BOOKS:
                x = (books.get(b) or {}).get('ml') or {}
                if x.get('away') and x.get('home'):
                    pa = novig(x['away']['odds'], x['home']['odds']); ps.append(pa if side == 'away' else 1 - pa)
            kr = next((r for r in krs if r['series'] == 'KXMLBGAME' and r['ticker'].endswith('-' + team) and r['bid'] > 0 and r['ask'] < 1), None)
            if len(ps) >= 3 and kr:
                med = st.median(ps); kmid = (kr['bid'] + kr['ask']) / 2; cost = kr['ask'] + KFEE(kr['ask'])
                if med - kmid >= LAG['ml'] and (med - cost) * 100 >= 2:
                    lag.append({**base, 'key': f"{base['date']}|{a}@{h}|lag_ml|{side}", 'mkt': 'lag_ml', 'market': 'Cross-venue lag (Fable live test)', 'side': side,
                                'label': f"{team} ML on Kalshi", 'venue': 'Kalshi', 'ticker': kr['ticker'], 'kalshi_side': 'YES', 'cost': r4(cost), 'cost_am': to_am(cost),
                                'model': r4(med), 'market_fair': r4(med), 'market_fair_src': f'median of {len(ps)} books no-vig', 'edge': round((med - cost) * 100, 2),
                                'edge_mkt': round((med - cost) * 100, 2), 'threshold': 2.0, 'target_kalshi_cents': max_kalshi(med - .02), 'level': 'lead',
                                'chips': [f"books {med:.3f} vs Kalshi mid {kmid:.3f} (gap {100 * (med - kmid):+.1f} pts)"], 'signals': {}, 'all_prices': []})
        # totals: books' implied total (line + (p_over - .5)/0.113) vs Kalshi fair total (strike where P(over) = .5)
        bt = []
        for b in BOOKS:
            x = (books.get(b) or {}).get('total') or {}
            if x.get('over') and x.get('under') and x['over'].get('line') is not None:
                bt.append(x['over']['line'] + (novig(x['over']['odds'], x['under']['odds']) - .5) / 0.113)
        kt = sorted([r for r in krs if r['series'] == 'KXMLBTOTAL' and r['bid'] > 0 and r['ask'] < 1], key=lambda r: r['strike'])
        kfair = None
        for r1, r2 in zip(kt, kt[1:]):
            m1, m2 = (r1['bid'] + r1['ask']) / 2, (r2['bid'] + r2['ask']) / 2
            if m1 >= .5 >= m2: kfair = r1['strike'] + (m1 - .5) / (m1 - m2 + 1e-9) * (r2['strike'] - r1['strike'])
        if len(bt) >= 3 and kfair is not None:
            bmed = st.median(bt); gap = bmed - kfair
            if abs(gap) >= LAG['total']:
                side = 'over' if gap > 0 else 'under'
                for r in kt:   # follow the books on every Kalshi rung: shift the Kalshi ladder by the gap
                    mid = (r['bid'] + r['ask']) / 2; fair_yes = min(.99, max(.01, mid + (0.113 * gap)))
                    px = r['ask'] if side == 'over' else 1 - r['bid']; fair = fair_yes if side == 'over' else 1 - fair_yes
                    e = (fair - px - KFEE(px)) * 100
                    if e >= 2:
                        lag.append({**base, 'key': f"{base['date']}|{a}@{h}|lag_total|{side}|{r['strike']}", 'mkt': 'lag_total', 'market': 'Cross-venue lag (Fable live test)',
                                    'side': side, 'label': f"{side.title()} {r['strike']} on Kalshi", 'venue': 'Kalshi', 'line': r['strike'], 'ticker': r['ticker'],
                                    'kalshi_side': 'YES' if side == 'over' else 'NO', 'cost': r4(px + KFEE(px)), 'cost_am': to_am(px + KFEE(px)), 'model': r4(fair),
                                    'market_fair': r4(fair), 'market_fair_src': 'Kalshi ladder shifted to the books', 'edge': round(e, 2), 'edge_mkt': round(e, 2), 'threshold': 2.0,
                                    'target_kalshi_cents': max_kalshi(fair - .02), 'level': 'lead', 'signals': {}, 'all_prices': [],
                                    'chips': [f"books' implied total {bmed:.2f} vs Kalshi fair {kfair:.2f} ({gap:+.2f} runs): all rungs in this game = ONE bet"]})
        cards += lag
        # ---------- Kalshi flow (ungraded context) per game ----------
        if prints_g:
            recent = [p for p in prints_g if p['created'] >= (NOW - dt.timedelta(hours=3)).isoformat()]
            for c in cards:
                if c.get('away') != a or c.get('home') != h or c.get('first_pitch') != sdt.isoformat(): continue
                tk = c.get('ticker')
                mine = [p for p in recent if tk and p['ticker'] == tk]
                if mine:
                    same = sum(p['usd'] for p in mine if (p['taker_side'] == 'yes') == (c.get('kalshi_side') == 'YES'))
                    opp = sum(p['usd'] for p in mine) - same
                    c['signals']['kalshi_prints_3h'] = {'same_side_usd': same, 'other_side_usd': opp, 'n': len(mine)}
                    c['chips'].append(f"Kalshi $1k+ prints (3 h): ${same:,} your side / ${opp:,} other (not a signal, Fable 10/7)")
    return cards

def main():
    cards = build()
    out = {'meta': {'built': NOW_S, 'lines_ts': json.load(open(os.path.join(GL, 'latest.json')))['ts'], 'thresholds': THRESH, 'lag': LAG,
                    'books': BOOKS, 'season_factor': SEASON_FACTOR, 'no_sheet_unders': NO_SHEET_UNDERS,
                    'note': 'Model = Sean GAME/F5 UPLOADER, runs x1.04 postseason (0.95 WS). Fable 10/7: bet = edge at the current price; moves, Kalshi prints, imbalance and public % are display only.'},
           'cards': cards}
    json.dump(out, open(os.path.join(GL, 'board_latest.json'), 'w'), default=str)
    # archive positions worth grading (bet / lean / lag), pregame only
    os.makedirs(os.path.join(GL, 'board'), exist_ok=True); os.makedirs(os.path.join(GL, 'board_close'), exist_ok=True)
    keep = [c for c in cards if not c['started'] and c['level'] in ('bet', 'lean', 'lead')]
    byd = collections.defaultdict(list)
    for c in keep: byd[c['date']].append({k: v for k, v in c.items() if k not in ('all_prices',)})
    for d, rows in byd.items():
        with open(os.path.join(GL, 'board', f'{d}.jsonl'), 'a') as f:
            for r in rows: f.write(json.dumps({'ts': NOW_S, **r}, default=str) + '\n')
    # freeze closes (every card, so CLV can be graded for anything that was ever shown) in the last FREEZE_MIN minutes
    frozen = 0
    for d in {c['date'] for c in cards}:
        p = os.path.join(GL, 'board_close', f'{d}.json'); cur = json.load(open(p)) if os.path.exists(p) else {}; ch = False
        for c in cards:
            if c['date'] != d or c['key'] in cur or c['started']: continue
            mins = (dt.datetime.fromisoformat(c['first_pitch']) - NOW).total_seconds() / 60
            if 0 <= mins <= FREEZE_MIN:
                cur[c['key']] = {'frozen_at': NOW_S, 'mins': round(mins, 1), 'pinnacle': c.get('pinnacle'), 'market_fair': c.get('market_fair'),
                                 'market_fair_src': c.get('market_fair_src'), 'best': {k: c.get(k) for k in ('venue', 'line', 'odds', 'cost', 'ticker')}}
                ch = True; frozen += 1
        if ch: json.dump(cur, open(p, 'w'), indent=0, default=str)
    lv = collections.Counter(c['level'] for c in cards)
    print(f"[sides] {len(cards)} cards {dict(lv)}; {len(keep)} positions archived; {frozen} closes frozen")

if __name__ == '__main__':
    main()
