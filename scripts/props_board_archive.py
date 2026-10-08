#!/usr/bin/env python3
"""Sharp Props board builder + archive (brief v2, 2026-10-06 evening).

ONE place for the board logic. Runs after each prop scan and every 20 min with the order-book logger. Writes:

  data/prop_scan/board_latest.json
      What the Sharp Props tab renders: one CARD per position (pitcher x direction, early-scoring per game,
      hitter unders per game, walks, outs trades, Kalshi-vs-Pinnacle, info), plus tickets and header meta.
      The dashboard only re-prices cards with live Kalshi quotes; every rule/size/level decision is made here.
  data/prop_scan/board/YYYY-MM-DD.jsonl
      Append-only: every gradeable POSITION on the board at this run (level, rules, size, model / market fair,
      Kalshi price and cost, best book, edges, target price, status). board_grade.py grades the first appearance.
  data/prop_scan/board_close/YYYY-MM-DD.json
      Frozen once per position inside the last FREEZE_MIN minutes before first pitch: Kalshi bid/ask/mid,
      Pinnacle close (power no-vig) and the board row.

Rules follow BARTOLO_BRIEF_props_dashboard_v2_2026-10-06 and OCTOBER_RULES.md. Read-only; places nothing.
"""
import json, os, re, csv, time, datetime as dt, urllib.request, unicodedata, collections

HERE = os.path.dirname(os.path.abspath(__file__))
SCAN = os.environ.get("SCAN_DIR") or os.path.join(HERE, "..", "data", "prop_scan")
ROOT = os.path.abspath(os.path.join(SCAN, "..", ".."))
BOARD = os.path.join(SCAN, "board"); CLOSE = os.path.join(SCAN, "board_close")
API = "https://api.elections.kalshi.com/trade-api/v2"
ET = dt.timezone(dt.timedelta(hours=-4))
MON = {m: i + 1 for i, m in enumerate(['JAN','FEB','MAR','APR','MAY','JUN','JUL','AUG','SEP','OCT','NOV','DEC'])}
TEAMS = ['ARI','AZ','ATL','BAL','BOS','CHC','CWS','CHW','CIN','CLE','COL','DET','HOU','KC','LAA','LAD','MIA','MIL','MIN','NYM','NYY','ATH','OAK','PHI','PIT','SD','SF','SEA','STL','TB','TEX','TOR','WSH','WAS']
FREEZE_MIN = float(os.environ.get("FREEZE_MIN", 30))

# model fair = market fair + this many pts (brief v2 3.1). F5 / YRFI / sub-risk use the scan's own model prob.
RULE_EDGE = {'K-LENGTH UNDER': 5, 'K-SEASON-ANCHOR UNDER': 5, 'VELO-DECLINE K UNDER (paper)': 4,
             'K-LENGTH OVER': 4, 'DEEP-SPOT OVERS': 4, 'WALKS OVER (lead)': 4, 'BIG OUTS TRADE': 1.5,
             'KALSHI OFF PINNACLE (ungraded)': 0}
OCT_HIT_EDGE = 3   # October hitter unders: NO fair = Kalshi NO mid + 3 pts
RULE_TIER = {'K-LENGTH UNDER': 1, 'K-SEASON-ANCHOR UNDER': 1, 'F5 OVER': 1, 'YRFI CURVE': 1, 'YRFI CURVE (Oct-adj only)': 3,
             'RFI INFO': 4, 'K-LENGTH OVER': 2, 'DEEP-SPOT OVERS': 2, 'VELO-DECLINE K UNDER (paper)': 2, 'BIG OUTS TRADE': 2,
             'WALKS OVER (lead)': 3, 'WALKS OVER (info: low-BB% pitcher)': 4, 'OCTOBER HITTER UNDERS (lean)': 3,
             'PS SUB-RISK HIT UNDER (lean)': 3, 'PS SUB-RISK (info)': 4, 'KALSHI OFF PINNACLE (ungraded)': 3,
             'BIG TRADES (no backtested edge)': 4, 'PRICE MOVE': 4, 'DEPTH IMBALANCE (ungraded)': 4, 'VELO-DECLINE (price too short)': 4}
K_ORDER = ['K-LENGTH UNDER', 'K-SEASON-ANCHOR UNDER', 'VELO-DECLINE K UNDER (paper)', 'K-LENGTH OVER', 'DEEP-SPOT OVERS', 'VELO-DECLINE (price too short)']

fee = lambda p: 0.07 * p * (1 - p)
imp = lambda o: 100 / (o + 100) if o > 0 else -o / (-o + 100)
def to_am(p):
    if p is None or p <= 0 or p >= 1: return None
    return -round(100 * p / (1 - p)) if p >= .5 else round(100 * (1 - p) / p)
def max_kalshi(t):
    """highest Kalshi price (cents) whose cost incl. fee is <= t"""
    if t is None: return None
    c = None
    for x in range(1, 100):
        if x / 100 + fee(x / 100) <= t: c = x
    return c
def r3(x): return None if x is None else round(x, 4)
nz = lambda s: unicodedata.normalize('NFKD', s or '').encode('ascii', 'ignore').decode().lower().replace('.', '').replace(' jr', '').strip()

def g(u):
    for i in range(4):
        try: return json.load(urllib.request.urlopen(urllib.request.Request(u, headers={'User-Agent': 'Mozilla/5.0'}), timeout=30))
        except Exception: time.sleep(1.5 * (i + 1))
    return {}

def split_teams(t):
    for a in TEAMS:
        if t.startswith(a) and t[len(a):] in TEAMS: return a, t[len(a):]
    return None, None

def parse_event(ev):
    c = str(ev or '').split('-', 1)[-1]
    m = re.match(r'^(\d\d)([A-Z]{3})(\d\d)(\d\d)(\d\d)([A-Z]+?)(G\d?)?$', c)
    if not m: return None
    yy, mon, dd, hh, mi, teams = m.groups()[:6]
    away, home = split_teams(teams)
    start = dt.datetime(2000 + int(yy), MON[mon], int(dd), int(hh), int(mi), tzinfo=ET)
    return {'code': c, 'away': away, 'home': home, 'label': f'{away} @ {home}' if away else teams,
            'date': start.date().isoformat(), 'start': start.isoformat()}

def norm_team(t): return {'CHW': 'CWS', 'AZ': 'ARI', 'WAS': 'WSH', 'OAK': 'ATH'}.get(t, t)

def event_of(row, kalshi_events):
    """Kalshi game code, or AWAY@HOME for sportsbook-only rows (mapped to the Kalshi code of the same game when listed)."""
    e = parse_event(row.get('event'))
    if e: return e
    m = re.match(r'^([A-Z]{2,3})@([A-Z]{2,3})$', str(row.get('event') or ''))
    d = dt.datetime.fromisoformat(row['ts']).astimezone(ET).date().isoformat()
    if m:
        a, h = norm_team(m.group(1)), norm_team(m.group(2))
        d1 = (dt.date.fromisoformat(d) + dt.timedelta(days=1)).isoformat()
        cand = [k for k in kalshi_events.values() if k['date'] in (d, d1) and norm_team(k['away'] or '') == a and norm_team(k['home'] or '') == h]
        if cand: return sorted(cand, key=lambda k: k['start'])[0]
        return {'code': f'{a}{h}-{d}', 'away': a, 'home': h, 'label': f'{a} @ {h}', 'date': d, 'start': None}
    return {'code': str(row.get('event')), 'away': None, 'home': None, 'label': str(row.get('event')), 'date': d, 'start': None}

def parse_note(n):
    n = n or ''; o = {}
    m = re.search(r'book[^:;]*:\s*([A-Za-z]+)\s*([+-]\d+)', n)
    if m: o['book'], o['book_odds'] = m.group(1), int(m.group(2))
    m = re.search(r'Pinnacle\s+([ou][\d.]+)\s*([+-]\d+)\s*\(fair\s*([\d.]+)%\)', n)
    if m: o['pin_line'], o['pin_odds'], o['pin_fair'] = m.group(1), int(m.group(2)), float(m.group(3)) / 100
    m = re.search(r'fair YRFI\s*([\d.]+)', n)
    if m: o['pin_yrfi'] = float(m.group(1))
    m = re.search(r'Kalshi total\s*([\d.]+)', n)
    if m: o['ktotal'] = float(m.group(1))
    m = re.search(r'YRFI curve\s*([\d.]+)\s*\(Oct-adj\s*([\d.]+)\)', n)
    if m: o['curve'], o['curve_oct'] = float(m.group(1)), float(m.group(2))
    m = re.search(r'taker \$\s*(\d+)% NRFI', n)
    if m: o['nrfi_pct'] = int(m.group(1))
    m = re.search(r'outs mkt\s*([\d.]+)\s*vs season\s*([\d.]+)\s*\(([+-]?[\d.]+)\)', n)
    if m: o['outs_mkt'], o['outs_season'], o['outs_gap'] = float(m.group(1)), float(m.group(2)), float(m.group(3))
    m = re.search(r'K mkt\s*([\d.]+)\s*vs season\s*([\d.]+)', n)
    if m: o['k_mkt'], o['k_season'] = float(m.group(1)), float(m.group(2))
    m = re.search(r'at\s*([\d.]+)\s*\([^)]*\);\s*now mid\s*([\d.]+)', n)
    if m: o['print_px'], o['now_mid'] = float(m.group(1)), float(m.group(2))
    m = re.search(r'(\d+)\s*starts', n)
    if m: o['starts'] = int(m.group(1))
    m = re.search(r'season BB%\s*([\d.]+)', n)
    if m: o['bb_pct'] = float(m.group(1))
    m = re.search(r'four-seam last 3\s*([\d.]+)\s*vs season\s*([\d.]+)\s*\(([+-][\d.]+)\s*mph', n)
    if m: o['velo_d3'] = float(m.group(3))
    return o

def pitcher_of(w):
    m = re.match(r'^(.*?)\s+(K|walks|outs|Outs)\b', str(w or ''))
    return m.group(1) if m else str(w or '').split(':')[0]

def cat_of(rule):
    if rule in ('F5 OVER',) or 'RFI' in rule: return 'EARLY'
    if rule.startswith('K-') or 'DEEP-SPOT' in rule or rule.startswith('VELO-DECLINE'): return 'K'
    if rule.startswith('WALKS'): return 'WALK'
    if 'HITTER UNDERS' in rule or 'SUB-RISK' in rule: return 'HIT'
    if rule == 'BIG OUTS TRADE': return 'OUTS'
    if rule.startswith('KALSHI OFF PINNACLE'): return 'KOP'
    return 'INFO'

def card_key(cat, ev, x):
    if cat == 'K': return f"{ev['code']}|K|{pitcher_of(x['what'])}|{'under' if x['side'] == 'NO' else 'over'}"
    if cat == 'EARLY': return f"{ev['code']}|EARLY"
    if cat == 'HIT': return f"{ev['code']}|HIT"
    if cat == 'WALK': return f"{ev['code']}|WALK|{pitcher_of(x['what'])}"
    return f"{ev['code']}|{x['rule']}|{x['what']}"

def load_jsonl(p):
    if not os.path.exists(p): return []
    out = []
    for l in open(p):
        try: out.append(json.loads(l))
        except Exception: pass
    return out

def power_novig(o_over, o_under):
    po, pu = imp(o_over), imp(o_under); lo, hi = 0.5, 3.0
    for _ in range(60):
        k = (lo + hi) / 2
        if po ** k + pu ** k > 1: lo = k
        else: hi = k
    return po ** k

def pricing(o, live):
    """Kalshi cost for the bet side (live if available), edges vs model and market, best venue, target prices."""
    m = live.get(o.get('ticker')) if o.get('ticker') else None
    if m and m.get('bid', 0) > 0 and 0 < m.get('ask', 0) < 1:
        o['kalshi_px'] = (1 - m['bid']) if o['side'] == 'NO' else m['ask']; o['price_src'] = 'live'
        o['kalshi_mid_side'] = (1 - (m['bid'] + m['ask']) / 2) if o['side'] == 'NO' else (m['bid'] + m['ask']) / 2
        if o.get('locked_from') and o.get('rule_edge') is not None:   # locked line: market = live Kalshi mid of the original rung
            o['market'] = o['kalshi_mid_side']; o['market_src'] = 'Kalshi mid (live)'; o['model'] = o['market'] + o['rule_edge'] / 100
    else:
        o['price_src'] = 'scan'
    px = o.get('kalshi_px'); kc = px + fee(px) if px is not None else None
    o['kalshi_cost'] = r3(kc); o['kalshi_am'] = to_am(kc)
    md, mk = o.get('model'), o.get('market')
    o['edge'] = round((md - kc) * 100, 2) if md is not None and kc is not None else None
    o['edge_mkt'] = round((mk - kc) * 100, 2) if mk is not None and kc is not None else None
    if o.get('book_odds') is not None:
        bc = imp(o['book_odds'])
        o['book_edge'] = round((md - bc) * 100, 2) if md is not None else None
        o['book_edge_mkt'] = round((mk - bc) * 100, 2) if mk is not None else None
        if kc is not None: o['best_venue'] = 'book' if bc < kc else 'kalshi'
        else: o['best_venue'] = 'book'
    if md is not None:
        o['target_am'] = to_am(md - .02)              # bet at or better than (+2 pts after fee / vig)
        o['target_kalshi_cents'] = max_kalshi(md - .02)
    return o

def build(SL, PL, live, now):
    latest = max(x['ts'] for x in SL)
    lat = dt.datetime.fromisoformat(latest).astimezone(ET)
    today = now.astimezone(ET).date()
    keep_from = (today - dt.timedelta(days=1)).isoformat()
    kev = {}
    for x in SL:
        e = parse_event(x.get('event'))
        if e: kev[e['code']] = e
    hist = collections.defaultdict(lambda: collections.defaultdict(list))   # key -> ts -> rows
    meta_ev = {}
    for x in SL:
        if 'hits allowed' in (x.get('what') or '').lower() or str(x.get('ticker') or '').startswith('KXMLBHA'): continue   # no-bet market
        ev = event_of(x, kev)
        if (ev['date'] or '') < keep_from: continue
        cat = cat_of(x['rule'])
        if cat == 'INFO' and x['ts'] != latest: continue
        k = card_key(cat, ev, x)
        hist[k][x['ts']].append(x); meta_ev[k] = (ev, cat)
    cards = []
    for k, by_ts in hist.items():
        ev, cat = meta_ev[k]
        tss = sorted(by_ts); last_ts = tss[-1]; rows = by_ts[last_ts]; live_now = last_ts == latest
        first_ts = tss[0]; first_rows = by_ts[first_ts]
        started = ev['start'] and dt.datetime.fromisoformat(ev['start']) <= now
        tier = min(r['tier'] for r in rows); rules = sorted({r['rule'] for r in rows}, key=lambda r: (RULE_TIER.get(r, 9), r))
        if not live_now and (started or tier > 2 or cat not in ('K', 'EARLY')): continue
        c = {'key': k, 'cat': cat, 'game': ev['label'], 'event': ev['code'], 'date': ev['date'], 'first_pitch': ev['start'],
             'away': ev['away'], 'home': ev['home'], 'tier': tier, 'rules': rules, 'live': live_now,
             'first_seen': first_ts, 'last_seen': last_ts, 'n_scans': len(tss), 'notes': [{'rule': r['rule'], 'note': r['note']} for r in rows], 'badges': []}
        c['started'] = bool(started)
        if started: live_g = {}   # in-game quotes are not pregame prices
        else: live_g = live
        N = parse_note('; '.join(r.get('note') or '' for r in rows))
        if cat == 'K':
            main = sorted(rows, key=lambda r: (r['tier'], K_ORDER.index(r['rule']) if r['rule'] in K_ORDER else 9))[0]
            first = sorted(first_rows, key=lambda r: r['tier'])[0]
            c.update(label=main['what'], side=main['side'], ticker=main.get('ticker'), pitcher=pitcher_of(main['what']),
                     direction='under' if main['side'] == 'NO' else 'over', kalshi_px=main.get('price'), first_px=first.get('price'))
            if first['what'] != main['what']:   # line lock: keep the first trigger's line, show the move
                c.update(locked_from=first['what'], label=first['what'], ticker=first.get('ticker'), now_label=main['what'],
                         now_ticker=main.get('ticker'), now_price=main.get('price'), kalshi_px=None)
            stake_rules = [r for r in rules if RULE_TIER.get(r, 9) <= 2]
            re_ = max([RULE_EDGE.get(r, 0) for r in stake_rules] + [0])
            pin = N.get('pin_fair') if 'locked_from' not in c else None
            mid = main.get('fair') if 'locked_from' not in c else None
            c['market'] = pin if pin is not None else mid; c['market_src'] = 'Pinnacle no-vig' if pin is not None else 'Kalshi mid'
            c['model'] = c['market'] + re_ / 100 if c['market'] is not None and stake_rules else None
            c['model_src'] = f"market + {re_:g}"; c['rule_edge'] = re_
            if 'locked_from' not in c and N.get('book_odds') is not None: c['book'], c['book_odds'] = N['book'], N['book_odds']
            c['inputs'] = {k2: N[k2] for k2 in ('outs_mkt', 'outs_season', 'outs_gap', 'k_mkt', 'k_season', 'starts', 'velo_d3') if k2 in N}
            if 'K-LENGTH UNDER' in rules: size = '½ unit'
            elif 'K-SEASON-ANCHOR UNDER' in rules:
                small = (N.get('starts') is not None and N['starts'] < 10) or (N.get('outs_gap') is not None and N['outs_gap'] > -2)
                size = '¼ unit' if small else '½ unit'
                if small: c['badges'].append('small sample / outs gap under 2: ¼')
            elif 'VELO-DECLINE K UNDER (paper)' in rules: size = 'paper / ¼'
            elif any(r in rules for r in ('K-LENGTH OVER', 'DEEP-SPOT OVERS')): size = '¼ unit'
            else: size = 'none'
            if 'VELO-DECLINE K UNDER (paper)' in rules: c['badges'].append('paper (Fable)')
            if tier == 1:
                conf = lat.date().isoformat() == ev['date'] and lat.hour >= 10
                c['level'] = 'bet' if conf else 'pending'
                c['status'] = f"Confirmed on the {lat.strftime('%-I:%M %p')} ET scan" if conf else 'Seen only on early scan(s): confirm on the 10 AM run'
            else: c['level'] = {2: 'lead'}.get(tier, 'info')
            c['size'] = size; c['panel'] = 'A' if tier == 1 else ('B' if tier == 2 else 'E')
            c['positions'] = [pos_from(c)]
        elif cat == 'EARLY':
            y = next((r for r in rows if 'RFI' in r['rule']), None); f5 = [r for r in rows if r['rule'] == 'F5 OVER']
            c['label'] = 'Early scoring (YRFI / F5 overs)'; c['side'] = 'YES'
            yr = None
            if y:
                Ny = parse_note(y['note'])
                model = Ny.get('curve_oct') if y['rule'] == 'YRFI CURVE (Oct-adj only)' else Ny.get('curve')
                yr = {'rule': y['rule'], 'ticker': y.get('ticker'), 'kalshi_px': y.get('price'), 'model': model,
                      'model_src': 'curve, Oct-adj (pending Fable)' if y['rule'] == 'YRFI CURVE (Oct-adj only)' else 'curve on Kalshi total',
                      'curve': Ny.get('curve'), 'curve_oct': Ny.get('curve_oct'), 'ktotal': Ny.get('ktotal'),
                      'market': Ny.get('pin_yrfi'), 'market_src': 'Pinnacle no-vig' if Ny.get('pin_yrfi') is not None else None,
                      'nrfi_pct': Ny.get('nrfi_pct'), 'side': 'YES', 'label': 'YRFI', 'cat': 'RFI',
                      'level': {'YRFI CURVE': 'bet', 'YRFI CURVE (Oct-adj only)': 'lean'}.get(y['rule'], 'info')}
                pricing(yr, live_g)
            rungs = []
            for r in f5:
                m = re.search(r'F5 o([\d.]+)', r['what'])
                rr = {'rule': 'F5 OVER', 'ticker': r.get('ticker'), 'kalshi_px': r.get('price'), 'model': r.get('fair'), 'model_src': 'F5 NB on Kalshi total',
                      'market': None, 'side': 'YES', 'label': f"F5 o{m.group(1) if m else '?'}", 'strike': float(m.group(1)) if m else None, 'cat': 'F5', 'level': 'bet'}
                pricing(rr, live_g); rungs.append(rr)
            rungs.sort(key=lambda r: -(r['edge'] if r.get('edge') is not None else -99))
            c['yrfi'] = yr; c['f5'] = rungs
            has1 = bool(rungs) or (yr and yr['rule'] == 'YRFI CURVE')
            c['level'] = 'bet' if has1 else ('lean' if yr and yr['rule'] == 'YRFI CURVE (Oct-adj only)' else 'info')
            c['size'] = 'small, one position' if has1 else ('small, late only' if c['level'] == 'lean' else 'none')
            c['panel'] = 'A' if has1 else 'C'
            c['positions'] = [pos_from({**c, **yr, 'rules': [yr['rule']], 'key': f"{ev['code']}|RFI", 'level': yr['level'], 'size': c['size'] if yr['level'] != 'info' else 'none'})] if yr else []
            if rungs: c['positions'].append(pos_from({**c, **rungs[0], 'rules': ['F5 OVER'], 'key': f"{ev['code']}|F5", 'size': c['size']}))
        elif cat == 'WALK':
            x = rows[0]
            c.update(label=x['what'], side='YES', ticker=x.get('ticker'), pitcher=pitcher_of(x['what']), kalshi_px=x.get('price'), first_px=first_rows[0].get('price'))
            c['market'] = N.get('pin_fair') if N.get('pin_fair') is not None else x.get('fair'); c['market_src'] = 'Pinnacle no-vig' if N.get('pin_fair') is not None else 'Kalshi mid'
            lead = x['rule'] == 'WALKS OVER (lead)'
            c['model'] = (x['fair'] + .04) if (lead and x.get('fair') is not None) else None; c['model_src'] = 'Kalshi mid + 4'
            if N.get('book_odds') is not None: c['book'], c['book_odds'] = N['book'], N['book_odds']
            c['inputs'] = {'bb_pct': N.get('bb_pct')}
            c['level'] = 'lean' if lead else 'info'; c['size'] = '¼ unit' if lead else 'none'; c['panel'] = 'D' if lead else 'E'
            c['positions'] = [pos_from(c)]
        elif cat == 'HIT':
            c['label'] = 'Hitter unders'; c['side'] = 'NO'
            octr = next((r for r in rows if r['rule'].startswith('OCTOBER')), None); rungs = []
            if octr:
                top = (octr['note'] or '').split('top:', 1)[-1]
                for part in top.split(';'):
                    m = re.match(r'\s*(.*?)\s+mid\s+([\d.]+)\s*$', part)
                    if not m: continue
                    mid_yes = float(m.group(2)); no_mid = 1 - mid_yes; fair = no_mid + OCT_HIT_EDGE / 100
                    rungs.append({'market': m.group(1), 'yes_mid': mid_yes, 'no_limit': round(no_mid, 3), 'no_fair': round(fair, 3),
                                  'edge_at_limit': round((fair - no_mid - fee(no_mid)) * 100, 2), 'buy_no_cents': max_kalshi(fair - .02)})
                c['oct_count'] = re.match(r'(\d+)', octr['what']).group(1) if re.match(r'(\d+)', octr['what']) else None
            subs = []
            for r in rows:
                if 'SUB-RISK' not in r['rule']: continue
                n = r['note'] or ''; s = {'rule': r['rule'], 'label': r['what'], 'model': r.get('fair'), 'lean': r['tier'] == 3}
                m = re.search(r'best\s+(\S+)\s+([+-]\d+)\s*\(u0\.5\s*(\w+)\)', n)
                if m: s['book'], s['book_odds'], s['book_mkt'] = m.group(1), int(m.group(2)), m.group(3)
                for fld, pat in (('ph_pct', r'pinch-hit-for\s*(\d+)%'), ('le2_pct', r'<=2 PA\s*(\d+)%'), ('starts', r'\((\d+) starts\)')):
                    m = re.search(pat, n)
                    if m: s[fld] = int(m.group(1))
                m = re.search(r'opp SP (.*?) ([\d.]+) outs/start', n)
                if m: s['opp_sp'], s['opp_outs'] = m.group(1), float(m.group(2)); s['short_leash'] = s['opp_outs'] < 15
                if s.get('book_odds') is not None and s.get('model') is not None:
                    s['edge'] = round((s['model'] - imp(s['book_odds'])) * 100, 2); s['target_am'] = to_am(s['model'] - .02)
                subs.append(s)
            subs.sort(key=lambda s: (not s['lean'], -(s.get('edge') or -99)))
            c['oct_rungs'] = rungs[:8]; c['subs'] = subs
            lean = any(r['tier'] == 3 for r in rows)
            c['level'] = 'lean' if lean else 'info'; c['size'] = 'small, spread (cap the game)' if lean else 'none'; c['panel'] = 'D' if lean else 'E'
            c['positions'] = [pos_from({**c, 'key': f"{k}|{s['label']}", 'label': s['label'], 'side': 'UNDER', 'model': s.get('model'),
                                        'book': s.get('book'), 'book_odds': s.get('book_odds'), 'level': 'lean' if s['lean'] else 'info', 'ticker': None})
                              for s in subs]
        else:   # OUTS, KOP, INFO
            x = rows[0]
            c.update(label=x['what'], side=x['side'], ticker=x.get('ticker'), kalshi_px=x.get('price'), first_px=first_rows[0].get('price'))
            if cat == 'OUTS':
                c['market'] = x.get('fair'); c['market_src'] = 'Kalshi mid'; c['model'] = (x['fair'] + RULE_EDGE['BIG OUTS TRADE'] / 100) if x.get('fair') is not None else None
                c['model_src'] = 'market + 1.5'
                mv = abs(N['print_px'] - N['now_mid']) * 100 if N.get('print_px') is not None and N.get('now_mid') is not None else None
                if x['tier'] == 3 or (mv is not None and mv > 2):
                    c['level'] = 'missed'; c['size'] = 'none'; c['status'] = f"Price moved {mv:.0f}¢ from the print" if mv is not None else 'Price already moved'
                else: c['level'] = 'lead'; c['size'] = '¼ unit'
                c['panel'] = 'B'
            elif cat == 'KOP':
                c['market'] = x.get('fair'); c['market_src'] = 'Pinnacle no-vig'; c['model'] = x.get('fair'); c['model_src'] = 'Pinnacle no-vig'
                c['level'] = 'watch'; c['size'] = 'paper'; c['badges'].append('ungraded'); c['panel'] = 'B'
            else:
                c['level'] = 'info'; c['size'] = 'none'; c['panel'] = 'E'; c['badges'].append('no backtested edge')
            c['positions'] = [pos_from(c)] if cat != 'INFO' else []
        if not live_now:
            c['level'] = 'gone'; c['panel'] = 'E'; c['status'] = f"No longer firing (last seen {dt.datetime.fromisoformat(last_ts).astimezone(ET).strftime('%-I:%M %p')} ET)"
            c['positions'] = []
        if cat not in ('EARLY', 'HIT'): pricing(c, live_g)
        for p in c.get('positions', []): pricing(p, live_g)
        cards.append(c)
    return latest, cards

def pos_from(c):
    """A gradeable position (what board_grade.py reads)."""
    keys = ('key', 'cat', 'rule_edge', 'game', 'event', 'first_pitch', 'rules', 'rule', 'label', 'side', 'ticker', 'level', 'size', 'model', 'model_src',
            'market', 'market_src', 'book', 'book_odds', 'kalshi_px', 'first_seen', 'locked_from', 'now_label', 'status')
    p = {k: c.get(k) for k in keys if c.get(k) is not None}
    if 'rule' in p and 'rules' not in p: p['rules'] = [p['rule']]
    return p

# ---------- Kalshi cross-venue lag (Fable live test), moved here from the retired Sharp Sides tab ----------
def lag_cards(now, live):
    """Read the lag-rule cards sides_board.py wrote (data/game_lines/board_latest.json) and show them in Leads.
    Graded by sides_grade.py (game_lines archive), so no positions here (no double counting)."""
    p = os.path.join(ROOT, 'data', 'game_lines', 'board_latest.json')
    try: B = json.load(open(p))
    except Exception: return []
    built = dt.datetime.fromisoformat(B['meta']['built'])
    if (now - built).total_seconds() > 45 * 60: return []   # stale: the 20-minute job has not run
    out, seen = [], set()
    for c in B.get('cards', []):
        if not str(c.get('mkt', '')).startswith('lag') or c.get('started'): continue
        k = f"{c['date']}|{c['game']}|LAG|{c['label']}"
        if k in seen: continue
        seen.add(k)
        d = {'key': k, 'cat': 'LAG', 'game': c['game'], 'event': c.get('kalshi_event') or c['game'], 'date': c['date'], 'first_pitch': c['first_pitch'],
             'away': c.get('away'), 'home': c.get('home'), 'tier': 2, 'rules': ['CROSS-VENUE LAG (Fable live test)'], 'live': True,
             'first_seen': B['meta']['built'], 'last_seen': B['meta']['built'], 'n_scans': 1, 'started': False,
             'notes': [{'rule': 'CROSS-VENUE LAG (Fable live test)', 'note': '; '.join(c.get('chips') or [])}],
             'badges': ['Fable live test'], 'label': c['label'], 'side': c.get('kalshi_side') or 'YES', 'ticker': c.get('ticker'),
             'kalshi_px': c.get('kalshi_px'), 'model': c.get('model'), 'model_src': c.get('market_fair_src'),
             'market': c.get('market_fair'), 'market_src': c.get('market_fair_src'), 'level': 'lead', 'size': 'small (live test)',
             'panel': 'B', 'positions': []}
        pricing(d, live)
        out.append(d)
    return out

# ---------- tickets ----------
def read_tickets():
    out = []
    for p in (os.path.join(SCAN, 'tickets.csv'), os.path.join(SCAN, 'october_ticket_log.csv'), os.path.join(ROOT, 'october_ticket_log.csv'), os.path.join(ROOT, 'data', 'october_ticket_log.csv')):
        if not os.path.exists(p): continue
        for r in csv.DictReader(open(p)):
            if not (r.get('date') or '').strip(): continue
            if 'notes' in r and 'example row' in (r.get('notes') or ''): continue
            bet = r.get('bet') or ' '.join(x for x in (r.get('market'), r.get('selection'), r.get('line')) if x)
            price = r.get('price') or r.get('price_paid')
            out.append({'date': r['date'].strip(), 'game': (r.get('game') or '').strip(), 'bet': bet.strip(), 'book': (r.get('book') or '').strip(),
                        'price': (price or '').strip(), 'size': (r.get('size') or r.get('stake') or '').strip(), 'src': os.path.basename(p)})
    return out

def ticket_teams(t):
    m = re.search(r'([A-Z]{2,3})\s*@\s*([A-Z]{2,3})', t.get('game') or '')
    return (norm_team(m.group(1)), norm_team(m.group(2))) if m else (None, None)

def overlaps(tickets, cards):
    """Flag tickets that share a view with a live card: same pitcher (outs vs K), same early-scoring game, opposite sides."""
    for t in tickets:
        a, h = ticket_teams(t); t['flags'] = []; txt = (t['bet'] or '').lower()
        for c in cards:
            if c['level'] in ('info', 'gone') or c.get('date') != t['date']: continue
            if not (a and {norm_team(c.get('away') or ''), norm_team(c.get('home') or '')} == {a, h}): continue
            if c['cat'] in ('K', 'WALK') and c.get('pitcher'):
                last = c['pitcher'].split()[-1].lower()
                if last in txt:
                    under = c.get('direction') == 'under' or c.get('side') == 'NO'
                    t_over = bool(re.search(r'\bover\b|\bo\d|\byes\b', txt)); t_under = bool(re.search(r'\bunder\b|\bu\d|\bno\b', txt))
                    clash = (under and t_over and 'out' in txt) or (not under and t_under and 'out' in txt)
                    t['flags'].append({'card': c['key'], 'text': f"{c['label']}: same pitcher, one view of his outing" + (" (CONFLICT: opposite direction)" if clash else '')})
            if c['cat'] == 'EARLY' and re.search(r'yrfi|nrfi|1st|first|f5', txt):
                clash = 'nrfi' in txt
                t['flags'].append({'card': c['key'], 'text': "Early-scoring card in this game: one position" + (" (CONFLICT: NRFI vs YRFI card)" if clash else '')})
    return tickets

def main():
    SL = load_jsonl(os.path.join(SCAN, 'scan_log.jsonl')); PL = load_jsonl(os.path.join(SCAN, 'pinnacle_log.jsonl'))
    if not SL: print('[board] no scan_log yet'); return
    now = dt.datetime.now(dt.timezone.utc); now_s = now.isoformat()
    latest = max(x['ts'] for x in SL)
    tickers = sorted({x['ticker'] for x in SL if x.get('ticker') and x['ts'] >= (now - dt.timedelta(days=2)).isoformat()})
    live = {}
    for i in range(0, len(tickers), 100):
        d = g(f"{API}/markets?limit=1000&tickers={','.join(tickers[i:i + 100])}")
        for m in d.get('markets', []):
            live[m['ticker']] = {'bid': float(m.get('yes_bid_dollars') or 0), 'ask': float(m.get('yes_ask_dollars') or 0), 'status': m.get('status')}
    latest, cards = build(SL, PL, live, now)
    cards += lag_cards(now, live)
    tickets = overlaps(read_tickets(), cards)
    velo_logged = len({(x['event'], pitcher_of(x['what'])) for x in SL if x['rule'] == 'VELO-DECLINE K UNDER (paper)'})
    pin_n = sum(1 for x in PL if x.get('ts') == latest)
    meta = {'built': now_s, 'latest_scan': latest, 'pinnacle_rows_latest': pin_n, 'velo_logged': velo_logged,
            'fable_review': {'VELO-DECLINE K UNDER (paper)': 150, 'HR anchor (paper, not in scan)': 200},
            'rule_tier': RULE_TIER}
    json.dump({'meta': meta, 'cards': cards, 'tickets': tickets}, open(os.path.join(SCAN, 'board_latest.json'), 'w'), indent=0, default=str)
    # archive positions (live, game not started)
    os.makedirs(BOARD, exist_ok=True); os.makedirs(CLOSE, exist_ok=True)
    by_date = collections.defaultdict(list)
    for c in cards:
        if not c['live']: continue
        if c.get('first_pitch') and dt.datetime.fromisoformat(c['first_pitch']) <= now: continue
        for p in c.get('positions', []): by_date[(c.get('first_pitch') or c.get('date') or now_s)[:10]].append(p)
    n = 0
    for d, rows in by_date.items():
        with open(os.path.join(BOARD, f'{d}.jsonl'), 'a') as f:
            for o in rows: f.write(json.dumps({'ts': now_s, 'scan_ts': latest, **o}, default=str) + '\n'); n += 1
    frozen = 0
    for d, rows in by_date.items():
        p = os.path.join(CLOSE, f'{d}.json'); cur = json.load(open(p)) if os.path.exists(p) else {}; changed = False
        for o in rows:
            fp = dt.datetime.fromisoformat(o['first_pitch']) if o.get('first_pitch') else None
            if not fp or o['key'] in cur: continue
            mins = (fp - now).total_seconds() / 60
            if not (0 <= mins <= FREEZE_MIN): continue
            m = live.get(o.get('ticker')) or {}
            mid = (m['bid'] + m['ask']) / 2 if m.get('bid') and m.get('ask') else None
            cur[o['key']] = {'frozen_at': now_s, 'mins_to_first_pitch': round(mins, 1),
                             'kalshi_close': {'bid': m.get('bid'), 'ask': m.get('ask'), 'mid_yes': mid,
                                              'mid_side': (1 - mid if o.get('side') == 'NO' else mid) if mid is not None else None},
                             'pinnacle_close': pinnacle_close(PL, o), 'board': o}
            changed = True; frozen += 1
        if changed: json.dump(cur, open(p, 'w'), indent=1, default=str)
    print(f'[board] {len(cards)} cards, {n} positions archived; {frozen} closes frozen; {len(tickets)} tickets; latest scan {latest}')

def pinnacle_close(PL, o):
    """Last Pinnacle quote before first pitch for the position's player/stat/line, as a no-vig prob for the bet side."""
    if not o.get('first_pitch'): return None
    fp = dt.datetime.fromisoformat(o['first_pitch'])
    if o.get('cat') == 'RFI':
        cand = [r for r in PL if r.get('stat') == 'RFI' and r.get('start') and abs((dt.datetime.fromisoformat(r['start'].replace('Z', '+00:00')) - fp).total_seconds()) < 3 * 3600]
        side_over = True
    else:
        m = re.match(r'^(.*?)\s+(K|walks)\s+([ou])([\d.]+)', o.get('label') or '')
        if not m: return None
        name, stat, ou, line = nz(m.group(1)), {'K': 'K', 'walks': 'BB'}[m.group(2)], m.group(3), float(m.group(4))
        cand = [r for r in PL if r.get('name') == name and r.get('stat') == stat and float(r.get('line') or -1) == line]
        side_over = ou == 'o'
    cand = [r for r in cand if dt.datetime.fromisoformat(r['ts']).astimezone(ET) <= fp and r.get('over') and r.get('under')]
    if not cand: return None
    r = max(cand, key=lambda r: r['ts'])
    p_over = power_novig(r['over'], r['under'])
    return {'ts': r['ts'], 'over': r['over'], 'under': r['under'], 'p_side': round(p_over if side_over else 1 - p_over, 4), 'limit': r.get('limit')}

if __name__ == '__main__':
    main()
