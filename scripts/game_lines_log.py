#!/usr/bin/env python3
"""Game-line logger for the Sharp Sides & Totals tab (runs every 20 min with the order-book job, and after each prop scan).

One snapshot per run, for today's and tomorrow's games (ET), full game + first five:
  data/game_lines/pinnacle_log.jsonl   Pinnacle (guest API): ML, main total, main run line, main team totals; F5 ML (0.0 spread, push on tie), F5 total, F5 TT. Limits.
  data/game_lines/an_log.jsonl         Action Network: open (book 30), consensus (15) with public tickets % / money %, and NY books
                                       DK 1548, FD 69, CZR 1005, BetRivers 972, Fanatics 2789, theScore 4621. BetMGM excluded; stale AN ids 68/1006 excluded.
  data/game_lines/kalshi_log.jsonl     Kalshi game markets (GAME, TOTAL, SPREAD, F5, F5TOTAL, F5SPREAD, TEAMTOTAL): bid/ask/last/volume for every open rung.
  data/game_lines/kalshi_prints.jsonl  Kalshi $1,000+ pregame prints on GAME, F5 and the TOTAL / F5TOTAL / TEAMTOTAL rungs nearest 50%, since the last run (deduped by trade id).
  data/game_lines/latest.json          The newest snapshot of all of the above, keyed by game (what sides_board.py reads).

Read-only public endpoints. Places nothing.
"""
import json, os, time, datetime as dt, urllib.request, urllib.error, collections, unicodedata

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.environ.get("LINES_DIR") or os.path.join(HERE, "..", "data", "game_lines")
os.makedirs(OUT, exist_ok=True)
ET = dt.timezone(dt.timedelta(hours=-4))
NOW = dt.datetime.now(dt.timezone.utc); NOW_S = NOW.isoformat()
TODAY = NOW.astimezone(ET).date(); DATES = [TODAY, TODAY + dt.timedelta(days=1)]
KAPI = "https://api.elections.kalshi.com/trade-api/v2"
PIN = "https://guest.api.arcadia.pinnacle.com/0.1"
AN_BOOKS = {'15': 'Consensus', '30': 'Open', '1548': 'DK', '69': 'FD', '1005': 'CZR', '972': 'BetRivers', '2789': 'Fanatics', '4621': 'theScore'}
KSER = ['KXMLBGAME', 'KXMLBTOTAL', 'KXMLBSPREAD', 'KXMLBF5', 'KXMLBF5TOTAL', 'KXMLBF5SPREAD', 'KXMLBTEAMTOTAL']
MON = ['JAN','FEB','MAR','APR','MAY','JUN','JUL','AUG','SEP','OCT','NOV','DEC']
CODES = {d.strftime('%y') + MON[d.month - 1] + d.strftime('%d') for d in DATES}
TEAM_AB = {'Arizona Diamondbacks':'ARI','Atlanta Braves':'ATL','Baltimore Orioles':'BAL','Boston Red Sox':'BOS','Chicago Cubs':'CHC','Chicago White Sox':'CWS',
  'Cincinnati Reds':'CIN','Cleveland Guardians':'CLE','Colorado Rockies':'COL','Detroit Tigers':'DET','Houston Astros':'HOU','Kansas City Royals':'KC',
  'Los Angeles Angels':'LAA','Los Angeles Dodgers':'LAD','Miami Marlins':'MIA','Milwaukee Brewers':'MIL','Minnesota Twins':'MIN','New York Mets':'NYM',
  'New York Yankees':'NYY','Athletics':'ATH','Oakland Athletics':'ATH','Philadelphia Phillies':'PHI','Pittsburgh Pirates':'PIT','San Diego Padres':'SD',
  'San Francisco Giants':'SF','Seattle Mariners':'SEA','St. Louis Cardinals':'STL','Tampa Bay Rays':'TB','Texas Rangers':'TEX','Toronto Blue Jays':'TOR','Washington Nationals':'WSH'}

def g(u, tries=5):
    for i in range(tries):
        try: return json.load(urllib.request.urlopen(urllib.request.Request(u, headers={'User-Agent': 'Mozilla/5.0'}), timeout=40))
        except urllib.error.HTTPError as e:
            if e.code == 404: return {}
            time.sleep(1.2 * (i + 1))
        except Exception: time.sleep(1.2 * (i + 1))
    return {}

def gkey(away, home, start_iso):
    """game key: AWAY@HOME|YYYY-MM-DDTHH (UTC hour of first pitch)"""
    return f"{away}@{home}|{start_iso[:13]}"

# ---------- Pinnacle ----------
def pinnacle():
    rows, latest = [], {}
    try:
        pm = g(f"{PIN}/leagues/246/matchups?brandId=0") or []; pk = g(f"{PIN}/leagues/246/markets/straight") or []
    except Exception as e:
        print('[lines] pinnacle failed', e); return rows, latest
    km = collections.defaultdict(list)
    for k in pk: km[k['matchupId']].append(k)
    for m in pm:
        if m.get('type') != 'matchup' or len(m.get('participants', [])) != 2 or 'Runs' in m['participants'][0]['name']: continue
        al = {p.get('alignment'): p['name'] for p in m['participants']}
        a, h = TEAM_AB.get(al.get('away')), TEAM_AB.get(al.get('home'))
        if not a or not h: continue
        key = gkey(a, h, m['startTime']); snap = latest.setdefault(key, {'start': m['startTime'], 'away': a, 'home': h})
        for k in km[m['id']]:
            per = k.get('period'); typ = k.get('type')
            if per not in (0, 1) or k.get('status') != 'open': continue
            if k.get('isAlternate') and not (per == 1 and typ == 'spread' and k['key'].endswith(';0.0')): continue
            lim = next((l['amount'] for l in k.get('limits') or [] if l['type'] == 'maxRiskStake'), None)
            P = {x['designation']: x for x in k['prices']}
            pfx = 'f5_' if per == 1 else ''
            if typ == 'moneyline' and 'home' in P and 'away' in P:
                rec = {'mkt': pfx + 'ml', 'away': P['away']['price'], 'home': P['home']['price'], 'limit': lim}
            elif typ == 'spread' and per == 1 and k['key'].endswith(';0.0'):
                rec = {'mkt': 'f5_ml', 'away': P['away']['price'], 'home': P['home']['price'], 'limit': lim, 'note': 'F5 0.0 spread (push on tie)'}
            elif typ == 'spread':
                rec = {'mkt': pfx + 'rl', 'line_home': P['home'].get('points'), 'away': P['away']['price'], 'home': P['home']['price'], 'limit': lim}
            elif typ == 'total':
                rec = {'mkt': pfx + 'total', 'line': P['over'].get('points'), 'over': P['over']['price'], 'under': P['under']['price'], 'limit': lim}
            elif typ == 'team_total':
                rec = {'mkt': pfx + 'tt_' + k.get('side', ''), 'line': P['over'].get('points'), 'over': P['over']['price'], 'under': P['under']['price'], 'limit': lim}
            else: continue
            if rec['mkt'] in snap and rec['mkt'] == 'f5_ml' and 'note' in rec: continue   # prefer a true F5 ML if Pinnacle lists one
            snap[rec['mkt']] = rec
            rows.append({'ts': NOW_S, 'game': key, **rec})
    return rows, latest

# ---------- Action Network ----------
def action_network():
    rows, latest = [], {}
    for d in DATES:
        x = g(f"https://api.actionnetwork.com/web/v2/scoreboard/mlb?bookIds={','.join(AN_BOOKS)}&date={d.strftime('%Y%m%d')}&periods=event,firstfiveinnings")
        for gm in x.get('games', []):
            tm = {t['id']: t.get('abbr') for t in gm.get('teams', [])}
            a, h = tm.get(gm['away_team_id']), tm.get(gm['home_team_id'])
            a = {'CHW': 'CWS', 'WAS': 'WSH', 'AZ': 'ARI', 'OAK': 'ATH'}.get(a, a); h = {'CHW': 'CWS', 'WAS': 'WSH', 'AZ': 'ARI', 'OAK': 'ATH'}.get(h, h)
            key = gkey(a, h, gm['start_time'].replace('.000Z', 'Z')); snap = latest.setdefault(key, {'start': gm['start_time'], 'away': a, 'home': h, 'an_id': gm['id'], 'status': gm.get('status'), 'books': {}})
            for bid, per_m in (gm.get('markets') or {}).items():
                bk = AN_BOOKS.get(bid)
                if not bk: continue
                for per, mk in per_m.items():
                    pfx = 'f5_' if per == 'firstfiveinnings' else ''
                    out = {}
                    for typ, outs in mk.items():
                        for o in outs:
                            if o.get('is_live') or o.get('is_alt_market'): continue
                            side = 'away' if o.get('team_id') == gm['away_team_id'] else ('home' if o.get('team_id') == gm['home_team_id'] else o.get('side'))
                            bi = o.get('bet_info') or {}
                            pub = {'tickets_pct': (bi.get('tickets') or {}).get('percent'), 'money_pct': (bi.get('money') or {}).get('percent')} if bk == 'Consensus' else {}
                            if typ == 'moneyline': out.setdefault(pfx + 'ml', {})[side] = {'odds': o['odds'], **pub}
                            elif typ == 'total': out.setdefault(pfx + 'total', {})[o['side']] = {'odds': o['odds'], 'line': o.get('value'), **pub}
                            elif typ == 'spread': out.setdefault(pfx + 'rl', {})[side] = {'odds': o['odds'], 'line': o.get('value'), **pub}
                            elif typ == 'core_bet_type_6_team_score': out.setdefault(pfx + 'tt_' + side, {})[o['side']] = {'odds': o['odds'], 'line': o.get('value')}
                    snap['books'].setdefault(bk, {}).update(out)
                    if out: rows.append({'ts': NOW_S, 'game': key, 'book': bk, 'period': per, **out})
    return rows, latest

# ---------- Kalshi ----------
def kalshi():
    rows, latest, mains = [], {}, []
    for s in KSER:
        cur = ''
        while True:
            x = g(f"{KAPI}/markets?series_ticker={s}&status=open&limit=1000" + (f"&cursor={cur}" if cur else ''))
            for m in x.get('markets', []):
                ev = m['event_ticker'].split('-', 1)[1]
                if ev[:7] not in CODES: continue
                r = {'ticker': m['ticker'], 'series': s, 'event': ev, 'title': m.get('yes_sub_title') or m.get('title'), 'strike': m.get('floor_strike'),
                     'bid': float(m.get('yes_bid_dollars') or 0), 'ask': float(m.get('yes_ask_dollars') or 0), 'last': float(m.get('last_price_dollars') or 0),
                     'vol': float(m.get('volume_fp') or 0), 'oi': float(m.get('open_interest_fp') or 0)}
                rows.append({'ts': NOW_S, **r}); latest.setdefault(ev, []).append(r)
            cur = x.get('cursor')
            if not cur or not x.get('markets'): break
    # rungs whose prints we track: GAME + F5 (all), plus the TOTAL / F5TOTAL / TEAMTOTAL rung nearest 50% per event (per team for TT)
    for ev, rs in latest.items():
        for r in rs:
            if r['series'] in ('KXMLBGAME', 'KXMLBF5'): mains.append(r['ticker'])
        for s in ('KXMLBTOTAL', 'KXMLBF5TOTAL'):
            c = [r for r in rs if r['series'] == s and r['bid'] > 0 and r['ask'] < 1]
            if c: mains.append(min(c, key=lambda r: abs((r['bid'] + r['ask']) / 2 - .5))['ticker'])
        tt = collections.defaultdict(list)
        for r in rs:
            if r['series'] == 'KXMLBTEAMTOTAL' and r['bid'] > 0 and r['ask'] < 1: tt[r['ticker'].rsplit('-', 1)[1].rstrip('0123456789')].append(r)
        for team, c in tt.items(): mains.append(min(c, key=lambda r: abs((r['bid'] + r['ask']) / 2 - .5))['ticker'])
    return rows, latest, mains

def kalshi_prints(mains):
    p = os.path.join(OUT, 'kalshi_prints.jsonl'); seen = set(); last_ts = None
    if os.path.exists(p):
        for l in open(p):
            try: x = json.loads(l); seen.add(x['trade_id']); last_ts = max(last_ts or x['created'], x['created'])
            except Exception: pass
    min_ts = int((NOW - dt.timedelta(hours=6)).timestamp())
    new = []
    for t in mains:
        x = g(f"{KAPI}/markets/trades?ticker={t}&limit=1000&min_ts={min_ts}")
        for tr in x.get('trades', []):
            if tr['trade_id'] in seen: continue
            px = float(tr['yes_price_dollars'] if tr['taker_side'] == 'yes' else tr['no_price_dollars'])
            usd = float(tr['count_fp']) * px
            if usd >= 1000:
                new.append({'trade_id': tr['trade_id'], 'created': tr['created_time'], 'ticker': t, 'taker_side': tr['taker_side'],
                            'yes_price': float(tr['yes_price_dollars']), 'count': float(tr['count_fp']), 'usd': round(usd)})
        time.sleep(0.12)
    with open(p, 'a') as f:
        for x in new: f.write(json.dumps(x) + '\n')
    return new

def append(name, rows):
    with open(os.path.join(OUT, name), 'a') as f:
        for r in rows: f.write(json.dumps(r) + '\n')

def main():
    prow, plat = pinnacle(); arow, alat = action_network(); krow, klat, mains = kalshi(); prints = kalshi_prints(mains)
    append('pinnacle_log.jsonl', prow); append('an_log.jsonl', arow); append('kalshi_log.jsonl', krow)
    json.dump({'ts': NOW_S, 'pinnacle': plat, 'action': alat, 'kalshi': klat}, open(os.path.join(OUT, 'latest.json'), 'w'))
    print(f"[lines] pinnacle {len(plat)} games / {len(prow)} rows; AN {len(alat)} games / {len(arow)} rows; Kalshi {len(klat)} events / {len(krow)} rungs; {len(prints)} new $1k+ prints")

if __name__ == '__main__':
    main()
