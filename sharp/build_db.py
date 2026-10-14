#!/usr/bin/env python3
"""Fold the append-only MLB logs into the sharp tracker schema (events + ticks), rebuilt from scratch on every run.

Sources (all written by the existing GitHub Actions jobs; this script only reads):
  data/game_lines/pinnacle_log.jsonl       Pinnacle game lines, with max-risk limits           -> venue Pinnacle
  data/game_lines/an_log.jsonl             Action Network per-book game lines (+ Consensus/Open) -> DK FD Caesars BetRivers Fanatics theScore, AN_Consensus, AN_Open
  data/game_lines/kalshi_log.jsonl         Kalshi game markets (bid/ask/last/vol)                -> Kalshi
  data/kalshi_oblog/YYYY-MM-DD.jsonl       Kalshi prop order books                               -> Kalshi (props)
  data/novig_log/YYYY-MM-DD.jsonl          Novig game + prop books                               -> Novig
  data/an_props_log/YYYY-MM-DD.jsonl       Action Network per-book props                         -> books (props)
  data/prop_scan/pinnacle_log.jsonl        Pinnacle props with limits                            -> Pinnacle (props)

Canonical event id = statsapi gamePk. Matching is by participants + ET date (+ nearest start for doubleheaders), never by name string.
Persistent caches (small, committed): data/sharp/schedule_cache.json, data/sharp/kalshi_markets.json.
Output: data/sharp/sharp.sqlite (gitignored; derived).
"""
import json, os, sys, glob, time, sqlite3, datetime as dt, urllib.request, collections, re
HERE = os.path.dirname(os.path.abspath(__file__)); sys.path.insert(0, os.path.dirname(HERE))
from sharp import norm as N

ROOT = os.path.abspath(os.path.join(HERE, '..'))
D = os.path.join(ROOT, 'data'); SD = os.path.join(D, 'sharp'); os.makedirs(SD, exist_ok=True)
DB = os.path.join(SD, 'sharp.sqlite')
SCHED = os.path.join(SD, 'schedule_cache.json'); KMETA = os.path.join(SD, 'kalshi_markets.json')
OFFLINE = os.environ.get('SHARP_OFFLINE') == '1'
NOW = dt.datetime.now(dt.timezone.utc)


def g(u, tries=4):
    if OFFLINE: return {}
    for i in range(tries):
        try: return json.load(urllib.request.urlopen(urllib.request.Request(u, headers={'User-Agent': 'Mozilla/5.0'}), timeout=40))
        except urllib.error.HTTPError as e:
            if e.code == 404: return {}
            time.sleep(1.5 * (i + 1))
        except Exception: time.sleep(1.5 * (i + 1))
    return {}


def jl(p):
    try:
        with open(p) as f:
            for l in f:
                l = l.strip()
                if l:
                    try: yield json.loads(l)
                    except Exception: pass
    except FileNotFoundError: return


# ----------------------------------------------------------------- schedule / events ----
class Events:
    """statsapi schedule per ET date; match (date, away, home[, start]) -> gamePk."""
    def __init__(self):
        self.cache = json.load(open(SCHED)) if os.path.exists(SCHED) else {}
        self.ev = {}          # gamePk -> dict
        self.vids = collections.defaultdict(lambda: collections.defaultdict(set))
        self.unmatched = collections.Counter()

    def games(self, date):
        d = date.isoformat()
        fresh = (date >= N.et_date(NOW) - dt.timedelta(days=1))
        if d not in self.cache or (fresh and self.cache[d].get('fetched', '') < (NOW - dt.timedelta(hours=1)).isoformat()):
            x = g(f"https://statsapi.mlb.com/api/v1/schedule?sportId=1&date={d}&hydrate=team")
            rows = []
            for dd in x.get('dates', []):
                for gm in dd['games']:
                    ab = lambda t: N.team(t['team'].get('abbreviation')) or N.team(t['team'].get('name'))
                    rows.append(dict(pk=str(gm['gamePk']), start=gm['gameDate'], away=ab(gm['teams']['away']), home=ab(gm['teams']['home']),
                                     status=(gm.get('status') or {}).get('abstractGameState'), detail=(gm.get('status') or {}).get('detailedState'),
                                     dh=gm.get('doubleHeader'), n=gm.get('gameNumber')))
            if rows or not OFFLINE: self.cache[d] = dict(fetched=NOW.isoformat(), games=rows)
        return self.cache.get(d, {}).get('games', [])

    def match(self, date, away, home, start=None, vendor=None, vid=None):
        if not (away and home): return None
        cands = [x for x in self.games(date) if x['away'] == away and x['home'] == home]
        if not cands and start is not None:   # a game key built on a UTC hour may sit on the next ET date
            for dd in (-1, 1):
                cands = [x for x in self.games(date + dt.timedelta(days=dd)) if x['away'] == away and x['home'] == home]
                if cands: break
        if not cands: self.unmatched[f"{date} {away}@{home}"] += 1; return None
        if len(cands) > 1 and start is not None:
            cands.sort(key=lambda x: abs((N.parse_ts(x['start']) - start).total_seconds()))
        c = cands[0]
        self.ev[c['pk']] = dict(event_id=c['pk'], sport='mlb', league='MLB', et_date=N.et_date(N.parse_ts(c['start'])).isoformat(),
                                start_utc=N.iso(N.parse_ts(c['start'])), away=away, home=home, status=c['status'])
        if vendor and vid is not None: self.vids[c['pk']][vendor].add(str(vid))
        return c['pk']

    def save(self):
        json.dump(self.cache, open(SCHED, 'w'))


EV = Events()


def gkey_parse(k):
    """'TB@NYY|2026-10-08T00' -> (away, home, start_hour_utc)."""
    teams, hr = k.split('|'); a, h = teams.split('@')
    return a, h, dt.datetime.strptime(hr, '%Y-%m-%dT%H').replace(tzinfo=dt.timezone.utc)


def ev_from_gkey(k, vendor=None, vid=None):
    a, h, st = gkey_parse(k)
    return EV.match(N.et_date(st), a, h, st, vendor, vid), st


# ----------------------------------------------------------------- tick writer ----
class Ticks:
    def __init__(self):
        self.rows = []; self.seen = set()

    def add(self, ts, event_id, start, market, side, venue, feed, src, participant=None, player_name=None, line=None,
            american=None, bid=None, ask=None, last=None, volume=None, limit=None, flags=None, extra=None, vendor_ts=None):
        if event_id is None: return
        key = (ts, event_id, market, line, side, venue, participant)
        if key in self.seen: return          # same observation logged twice (e.g. an extra snapshot in the same second)
        self.seen.add(key)
        dec = imp = mid = None
        if american is not None:
            american = int(round(float(american))); dec = N.american_to_decimal(american); imp = 1 / dec
        elif bid is not None or ask is not None:
            if bid is not None and ask is not None: mid = (bid + ask) / 2
            imp = ask                      # cost to take the side now
            if ask: dec = 1 / ask; american = N.prob_to_american(ask)
        self.rows.append((ts, vendor_ts, 'mlb', 'MLB', event_id, start, market, N.MARKET_CLASS(market), participant, player_name, line, side, venue,
                          american, dec, imp, bid, ask, mid, last, volume, limit, feed, ','.join(flags) if flags else None,
                          json.dumps(extra) if extra else None, src))


T = Ticks()


# ----------------------------------------------------------------- Pinnacle game lines ----
def load_pinnacle_games():
    src = 'game_lines/pinnacle_log.jsonl'; n = 0
    for r in jl(os.path.join(D, src)):
        ev, st = ev_from_gkey(r['game'])
        if not ev: continue
        ts = N.iso(N.parse_ts(r['ts'])); a, h, _ = gkey_parse(r['game']); m = r['mkt']; lim = r.get('limit')
        if m in ('ml', 'f5_ml'):
            T.add(ts, ev, None, m, 'away', 'Pinnacle', 'pinnacle_guest', src, participant=a, american=r['away'], limit=lim)
            T.add(ts, ev, None, m, 'home', 'Pinnacle', 'pinnacle_guest', src, participant=h, american=r['home'], limit=lim)
        elif m in ('rl', 'f5_rl'):
            lh = r.get('line_home')
            if lh is None: continue
            T.add(ts, ev, None, m, 'home', 'Pinnacle', 'pinnacle_guest', src, participant=h, line=lh, american=r['home'], limit=lim)
            T.add(ts, ev, None, m, 'away', 'Pinnacle', 'pinnacle_guest', src, participant=a, line=-lh, american=r['away'], limit=lim)
        elif m in ('total', 'f5_total', 'tt_home', 'tt_away', 'f5_tt_home', 'f5_tt_away'):
            part = h if m.endswith('home') else (a if m.endswith('away') else None)
            T.add(ts, ev, None, m, 'over', 'Pinnacle', 'pinnacle_guest', src, participant=part, line=r.get('line'), american=r['over'], limit=lim)
            T.add(ts, ev, None, m, 'under', 'Pinnacle', 'pinnacle_guest', src, participant=part, line=r.get('line'), american=r['under'], limit=lim)
        n += 1
    return n


# ----------------------------------------------------------------- Action Network game lines ----
def load_an_games():
    src = 'game_lines/an_log.jsonl'; n = 0
    for r in jl(os.path.join(D, src)):
        ev, st = ev_from_gkey(r['game'])
        if not ev: continue
        ts = N.iso(N.parse_ts(r['ts'])); a, h, _ = gkey_parse(r['game']); venue = N.AN_BOOK_GAME.get(r['book'])
        if not venue: continue
        for m, sides in r.items():
            if m in ('ts', 'game', 'book', 'period') or not isinstance(sides, dict): continue
            for side, o in sides.items():
                if o.get('odds') is None: continue
                part = {'home': h, 'away': a}.get(side)
                if m.startswith('tt_') or m.startswith('f5_tt_'): part = h if m.endswith('home') else a
                line = o.get('line')
                if m in ('ml', 'f5_ml'): line = None
                extra = {k: o[k] for k in ('tickets_pct', 'money_pct') if o.get(k) is not None} or None
                T.add(ts, ev, None, m, side, venue, 'action_network', src, participant=part, line=line, american=o['odds'], extra=extra)
        n += 1
    return n


# ----------------------------------------------------------------- Kalshi ----
def kalshi_ev(evt, vid=None):
    p = N.kalshi_event(evt)
    if not p: return None, None
    date, a, h, hhmm = p
    st = dt.datetime(date.year, date.month, date.day, int(hhmm[:2]), int(hhmm[2:]), tzinfo=N.ET).astimezone(dt.timezone.utc)
    return EV.match(date, a, h, st, 'kalshi', evt), st


def add_exchange_pair(ts, ev, market, side_yes, side_no, venue, feed, src, bid, ask, last, vol, part_yes=None, part_no=None, line=None,
                      player_name=None, extra=None, line_no=None):
    """Store the yes side and its complement so every market has both sides as rows."""
    flags = []
    if bid is not None and ask is not None and ask - bid > 0.04: flags.append('wide')
    if (bid or 0) <= 0 or (ask or 1) >= 1: flags.append('oneside')
    T.add(ts, ev, None, market, side_yes, venue, feed, src, participant=part_yes, player_name=player_name, line=line, bid=bid, ask=ask, last=last,
          volume=vol, flags=flags, extra=extra)
    nb = (1 - ask) if ask is not None else None; na = (1 - bid) if bid is not None else None
    T.add(ts, ev, None, market, side_no, venue, feed, src, participant=part_no, player_name=player_name, line=line if line_no is None else line_no,
          bid=nb, ask=na, last=(1 - last) if last else None, volume=vol, flags=flags, extra=extra)


def load_kalshi_games():
    src = 'game_lines/kalshi_log.jsonl'; n = 0
    for r in jl(os.path.join(D, src)):
        ev, st = kalshi_ev(r['event'])
        if not ev: continue
        p = N.kalshi_event(r['event']); date, a, h, _ = p
        ts = N.iso(N.parse_ts(r['ts'])); s = r['series']; suf = r['ticker'].rsplit('-', 1)[1]
        bid, ask, last, vol = (r.get('bid') or None), (r.get('ask') or None), r.get('last'), r.get('vol')
        if bid is not None and ask is not None and ask < bid: continue
        strike = r.get('strike')
        if s in ('KXMLBGAME', 'KXMLBF5'):
            m = 'ml' if s == 'KXMLBGAME' else 'f5_ml'
            if suf == 'TIE':
                T.add(ts, ev, None, m, 'draw', 'Kalshi', 'kalshi_api', src, bid=bid, ask=ask, last=last, volume=vol, flags=['threeway']); continue
            team = N.AB_FIX.get(suf, suf)
            if team not in (a, h): continue
            yes = 'home' if team == h else 'away'; no = 'away' if yes == 'home' else 'home'
            T.add(ts, ev, None, m, yes, 'Kalshi', 'kalshi_api', src, participant=team, bid=bid, ask=ask, last=last, volume=vol,
                  flags=(['wide'] if bid and ask and ask - bid > 0.04 else None))
            # the other team has its own ticker in the same log; do not synthesise it
        elif s in ('KXMLBTOTAL', 'KXMLBF5TOTAL'):
            if strike is None: continue
            add_exchange_pair(ts, ev, 'total' if s == 'KXMLBTOTAL' else 'f5_total', 'over', 'under', 'Kalshi', 'kalshi_api', src, bid, ask, last, vol, line=strike)
        elif s in ('KXMLBSPREAD', 'KXMLBF5SPREAD'):
            if strike is None: continue
            team = N.AB_FIX.get(re.sub(r'\d+$', '', suf), re.sub(r'\d+$', '', suf))
            if team not in (a, h): continue
            yes = 'home' if team == h else 'away'; other = a if yes == 'home' else h
            add_exchange_pair(ts, ev, 'rl' if s == 'KXMLBSPREAD' else 'f5_rl', yes, 'away' if yes == 'home' else 'home', 'Kalshi', 'kalshi_api', src,
                              bid, ask, last, vol, part_yes=team, part_no=other, line=-strike, line_no=strike)
        elif s == 'KXMLBTEAMTOTAL':
            if strike is None: continue
            team = N.AB_FIX.get(re.sub(r'\d+$', '', suf), re.sub(r'\d+$', '', suf))
            if team not in (a, h): continue
            m = 'tt_home' if team == h else 'tt_away'
            add_exchange_pair(ts, ev, m, 'over', 'under', 'Kalshi', 'kalshi_api', src, bid, ask, last, vol, part_yes=team, part_no=team, line=strike)
        n += 1
    return n


class KalshiMeta:
    def __init__(self):
        self.m = json.load(open(KMETA)) if os.path.exists(KMETA) else {}
        self.misses = 0

    def get(self, ticker):
        if ticker in self.m: return self.m[ticker]
        if OFFLINE or self.misses > 60: return None
        x = g(f"https://api.elections.kalshi.com/trade-api/v2/markets/{ticker}").get('market')
        if not x: self.misses += 1; self.m[ticker] = None; return None
        self.m[ticker] = dict(title=x.get('title'), sub=x.get('yes_sub_title'), floor=x.get('floor_strike'), stype=x.get('strike_type'))
        return self.m[ticker]

    def prefetch(self, pairs):
        """One call per (series, event_ticker) instead of one per market; threaded. Settled markets need status=settled|closed too."""
        import concurrent.futures as cf
        todo = [p for p in sorted(set(pairs)) if not self.m.get(f"_ev:{p[0]}:{p[1]}")]
        if OFFLINE or not todo: return
        def fetch(p):
            s, e = p; out = {}
            for st in ('', '&status=settled', '&status=closed'):
                for m in g(f"https://api.elections.kalshi.com/trade-api/v2/markets?series_ticker={s}&event_ticker={s}-{e}&limit=1000{st}").get('markets', []):
                    out[m['ticker']] = dict(title=m.get('title'), sub=m.get('yes_sub_title'), floor=m.get('floor_strike'), stype=m.get('strike_type'))
            return p, out
        with cf.ThreadPoolExecutor(8) as ex:
            for p, out in ex.map(fetch, todo):
                self.m.update(out); self.m[f"_ev:{p[0]}:{p[1]}"] = NOW.isoformat()
        self.save()
        print(f"[build] kalshi metadata: fetched {len(todo)} series-events, {sum(1 for k in self.m if not k.startswith('_'))} tickers cached")

    def save(self): json.dump(self.m, open(KMETA, 'w'))


KM = KalshiMeta()


def load_kalshi_props():
    n = 0
    pairs = set()
    for f in glob.glob(os.path.join(D, 'kalshi_oblog', '*.jsonl')):
        for r in jl(f):
            p = r['ticker'].split('-')
            if len(p) >= 3 and r['series'] in N.STAT_KALSHI and r['series'] != 'KXMLBRFI': pairs.add((r['series'], p[1]))
    KM.prefetch(pairs)
    for f in sorted(glob.glob(os.path.join(D, 'kalshi_oblog', '*.jsonl'))):
        src = 'kalshi_oblog/' + os.path.basename(f)
        for r in jl(f):
            s = r['series']; parts = r['ticker'].split('-')
            if len(parts) < 3: continue
            evt = parts[1]; ev, st = kalshi_ev(evt)
            if not ev: continue
            ts = N.iso(N.parse_ts(r['ts'])); bid = r.get('yes_bid') or None; ask = r.get('yes_ask')
            if ask is None or (bid is not None and ask < bid): continue
            extra = {k: r.get(k) for k in ('yes_depth3', 'no_depth3', 'yes_depth10', 'no_depth10', 'yes_total', 'no_total')}
            if s in ('KXMLBTOTAL', 'KXMLBF5TOTAL'):
                line = float(parts[-1]) - 0.5
                add_exchange_pair(ts, ev, 'total' if s == 'KXMLBTOTAL' else 'f5_total', 'over', 'under', 'Kalshi', 'kalshi_api', src, bid, ask, None, r.get('vol'),
                                  line=line, extra=extra); n += 1; continue
            stat = N.STAT_KALSHI.get(s)
            if not stat: continue
            meta = KM.get(r['ticker'])
            if s == 'KXMLBRFI':
                add_exchange_pair(ts, ev, 'rfi', 'over', 'under', 'Kalshi', 'kalshi_api', src, bid, ask, None, r.get('vol'), line=0.5, extra=extra); n += 1; continue
            if not meta or meta.get('floor') is None: continue
            name = (meta.get('title') or '').split(':')[0].strip()
            pk = N.player_key(name)
            add_exchange_pair(ts, ev, f'prop_{stat}', 'over', 'under', 'Kalshi', 'kalshi_api', src, bid, ask, None, r.get('vol'),
                              part_yes=pk, part_no=pk, line=float(meta['floor']), player_name=name, extra=extra)
            n += 1
    return n


# ----------------------------------------------------------------- Novig ----
def load_novig():
    n = 0
    for f in sorted(glob.glob(os.path.join(D, 'novig_log', '*.jsonl'))):
        src = 'novig_log/' + os.path.basename(f)
        for r in jl(f):
            if '@' not in r.get('event', '') or not r.get('book'): continue
            aw, hm = [s.strip() for s in r['event'].split('@', 1)]; a, h = N.team(aw), N.team(hm)
            st = N.parse_ts(r['start']); ev = EV.match(N.et_date(st), a, h, st, 'novig', r['event'])
            if not ev: continue
            ts = N.iso(N.parse_ts(r['ts'])); typ = r['type']; strike = float(r['strike']) if r.get('strike') not in (None, '') else None
            book = r['book']
            def bidq(name):
                o = book.get(name); return (o['best'], o) if o else (None, None)
            def sides(names):
                """names: {side: outcome-name}; returns per-side (bid, ask) with the ask derived from the other side's bid."""
                b = {s: bidq(nm) for s, nm in names.items()}
                out = {}
                for s in names:
                    other = [t for t in names if t != s][0]
                    bid, o = b[s]; ob, _ = b[other]
                    ask = (1 - ob) if ob is not None else None
                    out[s] = (bid, ask, o)
                return out
            def emit(market, names, part=None, line=None, player=None):
                nonlocal n
                ss = sides(names); flags = []
                bids = [v[0] for v in ss.values() if v[0] is not None]
                if len(bids) < 2: flags.append('oneside')
                elif 1 - sum(bids) > 0.04: flags.append('wide')
                for s, (bid, ask, o) in ss.items():
                    if bid is None and ask is None: continue
                    extra = {'qty_best': o.get('qty_best'), 'qty_3c': o.get('qty_3c')} if o else None
                    p = part.get(s) if isinstance(part, dict) else part
                    T.add(ts, ev, None, market, s, 'Novig', 'novig_api', src, participant=p, player_name=player, line=line, bid=bid, ask=ask, flags=flags, extra=extra)
                n += 1
            if typ in ('MONEY', 'MONEY_1H'):
                names = {}
                for nm in book:
                    t = N.team(nm)
                    if t == h: names['home'] = nm
                    elif t == a: names['away'] = nm
                if len(names) == 2: emit('ml' if typ == 'MONEY' else 'f5_ml', names, part={'home': h, 'away': a})
            elif typ in ('SPREAD', 'SPREAD_1H'):
                names, lines = {}, {}
                for nm in book:
                    m = re.match(r'^(\w+)\s+([+-]?\d+\.?\d*)$', nm)
                    if not m: continue
                    t = N.team(m.group(1)); side = 'home' if t == h else ('away' if t == a else None)
                    if side: names[side] = nm; lines[side] = float(m.group(2))
                if len(names) == 2:
                    ss = sides(names); flags = ['wide'] if (ss['home'][0] is not None and ss['away'][0] is not None and 1 - ss['home'][0] - ss['away'][0] > 0.04) else []
                    for s, (bid, ask, o) in ss.items():
                        if bid is None and ask is None: continue
                        T.add(ts, ev, None, 'rl' if typ == 'SPREAD' else 'f5_rl', s, 'Novig', 'novig_api', src, participant=h if s == 'home' else a, line=lines[s],
                              bid=bid, ask=ask, flags=flags, extra={'qty_best': o.get('qty_best')} if o else None)
                    n += 1
            elif typ in ('TOTAL', 'TOTAL_1H', 'TEAM_TOTAL', 'FIRST_INNING_TOTAL') or typ in N.STAT_NOVIG:
                ov = next((nm for nm in book if nm.lower().startswith('over')), None); un = next((nm for nm in book if nm.lower().startswith('under')), None)
                if not (ov or un): continue
                names = {'over': ov or 'Over ?', 'under': un or 'Under ?'}
                if typ == 'TOTAL': emit('total', names, line=strike)
                elif typ == 'TOTAL_1H': emit('f5_total', names, line=strike)
                elif typ == 'FIRST_INNING_TOTAL': emit('rfi', names, line=0.5)
                elif typ == 'TEAM_TOTAL':
                    t = N.team(r['desc'].rsplit(' ', 2)[0])
                    if t in (a, h): emit('tt_home' if t == h else 'tt_away', names, part=t, line=strike)
                else:
                    stat = N.STAT_NOVIG[typ]; name = r['desc'].rsplit(' ', 2)[0]
                    emit(f'prop_{stat}', names, part=N.player_key(name), line=strike, player=name)
    return n


# ----------------------------------------------------------------- Action Network props ----
AN_PROP_BOOK = {'DK': 'DK', 'FD': 'FD', 'CZR': 'Caesars', 'BetRivers': 'BetRivers', 'Fanatics': 'Fanatics', 'theScore': 'theScore', 'MGM': 'BetMGM'}


def an_id_map():
    """AN event id -> gamePk, from the CLV archive and the game-lines snapshot (both already carry the mapping)."""
    m = {}
    for f in glob.glob(os.path.join(D, 'clv', '*.json')):
        try:
            for an, gm in json.load(open(f)).get('games', {}).items():
                if gm.get('game_pk'): m[str(an)] = str(gm['game_pk'])
        except Exception: pass
    lat = json.load(open(os.path.join(D, 'game_lines', 'latest.json'))) if os.path.exists(os.path.join(D, 'game_lines', 'latest.json')) else {}
    for k, v in (lat.get('action') or {}).items():
        if v.get('an_id'):
            ev, _ = ev_from_gkey(k, 'an', v['an_id'])
            if ev: m[str(v['an_id'])] = ev
    return m


def load_an_props():
    src_n = 0; m = an_id_map(); miss = collections.Counter()
    for f in sorted(glob.glob(os.path.join(D, 'an_props_log', '*.jsonl'))):
        src = 'an_props_log/' + os.path.basename(f)
        for r in jl(f):
            if r.get('src') != 'book' or r.get('odds') is None or r.get('line') is None: continue
            ev = m.get(str(r.get('game')))
            if not ev: miss[str(r.get('game'))] += 1; continue
            stat = N.STAT_AN.get(r['type']); venue = AN_PROP_BOOK.get(r['book'])
            if not stat or not venue: continue
            T.add(N.iso(N.parse_ts(r['ts'])), ev, None, f'prop_{stat}', r['side'], venue, 'action_network', src, participant=N.player_key(r['player']),
                  player_name=r['player'], line=float(r['line']), american=r['odds'])
            src_n += 1
    if miss: print(f"[build] AN props: {sum(miss.values())} rows on {len(miss)} unmapped AN game ids (no CLV/latest mapping): {list(miss)[:5]}")
    return src_n


# ----------------------------------------------------------------- Pinnacle props ----
def load_pinnacle_props(player_event):
    src = 'prop_scan/pinnacle_log.jsonl'; n = 0; miss = 0
    by_start = collections.defaultdict(set)
    for e in EV.ev.values(): by_start[e['start_utc'][:16]].add(e['event_id'])
    for r in jl(os.path.join(D, src)):
        stat = N.STAT_PINNACLE.get(str(r.get('stat', '')).upper())
        if not stat or r.get('line') is None: continue
        st = N.parse_ts(r['start']); pk = N.player_key(r['name']); date = N.et_date(st).isoformat()
        ev = player_event.get((date, pk))
        if not ev:
            c = by_start.get(N.iso(st)[:16], set())
            ev = next(iter(c)) if len(c) == 1 else None
        if not ev: miss += 1; continue
        ts = N.iso(N.parse_ts(r['ts']))
        for side in ('over', 'under'):
            if r.get(side) is None: continue
            T.add(ts, ev, None, f'prop_{stat}', side, 'Pinnacle', 'pinnacle_guest', src, participant=pk, player_name=r['name'], line=float(r['line']),
                  american=r[side], limit=r.get('limit'))
        n += 1
    if miss: print(f"[build] Pinnacle props: {miss} rows with no player->game match")
    return n


# ----------------------------------------------------------------- open / pulled flags ----
def flag_opens_and_pulls(conn):
    """'open' = first observed row per venue x market x line x side x event. 'pulled' = a venue's last row for a market is more than
    CLOSE_WINDOW + 2 cadences before the start while another venue kept quoting (survivorship rule, spec §10)."""
    conn.execute("""UPDATE ticks SET flags = CASE WHEN flags IS NULL THEN 'open' ELSE flags || ',open' END WHERE rowid IN (
        SELECT MIN(rowid) FROM ticks GROUP BY event_id, market, line, side, venue)""")
    conn.execute("""CREATE TEMP TABLE lastq AS SELECT event_id, market, venue, MAX(ts_utc) AS last_ts FROM ticks GROUP BY event_id, market, venue""")
    conn.execute("""CREATE TEMP TABLE mkt_last AS SELECT event_id, market, MAX(last_ts) AS mkt_ts FROM lastq GROUP BY event_id, market""")
    conn.execute("""UPDATE ticks SET flags = CASE WHEN flags IS NULL THEN 'pulled' ELSE flags || ',pulled' END WHERE rowid IN (
        SELECT t.rowid FROM ticks t JOIN lastq l ON l.event_id=t.event_id AND l.market=t.market AND l.venue=t.venue AND l.last_ts=t.ts_utc
        JOIN mkt_last m ON m.event_id=t.event_id AND m.market=t.market
        JOIN events e ON e.event_id=t.event_id
        WHERE (julianday(m.mkt_ts) - julianday(l.last_ts)) * 1440 > 60 AND julianday(e.start_utc) > julianday(l.last_ts))""")


def main():
    t0 = time.time()
    if os.path.exists(DB): os.remove(DB)
    conn = sqlite3.connect(DB); conn.executescript(open(os.path.join(HERE, 'schema.sql')).read())
    counts = {}
    counts['pinnacle_games'] = load_pinnacle_games()
    counts['an_games'] = load_an_games()
    counts['kalshi_games'] = load_kalshi_games(); EV.save()
    counts['kalshi_props'] = load_kalshi_props()
    counts['novig'] = load_novig()
    counts['an_props'] = load_an_props()
    # player -> event map from exchange prop rows (for Pinnacle props, which carry no game id)
    pe = {}
    for r in T.rows:
        if r[7] == 'prop' and r[8]:
            e = EV.ev.get(r[4]); pe.setdefault((e['et_date'], r[8]), r[4]) if e else None
    counts['pinnacle_props'] = load_pinnacle_props(pe)
    for pk, e in EV.ev.items():
        e['venue_ids'] = json.dumps({k: sorted(v) for k, v in EV.vids.get(pk, {}).items()})
    conn.executemany("INSERT OR REPLACE INTO events VALUES (:event_id,:sport,:league,:et_date,:start_utc,:away,:home,:status,:venue_ids)", list(EV.ev.values()))
    conn.executemany("INSERT INTO ticks VALUES (" + ','.join('?' * 26) + ")", T.rows)
    conn.execute("UPDATE ticks SET start_utc = (SELECT start_utc FROM events e WHERE e.event_id = ticks.event_id)")
    flag_opens_and_pulls(conn)
    conn.execute("DELETE FROM log_health")
    conn.execute("""INSERT INTO log_health SELECT e.et_date, t.venue, t.mclass, COUNT(*), COUNT(DISTINCT t.event_id), MIN(t.ts_utc), MAX(t.ts_utc)
                    FROM ticks t JOIN events e ON e.event_id=t.event_id GROUP BY 1,2,3""")
    conn.commit()
    EV.save(); KM.save()
    nt = conn.execute("SELECT COUNT(*) FROM ticks").fetchone()[0]; ne = conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]
    print(f"[build] {nt} ticks, {ne} events in {time.time() - t0:.1f}s; source rows: {counts}")
    if EV.unmatched: print(f"[build] unmatched game keys: {dict(EV.unmatched.most_common(8))}")
    conn.close()


if __name__ == '__main__':
    main()
