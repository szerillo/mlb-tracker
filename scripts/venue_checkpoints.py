#!/usr/bin/env python3
"""Cross-venue checkpoint log: Novig, Pinnacle, Circa, DraftKings, FanDuel at T-24h, T-6h and T-1h before first pitch.

Runs inside the 20-minute kalshi_oblog job. Each game gets ONE snapshot per checkpoint (write-once): the first run that lands
inside the checkpoint window. Rows go to data/venue_checkpoints/<game date ET>.jsonl; captured checkpoints are tracked in
data/venue_checkpoints/_done.json. Every row carries minutes_to_start so a late capture is visible, never hidden.

Per venue, game markets only: full-game ML / run line / total / team totals, and F5 ML / total.
  Pinnacle  guest arcadia API (same pull as game_lines_log.py), with max-risk limits
  DK, FD    Action Network scoreboard, NY book ids 1548 / 69 (same pull as game_lines_log.py)
  Novig     public exchange API: best resting price + size per outcome (no pregame fee). Totals keep the
            strikes within 1.5 runs of the main line; team totals all strikes
  Circa     no public feed (Action Network lists the book, id 78, but returns no prices for it). Logged as
            unavailable so the slot exists; fill it if a feed is added.
"""
import json, os, sys, time, datetime as dt, urllib.request, collections
HERE = os.path.dirname(os.path.abspath(__file__)); sys.path.insert(0, HERE)
import game_lines_log as GL

OUT = os.path.join(HERE, '..', 'data', 'venue_checkpoints'); os.makedirs(OUT, exist_ok=True)
DONE = os.path.join(OUT, '_done.json')
NOW = dt.datetime.now(dt.timezone.utc); ET = dt.timezone(dt.timedelta(hours=-4))
# checkpoint -> (target minutes before first pitch, capture window in minutes). The job runs every 20 min
# (10 AM to midnight ET), so windows are wide enough to always catch one run; minutes_to_start records the truth.
CP = {'24h': (24 * 60, 120), '6h': (6 * 60, 90), '1h': (60, 45)}
NOVIG = "https://api.novig.com/v3/public"
NOVIG_TYPES = {'MONEY': 'ml', 'MONEY_1H': 'f5_ml', 'TOTAL': 'total', 'TOTAL_1H': 'f5_total', 'TEAM_TOTAL': 'tt', 'SPREAD': 'rl'}


def ng(u):
    for i in range(5):
        try: return json.load(urllib.request.urlopen(urllib.request.Request(u, headers={'User-Agent': 'bartolo-logger'}), timeout=30))
        except urllib.error.HTTPError as e:
            if e.code == 429: time.sleep(2 * (i + 1)); continue
            return {}
        except Exception: time.sleep(1.5 * (i + 1))
    return {}


def novig_game(ev):
    """{market: ...} for one Novig event: best price + size per outcome."""
    out = collections.defaultdict(list)
    for m in ng(f"{NOVIG}/catalog/markets?event={ev['eventId']}&limit=2000").get('items', []):
        key = NOVIG_TYPES.get(m['marketType'])
        if not key or m.get('status') != 'OPEN': continue
        if key == 'rl' and abs(float(m.get('strike') or 0)) != 1.5: continue
        oc = {o['outcomeId']: o['name'] for o in m['outcomes']}
        book = ng(f"{NOVIG}/catalog/markets/{m['marketId']}/book?depth=5").get('orders', {})
        sides = {}
        for oid, L in book.items():
            ps = [(float(o['price']), o['qty']) for o in L]
            if ps:
                bp = max(p for p, _ in ps); sides[oc.get(oid, oid)] = {'best': bp, 'qty': sum(q for p, q in ps if p == bp)}
        out[key].append({'desc': m['description'], 'strike': m.get('strike'), 'book': sides})
    for k in ('total', 'f5_total'):   # keep strikes near the main line (the one closest to 50/50)
        L = [x for x in out.get(k, []) if x['book']]
        def gap(x):
            o = next((v['best'] for n, v in x['book'].items() if n.lower().startswith('over')), None)
            return abs((o if o is not None else 0) - 0.5)
        if L:
            main = min(L, key=gap)['strike']
            out[k] = [x for x in L if abs(float(x['strike']) - float(main)) <= 1.5]
    return dict(out)


def main():
    done = json.load(open(DONE)) if os.path.exists(DONE) else {}
    _, pin = GL.pinnacle()
    _, an = GL.action_network()
    games = {}
    for src in (an, pin):
        for k, v in src.items():
            games.setdefault(k, {'start': v['start'], 'away': v['away'], 'home': v['home']})
    due = []
    for k, v in games.items():
        try: st = dt.datetime.fromisoformat(v['start'].replace('Z', '+00:00').replace('.000', ''))
        except Exception: continue
        mins = (st - NOW).total_seconds() / 60
        for cp, (tgt, win) in CP.items():
            if tgt - win < mins <= tgt and cp not in done.get(k, {}):
                due.append((k, cp, mins, st))
    if not due:
        json.dump(done, open(DONE, 'w'), indent=1)   # always leave a file so the workflow's git add path exists
        print('[venue_cp] no checkpoint due'); return
    # Novig events, matched to the same game key
    nov = {}
    evs = [e for e in ng(f"{NOVIG}/catalog/events?league=MLB&status=OPEN_PREGAME&limit=500").get('items', []) if '@' in e['description']]
    for e in evs:
        aw, hm = [s.strip() for s in e['description'].split('@', 1)]
        a, h = GL.TEAM_AB.get(aw), GL.TEAM_AB.get(hm)
        if a and h:
            iso = dt.datetime.fromtimestamp(e['startsTs'] / 1000, dt.timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
            nov[GL.gkey(a, h, iso)] = e
    wrote = collections.Counter()
    for k, cp, mins, st in due:
        p = pin.get(k, {}); a = an.get(k, {}).get('books', {})
        ev = nov.get(k)
        row = {'ts': NOW.isoformat(), 'checkpoint': cp, 'minutes_to_start': round(mins, 1), 'game': k,
               'away': games[k]['away'], 'home': games[k]['home'], 'start': games[k]['start'],
               'venues': {
                   'Pinnacle': {m: x for m, x in p.items() if m not in ('start', 'away', 'home')} or None,
                   'DK': a.get('DK') or None,
                   'FD': a.get('FD') or None,
                   'Novig': novig_game(ev) if ev else None,
                   'Circa': None}}
        row['missing'] = [v for v, x in row['venues'].items() if not x]
        gd = st.astimezone(ET).date().isoformat()
        with open(os.path.join(OUT, f'{gd}.jsonl'), 'a') as f: f.write(json.dumps(row) + '\n')
        done.setdefault(k, {})[cp] = NOW.isoformat(); wrote[cp] += 1
        print(f"[venue_cp] {k} {cp} at T-{mins:.0f}m; missing: {', '.join(row['missing']) or 'none'}")
    # prune games more than 3 days old from the done index
    cutoff = (NOW - dt.timedelta(days=3)).strftime('%Y-%m-%dT%H')
    done = {k: v for k, v in done.items() if k.split('|')[-1] >= cutoff}
    json.dump(done, open(DONE, 'w'), indent=1)
    print('[venue_cp] wrote', dict(wrote))


if __name__ == '__main__':
    main()
