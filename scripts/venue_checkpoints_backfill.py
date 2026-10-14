#!/usr/bin/env python3
"""One-off: rebuild T-24h / T-6h / T-1h venue checkpoints for games already played, from our own logs.

Sources (whatever was logged; no new pulls):
  data/game_lines/pinnacle_log.jsonl   Pinnacle (with limits)          since 10/8
  data/game_lines/an_log.jsonl         DK / FD (+ Consensus, Open)     since 10/8
  data/novig_log/*.jsonl               Novig game markets              since 10/10
For each game and checkpoint, the LATEST snapshot at or before the target time is used; minutes_to_start records how
far before first pitch it really was (so a sparse log shows as T-9h instead of being passed off as T-6h). A checkpoint
with no snapshot within MAX_EARLY minutes before the target is left out for that venue.
Rows are tagged "backfilled": true and written to data/venue_checkpoints/<date>.jsonl, same shape as venue_checkpoints.py.
"""
import json, os, glob, collections, datetime as dt
ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'data')
OUT = os.path.join(ROOT, 'venue_checkpoints'); os.makedirs(OUT, exist_ok=True)
ET = dt.timezone(dt.timedelta(hours=-4)); NOW = dt.datetime.now(dt.timezone.utc)
CP = {'24h': 24 * 60, '6h': 6 * 60, '1h': 60}
MAX_EARLY = {'24h': 6 * 60, '6h': 4 * 60, '1h': 2 * 60}   # how stale a snapshot may be and still count for the checkpoint
NOVIG_TYPES = {'MONEY': 'ml', 'MONEY_1H': 'f5_ml', 'TOTAL': 'total', 'TOTAL_1H': 'f5_total', 'TEAM_TOTAL': 'tt', 'SPREAD': 'rl'}
TEAM_AB = None


def ts(s): return dt.datetime.fromisoformat(s.replace('Z', '+00:00'))


def load():
    pin = collections.defaultdict(lambda: collections.defaultdict(dict))     # game -> ts -> mkt -> rec
    for l in open(os.path.join(ROOT, 'game_lines', 'pinnacle_log.jsonl')):
        r = json.loads(l); pin[r['game']][r['ts']][r['mkt']] = {k: v for k, v in r.items() if k not in ('ts', 'game')}
    an = collections.defaultdict(lambda: collections.defaultdict(lambda: collections.defaultdict(dict)))   # game -> ts -> book -> mkts
    for l in open(os.path.join(ROOT, 'game_lines', 'an_log.jsonl')):
        r = json.loads(l)
        if r.get('book') not in ('DK', 'FD'): continue
        an[r['game']][r['ts']][r['book']].update({k: v for k, v in r.items() if k not in ('ts', 'game', 'book', 'period')})
    nov = collections.defaultdict(lambda: collections.defaultdict(lambda: collections.defaultdict(list)))  # (event,start) -> ts -> mkt -> rows
    for f in sorted(glob.glob(os.path.join(ROOT, 'novig_log', '2*.jsonl'))):
        for l in open(f):
            r = json.loads(l); k = NOVIG_TYPES.get(r.get('type'))
            if not k: continue
            nov[(r['event'], r['start'])][r['ts']][k].append({'desc': r['desc'], 'strike': r['strike'], 'book': r['book']})
    return pin, an, nov


def pick(series, start, cp):
    """latest snapshot key at or before start - target; returns (key, minutes_to_start) or (None, None)."""
    tgt = start - dt.timedelta(minutes=CP[cp]); best = None
    for k in series:
        t = ts(k) if isinstance(k, str) else dt.datetime.fromtimestamp(k, dt.timezone.utc)
        if t <= tgt and (tgt - t).total_seconds() / 60 <= MAX_EARLY[cp] and (best is None or t > best[1]): best = (k, t)
    return (best[0], round((start - best[1]).total_seconds() / 60, 1)) if best else (None, None)


def main():
    import sys; sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    import game_lines_log as GL
    pin, an, nov = load()
    novkey = {}
    for (desc, st) in nov:
        aw, hm = [s.strip() for s in desc.split('@', 1)]
        a, h = GL.TEAM_AB.get(aw), GL.TEAM_AB.get(hm)
        if a and h: novkey[GL.gkey(a, h, dt.datetime.fromtimestamp(st / 1000, dt.timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'))] = (desc, st)
    games = set(pin) | set(an)
    starts = {}   # exact scheduled first pitch from MLB, keyed like the logs (AWAY@HOME|UTC hour)
    for d in sorted({g.split('|')[1][:10] for g in games}):
        for dd in GL.g(f"https://statsapi.mlb.com/api/v1/schedule?sportId=1&date={d}&hydrate=team").get('dates', []):
            for gm in dd['games']:
                a_ = gm['teams']['away']['team'].get('abbreviation'); h_ = gm['teams']['home']['team'].get('abbreviation')
                fix = {'CHW': 'CWS', 'WAS': 'WSH', 'AZ': 'ARI', 'OAK': 'ATH'}
                k_ = GL.gkey(fix.get(a_, a_), fix.get(h_, h_), gm['gameDate'])
                starts[k_] = ts(gm['gameDate'])
    n = collections.Counter(); out = collections.defaultdict(list)
    for g in sorted(games):
        # start time: from the game key's UTC hour is too coarse; use the AN/Pinnacle latest.json start if present, else the key hour
        start = starts.get(g) or ts(g.split('|')[1] + ':00:00+00:00')
        if start > NOW: continue                                  # only games already played
        for cp in CP:
            row = {'ts': None, 'checkpoint': cp, 'game': g, 'backfilled': True, 'venues': {}, 'minutes_to_start': {}}
            k, m = pick(pin.get(g, {}), start, cp)
            row['venues']['Pinnacle'] = pin[g][k] if k else None; row['minutes_to_start']['Pinnacle'] = m
            k, m = pick(an.get(g, {}), start, cp)
            for b in ('DK', 'FD'):
                row['venues'][b] = (an[g][k].get(b) or None) if k else None; row['minutes_to_start'][b] = m if row['venues'][b] else None
            nk = novkey.get(g)
            if nk:
                k, m = pick(nov[nk], start, cp)
                row['venues']['Novig'] = dict(nov[nk][k]) if k else None; row['minutes_to_start']['Novig'] = m
            else:
                row['venues']['Novig'] = None; row['minutes_to_start']['Novig'] = None
            row['venues']['Circa'] = None
            row['missing'] = [v for v, x in row['venues'].items() if not x]
            if len(row['missing']) == 5: continue
            out[start.astimezone(ET).date().isoformat()].append(row)
            n[cp] += 1
    for d, rows in out.items():
        p = os.path.join(OUT, f'{d}.jsonl')
        keep = [l for l in open(p)] if os.path.exists(p) else []
        keep = [l for l in keep if not json.loads(l).get('backfilled')]   # idempotent: replace earlier backfill rows
        with open(p, 'w') as f:
            for r in rows: f.write(json.dumps(r) + '\n')
            for l in keep: f.write(l)
    print('[backfill] rows by checkpoint', dict(n), '| games', len(games))


if __name__ == '__main__':
    main()
