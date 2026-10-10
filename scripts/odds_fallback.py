#!/usr/bin/env python3
"""Keep data/odds.json populated for today's slate (ML, run line, totals, team totals).

Replaces the inline fallback steps in odds_refresh.yml and refresh.yml, which had a bug:
whenever refresh_odds.py skipped (game-aware schedule: no game within ~4h) and odds.json
still carried yesterday's date, the multi-source scraper ran and overwrote odds.json with
its EMPTY payload ({"source": "none", "games": []}). On one-game postseason days that wiped
ML / team-total odds (and every ML / TT edge) from early morning until ~4h before first pitch.

Order:
  1. odds.json already has today's date + moneylines        -> leave it.
  2. Force the primary Action Network pull (FORCE_RUN=1)     -> usually fixes it.
  3. Still nothing: multi-source scraper, then The Odds API (if ODDS_API_KEY set).
     Their output is written ONLY if it has moneylines; an empty payload never
     replaces a file that has games.
"""
import datetime as dt, json, os, subprocess, sys

HERE = os.path.dirname(os.path.abspath(__file__))
P = os.path.join(HERE, '..', 'data', 'odds.json')
PY = sys.executable


def today_et():
    return (dt.datetime.utcnow() - dt.timedelta(hours=4)).date().isoformat()


def load(path=P):
    try:
        with open(path) as f: return json.load(f)
    except Exception: return None


def has_ml(d):
    return bool(d and d.get('games') and any(((g.get('moneyline') or {}).get('away') or {}).get('odds') is not None
                                             or (g.get('moneyline') or {}).get('away') for g in d['games']))


def fresh(d):
    return bool(d and d.get('date') == today_et() and has_ml(d))


def age_min(d):
    try:
        t = dt.datetime.fromisoformat(str(d.get('generated_at')).replace('Z', '+00:00'))
        if t.tzinfo is None: t = t.replace(tzinfo=dt.timezone.utc)
        return (dt.datetime.now(dt.timezone.utc) - t).total_seconds() / 60
    except Exception: return 1e9


def try_stdout_source(script, env=None):
    """Run a fallback that prints a payload to stdout; adopt it only if it has moneylines."""
    try:
        out = subprocess.run([PY, os.path.join(HERE, script)], capture_output=True, text=True, timeout=240,
                             env={**os.environ, **(env or {})}).stdout
        d = json.loads(out)
    except Exception as e:
        print(f'[odds_fallback] {script} failed: {e}'); return False
    if not has_ml(d):
        print(f'[odds_fallback] {script} returned no moneylines; odds.json left untouched'); return False
    d.setdefault('date', today_et())
    with open(P, 'w') as f: json.dump(d, f, indent=2)
    print(f'[odds_fallback] wrote {len(d["games"])} games from {script}'); return True


def main():
    d = load()
    if fresh(d):
        print('[odds_fallback] odds.json is current; nothing to do'); return
    # 2. force the primary pull, at most every 15 min when it keeps coming back empty
    if d is None or d.get('date') != today_et() or age_min(d) >= 15:
        print('[odds_fallback] odds.json stale/empty; forcing Action Network pull')
        subprocess.run([PY, os.path.join(HERE, 'refresh_odds.py')], env={**os.environ, 'FORCE_RUN': '1'}, timeout=240)
        d = load()
        if fresh(d):
            print('[odds_fallback] primary pull restored odds.json'); return
        if d and d.get('date') == today_et() and not d.get('games'):
            print('[odds_fallback] primary reports no games today; leaving as-is'); return
    # 3. secondary sources, never overwriting with an empty payload
    if try_stdout_source('refresh_odds_multi.py'): return
    if os.environ.get('ODDS_API_KEY') and try_stdout_source('refresh_odds_oddsapi.py'): return
    print('[odds_fallback] no source had moneylines; odds.json unchanged')


if __name__ == '__main__':
    main()
