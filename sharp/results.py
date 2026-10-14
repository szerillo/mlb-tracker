#!/usr/bin/env python3
"""Results table (spec §4): final score, F5 score, first-inning runs, per-player box score for every event in the log that has finished.
Appends to data/sharp/results.jsonl (one row per event, written once) and loads the table into the SQLite view.
"""
import json, os, sys, sqlite3, time, urllib.request, datetime as dt
HERE = os.path.dirname(os.path.abspath(__file__)); sys.path.insert(0, os.path.dirname(HERE))
from sharp import norm as N

ROOT = os.path.abspath(os.path.join(HERE, '..')); SD = os.path.join(ROOT, 'data', 'sharp')
DB = os.path.join(SD, 'sharp.sqlite'); RES = os.path.join(SD, 'results.jsonl')
OFFLINE = os.environ.get('SHARP_OFFLINE') == '1'


def g(u, tries=4):
    for i in range(tries):
        try: return json.load(urllib.request.urlopen(urllib.request.Request(u, headers={'User-Agent': 'Mozilla/5.0'}), timeout=60))
        except Exception: time.sleep(1.5 * (i + 1))
    return {}


def pull(pk):
    x = g(f"https://statsapi.mlb.com/api/v1.1/game/{pk}/feed/live")
    gd, ld = x.get('gameData', {}), x.get('liveData', {})
    st = (gd.get('status') or {}).get('abstractGameState'); det = (gd.get('status') or {}).get('detailedState', '')
    if st != 'Final':
        if 'Postponed' in det or 'Cancelled' in det or 'Suspended' in det: return dict(event_id=str(pk), status='postponed', detail=det)
        return None
    ls = ld.get('linescore', {}); inn = ls.get('innings', [])
    f5h = sum((i.get('home') or {}).get('runs') or 0 for i in inn[:5]); f5a = sum((i.get('away') or {}).get('runs') or 0 for i in inn[:5])
    rfi = ((inn[0].get('home') or {}).get('runs') or 0) + ((inn[0].get('away') or {}).get('runs') or 0) if inn else None
    players = {}
    for side in ('home', 'away'):
        for pid, p in (ld.get('boxscore', {}).get('teams', {}).get(side, {}).get('players') or {}).items():
            name = (p.get('person') or {}).get('fullName'); key = N.player_key(name)
            if not key: continue
            b = (p.get('stats') or {}).get('batting') or {}; pt = (p.get('stats') or {}).get('pitching') or {}
            row = {}
            if b.get('plateAppearances') or b.get('atBats') is not None and b:
                row.update(H=b.get('hits', 0), R=b.get('runs', 0), RBI=b.get('rbi', 0), HR=b.get('homeRuns', 0), TB=b.get('totalBases', 0),
                           BK=b.get('strikeOuts', 0), BBB=b.get('baseOnBalls', 0), PA=b.get('plateAppearances', 0))
                row['HRR'] = row['H'] + row['R'] + row['RBI']
            if pt.get('numberOfPitches') or pt.get('outs') is not None and pt:
                ip = pt.get('inningsPitched', '0.0'); outs = pt.get('outs')
                if outs is None and ip: w, f = ip.split('.'); outs = int(w) * 3 + int(f)
                row.update(K=pt.get('strikeOuts', 0), OUTS=outs or 0, HA=pt.get('hits', 0), BBA=pt.get('baseOnBalls', 0), ER=pt.get('earnedRuns', 0),
                           pitches=pt.get('numberOfPitches', 0), started=1 if pt.get('gamesStarted') else 0)
            if row: players[key] = dict(name=name, team=side, mlbam=pid.replace('ID', ''), **row)
    teams = ls.get('teams', {})
    return dict(event_id=str(pk), status='final', actual_start=(gd.get('datetime') or {}).get('dateTime'),
                home_score=(teams.get('home') or {}).get('runs'), away_score=(teams.get('away') or {}).get('runs'),
                f5_home=f5h, f5_away=f5a, rfi=rfi, innings=len(inn), players=players)


def load_existing():
    out = {}
    if os.path.exists(RES):
        for l in open(RES):
            try: r = json.loads(l); out[r['event_id']] = r
            except Exception: pass
    return out


def main():
    conn = sqlite3.connect(DB); have = load_existing(); new = 0
    now = dt.datetime.now(dt.timezone.utc)
    for pk, start in conn.execute("SELECT event_id, start_utc FROM events").fetchall():
        if pk in have and have[pk]['status'] in ('final', 'postponed'): continue
        if N.parse_ts(start) > now - dt.timedelta(hours=2) or OFFLINE: continue
        r = pull(pk)
        if r:
            have[pk] = r; new += 1
            with open(RES, 'a') as f: f.write(json.dumps(r) + '\n')
    conn.execute("DELETE FROM results")
    conn.executemany("INSERT INTO results VALUES (?,?,?,?,?,?,?,?,?)",
                     [(r['event_id'], r['status'], r.get('actual_start'), r.get('home_score'), r.get('away_score'), r.get('f5_home'), r.get('f5_away'),
                       r.get('rfi'), json.dumps(r.get('players') or {})) for r in have.values()])
    conn.execute("UPDATE events SET status='final' WHERE event_id IN (SELECT event_id FROM results WHERE status='final')")
    conn.commit(); conn.close()
    print(f"[results] {new} new, {sum(1 for r in have.values() if r['status']=='final')} finals on file")


if __name__ == '__main__':
    main()
