#!/usr/bin/env python3
"""Paired archive for October games: sheet total / close / result, one row per postseason game (Fable 10/14 midpoint check).

Per game (gameType F, D, L, W from MLB StatsAPI, 2026 postseason):
  sheet      data/archive/<date>/sheet_projections.json   Sean's sheet number (locked at lineups): total, away/home runs, home_wp
  model      data/archive/<date>/pregame_inputs.json       internal model total (for reference only)
  close      Pinnacle: last data/game_lines/pinnacle_log.jsonl snapshot before first pitch (main total, no-vig over prob; ML no-vig). Log starts 10/8;
             used as THE close only if within 120 min of first pitch (pin_minutes_before), else AN consensus.
             books:    data/archive/<date>/closing_odds.json (best posted price per side across books, so its "no-vig" runs low on hold; use Pinnacle where present)
  close      AN consensus: data/odds_archive/<date>.json consensus total + both sides (used when Pinnacle is missing)
  open       data/odds_archive open total, else data/archive/<date>/full_odds_open.json
  result     MLB StatsAPI linescore: final runs (9-inning and full), F5 runs, extra innings flag
Writes data/october_paired.csv and data/october_paired.json. Read-only everywhere; safe to re-run (rebuilds from scratch).
"""
import json, os, csv, datetime as dt, urllib.request, time
HERE = os.path.dirname(os.path.abspath(__file__)); D = os.path.join(HERE, '..', 'data')
MLB = "https://statsapi.mlb.com/api/v1"
START, END = os.environ.get('OCT_FROM', '2026-09-28'), os.environ.get('OCT_TO', dt.date.today().isoformat())

def g(u):
    for i in range(4):
        try: return json.load(urllib.request.urlopen(urllib.request.Request(u, headers={'User-Agent': 'Mozilla/5.0'}), timeout=40))
        except Exception: time.sleep(1.5 * (i + 1))
    return {}

def load(p, default=None):
    try: return json.load(open(p))
    except Exception: return default

def imp(o): o = float(o); return 100 / (o + 100) if o > 0 else -o / (-o + 100)
def novig(a, b):
    try: pa, pb = imp(a), imp(b); return pa / (pa + pb)
    except Exception: return None

def pin_closes():
    """game key (AWAY@HOME|YYYY-MM-DDTHH) -> last pregame Pinnacle record per market."""
    out = {}; p = os.path.join(D, 'game_lines', 'pinnacle_log.jsonl')
    if not os.path.exists(p): return out
    for l in open(p):
        try: r = json.loads(l)
        except Exception: continue
        out.setdefault(r['game'], {}).setdefault(r['mkt'], []).append(r)
    return out

def pin_at(PC, away, home, start):
    st = dt.datetime.fromisoformat(start.replace('Z', '+00:00'))
    for k, mk in PC.items():
        a_h, hr = k.split('|')
        if a_h != f'{away}@{home}' or abs((dt.datetime.fromisoformat(hr + ':00:00+00:00') - st).total_seconds()) > 3 * 3600: continue
        res = {}
        for m, rows in mk.items():
            pre = [r for r in rows if dt.datetime.fromisoformat(r['ts']) <= st]
            if pre: res[m] = max(pre, key=lambda r: r['ts'])
        return res
    return {}

FIX = {'CHW': 'CWS', 'WAS': 'WSH', 'AZ': 'ARI', 'OAK': 'ATH'}

def main():
    PC = pin_closes(); rows = []
    sch = g(f"{MLB}/schedule?sportId=1&startDate={START}&endDate={END}&gameType=F,D,L,W&hydrate=team,linescore")
    for dd in sch.get('dates', []):
        for gm in dd['games']:
            if gm.get('status', {}).get('codedGameState') not in ('F', 'O'): continue   # final only
            pk = gm['gamePk']; start = gm['gameDate']
            ab = lambda s: FIX.get(gm['teams'][s]['team'].get('abbreviation'), gm['teams'][s]['team'].get('abbreviation'))
            away, home = ab('away'), ab('home')
            # archive day = ET date of first pitch
            et = (dt.datetime.fromisoformat(start.replace('Z', '+00:00')) - dt.timedelta(hours=4)).date().isoformat()
            A = os.path.join(D, 'archive', et)
            sp = ((load(os.path.join(A, 'sheet_projections.json'), {}) or {}).get('games') or {}).get(str(pk)) or {}
            pi = (((load(os.path.join(A, 'pregame_inputs.json'), {}) or {}).get('games') or {}).get(str(pk)) or {}).get('model') or {}
            co = next((x for x in ((load(os.path.join(A, 'closing_odds.json'), {}) or {}).get('games') or []) if x.get('game_pk') == pk), {})
            op = next((x for x in ((load(os.path.join(A, 'full_odds_open.json'), {}) or {}).get('games') or []) if x.get('game_pk') == pk), {}) \
                if isinstance((load(os.path.join(A, 'full_odds_open.json'), {}) or {}).get('games'), list) else {}
            oa = next((x for x in ((load(os.path.join(D, 'odds_archive', f'{et}.json'), {}) or {}).get('games') or []) if x.get('game_pk') == pk), {})
            cons = (oa.get('consensus') or {}); ctot = cons.get('total') or {}; otot = (oa.get('open') or {}).get('total') or {}
            ls = gm.get('linescore') or {}; inn = ls.get('innings') or []
            ar = sum((i.get('away') or {}).get('runs', 0) or 0 for i in inn); hr_ = sum((i.get('home') or {}).get('runs', 0) or 0 for i in inn)
            ar9 = sum((i.get('away') or {}).get('runs', 0) or 0 for i in inn[:9]); hr9 = sum((i.get('home') or {}).get('runs', 0) or 0 for i in inn[:9])
            f5 = sum(((i.get('away') or {}).get('runs', 0) or 0) + ((i.get('home') or {}).get('runs', 0) or 0) for i in inn[:5])
            ct = co.get('total') or {}; cml = co.get('moneyline') or {}
            P = pin_at(PC, away, home, start)
            pt = P.get('total') or {}; pml = P.get('ml') or {}
            r = {'date': et, 'game_pk': pk, 'game_type': gm.get('gameType'), 'series_game': gm.get('seriesGameNumber'), 'away': away, 'home': home,
                 'first_pitch': start,
                 'sheet_total': sp.get('total'), 'sheet_away': sp.get('away_runs'), 'sheet_home': sp.get('home_runs'), 'sheet_home_wp': sp.get('home_wp'),
                 'sheet_locked': sp.get('locked'), 'model_total': pi.get('total'), 'model_home_wp': pi.get('home_wp'),
                 'open_total': otot.get('line') if otot.get('line') is not None else ((op.get('total') or {}).get('over') or {}).get('line'),
                 'close_total_consensus': ctot.get('line'), 'close_over_novig_consensus': round(novig(ctot['over'], ctot['under']), 4) if ctot.get('over') is not None else None,
                 'close_home_novig_consensus': round(novig(cons['ml_home'], cons['ml_away']), 4) if cons.get('ml_home') is not None else None,
                 'close_total_books': (ct.get('over') or {}).get('line'),
                 'close_over_odds': (ct.get('over') or {}).get('odds'), 'close_under_odds': (ct.get('under') or {}).get('odds'),
                 'close_over_novig_books': novig((ct.get('over') or {}).get('odds'), (ct.get('under') or {}).get('odds'))
                     if (ct.get('over') or {}).get('line') == (ct.get('under') or {}).get('line') else None,
                 'close_total_pin': pt.get('line'), 'close_over_novig_pin': round(novig(pt['over'], pt['under']), 4) if pt.get('over') is not None else None,
                 'close_home_ml_books': (cml.get('home') or {}).get('odds'), 'close_away_ml_books': (cml.get('away') or {}).get('odds'),
                 'close_home_novig_books': novig((cml.get('home') or {}).get('odds'), (cml.get('away') or {}).get('odds')),
                 'close_home_novig_pin': round(novig(pml['home'], pml['away']), 4) if pml.get('home') is not None else None, 'pin_ts': pt.get('ts') or pml.get('ts'),
                 'pin_minutes_before': round((dt.datetime.fromisoformat(start.replace('Z', '+00:00')) - dt.datetime.fromisoformat(pt.get('ts') or pml.get('ts'))).total_seconds() / 60) if (pt.get('ts') or pml.get('ts')) else None,
                 'away_runs': ar, 'home_runs': hr_, 'total_runs': ar + hr_, 'total_runs_9': ar9 + hr9, 'extras': len(inn) > 9, 'f5_runs': f5,
                 'home_win': int(hr_ > ar)}
            pin_ok = r['close_total_pin'] is not None and (r.get('pin_minutes_before') or 999) <= 120   # a Pinnacle snapshot hours early is not a close
            src = next(((r[k], n) for k, n in (('close_total_pin' if pin_ok else '_', 'Pinnacle'), ('close_total_consensus', 'AN consensus'), ('close_total_books', 'best posted'))
                        if r.get(k) is not None), (None, None))
            cl = src[0]; r['close_total'] = cl; r['close_src'] = src[1]
            if cl is not None:
                r['result_vs_close'] = 'over' if r['total_runs'] > cl else ('under' if r['total_runs'] < cl else 'push')
                if r['sheet_total'] is not None: r['sheet_minus_close'] = round(r['sheet_total'] - cl, 2)
            if r['sheet_total'] is not None: r['actual_minus_sheet'] = round(r['total_runs'] - r['sheet_total'], 2)
            rows.append(r)
    rows.sort(key=lambda r: (r['first_pitch'], r['game_pk']))
    cols = list(dict.fromkeys(k for r in rows for k in r))
    with open(os.path.join(D, 'october_paired.csv'), 'w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=cols); w.writeheader(); [w.writerow(r) for r in rows]
    json.dump({'generated_at': dt.datetime.now(dt.timezone.utc).isoformat(), 'n': len(rows), 'games': rows}, open(os.path.join(D, 'october_paired.json'), 'w'), indent=1)
    print(f'[october_paired] {len(rows)} games; sheet {sum(r["sheet_total"] is not None for r in rows)}, close {sum(r.get("close_total") is not None for r in rows)}, '
          f'pinnacle {sum(r["close_total_pin"] is not None for r in rows)}')

if __name__ == '__main__':
    main()
