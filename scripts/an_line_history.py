#!/usr/bin/env python3
"""Save Action Network's full pregame line history for each game before it disappears.

AN keeps every price move per book (open to now) at /web/v2/markets/event/<id>/history, but wipes it once the game is
over. This runs in the 20-minute job: a game is saved once in its last 40 minutes before first pitch (re-saved once more
if that first save was more than 25 minutes out), so the file holds the complete pregame path, open to near first pitch.

Output: data/line_history/<game date ET>/<AN game id>.json
  {"an_id", "away", "home", "start", "saved_at", "books": {<book>: {<period>: {<market>: [{side, value, odds, history:[...]}]}}}}
Books: AN consensus (15) and open (30), DK (NY 1548 / 68), FD (NY 69), Caesars (1005 / 123), BetRivers (972 / 71),
Fanatics (2789), theScore (4621), BetMGM (75), bet365 (79). Pinnacle and Circa are not on AN.
"""
import json, os, sys, datetime as dt
HERE = os.path.dirname(os.path.abspath(__file__)); sys.path.insert(0, HERE)
import game_lines_log as GL

OUT = os.path.join(HERE, '..', 'data', 'line_history')
BOOKS = {'15': 'Consensus', '30': 'Open', '1548': 'DK', '68': 'DK', '69': 'FD', '1005': 'CZR', '123': 'CZR', '972': 'BetRivers',
         '71': 'BetRivers', '2789': 'Fanatics', '4621': 'theScore', '75': 'BetMGM', '79': 'bet365'}
ET = dt.timezone(dt.timedelta(hours=-4)); NOW = dt.datetime.now(dt.timezone.utc)


KEEP = {'moneyline': 'ml', 'total': 'total', 'spread': 'rl', 'core_bet_type_6_team_score': 'tt'}


def slim(per):
    """Main markets only; each outcome -> {side, value, odds, h: [[updated_at, odds, value], ...]} (about 1/4 the raw size)."""
    out = {}
    for period, mk in (per or {}).items():
        if period not in ('event', 'firstfiveinnings'): continue
        for typ, outs in (mk or {}).items():
            if typ not in KEEP: continue
            L = []
            for o in outs:
                if o.get('is_live'): continue
                L.append({'side': o.get('side'), 'value': o.get('value'), 'odds': o.get('odds'),
                          'h': [[x.get('updated_at'), x.get('odds'), x.get('value')] for x in (o.get('history') or [])]})
            if L: out.setdefault(period, {})[KEEP[typ]] = L
    return out


def main():
    saved = 0
    for d in (NOW.astimezone(ET).date(), NOW.astimezone(ET).date() + dt.timedelta(days=1)):
        sb = GL.g(f"https://api.actionnetwork.com/web/v2/scoreboard/mlb?bookIds=15&date={d.strftime('%Y%m%d')}")
        for gm in sb.get('games', []):
            st = dt.datetime.fromisoformat(gm['start_time'].replace('Z', '+00:00'))
            mins = (st - NOW).total_seconds() / 60
            if not (-15 <= mins <= 40): continue
            gd = st.astimezone(ET).date().isoformat(); fp = os.path.join(OUT, gd, f"{gm['id']}.json")
            if os.path.exists(fp):   # one save per game (keeps git history small); re-save only if the first one was early
                try:
                    if json.load(open(fp)).get('minutes_to_start', 99) <= 25: continue
                except Exception: pass
            h = GL.g(f"https://api.actionnetwork.com/web/v2/markets/event/{gm['id']}/history?bookIds={','.join(BOOKS)}")
            h2 = GL.g(f"https://api.actionnetwork.com/web/v2/markets/event/{gm['id']}/history")   # AN's default book set
            books = {}
            for src in (h2, h):
                for bid, per in (src or {}).items():
                    if bid in BOOKS and per: books[f"{BOOKS[bid]}#{bid}"] = slim(per)
            if not books: continue
            tm = {t['id']: t.get('abbr') for t in gm.get('teams', [])}
            os.makedirs(os.path.join(OUT, gd), exist_ok=True)
            json.dump({'an_id': gm['id'], 'away': tm.get(gm['away_team_id']), 'home': tm.get(gm['home_team_id']),
                       'start': gm['start_time'], 'saved_at': NOW.isoformat(), 'minutes_to_start': round(mins, 1), 'books': books},
                      open(os.path.join(OUT, gd, f"{gm['id']}.json"), 'w'), separators=(',', ':'))
            saved += 1
    os.makedirs(OUT, exist_ok=True)
    open(os.path.join(OUT, '.keep'), 'w').close()   # git add path always exists
    print(f'[line_history] saved {saved} games')


if __name__ == '__main__':
    main()
