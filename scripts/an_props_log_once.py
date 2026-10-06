"""Snapshot NY sportsbook player-prop prices (Action Network) + Koerner projections for today's MLB slate.
Appends to data/an_props_log/YYYY-MM-DD.jsonl. Gives us book-move history (which book moves first, steam timing) going forward."""
import json,time,urllib.request,datetime,os
BOOKS={'1548':'DK','1006':'FD','1005':'CZR','972':'BetRivers','2789':'Fanatics','4621':'theScore','939':'MGM'}
TYPES=['core_bet_type_37_strikeouts','core_bet_type_42_pitching_outs','core_bet_type_72_hits_allowed','core_bet_type_76_walks','core_bet_type_74_earned_runs',
       'core_bet_type_36_hits','core_bet_type_77_total_bases','core_bet_type_33_hr','core_bet_type_431_hits_runs_rbis','core_bet_type_34_rbi','core_bet_type_78_runs_scored']
def g(u):
    for i in range(5):
        try: return json.load(urllib.request.urlopen(urllib.request.Request(u,headers={'User-Agent':'Mozilla/5.0'}),timeout=40))
        except Exception: time.sleep(2*(i+1))
    return {}
et=datetime.datetime.utcnow()-datetime.timedelta(hours=4); D=et.strftime('%Y%m%d'); ts=int(time.time())
rows=[]
for i in range(0,len(TYPES),4):
    ch=TYPES[i:i+4]
    d=g(f"https://api.actionnetwork.com/web/v2/scoreboard/mlb/markets?bookIds={','.join(BOOKS)}&customPickTypes={','.join(ch)}&date={D}")
    pl={p['id']:p['full_name'] for p in d.get('players',[])}
    for bid,ev in (d.get('markets') or {}).items():
        if bid not in BOOKS: continue
        for typ,outs in (ev.get('event') or {}).items():
            for o in outs:
                rows.append(dict(ts=ts,src='book',book=BOOKS[bid],type=typ,game=o.get('event_id'),player=pl.get(o.get('player_id')),side=o.get('side'),line=o.get('value'),odds=o.get('odds')))
for typ in TYPES:
    x=g(f"https://api.actionnetwork.com/web/v2/leagues/8/projections/available?date={D}&stateCode=NY&customPickTypes={typ}&limit=1000")
    pn={p['id']:p.get('full_name') for p in x.get('players',[])}
    seen=set()
    for p in x.get('playerProps',[]):
        for l in p['lines']:
            k=(p['player_id'],l.get('value'))
            if l.get('projection') is None or k in seen: continue
            seen.add(k); rows.append(dict(ts=ts,src='koerner',type=typ,game=l.get('event_id'),player=pn.get(p['player_id']),line=l.get('value'),projection=l.get('projection')))
os.makedirs('data/an_props_log',exist_ok=True)
with open(f"data/an_props_log/{et.date().isoformat()}.jsonl",'a') as f:
    for r in rows: f.write(json.dumps(r)+'\n')
print('rows',len(rows))
