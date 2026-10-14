#!/usr/bin/env python3
"""Fill log with the sharp ruler captured AT FILL and AT CLOSE (Bartolo 10/14, spec from the grader).

One CSV, data/fills.csv, one row per ticket. Each row carries five prices: the book price, Kalshi best bid/ask on the matching rung,
and the Pinnacle / Novig no-vig fair, all at fill time; then the same at close; then the result. That is what grades CLV against the
sharp ruler and CLV-to-result conversion, split over/under, once there are 100+ rows.

Usage
  add:    python3 fill_log.py add DATE GAME MARKET "PLAYER or TEAM" SIDE LINE BOOK PRICE STAKE "RULE" ["NOTE"]
            DATE    2026-10-14
            GAME    MIL@LAD            (away@home, MLB abbreviations)
            MARKET  K | OUTS | HA | BB | H | TB | HR | HRR | RBI | TOTAL | TT | F5 | RFI | ML
            PLAYER  "Robert Gasser" (props), team abbr (TT, ML), or - (TOTAL, F5, RFI)
            SIDE    over | under       (ML: the team is the side, pass over)
            LINE    11.5               (ML: 0)
            BOOK    DK | FD | CZR | BetRivers | Fanatics | Kalshi | Novig | Pinnacle | ...
            PRICE   -120               (American; for Kalshi/Novig pass the cents you paid, e.g. 0.55)
            STAKE   0.2u  or  "to win 0.25u"
            RULE    the scan rule name at fill, or "discretionary"
          e.g. python3 fill_log.py add 2026-10-14 MIL@LAD OUTS "Robert Gasser" over 11.5 DK -120 "to win 0.2u" "sim3 hazard v4"
  close:  python3 fill_log.py close            (fills close columns + result for every row whose game has started)
  void:   python3 fill_log.py void ROW_ID       (player did not play, etc.)

Ruler at fill:  Kalshi open market (best bid/ask on the rung), Pinnacle guest API (devigged), Novig public API (devigged best prices).
Ruler at close: Kalshi last pregame candle (bid/ask), Pinnacle from pinnacle_log.jsonl (last snapshot before first pitch, written by prop_scan.py),
                Novig from data/novig_log/ (last snapshot before first pitch, written by novig_log_once.py). Result from Kalshi settlement, else from the box score.
Nothing here places an order. Read-only everywhere.
"""
import sys,os,json,csv,re,time,datetime as dt,unicodedata,urllib.request,glob,collections
HERE=os.path.dirname(os.path.abspath(__file__)); ROOT=os.path.abspath(os.path.join(HERE,'..'))
CSVF=os.path.join(ROOT,'data','fills.csv'); os.makedirs(os.path.dirname(CSVF),exist_ok=True)
KAPI="https://api.elections.kalshi.com/trade-api/v2"; PA="https://guest.api.arcadia.pinnacle.com/0.1"; NA="https://api.novig.com/v3/public"; MLB="https://statsapi.mlb.com/api/v1"
COLS=['id','date','game','market','player','side','line','book','book_price','stake','rule','note','fill_ts',
      'kalshi_ticker','kalshi_side','k_bid_fill','k_ask_fill','pin_fair_fill','pin_price_fill','novig_fair_fill','novig_price_fill',
      'book_price_close','k_bid_close','k_ask_close','pin_fair_close','novig_fair_close','close_ts','result','actual','settled_by']
nz=lambda s: unicodedata.normalize('NFKD',s or '').encode('ascii','ignore').decode().lower().replace('.','').replace(' jr','').replace("'",'').strip()
def g(u,tries=4):
    for i in range(tries):
        try: return json.load(urllib.request.urlopen(urllib.request.Request(u,headers={'User-Agent':'Mozilla/5.0'}),timeout=40))
        except urllib.error.HTTPError as e:
            if e.code==429: time.sleep(2*(i+1)); continue
            return None
        except Exception: time.sleep(1.5*(i+1))
    return None
def amer_to_p(a):
    a=float(a); return 100/(a+100) if a>0 else -a/(-a+100)
def devig(a,b):
    ia,ib=amer_to_p(a),amer_to_p(b); lo,hi=0.5,3.0
    for _ in range(60):
        k=(lo+hi)/2
        if ia**k+ib**k>1: lo=k
        else: hi=k
    return ia**k
SER={'K':'KXMLBKS','OUTS':'KXMLBOUTS','HA':'KXMLBHA','BB':'KXMLBWA','H':'KXMLBHIT','TB':'KXMLBTB','HR':'KXMLBHR','HRR':'KXMLBHRR','RBI':'KXMLBRBI',
     'TOTAL':'KXMLBTOTAL','TT':'KXMLBTEAMTOTAL','F5':'KXMLBF5TOTAL','RFI':'KXMLBRFI','ML':'KXMLBGAME'}
PSTAT={'K':'Total Strikeouts','OUTS':'Total Pitching Outs','HA':'Total Hits Allowed','BB':'Total Walks Allowed','TB':'Total Bases','HR':'Total Home Runs','H':'Total Hits'}
NTYPE={'K':'PITCHER_STRIKEOUTS','OUTS':'PITCHER_OUTS','HA':'HITS_ALLOWED','BB':'WALKS','H':'HITS','TB':'TOTAL_BASES','HRR':'HITS_RUNS_RBIS','HR':'HOME_RUNS','RBI':'RBIS',
       'TOTAL':'TOTAL','TT':'TEAM_TOTAL','F5':'TOTAL_1H','RFI':'FIRST_INNING_TOTAL','ML':'MONEY'}
def load():
    if not os.path.exists(CSVF): return []
    return list(csv.DictReader(open(CSVF)))
def save(rows):
    w=csv.DictWriter(open(CSVF,'w',newline=''),fieldnames=COLS); w.writeheader()
    for r in rows: w.writerow({c:r.get(c,'') for c in COLS})
# ---------- Kalshi ----------
def kalshi_rung(date,game,market,player,side,line,status='open'):
    """Return (ticker, kalshi_side, title, bid, ask) for the rung matching this ticket. kalshi_side is YES/NO for the ticket's side."""
    ser=SER[market]; away,home=game.split('@'); code=date[2:4]+['JAN','FEB','MAR','APR','MAY','JUN','JUL','AUG','SEP','OCT','NOV','DEC'][int(date[5:7])-1]+date[8:10]
    ms=[]; cur=None
    while True:
        d=g(f"{KAPI}/markets?series_ticker={ser}&status={status}&limit=1000"+(f"&cursor={cur}" if cur else "")) or {}
        ms+=[m for m in d.get('markets',[]) if code in m['event_ticker'] and away in m['event_ticker'] and home in m['event_ticker']]
        cur=d.get('cursor')
        if not cur or not d.get('markets'): break
    # strike: Kalshi rungs are "N+" = over N-0.5 ; line 11.5 over -> rung 12+ ; under 11.5 -> NO on 12+
    th=int(float(line)+0.5)
    cand=[]
    for m in ms:
        t=m['ticker']; tail=t.split('-')[-1]; title=m.get('title','')
        if market in ('TOTAL','F5','RFI'):
            if market=='RFI' or tail==str(th): cand.append(m)
        elif market=='ML':
            if tail==player: cand.append(m)
        elif market=='TT':
            if tail.startswith(player) and tail.endswith(str(th)) or (player in t and tail==str(th)): cand.append(m)
        else:
            tn=nz(title.split(':')[0]); pn=nz(player)
            if tail==str(th) and pn.split()[-1] in tn and (tn[:1]==pn[:1] or tn.split()[-1]!=pn.split()[-1]): cand.append(m)
    if not cand: return None
    if len(cand)>1:   # same last name twice in one game (Hernandez): the first initial decides; if still ambiguous, give up rather than guess
        c2=[m for m in cand if nz(m.get('title','').split(':')[0])[:1]==nz(player)[:1]]
        if len(c2)!=1: return None
        cand=c2
    m=cand[0]; kside='YES' if side=='over' else 'NO'
    bid=m.get('yes_bid_dollars') or m.get('yes_bid'); ask=m.get('yes_ask_dollars') or m.get('yes_ask')
    bid=float(bid) if bid is not None else None; ask=float(ask) if ask is not None else None
    if bid is not None and bid>1: bid/=100
    if ask is not None and ask>1: ask/=100
    if kside=='NO' and bid is not None and ask is not None: bid,ask=1-ask,1-bid
    bid=round(bid,3) if bid is not None else None; ask=round(ask,3) if ask is not None else None
    return dict(ticker=m['ticker'],kside=kside,title=m.get('title'),bid=bid,ask=ask,result=m.get('result'),event=m['event_ticker'])
def kalshi_close(ticker,kside,start_ts):
    d=g(f"{KAPI}/markets/candlesticks?market_tickers={ticker}&start_ts={start_ts-30*3600}&end_ts={start_ts}&period_interval=60") or {}
    for mk in d.get('markets',[]):
        cs=[c for c in mk.get('candlesticks',[]) if c['end_period_ts']<=start_ts]
        if not cs: return None,None
        c=cs[-1]; b=c['yes_bid'].get('close_dollars'); a=c['yes_ask'].get('close_dollars')
        b=float(b) if b else None; a=float(a) if a else None
        if kside=='NO' and b is not None and a is not None: b,a=1-a,1-b
        return b,a
    return None,None
# ---------- Pinnacle ----------
def pinnacle_fair(player,market,side,line,game=None):
    if market not in PSTAT and market!='RFI': return None,None
    pm=g(f"{PA}/leagues/246/matchups?brandId=0") or []; pk=g(f"{PA}/leagues/246/markets/straight") or []
    km=collections.defaultdict(list)
    for k in pk: km[k['matchupId']].append(k)
    for m in pm:
        if market=='RFI':
            if m.get('type')=='matchup' and len(m.get('participants',[]))==2:
                for k in km[m['id']]:
                    if k['key']=='s;3;ou;0.5':
                        pr={x['designation']:x['price'] for x in k['prices']}
                        if 'over' in pr and 'under' in pr:
                            names=' '.join(p['name'] for p in m['participants']).lower()
                            if game and all(w.lower() in names for w in []):  # team names not abbreviations; accept the first RFI matchup on the date is unsafe, so require caller to pass none
                                pass
            continue
        if m.get('type')=='special' and (m.get('special') or {}).get('category')=='Player Props':
            d=m['special']['description']
            if not d.endswith(PSTAT[market]): continue
            nm=nz(d[:-len(PSTAT[market])].strip())
            if nm!=nz(player): continue
            sd={p['id']:p['name'].lower() for p in m['participants']}
            for k in km[m['id']]:
                pr={sd.get(x['participantId']):x for x in k['prices']}
                if 'over' in pr and 'under' in pr and float(pr['over']['points'])==float(line):
                    po=devig(pr['over']['price'],pr['under']['price'])
                    return (po if side=='over' else 1-po),(pr['over']['price'] if side=='over' else pr['under']['price'])
    return None,None
def pinnacle_close_from_log(player,market,side,line,start_iso):
    # the scan writes pinnacle_log.jsonl into SCAN_DIR (data/prop_scan in the repo workflow) or next to itself
    cands=[os.path.join(ROOT,'data','prop_scan','pinnacle_log.jsonl'),os.path.join(ROOT,'scripts','pinnacle_log.jsonl'),os.path.join(HERE,'pinnacle_log.jsonl')]
    if os.environ.get('SCAN_DIR'): cands.insert(0,os.path.join(os.environ['SCAN_DIR'],'pinnacle_log.jsonl'))
    f=next((c for c in cands if os.path.exists(c)),None)
    if not f: return None
    best=None
    for l in open(f):
        try: r=json.loads(l)
        except: continue
        if nz(r.get('name'))!=nz(player) or r.get('stat')!=market or float(r.get('line',-1))!=float(line): continue
        if start_iso and r['ts']>start_iso: continue
        if best is None or r['ts']>best['ts']: best=r
    if not best: return None
    po=devig(best['over'],best['under']); return po if side=='over' else 1-po
# ---------- Novig ----------
def novig_fair(player,market,side,line,game):
    t=NTYPE.get(market)
    if not t: return None,None
    ev=g(f"{NA}/catalog/events?league=MLB&status=OPEN_PREGAME&limit=500") or {}
    away,home=game.split('@'); names={}
    for t in (g(f"{MLB}/teams?sportId=1") or {}).get('teams',[]): names[t.get('abbreviation')]=t.get('name','')
    for e in ev.get('items',[]):
        desc=e.get('description','')
        if '@' not in desc or names.get(away,'#') not in desc or names.get(home,'#') not in desc: continue
        ms=g(f"{NA}/catalog/markets?event={e['eventId']}&limit=2000") or {}
        for m in ms.get('items',[]):
            if m['marketType']!=t or m.get('status')!='OPEN': continue
            if market in PSTAT or market in ('HRR','RBI'):
                if nz(player).split()[-1] not in nz(m.get('description','')): continue
            if m.get('strike') is not None and float(m['strike'])!=float(line): continue
            b=g(f"{NA}/catalog/markets/{m['marketId']}/book?depth=5") or {}
            oc={o['outcomeId']:o['name'].lower() for o in m['outcomes']}; best={}
            for oid,L in (b.get('orders') or {}).items():
                ps=[float(o['price']) for o in L]
                if ps: best[oc.get(oid,oid)]=max(ps)
            # Novig prices are probabilities (cents) for each outcome; fair = normalized
            o=best.get('over'); u=best.get('under')
            if o is None or u is None: continue
            # best bid for over = what you can sell at; the ask for over = 1 - best bid for under
            ask_over=1-u; ask_under=1-o; fair_over=(ask_over+o)/2
            return (fair_over if side=='over' else 1-fair_over),(ask_over if side=='over' else ask_under)
    return None,None
def novig_close_from_log(player,market,side,line,start_ts):
    t=NTYPE.get(market); best=None
    for f in sorted(glob.glob(os.path.join(ROOT,'data','novig_log','*.jsonl'))):
        for l in open(f):
            try: r=json.loads(l)
            except: continue
            if r.get('type')!=t or (r.get('strike') is not None and float(r['strike'])!=float(line)): continue
            if (market in PSTAT or market in ('HRR','RBI')) and nz(player).split()[-1] not in nz(r.get('desc','')): continue
            if r['ts']>start_ts: continue
            if best is None or r['ts']>best['ts']: best=r
    if not best: return None
    bk=best.get('book') or {}; o=(bk.get('Over') or bk.get('over') or {}); u=(bk.get('Under') or bk.get('under') or {})
    if not o or not u: return None
    fo=((1-u['best'])+o['best'])/2; return fo if side=='over' else 1-fo
# ---------- MLB ----------
def game_start(date,game):
    away,home=game.split('@')
    d=g(f"{MLB}/schedule?sportId=1&date={date}&hydrate=team") or {}
    for dd in d.get('dates',[]):
        for gm in dd['games']:
            if gm['teams']['away']['team'].get('abbreviation')==away and gm['teams']['home']['team'].get('abbreviation')==home:
                return gm['gamePk'],gm['gameDate'],gm['status']['abstractGameState']
    return None,None,None
def box_actual(pk,market,player,game):
    bx=g(f"{MLB}/game/{pk}/boxscore") or {}; ls=g(f"{MLB}/game/{pk}/linescore") or {}
    if market in ('TOTAL','F5','RFI','TT','ML'):
        inn=ls.get('innings',[]); tr=ls.get('teams',{})
        a=tr.get('away',{}).get('runs',0); h=tr.get('home',{}).get('runs',0)
        if market=='TOTAL': return a+h
        if market=='F5': return sum((i['away'].get('runs') or 0)+(i['home'].get('runs') or 0) for i in inn[:5])
        if market=='RFI': return (inn[0]['away'].get('runs') or 0)+(inn[0]['home'].get('runs') or 0) if inn else None
        away,home=game.split('@')
        if market=='TT': return a if player==away else h
        if market=='ML': return 1 if (player==home and h>a) or (player==away and a>h) else 0
    for s in ('away','home'):
        for k,p in bx.get('teams',{}).get(s,{}).get('players',{}).items():
            if nz(p['person']['fullName'])!=nz(player): continue
            b=p['stats'].get('batting',{}); pi=p['stats'].get('pitching',{})
            if market in ('K','OUTS','HA','BB'):
                if not pi.get('inningsPitched'): return 'DNP'
                ip=pi['inningsPitched']; outs=int(ip.split('.')[0])*3+int(ip.split('.')[1])
                return {'K':pi.get('strikeOuts'),'OUTS':outs,'HA':pi.get('hits'),'BB':pi.get('baseOnBalls')}[market]
            if not b.get('plateAppearances'): return 'DNP'
            return {'H':b.get('hits'),'TB':b.get('totalBases'),'HR':b.get('homeRuns'),'RBI':b.get('rbi'),'HRR':(b.get('hits') or 0)+(b.get('runs') or 0)+(b.get('rbi') or 0)}[market]
    return None
def grade(side,line,actual):
    if actual in (None,'DNP'): return 'VOID' if actual=='DNP' else ''
    a=float(actual); L=float(line)
    if a==L: return 'PUSH'
    return 'WIN' if (a>L)==(side=='over') else 'LOSS'
# ---------- commands ----------
def cmd_add(a):
    date,game,market,player,side,line,book,price,stake,rule=a[:10]; note=a[10] if len(a)>10 else ''
    market=market.upper(); side=side.lower(); rows=load()
    rid=f"{date}-{len(rows)+1:03d}"
    row=dict(id=rid,date=date,game=game,market=market,player=player,side=side,line=line,book=book,book_price=price,stake=stake,rule=rule,note=note,
             fill_ts=dt.datetime.now(dt.timezone.utc).isoformat(timespec='seconds'))
    try:
        _pk,_start,_state=game_start(date,game)
        # lineup check: a hitter ticket on a player who is not in the posted lineup is no action (Hernandez 10/14)
        if _pk and market in ('H','TB','HR','HRR','RBI') and player not in ('-',''):
            _lu={}
            for _dd in (g(f"{MLB}/schedule?sportId=1&date={date}&hydrate=lineups") or {}).get('dates',[]):
                for _gm in _dd['games']:
                    if _gm['gamePk']==_pk: _lu=_gm.get('lineups') or {}
            _names=[nz(p['fullName']) for k in ('awayPlayers','homePlayers') for p in _lu.get(k,[])]
            _pn=nz(player); _ok=lambda n: n==_pn or (n.split()[-1]==_pn.split()[-1] and n[:1]==_pn[:1])
            if _names and not any(_ok(n) for n in _names):
                row['note']=(row.get('note') or '')+('; ' if row.get('note') else '')+'WARNING: not in the posted lineup'; print('WARNING: %s is not in the posted lineup (posted: %d names). Ticket is likely no action.'%(player,len(_names)))
            elif not _names: print('note: lineups not posted yet; re-check the player is starting')
        if _state and _state!='Preview':
            row['note']=(note+'; ' if note else '')+'LATE: logged after first pitch, fill-time ruler is not pregame'; print('WARNING: game already started; the grader needs the rule tag and ruler BEFORE first pitch')
    except Exception: pass
    k=None
    try: k=kalshi_rung(date,game,market,player,side,line)
    except Exception as e: print('kalshi lookup failed:',e)
    if k: row.update(kalshi_ticker=k['ticker'],kalshi_side=k['kside'],k_bid_fill=k['bid'],k_ask_fill=k['ask']); print('Kalshi',k['title'],k['kside'],'bid',k['bid'],'ask',k['ask'])
    else: print('Kalshi: rung not listed (or name/line mismatch); left blank')
    try:
        pf,pp=pinnacle_fair(player,market,side,line,game)
        if pf is not None: row.update(pin_fair_fill=round(pf,4),pin_price_fill=pp); print('Pinnacle fair %.3f (%+d)'%(pf,pp))
        else: print('Pinnacle: not offered')
    except Exception as e: print('pinnacle failed:',e)
    try:
        nf,np_=novig_fair(player,market,side,line,game)
        if nf is not None: row.update(novig_fair_fill=round(nf,4),novig_price_fill=round(np_,3)); print('Novig fair %.3f (ask %.3f)'%(nf,np_))
        else: print('Novig: not offered')
    except Exception as e: print('novig failed:',e)
    rows.append(row); save(rows); print('logged',rid)
def cmd_close():
    rows=load(); n=0
    for r in rows:
        if r.get('result') or r.get('close_ts'): continue
        pk,start,state=game_start(r['date'],r['game'])
        if not pk or state=='Preview': continue
        st=int(dt.datetime.fromisoformat(start.replace('Z','+00:00')).timestamp())
        if r.get('kalshi_ticker'):
            b,a=kalshi_close(r['kalshi_ticker'],r['kalshi_side'],st)
            r['k_bid_close']=b; r['k_ask_close']=a
            m=g(f"{KAPI}/markets/{r['kalshi_ticker']}") or {}
            res=(m.get('market') or {}).get('result')
            if res in ('yes','no'):
                r['result']='WIN' if res.upper()==r['kalshi_side'] else 'LOSS'; r['settled_by']='kalshi'
        pf=pinnacle_close_from_log(r['player'],r['market'],r['side'],r['line'],start)
        if pf is not None: r['pin_fair_close']=round(pf,4)
        nf=novig_close_from_log(r['player'],r['market'],r['side'],r['line'],st)
        if nf is not None: r['novig_fair_close']=round(nf,4)
        if state=='Final':
            act=box_actual(pk,r['market'],r['player'],r['game']); r['actual']=act
            if not r.get('result'):
                r['result']=grade(r['side'],r['line'],act) if r['market']!='ML' else ('WIN' if act==1 else 'LOSS'); r['settled_by']='box'
            elif act=='DNP': r['result']='VOID'
        r['close_ts']=dt.datetime.now(dt.timezone.utc).isoformat(timespec='seconds'); n+=1
        print(r['id'],r['player'],r['market'],r['side'],r['line'],'| close K',r.get('k_bid_close'),r.get('k_ask_close'),'| pin',r.get('pin_fair_close'),'| novig',r.get('novig_fair_close'),'|',r.get('result'),r.get('actual'))
    save(rows); print('closed',n,'rows')
def cmd_void(rid):
    rows=load()
    for r in rows:
        if r['id']==rid: r['result']='VOID'
    save(rows)
if __name__=='__main__':
    a=sys.argv[1:]
    if not a or a[0] not in ('add','close','void'): print(__doc__); sys.exit(1)
    if a[0]=='add': cmd_add(a[1:])
    elif a[0]=='close': cmd_close()
    else: cmd_void(a[1])
