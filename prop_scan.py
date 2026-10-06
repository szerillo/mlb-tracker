"""BARTOLO daily Kalshi prop scan: rule triggers + order flow + liquidity watch.
Usage: python3 prop_scan.py [YYYY-MM-DD ...]   (default: today and tomorrow, ET)
Writes scan_<stamp>.md (watchlist), appends scan_log.jsonl (every trigger, for grading), keeps state/ for 'since last scan' flow.
Public endpoints only (Kalshi, MLB StatsAPI, Action Network). Never places orders."""
import sys,os,json,time,math,datetime as dt,urllib.request,concurrent.futures as cf,collections,unicodedata,re
import numpy as np
from scipy import stats,optimize
API="https://api.elections.kalshi.com/trade-api/v2"; MLB="https://statsapi.mlb.com/api/v1"
HERE=os.environ.get('SCAN_DIR') or os.path.dirname(os.path.abspath(__file__)); os.makedirs(HERE,exist_ok=True); ST=os.path.join(HERE,'state'); os.makedirs(ST,exist_ok=True)
MON=['JAN','FEB','MAR','APR','MAY','JUN','JUL','AUG','SEP','OCT','NOV','DEC']
PSER={'KXMLBKS':'K','KXMLBOUTS':'OUTS','KXMLBHA':'HA','KXMLBWA':'BB'}
HSER={'KXMLBHIT':'H','KXMLBTB':'TB','KXMLBHR':'HR','KXMLBHRR':'HRR','KXMLBRBI':'RBI'}
GSER=['KXMLBRFI','KXMLBTOTAL','KXMLBF5TOTAL']
BIG=1000; MOVE=0.05; IMB=3.0; MINDEP=200
fee=lambda p:0.07*p*(1-p)
nz=lambda s: unicodedata.normalize('NFKD',s or '').encode('ascii','ignore').decode().lower().replace('.','').replace(' jr','').replace("'",'').strip()
def g(u,hdr=None):
    for i in range(6):
        try: return json.load(urllib.request.urlopen(urllib.request.Request(u,headers=hdr or {'User-Agent':'Mozilla/5.0'}),timeout=40))
        except urllib.error.HTTPError as e:
            if e.code==404: return {}
            time.sleep(1.5*(i+1))
        except Exception: time.sleep(1.5*(i+1))
    return {}
now=dt.datetime.now(dt.timezone.utc); et_now=now-dt.timedelta(hours=4)
dates=[dt.date.fromisoformat(x) for x in sys.argv[1:]] or [et_now.date(),et_now.date()+dt.timedelta(days=1)]
codes={d.strftime('%y')+MON[d.month-1]+d.strftime('%d'):d for d in dates}
# ---------- 1. markets ----------
def series_markets(s):
    out=[];cur=''
    while True:
        d=g(f"{API}/markets?series_ticker={s}&status=open&limit=1000"+(f"&cursor={cur}" if cur else ""))
        out+=[m for m in d.get('markets',[]) if any(c in m['event_ticker'] for c in codes)]; cur=d.get('cursor')
        if not cur or not d.get('markets'): break
    return [(s,m) for m in out]
MK=[]
with cf.ThreadPoolExecutor(6) as ex:
    for L in ex.map(series_markets,list(PSER)+list(HSER)+GSER): MK+=L
prev=json.load(open(os.path.join(ST,'last.json'))) if os.path.exists(os.path.join(ST,'last.json')) else {}
def one(sm):
    s,m=sm; t=m['ticker']
    ob=g(f"{API}/markets/{t}/orderbook").get('orderbook_fp') or {}
    yes=[(float(p),float(q)) for p,q in ob.get('yes_dollars') or []]; no=[(float(p),float(q)) for p,q in ob.get('no_dollars') or []]
    bb=max([p for p,_ in yes],default=0.0); nb=max([p for p,_ in no],default=0.0); ba=1-nb if nb else 1.0
    dep=lambda L,top,w: sum(q*p for p,q in L if p>=top-w+1e-9)
    since=prev.get(t,{}).get('last_ts','1970')
    tr=[];cur=''
    for _ in range(30):
        d=g(f"{API}/markets/trades?ticker={t}&limit=1000"+(f"&cursor={cur}" if cur else "")); B=d.get('trades',[]); tr+=B; cur=d.get('cursor')
        if not cur: break
    new=[x for x in tr if x['created_time']>since]
    def flow(T):
        y=sum(float(x['count_fp'])*float(x['yes_price_dollars']) for x in T if x['taker_side']=='yes')
        n=sum(float(x['count_fp'])*(1-float(x['yes_price_dollars'])) for x in T if x['taker_side']=='no'); return y,n
    big=[];big_all=[]
    for x in tr:
        q=float(x['count_fp']); p=float(x['yes_price_dollars']); side=x['taker_side'].upper(); notl=q*p if side=='YES' else q*(1-p)
        if notl>=BIG:
            b=dict(ts=x['created_time'][5:16],side=side,usd=round(notl),px=p); big_all.append(b)
            if x['created_time']>since: big.append(b)
    fy,fn=flow(tr); ny,nn=flow(new)
    return dict(series=s,ticker=t,event=m['event_ticker'],title=m.get('title',''),strike=m.get('floor_strike'),bid=bb,ask=ba,mid=(bb+ba)/2 if bb else None,
        dy3=dep(yes,bb,.03),dn3=dep(no,nb,.03),dy10=dep(yes,bb,.10),dn10=dep(no,nb,.10),vol=float(m.get('volume_fp') or 0),
        fy=fy,fn=fn,ny=ny,nn=nn,big=big,big_all=big_all,last_ts=tr[0]['created_time'] if tr else since,prev_mid=prev.get(t,{}).get('mid'))
with cf.ThreadPoolExecutor(8) as ex: R=list(ex.map(one,MK))
json.dump({r['ticker']:dict(mid=r['mid'],last_ts=r['last_ts']) for r in R},open(os.path.join(ST,'last.json'),'w'))
print('markets',len(R),'codes',list(codes))
# ---------- 2. pitchers: ladders + season stats ----------
pname=lambda r: r['title'].split(':')[0].strip()
lad=collections.defaultdict(list)
for r in R:
    if r['series'] in PSER and r['mid'] is not None: lad[(r['event'].split('-',1)[1],pname(r),PSER[r['series']])].append(r)
pp={}
for d in dates:
    s=g(f"{MLB}/schedule?sportId=1&date={d}&hydrate=probablePitcher")
    for dd in s.get('dates',[]):
        for gm in dd['games']:
            for side in ('away','home'):
                p=gm['teams'][side].get('probablePitcher')
                if p: pp[nz(p['fullName'])]=p['id']
def season(pid):
    d=g(f"{MLB}/people/{pid}/stats?stats=season&group=pitching&season=2026&gameType=R")
    try: s=d['stats'][0]['splits'][0]['stat']
    except Exception: return None
    gs=s.get('gamesStarted',0)
    if not gs: return None
    ip=s['inningsPitched']; outs=int(ip.split('.')[0])*3+int(ip.split('.')[1] if '.' in ip else 0)
    # starts-only approximation: season totals over starts (relief outs inflate it slightly for swingmen)
    return dict(gs=gs,g=s.get('gamesPlayed'),k_gs=s['strikeOuts']/gs if s.get('gamesPlayed')==gs else None,outs_gs=outs/gs if s.get('gamesPlayed')==gs else None,
                k_tot=s['strikeOuts'],outs_tot=outs)
def season_starts(pid):
    """Per-start averages from the game log (starts only)."""
    d=g(f"{MLB}/people/{pid}/stats?stats=gameLog&group=pitching&season=2026&gameType=R")
    try: L=d['stats'][0]['splits']
    except Exception: return None
    S=[x['stat'] for x in L if x['stat'].get('gamesStarted')==1]
    if not S: return None
    o=lambda s: int(s['inningsPitched'].split('.')[0])*3+int(s['inningsPitched'].split('.')[1])
    return dict(gs=len(S),k_gs=sum(s['strikeOuts'] for s in S)/len(S),outs_gs=sum(o(s) for s in S)/len(S),
                l5_outs=sum(o(s) for s in S[-5:])/min(5,len(S)),bb_pct=sum(s.get('baseOnBalls',0) for s in S)/max(1,sum(s.get('battersFaced',0) for s in S)),l5_pitches=sum(s.get('numberOfPitches',0) for s in S[-5:])/min(5,len(S)))
def ladder_mean(rs,kind):
    rs=sorted(rs,key=lambda r:r['strike']); th=np.array([r['strike']+0.5 for r in rs]); mid=np.clip(np.array([r['mid'] for r in rs]),.01,.99)
    if kind=='OUTS':   # normal with continuity correction
        if len(rs)<3:   # one or two rungs: fix the spread at the 2026 starter sd (3.5 outs) and solve the mean
            def loss1(p): return np.sum((stats.norm.sf(th-0.5,p[0],3.5)-mid)**2)
            return optimize.minimize(loss1,[float(th[0])],method='Nelder-Mead').x[0]
        def loss(p): return np.sum((stats.norm.sf(th-0.5,p[0],abs(p[1])+.5)-mid)**2)
        o=optimize.minimize(loss,[float(np.interp(.5,mid[::-1],th[::-1])),3.5],method='Nelder-Mead'); return o.x[0]
    def sf(par):
        mu,lr=par; rr=math.exp(lr); q=rr/(rr+mu); return stats.nbinom.sf(th-1,rr,q)
    def loss(par):
        if par[0]<=.1: return 1e9
        f=np.clip(sf(par),1e-4,1-1e-4); return np.sum((np.log(f/(1-f))-np.log(mid/(1-mid)))**2)
    mu0=float(np.interp(.5,mid[::-1],th[::-1]))
    return min((optimize.minimize(loss,[max(mu0,.5),lr],method='Nelder-Mead') for lr in (1.5,3.0)),key=lambda o:o.fun).x[0]
P={}
for (ev,nm,kind),rs in lad.items():
    if len(rs)<2 and kind!='OUTS': continue
    P.setdefault((ev,nm),{})[kind]=dict(mean=ladder_mean(rs,kind) if rs else None,rungs=sorted(rs,key=lambda r:r['strike']))
SS={}
for (ev,nm) in P:
    pid=pp.get(nz(nm))
    if pid: SS[(ev,nm)]=season_starts(pid)
# ---------- 3. Action Network best NY book prices for K / outs ----------
BOOKS={'1548':'DK','1006':'FD','1005':'CZR','972':'BetRivers','2789':'Fanatics','4621':'theScore'}
AT={'core_bet_type_37_strikeouts':'K','core_bet_type_42_pitching_outs':'OUTS','core_bet_type_72_hits_allowed':'HA','core_bet_type_76_walks':'BB'}
BK=collections.defaultdict(dict)
for d in dates:
    x=g(f"https://api.actionnetwork.com/web/v2/scoreboard/mlb/markets?bookIds={','.join(BOOKS)}&customPickTypes={','.join(AT)}&date={d.strftime('%Y%m%d')}")
    pl={p['id']:nz(p['full_name']) for p in x.get('players',[])}
    for bid,ev in (x.get('markets') or {}).items():
        if bid not in BOOKS: continue
        for typ,outs in ev.get('event',{}).items():
            if typ not in AT: continue
            for o in outs:
                nm=pl.get(o.get('player_id')); v=o.get('value'); od=o.get('odds'); sd=o.get('side')
                if not nm or v is None or od is None or sd not in('over','under') or not 100<=abs(od)<=500: continue
                k=(nm,AT[typ],float(v),sd); 
                if od>BK[k].get('odds',-9999): BK[k]=dict(odds=od,book=BOOKS[bid])
am=lambda p: f"{-100*p/(1-p):+.0f}" if p>=.5 else f"+{100*(1-p)/p:.0f}"
def best_book(nm,kind,line,side):
    b=BK.get((nz(nm),kind,line,side))
    return f"{b['book']} {b['odds']:+d}" if b else '-'
TEAM={'ARI':'Arizona Diamondbacks','ATL':'Atlanta Braves','BAL':'Baltimore Orioles','BOS':'Boston Red Sox','CHC':'Chicago Cubs','CWS':'Chicago White Sox',
 'CIN':'Cincinnati Reds','CLE':'Cleveland Guardians','COL':'Colorado Rockies','DET':'Detroit Tigers','HOU':'Houston Astros','KC':'Kansas City Royals',
 'LAA':'Los Angeles Angels','LAD':'Los Angeles Dodgers','MIA':'Miami Marlins','MIL':'Milwaukee Brewers','MIN':'Minnesota Twins','NYM':'New York Mets',
 'NYY':'New York Yankees','ATH':'Athletics','OAK':'Athletics','PHI':'Philadelphia Phillies','PIT':'Pittsburgh Pirates','SD':'San Diego Padres','SF':'San Francisco Giants',
 'SEA':'Seattle Mariners','STL':'St. Louis Cardinals','TB':'Tampa Bay Rays','TEX':'Texas Rangers','TOR':'Toronto Blue Jays','WSH':'Washington Nationals'}
# ---------- 3b. Pinnacle (public guest API used by pinnacle.com; read-only) ----------
PA="https://guest.api.arcadia.pinnacle.com/0.1"
PSTAT={'Total Strikeouts':'K','Total Pitching Outs':'OUTS','Total Hits Allowed':'HA','Total Earned Runs':'ER','Total Walks Allowed':'BB',
       'Total Bases':'TB','Total Home Runs':'HR','Total Hits':'H'}
def devig(a,b):
    ia=100/(a+100) if a>0 else -a/(-a+100); ib=100/(b+100) if b>0 else -b/(-b+100)
    lo,hi=0.5,3.0
    for _ in range(60):
        k=(lo+hi)/2
        if ia**k+ib**k>1: lo=k
        else: hi=k
    return ia**k   # power devig, probability of side a
PIN={}; PINRFI={}; PLOG=[]
try:
    pm=g(f"{PA}/leagues/246/matchups?brandId=0") or []; pk=g(f"{PA}/leagues/246/markets/straight") or []
    km=collections.defaultdict(list)
    for k in pk: km[k['matchupId']].append(k)
    for m in pm:
        if m.get('type')=='special' and (m.get('special') or {}).get('category')=='Player Props':
            d=m['special']['description']; stat=next((v for k,v in PSTAT.items() if d.endswith(k)),None)
            if not stat: continue
            nm=nz(d[:-len(next(k for k in PSTAT if d.endswith(k)))].strip()); side={p['id']:p['name'].lower() for p in m['participants']}
            for k in km[m['id']]:
                pr={side.get(x['participantId']):x for x in k['prices']}
                if 'over' in pr and 'under' in pr:
                    po=devig(pr['over']['price'],pr['under']['price']); line=pr['over']['points']
                    lim=next((l['amount'] for l in k.get('limits') or [] if l['type']=='maxRiskStake'),None)
                    PIN[(nm,stat,float(line))]=dict(over=po,o=pr['over']['price'],u=pr['under']['price'],limit=lim)
                    PLOG.append(dict(ts=now.isoformat(),name=nm,stat=stat,line=line,over=pr['over']['price'],under=pr['under']['price'],limit=lim,start=m.get('startTime')))
        elif m.get('type')=='matchup' and len(m.get('participants',[]))==2 and 'Runs' not in m['participants'][0]['name']:
            hm=next((p['name'] for p in m['participants'] if p.get('alignment')=='home'),None)
            for k in km[m['id']]:
                if k['key']=='s;3;ou;0.5':
                    pr={x['designation']:x['price'] for x in k['prices']}
                    PINRFI[(nz(hm),m['startTime'][:13])]=dict(yrfi=devig(pr['over'],pr['under']),o=pr['over'],u=pr['under'])
                    PLOG.append(dict(ts=now.isoformat(),name=hm,stat='RFI',line=0.5,over=pr['over'],under=pr['under'],start=m['startTime']))
except Exception as e: print('pinnacle fetch failed',e)
print('pinnacle props',len(PIN),'rfi',len(PINRFI))
with open(os.path.join(HERE,'pinnacle_log.jsonl'),'a') as f:
    for x in PLOG: f.write(json.dumps(x)+'\n')
def pin_fair(nm,kind,line,side):
    p=PIN.get((nz(nm),kind,float(line)))
    if not p: return '-'
    q=p['over'] if side=='over' else 1-p['over']
    return f"Pinnacle {side[0]}{line} {p['o'] if side=='over' else p['u']:+d} (fair {100*q:.1f}%)"
# ---------- 4. triggers ----------
TR=[]
def add(tier,rule,ev,what,side,price,fair,note,ticker=None):
    rd=lambda x: round(float(x),3) if x is not None else None
    TR.append(dict(ts=now.isoformat(),tier=tier,rule=rule,event=ev,what=what,side=side,price=rd(price),fair=rd(fair),note=note,ticker=ticker))
def nearest50(rs): return min(rs,key=lambda r:abs(r['mid']-.5))
for (ev,nm),D in P.items():
    s=SS.get((ev,nm))
    if not s: continue
    K=D.get('K'); O=D.get('OUTS')
    small=' small sample' if s['gs']<8 else ''
    if O and O['mean'] is not None:
        dev=O['mean']-s['outs_gs']
        if K and abs(dev)>=2:
            r=nearest50(K['rungs']); line=r['strike']
            if dev<=-2:
                add(1,'K-LENGTH UNDER',ev,f"{nm} K u{line}",'NO',round(1-r['bid'],2),round(1-r['mid'],3),
                    f"outs mkt {O['mean']:.1f} vs season {s['outs_gs']:.1f} ({dev:+.1f}); L5 outs {s['l5_outs']:.1f}{small}; book u{line}: {best_book(nm,'K',line,'under')}; {pin_fair(nm,'K',line,'under')}",r['ticker'])
            else:
                add(2,'K-LENGTH OVER',ev,f"{nm} K o{line}",'YES',round(r['ask'],2),round(r['mid'],3),
                    f"outs mkt {O['mean']:.1f} vs season {s['outs_gs']:.1f} ({dev:+.1f}){small}; book o{line}: {best_book(nm,'K',line,'over')}; {pin_fair(nm,'K',line,'over')}",r['ticker'])
    if K and K['mean'] is not None:
        kd=K['mean']-s['k_gs']; r=nearest50(K['rungs']); line=r['strike']
        if kd<=-1:
            add(1,'K-SEASON-ANCHOR UNDER',ev,f"{nm} K u{line}",'NO',round(1-r['bid'],2),round(1-r['mid'],3),
                f"K mkt {K['mean']:.2f} vs season {s['k_gs']:.2f} ({kd:+.2f}){small}; same bet as K-LENGTH if both fire; book: {best_book(nm,'K',line,'under')}; {pin_fair(nm,'K',line,'under')}",r['ticker'])
        if kd>=0.5 and O:
            ro=nearest50(O['rungs'])
            add(2,'DEEP-SPOT OVERS',ev,f"{nm} K o{line} / outs o{ro['strike']}",'YES',round(r['ask'],2),round(r['mid'],3),
                f"K mkt {K['mean']:.2f} vs season {s['k_gs']:.2f} ({kd:+.2f}); one position, not two{small}",r['ticker'])
# YRFI curve
tot=collections.defaultdict(list); rfi={}
for r in R:
    if r['series']=='KXMLBTOTAL' and r['mid'] is not None: tot[r['event'].split('-',1)[1]].append(r)
    if r['series']=='KXMLBRFI': rfi[r['event'].split('-',1)[1]]=r
for code,rs in tot.items():
    rs=sorted(rs,key=lambda r:r['strike']); T=None
    for a,b in zip(rs,rs[1:]):
        if a['mid']>=.5>=b['mid']: T=a['strike']+(a['mid']-.5)/(a['mid']-b['mid']+1e-9)*(b['strike']-a['strike'])
    x=rfi.get(code)
    if T is None or not x or not x['bid']: continue
    cur=1-1/(1+math.exp(-(1.92-0.229*T))); octv=1-1/(1+math.exp(-(1.92-0.229*T-0.15)))
    e=cur-x['ask']-fee(x['ask']); eo=octv-x['ask']-fee(x['ask'])
    tier=1 if e>=.02 else (3 if eo>=.02 else None)
    nofl=x['fn']/(x['fy']+x['fn']) if x['fy']+x['fn'] else 0
    line=f"Kalshi total {T:.2f}; YRFI curve {cur:.3f} (Oct-adj {octv:.3f}); ask {x['ask']:.2f}; edge {100*e:+.1f} (Oct {100*eo:+.1f}); taker $ {100*nofl:.0f}% NRFI (normal is 74%)"
    pr=None
    try:
        hh,mm=int(code[7:9]),int(code[9:11]); d0=codes[code[:7]]
        utc=(dt.datetime(d0.year,d0.month,d0.day,hh,mm)+dt.timedelta(hours=4)).strftime('%Y-%m-%dT%H')
        cand=[v for (h,t),v in PINRFI.items() if t==utc and nz(TEAM.get(code[-3:] if code[-3:] in TEAM else code[-2:],''))==h]
        pr=cand[0] if cand else None
    except Exception: pass
    if pr:
        pe=pr['yrfi']-x['ask']-fee(x['ask'])
        line+=f"; Pinnacle YRFI {pr['o']:+d} / NRFI {pr['u']:+d} (fair YRFI {pr['yrfi']:.3f}, Kalshi edge vs Pinnacle {100*pe:+.1f})"
    if tier: add(tier,'YRFI CURVE' if tier==1 else 'YRFI CURVE (Oct-adj only)',x['event'],f"{code} YRFI",'YES',x['ask'],round(cur,3),line,x['ticker'])
    else: add(4,'RFI INFO',x['event'],f"{code} RFI",'-',x['ask'],round(cur,3),line,x['ticker'])
# F5 overs (Fable rule, OCTOBER_RULES row 21): F5 mean = 0.573 x Kalshi full-game fair total, negative binomial var/mean 2.0; YES only, edge >= +2 after fee
f5=collections.defaultdict(list)
for r in R:
    if r['series']=='KXMLBF5TOTAL' and r['bid'] and r['ask']<1: f5[r['event'].split('-',1)[1]].append(r)
for code,rs in tot.items():
    rs=sorted(rs,key=lambda r:r['strike']); T=None
    for a,b in zip(rs,rs[1:]):
        if a['mid']>=.5>=b['mid']: T=a['strike']+(a['mid']-.5)/(a['mid']-b['mid']+1e-9)*(b['strike']-a['strike'])
    if T is None: continue
    mu=0.573*T
    for x in sorted(f5.get(code,[]),key=lambda r:r['strike']):
        p=float(stats.nbinom.sf(math.floor(x['strike']),mu,0.5))   # P(F5 runs >= strike+0.5), r=mu, p=.5 gives var=2*mean
        e=p-x['ask']-fee(x['ask'])
        if e>=.02: add(1,'F5 OVER',x['event'],f"{code} F5 o{x['strike']}",'YES',x['ask'],round(p,3),f"Kalshi full-game fair total {T:.2f} -> F5 mean {mu:.2f}; model {p:.3f} vs ask {x['ask']:.2f}; edge {100*e:+.1f} after fee (backtest +39% on 49, Aug +53 / Sep +32); all F5 rungs in one game = ONE position, take the best edge",x['ticker'])
# big outs trades (lead) + watch items
for r in R:
    for b in r['big']:
        if r['series']=='KXMLBOUTS':
            moved=abs((r['mid'] or 0)-b['px'])
            add(2 if moved<=.02 else 3,'BIG OUTS TRADE',r['event'],r['title'],b['side'],b['px'],r['mid'],f"${b['usd']:,} {b['side']} at {b['px']:.2f} ({b['ts']} UTC); now mid {r['mid']}; {'price within 2c, followable' if moved<=.02 else 'price already moved'}",r['ticker'])

    nb=[b for b in r['big_all']]
    if nb and r['series']!='KXMLBOUTS' and r['series'] in PSER or (nb and r['series']=='KXMLBRFI'):
        y=sum(b['usd'] for b in nb if b['side']=='YES'); n=sum(b['usd'] for b in nb if b['side']=='NO')
        add(4,'BIG TRADES (no backtested edge)',r['event'],r['title'],'YES' if y>n else 'NO',r['mid'],None,f"{len(nb)} prints $1k+ since listing: YES ${y:,} / NO ${n:,}" + ("; RFI NO size is normal retail flow (74% of RFI money is NRFI, YRFI realizes +2 pts over price)" if r['series']=='KXMLBRFI' else ''),r['ticker'])
    if r['mid'] is None: continue
    if r['prev_mid'] is not None and abs(r['mid']-r['prev_mid'])>=MOVE:
        add(4,'PRICE MOVE',r['event'],r['title'],'-',r['mid'],r['prev_mid'],f"{r['prev_mid']:.3f} -> {r['mid']:.3f} since last scan; new taker $ YES {r['ny']:.0f} / NO {r['nn']:.0f}" + ("; 12+ pt jumps partly reverse (limit fade only)" if abs(r['mid']-r['prev_mid'])>=.12 else ''),r['ticker'])
    a,b=r['dy3'],r['dn3']
    # Prop books on Kalshi are NO-heavy by default (market makers rest NO: 50-100% of prop rungs are 3x+ NO-heavy).
    # So only the unusual direction is flagged on props: YES-heavy, or NO-heavy at 10x+. Game markets: both ways at 3x.
    if .2<=r['mid']<=.8 and max(a,b)>=MINDEP:
        if r['series'] in PSER or r['series'] in HSER: hit=('YES' if a>=IMB*max(b,1) else None) or ('NO' if b>=10*max(a,1) and r['series'] in PSER else None)
        else: hit=('YES' if a>=IMB*max(b,1) else ('NO' if b>=IMB*max(a,1) else None))
        if hit: add(4,'DEPTH IMBALANCE (ungraded)',r['event'],r['title'],hit,round(r['mid'],3),None,f"resting within 3c: YES bids ${a:,.0f} vs NO bids ${b:,.0f}; heavier bidders {hit}; logged for grading",r['ticker'])
# velocity decline (Fable paper rule 10/6): last-3-start four-seam velo >= 0.5 mph under season-to-date -> K NO on main rung,
# fair YES = ladder mid - 4 pts, floor +2 after fee. Paper until 150 logged starts. Source: Baseball Savant (public CSV).
import csv as _csv, io as _io
def ff_trend(pid):
    try:
        u=("https://baseballsavant.mlb.com/statcast_search/csv?all=true&player_type=pitcher&pitchers_lookup%5B%5D="+str(pid)+
           "&hfSea=2026%7C&hfPT=FF%7C&hfGT=R%7CF%7CD%7CL%7CW%7C&type=details")
        txt=urllib.request.urlopen(urllib.request.Request(u,headers={'User-Agent':'Mozilla/5.0'}),timeout=90).read().decode('utf-8-sig')
        G=collections.defaultdict(list)
        for row in _csv.DictReader(_io.StringIO(txt)):
            try: G[row['game_date']].append(float(row['release_speed']))
            except Exception: pass
        gm=[(d,sum(v)/len(v)) for d,v in sorted(G.items()) if len(v)>=15]
        if len(gm)<6: return None
        seas=sum(v for _,v in gm)/len(gm); l3=sum(v for _,v in gm[-3:])/3
        return dict(seas=seas,l3=l3,d3=l3-seas,last=gm[-1][1]-seas,n=len(gm),lastdate=gm[-1][0])
    except Exception: return None
for (ev,nm),D in P.items():
    K=D.get('K'); pid=pp.get(nz(nm))
    if not K or not pid: continue
    v=ff_trend(pid)
    if not v: continue
    r=nearest50(K['rungs']); line=r['strike']
    nofair=1-(r['mid']-0.04); cost=1-r['bid']; e=nofair-cost-fee(cost)
    note=f"four-seam last 3 {v['l3']:.1f} vs season {v['seas']:.1f} ({v['d3']:+.2f} mph; last start {v['last']:+.2f}); fair NO {nofair:.3f} (mid - 4 pts) vs cost {cost:.2f}, edge {100*e:+.1f}; book u{line}: {best_book(nm,'K',line,'under')}; {pin_fair(nm,'K',line,'under')}"
    if v['d3']<=-0.5 and e>=0.02:
        add(2,'VELO-DECLINE K UNDER (paper)',ev,f"{nm} K u{line}",'NO',round(cost,2),round(1-r['mid'],3),note+"; Fable paper rule (101 starts, +16.8% per start, all 3 months positive); stake rule after 150 logged",r['ticker'])
    elif v['d3']<=-0.5:
        add(4,'VELO-DECLINE (price too short)',ev,f"{nm} K u{line}",'NO',round(cost,2),round(1-r['mid'],3),note,r['ticker'])
# walks over lead (Kalshi walks 2+ rungs: overs hit 4 pts above mid in Aug and Sep, n 597; spread eats it at the touch)
for (ev,nm),D in P.items():
    W=D.get('BB')
    if not W: continue
    r=nearest50(W['rungs']); line=r['strike']; s_=SS.get((ev,nm)) or {}; bbp=s_.get('bb_pct')
    hi=bbp is not None and bbp>=0.085
    add(3 if hi else 4,'WALKS OVER (lead)' if hi else 'WALKS OVER (info: low-BB% pitcher)',ev,f"{nm} walks o{line}",'YES',round(r['ask'],2),round(r['mid'],3),
        (f"season BB% {100*bbp:.1f}" if bbp is not None else "season BB% n/a") +
        f"; edge concentrates in BB% >= 8.5 (market prices walks too flat across pitchers); {"bet only at or better than Kalshi mid %.3f + 4 pts = fair about %.3f" % (r['mid'],min(r['mid']+.04,.99)) if hi else "no edge for low-BB%% pitchers (overs ran 0.8 pts BELOW mid); fair = Kalshi mid %.3f" % r['mid']}; Kalshi {r['bid']:.2f}/{r['ask']:.2f}; book o{line}: {best_book(nm,'BB',line,'over')}; {pin_fair(nm,'BB',line,'over')}",r['ticker'])
# October hitter-under lean (2026 PS: Hits 1+, TB 2+, H+R+RBI 1+/2+ overs ran 4 to 6 pts below mid, 16 games; 2021-25 PS H/PA 3 to 13% below RS)
if any(d.month==10 for d in dates):
    HU=collections.defaultdict(list)
    for r in R:
        if r['mid'] is None or r['ask']-r['bid']>0.03: continue
        if (r['series'],r['strike']) in (('KXMLBHIT',0.5),('KXMLBTB',1.5),('KXMLBHRR',0.5),('KXMLBHRR',1.5)): HU[r['event'].split('-',1)[1]].append(r)
    for code,L in HU.items():
        L.sort(key=lambda r:-r['mid'])
        add(3,'OCTOBER HITTER UNDERS (lean)',code,f"{len(L)} tight hitter rungs (Hits 1+, TB 2+, HRR 1+/2+)",'NO',None,None,
            "limit NO at the mid, small and spread across hitters; top: "+'; '.join(f"{r['title'].replace('?','')} mid {r['mid']:.3f}" for r in L[:6]),None)
# Postseason sub-risk hitter UNDER (Bartolo 10/6): part-timers lose PA to earlier October pinch-hitting.
# Fair P(0 hits) = sum over his 2026 RS PA distribution of (1 - h/PA)^(PA * tier multiplier); h/PA shrunk 200 PA to .220, x0.95 Oct contact.
# Sportsbook prices (Kalshi rarely lists these hitters). Lean: tier 3-20% RS pinch-hit-for share, edge >= +2 pts vs best under 0.5 hits.
PH_MULT=[(0.03,0.976),(0.10,0.930),(0.20,0.876),(1.01,0.852)]   # RS pinch-hit-for share -> PS PA multiplier (2016-20 + 2022-26 same player-season, 6,489 PS starts, pitchers excluded)
try:
    SUBR=json.load(open(os.path.join(os.path.dirname(os.path.abspath(__file__)),'sub_risk_2026.json')))
except Exception: SUBR={}
if SUBR and any(d.month>=10 for d in dates):
    HB=collections.defaultdict(list)
    for d in dates:
        x=g(f"https://api.actionnetwork.com/web/v2/scoreboard/mlb/markets?bookIds={','.join(BOOKS)}&customPickTypes=core_bet_type_36_hits,core_bet_type_77_total_bases&date={d.strftime('%Y%m%d')}")
        pl={p['id']:nz(p['full_name']) for p in x.get('players',[])}
        for bid,ev in (x.get('markets') or {}).items():
            if bid not in BOOKS: continue
            for typ,outs in ev.get('event',{}).items():
                for o in outs:
                    if o.get('value')==0.5 and o.get('side')=='under' and o.get('odds') is not None and pl.get(o.get('player_id')):
                        HB[pl[o['player_id']]].append((o['odds'],BOOKS[bid],'H' if typ.endswith('hits') else 'TB'))
    ipr=lambda o: 100/(o+100) if o>0 else -o/(-o+100)
    for d in dates:
        sch=g(f"{MLB}/schedule?sportId=1&date={d}&hydrate=lineups,team")
        for dd in sch.get('dates',[]):
            for gm in dd['games']:
                if gm.get('gameType')=='R': continue
                lu=gm.get('lineups') or {}
                gl=f"{gm['teams']['away']['team'].get('abbreviation','')}@{gm['teams']['home']['team'].get('abbreviation','')}"
                for side in ('awayPlayers','homePlayers'):
                    for p in lu.get(side,[]):
                        sr=SUBR.get(str(p['id']))
                        if not sr or sr['ph']<0.03: continue
                        mult=next(m for cut,m in PH_MULT if sr['ph']<cut)
                        hpa=(sr['h']+0.22*200)/(sr['pa']+200)*0.95; n=sr['starts']
                        p0=sum(c/n*(1-hpa)**(int(k)*mult) for k,c in sr['pa_dist'].items())
                        q=HB.get(nz(p['fullName']),[])
                        if not q: continue
                        best=max(q,key=lambda t:t[0]); e=p0-ipr(best[0])
                        tier=3 if (e>=0.02 and n>=15) else 4
                        add(tier,'PS SUB-RISK HIT UNDER (lean)' if tier==3 else 'PS SUB-RISK (info)',gl,f"{p['fullName']} under 0.5 hits",'UNDER',None,round(p0,3),
                            f"RS pinch-hit-for {100*sr['ph']:.0f}%, <=2 PA {100*sr['le2']:.0f}% ({n} starts), RS PA {sr['pa']/n:.2f} x Oct mult {mult}; fair P(0 H) {p0:.3f} vs best {best[1]} {int(best[0]):+d} (u0.5 {best[2]}), edge {100*e:+.1f}. Lineups confirmed only; small size"+("; under 15 RS starts: info only" if n<15 else ""),None)
for r in R:
    if r['series'] not in PSER or r['mid'] is None or r['ask']-r['bid']>0.04: continue
    p=PIN.get((nz(pname(r)),PSER[r['series']],float(r['strike'])))
    if not p: continue
    ey=p['over']-r['ask']-fee(r['ask']); en=(1-p['over'])-(1-r['bid'])-fee(1-r['bid'])
    if max(ey,en)>=0.02:
        side='YES' if ey>en else 'NO'
        add(3,'KALSHI OFF PINNACLE (ungraded)',r['event'],r['title'],side,r['ask'] if side=='YES' else 1-r['bid'],p['over'] if side=='YES' else 1-p['over'],
            f"Pinnacle o{r['strike']} {p['o']:+d} / u {p['u']:+d} (no-vig over {p['over']:.3f}) vs Kalshi {r['bid']:.2f}/{r['ask']:.2f}; edge at touch {100*max(ey,en):+.1f}; Pinnacle limit ${p['limit']}",r['ticker'])
# ---------- 5. output ----------
stamp=et_now.strftime('%Y%m%d_%H%M')
with open(os.path.join(HERE,'scan_log.jsonl'),'a') as f:
    for t in TR: f.write(json.dumps(t)+'\n')
TN={1:'TIER 1: backtested rules (bet if price holds)',2:'TIER 2: leads (half size or paper)',3:'TIER 3: weaker / already moved (watch)',4:'WATCH: information only, no backtested edge'}
L=[f"# Prop scan {et_now:%Y-%m-%d %H:%M} ET ({', '.join(str(d) for d in dates)})",'',f"{len(R)} Kalshi markets scanned; {len(P)} pitcher ladders; {sum(1 for k in SS if SS[k])} matched to season logs.",'']
for tier in (1,2,3,4):
    T=[t for t in TR if t['tier']==tier]
    if not T: continue
    L+=[f"## {TN[tier]}",'','| Rule | Game | Bet | Side | Kalshi price | Kalshi fair (mid) | Detail |','|---|---|---|---|---|---|---|']
    for t in sorted(T,key=lambda t:(t['rule'],t['event'])):
        L.append(f"| {t['rule']} | {t['event'].split('-',1)[-1]} | {t['what']} | {t['side']} | {t['price'] if t['price'] is not None else ''} | {(round(t['fair'],3) if t['fair'] is not None else '')} | {t['note']} |")
    L.append('')
L+=['## Pitcher ladders','','| Game | Pitcher | Season GS | K mkt mean | Season K/GS | Outs mkt mean | Season outs/GS | L5 outs | L5 pitches |','|---|---|---|---|---|---|---|---|---|']
for (ev,nm),D in sorted(P.items()):
    s=SS.get((ev,nm)) or {}
    f=lambda x,n=2: f"{x:.{n}f}" if isinstance(x,(int,float)) else '-'
    L.append(f"| {ev.split('-',1)[-1]} | {nm} | {s.get('gs','-')} | {f(D.get('K',{}).get('mean'))} | {f(s.get('k_gs'))} | {f(D.get('OUTS',{}).get('mean'),1)} | {f(s.get('outs_gs'),1)} | {f(s.get('l5_outs'),1)} | {f(s.get('l5_pitches'),0)} |")
open(os.path.join(HERE,f'scan_{stamp}.md'),'w').write('\n'.join(L)+'\n')
print('\n'.join(L))
