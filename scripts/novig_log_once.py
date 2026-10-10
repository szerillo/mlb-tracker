"""One Novig snapshot of every open pregame MLB game market we use (public API, no key).
Appends to data/novig_log/YYYY-MM-DD.jsonl: best price + size per outcome, depth within 3c, and trades since the last run.
Novig charges no fee pregame (fee charged WHEN_LIVE), so it is often the cheapest venue to execute."""
import json,time,urllib.request,datetime,os,concurrent.futures as cf
A="https://api.novig.com/v3/public"
TYPES={'PITCHER_STRIKEOUTS','PITCHER_OUTS','HITS_ALLOWED','WALKS','EARNED_RUNS','HITS','TOTAL_BASES','HITS_RUNS_RBIS','HOME_RUNS','RBIS','RUNS',
       'FIRST_INNING_TOTAL','TOTAL','TEAM_TOTAL','TOTAL_1H','SPREAD_1H','MONEY','MONEY_1H','BATTING_STRIKEOUTS','BATTING_WALKS'}
def g(u):
    for i in range(5):
        try: return json.load(urllib.request.urlopen(urllib.request.Request(u,headers={'User-Agent':'bartolo-logger'}),timeout=30))
        except urllib.error.HTTPError as e:
            if e.code==429: time.sleep(2*(i+1)); continue
            return {}
        except Exception: time.sleep(1.5*(i+1))
    return {}
now=time.time(); et=datetime.datetime.utcnow()-datetime.timedelta(hours=4)
os.makedirs('data/novig_log',exist_ok=True); stf='data/novig_log/_last_trade.json'
last=json.load(open(stf)) if os.path.exists(stf) else {}
ev=[e for e in g(f"{A}/catalog/events?league=MLB&status=OPEN_PREGAME&limit=500").get('items',[]) if '@' in e['description'] and e['startsTs']/1000-now<36*3600]
mk=[]
for e in ev:
    mk+=[(e,m) for m in g(f"{A}/catalog/markets?event={e['eventId']}&limit=2000").get('items',[]) if m['marketType'] in TYPES and m.get('status')=='OPEN']
def snap(x):
    e,m=x; mid=m['marketId']
    b=g(f"{A}/catalog/markets/{mid}/book?depth=10").get('orders',{})
    oc={o['outcomeId']:o['name'] for o in m['outcomes']}
    side={}
    for oid,L in b.items():
        ps=[(float(o['price']),o['qty']) for o in L]
        if not ps: side[oc.get(oid,oid)]=None; continue
        bp=max(p for p,_ in ps)
        side[oc.get(oid,oid)]=dict(best=bp,qty_best=sum(q for p,q in ps if p==bp),qty_3c=sum(q for p,q in ps if p>=bp-0.03))
    tr=g(f"{A}/catalog/markets/{mid}/trades?limit=200").get('items',[])
    lt=last.get(mid,0); new=[dict(o=oc.get(t['outcomeId']),p=float(t['price']),q=t['qty'],ts=t['ts']) for t in tr if t['ts']>lt]
    return dict(ts=int(now),event=e['description'],start=e['startsTs'],market=mid,type=m['marketType'],desc=m['description'],strike=m.get('strike'),book=side,new_trades=new,
                last_ts=max([t['ts'] for t in tr],default=lt))
with cf.ThreadPoolExecutor(6) as ex: rows=list(ex.map(snap,mk))
with open(f"data/novig_log/{et.date().isoformat()}.jsonl",'a') as f:
    for r in rows: f.write(json.dumps({k:v for k,v in r.items() if k!='last_ts'})+'\n')
last.update({r['market']:r['last_ts'] for r in rows}); json.dump(last,open(stf,'w'))
print('novig snapshot',len(rows),'markets',len(ev),'events')
