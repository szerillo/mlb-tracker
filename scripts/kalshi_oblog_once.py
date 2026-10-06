"""One Kalshi MLB prop order-book snapshot for every open game today (ET). Appends to data/kalshi_oblog/YYYY-MM-DD.jsonl.
Run every 20 min from GitHub Actions. No auth needed (public Kalshi endpoints)."""
import json,time,urllib.request,datetime,os,concurrent.futures as cf
API="https://api.elections.kalshi.com/trade-api/v2"
SER=['KXMLBKS','KXMLBOUTS','KXMLBHA','KXMLBHIT','KXMLBHR','KXMLBTB','KXMLBRBI','KXMLBHRR','KXMLBWA','KXMLBRFI','KXMLBTOTAL','KXMLBF5TOTAL']
MON=['JAN','FEB','MAR','APR','MAY','JUN','JUL','AUG','SEP','OCT','NOV','DEC']
def g(u):
    for i in range(6):
        try: return json.load(urllib.request.urlopen(urllib.request.Request(u,headers={'User-Agent':'x'}),timeout=30))
        except Exception: time.sleep(1.5*(i+1))
    return {}
et=datetime.datetime.utcnow()-datetime.timedelta(hours=4); dc=et.strftime('%y')+MON[et.month-1]+et.strftime('%d')
ms=[]
for s in SER:
    cur=''
    while True:
        d=g(f"{API}/markets?series_ticker={s}&status=open&limit=1000"+(f"&cursor={cur}" if cur else ""))
        ms+=[(s,m) for m in d.get('markets',[]) if dc in m['event_ticker']]; cur=d.get('cursor')
        if not cur or not d.get('markets'): break
def book(x):
    s,m=x; t=m['ticker']
    ob=g(f"{API}/markets/{t}/orderbook").get('orderbook_fp') or {}
    yes=[(float(p),float(q)) for p,q in ob.get('yes_dollars') or []]; no=[(float(p),float(q)) for p,q in ob.get('no_dollars') or []]
    bb=max([p for p,_ in yes],default=0); nb=max([p for p,_ in no],default=0)
    dep=lambda L,top,w: round(sum(q*p for p,q in L if p>=top-w+1e-9),2)
    return dict(ts=int(time.time()),series=s,ticker=t,vol=float(m.get('volume_fp') or 0),yes_bid=bb,yes_ask=round(1-nb,2) if nb else None,
                yes_depth3=dep(yes,bb,.03),no_depth3=dep(no,nb,.03),yes_depth10=dep(yes,bb,.10),no_depth10=dep(no,nb,.10),
                yes_total=round(sum(q*p for p,q in yes),2),no_total=round(sum(q*p for p,q in no),2))
with cf.ThreadPoolExecutor(8) as ex: rows=list(ex.map(book,ms))
os.makedirs('data/kalshi_oblog',exist_ok=True)
with open(f"data/kalshi_oblog/{et.date().isoformat()}.jsonl",'a') as f:
    for r in rows: f.write(json.dumps(r)+'\n')
print('snapshot',len(rows),'markets',dc)
