"""Grade scan_log.jsonl triggers (tiers 1-3) after settlement: result, P/L per contract after fee at the logged price, and CLV vs the
Kalshi close (VWAP of the last 10 trades before the market's first-pitch time, from the ticker). Usage: python3 scan_grade.py"""
import json,os,time,urllib.request,re,datetime as dt,collections,statistics as st,math
API="https://api.elections.kalshi.com/trade-api/v2"; HERE=os.environ.get('SCAN_DIR') or os.path.dirname(os.path.abspath(__file__))
MON={m:i+1 for i,m in enumerate(['JAN','FEB','MAR','APR','MAY','JUN','JUL','AUG','SEP','OCT','NOV','DEC'])}
fee=lambda p:0.07*p*(1-p)
def g(u):
    for i in range(5):
        try: return json.load(urllib.request.urlopen(urllib.request.Request(u,headers={'User-Agent':'Mozilla/5.0'}),timeout=30))
        except Exception: time.sleep(1.5*(i+1))
    return {}
T=[json.loads(l) for l in open(os.path.join(HERE,'scan_log.jsonl'))]
T=[t for t in T if t['tier']<=3 and t.get('ticker') and t['side'] in('YES','NO')]
def _fp(tk):
    c=re.search(r'-(\d\d)([A-Z]{3})(\d\d)(\d\d)(\d\d)',tk)
    return dt.datetime(2000+int(c.group(1)),MON[c.group(2)],int(c.group(3)),int(c.group(4)),int(c.group(5)),tzinfo=dt.timezone(dt.timedelta(hours=-4))) if c else None
T=[t for t in T if _fp(t['ticker']) and dt.datetime.fromisoformat(t['ts'])<_fp(t['ticker'])]   # pregame flags only (scan logged in-game rows before the 10/10 fix)
seen=set(); U=[]
for t in T:   # first trigger per ticker+rule only (later scans repeat it)
    k=(t['ticker'],t['rule'])
    if k not in seen: seen.add(k); U.append(t)
rows=[]
for t in U:
    m=g(f"{API}/markets/{t['ticker']}").get('market',{})
    if m.get('result') not in('yes','no'): continue
    c=re.search(r'-(\d\d)([A-Z]{3})(\d\d)(\d\d)(\d\d)',t['ticker'])
    fp=dt.datetime(2000+int(c.group(1)),MON[c.group(2)],int(c.group(3)),int(c.group(4)),int(c.group(5)),tzinfo=dt.timezone(dt.timedelta(hours=-4)))
    tr=g(f"{API}/markets/trades?ticker={t['ticker']}&limit=1000").get('trades',[])
    pre=[x for x in tr if dt.datetime.fromisoformat(x['created_time'].replace('Z','+00:00'))<fp][:10]
    close=sum(float(x['count_fp'])*float(x['yes_price_dollars']) for x in pre)/sum(float(x['count_fp']) for x in pre) if pre else None
    y=1 if m['result']=='yes' else 0; win=y if t['side']=='YES' else 1-y
    p=t['price']; pl=win-p-fee(p)
    clv=None if close is None else ((close-p) if t['side']=='YES' else ((1-close)-p))
    rows.append(dict(**t,result=m['result'],pl=pl,close=close,clv=clv))
by=collections.defaultdict(list)
for r in rows: by[r['rule']].append(r)
print(f"{'rule':32s} n   P/L c/contract   CLV pts")
for k,v in sorted(by.items()):
    c=[r['clv'] for r in v if r['clv'] is not None]
    print(f"{k:32s} {len(v):3d} {100*st.mean(r['pl'] for r in v):+6.1f}        {100*st.mean(c) if c else float('nan'):+5.1f}")
json.dump(rows,open(os.path.join(HERE,'scan_graded.json'),'w'),indent=0)
