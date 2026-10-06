"""Grade the order-book-imbalance rule once games settle. Usage: python3 scripts/kalshi_oblog_grade.py data/kalshi_oblog/*.jsonl
Rule tested: when YES is offered much more than bid (no_depth3 >> yes_depth3), the NO side is the sharp side (and vice versa).
For each market: imbalance at the last snapshot >= 60 min before first pitch, vs the final pregame snapshot mid (CLV) and the settled result."""
import json,sys,collections,math,urllib.request,time,re,datetime
MON={'JAN':1,'FEB':2,'MAR':3,'APR':4,'MAY':5,'JUN':6,'JUL':7,'AUG':8,'SEP':9,'OCT':10,'NOV':11,'DEC':12}
def fp(t):
    c=t.split('-')[1]; yy,mo,dd,hh,mi=re.match(r'(\d\d)([A-Z]{3})(\d\d)(\d\d)(\d\d)',c).groups()
    return (datetime.datetime(2000+int(yy),MON[mo],int(dd),int(hh),int(mi),tzinfo=datetime.timezone.utc)+datetime.timedelta(hours=4)).timestamp()
S=collections.defaultdict(list)
for fn in sys.argv[1:]:
    for l in open(fn): r=json.loads(l); S[r['ticker']].append(r)
def res(t):
    try: m=json.load(urllib.request.urlopen(urllib.request.Request(f"https://api.elections.kalshi.com/trade-api/v2/markets/{t}",headers={'User-Agent':'x'}),timeout=30))['market']; return m.get('result')
    except Exception: return None
rows=[]
for t,L in S.items():
    f=fp(t); pre=sorted([x for x in L if x['ts']<f],key=lambda x:x['ts'])
    sig=[x for x in pre if x['ts']<=f-3600]
    if not sig or not pre: continue
    s=sig[-1]; c=pre[-1]
    if not (s['yes_bid'] and s['yes_ask'] and c['yes_bid'] and c['yes_ask']): continue
    a,b=s['no_depth3'],s['yes_depth3']          # YES offered vs YES bid
    if a+b<500: continue
    ratio=(a+1)/(b+1); lean=-1 if ratio>=3 else (1 if ratio<=1/3 else 0)   # -1 = NO side sharp
    if not lean: continue
    m0=(s['yes_bid']+s['yes_ask'])/2; m1=(c['yes_bid']+c['yes_ask'])/2
    r=res(t); y=None if r not in ('yes','no') else (1 if r=='yes' else 0); time.sleep(0.1)
    rows.append(dict(t=t,game=t.split('-')[1],series=s['series'],ratio=ratio,lean=lean,clv=lean*(m1-m0),res=None if y is None else lean*(y-m0)))
def cm(xs,f):
    g=collections.defaultdict(list)
    for x in xs: g[x['game']].append(f(x))
    n=len(xs); m=sum(sum(v) for v in g.values())/n; k=len(g)
    if k<2: return m,float('nan')
    return m,math.sqrt(sum((sum(v)-m*len(v))**2 for v in g.values())*k/(k-1)/n**2)
for lab,f in (('all',lambda x:True),('lean NO',lambda x:x['lean']<0),('lean YES',lambda x:x['lean']>0),('ratio>=10x',lambda x:max(x['ratio'],1/x['ratio'])>=10)):
    xs=[x for x in rows if f(x)]
    if not xs: continue
    a=cm(xs,lambda x:x['clv']); ys=[x for x in xs if x['res'] is not None]; b=cm(ys,lambda x:x['res']) if ys else (float('nan'),)*2
    print(f"{lab:12s} n {len(xs):5d} games {len({x['game'] for x in xs}):4d}  lean-side CLV {100*a[0]:+.1f} (se {100*a[1]:.1f})  lean-side result-mid {100*b[0]:+.1f} (se {100*b[1]:.1f})")
