#!/usr/bin/env python3
"""World Series MVP board: Sean's model vs the best listed price (sportsbooks + live Kalshi).

Inputs
  data/ws_mvp_model.json   Sean's projections: {players:[{name, team, pitcher, p_model}], series_p, ...}
  data/ws_mvp_books.json   pasted sportsbook futures {players:{name:{consensus, FanDuel, BetMGM, DraftKings, Caesars, BetRivers}}}
  Kalshi KXMLBWSMVP        live, public API (YES ask + the 7% taker fee = cost)
Output
  data/ws_mvp.json         one row per player: model %, fair odds, best price + venue, edge (pts), stars

Best price excludes BetMGM (standing rule). Edge = model probability - implied probability of the best price (pts),
stars: >= 2 pts one star, >= 4 pts two stars (same as the series cards). Read-only; places nothing.
"""
import json, os, re, unicodedata, urllib.request, datetime as dt
HERE = os.path.dirname(os.path.abspath(__file__)); D = os.path.join(HERE, '..', 'data')
KALSHI = "https://api.elections.kalshi.com/trade-api/v2/markets?series_ticker=KXMLBWSMVP&status=open&limit=500"
BOOKS = ['FanDuel', 'DraftKings', 'Caesars', 'BetRivers', 'BetOnline', 'Bookmaker']          # BetMGM excluded from best price
AB = {'FanDuel': 'FD', 'DraftKings': 'DK', 'Caesars': 'CZR', 'BetRivers': 'BR', 'BetOnline': 'BOL', 'Bookmaker': 'BKM', 'Kalshi': 'Kalshi'}
fee = lambda p: 0.07 * p * (1 - p)
imp = lambda o: 100 / (o + 100) if o > 0 else -o / (-o + 100)
def am(p):
    if p is None or p <= 0 or p >= 1: return None
    return round(100 * (1 - p) / p) if p < 0.5 else -round(100 * p / (1 - p))
def nz(s):
    s = unicodedata.normalize('NFKD', s or '').encode('ascii', 'ignore').decode().lower()
    return re.sub(r'\b(jr|sr|ii|iii)\b|[^a-z ]', '', s).strip()

def kalshi():
    out = {}
    try:
        d = json.load(urllib.request.urlopen(urllib.request.Request(KALSHI, headers={'User-Agent': 'bartolo'}), timeout=30))
        for m in d.get('markets', []):
            nm = m.get('yes_sub_title') or ''
            ask = float(m.get('yes_ask_dollars') or 0); bid = float(m.get('yes_bid_dollars') or 0)
            na = float(m.get('no_ask_dollars') or 0)
            if nm and 0 < ask < 1: out[nz(nm)] = {'ask': ask, 'bid': bid, 'no_ask': na if 0 < na < 1 else None, 'ticker': m['ticker']}
    except Exception as e:
        print('[ws_mvp] Kalshi fetch failed:', e)
    return out

def main():
    M = json.load(open(os.path.join(D, 'ws_mvp_model.json')))
    B = json.load(open(os.path.join(D, 'ws_mvp_books.json'))) if os.path.exists(os.path.join(D, 'ws_mvp_books.json')) else {'players': {}}
    bk = {nz(k): v for k, v in B.get('players', {}).items()}
    K = kalshi()
    rows = []
    for p in M['players']:
        k = nz(p['name']); pm = p['p_model']
        offers = []
        for b in BOOKS:
            o = (bk.get(k) or {}).get(b)
            if o is not None: offers.append((imp(o), b, o))
        q = K.get(k)
        if q:
            c = q['ask'] + fee(q['ask']); offers.append((c, 'Kalshi', am(c)))
        best = min(offers) if offers else None
        r = {'name': p['name'], 'team': p['team'], 'pitcher': p.get('pitcher', False), 'p_model': round(pm, 4), 'fair': am(pm),
             'kalshi_ask': q['ask'] if q else None, 'book_consensus': (bk.get(k) or {}).get('consensus')}
        if best:
            r.update(best_venue=AB[best[1]], best_odds=best[2], best_implied=round(best[0], 4),
                     edge_pts=round((pm - best[0]) * 100, 2), ev=round(pm / best[0] - 1, 3))
            r['stars'] = 2 if r['edge_pts'] >= 4 else (1 if r['edge_pts'] >= 2 else 0)
        if q and q.get('no_ask'):                       # Kalshi NO side (Fable's book leans on these)
            cn = q['no_ask'] + fee(q['no_ask'])
            r.update(kalshi_no_ask=q['no_ask'], no_edge_pts=round(((1 - pm) - cn) * 100, 2))
        rows.append(r)
    rows.sort(key=lambda r: -r['p_model'])
    out = {'generated_at': dt.datetime.now(dt.timezone.utc).isoformat(timespec='seconds'), 'series': M.get('series'), 'stage': M.get('stage'),
           'series_p': M.get('series_p'), 'model_updated_at': M.get('updated_at'), 'books_pasted_at': B.get('pasted_at'),
           'kalshi_live': bool(K), 'players': rows}
    json.dump(out, open(os.path.join(D, 'ws_mvp.json'), 'w'), indent=1)
    pos = [r for r in rows if (r.get('edge_pts') or 0) > 0]
    neg = [r for r in rows if (r.get('no_edge_pts') or 0) > 0]
    print(f"[ws_mvp] {len(rows)} players, {len(pos)} positive YES edges, {len(neg)} Kalshi NO edges; Kalshi live: {bool(K)}")

if __name__ == '__main__':
    main()
