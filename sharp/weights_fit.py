#!/usr/bin/env python3
"""Fit the ruler weights (spec §7): the weight vector minimising Brier of the weighted fair against outcomes on a held-out half of the
settled log, ridge-penalised toward equal weights, every venue capped at WEIGHT_CAP. Venues failing Test A or below 50% on Test C get 0.
Refuses to write a fitted vector below the publication gate; writes the gate status instead so the dashboard can show progress.

Output: data/sharp/weights_fitted.json  {"mlb:game": {...}, "mlb:prop": {...}}
"""
import os, sys, json, sqlite3, random, statistics, collections, datetime as dt
HERE = os.path.dirname(os.path.abspath(__file__)); sys.path.insert(0, os.path.dirname(HERE))
from sharp import constants as C, fair as F, tests as T

ROOT = os.path.abspath(os.path.join(HERE, '..')); SD = os.path.join(ROOT, 'data', 'sharp'); DB = os.path.join(SD, 'sharp.sqlite')
OUT = os.path.join(SD, 'weights_fitted.json')


def cells(conn, mclass):
    """[(event_id, {venue: p_close}, outcome)] for cells with 2+ ruler-eligible venues and a fresh close."""
    rs = T.rows(conn, "AND mclass=? AND close_age_min <= ?", (mclass, C.STALE_BOOK_MIN['value']))
    g = collections.defaultdict(dict); out = []
    for r in rs:
        if r['venue'] in C.RULER_ELIGIBLE: g[(r['event_id'], r['market'], r['line'], r['side'], r['participant'])][r['venue']] = (r['p_close'], r['outcome'])
    for k, vs in g.items():
        if len(vs) >= 2: out.append((k[0], {v: p for v, (p, o) in vs.items()}, next(iter(vs.values()))[1]))
    return out


def brier(w, data):
    s = 0.0; n = 0
    for _, ps, y in data:
        cw = F.capped_weights({v: w.get(v, 0) for v in ps})
        if not cw: continue
        f = sum(cw[v] * ps[v] for v in cw); s += (f - y) ** 2; n += 1
    return s / n if n else None


def fit(mclass, data, venues, prior, ridge=0.05, iters=60, seed=11):
    """Coordinate search on log-weights (no scipy dependency); ridge toward the prior vector. Returns (weights, train_brier, heldout_brier)."""
    rnd = random.Random(seed); evs = sorted({d[0] for d in data}); rnd.shuffle(evs)
    half = set(evs[:len(evs) // 2]); train = [d for d in data if d[0] in half]; test = [d for d in data if d[0] not in half]
    ps = sum(prior.get(v, 0) for v in venues) or 1
    w = {v: max(prior.get(v, 0) / ps, 0.02) for v in venues}
    def obj(w):
        return brier(w, train) + ridge * sum((w[v] - prior.get(v, 0) / ps) ** 2 for v in venues)
    best = obj(w); step = 0.5
    for _ in range(iters):
        improved = False
        for v in venues:
            for m in (1 + step, 1 / (1 + step)):
                w2 = dict(w); w2[v] = w[v] * m
                o = obj(w2)
                if o < best - 1e-9: best, w, improved = o, w2, True
        if not improved: step /= 2
        if step < 0.01: break
    s = sum(w.values()); w = {v: round(x / s, 4) for v, x in w.items()}
    return w, round(brier(w, train), 5), round(brier(w, test), 5)


def main():
    conn = sqlite3.connect(DB); res = json.load(open(OUT)) if os.path.exists(OUT) else {}
    tests_a = {m: T.test_a(conn, m, preview=True) for m in ('game', 'prop')}; test_c = T.test_c(conn, preview=True)
    for mclass in ('game', 'prop'):
        data = cells(conn, mclass); n = len({d[0] for d in data}) if mclass == 'game' else len(data)
        gate = T.gate_for(mclass); key = f"mlb:{mclass}"
        if n < gate:
            res[key] = dict(status='gated', n=n, gate=gate, checked=dt.datetime.now(dt.timezone.utc).isoformat(), note=f"prior weights in force until {gate}")
            print(f"[weights] {key}: gated ({n}/{gate}); prior stays"); continue
        venues = sorted({v for _, ps, _ in data for v in ps})
        prior = C.WEIGHTS[('mlb', mclass)]['value']
        # exclusions: Test A failure = |priced - hit| > 3 pts on 1500+ rows; Test C below 50%
        zero = set()
        for v, a in (tests_a[mclass].get('venues') or {}).items():
            if a['n'] >= 1500 and abs(a['priced'] - a['hit']) > 0.03: zero.add(v)
        if mclass == 'prop':
            for v, c in (test_c.get('venues') or {}).items():
                if c.get('published') and c.get('edge_real_pct') is not None and c['edge_real_pct'] < 50: zero.add(v)
        w, b_train, b_test = fit(mclass, data, [v for v in venues if v not in zero], prior)
        for v in zero: w[v] = 0.0
        res[key] = dict(status='fitted', value=w, fit_date=dt.datetime.now(dt.timezone.utc).date().isoformat(), sample=n, heldout_brier=b_test, train_brier=b_train,
                        zeroed=sorted(zero), ridge=0.05, cap=C.WEIGHT_CAP['value'], source='sharp/weights_fit.py on data/sharp/closes; refit monthly')
        print(f"[weights] {key}: fitted on {n}, held-out Brier {b_test}: {w}")
    json.dump(res, open(OUT, 'w'), indent=1); conn.close()


if __name__ == '__main__':
    main()
