#!/usr/bin/env python3
"""Closing-line table: for every settled event x venue x market x line x side x participant, the open, the close, the devigged
probabilities at 24h / 6h / 1h / close, the close age, and the outcome. Everything in tests.py reads this table.
Writes table `closes` into the SQLite view and data/sharp/closes.jsonl (so the dashboard and ad-hoc work can read it without SQLite).
"""
import os, sys, json, sqlite3, datetime as dt, collections
HERE = os.path.dirname(os.path.abspath(__file__)); sys.path.insert(0, os.path.dirname(HERE))
from sharp import constants as C, norm as N, fair as F, settle as S

ROOT = os.path.abspath(os.path.join(HERE, '..')); SD = os.path.join(ROOT, 'data', 'sharp'); DB = os.path.join(SD, 'sharp.sqlite')
DDL = """
CREATE TABLE IF NOT EXISTS closes (
  event_id TEXT, et_date TEXT, start_utc TEXT, venue TEXT, vclass TEXT, market TEXT, mclass TEXT, line REAL, side TEXT, participant TEXT,
  open_ts TEXT, open_implied REAL, p_open REAL,
  close_ts TEXT, close_implied REAL, p_close REAL, close_age_min REAL, close_limit REAL, close_flags TEXT,
  p_24h REAL, p_6h REAL, p_1h REAL,
  outcome INTEGER, push INTEGER, void INTEGER, n_ticks INTEGER,
  PRIMARY KEY (event_id, venue, market, line, side, participant)
);"""


def build():
    conn = sqlite3.connect(DB); conn.execute("DROP TABLE IF EXISTS closes"); conn.executescript(DDL)
    res = {r[0]: dict(status=r[1], home_score=r[3], away_score=r[4], f5_home=r[5], f5_away=r[6], rfi=r[7], players=json.loads(r[8] or '{}'))
           for r in conn.execute("SELECT * FROM results")}
    rows = []
    for ev, et_date, start, away, home in conn.execute("SELECT event_id, et_date, start_utc, away, home FROM events").fetchall():
        r = res.get(ev)
        if not r or r['status'] != 'final': continue
        eq = F.EventQuotes(conn, ev); st = N.parse_ts(start); start_iso = N.iso(st)
        method_for = lambda m: C.DEVIG_METHOD[('mlb', N.MARKET_CLASS(m))]['value']
        for (v, m, l, s, p), L in eq.idx.items():
            if C.VENUE_CLASS.get(v) == 'reference': continue
            pre = [t for t in L if t[0] <= start_iso]
            if not pre: continue
            o, c = pre[0], pre[-1]
            meth = method_for(m)
            p_open = eq.venue_prob(v, m, l, s, o[0], meth, p)[0]
            p_close = eq.venue_prob(v, m, l, s, c[0], meth, p)[0]
            age = (st - N.parse_ts(c[0])).total_seconds() / 60
            hz = {}
            for name, mins in (('24h', 1440), ('6h', 360), ('1h', 60)):
                hz[name] = eq.venue_prob(v, m, l, s, N.iso(st - dt.timedelta(minutes=mins)), meth, p, max_age=C.HORIZON_TOLERANCE_MIN['value'])[0]
            out = S.outcome(m, l, s, p, r, v, (away, home))
            rows.append((ev, et_date, start, v, C.VENUE_CLASS.get(v), m, N.MARKET_CLASS(m), l, s, p,
                         o[0], o[5], p_open, c[0], c[5], p_close, round(age, 1), c[11], c[9], hz['24h'], hz['6h'], hz['1h'],
                         out if out in (0, 1) else None, 1 if out is None else 0, 1 if out == 'void' else 0, len(pre)))
    conn.executemany("INSERT OR REPLACE INTO closes VALUES (" + ','.join('?' * 26) + ")", rows)
    conn.commit()
    cols = ['event_id', 'et_date', 'start_utc', 'venue', 'vclass', 'market', 'mclass', 'line', 'side', 'participant', 'open_ts', 'open_implied', 'p_open',
            'close_ts', 'close_implied', 'p_close', 'close_age_min', 'close_limit', 'close_flags', 'p_24h', 'p_6h', 'p_1h', 'outcome', 'push', 'void', 'n_ticks']
    with open(os.path.join(SD, 'closes.jsonl'), 'w') as f:
        for r in rows: f.write(json.dumps(dict(zip(cols, r))) + '\n')
    n_ev = len({r[0] for r in rows})
    print(f"[grade] closes: {len(rows)} rows over {n_ev} settled events; graded {sum(1 for r in rows if r[22] is not None)}, push {sum(r[23] for r in rows)}, void {sum(r[24] for r in rows)}")
    conn.close()


if __name__ == '__main__':
    build()
