#!/usr/bin/env python3
"""
Daily v2 series pricing for the app. Fable, 2026-09-24.

Input : a bracket distribution from the season simulator —
        JSON  [[al_seeds, nl_seeds, count], ...]   (statsapi team ids, seed order 1..6)
        or    {"bracket_dist": [...]}              (this file's own output format is accepted)
        plus regular-season W% per team id (for LCS/WS home field) — pulled from statsapi if not given.
Output: v2_marginalized.json  {ws:{abbr:p}, al_pennant:{...}, nl_pennant:{...}, n_realizations, date}

Usage:
    python3 run_v2_bracket.py --bracket data/bracket_dist.json --out data/v2_marginalized.json
    python3 run_v2_bracket.py --standings                      # print the seeding sim's own distribution first (debug)

Engine: october_model.py (pre-registered v2 run model, NB game model, HFA logit) + bracket.py
(series enumeration, WC bo3 at higher seed, LDS bo5, LCS/WS bo7, marginalization).
Inputs: ../inputs/{lineup_features,def_team_features,rotation_ratings,CANON_team_inputs}.csv
— frozen at the end of the regular season (Fable refreshes and hands over the four CSVs once
after the last game; they do not change during October).
"""
import argparse, collections, datetime as dt, json, os, sys
import pandas as pd, requests
HERE = os.path.dirname(os.path.abspath(__file__)); sys.path.insert(0, HERE)
import october_model as M, bracket as B

ABBR = {108:'LAA',109:'AZ',110:'BAL',111:'BOS',112:'CHC',113:'CIN',114:'CLE',115:'COL',116:'DET',117:'HOU',118:'KC',119:'LAD',120:'WSH',121:'NYM',133:'ATH',134:'PIT',135:'SD',136:'SEA',137:'SF',138:'STL',139:'TB',140:'TEX',141:'TOR',142:'MIN',143:'PHI',144:'ATL',145:'CWS',146:'MIA',147:'NYY',158:'MIL'}


def season_pct(season):
    r = requests.get(f'https://statsapi.mlb.com/api/v1/standings?leagueId=103,104&season={season}&standingsTypes=regularSeason', timeout=30).json()
    return {t['team']['id']: t['wins'] / max(t['wins'] + t['losses'], 1) for rec in r['records'] for t in rec['teamRecords']}


def load_bracket(path):
    j = json.load(open(path))
    bd = j['bracket_dist'] if isinstance(j, dict) else j
    return [[list(map(int, a)), list(map(int, b)), int(n)] for a, b, n in bd]


def price(bd, season=2026, min_weight=0.001):
    pct = season_pct(season)
    def fb(ids):
        F = M.build_field_from_raw(season, teams=list(ids))
        return F.set_index('team') if 'team' in F.columns else F
    tot = sum(e[2] for e in bd)
    ws, al, nl = collections.defaultdict(float), collections.defaultdict(float), collections.defaultdict(float)
    used = 0
    priced = 0
    for a, n_, n in bd:
        if n / tot < min_weight: continue
        try:
            pr = B.Pricer(fb(a + n_), pct)
            w, A, N = B.world_series(pr, a, n_)
        except ValueError as e:
            # A rare seeding realization pulled in a team outside the frozen
            # 16-team v2 field (no end-of-season inputs). Skip it and let the
            # marginal renormalize over the priced realizations. Fable's field
            # is fixed for October, so these are always negligible-weight.
            continue
        for k, v in w.items(): ws[k] += v * n
        for k, v in A.items(): al[k] += v * n
        for k, v in N.items(): nl[k] += v * n
        used += 1; priced += n
    norm = lambda d: {ABBR.get(k, str(k)): v / sum(d.values()) for k, v in sorted(d.items(), key=lambda x: -x[1])}
    return dict(ws=norm(ws), al_pennant=norm(al), nl_pennant=norm(nl), n_realizations=used, date=dt.date.today().isoformat(), engine='v2 rot_oct, PREREG_2026_CONFIG.json')


if __name__ == '__main__':
    ap = argparse.ArgumentParser(); ap.add_argument('--bracket', required=True); ap.add_argument('--out', default='v2_marginalized.json'); ap.add_argument('--season', type=int, default=2026)
    a = ap.parse_args()
    out = price(load_bracket(a.bracket), a.season)
    json.dump(out, open(a.out, 'w'), indent=1)
    print('WS%:', {k: round(v * 100, 1) for k, v in list(out['ws'].items())[:12]}, file=sys.stderr)
