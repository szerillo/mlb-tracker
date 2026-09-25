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
import numpy as np, pandas as pd, requests
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


def price(bd, season=2026, min_weight=0.001, temperature=1.0, coef=None):
    """temperature b: series probabilities are re-calibrated as sigmoid(b * logit(p)). b=1 is the raw engine.
    SERIES_TEMPERATURE = 1.0 (2026-09-25, final): the market-matched test (89 series 2016-2025 with actual
    series prices and true home orientation) shows no top-end over-confidence -- model favourites >= .70 won
    11 of 14 at a model mean of .77 vs a market mean of .65; Platt slope 0.98 +/- 0.43; b=1 beats b=0.75
    (P 0.71). The 0.75 fitted earlier today used W%-inferred orientation and is withdrawn. Refit after 2026."""
    pct = season_pct(season)
    def fb(ids):
        F = M.build_field_from_raw(season, teams=list(ids))
        return F.set_index('team') if 'team' in F.columns else F
    tot = sum(e[2] for e in bd)
    ws, al, nl = collections.defaultdict(float), collections.defaultdict(float), collections.defaultdict(float)
    used = 0
    for a, n_, n in bd:
        if n / tot < min_weight: continue
        try:
            pr = B.Pricer(fb(a + n_), pct, coef=(np.array([coef[t] for t in M.OWN_TERMS + M.OPP_TERMS]) if coef else None))
        except ValueError:
            # Deploy-safety guard (Bartolo): a rare seeding realization pulled in a
            # team outside the frozen 16-team field (no end-of-season inputs). Skip it
            # and let the marginal renormalize; the field is fixed for October so these
            # are always negligible weight. Changes no priced output vs the shipped runner.
            continue
        if temperature != 1.0:
            _orig = pr.p_series
            def _ps(x, y, best_of, a_hosts=True, _o=_orig, _b=temperature):
                p = _o(x, y, best_of, a_hosts); l = np.log(p / (1 - p)) * _b; return 1.0 / (1.0 + np.exp(-l))
            pr.p_series = _ps
        w, A, N = B.world_series(pr, a, n_)
        for k, v in w.items(): ws[k] += v * n
        for k, v in A.items(): al[k] += v * n
        for k, v in N.items(): nl[k] += v * n
        used += 1
    norm = lambda d: {ABBR.get(k, str(k)): v / sum(d.values()) for k, v in sorted(d.items(), key=lambda x: -x[1])}
    return dict(ws=norm(ws), al_pennant=norm(al), nl_pennant=norm(nl), n_realizations=used, date=dt.date.today().isoformat(), engine='v2 rot_oct', coef=(coef or M.COEF), series_temperature=temperature)


if __name__ == '__main__':
    ap = argparse.ArgumentParser(); ap.add_argument('--bracket', required=True); ap.add_argument('--out', default='v2_marginalized.json'); ap.add_argument('--season', type=int, default=2026); ap.add_argument('--temperature', type=float, default=1.0, help='series calibration b; 1.0 = raw engine'); ap.add_argument('--config', default=None, help='JSON with a "coef" dict to override PREREG coefficients (e.g. OPERATIVE_2026_CONFIG.json)')
    a = ap.parse_args()
    coef = json.load(open(a.config))['coef'] if a.config else None
    out = price(load_bracket(a.bracket), a.season, temperature=a.temperature, coef=coef)
    json.dump(out, open(a.out, 'w'), indent=1)
    print('WS%:', {k: round(v * 100, 1) for k, v in list(out['ws'].items())[:12]}, file=sys.stderr)
