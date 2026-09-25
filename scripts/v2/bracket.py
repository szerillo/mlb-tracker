"""
Bracket layer for the October run model v2.

Turns team-level net runs into pennant / World Series probabilities through the
actual postseason structure (byes, seeding, series lengths, home patterns), and
marginalizes over bracket uncertainty while the regular season is still running.

This matters more than it sounds: a global talent exponent cannot represent
"2-seed with a bye" vs "3-seed into the wild-card round". That structural gap is
most of the apparent LAD-vs-ATL trade-off.

Usage:
    python bracket.py --seeds            # WS probs for the current most-likely bracket
    python bracket.py --marginalize      # marginalize over remaining-season uncertainty
"""
import json, os, argparse, collections
from itertools import product
import numpy as np, pandas as pd
from scipy.stats import nbinom

import october_model as M

HERE = os.path.dirname(os.path.abspath(__file__))
INP  = os.path.join(HERE, '..', 'inputs')

# best-of-N -> home pattern from the HIGHER seed's perspective
HOME_PATTERN = {7: [1,1,0,0,0,1,1], 5: [1,1,0,0,1], 3: [1,1,1]}
WC_LEN, LDS_LEN, LCS_LEN, WS_LEN = 3, 5, 7, 7


def _pmf(mu, kmax=40):
    p = 1.0 / M.VARMEAN
    return nbinom.pmf(np.arange(kmax + 1), max(mu, 0.5) * p / (1 - p), p)


class Pricer:
    """Caches game probabilities for one field."""
    def __init__(self, F, pct, coef=None, intercept=None, own=None, opp=None):
        self.F = F                                  # indexed by team key
        self.pct = pct                              # team key -> reg season win pct (home field)
        self.coef = coef if coef is not None else np.array(
            [M.COEF[t] for t in M.OWN_TERMS + M.OPP_TERMS])
        self.intercept = intercept if intercept is not None else M.INTERCEPT
        self.own = own or M.OWN_TERMS
        self.opp = opp or M.OPP_TERMS
        self._g = {}

    def runs(self, a, b):
        n = len(self.own)
        return (self.intercept
                + sum(self.coef[i] * self.F.loc[a, self.own[i]] for i in range(n))
                + sum(self.coef[n + i] * self.F.loc[b, self.opp[i]] for i in range(len(self.opp))))

    def p_game(self, a, b, a_home):
        k = (a, b, a_home)
        if k in self._g: return self._g[k]
        pa, pb = _pmf(self.runs(a, b)), _pmf(self.runs(b, a))
        cdf_b = np.concatenate([[0.0], np.cumsum(pb)[:-1]])
        q = np.clip(float((pa * cdf_b).sum()) + 0.5 * float((pa * pb).sum()), 1e-9, 1 - 1e-9)
        v = 1.0 / (1.0 + np.exp(-(np.log(q / (1 - q)) + (M.HFA_LOGIT if a_home else -M.HFA_LOGIT))))
        self._g[k] = v
        return v

    def p_series(self, a, b, best_of, a_hosts=True):
        pat = HOME_PATTERN[best_of]
        if not a_hosts: pat = [1 - h for h in pat]
        ps = [self.p_game(a, b, h == 1) for h in pat]
        need = best_of // 2 + 1
        return float(sum(np.prod([ps[i] if w else 1 - ps[i] for i, w in enumerate(o)])
                         for o in product([0, 1], repeat=best_of) if sum(o) >= need))


def league_probs(pr, seeds):
    """seeds: list of 6 team keys, index 0 = 1-seed. Returns {team: P(pennant)}."""
    s = {i + 1: t for i, t in enumerate(seeds)}
    out = collections.defaultdict(float)
    p36 = pr.p_series(s[3], s[6], WC_LEN, True)     # wild card, all games at higher seed
    p45 = pr.p_series(s[4], s[5], WC_LEN, True)
    for w36, q36 in ((s[3], p36), (s[6], 1 - p36)):
        for w45, q45 in ((s[4], p45), (s[5], 1 - p45)):
            pA = pr.p_series(s[1], w45, LDS_LEN, True)   # 1 seed hosts
            pB = pr.p_series(s[2], w36, LDS_LEN, True)   # 2 seed hosts
            for a, qa in ((s[1], pA), (w45, 1 - pA)):
                for b, qb in ((s[2], pB), (w36, 1 - pB)):
                    pL = pr.p_series(a, b, LCS_LEN, pr.pct[a] >= pr.pct[b])
                    for champ, qc in ((a, pL), (b, 1 - pL)):
                        out[champ] += q36 * q45 * qa * qb * qc
    return dict(out)


def world_series(pr, al_seeds, nl_seeds):
    AL, NL = league_probs(pr, al_seeds), league_probs(pr, nl_seeds)
    ws = collections.defaultdict(float)
    for a, pa in AL.items():
        for b, pb in NL.items():
            q = pr.p_series(a, b, WS_LEN, pr.pct[a] >= pr.pct[b])
            ws[a] += pa * pb * q
            ws[b] += pa * pb * (1 - q)
    return dict(ws), AL, NL


def marginalize(bracket_dist, field_builder, pct, min_weight=0.002):
    """bracket_dist: [[al_seeds, nl_seeds, count], ...] from a season simulation.
    field_builder(team_ids) -> z-scored field DataFrame indexed the same way.
    Re-standardizes per realization, because the z-scores depend on who qualifies."""
    total = sum(e[2] for e in bracket_dist)
    acc = collections.defaultdict(float)
    for al, nl, n in bracket_dist:
        if n / total < min_weight: continue
        F = field_builder(al + nl)
        pr = Pricer(F, pct)
        ws, _, _ = world_series(pr, al, nl)
        for t, v in ws.items(): acc[t] += v * n
    s = sum(acc.values())
    return {t: v / s for t, v in acc.items()}


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('--seeds', action='store_true')
    ap.add_argument('--marginalize', action='store_true')
    a = ap.parse_args()
    print("See BRACKET_MARGINALIZED_2026-09-22.md for the current numbers.")
    print("Wire field_builder to october_model.build_field_from_raw and pass a")
    print("bracket distribution from your own remaining-schedule simulation.")
