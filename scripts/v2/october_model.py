"""
October run model v2 — reference implementation.
Frozen 2026-09-22. Do not refit or re-specify before the last out of the World Series.

Usage:
    python october_model.py --selftest          # run acceptance tests
    python october_model.py --field             # print the 2026 field
    python october_model.py --matchup LAD MIL   # one matchup, both home patterns
"""
import json, os, argparse
from itertools import product
import numpy as np, pandas as pd
from scipy.stats import nbinom

HERE = os.path.dirname(os.path.abspath(__file__))
INP  = os.path.join(HERE, 'inputs')

CFG   = json.load(open(os.path.join(INP, 'PREREG_2026_CONFIG.json')))
COEF  = CFG['coef']; INTERCEPT = CFG['intercept']
VARMEAN = CFG['var_over_mean']; HFA_LOGIT = CFG['hfa_logit']

OWN_TERMS = ['z_lineup_xwoba', 'z_lu_kpct']      # own offense
OPP_TERMS = ['z_lu_oaa', 'z_rot_oct']            # opponent prevention

HOME_PATTERN = {7: [1,1,0,0,0,1,1], 5: [1,1,0,0,1], 3: [1,1,1]}


# ---------------------------------------------------------------- inputs
def load_field(path=None):
    """2026 qualifying field with z-scored inputs, indexed by team abbreviation."""
    return pd.read_csv(path or os.path.join(INP, 'PREREG_2026_FIELD.csv')).set_index('ab')


def standardize(df, cols, ddof=1):
    """Within-season z across the qualifying field. THIS IS THE ONLY CORRECT WAY.

    Never standardize against pooled history: the raw inputs drift at league level
    (2026 lineup xwOBA averages .3190 vs a 2015-25 average of .3270), which pushes
    every current team below average -- an artifact, not a read.
    """
    out = df.copy()
    for c in cols:
        out['z_' + c] = (df[c] - df[c].mean()) / df[c].std(ddof=ddof)
    return out


def build_field_from_raw(season=2026, teams=None):
    """Rebuild the z-scored field from the four source files. Use this for the Oct-1
    refresh once the final 12 are known: pass teams=[list of 12 statsapi team ids]."""
    lf = pd.read_csv(os.path.join(INP, 'lineup_features.csv'))
    df = pd.read_csv(os.path.join(INP, 'def_team_features.csv'))
    rr = pd.read_csv(os.path.join(INP, 'rotation_ratings.csv'))
    ci = pd.read_csv(os.path.join(INP, 'CANON_team_inputs.csv'))   # carries lu_kpct
    T = (lf[['season','team','lineup_xwoba']]
         .merge(df[['season','team','lu_oaa']], on=['season','team'])
         .merge(rr[['season','team','rot_oct']], on=['season','team'])
         .merge(ci[['season','team','lu_kpct']], on=['season','team']))
    T = T[T.season == season]
    if teams is not None:
        T = T[T.team.isin(teams)]
        if len(T) != len(teams):
            missing = set(teams) - set(T.team)
            raise ValueError(f'missing inputs for team ids {missing}')
    return standardize(T, ['lineup_xwoba', 'lu_kpct', 'lu_oaa', 'rot_oct'])


# ---------------------------------------------------------------- model
def runs(a, b, F):
    """Expected runs scored by team a against team b."""
    return (INTERCEPT
            + sum(COEF[t] * F.loc[a, t] for t in OWN_TERMS)
            + sum(COEF[t] * F.loc[b, t] for t in OPP_TERMS))


def _pmf(mu, kmax=40):
    p = 1.0 / VARMEAN
    r = mu * p / (1.0 - p)
    return nbinom.pmf(np.arange(kmax + 1), r, p)


def p_game(a, b, F, a_is_home):
    """Analytic, not simulated -- exact and free of RNG drift."""
    pa, pb = _pmf(runs(a, b, F)), _pmf(runs(b, a, F))
    cdf_b = np.concatenate([[0.0], np.cumsum(pb)[:-1]])   # P(B < i)
    win = float((pa * cdf_b).sum())
    tie = float((pa * pb).sum())
    q = np.clip(win + 0.5 * tie, 1e-9, 1 - 1e-9)
    logit = np.log(q / (1 - q)) + (HFA_LOGIT if a_is_home else -HFA_LOGIT)
    return float(1.0 / (1.0 + np.exp(-logit)))


def p_series(a, b, F, best_of, a_hosts=True):
    pat = HOME_PATTERN[best_of]
    if not a_hosts:
        pat = [1 - h for h in pat]
    ps = [p_game(a, b, F, h == 1) for h in pat]
    need = best_of // 2 + 1
    return float(sum(np.prod([ps[i] if w else 1 - ps[i] for i, w in enumerate(o)])
                     for o in product([0, 1], repeat=best_of) if sum(o) >= need))


# ---------------------------------------------------------------- tests
ACCEPTANCE = [
    # a,    b,     R_a,   R_b,   p_home, p_away, p7,     p5
    ('LAD','MIL', 4.087, 3.851, 0.5595, 0.4878, 0.5629, 0.5578),
    ('LAD','CHC', 4.376, 4.031, 0.5693, 0.4977, 0.5842, 0.5761),
    ('MIL','CHC', 3.971, 3.862, 0.5469, 0.4751, 0.5353, 0.5341),
    ('LAD','BAL', 5.059, 3.305, 0.6988, 0.6350, 0.8346, 0.8004),
    ('NYY','TOR', 3.837, 2.997, 0.6253, 0.5557, 0.7018, 0.6786),
    ('CHC','PHI', 4.293, 4.057, 0.5589, 0.4872, 0.5616, 0.5567),
]

def selftest(tol=5e-4):
    F = load_field(); ok = True
    print(f"{'matchup':<12s}{'R_a':>8s}{'R_b':>8s}{'p_home':>9s}{'p_away':>9s}{'p7':>9s}{'p5':>9s}   status")
    for a, b, Ra, Rb, ph, pa_, p7, p5 in ACCEPTANCE:
        got = (runs(a,b,F), runs(b,a,F), p_game(a,b,F,True), p_game(a,b,F,False),
               p_series(a,b,F,7), p_series(a,b,F,5))
        exp = (Ra, Rb, ph, pa_, p7, p5)
        good = all(abs(g - e) < (1e-3 if i < 2 else tol) for i, (g, e) in enumerate(zip(got, exp)))
        ok &= good
        print(f"{a+' v '+b:<12s}" + "".join(f"{g:9.4f}" for g in got) + f"   {'PASS' if good else 'FAIL'}")
    # scalars
    p = 1.0/VARMEAN; r = 4.0*p/(1-p)
    checks = [("intercept", INTERCEPT, 3.9914), ("nb p", p, 0.471965), ("nb r @ mu=4", r, 3.575259),
              ("nb sd @ mu=4", np.sqrt(4.0*VARMEAN), 2.9112),
              ("p_game home at z=0", 1/(1+np.exp(-HFA_LOGIT)), 0.5359)]
    print()
    for nm, got, exp in checks:
        good = abs(got-exp) < 1e-3; ok &= good
        print(f"   {nm:<22s} {got:.6f}  expected {exp:.6f}   {'PASS' if good else 'FAIL'}")
    print("\n" + ("ALL TESTS PASS" if ok else "*** FAILURES -- do not ship ***"))
    return ok


def print_field():
    F = load_field()
    F = F.assign(R_scored=[INTERCEPT + sum(COEF[t]*F.loc[i,t] for t in OWN_TERMS) for i in F.index],
                 R_allowed=[INTERCEPT + sum(COEF[t]*F.loc[i,t] for t in OPP_TERMS) for i in F.index])
    F['net'] = F.R_scored - F.R_allowed
    print(F.sort_values('net', ascending=False)[
        ['z_lineup_xwoba','z_lu_kpct','z_lu_oaa','z_rot_oct','R_scored','R_allowed','net']].round(3).to_string())
    print("\nDo NOT publish the top three as an ordering. Across ten defensible specifications,")
    print("P(best in field) = CHC 0.518, LAD 0.350, MIL 0.131, everyone else 0.000.")


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('--selftest', action='store_true')
    ap.add_argument('--field', action='store_true')
    ap.add_argument('--matchup', nargs=2, metavar=('A','B'))
    ap.add_argument('--best-of', type=int, default=7)
    a = ap.parse_args()
    if a.selftest: raise SystemExit(0 if selftest() else 1)
    if a.field: print_field()
    if a.matchup:
        F = load_field(); x, y = a.matchup
        print(f"{x} {runs(x,y,F):.3f} runs   {y} {runs(y,x,F):.3f} runs")
        print(f"p_game {x} home {p_game(x,y,F,True):.4f}   {x} away {p_game(x,y,F,False):.4f}")
        print(f"p_series best-of-{a.best_of}, {x} hosts: {p_series(x,y,F,a.best_of,True):.4f}")
        print(f"p_series best-of-{a.best_of}, {y} hosts: {1-p_series(y,x,F,a.best_of,True):.4f}")
