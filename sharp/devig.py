"""Vig removal (spec §5). Three methods, all implemented; the method per sport/market is chosen by calibration, not preference.

All functions take a list of raw implied probabilities (1/decimal) for every outcome of one market and return fair probabilities summing to 1.
"""
import math


def proportional(imps):
    s = sum(imps)
    return [x / s for x in imps]


def power(imps, tol=1e-10):
    """p_i = imp_i ** k, k solved so sum = 1 (bisection on k in [0.5, 5])."""
    if sum(imps) <= 1: return proportional(imps)
    lo, hi = 0.5, 5.0
    f = lambda k: sum(x ** k for x in imps) - 1
    for _ in range(200):
        mid = (lo + hi) / 2
        if f(mid) > 0: lo = mid
        else: hi = mid
        if hi - lo < tol: break
    k = (lo + hi) / 2
    return proportional([x ** k for x in imps])


def shin(imps, tol=1e-10):
    """Shin (1993): overround as the share z of informed traders. Closed-form for 2 outcomes; iterate on z for n."""
    s = sum(imps); n = len(imps)
    if s <= 1: return proportional(imps)
    # p_i = (sqrt(z^2 + 4(1-z) imp_i^2 / s) - z) / (2(1-z)); z solved so sum p = 1
    def probs(z):
        return [(math.sqrt(z * z + 4 * (1 - z) * x * x / s) - z) / (2 * (1 - z)) for x in imps]
    lo, hi = 0.0, 0.5
    for _ in range(200):
        z = (lo + hi) / 2
        if sum(probs(z)) > 1: lo = z
        else: hi = z
        if hi - lo < tol: break
    return proportional(probs((lo + hi) / 2))


METHODS = {'proportional': proportional, 'power': power, 'shin': shin}


def devig(imps, method='power'):
    imps = [float(x) for x in imps]
    if any(x <= 0 for x in imps): raise ValueError('non-positive implied probability')
    return METHODS[method](imps)


def devig_pair(imp_side, imp_other, method='power'):
    """Fair probability of `side` given the two raw implieds of a two-way market."""
    return devig([imp_side, imp_other], method)[0]


if __name__ == '__main__':
    for imps in ([0.5238, 0.5238], [0.6667, 0.4000], [0.90, 0.16], [0.45, 0.30, 0.30]):
        print(imps, {m: [round(p, 4) for p in f(imps)] for m, f in METHODS.items()})
