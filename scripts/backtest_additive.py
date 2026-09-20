#!/usr/bin/env python3
"""
backtest_additive.py
====================
Full-archive backtest that answers "is the additive (more SP weight) justified?"
using ONLY the parts that are trustworthy from archived data.

Runs INSIDE the mlb-tracker repo (all inputs local under data/). No network.
Writes: data/additive_backtest.json   Prints: a human-readable report.

Scope note (2026-09-20)
-----------------------
An EXACT historical reconstruction of the sheet additive is NOT possible from
the archive: the daily folders don't store the sheet's Last-Four feeds
(dated FLD/BSR, offense-by-game) and the banked First-Five carries tilt/anchor
adjustments that aren't recoverable. Attempted reconstruction validated at only
MAE ~0.04 vs the live sheet additive -> too loose for a head-to-head grade.
=> The exact additive-vs-old head-to-head comes from the LIVE bake-off archive
   (Model!AC/AD/AE + fg_bakeoff_archive), accumulating from 2026-09-19 forward.
This script therefore reports the two tests that need NO reconstruction:

  A) SP-RESPONSE SLOPE TEST  (the mechanism: does the model under-weight SP?)
     - beta_outcome : how much game OUTCOMES swing per unit of SP-quality gap
     - beta_model   : how much the model's WP swings per the same gap
     If beta_outcome > beta_model with z>~2, the model under-weights the SP axis
     -> banking the game-specific First-Five (the additive) is justified.
     (Extends Fable's z=3.7 / 726g finding across the full window.)

  B) MODEL GRADE  (the bar to beat)
     - vs results : log-loss, Brier, calibration slope, home-win-rate anchor
     - vs CLOSE   : de-vigged closing moneyline; does the model beat the close?
                    (log-loss model vs market on the shared sample) + edge calib.
"""

import json, os, math, sys, datetime

# ---------------------------------------------------------------- config
DATA_DIR    = os.environ.get("MLB_DATA_DIR", "data")
ARCHIVE_DIR = os.path.join(DATA_DIR, "archive")
START_DATE  = "2026-07-09"                 # clean archive begins here
OUT_PATH    = os.path.join(DATA_DIR, "additive_backtest.json")

try:
    import numpy as np
    HAVE_NP = True
except Exception:
    HAVE_NP = False

# ---------------------------------------------------------------- helpers
def clip(p, lo=1e-6, hi=1-1e-6):
    return max(lo, min(hi, p))

def logit(p):
    p = clip(p); return math.log(p / (1 - p))

def amer_to_prob(odds):
    if odds is None: return None
    o = float(odds)
    return (-o) / ((-o) + 100.0) if o < 0 else 100.0 / (o + 100.0)

def brier(pred, actual):
    return sum((p - a) ** 2 for p, a in zip(pred, actual)) / len(pred)

def log_loss(pred, actual):
    s = 0.0
    for p, a in zip(pred, actual):
        p = clip(p); s += -(a * math.log(p) + (1 - a) * math.log(1 - p))
    return s / len(pred)

def ols_slope(x, y):
    n = len(x)
    if n < 3: return float("nan")
    mx, my = sum(x)/n, sum(y)/n
    sxx = sum((xi-mx)**2 for xi in x)
    if sxx == 0: return float("nan")
    return sum((xi-mx)*(yi-my) for xi, yi in zip(x, y)) / sxx

def logistic_fit(x, y):
    """IRLS logistic y ~ 1 + x -> (slope, slope_se)."""
    if not HAVE_NP:
        return _logit_fit_pure(x, y)
    X = np.column_stack([np.ones(len(x)), np.asarray(x, float)])
    y = np.asarray(y, float); beta = np.zeros(2)
    for _ in range(60):
        eta = X @ beta; mu = 1/(1+np.exp(-eta))
        W = np.clip(mu*(1-mu), 1e-9, None); z = eta + (y-mu)/W
        XtW = X.T*W
        try: nb = np.linalg.solve(XtW@X, XtW@z)
        except np.linalg.LinAlgError: break
        if np.max(np.abs(nb-beta)) < 1e-9: beta = nb; break
        beta = nb
    eta = X @ beta; mu = 1/(1+np.exp(-eta)); W = np.clip(mu*(1-mu), 1e-9, None)
    cov = np.linalg.inv((X.T*W)@X)
    return float(beta[1]), float(math.sqrt(cov[1,1]))

def _logit_fit_pure(x, y):
    b0 = b1 = 0.0; n = len(x); lr = 0.01
    for _ in range(20000):
        g0 = g1 = 0.0
        for xi, yi in zip(x, y):
            mu = 1/(1+math.exp(-(b0+b1*xi))); g0 += mu-yi; g1 += (mu-yi)*xi
        b0 -= lr*g0/n; b1 -= lr*g1/n
    return b1, float("nan")

def calibration(pred, actual, nbins=10):
    bins = []
    for i in range(nbins):
        lo, hi = i/nbins, (i+1)/nbins
        idx = [j for j, p in enumerate(pred)
               if p >= lo and (p < hi or (i == nbins-1 and p <= hi))]
        if not idx: continue
        bins.append({"bin": f"{lo:.1f}-{hi:.1f}", "n": len(idx),
                     "pred": round(sum(pred[j] for j in idx)/len(idx), 4),
                     "actual": round(sum(actual[j] for j in idx)/len(idx), 4)})
    slope = (ols_slope([b["pred"] for b in bins], [b["actual"] for b in bins])
             if len(bins) >= 3 else float("nan"))
    return bins, slope

def load_json(path):
    try:
        with open(path) as f: return json.load(f)
    except Exception: return None

def _is_date(s):
    try: datetime.date.fromisoformat(s); return True
    except Exception: return False

def dated_folders(start=START_DATE):
    out = []
    if not os.path.isdir(ARCHIVE_DIR): return out
    for name in sorted(os.listdir(ARCHIVE_DIR)):
        p = os.path.join(ARCHIVE_DIR, name)
        if os.path.isdir(p) and name >= start and _is_date(name):
            out.append((name, p))
    return out

def load_results():
    """game_pk(str) -> home_win. From pregame_backtest.json."""
    pb = load_json(os.path.join(DATA_DIR, "pregame_backtest.json"))
    res = {}
    if pb and isinstance(pb.get("games"), list):
        for g in pb["games"]:
            if g.get("actual_home_win") is None: continue
            res[str(g.get("game_pk"))] = int(g["actual_home_win"])
    return res

def close_market_wp(close, pk):
    """De-vigged home win prob from the closing moneyline for game_pk."""
    if not close: return None
    games = close.get("games", close)
    g = None
    if isinstance(games, list):
        g = next((x for x in games if str(x.get("game_pk")) == pk), None)
    elif isinstance(games, dict):
        g = games.get(pk)
    if not isinstance(g, dict): return None
    ml = g.get("moneyline") or {}
    ph = amer_to_prob((ml.get("home") or {}).get("odds"))
    pa = amer_to_prob((ml.get("away") or {}).get("odds"))
    if ph is None or pa is None or (ph + pa) == 0: return None
    return ph / (ph + pa)

# ---------------------------------------------------------------- main
def main():
    results = load_results()
    rows = []
    miss = {"pregame": 0, "result": 0, "close": 0}

    for date, folder in dated_folders():
        pg = load_json(os.path.join(folder, "pregame_inputs.json"))
        close = load_json(os.path.join(folder, "closing_odds.json"))
        if not pg or not pg.get("games"):
            miss["pregame"] += 1; continue
        for pk, g in pg["games"].items():
            pk = str(pk); m = g.get("model") or {}
            if not m.get("confirmed"): continue
            comp = m.get("components"); owp = m.get("home_wp")
            if comp is None or owp is None: continue
            hw = results.get(pk)
            if hw is None:
                miss["result"] += 1; continue
            try:
                sp_gap = comp["away"]["staff"]["spRA"] - comp["home"]["staff"]["spRA"]
            except Exception:
                sp_gap = None
            mkt = close_market_wp(close, pk)
            if mkt is None: miss["close"] += 1
            rows.append({"date": date, "pk": pk, "old_wp": owp,
                         "sp_gap": sp_gap, "home_win": hw, "mkt_wp": mkt})

    if not rows:
        print("No usable rows â run from repo root (data/archive present)."); sys.exit(1)

    y = [r["home_win"] for r in rows]
    old = [clip(r["old_wp"]) for r in rows]
    home_rate = sum(y) / len(y)
    old_ll, old_br = log_loss(old, y), brier(old, y)
    old_bins, old_cal = calibration(old, y)

    # A) SP-response slope
    sr = [r for r in rows if r["sp_gap"] is not None]
    xs = [r["sp_gap"] for r in sr]
    beta_out, se_out = logistic_fit(xs, [r["home_win"] for r in sr])
    beta_model = ols_slope(xs, [logit(r["old_wp"]) for r in sr])
    z_under = ((beta_out - beta_model) / se_out
               if se_out and not math.isnan(se_out) else float("nan"))

    # B vs close
    sh = [r for r in rows if r["mkt_wp"] is not None]
    vs_close = None
    if sh:
        shy = [r["home_win"] for r in sh]
        m_ll = log_loss([clip(r["old_wp"]) for r in sh], shy)
        k_ll = log_loss([clip(r["mkt_wp"]) for r in sh], shy)
        edges = [r["old_wp"] - r["mkt_wp"] for r in sh]
        eb = []
        for lo, hi in [(-1, -0.05), (-0.05, -0.02), (-0.02, 0.02), (0.02, 0.05), (0.05, 1)]:
            idx = [i for i, e in enumerate(edges) if lo <= e < hi]
            if not idx: continue
            eb.append({"edge_band": f"{lo:+.2f},{hi:+.2f}", "n": len(idx),
                       "mean_model_edge": round(sum(edges[i] for i in idx)/len(idx), 4),
                       "actual_minus_mkt": round(
                           sum(sh[i]["home_win"] - sh[i]["mkt_wp"] for i in idx)/len(idx), 4)})
        vs_close = {"n": len(sh), "model_log_loss": round(m_ll, 4),
                    "market_log_loss": round(k_ll, 4),
                    "model_minus_market": round(m_ll - k_ll, 4),
                    "mean_abs_model_vs_market": round(sum(abs(e) for e in edges)/len(edges), 4),
                    "edge_calibration": eb,
                    "note": "model_minus_market < 0 => model beats the close"}

    summary = _clean({
        "generated_at": datetime.datetime.utcnow().isoformat() + "Z",
        "window": {"start": START_DATE, "n_games": len(rows)},
        "home_win_rate": round(home_rate, 4),
        "sp_slope_test": {
            "n": len(sr),
            "beta_outcome": round(beta_out, 4),
            "beta_outcome_se": round(se_out, 4) if not math.isnan(se_out) else None,
            "beta_model": round(beta_model, 4),
            "z_model_underweights_SP": round(z_under, 2) if not math.isnan(z_under) else None,
            "verdict": ("model UNDER-weights SP (additive justified)"
                        if (not math.isnan(z_under) and z_under > 2) else
                        "no significant SP under-weighting" if not math.isnan(z_under) else "n/a"),
            "note": "beta_outcome > beta_model with z>~2 => additive (more SP weight) justified",
        },
        "model_vs_results": {"n": len(rows), "log_loss": round(old_ll, 4),
                             "brier": round(old_br, 4),
                             "calibration_slope": round(old_cal, 3),
                             "reliability": old_bins},
        "model_vs_close": vs_close,
        "exact_head_to_head": "see fg_bakeoff_archive tab (live additive vs old, from 2026-09-19)",
        "coverage_gaps": miss,
    })
    with open(OUT_PATH, "w") as f:
        json.dump(summary, f, indent=2)
    _report(summary)


def _clean(o):
    if isinstance(o, float):
        return None if (math.isnan(o) or math.isinf(o)) else o
    if isinstance(o, dict): return {k: _clean(v) for k, v in o.items()}
    if isinstance(o, list): return [_clean(v) for v in o]
    return o


def _report(s):
    p = print
    p("=" * 64)
    p(f"ADDITIVE BACKTEST  |  {s['window']['start']} -> present  |  {s['window']['n_games']} games")
    p("=" * 64)
    t = s["sp_slope_test"]
    p("\n[A] SP-RESPONSE SLOPE  (does the model under-weight SP?)")
    p(f"    outcome slope : {t['beta_outcome']}  (se {t['beta_outcome_se']})")
    p(f"    model slope   : {t['beta_model']}")
    p(f"    z             : {t['z_model_underweights_SP']}   -> {t['verdict']}")
    r = s["model_vs_results"]
    p("\n[B] MODEL vs RESULTS")
    p(f"    log-loss {r['log_loss']} | brier {r['brier']} | calib slope {r['calibration_slope']}")
    p(f"    home-win rate {s['home_win_rate']}  (anchor ~0.529-0.531)")
    c = s["model_vs_close"]
    if c:
        p("\n[B] MODEL vs CLOSE")
        p(f"    n={c['n']}  model LL {c['model_log_loss']}  vs  market LL {c['market_log_loss']}")
        p(f"    model - market = {c['model_minus_market']}  ({'model beats close' if c['model_minus_market']<0 else 'close beats model'})")
        p(f"    mean |model-market| = {c['mean_abs_model_vs_market']}")
    p(f"\nexact additive head-to-head: {s['exact_head_to_head']}")
    p(f"coverage gaps: {s['coverage_gaps']}")
    p(f"\nwrote {OUT_PATH}")


if __name__ == "__main__":
    main()
