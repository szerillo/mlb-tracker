#!/usr/bin/env python3
"""
Compute a unified pitcher quality value: a weighted average of FIP-style
metrics that lands on the same scale as the inputs (≈2.5–6.0, lower = better).
This is *not* a z-score — it's the actual blended "weighted FIP" number.

Reads:  data/pitcher_stats.json       (xera, bot_era, fip_proj, season xfip/siera)
        data/pitcher_gamelogs.json    (per-start l5 + season xfip/siera → rolling)
Writes: data/pitcher_stats.json (enriched with `unified_score`, `unified_tier`,
        `unified_components`, `unified_weight_covered` per pitcher)

Weights (Sean's 5/25 recalibration — rolling-led, projection downweighted):
    rolling xFIP   28%   ← stabilized: 0.6·L5 + 0.4·season-to-date
    rolling SIERA  22%   ← stabilized: 0.6·L5 + 0.4·season-to-date
    xERA           18%   ← independent contact-quality estimator
    botERA         17%   ← independent stuff-model estimator
    projection     15%   ← preseason true-talent (least, now in-season)

Why this blend (from the split-half backtest, 120–144 SP sample):
  • K-BB% is the best single forward indicator — and xFIP/SIERA are ~0.82
    correlated with it, i.e. they ARE a recent-K-BB% model. So a rolling
    xFIP/SIERA-led core leans on the strongest signal.
  • A short pure-L5 window is noisier than season-to-date; a 60/40 L5/season
    blend matched full-season forecast accuracy while staying responsive to
    form/velo/injury changes — hence "stabilized rolling".
  • xERA (r≈0.50 vs xFIP) and botERA add genuinely independent contact/stuff
    information that K-BB% misses, so they earn real weight.
  • Projections add the least *new* in-season info, so they're smallest.

Score = Σ(weight_i × value_i) / Σ(weight_i_available)
        ≈ a single FIP-equivalent number (lower = better).

If a pitcher has < half the total weight in available metrics the score is
omitted (too sparse). Renormalization by available weight keeps the value
scale consistent regardless of which metrics are missing.

Tier thresholds (FIP scale — lower is better):
    val ≤ 3.25  → "Elite"
    val ≤ 3.75  → "Good"
    val ≤ 4.25  → "Avg"
    val ≤ 4.75  → "Bad"
    val >  4.75 → "Worst"

USAGE:
    python scripts/compute_pitcher_score.py
"""
from __future__ import annotations
import datetime
import json
import os
import sys
import unicodedata

HERE      = os.path.dirname(__file__)
INPUT     = os.path.join(HERE, "..", "data", "pitcher_stats.json")
GAMELOGS  = os.path.join(HERE, "..", "data", "pitcher_gamelogs.json")
ROLES     = os.path.join(HERE, "..", "pitcher_roles.json")  # SP/RP classification (repo root)
OUTPUT    = INPUT  # in-place enrichment

# Stabilized-rolling blend for xFIP/SIERA — PER-POPULATION (Fable 2026-08-24):
# SP recency 0.6 (5 starts is real signal); RP recency 0.2 (5 appearances ~10 IP
# is noise). These globals are RP/default; SP 0.6 is passed at the call site by role.
ROLL_L5_WEIGHT     = 0.2
ROLL_SEASON_WEIGHT = 0.8

# IN-SEASON CORE = these four, in their relative proportions (renormalized among
# whichever are available). Rolling xFIP/SIERA lead; xERA/botERA add independent
# contact/stuff info K-BB% misses.
STUFF_ERA_INTERCEPT = 9.48
STUFF_ERA_SLOPE     = 0.0542

COMPONENTS = [
    ("roll_xfip",  0.0),
    ("roll_siera", 40.0),
    ("xera",       21.0),
    ("bot_era",    4.0),
    ("stuff_era",  35.0),
]
# insea_v2 core: legacy-comparison score, and the shape the four non-stuff
# terms renormalize to when stuff_era is absent (no Stuff+, or a reliever).
COMPONENTS_V2 = [
    ("roll_xfip",  20.0),
    ("roll_siera", 40.0),
    ("xera",       25.0),
    ("bot_era",    15.0),
]

# DYNAMIC in-season vs projection weighting (mid-2026 recalibration).
# The projection is no longer a flat 15% for everyone. Instead the IN-SEASON
# core's weight GROWS with the pitcher's actual innings and the projection fades
# (but never to zero): w_season = IP / (IP + K), w_proj = K / (IP + K). K is the
# innings at which in-season data earns 50% of the weight. Relievers get a larger
# K (their per-IP rate stats are noisier and role changes matter), so they lean
# on the projection longer than starters at equal innings.
K_SP = 250.0   # starter half-weight innings
K_RP = 250.0   # reliever half-weight innings
# Reliever half-weight innings is ASYMMETRIC (Fable 2026-08-06 calibration):
# a reliever whose in-season mix beats his projection is showing skill the
# career-prior systems under-trust (stuff/velo/new pitch) -> earn weight fast;
# a reliever below his projection is mostly BABIP/HR noise -> anchor to proj.
K_RP_GOOD = 35.0    # in-season BETTER than fip_proj -> trust it fast
K_RP_BAD = 400.0    # in-season WORSE  than fip_proj -> lean on projection
DEFAULT_K = K_RP
K_GOOD = 15.0   # in-season beats projection -> earn weight fast (SP & RP, Fable 2026-08-24)
K_BAD  = 250.0  # in-season worse/unusable -> lean on projection

# Rolling K-BB% TREND tilt. Sean's ask: use the L5-vs-season K-BB% trend as a
# guide. K-BB% is the best forward rate signal; a pitcher whose recent K-BB% has
# jumped is pitching better than his blended composite (which lags) yet shows.
# We nudge the score by the L5-minus-season K-BB% delta, SMALL and capped (the
# rolling core already carries most recent form, so this is only the residual),
# and SCALED BY THE IN-SEASON WEIGHT so it informs established arms and barely
# touches tiny-sample/projection-driven ones.
KBB_TILT_SLOPE = 0.0   # FIP per point of (L5 - season) K-BB%; sign: better K-BB -> lower FIP
KBB_TILT_CAP   = 0.20   # max |tilt| in FIP units
KBB_TILT_MIN_L5 = 3     # need at least this many recent starts/appearances

# Velo + CSW LEVEL adjustment (historical panel): xFIP UNDER-weights fastball
# velocity and CSW level. Each ~0.049 FIP per unit vs the league mean (per mph;
# per CSW percentage-point). Sign: higher velo / higher CSW -> lower FIP. Applied
# as a post-core level correction, scaled by the in-season weight and capped.
VELOCSW_SLOPE = 0.11
CSW_SLOPE     = 0.03
VELOCSW_CAP   = 0.40    # max |combined adj| in FIP units
VELOCSW_MIN_IP = 20.0   # only for arms with a stabilized velo/CSW sample

MIN_CORE_WEIGHT = 40.0  # need ~half of the 85-pt core (or a projection) to score

# Tier on the FIP scale (lower = better)
TIERS = [
    (3.25, "Elite"),
    (3.75, "Good"),
    (4.25, "Avg"),
    (4.75, "Bad"),
    (float("inf"), "Worst"),
]


def _f(v):
    """Coerce to float, returning None for missing/non-numeric/NaN."""
    if v is None:
        return None
    try:
        v = float(v)
        return v if v == v else None
    except (TypeError, ValueError):
        return None


def _norm(s: str) -> str:
    """Mirror the frontend normName: strip accents/suffixes/periods, lowercase."""
    s = unicodedata.normalize("NFKD", s or "")
    s = "".join(c for c in s if not unicodedata.combining(c)).lower()
    for suf in (" jr", " sr", " iii", " ii"):
        if s.endswith(suf) or s.endswith(suf + "."):
            s = s[: s.rfind(suf)]
            break
    return s.replace(".", "").strip()


def load_roles():
    """normalized name -> 'SP' | 'RP' from pitcher_roles.json (repo root)."""
    try:
        d = json.load(open(ROLES))
    except Exception as e:
        print(f"[score] no pitcher_roles ({e}); all arms use K_RP", file=sys.stderr)
        return {}
    out = {}
    for k, v in (d.get("pitchers") or {}).items():
        rl = (v.get("role") or "").upper()
        out[_norm(v.get("name", k))] = "SP" if rl == "SP" else "RP"
        out[_norm(k)] = out[_norm(v.get("name", k))]
    return out


def _l5_kbb(g):
    """L5 K-BB% (points) from the game-log l5 aggregate; None if too sparse."""
    if not g:
        return None
    l5 = g.get("l5") or {}
    if (g.get("l5_n") or l5.get("n") or 0) < KBB_TILT_MIN_L5:
        return None
    kp, bp = l5.get("k_pct"), l5.get("bb_pct")
    if kp is None or bp is None:
        return None
    return kp - bp


def load_gamelogs():
    """Return {normName: gamelog_entry} or {} if the file is missing/unreadable."""
    if not os.path.exists(GAMELOGS):
        print("[score] no gamelogs file — rolling falls back to season stats",
              file=sys.stderr)
        return {}
    try:
        with open(GAMELOGS) as f:
            gl = json.load(f)
    except Exception as e:
        print(f"[score] gamelogs unreadable ({e}) — rolling falls back to season",
              file=sys.stderr)
        return {}
    out = {}
    for v in (gl.get("pitchers") or {}).values():
        if isinstance(v, dict) and v.get("name"):
            out[_norm(v["name"])] = v
    return out


ROLL_DUMP = os.path.join(HERE, "..", "data", "_fg_roll.json")
ROLL_MIN_IP = 5.0   # need a usable recent sample before trusting a window value


def load_roll():
    """Recent-window (last ~30d) xFIP/SIERA from a committed browser dump. Used
    as the rolling source when FanGraphs' per-start game-log endpoint is 403
    server-side (which nulls l5.xfip/siera). Keyed by normalized name."""
    if not os.path.exists(ROLL_DUMP):
        return {}
    try:
        d = json.load(open(ROLL_DUMP))
        return d.get("pitchers") or {}
    except Exception as e:
        print(f"[score] roll dump unreadable ({e})", file=sys.stderr)
        return {}


def stabilized_roll(stat_key, p, g, r=None, l5w=ROLL_L5_WEIGHT, sew=ROLL_SEASON_WEIGHT):
    """Stabilized rolling value = 0.6·recent + 0.4·season-to-date.
    Source priority for the recent term: (1) gamelog L5 (per-start, preferred),
    (2) the committed recent-window dump _fg_roll.json (used when FG's game-log
    endpoint is blocked and l5 is null), then plain season fallbacks so sparse
    arms still get a sane component."""
    se_stats = _f(p.get(stat_key))  # pitcher_stats season xfip/siera
    # 1. gamelog L5 (per-start)
    if g:
        l5 = _f((g.get("l5") or {}).get(stat_key))
        se = _f((g.get("season") or {}).get(stat_key))
        if l5 is not None and se is not None:
            return l5w * l5 + sew * se
        if l5 is not None:
            return l5
    # 2. recent-window dump (FG game-log blocked) blended with season
    if r is not None and (_f(r.get("ip")) or 0) >= ROLL_MIN_IP:
        rv = _f(r.get(stat_key))
        if rv is not None and se_stats is not None:
            return l5w * rv + sew * se_stats
        if rv is not None:
            return rv
    # 3. season fallbacks
    if g:
        se = _f((g.get("season") or {}).get(stat_key))
        if se is not None:
            return se
    return se_stats


def main() -> int:
    if not os.path.exists(INPUT):
        print(f"[score] no pitcher_stats at {INPUT}", file=sys.stderr)
        return 1

    with open(INPUT) as f:
        d = json.load(f)
    pitchers = d.get("pitchers", {})
    if not pitchers:
        print("[score] no pitchers found", file=sys.stderr)
        return 0

    glby = load_gamelogs()
    rollby = load_roll()
    rolesby = load_roles()
    print(f"[score] loaded {len(pitchers)} pitchers, {len(glby)} gamelog arms, "
          f"{len(rollby)} recent-window arms, {len(rolesby)} role tags", file=sys.stderr)

    # League means for the velo/CSW level adjustment — computed from established
    # arms (>=30 IP) so the reference is stable and auto-updates through the year.
    _vv, _cc = [], []
    for _k, _p in pitchers.items():
        if not isinstance(_p, dict):
            continue
        if (_f(_p.get("ip")) or 0) < 30:
            continue
        _g = glby.get(_k) or glby.get(_norm(_p.get("name", "")))
        _se = (_g or {}).get("season") or {}
        _sv, _sc = _f(_se.get("velo")), _f(_se.get("csw"))
        if _sv is not None: _vv.append(_sv)
        if _sc is not None: _cc.append(_sc)
    LG_VELO = sum(_vv) / len(_vv) if _vv else 93.5
    LG_CSW  = sum(_cc) / len(_cc) if _cc else 0.28
    print(f"[score] velo/CSW league means: {LG_VELO:.1f} mph / {LG_CSW*100:.1f}% (n={len(_vv)}/{len(_cc)})", file=sys.stderr)

    n_scored = n_sparse = n_rolling = 0
    tier_counts = {label: 0 for _, label in TIERS}
    for k, p in pitchers.items():
        if not isinstance(p, dict):
            continue
        g = glby.get(k) or glby.get(_norm(p.get("name", "")))
        # Backfill FG-standard display fields from gamelogs when the FanGraphs
        # leaderboard row is missing (name mismatch or CI block): gives the arm
        # real innings (so w_proj is not pinned to 1.0) and fills the modal /
        # bullpen table (k_bb_pct, stuff_plus, ...). Only fills absent fields.
        if isinstance(g, dict):
            _gs = g.get("season") or {}
            _gl5 = g.get("l5") or {}
            if p.get("mlbam_id") is None and g.get("mlbam_id") is not None:
                p["mlbam_id"] = g.get("mlbam_id")
            if p.get("ip") is None and _gs.get("ip_outs"):
                p["ip"] = round(_f(_gs.get("ip_outs")) / 3.0, 1)
            for _sk in ("k_pct", "bb_pct", "xfip", "siera"):
                if p.get(_sk) is None and _gs.get(_sk) is not None:
                    p[_sk] = _gs.get(_sk)
            if (p.get("k_bb_pct") is None and p.get("k_pct") is not None
                    and p.get("bb_pct") is not None):
                p["k_bb_pct"] = round(_f(p.get("k_pct")) - _f(p.get("bb_pct")), 1)
            _st = _gs.get("stuff")
            if _st is None:
                _st = _gl5.get("stuff")
            if p.get("stuff_plus") is None and _st is not None:
                p["stuff_plus"] = round(_f(_st), 1)
        r = rollby.get(k) or rollby.get(_norm(p.get("name", "")))
        _stuffp = _f(p.get("stuff_plus"))
        # stuffERA is SP-only (unified_v3 caveat: reliever stuff weighting untested).
        _role_sp = (rolesby.get(k) or rolesby.get(_norm(p.get("name", "")))) == "SP"
        _rl5, _rse = (0.6, 0.4) if _role_sp else (0.2, 0.8)   # SP recency 0.6, RP 0.2 (Fable 2026-08-24)
        vals = {
            "roll_xfip":  stabilized_roll("xfip", p, g, r, _rl5, _rse),
            "roll_siera": stabilized_roll("siera", p, g, r, _rl5, _rse),
            "xera":       _f(p.get("xera")),
            "bot_era":    _f(p.get("bot_era")),
            "stuff_era":  (STUFF_ERA_INTERCEPT - STUFF_ERA_SLOPE * _stuffp)
                          if (_stuffp is not None) else None,
        }
        proj = _f(p.get("fip_proj"))

        # --- in-season CORE: available components in their relative proportions ---
        core_sum = core_w_avail = 0.0
        # RP addendum (2026-08-11): relievers get an IP-ramped stuff weight and a
        # resmix that drops xERA; starters keep the static unified_v3 mix.
        comps = COMPONENTS   # SP & RP both 5-way; renorm gives 62/32/6 no-stuff fallback (Fable 2026-08-24)
        components = {}
        for field, weight in comps:
            v = vals[field]
            if v is None:
                continue
            components[field] = round(v, 3)
            core_sum += weight * v
            core_w_avail += weight
        core = core_sum / core_w_avail if core_w_avail else None

        # need enough in-season signal, or a projection to fall back on
        if core is None and proj is None:
            for key in ("unified_score", "unified_tier", "unified_components",
                        "unified_weight_covered", "unified_rolling",
                        "unified_proj_weight"):
                p.pop(key, None)
            n_sparse += 1
            continue
        if core is not None and core_w_avail < MIN_CORE_WEIGHT and proj is None:
            for key in ("unified_score", "unified_tier", "unified_components",
                        "unified_weight_covered", "unified_rolling",
                        "unified_proj_weight"):
                p.pop(key, None)
            n_sparse += 1
            continue

        # --- DYNAMIC in-season vs projection weight, driven by innings + role ---
        ip = _f(p.get("ip")) or 0.0
        role = rolesby.get(k) or rolesby.get(_norm(p.get("name", "")))
        if core is not None and proj is not None and core < proj:
            K = K_GOOD   # in-season beats projection -> earn weight fast (SP & RP)
        else:
            K = K_BAD    # in-season worse (or unusable) -> lean on projection
        if core is None:                      # no in-season metrics -> pure projection
            w_season, w_proj = 0.0, 1.0
            score = proj
        elif proj is None:                    # no projection -> pure in-season
            w_season, w_proj = 1.0, 0.0
            score = core
        else:
            w_proj   = K / (ip + K)
            w_season = 1.0 - w_proj
            score = w_season * core + w_proj * proj

        # --- rolling K-BB% trend nudge (scaled by the in-season weight) ---
        kbb_tilt = 0.0
        l5_kbb = _l5_kbb(g)
        season_kbb = _f(p.get("k_bb_pct"))
        if l5_kbb is not None and season_kbb is not None and core is not None:
            raw = -KBB_TILT_SLOPE * (l5_kbb - season_kbb)     # better K-BB -> lower FIP
            raw = max(-KBB_TILT_CAP, min(KBB_TILT_CAP, raw))
            kbb_tilt = raw * w_season                          # informs established arms most
            score += kbb_tilt

        # --- velo + CSW LEVEL adjustment (historical panel): xFIP underweights
        # fastball velocity + CSW level. Scaled by in-season weight, capped. ---
        velocsw_adj = 0.0
        if core is not None and g and (_f(p.get("ip")) or 0) >= VELOCSW_MIN_IP:
            _se = g.get("season") or {}
            _sv, _sc = _f(_se.get("velo")), _f(_se.get("csw"))
            a = 0.0
            if _sv is not None: a += -VELOCSW_SLOPE * (_sv - LG_VELO)
            if _sc is not None: a += -CSW_SLOPE * (_sc * 100.0 - LG_CSW * 100.0)
            a = max(-VELOCSW_CAP, min(VELOCSW_CAP, a))
            velocsw_adj = a * w_season
            score += velocsw_adj

        # --- legacy score (old 0.6/0.4 blend, no velo/CSW adj) kept visible so the
        # shift from this recalibration can be eyeballed for a few days. ---
        core_leg = None
        _cs = _cw = 0.0
        _valsL = {"roll_xfip": stabilized_roll("xfip", p, g, r, 0.6, 0.4),
                  "roll_siera": stabilized_roll("siera", p, g, r, 0.6, 0.4),
                  "xera": vals["xera"], "bot_era": vals["bot_era"]}
        for field, weight in COMPONENTS_V2:
            v = _valsL[field]
            if v is None: continue
            _cs += weight * v; _cw += weight
        core_leg = _cs / _cw if _cw else None
        if core_leg is None and proj is None:
            score_legacy = None
        elif core_leg is None:
            score_legacy = proj
        elif proj is None:
            score_legacy = core_leg
        else:
            score_legacy = w_season * core_leg + w_proj * proj
        if score_legacy is not None and kbb_tilt:
            score_legacy += kbb_tilt

        # keep the projection visible in the component breakdown
        if proj is not None:
            components["fip_proj"] = round(proj, 3)

        tier = TIERS[-1][1]
        for thr, label in TIERS:
            if score <= thr:
                tier = label
                break

        # True only when the rolling xFIP/SIERA actually came from L5 game logs
        # (so the frontend can mark the score as form-weighted).
        rolling = bool((g and (g.get("l5") or {}).get("xfip") is not None)
                       or (r and (_f(r.get("ip")) or 0) >= ROLL_MIN_IP
                           and r.get("xfip") is not None))
        if rolling:
            n_rolling += 1

        p["unified_score"]          = round(score, 2)
        p["unified_tier"]           = tier
        p["unified_components"]     = components
        p["unified_weight_covered"] = round(core_w_avail + (15.0 if proj is not None else 0.0), 1)
        p["unified_rolling"]        = rolling
        p["unified_proj_weight"]    = round(w_proj, 3)
        if kbb_tilt:
            p["unified_kbb_tilt"] = round(kbb_tilt, 3)
        else:
            p.pop("unified_kbb_tilt", None)
        if velocsw_adj:
            p["unified_velocsw_adj"] = round(velocsw_adj, 3)
        else:
            p.pop("unified_velocsw_adj", None)
        if score_legacy is not None:
            p["unified_score_legacy"] = round(score_legacy, 2)   # old 0.6/0.4, no velo/CSW
        else:
            p.pop("unified_score_legacy", None)
        n_scored += 1
        tier_counts[tier] = tier_counts.get(tier, 0) + 1

    d.setdefault("scoring", {})
    d["scoring"]["unified_score"] = {
        "computed_at": datetime.datetime.utcnow().isoformat() + "Z",
        "scale": "FIP (weighted average — lower = better)",
        "method": ("rolling-led in-season core (stabilized 0.6*L5 + 0.4*season "
                   "xFIP/SIERA + xERA/botERA), blended vs projection with a "
                   "DYNAMIC weight w_season = IP/(IP+K) that grows with innings "
                   f"(K_good={K_GOOD:g}/K_bad={K_BAD:g}, SP+RP asymmetric); plus a small rolling K-BB% "
                   "trend nudge; plus a velo+CSW LEVEL adjustment (historical-panel recalibration)."),
        "core_weights": {f: w for f, w in COMPONENTS},
        "stuff_era": {"intercept": STUFF_ERA_INTERCEPT, "slope_per_stuff_plus": STUFF_ERA_SLOPE,
                      "form": "stuffERA = 9.48 - 0.0542*Stuff+", "insea_share": 0.35,
                      "scope": "SP+RP", "fallback": "no Stuff+ -> drop term; non-stuff renormalize to 62/32/6"},
        "dynamic_projection": {"K_good": K_GOOD, "K_bad": K_BAD, "populations": "SP+RP",
                               "form": "w_proj = K/(IP+K); w_season = 1 - w_proj"},
        "kbb_tilt": {"slope_fip_per_pt": KBB_TILT_SLOPE, "cap": KBB_TILT_CAP,
                     "min_l5": KBB_TILT_MIN_L5, "scaled_by": "w_season"},
        "rolling_blend": {"l5": ROLL_L5_WEIGHT, "season": ROLL_SEASON_WEIGHT,
                          "note": "was 0.6/0.4; recalibrated on the 2023-26 historical panel (purged CV)"},
        "velocsw_adj": {"slope_fip_per_unit": VELOCSW_SLOPE, "cap": VELOCSW_CAP,
                        "min_ip": VELOCSW_MIN_IP, "lg_velo": round(LG_VELO,1), "lg_csw_pct": round(LG_CSW*100,1),
                        "scaled_by": "w_season", "form": "-slope*(velo-lg) -slope*(CSW%-lg)"},
        "tier_thresholds": [{"max_val": t, "label": l}
                            for t, l in TIERS if t < float("inf")],
        "n_scored": n_scored,
        "n_rolling": n_rolling,
        "n_too_sparse": n_sparse,
        "tier_counts": tier_counts,
    }

    with open(OUTPUT, "w") as f:
        json.dump(d, f, indent=2)

    # --- Also emit a slim wFIP lookup for the Google-Sheet pull (name -> unified_score)
    try:
        def _wnorm(s):
            s = unicodedata.normalize("NFKD", str(s or ""))
            s = "".join(c for c in s if not unicodedata.combining(c)).lower()
            s = "".join(c for c in s if c.isalpha() or c == " ")
            return " ".join(s.split())
        _wmap = {}
        for _wk, _wv in pitchers.items():
            _us = _wv.get("unified_score")
            if _us is None:
                continue
            _nm = _wnorm(_wv.get("name", _wk))
            if _nm:
                _wmap[_nm] = round(float(_us), 2)
        _wpath = os.path.join(HERE, "..", "data", "wfip_lookup.json")
        with open(_wpath, "w") as _wf:
            json.dump({
                "generated_at": datetime.datetime.utcnow().isoformat(timespec="seconds") + "Z",
                "source": "pitcher_stats.json unified_score (wFIP composite)",
                "n": len(_wmap),
                "wfip": _wmap,
            }, _wf)
        print(f"[score] wrote wfip_lookup.json ({len(_wmap)} pitchers)")
    except Exception as _we:
        print(f"[score] WARN: wfip_lookup emit failed: {_we}", file=sys.stderr)
    print(f"[score] scored {n_scored} pitchers "
          f"({n_rolling} with L5 rolling, {n_sparse} too sparse)", file=sys.stderr)
    print("[score] tiers: " + ", ".join(f"{l}={n}" for l, n in tier_counts.items()),
          file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
