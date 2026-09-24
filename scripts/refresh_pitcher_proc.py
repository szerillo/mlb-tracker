#!/usr/bin/env python3
"""
refresh_pitcher_proc.py — starter "process index" (proc_z) and the RA9 base-rate
adjustment it implies. Fable, 2026-09-24 (SP_PROCESS_INDEX_SIX_SEASONS).

Six seasons (2021-2026, ~4.2M pitches, 13,195 SP-snapshots) show that a starter
whose last 14 days beat his own prior baseline on the things he controls out-pitches
that baseline over the next month, six of six seasons: pooled -0.089 runs/9 per SD of
the index on next-30-day RA9 (t -3.67), velocity-led, flat across the next five starts
(a change in current ability, not a hot streak).

INDEX (proc_z)
    For each starter, 14-day window vs. his own prior-46-day baseline (days 15-60) on:
        Δ fastball velo, Δ whiff%, Δ CSW%, Δ (K-BB%)         [pitch/PA weighted]
    plus the 14-day pitches-per-start level (the manager's leash — a deep-start
    allowance is information about current form the aggregates don't have; §6).
    Each component is standardized across the SP population, sign-oriented so
    positive = better process, averaged, re-standardized, and clipped to +/-2.5.

    NOTE — this ships the 5-component index, not Fable's proc6. The sixth component,
    secondary-pitch velocity, needs per-start pitch-type velocity, which the repo's
    gamelogs don't carry (only overall `velo` + pitch-type shares in `mix`). Because
    proc6 (-0.108) is a modest step over proc4 (-0.089), we ship the conservative,
    fully-computable index and coefficient rather than approximate the missing term.

ADJUSTMENT (SP only)
    unified_proc = unified_score - PROC_COEF * proc_z          # higher proc -> lower RA9
    PROC_COEF = 0.09   runs/9 per SD; pooled 2021-2026, t -3.67. This is the value Fable
                       would defend as *incremental to unified* — the wFIP composite
                       already carries a rolling channel, so 0.09 is deliberately inside
                       the 2026-only estimate (-0.185) rather than a naive add.
                       Refit each November on the prior six seasons; drop if pooled |t| < 2.5.

    We DO NOT mutate unified_score (the ranking value stays clean and RPs are untouched);
    we publish unified_proc alongside it. index.html reads unified_proc as the SP's RA9
    base in _pgStaff (spRA); everything else keeps reading unified_score.

    proc_z is written for every graded SP regardless of the adjustment, so October 2026
    grades the coefficient against outcomes (the anchors archive freezes it daily).

Reads:  data/pitcher_gamelogs.json  (per-start dated velo/whiff/csw/k/bb/tbf/pitches)
        data/pitcher_roles.json     (SP set)
        data/pitcher_stats.json     (unified_score to adjust; enriched in place)
Writes: data/pitcher_stats.json     (+ proc_z, proc_components, unified_proc on SP rows)
        data/pitcher_proc.json      (slim feed + population stats, for audit/2027 grading)
"""
from __future__ import annotations
import datetime, json, math, statistics, sys
from pathlib import Path

REPO_ROOT   = Path(__file__).resolve().parent.parent
GL_FILE     = REPO_ROOT / "data" / "pitcher_gamelogs.json"
ROLES_FILE  = REPO_ROOT / "data" / "pitcher_roles.json"
STATS_FILE  = REPO_ROOT / "data" / "pitcher_stats.json"
OUT_FILE    = REPO_ROOT / "data" / "pitcher_proc.json"

PROC_COEF   = 0.09   # runs/9 per SD of proc_z; pooled 2021-2026 (t -3.67). Refit each Nov; drop if |t|<2.5.
CLIP        = 2.5    # clip proc_z to +/-CLIP SD
WIN_RECENT  = 14     # days: recent window
WIN_BASE_LO = 15     # days: baseline window is [15, 60) before ref
WIN_BASE_HI = 60
MIN_START_IP = 2.5   # an appearance counts as a start only if it went >= this many IP
MIN_PITCHES  = 20    # ignore tiny cameos when weighting


def _num(v):
    try:
        f = float(v)
        return f if f == f else None
    except (TypeError, ValueError):
        return None


def _date(s):
    try:
        return datetime.date.fromisoformat(str(s)[:10])
    except Exception:
        return None


def _win_agg(starts):
    """Pitch/PA-weighted window aggregate over a list of start dicts.
    Returns {velo, whiff, csw, kbb, ppg, n, pitches} or None if empty."""
    sp = sw = svelo = swhiff = scsw = 0.0     # weighted sums (by pitches)
    sk = sbb = stbf = 0.0                      # counts for K-BB
    n = 0; tot_pitches = 0.0
    for s in starts:
        pit = _num(s.get("pitches")) or 0.0
        if pit < MIN_PITCHES:
            continue
        n += 1; tot_pitches += pit
        for acc, key in ((0, "velo"), (1, "whiff"), (2, "csw")):
            v = _num(s.get(key))
            if v is not None:
                if acc == 0: svelo += v * pit
                elif acc == 1: swhiff += v * pit
                else: scsw += v * pit
        sw += pit
        k, bb, tbf = _num(s.get("k")), _num(s.get("bb")), _num(s.get("tbf"))
        if k is not None: sk += k
        if bb is not None: sbb += bb
        if tbf is not None: stbf += tbf
    if n == 0 or sw <= 0:
        return None
    return {
        "velo":  svelo / sw,
        "whiff": swhiff / sw,
        "csw":   scsw / sw,
        "kbb":   ((sk - sbb) / stbf) if stbf > 0 else None,
        "ppg":   tot_pitches / n,          # pitches per start (14d leash level)
        "n":     n,
        "pitches": tot_pitches,
    }


def _zmap(vals):
    """values dict {key: x} -> {key: z}. Population mean/SD over non-null x."""
    xs = [x for x in vals.values() if x is not None]
    if len(xs) < 5:
        return {k: 0.0 for k in vals}
    mu = statistics.fmean(xs)
    sd = statistics.pstdev(xs)
    if sd <= 1e-9:
        return {k: 0.0 for k in vals}
    return {k: ((x - mu) / sd if x is not None else None) for k, x in vals.items()}


def main():
    if not GL_FILE.exists() or not STATS_FILE.exists():
        print("[proc] missing inputs; skipping", file=sys.stderr)
        return 0
    gl = json.loads(GL_FILE.read_text()).get("pitchers") or {}
    stats = json.loads(STATS_FILE.read_text())
    pit_stats = stats.get("pitchers") or {}

    sp_keys = set()
    if ROLES_FILE.exists():
        try:
            roles = json.loads(ROLES_FILE.read_text()).get("pitchers") or {}
            sp_keys = {k for k, v in roles.items() if (v or {}).get("role") == "SP"}
        except Exception as e:
            print(f"[proc] roles read failed: {e}", file=sys.stderr)

    # reference date = latest start date in the feed (robust to a stale pull)
    ref = None
    for rec in gl.values():
        for s in (rec.get("starts") or []):
            d = _date(s.get("date"))
            if d and (ref is None or d > ref):
                ref = d
    if ref is None:
        print("[proc] no dated starts; skipping", file=sys.stderr)
        return 0
    lo_recent = ref - datetime.timedelta(days=WIN_RECENT)
    lo_base   = ref - datetime.timedelta(days=WIN_BASE_HI)
    hi_base   = ref - datetime.timedelta(days=WIN_BASE_LO)

    # 1) per-SP raw component deltas / levels
    raw = {}   # key -> {d_velo,d_whiff,d_csw,d_kbb,ppg14,n14,n46,mlbam_id}
    for key, rec in gl.items():
        if sp_keys and key not in sp_keys:
            continue
        starts = [s for s in (rec.get("starts") or [])
                  if (_num(s.get("ip")) or 0) >= MIN_START_IP and _date(s.get("date"))]
        recent = [s for s in starts if _date(s["date"]) >= lo_recent]
        base   = [s for s in starts if lo_base <= _date(s["date"]) < hi_base]
        ar, ab = _win_agg(recent), _win_agg(base)
        if not ar or not ab:
            continue
        def dd(k):
            return (ar[k] - ab[k]) if (ar.get(k) is not None and ab.get(k) is not None) else None
        raw[key] = {
            "d_velo":  dd("velo"), "d_whiff": dd("whiff"),
            "d_csw":   dd("csw"),  "d_kbb":   dd("kbb"),
            "ppg14":   ar["ppg"],
            "n14": ar["n"], "n46": ab["n"],
            "mlbam_id": rec.get("mlbam_id"),
        }
    if len(raw) < 5:
        print(f"[proc] only {len(raw)} SPs with both windows; skipping", file=sys.stderr)
        return 0

    # 2) standardize each component across the SP population (all positive-oriented)
    comps = ("d_velo", "d_whiff", "d_csw", "d_kbb", "ppg14")
    zc = {c: _zmap({k: raw[k][c] for k in raw}) for c in comps}

    # 3) proc_z = re-standardized mean of available component z's
    mean_z = {}
    for k in raw:
        zs = [zc[c][k] for c in comps if zc[c].get(k) is not None]
        # require at least the four skill deltas (velo/whiff/csw/kbb) to be present
        have_deltas = sum(1 for c in ("d_velo", "d_whiff", "d_csw", "d_kbb")
                          if zc[c].get(k) is not None)
        mean_z[k] = statistics.fmean(zs) if (zs and have_deltas >= 3) else None
    zfin = _zmap(mean_z)
    proc = {k: max(-CLIP, min(CLIP, z)) for k, z in zfin.items() if z is not None}

    # 4) enrich pitcher_stats: proc_z + unified_proc (SP only, no mutation of unified_score)
    n_adj = 0
    for key, pz in proc.items():
        p = pit_stats.get(key)
        if not isinstance(p, dict):
            continue
        us = _num(p.get("unified_score"))
        p["proc_z"] = round(pz, 3)
        p["proc_components"] = {c: (round(raw[key][c], 4) if raw[key][c] is not None else None)
                                for c in comps}
        if us is not None and us < 10:            # sane RA9-scale base only
            p["unified_proc"] = round(us - PROC_COEF * pz, 3)
            n_adj += 1
    stats["proc_meta"] = {
        "generated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds"),
        "ref_date": ref.isoformat(), "coef": PROC_COEF, "clip": CLIP,
        "windows": {"recent_days": WIN_RECENT, "baseline_days": [WIN_BASE_LO, WIN_BASE_HI]},
        "components": list(comps), "n_sp": len(proc),
    }
    STATS_FILE.write_text(json.dumps(stats, separators=(",", ":")))

    # 5) slim audit feed (also the 2027 grading record)
    feed = {k: {"proc_z": round(proc[k], 3),
                "mlbam_id": raw[k].get("mlbam_id"),
                **{c: raw[k][c] for c in comps},
                "n14": raw[k]["n14"], "n46": raw[k]["n46"]}
            for k in proc}
    OUT_FILE.write_text(json.dumps({
        "generated_at": stats["proc_meta"]["generated_at"],
        "ref_date": ref.isoformat(), "coef": PROC_COEF, "n_sp": len(proc),
        "note": "SP process index (proc5: velo/whiff/csw/kbb deltas 14d-vs-46d + pitches/start-14d). "
                "unified_proc = unified_score - coef*proc_z applied SP-only in pitcher_stats.",
        "pitchers": feed,
    }, separators=(",", ":")))
    print(f"[proc] ref {ref} — {len(proc)} SPs indexed, {n_adj} unified_proc adjustments "
          f"(coef {PROC_COEF})", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
