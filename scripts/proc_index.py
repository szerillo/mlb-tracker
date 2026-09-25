#!/usr/bin/env python3
"""
SP process index (proc_z) — Fable reference implementation (2026-09-24), wired for the repo.

For every starting pitcher: six components, each = last 14 days minus the prior 46 days
(days 15-60), scaled by fixed SDs fit on the 2021-2026 panel, averaged, re-scaled, clipped.

    proc_z = clip( mean(z_i) / COMP_SD , -2.5, +2.5 )
    RA9_in = RA9_base - PROC_COEF * proc_z          # PROC_COEF = 0.10 runs/9 per SD (SP only)

Pulls the trailing 60 days from Baseball Savant (statcast_search CSV, pitcher view),
~250k rows, chunked + cached (~2-3 min). Run once daily after the previous night's games
post, BEFORE compute_pitcher_score.py (which reads the output via refresh_pitcher_proc.py).

    python scripts/proc_index.py                 # -> data/sp_process.json (+ .csv)
    python scripts/proc_index.py --date 2026-09-24 --out data/sp_process.json

Spec: SP_PROCESS_INDEX_SIX_SEASONS_2026-09-24.md §4/§6; IMPLEMENTATION_BRIEF §1.
"""
import argparse, datetime as dt, io, json, os, sys
import math
from pathlib import Path
import numpy as np, pandas as pd, requests

REPO_ROOT = Path(__file__).resolve().parent.parent
DEF_OUT   = str(REPO_ROOT / "data" / "sp_process.json")
CACHE_DIR = str(REPO_ROOT / ".proc_cache")

# ---- named constants (provenance: october/sp_full_panel_2021_2026.pkl, sp_process_2023_2026.pkl) ----
SCALE = {            # SD of the 14d-vs-prior-46d delta, pooled 2021-2026 starters
    'velo': 0.575,       # four-seam mph
    'whiff': 0.0609,     # whiffs / swings
    'csw': 0.0381,       # (called strikes + whiffs) / pitches
    'kbb': 0.0925,       # (K - BB) / PA
    'sec_velo': 0.92,    # secondary-pitch mph (SL/ST/SW/CU/KC/CH/FS)
    'pitches14': 56.5,   # pitches thrown in the last 14 days (level, not delta)
}
MU_PITCHES14 = 204.4     # mean pitches/14d, pooled panel
COMP_SD = 0.59           # SD of the raw component mean before final scaling
CLIP = 2.5
PROC_COEF = 0.10         # runs/9 per SD; pooled proc6 -0.108 (t -4.4), rounded down. Refit each November.
MIN_P14, MIN_P46 = 80, 200
SEC = {'SL', 'ST', 'SW', 'CU', 'KC', 'CH', 'FS'}
SWING = {'swinging_strike', 'swinging_strike_blocked', 'foul', 'foul_tip', 'hit_into_play', 'foul_bunt', 'missed_bunt', 'bunt_foul_tip'}
WHIFF = {'swinging_strike', 'swinging_strike_blocked', 'foul_tip', 'missed_bunt'}
COLS = ['game_date', 'game_pk', 'pitcher', 'player_name', 'pitch_type', 'release_speed', 'description', 'events', 'inning', 'at_bat_number']


def savant(yr, a, b):
    u = (f"https://baseballsavant.mlb.com/statcast_search/csv?all=true&hfGT=R%7C&hfSea={yr}%7C&player_type=pitcher"
         f"&game_date_gt={a}&game_date_lt={b}&min_pitches=0&min_results=0&group_by=name&sort_col=pitches"
         f"&player_event_sort=api_p_release_speed&sort_order=desc&min_pas=0&type=details")
    t = requests.get(u, timeout=180).text
    return pd.read_csv(io.StringIO(t), usecols=COLS, low_memory=False)


def pull(date, days=60, cache=CACHE_DIR):
    """Chunked + cached so a re-run resumes; safe to call repeatedly until it returns."""
    os.makedirs(cache, exist_ok=True)
    end = date - dt.timedelta(days=1); start = end - dt.timedelta(days=days - 1)
    parts, d = [], start
    while d <= end:
        e = min(d + dt.timedelta(days=4), end); fn = f'{cache}/{d.isoformat()}.parquet'
        if not os.path.exists(fn): savant(date.year, d.isoformat(), e.isoformat()).to_parquet(fn)
        parts.append(pd.read_parquet(fn)); d = e + dt.timedelta(days=1)
    D = pd.concat(parts, ignore_index=True); D['game_date'] = pd.to_datetime(D.game_date)
    return D


def feat(d):
    n = len(d); ff = d[d.pitch_type == 'FF']; s = d[d.pitch_type.isin(SEC)]
    return dict(n=n,
                velo=ff.release_speed.mean() if len(ff) >= 20 else np.nan,
                whiff=d.description.isin(WHIFF).sum() / max(d.description.isin(SWING).sum(), 1),
                csw=((d.description == 'called_strike').sum() + d.description.isin(WHIFF).sum()) / max(n, 1),
                kbb=(d.events.isin(['strikeout', 'strikeout_double_play']).sum() - (d.events == 'walk').sum()) / max(d.events.notna().sum(), 1),
                sec_velo=s.release_speed.mean() if len(s) >= 20 else np.nan)


def compute(D, date):
    t = pd.Timestamp(date)
    first = D.sort_values(['game_pk', 'pitcher', 'at_bat_number']).groupby(['game_pk', 'pitcher']).inning.first().reset_index()
    start_share = first.groupby('pitcher').inning.apply(lambda x: (x == 1).mean())
    rows = []
    for p, d in D.groupby('pitcher'):
        if start_share.get(p, 0) < 0.5: continue                       # starters only
        w14 = d[(d.game_date < t) & (d.game_date >= t - pd.Timedelta(days=14))]
        w46 = d[(d.game_date < t - pd.Timedelta(days=14)) & (d.game_date >= t - pd.Timedelta(days=60))]
        if len(w14) < MIN_P14 or len(w46) < MIN_P46: continue
        a, b = feat(w14), feat(w46)
        z = {k: (a[k] - b[k]) / SCALE[k] for k in ['velo', 'whiff', 'csw', 'kbb', 'sec_velo']}
        z['pitches14'] = (a['n'] - MU_PITCHES14) / SCALE['pitches14']
        vals = [v for v in z.values() if pd.notna(v)]
        pz = float(np.clip(np.mean(vals) / COMP_SD, -CLIP, CLIP))
        rows.append(dict(mlbam_id=int(p), name=d.player_name.iloc[0], date=date.isoformat(), proc_z=round(pz, 3),
                         ra9_adj=round(-PROC_COEF * pz, 3), n14=int(a['n']), n46=int(b['n']),
                         d_velo=None if pd.isna(z['velo']) else round(a['velo'] - b['velo'], 2),
                         **{f'z_{k}': (None if pd.isna(v) else round(v, 2)) for k, v in z.items()}))
    return pd.DataFrame(rows)


def _et_today():
    return (dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=4)).date()


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('--date', default=_et_today().isoformat())
    ap.add_argument('--out', default=DEF_OUT)
    a = ap.parse_args(); date = dt.date.fromisoformat(a.date)
    try:
        D = pull(date); R = compute(D, date)
    except Exception as e:
        print(f"[proc_index] Savant pull/compute failed: {e}; leaving previous sp_process.json", file=sys.stderr)
        sys.exit(0)
    assert abs(R.proc_z.mean()) < 0.35 and 0.6 < R.proc_z.std() < 1.4, f'proc_z distribution off: mean {R.proc_z.mean():.2f} sd {R.proc_z.std():.2f}'
    # NaN (missing components) must serialize as null, not the bare `NaN` literal
    # json.dump emits by default — that is invalid JSON and breaks strict browser
    # parses downstream (pitcher_stats.json feeds the app). allow_nan=False + a clean
    # dict keeps sp_process.json valid JSON.
    def _clean(v):
        return None if isinstance(v, float) and not math.isfinite(v) else v
    _rows = {str(r.mlbam_id): {k: _clean(v) for k, v in r._asdict().items()} for r in R.itertuples(index=False)}
    json.dump(_rows, open(a.out, 'w'), indent=1, allow_nan=False)
    R.sort_values('proc_z', ascending=False).to_csv(a.out.replace('.json', '.csv'), index=False)
    print(f'{len(R)} starters indexed for {date}; proc_z mean {R.proc_z.mean():+.2f} sd {R.proc_z.std():.2f}', file=sys.stderr)
