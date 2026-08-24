"""
V9 MLB Weather Model — physical run-adjustment engine + all park data.
(Module/function names kept as v8 / compute_v8 for import stability.)

History:
  V8.0  original port of the BallparkPal-style methodology.
  V8.1  operational cold cap.
  V8.2/V9 (2026-05-22) RECALIBRATION against BallparkPal weather-only runs on a
        251-game historical sample: fixed the inverted wind sign, softened the
        cold over-penalty, neutralized the broken `carry` scale, retuned the
        global constants (MAE 6.0→4.8, RMSE 8.7→7.0 vs BP). See the V8.2 note in
        the constants block. The "V9" label also covers two BACKEND steps done in
        refresh_weather.py (not here): feeding BallparkPal's barometric PRESSURE
        into this model as the `pres` input, and weighting the final published
        number toward BP's weather-only runs on the games BP covers. This module
        stays a pure physical model; refresh_weather.py does the BP ingestion.
  V9.2  (2026-07-19) FULL 30-PARK RECALIBRATION on a 13,286-game pull
        (2021-2026 home Finals, MLB game feeds; year-demeaned two-var temp+wind
        regression, bootstrap CIs). Changed ONLY parks whose empirical 95%% CI
        excludes the model value; 9 changed, 21 held. Temp: PARK_T_MULT WAS x1.9,
        COL x0.75, CIN x0.55. Wind: PIT wr_out 0.84->7.0/wr_in 2.48->4.0,
        BAL wr_out 2.40->5.0, CHC wr_out 19.30->15.0, CLE wr_in 2.47->0.75,
        PHI wr_out 4.0->3.0. ATH base 81->79. TEMP_TREND_AMP 0.15->0.08 (halve
        the unvalidated rising-temp amplifier). Every changed park re-checked to
        land inside its empirical CI. See weather_recalibration_2026-07-19 doc.
  V9.1  (2026-07-18) INDEPENDENT VALIDATION — no coefficient changes.
        Pulled 4,312 completed games (2022-2026, 12 open-air parks) from MLB
        StatsAPI game feeds (game-time temp + reported wind) and regressed total
        runs on temperature, demeaned by park-season to strip park and run-
        environment effects.
          - Empirical temperature slope: 0.411 %/degF (95% boot CI 0.285-0.555).
            This model delivers a mean 0.421 %/degF across the 23 open-air parks
            (median 0.415). Temperature term is CORRECTLY calibrated; leave it.
          - The ALTITUDE_COEF temp amplifier is effectively a Coors-only knob
            (COL 1.14x; every other park 1.00-1.02x). Suspected it was backwards
            on physics (humidor decouples ball COR from ambient temp; thinner air
            means a smaller ABSOLUTE density change per degF). The data does not
            support that: COL empirical 0.380 %/degF vs an 11-park control mean
            of 0.313 -> ratio 1.21, against the model's assumed 1.14. Coors is if
            anything MORE temp-sensitive than average, not less. Left unchanged.
          - Wind receptivity, per park, controlling for temp (~370 games/park,
            %/10mph): CHC 18.4 (model 19.3), KC 13.2 (6.5), PIT 8.1 (0.8),
            CIN 3.8 (-1.6), BOS 3.3 (4.6), PHI 2.9 (4.0), COL 1.5 (3.0),
            MIN 1.2 (1.6), DET 0.6 (0.4), CLE 0.3 (0.8), NYY -1.2 (2.5),
            ATL -1.7 (2.6). EVERY current wr_out falls inside its 95% bootstrap
            CI, so none is rejected -- the CIs are +/-7 to 16 %/10mph because
            StatsAPI's coarse out/in wind tag is a noisy regressor. Wrigley's
            19.30 is independently confirmed (18.4 on 370 games). No changes.
        Net: this pass validated the model rather than retuning it. Do not read
        BallparkPal disagreement as evidence we are wrong -- on temperature the
        5-season sample sides with us, and BP runs 3-5 degF hotter on its own
        inputs (our forecast MAE 2.94 degF vs BP 4.21 degF).

Usage:
    from v8_weather import compute_v8, TEAM_TO_PARK
    result = compute_v8("BOS", {
        "t": 55, "hum": 70, "ws": 10, "wd_compass": 45, "pres": 1014, "precip": 20,
        "t_hours": [55, 55, 55],
    })
    # result = {"run_adj_pct": -3.2, "components": {...}}
"""
import math

# ============================================================================
# Mapping: MLB team name → park code
# ============================================================================
TEAM_TO_PARK = {
    "Los Angeles Angels":"LAA", "Baltimore Orioles":"BAL", "Boston Red Sox":"BOS",
    "Chicago White Sox":"CHW", "Cleveland Guardians":"CLE", "Kansas City Royals":"KC",
    "Tampa Bay Rays":"TB", "Toronto Blue Jays":"TOR", "New York Yankees":"NYY",
    "Detroit Tigers":"DET", "Minnesota Twins":"MIN", "Houston Astros":"HOU",
    "Texas Rangers":"TEX", "Seattle Mariners":"SEA", "Athletics":"ATH",
    "Atlanta Braves":"ATL", "Miami Marlins":"MIA", "New York Mets":"NYM",
    "Philadelphia Phillies":"PHI", "Washington Nationals":"WAS",
    "Chicago Cubs":"CHC", "Cincinnati Reds":"CIN", "Milwaukee Brewers":"MIL",
    "Pittsburgh Pirates":"PIT", "St. Louis Cardinals":"STL", "Arizona Diamondbacks":"ARI",
    "Colorado Rockies":"COL", "Los Angeles Dodgers":"LAD", "San Diego Padres":"SD",
    "San Francisco Giants":"SF",
}

# ============================================================================
# BP_BASE — per-park baselines (temp, hum, pres, carry, wr, of, cr, cq, var, runs, alt, dome)
# ============================================================================
BP_BASE = {
    "LAA":{"temp":77,"hum":51,"pres":1013,"carry":-48.00,"wr_out":-9.71,"wr_in":3.32,"of":"Small","cr":"Avg","cq":"Good","var":0.79,"runs":-1,"alt":160},
    "BAL":{"temp":76,"hum":59,"pres":1015,"carry":-69.00,"wr_out":1.92,"wr_in":0.62,"of":"Variable","cr":"Great","cq":"Good","var":1.41,"runs":9,"alt":130},
    "BOS":{"temp":70,"hum":60,"pres":1015,"carry":-1.55,"wr_out":2.0,"wr_in":1.20,"of":"Variable","cr":"Good","cq":"Great","var":1.84,"runs":12,"alt":20},
    "CHW":{"temp":70,"hum":63,"pres":1015,"carry":-1.06,"wr_out":2.81,"wr_in":-0.38,"of":"Small","cr":"Bad","cq":"Avg","var":1.18,"runs":-3,"alt":596},
    "CLE":{"temp":70,"hum":65,"pres":1016,"carry":-77.00,"wr_out":0,"wr_in":0.75,"of":"Small","cr":"Avg","cq":"Poor","var":1.51,"runs":-3,"alt":582},
    "KC": {"temp":78,"hum":56,"pres":1014,"carry":23.00,"wr_out":3.2,"wr_in":7.0,"of":"X","cr":"Great","cq":"Good","var":1.21,"runs":7,"alt":750},
    "TB": {"temp":72,"hum":44,"pres":1014,"carry":-53.00,"wr_out":0,"wr_in":0,"of":"Medium","cr":"Poor","cq":"Poor","var":0.03,"runs":-7,"alt":0,"dome":True},
    "TOR":{"temp":73,"hum":59,"pres":1015,"carry":-1.67,"wr_out":0,"wr_in":-1.37,"of":"Medium","cr":"Great","cq":"Good","var":0.81,"runs":-3,"alt":247,"dome":True},
    "NYY":{"temp":74,"hum":56,"pres":1015,"carry":-1.50,"wr_out":10.63,"wr_in":3.90,"of":"Variable","cr":"Avg","cq":"Great","var":1.95,"runs":-3,"alt":54},
    "DET":{"temp":72,"hum":56,"pres":1015,"carry":-1.54,"wr_out":0,"wr_in":12.0,"of":"Large","cr":"Avg","cq":"Avg","var":1.31,"runs":-1,"alt":596},
    "MIN":{"temp":73,"hum":53,"pres":1014,"carry":-53.00,"wr_out":0,"wr_in":-0.11,"of":"Medium","cr":"Avg","cq":"Good","var":0.98,"runs":3,"alt":812},
    "HOU":{"temp":80,"hum":48,"pres":1015,"carry":-1.68,"wr_out":9.47,"wr_in":-3.21,"of":"Variable","cr":"Bad","cq":"Poor","var":0.50,"runs":-3,"alt":38,"dome":True},
    "TEX":{"temp":81,"hum":42,"pres":1013,"carry":-1.18,"wr_out":1.34,"wr_in":0.79,"of":"Medium","cr":"Avg","cq":"Great","var":0.40,"runs":-5,"alt":616,"dome":True},
    "SEA":{"temp":71,"hum":51,"pres":1016,"carry":-2.15,"wr_out":-8.16,"wr_in":1.35,"of":"Small","cr":"Poor","cq":"Bad","var":0.88,"runs":-13,"alt":10},
    "ATH":{"temp":79,"hum":40,"pres":1012,"carry":-95.00,"wr_out":5.0,"wr_in":8.0,"of":"Large","cr":"Good","cq":"Avg","var":1.00,"runs":15,"alt":26},  # 2026-07-27 prior bump wr_out 3.0->5.0 (small-sample + physics: open-air, hot, dry Sacramento = great carry, NOT altitude). Coliseum-era of/carry/runs still stale; cross-wind/orientation (GPS_CF 46) needs a Sutter-specific look. Re-fit at n~150.
    "ATL":{"temp":82,"hum":50,"pres":1015,"carry":-44.00,"wr_out":12.87,"wr_in":-0.62,"of":"Medium","cr":"Poor","cq":"Great","var":0.90,"runs":-7,"alt":1050},
    "MIA":{"temp":80,"hum":59,"pres":1017,"carry":-1.07,"wr_out":4.24,"wr_in":2.01,"of":"Large","cr":"Good","cq":"Avg","var":0.30,"runs":-1,"alt":15,"dome":True},
    "NYM":{"temp":73,"hum":57,"pres":1015,"carry":-1.21,"wr_out":3.76,"wr_in":-0.43,"of":"Medium","cr":"Poor","cq":"Poor","var":1.37,"runs":-9,"alt":54},
    "PHI":{"temp":77,"hum":55,"pres":1015,"carry":-1.17,"wr_out":6.98,"wr_in":4.08,"of":"Small","cr":"Bad","cq":"Great","var":1.83,"runs":3,"alt":9},
    "WAS":{"temp":78,"hum":55,"pres":1015,"carry":-64.00,"wr_out":9.31,"wr_in":2.0,"of":"Medium","cr":"Great","cq":"Great","var":0.98,"runs":4,"alt":25},
    "CHC":{"temp":70,"hum":63,"pres":1015,"carry":-1.85,"wr_out":5.81,"wr_in":10.00,"of":"Medium","cr":"Poor","cq":"Bad","var":2.67,"runs":-4,"alt":596},
    "CIN":{"temp":76,"hum":61,"pres":1015,"carry":-49.00,"wr_out":-1.60,"wr_in":0.0,"of":"Small","cr":"Bad","cq":"Avg","var":0.88,"runs":10,"alt":683},
    "MIL":{"temp":76,"hum":60,"pres":1015,"carry":-1.01,"wr_out":6.18,"wr_in":-0.55,"of":"Medium","cr":"Avg","cq":"Avg","var":0.67,"runs":-10,"alt":0,"dome":True},
    "PIT":{"temp":74,"hum":58,"pres":1015,"carry":-73.00,"wr_out":4.57,"wr_in":4.00,"of":"Variable","cr":"Good","cq":"Bad","var":1.01,"runs":0,"alt":743},
    "STL":{"temp":79,"hum":58,"pres":1014,"carry":-77.00,"wr_out":6.08,"wr_in":1.28,"of":"Large","cr":"Good","cq":"Avg","var":1.25,"runs":-5,"alt":455},
    "ARI":{"temp":88,"hum":15,"pres":1010,"carry":68.00,"wr_out":0,"wr_in":1.09,"of":"Large","cr":"Great","cq":"Bad","var":0.48,"runs":2,"alt":1082,"dome":True},
    "COL":{"temp":75,"hum":28,"pres":1012,"carry":3.75,"wr_out":0,"wr_in":12.0,"of":"X","cr":"Great","cq":"Avg","var":1.36,"runs":32,"alt":5183},
    "LAD":{"temp":78,"hum":47,"pres":1012,"carry":-1.49,"wr_out":1.63,"wr_in":3.19,"of":"Medium","cr":"Avg","cq":"Great","var":0.87,"runs":0,"alt":267},
    "SD": {"temp":72,"hum":62,"pres":1013,"carry":-1.77,"wr_out":0,"wr_in":1.64,"of":"Medium","cr":"Avg","cq":"Avg","var":1.00,"runs":-3,"alt":13},
    "SF": {"temp":66,"hum":64,"pres":1014,"carry":-2.30,"wr_out":2.06,"wr_in":0.82,"of":"Variable","cr":"Good","cq":"Poor","var":0.81,"runs":-3,"alt":63},
}

# Per-park DIRECTIONAL out-wind receptivity (2026-07-27, Fable): a few parks
# respond to out-wind very differently by field (CF vs corners). Keyed by the
# _compass_to_bucket_idx arrow (OutCenter / OutLeft / OutRight); missing arrows
# fall back to BP_BASE[park]["wr_out"]. Single coefficient is fine everywhere else.
# PIT: CF slope ~0, RF (To-Right) big -> 2 CF / 12 RF (LF kept at the base 7).
BP_DIR_WR_OUT = {}  # WIND=cos3_az_v1: kernel handles direction; directional overrides retired
_RETIRED_BP_DIR_WR_OUT = {
    "PIT": {"OutCenter": 2.0, "OutLeft": 7.0, "OutRight": 12.0},
}

CAL_PARAMS = {
    "ATL":{"t_sens":1.0,"cold_mult":1.0},
    "BAL":{"t_sens":1.0,"cold_mult":4.0},
    "CHC":{"t_sens":2.0,"cold_mult":0.0},
    "CIN":{"t_sens":1.8,"cold_mult":1.0},
    "DET":{"t_sens":1.6,"cold_mult":0.0},
    "KC": {"t_sens":1.0,"cold_mult":2.0},
    "LAD":{"t_sens":1.3,"cold_mult":0.0},
    "MIL":{"t_sens":1.0,"cold_mult":1.0,"dome_damp":0.05},
    "NYM":{"t_sens":0.1,"cold_mult":0.0},
    "PHI":{"t_sens":1.0,"cold_mult":4.0},
    "SD": {"t_sens":1.8,"cold_mult":1.0},
    "SEA":{"t_sens":1.2,"cold_mult":0.0},
    "STL":{"t_sens":1.0,"cold_mult":2.0},
    "TB": {"t_sens":1.0,"cold_mult":1.0,"dome_damp":0.00},
    "TOR":{"t_sens":1.0,"cold_mult":1.0,"dome_damp":0.42},
    "HOU":{"t_sens":1.0,"cold_mult":1.0,"dome_damp":0.15},
    "ARI":{"t_sens":1.0,"cold_mult":1.0,"dome_damp":0.15},
    "MIA":{"t_sens":1.0,"cold_mult":1.0,"dome_damp":0.15},
}

# Home-plate -> center-field compass bearing (deg from N). Drives the wind
# in/out projection. Real MLB orientations span ~22 deg (NNE) clockwise to
# ~202 deg (SSW). The SOUTH/SE-facing parks were corrected 2026-05 after the
# original table mis-stored them as NE/ENE, which flipped in/out winds
# (e.g. a NNE wind at Comerica read "in from LF" instead of the correct
# "out to RF"). Southward cohort per Hardball Times / Baseball Almanac:
#   CWS (Rate Field, "points SE"), DET (Comerica, most-southward pre-2017),
#   ATL (Truist, most-southward), MIL + TEX (southward; usually roofed).
# NE-facing parks left unchanged (plausible range) pending a full
# coordinate-based recompute.
# Home-plate -> center-field compass bearing (deg from true N). AUTHORITATIVE:
# MLB StatsAPI venue location.azimuthAngle (official, physically-fixed). Replaced
# the prior hand-maintained table 2026-07 after an audit found 23/30 parks off by
# >=15 deg (CIN was 41 vs the true 122 -> in/out winds were flipped; NYY 37 vs 75;
# PIT 37 vs 116). Drives ONLY the live wind out/in classification; the wr_out/wr_in
# responsiveness magnitudes are BallparkPal park constants and are unaffected.
GPS_CF = {
    "ARI":0,"ATL":145,"BAL":31,"BOS":45,"CHC":37,"CHW":127,"CIN":122,"CLE":0,
    "COL":4,"DET":150,"HOU":10,"KC":46,"LAA":44,"LAD":26,"MIA":128,"MIL":129,
    "MIN":85,"NYM":13,"NYY":75,"PHI":9,"PIT":116,"SD":0,"SF":85,"SEA":49,
    "STL":62,"TB":359,"TEX":30,"TOR":345,"WAS":28,"ATH":46,
}

# Per-park temp and wind distribution buckets (BP_DIST)
BP_DIST = {
    "LAA":{"wind_dir":[0,1,14,77,0,0,0,8],"wind_spd":[2,57,39,2,0],"temp_dist":[0,1,19,42,32,6]},
    "BAL":{"wind_dir":[10,15,19,14,8,8,14,12],"wind_spd":[12,48,28,11,0],"temp_dist":[1,9,12,34,33,9]},
    "BOS":{"wind_dir":[14,14,19,18,7,2,10,16],"wind_spd":[9,50,31,11,0],"temp_dist":[5,19,18,35,18,4]},
    "CHW":{"wind_dir":[10,15,8,8,11,16,22,9],"wind_spd":[4,32,35,26,3],"temp_dist":[7,13,18,42,17,3]},
    "CLE":{"wind_dir":[17,3,3,8,12,13,22,21],"wind_spd":[8,39,32,20,1],"temp_dist":[7,11,18,45,19,0]},
    "KC": {"wind_dir":[10,12,27,16,11,10,9,5],"wind_spd":[6,32,35,23,5],"temp_dist":[2,7,10,30,30,20]},
    "TB": None,
    "TOR":{"wind_dir":[2,7,19,12,16,14,13,17],"wind_spd":[7,48,27,16,2],"temp_dist":[0,2,22,60,15,1]},
    "ARI":{"wind_dir":[2,2,4,26,1,5,38,23],"wind_spd":[22,49,10,16,4],"temp_dist":[0,0,0,22,22,55]},
    "CHC":{"wind_dir":[18,11,11,14,24,9,5,9],"wind_spd":[4,31,34,26,4],"temp_dist":[10,11,15,42,19,2]},
    "COL":{"wind_dir":[25,27,12,7,7,7,6,10],"wind_spd":[10,52,22,14,2],"temp_dist":[3,9,15,29,32,12]},
    "LAD":{"wind_dir":[0,0,4,50,0,0,1,45],"wind_spd":[0,32,64,3,1],"temp_dist":[0,2,13,40,36,8]},
    "PIT":{"wind_dir":[5,18,27,20,8,5,5,11],"wind_spd":[15,56,25,4,0],"temp_dist":[4,9,15,38,31,4]},
    "MIL":{"wind_dir":[14,16,15,9,17,15,12,2],"wind_spd":[5,41,35,18,0],"temp_dist":[0,2,13,51,29,5]},
    "SEA":{"wind_dir":[1,2,14,21,0,31,26,5],"wind_spd":[24,63,12,1,0],"temp_dist":[2,13,26,38,17,4]},
    "HOU":{"wind_dir":[4,2,5,45,18,11,5,11],"wind_spd":[0,23,25,50,2],"temp_dist":[0,0,4,36,57,4]},
    "DET":{"wind_dir":[15,19,13,6,11,11,16,9],"wind_spd":[4,35,37,23,1],"temp_dist":[7,10,16,39,24,4]},
    "SF": {"wind_dir":[0,1,6,82,0,0,0,9],"wind_spd":[0,27,46,23,2],"temp_dist":[0,22,43,30,5,1]},
    "CIN":{"wind_dir":[9,26,19,16,5,6,12,8],"wind_spd":[14,50,27,9,0],"temp_dist":[3,8,10,36,34,9]},
    "SD": {"wind_dir":[0,0,0,1,0,15,61,22],"wind_spd":[1,24,66,9,0],"temp_dist":[0,3,30,54,12,0]},
    "PHI":{"wind_dir":[9,10,12,16,3,11,21,19],"wind_spd":[7,44,38,11,0],"temp_dist":[2,10,14,29,32,14]},
    "STL":{"wind_dir":[13,17,20,8,10,10,11,11],"wind_spd":[6,41,32,20,2],"temp_dist":[1,6,11,25,38,19]},
    "NYM":{"wind_dir":[8,9,20,22,4,10,14,12],"wind_spd":[4,29,40,25,2],"temp_dist":[3,12,15,35,29,6]},
    "WAS":{"wind_dir":[11,12,16,15,6,10,18,13],"wind_spd":[12,58,23,7,0],"temp_dist":[1,8,12,27,38,14]},
    "MIN":{"wind_dir":[17,18,12,13,10,8,8,14],"wind_spd":[6,30,39,23,1],"temp_dist":[7,11,13,36,26,7]},
    "NYY":{"wind_dir":[9,28,17,13,7,4,7,14],"wind_spd":[4,31,40,23,1],"temp_dist":[4,11,16,36,27,7]},
    "MIA":{"wind_dir":[15,6,1,2,32,38,4,1],"wind_spd":[1,11,31,56,1],"temp_dist":[0,0,0,48,48,4]},
    "ATL":{"wind_dir":[12,14,18,20,11,14,9,1],"wind_spd":[10,51,33,6,0],"temp_dist":[0,3,6,25,40,26]},
    "TEX":{"wind_dir":[10,24,22,5,5,19,10,3],"wind_spd":[5,26,48,21,0],"temp_dist":[0,0,3,43,38,16]},
    "ATH":{"wind_dir":[0,0,30,55,0,1,6,8],"wind_spd":[5,14,25,54,2],"temp_dist":[0,9,9,22,36,25]},
}

# ============================================================================
# V8 Global Constants
# ============================================================================
# V8.2 RECALIBRATION (2026-05-22): refit against BallparkPal's weather-only
# runs% on a 251-game historical sample (34 dates, Apr 20 – May 22) recovered
# from git history of data/bp_weather.json + data/weather.json. Constants below
# were chosen by Nelder-Mead minimization of MAE vs BP, validated with 5-fold
# cross-validation BY DATE (out-of-sample MAE 5.0 vs 6.0 before).
#   Before → after on the sample:  MAE 6.0→4.8 · RMSE 8.7→7.0 · sign-agree 73%→78%
# Two root-cause fixes were folded in:
#   (1) WIND SIGN BUG — wind blowing IN was being added as +runs (double
#       negative: wr_in flipped negative × negative in-component = positive).
#       Now the wind adjustment follows the sign of the out-component with a
#       positive responsiveness magnitude. (Fixed inline in compute_v8.)
#       e.g. 14mph E wind at Yankee Stadium: +1.0% (old, wrong) → -8.9% (BP -9).
#   (2) COLD OVER-PENALTY — the cold quadratic was too steep at moderate cold
#       (52°F games showed -15% vs BP -4); softened via COLD_T/COLD_A/cap.
# Note: the broken-scale per-park `carry` baseline (values span -1.5 .. -95)
# correlated ~0 with BP, so CARRY_SCALE was shrunk 8x (0.04→0.005) to neutralize
# it without removing the field. Residual error is now dominated by our coarse
# single-hour NWS wind octant not matching BP's finer wind input — not by the
# coefficients (park wind term explains R²≈0.02 of BP's post-temp residual).
TEMP_C = 0.0005768   # 2026-07-27: trimmed from 0.0008 so the typical-range temp slope lands ~0.35%/degF (Fable fit 0.333), matching realized runs
COLD_T = 9
COLD_A = 0.00012
WIND_O = 0.0015
WIND_I = 0.0015
# Per-park wr_out/wr_in are already in "%/10mph" units; WIND_SCALE is the global
# multiplier on top. Refit to 0.95 (was 2.0) with the sign bug fixed.
WIND_SCALE = 0.95
# Wind-only contribution ceiling. Removed per 2026-06 calibration decision —
# the 0.20 (20pt) cap was clipping legit big wind-out parks (CHC/KC) below their
# BallparkPal-style targets. Set to an effectively-unbounded sentinel so genuine
# strong wind-out games can run high; the magnitude is still governed by per-park
# wr_out + WIND_SCALE + EMPIRICAL_SCALE.
WIND_CAP = 5.00
# Diminishing-returns soft ceiling on the wind %: w_adj -> WIND_SOFT*tanh(w_adj/WIND_SOFT).
# Linear wind response let extreme winds (Wrigley 22mph out) run to +63% wind / +74%
# total, which overshoots — wind effect saturates (25mph doesn't help 2.5x a 10mph).
# ~0.30 keeps the common 5-12mph range near-linear and tapers the tail.
WIND_SOFT = 0.45
DP_C = 0.0014
PRES_C = 0.0030   # 2026-06 recal: wind-controlled pressure slope ~-0.24 %/mb (earlier -0.39 was wind confound); net ~-0.26.
CARRY_INT = 0.0003
CARRY_SCALE = 0.005
T_SENS_FLOOR_HOT = 4.5   # 2026-06 recal vs BallparkPal weather-only (5d, 44 open-air games, wind-controlled regression): temp +0.36-0.54 %/F; 4.5 floor = net ~0.36%/F.
T_SENS_FLOOR_COLD = 4.5
COLD_MULT_CAP = 0.5
DOME_DAMP_DEFAULT = 0.15

VARIATION_AMP_COEF = 0.15
RUNS_AMP_COEF = 0.25
NARROW_AMP_COEF = 0.20
ALTITUDE_REF = 500
ALTITUDE_COEF = 0.15

WIND_DIR_RARITY_AMP = 0.0    # retired (WIND=cos3_az_v1): coeffs calibrated at rarity=0
WIND_SPEED_RARITY_AMP = 0.10
TEMP_PCT_AMP = 0.15
EXTREME_PCT_BOOST = 0.08

PRECIP_T1 = 25
PRECIP_T2 = 60
PRECIP_MAX_PENALTY = 0.0   # noprecip_v1 (Fable 2026-08-18): probability precip dock removed

TEMP_TREND_AMP = 0.08
TEMP_TREND_DAMPEN = 0.85

# V9.2 (2026-07-19) per-park TEMP multiplier from the 13,286-game all-park pull
# (year-demeaned two-var temp+wind regression). Applied AFTER the temp amps.
# Only parks whose empirical %/F 95% CI EXCLUDES the model's ~0.42 baseline are
# moved; noisy parks (wide CIs) are left at 1.0. See recalibration doc.
PARK_T_MULT = {
    "WAS": 1.90,   # emp 0.90 %/F, CI[0.50,1.28] -> excludes 0.44; DC heat carries
    "COL": 0.75,   # emp 0.30 %/F, CI[0.02,0.62]; humidor+thin-air cap the temp effect
    "CIN": 0.55,   # emp -0.21 %/F, CI upper 0.25 < 0.40; shrink toward ~0.2
}

CR_MULT = {"Bad":0.94,"Poor":0.97,"Avg":1.0,"Good":1.04,"Great":1.07}
CQ_MULT = {"Bad":0.94,"Poor":0.97,"Avg":1.0,"Good":1.04,"Great":1.07}
OF_MULT = {"Small":0.90,"Medium":1.0,"Variable":1.04,"Large":1.12,"X":1.08}

# Operational cold floor. Refit to -20% (was -25%): the -25 floor was clipping
# moderate-cold games far below BP. Extreme cold (<42°F) is still allowed to go
# deeper (the floor is only applied at t >= 42 in compute_v8).
COLD_FLOOR_PCT = -20.0

# ============================================================================
# Helpers
# ============================================================================
def dew_point(temp_f, hum_pct):
    if hum_pct is None or hum_pct <= 0:
        return temp_f
    t_c = (temp_f - 32) * 5/9
    gamma = math.log(hum_pct/100) + (17.625 * t_c) / (243.04 + t_c)
    td_c = 243.04 * gamma / (17.625 - gamma)
    return td_c * 9/5 + 32


def _temp_percentile(park, temp_f):
    """Where does temp_f sit in the park's historical temp distribution? Returns 0-1."""
    dist = (BP_DIST.get(park) or {}).get("temp_dist")
    if not dist: return 0.5
    if temp_f < 50: return dist[0] / 2 / 100
    cum = dist[0]
    lo = 50
    for i, bucket_hi in enumerate([60, 70, 80, 90, 200]):
        if temp_f < bucket_hi:
            frac = (temp_f - lo) / 10 if bucket_hi < 200 else 0.5
            return (cum + frac * dist[i+1]) / 100
        cum += dist[i+1]
        lo = bucket_hi
    return 1.0


# ── Targeted wind-receptivity shrink (2026-07 calibration) ──────────────────
# Longer backtest (892 games, model on OBSERVED weather vs closing+actual):
# high wr_out parks (CHC/BOS/PHI/COL/STL/ATL/NYY/BAL) calibrate at ~1.04x and the
# high-vs-low-wind gap is significant (bootstrap 90% CI excludes 0). Low-wind
# parks realize ~0.40x — their wind adjustment is largely noise. So we SHRINK the
# wind component ONLY at low-receptivity parks and leave the validated high-wind
# tier untouched. Conservative floor (keep 60% of wind) + directional, while we
# keep gathering sample. wr_out magnitude is the receptivity proxy.
WR_FULL = 2.4              # |wr_out| at/above which wind keeps full weight (validated tier)
WIND_SHRINK_FLOOR = 0.60   # least wind-receptive parks keep 60% of the wind adj
# Parks whose wr_out/wr_in were individually re-fit (2026-07-27, Fable/BetLabs
# 2005-26 WLS): calibrated at the source, so the blanket low-receptivity shrink
# must NOT also trim them (it partly existed to compensate the very values now
# corrected). Exempt them from _wind_receptivity_shrink.
WIND_SHRINK_EXEMPT = {"SEA", "BAL", "KC", "NYM", "BOS", "WAS", "CIN"}


def _wind_receptivity_shrink(park):
    base = BP_BASE.get(park)
    if not base:
        return 1.0
    if park in WIND_SHRINK_EXEMPT:
        return 1.0
    wr = abs(base.get("wr_out", 0) or 0)
    if wr >= WR_FULL:
        return 1.0
    return WIND_SHRINK_FLOOR + (1.0 - WIND_SHRINK_FLOOR) * (wr / WR_FULL)


def _wind_speed_rarity(park, ws):
    dist = (BP_DIST.get(park) or {}).get("wind_spd")
    if not dist: return 0
    bucket = 0 if ws < 4 else 1 if ws < 8 else 2 if ws < 12 else 3 if ws < 19 else 4
    freq = dist[bucket] / 100
    return max(0, min(1.0, 1 - freq * 3))


# BP wind bucket indices (baseball-relative)
BP_BUCKETS = ["InRight","FromRight","OutLeft","OutCenter","InCenter","InLeft","FromLeft","OutRight"]


def _compass_to_bucket_idx(park, wd_degrees, ws):
    """Classify compass wind direction into park-relative arrow bucket index."""
    cf = GPS_CF.get(park, 0)
    if ws < 1: return None
    # wd_degrees is direction FROM (where wind originates). Flip to "toward".
    wt = (wd_degrees + 180) % 360
    angle_from_cf = ((wt - cf + 180) % 360) - 180
    abs_a = abs(angle_from_cf)
    if abs_a < 22.5: arrow = "OutCenter"
    elif abs_a < 67.5: arrow = "OutLeft" if angle_from_cf < 0 else "OutRight"
    elif abs_a < 112.5: arrow = "FromLeft" if angle_from_cf < 0 else "FromRight"
    elif abs_a < 157.5: arrow = "InLeft" if angle_from_cf < 0 else "InRight"
    else: arrow = "InCenter"
    return BP_BUCKETS.index(arrow), arrow


def _wind_dir_rarity(park, wd_degrees, ws):
    dist = (BP_DIST.get(park) or {}).get("wind_dir")
    if not dist or ws < 1: return 0
    result = _compass_to_bucket_idx(park, wd_degrees, ws)
    if not result: return 0
    idx, arrow = result
    freq = dist[idx] / 100
    return max(0, min(1.0, 1 - freq * 5))


def _precip_penalty(precip_pct):
    if precip_pct is None: return 0
    if precip_pct < PRECIP_T1: return 0
    if precip_pct > PRECIP_T2: return PRECIP_MAX_PENALTY
    return PRECIP_MAX_PENALTY * (precip_pct - PRECIP_T1) / (PRECIP_T2 - PRECIP_T1)


def _temp_trend_mult(t_trend, delta_t):
    if abs(t_trend) < 2: return 1.0
    aligned = (delta_t > 0 and t_trend > 0) or (delta_t < 0 and t_trend < 0)
    opposed = (delta_t > 0 and t_trend < -3) or (delta_t < 0 and t_trend > 3)
    if aligned: return 1 + TEMP_TREND_AMP
    if opposed: return TEMP_TREND_DAMPEN
    return 1.0


def _compass_to_out_component(park, wd_degrees, ws):
    """Return (out_component, sign_for_wr) where positive = wind toward outfield."""
    cf = GPS_CF.get(park, 0)
    wt = (wd_degrees + 180) % 360
    angle_from_cf = ((wt - cf + 180) % 360) - 180
    # Project speed onto the CF axis: positive if within ±90° of CF direction
    return ws * math.cos(math.radians(angle_from_cf))**3  # cos^3 angle kernel (WIND=cos3_az_v1): quartering/cross winds contribute far less to runs than straight-out


# ============================================================================
# Main compute
# ============================================================================
def compute_v8(park, wx, treat_as_open=False):
    """
    Compute V8 weather-only run adjustment percentage for a park.
    treat_as_open=True computes a retractable-roof park as a true OUTDOOR park
    (skips the dome dampening), used when the roof is open / likely open.

    Args:
        park: park code ("BOS", "CHC", etc.)
        wx: dict with keys:
            t: current temp (°F)
            hum: current humidity %
            ws: wind speed (mph)
            wd_compass: wind direction degrees (0-360, FROM direction)
            pres: pressure (mb) — optional, defaults to 1015
            precip: precipitation probability % — optional, defaults to 0
            t_hours: list of 3+ hourly temps around game time — optional

    Returns:
        dict with run_adj_pct, components dict, and flags.
    """
    base = BP_BASE.get(park)
    if not base:
        return {"run_adj_pct": 0.0, "error": f"Unknown park: {park}"}
    cal = CAL_PARAMS.get(park, {"t_sens": 1.0, "cold_mult": 1.0})
    is_dome = base.get("dome", False)

    t = wx.get("t")
    if t is None:
        return {"run_adj_pct": 0.0, "error": "No temperature"}
    hum = wx.get("hum") or base["hum"]
    ws = wx.get("ws") or 0
    wd = wx.get("wd_compass")
    pres = wx.get("pres") or 1015
    precip = wx.get("precip") or 0
    t_hours = wx.get("t_hours")

    delta_t = t - base["temp"]
    is_hot = delta_t > 0

    # Park-derived multipliers
    narrow_factor = max(BP_DIST.get(park) or {"temp_dist":[30]}).__getitem__("temp_dist") if False else (
        max((BP_DIST.get(park) or {"temp_dist":[30]})["temp_dist"]) / 100.0
    )
    # ^ clumsy: compute narrow_factor cleanly
    dist = BP_DIST.get(park)
    narrow_factor = max(dist["temp_dist"]) / 100.0 if dist else 0.30

    variation_mult = 1 + (base["var"] - 1.0) * VARIATION_AMP_COEF
    runs_amp      = 1 + (base["runs"] / 100) * RUNS_AMP_COEF
    narrow_amp    = 1 + max(narrow_factor - 0.30, 0) * NARROW_AMP_COEF
    altitude_mult = 1 + max(base["alt"] - ALTITUDE_REF, 0) / 5000 * ALTITUDE_COEF

    # Temperature component
    ts_eff = max(T_SENS_FLOOR_HOT if is_hot else T_SENS_FLOOR_COLD, cal.get("t_sens", 1.0))
    t_adj = delta_t * TEMP_C * ts_eff
    if delta_t < -COLD_T:
        cold_mult = min(cal.get("cold_mult", 1.0), COLD_MULT_CAP)
        t_adj -= (abs(delta_t) - COLD_T) ** 2 * COLD_A * cold_mult
    if t_hours and len(t_hours) >= 2:
        t_adj *= _temp_trend_mult(t_hours[-1] - t_hours[0], delta_t)
    t_adj *= narrow_amp * altitude_mult
    pct = _temp_percentile(park, t)
    rarity = abs(pct - 0.5) * 2
    amp = rarity * TEMP_PCT_AMP
    if pct <= 0.1 or pct >= 0.9:
        amp += EXTREME_PCT_BOOST
    t_adj *= (1 + amp)
    t_adj *= PARK_T_MULT.get(park, 1.0)

    # Wind component (requires wd_compass)
    w_adj = 0
    wind_info = None
    if wd is not None and ws > 0:
        out_c = _compass_to_out_component(park, wd, ws)
        # out_c already carries direction: positive = wind blowing toward the
        # outfield (helps runs), negative = blowing in (hurts). So the wind
        # adjustment must take the SIGN OF out_c and a POSITIVE responsiveness
        # magnitude (how reactive this park is to wind, per wr_out/wr_in).
        #
        # BUG FIXED (V8.2): the old code did `wr_use = -abs(wr_in)` for in-wind
        # and then `out_c * wr_use`, i.e. negative × negative = POSITIVE — so a
        # strong wind blowing IN was being scored as +runs. (Comerica with a
        # 13mph ENE wind read +10% instead of a penalty; Yankee Stadium with a
        # 14mph E wind read +1.0% vs BallparkPal's -9%.) Using the magnitude of
        # the appropriate wr and letting out_c supply the sign fixes it.
        if out_c > 0:
            wr_mag = abs(base["wr_out"])
            _dir = BP_DIR_WR_OUT.get(park)
            if _dir:
                _b = _compass_to_bucket_idx(park, wd, ws)
                if _b and _b[1] in _dir:
                    wr_mag = abs(_dir[_b[1]])
        else:
            wr_mag = abs(base["wr_in"])
        w_adj = out_c * WIND_O * wr_mag * WIND_SCALE
        w_adj *= OF_MULT.get(base["of"], 1.0) * (CR_MULT.get(base["cr"], 1.0) + CQ_MULT.get(base["cq"], 1.0)) / 2
        wd_rarity = _wind_dir_rarity(park, wd, ws)
        ws_rarity = _wind_speed_rarity(park, ws)
        rarity_amp = max(wd_rarity * WIND_DIR_RARITY_AMP, ws_rarity * WIND_SPEED_RARITY_AMP)
        w_adj *= (1 + rarity_amp)
        w_adj *= _wind_receptivity_shrink(park)   # trim wind at low-receptivity parks (noise); high-wind tier untouched
        # Cap the per-component wind impact so a single park's bad wr value
        # can't dominate the total. Our single-hour NWS wind octant is too
        # coarse to justify a larger swing than this (see V8.2 calibration note).
        import math as _m
        w_adj = WIND_SOFT * _m.tanh(w_adj / WIND_SOFT)   # diminishing returns on strong wind
        w_adj = max(-WIND_CAP, min(WIND_CAP, w_adj))     # hard sentinel (rarely binds)
        wind_info = {"out_component": round(out_c, 2), "dir_rarity": round(wd_rarity, 2),
                     "spd_rarity": round(ws_rarity, 2)}

    # Dew point component
    today_dp = dew_point(t, hum)
    base_dp = dew_point(base["temp"], base["hum"])
    dp_adj = (today_dp - base_dp) * DP_C * altitude_mult

    # Pressure component
    p_adj = -(pres - base["pres"]) * PRES_C * altitude_mult

    # Carry offset
    carry_raw = base["carry"]
    # If carry is a large value like -48, -53, it's likely already a % offset.
    # V8 uses it as-is scaled by 100. Add interaction with out-wind.
    carry = carry_raw / 100 * CARRY_SCALE
    interact = delta_t * (wind_info["out_component"] if wind_info and wind_info["out_component"] > 0 else 0) * CARRY_INT * 0.01

    # Precipitation
    precip_adj = _precip_penalty(precip)

    # Sum
    weather_sum = t_adj + w_adj + dp_adj + p_adj + interact + precip_adj
    weather_sum *= variation_mult * runs_amp
    total = weather_sum + carry

    # Dome dampen — skipped when the retractable roof is (likely) open, so the
    # park computes as a normal outdoor venue.
    if is_dome and not treat_as_open:
        dd = cal.get("dome_damp", DOME_DAMP_DEFAULT)
        total = -0.01 + total * dd

    # ── Outcome-anchored scale (2026 carry calibration) ───────────────────
    # V9's physical magnitude was originally tuned to BallparkPal's PREDICTED
    # runs. Validated against REALIZED batted-ball carry over 5,657 games
    # (2024-26): direction & rank-ordering are correct (within-park r=0.47 vs
    # carry residual; Coors altitude control r=0.76), but the run-% magnitude
    # runs hot. carry→runs calibration (β=0.0083 run/ft × ~9.7 FB/game, and
    # 0.83 ft of carry per +1% adj) implies ~0.067 realized runs per +1% vs the
    # 0.087 the raw % asserts → k≈0.77 (a carry-only lower bound; true k≈0.85
    # once non-carry effects are folded in). Shrink the published % toward the
    # realized run impact.
    EMPIRICAL_SCALE = 0.85
    run_adj_pct = total * 100 * EMPIRICAL_SCALE

    # ── V10 HR→runs damper (2026-07-21) ───────────────────────────────────
    # Validated on 1,127 station-obs 2026 games (park-year-demeaned, LOWO CV):
    # the published % tracks BallparkPal's HR number, not its RUNS number.
    # Per-side realized_x: suppression 1.14 (real, untouched), boosts 0.11 —
    # warm/wind-out boosts saturate ~+6% in realized runs and the big ones are
    # ~90% phantom. So ONLY the positive side is damped, via 6·tanh(raw/6).
    # WRIGLEY (CHC) is EXEMPT: its wind genuinely plays (2026 realized_x 0.86;
    # BP runs +30 = our +30 on 7/20). Applied to the published % (already
    # inclusive of WIND_SOFT + EMPIRICAL_SCALE — do not re-tune those).
    # Boost-damper exemptions: parks where wind genuinely realizes in runs get
    # NO positive damping, only a safety cap (value = the cap). CHC (Wrigley) is
    # VALIDATED (2026 realized_x 0.86; BP +30 = our +30 on 7/20). ATH (Sutter
    # Health) is a PRIOR add (2026-07-27) — small, hot, dry Sacramento launching
    # pad, physically Wrigley-like, but n~82 and UNVALIDATED; lower/conservative
    # cap. Re-check this exemption (keep/pull) once Sutter has n~150 out-wind games.
    DAMPER_EXEMPT = {"CHC": 40.0, "ATH": 15.0}
    DAMPER_A = 6.0
    if run_adj_pct > 0 and park not in DAMPER_EXEMPT:
        run_adj_pct = DAMPER_A * math.tanh(run_adj_pct / DAMPER_A)
    elif park in DAMPER_EXEMPT and run_adj_pct > DAMPER_EXEMPT[park]:
        run_adj_pct = DAMPER_EXEMPT[park]

    # Apply V8.1 cold cap for most parks.
    # For extreme cold (<42°F) at non-dome parks, let it go deeper (BP has shown larger penalties).
    capped = False
    if not is_dome and run_adj_pct < COLD_FLOOR_PCT and t >= 42:
        run_adj_pct = COLD_FLOOR_PCT
        capped = True

    return {
        "run_adj_pct": round(run_adj_pct, 1),
        "capped": capped,
        "is_dome": is_dome,
        "components": {
            "temp_delta": round(delta_t, 1),
            "t_adj_pct": round(t_adj * 100, 1),
            "w_adj_pct": round(w_adj * 100, 1) if wind_info else 0.0,
            "dp_adj_pct": round(dp_adj * 100, 1),
            "p_adj_pct": round(p_adj * 100, 1),
            "precip_adj_pct": round(precip_adj * 100, 1),
            "carry_pct": round(carry * 100, 1),
            "wind": wind_info,
        },
    }


# Helper: convert NWS wind direction string (e.g. "NW") to compass degrees
NWS_COMPASS = {
    "N":0, "NNE":22.5, "NE":45, "ENE":67.5, "E":90, "ESE":112.5, "SE":135, "SSE":157.5,
    "S":180, "SSW":202.5, "SW":225, "WSW":247.5, "W":270, "WNW":292.5, "NW":315, "NNW":337.5,
}

def nws_wind_to_compass(s):
    if not s: return None
    return NWS_COMPASS.get(s.strip().upper())
