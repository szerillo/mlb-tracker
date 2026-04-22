"""
V8 MLB Weather Model — port of the compute function and all park data.

Based on V8_WEATHER_MODEL_METHODOLOGY.md. Includes the cold-quadratic cap
at -15% (V8.1 operational guidance).

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
    "LAA":{"temp":77,"hum":51,"pres":1013,"carry":-48.00,"wr_out":-2.36,"wr_in":3.32,"of":"Small","cr":"Avg","cq":"Good","var":0.79,"runs":-1,"alt":160},
    "BAL":{"temp":76,"hum":59,"pres":1015,"carry":-69.00,"wr_out":2.40,"wr_in":0.62,"of":"Variable","cr":"Great","cq":"Good","var":1.41,"runs":9,"alt":130},
    "BOS":{"temp":70,"hum":60,"pres":1015,"carry":-1.55,"wr_out":4.58,"wr_in":1.20,"of":"Variable","cr":"Good","cq":"Great","var":1.84,"runs":12,"alt":20},
    "CHW":{"temp":70,"hum":63,"pres":1015,"carry":-1.06,"wr_out":1.27,"wr_in":-0.38,"of":"Small","cr":"Bad","cq":"Avg","var":1.18,"runs":-3,"alt":596},
    "CLE":{"temp":70,"hum":65,"pres":1016,"carry":-77.00,"wr_out":0.75,"wr_in":2.47,"of":"Small","cr":"Avg","cq":"Poor","var":1.51,"runs":-3,"alt":582},
    "KC": {"temp":78,"hum":56,"pres":1014,"carry":23.00,"wr_out":0.65,"wr_in":1.99,"of":"X","cr":"Great","cq":"Good","var":1.21,"runs":7,"alt":750},
    "TB": {"temp":72,"hum":44,"pres":1014,"carry":-53.00,"wr_out":0,"wr_in":0,"of":"Medium","cr":"Poor","cq":"Poor","var":0.03,"runs":-7,"alt":0,"dome":True},
    "TOR":{"temp":73,"hum":59,"pres":1015,"carry":-1.67,"wr_out":-1.02,"wr_in":-1.37,"of":"Medium","cr":"Great","cq":"Good","var":0.81,"runs":-3,"alt":247,"dome":True},
    "NYY":{"temp":74,"hum":56,"pres":1015,"carry":-1.50,"wr_out":1.53,"wr_in":3.90,"of":"Variable","cr":"Avg","cq":"Great","var":1.95,"runs":-3,"alt":54},
    "DET":{"temp":72,"hum":56,"pres":1015,"carry":-1.54,"wr_out":0.36,"wr_in":4.69,"of":"Large","cr":"Avg","cq":"Avg","var":1.31,"runs":-1,"alt":596},
    "MIN":{"temp":73,"hum":53,"pres":1014,"carry":-53.00,"wr_out":1.62,"wr_in":-0.11,"of":"Medium","cr":"Avg","cq":"Good","var":0.98,"runs":3,"alt":812},
    "HOU":{"temp":80,"hum":48,"pres":1015,"carry":-1.68,"wr_out":1.83,"wr_in":-3.21,"of":"Variable","cr":"Bad","cq":"Poor","var":0.50,"runs":-3,"alt":38,"dome":True},
    "TEX":{"temp":81,"hum":42,"pres":1013,"carry":-1.18,"wr_out":1.34,"wr_in":0.79,"of":"Medium","cr":"Avg","cq":"Great","var":0.40,"runs":-5,"alt":616,"dome":True},
    "SEA":{"temp":71,"hum":51,"pres":1016,"carry":-2.15,"wr_out":2.32,"wr_in":1.35,"of":"Small","cr":"Poor","cq":"Bad","var":0.88,"runs":-13,"alt":10},
    "ATH":{"temp":81,"hum":40,"pres":1012,"carry":-95.00,"wr_out":1.01,"wr_in":2.52,"of":"Large","cr":"Good","cq":"Avg","var":1.00,"runs":15,"alt":26},
    "ATL":{"temp":82,"hum":50,"pres":1015,"carry":-44.00,"wr_out":2.61,"wr_in":-0.62,"of":"Medium","cr":"Poor","cq":"Great","var":0.90,"runs":-7,"alt":1050},
    "MIA":{"temp":80,"hum":59,"pres":1017,"carry":-1.07,"wr_out":0.04,"wr_in":2.01,"of":"Large","cr":"Good","cq":"Avg","var":0.30,"runs":-1,"alt":15,"dome":True},
    "NYM":{"temp":73,"hum":57,"pres":1015,"carry":-1.21,"wr_out":0.16,"wr_in":-0.43,"of":"Medium","cr":"Poor","cq":"Poor","var":1.37,"runs":-9,"alt":54},
    "PHI":{"temp":77,"hum":55,"pres":1015,"carry":-1.17,"wr_out":1.33,"wr_in":4.08,"of":"Small","cr":"Bad","cq":"Great","var":1.83,"runs":3,"alt":9},
    "WAS":{"temp":78,"hum":55,"pres":1015,"carry":-64.00,"wr_out":1.92,"wr_in":1.19,"of":"Medium","cr":"Great","cq":"Great","var":0.98,"runs":4,"alt":25},
    "CHC":{"temp":70,"hum":63,"pres":1015,"carry":-1.85,"wr_out":4.43,"wr_in":4.72,"of":"Medium","cr":"Poor","cq":"Bad","var":2.67,"runs":-4,"alt":596},
    "CIN":{"temp":76,"hum":61,"pres":1015,"carry":-49.00,"wr_out":-1.60,"wr_in":2.87,"of":"Small","cr":"Bad","cq":"Avg","var":0.88,"runs":10,"alt":683},
    "MIL":{"temp":76,"hum":60,"pres":1015,"carry":-1.01,"wr_out":0.50,"wr_in":-0.55,"of":"Medium","cr":"Avg","cq":"Avg","var":0.67,"runs":-10,"alt":0,"dome":True},
    "PIT":{"temp":74,"hum":58,"pres":1015,"carry":-73.00,"wr_out":0.84,"wr_in":2.48,"of":"Variable","cr":"Good","cq":"Bad","var":1.01,"runs":0,"alt":743},
    "STL":{"temp":79,"hum":58,"pres":1014,"carry":-77.00,"wr_out":2.63,"wr_in":1.28,"of":"Large","cr":"Good","cq":"Avg","var":1.25,"runs":-5,"alt":455},
    "ARI":{"temp":88,"hum":15,"pres":1010,"carry":68.00,"wr_out":0.19,"wr_in":1.09,"of":"Large","cr":"Great","cq":"Bad","var":0.48,"runs":2,"alt":1082,"dome":True},
    "COL":{"temp":75,"hum":28,"pres":1012,"carry":3.75,"wr_out":0.82,"wr_in":1.81,"of":"X","cr":"Great","cq":"Avg","var":1.36,"runs":32,"alt":5183},
    "LAD":{"temp":78,"hum":47,"pres":1012,"carry":-1.49,"wr_out":-0.32,"wr_in":3.19,"of":"Medium","cr":"Avg","cq":"Great","var":0.87,"runs":0,"alt":267},
    "SD": {"temp":72,"hum":62,"pres":1013,"carry":-1.77,"wr_out":0.68,"wr_in":1.64,"of":"Medium","cr":"Avg","cq":"Avg","var":1.00,"runs":-3,"alt":13},
    "SF": {"temp":66,"hum":64,"pres":1014,"carry":-2.30,"wr_out":0.08,"wr_in":0.82,"of":"Variable","cr":"Good","cq":"Poor","var":0.81,"runs":-3,"alt":63},
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

GPS_CF = {
    "ARI":52,"ATL":42,"BAL":49,"BOS":63,"CHC":9,"CHW":37,"CIN":41,"CLE":9,
    "COL":69,"DET":81,"HOU":41,"KC":49,"LAA":50,"LAD":53,"MIA":37,"MIL":46,
    "MIN":38,"NYM":7,"NYY":37,"PHI":46,"PIT":37,"SD":48,"SEA":2,"SF":64,
    "STL":47,"TB":45,"TEX":43,"TOR":44,"WAS":44,"ATH":43,
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
TEMP_C = 0.003
COLD_T = 12
COLD_A = 0.0002
WIND_O = 0.001
WIND_I = 0.001
# WIND_SCALE was 10.0 which double-counted the per-park wr_out/wr_in
# values (those are already in "%/10mph" units). Calibrated to 2.0 to
# match BallparkPal magnitudes on a 15-game slate (Wrigley drops from
# +37% wind to ~+7%, LAA from -22% to ~-4%).
WIND_SCALE = 2.0
DP_C = 0.0010
PRES_C = 0.0018
CARRY_INT = 0.0003
CARRY_SCALE = 0.04
T_SENS_FLOOR_HOT = 1.5
T_SENS_FLOOR_COLD = 0.8
COLD_MULT_CAP = 4.0
DOME_DAMP_DEFAULT = 0.15

VARIATION_AMP_COEF = 0.15
RUNS_AMP_COEF = 0.25
NARROW_AMP_COEF = 0.20
ALTITUDE_REF = 500
ALTITUDE_COEF = 0.15

WIND_DIR_RARITY_AMP = 0.15
WIND_SPEED_RARITY_AMP = 0.10
TEMP_PCT_AMP = 0.15
EXTREME_PCT_BOOST = 0.08

PRECIP_T1 = 25
PRECIP_T2 = 60
PRECIP_MAX_PENALTY = -0.035

TEMP_TREND_AMP = 0.15
TEMP_TREND_DAMPEN = 0.85

CR_MULT = {"Bad":0.94,"Poor":0.97,"Avg":1.0,"Good":1.04,"Great":1.07}
CQ_MULT = {"Bad":0.94,"Poor":0.97,"Avg":1.0,"Good":1.04,"Great":1.07}
OF_MULT = {"Small":0.90,"Medium":1.0,"Variable":1.04,"Large":1.12,"X":1.08}

# V8.1 operational cold cap (user-tuned to -25%, allowing deep cold BP-style penalties)
COLD_FLOOR_PCT = -25.0

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
    return ws * math.cos(math.radians(angle_from_cf))


# ============================================================================
# Main compute
# ============================================================================
def compute_v8(park, wx):
    """
    Compute V8 weather-only run adjustment percentage for a park.

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

    # Wind component (requires wd_compass)
    w_adj = 0
    wind_info = None
    if wd is not None and ws > 0:
        out_c = _compass_to_out_component(park, wd, ws)
        wr_use = base["wr_out"] if out_c > 0 else base["wr_in"]
        w_adj = out_c * WIND_O * wr_use * WIND_SCALE
        w_adj *= OF_MULT.get(base["of"], 1.0) * (CR_MULT.get(base["cr"], 1.0) + CQ_MULT.get(base["cq"], 1.0)) / 2
        wd_rarity = _wind_dir_rarity(park, wd, ws)
        ws_rarity = _wind_speed_rarity(park, ws)
        rarity_amp = max(wd_rarity * WIND_DIR_RARITY_AMP, ws_rarity * WIND_SPEED_RARITY_AMP)
        w_adj *= (1 + rarity_amp)
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

    # Dome dampen
    if is_dome:
        dd = cal.get("dome_damp", DOME_DAMP_DEFAULT)
        total = -0.01 + total * dd

    run_adj_pct = total * 100

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
