#!/usr/bin/env python3
"""
Player Futures awards model — V5.1 / V6.3 methodology.

Inputs
  • data/player_futures_odds.json           (refresh_player_futures_odds.py)
  • data/player_war_projections.json        (refresh_player_war_projections.py)
  • data/team_futures.json (for MVP team bonus — Playoff% + Division%)

Output
  data/player_futures.json — six markets keyed AL_MVP / NL_MVP / AL_CY /
  NL_CY / AL_ROY / NL_ROY. Each market has the model's top-N candidates with
  composite score, model probability, market-implied probability (best price),
  edge, and a per-player star rating (★ 0.5-2%, ★★ 2-4%, ★★★ 4%+).

Methodology (per the user's "Awards Model 5.1.rtf"):

  MVP (PA ≥ 400, league-assigned hitters):
    z-weights → WAR .791 / OPS .714 / R .587 / HR .561 / RBI .450 / SB .187
    composite = weighted_z + 0.20·z(Dim) + 0.10·z(Skew) − 0.10·z(Vol)
              + 0.06·Playoff% + 0.04·Division%
    Special case: Ohtani combined hit + pitch WAR.

  CY (IP ≥ 120, SV < 5, league-assigned pitchers):
    z-weights → WAR .705 / ERA⁻¹ .409 / K-BB% .350 / K .368 / W .300
                 / WHIP⁻¹ .243 / IP .174
    composite = weighted_z + 0.10·z(Skew) − 0.10·z(Vol) + 0.05·z(Dim)

  ROY (rookies only; hitter PA ≥ 100 or pitcher IP ≥ 30):
    Hitter actual: WAR .731 / R .615 / RBI .459 / H .455 / HR .419 / OPS .400
    Pitcher actual: WAR .680 / W .548 / K .547 / IP .428 / ERA⁻¹ .400
    talent_z = z(EOS WAR) within the rookie pool (proxy for Steamer 600 talent)
    PT_factor = √(PT_ratio capped at 1.0)
    prospect_bonus = 0.30 · max(0, 1 − rank / 150)
    score = 0.40·actual_z + 0.40·(talent_z·PT_factor) + 0.30·prospect_bonus
            + 0.05·z(Skew)
    Final cross-pool: 0.60·z(within-pool) + 0.40·z(raw WAR)

  Calibration: softmax(score / temp). Temperature found by bounded scalar
  KL-divergence minimization vs market implied probs. Top-N pools: MVP 30,
  CY 15, ROY 12.

  Edge = model_p − best_market_implied_p.

  Best-price comparison uses American odds payout ratio.
"""
from __future__ import annotations
import datetime, json, math, sys, unicodedata
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

ODDS_PATH = REPO_ROOT / "data" / "player_futures_odds.json"
WAR_PATH  = REPO_ROOT / "data" / "player_war_projections.json"
TF_PATH   = REPO_ROOT / "data" / "team_futures.json"
OUTPUT    = REPO_ROOT / "data" / "player_futures.json"

# ── Rookie eligibility list (preseason consensus, from Awards Data.xlsx) ─────
# Tuple: (name, avg_rank, is_roy_candidate, team_preseason, league_preseason)
# Names match VegasInsider/FG strings (no accents); ROY filter unions this list
# with anyone the VI ROY board posts who's missing here (rookies added live).
ROOKIES_RAW = [
    ('Konnor Griffin', 1.0, True, 'PIT', 'NL'),
    ('Kevin McGonigle', 2.5, True, 'DET', 'AL'),
    ('Jesus Made', 3.0, True, None, None),
    ('JJ Wetherholt', 7.0, True, 'STL', 'NL'),
    ('Max Clark', 7.0, True, 'DET', 'AL'),
    ('Samuel Basallo', 7.0, True, 'BAL', 'AL'),
    ('Nolan McLean', 8.2, True, 'NYM', 'NL'),
    ('Leo De Vries', 8.5, False, 'ATH', 'AL'),
    ('Colt Emerson', 8.7, True, 'SEA', 'AL'),
    ('Bubba Chandler', 12.3, True, 'PIT', 'NL'),
    ('Trey Yesavage', 12.7, True, 'TOR', 'AL'),
    ('Aidan Miller', 13.2, True, 'PHI', 'NL'),
    ('Sebastian Walcott', 14.2, True, 'TEX', 'AL'),
    ('Walker Jenkins', 14.8, True, 'MIN', 'AL'),
    ('Carson Benge', 16.5, True, 'NYM', 'NL'),
    ('Josue De Paula', 18.5, True, 'LAD', 'NL'),
    ('Thomas White', 19.8, True, 'MIA', 'NL'),
    ('Payton Tolle', 23.0, True, 'BOS', 'AL'),
    ('Carter Jensen', 23.3, True, 'KCR', 'AL'),
    ('Rainel Rodriguez', 26.8, False, None, None),
    ('Sal Stewart', 26.8, True, 'CIN', 'NL'),
    ('Eduardo Quintero', 27.7, False, 'LAD', 'NL'),
    ('Franklin Arias', 27.8, False, 'BOS', 'AL'),
    ('Kade Anderson', 28.0, False, None, None),
    ('Eli Willits', 28.3, True, None, None),
    ('Bryce Eldridge', 29.0, True, 'SFG', 'NL'),
    ('Bryce Rainer', 29.2, False, 'DET', 'AL'),
    ('Luis Pena', 30.0, False, None, None),
    ('Andrew Painter', 31.2, True, 'PHI', 'NL'),
    ('Ryan Sloan', 31.7, False, 'SEA', 'AL'),
    ('Liam Doyle', 32.2, False, None, None),
    ('Travis Bazzana', 32.2, True, 'CLE', 'AL'),
    ('Josuar Gonzalez', 34.3, False, 'SFG', 'NL'),
    ('George Lombard Jr.', 35.5, True, 'NYY', 'AL'),
    ('Caleb Bonemer', 37.5, False, 'CHW', 'AL'),
    ('Zyhir Hope', 37.5, False, 'LAD', 'NL'),
    ('Jonah Tong', 46.0, True, 'NYM', 'NL'),
    ('Ryan Waldschmidt', 46.3, True, 'ARI', 'NL'),
    ('Gage Jump', 49.0, True, 'ATH', 'AL'),
    ('Connelly Early', 50.2, False, 'BOS', 'AL'),
    ('Alfredo Duno', 51.5, False, 'CIN', 'NL'),
    ('Carson Williams', 52.3, True, 'TBR', 'AL'),
    ('Jett Williams', 52.5, False, 'MIL', 'NL'),
    ('Edward Florentino', 53.3, False, 'PIT', 'NL'),
    ('Chase DeLauter', 54.0, True, 'CLE', 'AL'),
    ('Robby Snelling', 54.5, True, 'MIA', 'NL'),
    ('Seth Hernandez', 54.8, False, None, None),
    ('Josue Briceno', 55.8, True, None, None),
    ('Angel Genao', 56.8, False, 'CLE', 'AL'),
    ('Dylan Beavers', 58.2, True, 'BAL', 'AL'),
    ('Jamie Arnold', 58.7, False, None, None),
    ('Moises Ballesteros', 58.8, False, None, None),
    ('Mike Sirota', 59.3, False, 'LAD', 'NL'),
    ('Ethan Holliday', 59.7, True, None, None),
    ('Aiva Arquette', 59.8, False, 'MIA', 'NL'),
    ('Joe Mack', 63.0, True, 'MIA', 'NL'),
    ('JoJo Parker', 63.8, False, None, None),
    ('Michael Arroyo', 65.3, True, 'SEA', 'AL'),
    ('Eduardo Tait', 66.2, False, 'MIN', 'AL'),
    ('Jarlin Susana', 66.5, True, 'WSN', 'NL'),
    ('Noah Schultz', 67.0, True, 'CHW', 'AL'),
    ('Owen Caissie', 69.3, True, 'MIA', 'NL'),
    ('Tyler Bremner', 70.0, False, None, None),
    ('Ralphy Velazquez', 70.2, False, 'CLE', 'AL'),
    ('Brody Hopkins', 70.3, False, 'TBR', 'AL'),
    ('Cam Caminiti', 70.3, False, 'ATL', 'NL'),
    ('Lazaro Montes', 72.7, True, 'SEA', 'AL'),
    ('Arjun Nimmala', 73.2, True, 'TOR', 'AL'),
    ('Braden Montgomery', 73.2, True, 'CHW', 'AL'),
    ('Emil Morales', 73.3, False, 'LAD', 'NL'),
    ('Brandon Sproat', 73.7, False, 'MIL', 'NL'),
    ('Elmer Rodriguez', 73.7, False, None, None),
    ('Emmanuel Rodriguez', 74.0, True, 'MIN', 'AL'),
    ('Justin Crawford', 75.7, True, 'PHI', 'NL'),
    ('AJ Ewing', 75.8, False, None, None),
    ('Kaelen Culpepper', 75.8, False, 'MIN', 'AL'),
    ('Jaxon Wiggins', 76.5, False, 'CHC', 'NL'),
    ('Dax Kilby', 77.7, False, None, None),
    ('Parker Messick', 80.8, False, 'CLE', 'AL'),
    ('Kyson Witherspoon', 81.0, False, None, None),
    ('Charlie Condon', 81.2, True, 'COL', 'NL'),
    ('Hagen Smith', 81.3, True, 'CHW', 'AL'),
    ('Theo Gillen', 81.8, False, 'TBR', 'AL'),
    ('Carlos Lagrange', 82.5, False, 'NYY', 'AL'),
    ('Cooper Pratt', 82.7, False, 'MIL', 'NL'),
    ('Trey Gibson', 84.2, False, 'BAL', 'AL'),
    ('Kevin Alcantara', 84.3, False, None, None),
    ('Travis Sykora', 85.5, False, 'WSN', 'NL'),
    ('JR Ritchie', 86.7, False, 'ATL', 'NL'),
    ('Jonny Farmelo', 87.2, False, 'SEA', 'AL'),
    ('Harry Ford', 88.2, True, 'WSN', 'NL'),
    ('Jeferson Quero', 88.2, True, 'MIL', 'NL'),
    ('Jhonny Level', 88.3, False, 'SFG', 'NL'),
    ('Joshua Baez', 88.3, False, 'STL', 'NL'),
    ('Nate George', 88.8, False, 'BAL', 'AL'),
    ('Jurrangelo Cjintje', 89.0, False, None, None),
    ('Billy Carlson', 89.2, False, None, None),
    ('Caden Scarborough', 89.8, False, 'TEX', 'AL'),
    ('Jefferson Rojas', 89.8, False, 'CHC', 'NL'),
    ('Blake Mitchell', 90.2, False, 'KCR', 'AL'),
    ('Rhett Lowder', 90.3, True, 'CIN', 'NL'),
    ('Tyson Lewis', 91.3, False, 'CIN', 'NL'),
    ('Gavin Fien', 91.5, False, None, None),
    ('Logan Henderson', 92.0, True, 'MIL', 'NL'),
    ('Ethan Conray', 92.3, False, None, None),
    ('Kendry Chourio', 93.0, False, 'KCR', 'AL'),
    ('Jacob Reimer', 93.2, False, 'NYM', 'NL'),
    ('Connor Prielipp', 93.8, True, 'MIN', 'AL'),
    ('George Klassen', 93.8, False, 'LAA', 'AL'),
    ('Khal Stephen', 94.0, False, 'CLE', 'AL'),
    ('Luis De Leon', 94.0, False, 'SDP', 'NL'),
    ('Jacob Melton', 94.3, False, 'TBR', 'AL'),
    ('River Ryan', 94.5, False, 'LAD', 'NL'),
    ('Felnin Celesten', 94.7, False, 'SEA', 'AL'),
    ('Ethan Salas', 95.7, False, 'SDP', 'NL'),
    ('Braylon Doughty', 95.8, False, 'CLE', 'AL'),
    ('Yolfran Castillo', 96.0, False, 'TEX', 'AL'),
    ('Aroon Escobar', 96.3, False, 'PHI', 'NL'),
    ('Cooper Ingle', 96.3, False, 'CLE', 'AL'),
    ('Didier Fuentes', 97.2, False, 'ATL', 'NL'),
    ('Johnny King', 97.3, False, 'TOR', 'AL'),
    ('Gavin Kilen', 97.5, False, None, None),
    ('Jack Wenninger', 97.5, False, 'NYM', 'NL'),
    ('Wehiwa Aloy', 98.2, False, None, None),
    ('Andrew Fischer', 98.3, False, None, None),
    ('Gage Wood', 98.7, False, None, None),
    ('David Davalillo', 99.2, False, 'TEX', 'AL'),
    ('Justin Gonzales', 99.5, False, 'BOS', 'AL'),
    ('Michael Forret', 99.7, False, 'TBR', 'AL'),
    ('Jimmy Crooks', 99.8, False, 'STL', 'NL'),
    ('Steele Hall', 99.8, False, None, None),
    ('Joey Oakie', 100.0, False, 'CLE', 'AL'),
    ('Christian Oppor', 100.2, False, 'CHW', 'AL'),
    ('Ike Irish', 100.2, False, None, None),
    ('Ryan Clifford', 100.2, False, 'NYM', 'NL'),
    ('Alex Freeland', 100.5, True, 'LAD', 'NL'),
    ('Bo Davidson', 100.5, False, 'SFG', 'NL'),
    ('Ben Hess', 100.7, False, 'NYY', 'AL'),
    ('Kruz Schoolcraft', 100.7, False, None, None),
    ('Tommy Troy', 100.7, False, 'ARI', 'NL'),
    ('Roldy Brito', 100.8, False, 'COL', 'NL'),
    ('Juan Valera', 101.0, False, 'BOS', 'AL'),
    ('Bishop Letson', 101.2, False, 'MIL', 'NL'),
    ('Max Anderson', 101.2, False, 'DET', 'AL'),
    ('Tanner McDougal', 101.5, False, 'CHW', 'AL'),
    ('Winston Santos', 101.5, False, 'TEX', 'AL'),
    ('Charlee Soto', 101.7, False, 'MIN', 'AL'),
    ('Seaver King', 101.7, False, 'WSN', 'NL'),
    ('Ty Johnson', 101.7, False, 'TBR', 'AL'),
    ('Leonardo Bernal', 101.8, False, 'STL', 'NL'),
    ('Thayron Liranzo', 101.8, False, 'DET', 'AL'),
    ('Will Watson', 101.8, False, 'NYM', 'NL'),
    ('Daniel Pierce', 102.0, False, None, None),
    ('Hunter Barco', 102.0, True, 'PIT', 'NL'),
    ('Enrique Bradfield Jr.', 102.2, False, 'BAL', 'AL'),
    ('Kayson Cunningham', 102.2, False, None, None),
    ('Jake Bloss', 102.3, False, 'TOR', 'AL'),
    ('Cam Collier', None, True, 'CIN', 'NL'),
    ('Chase Petty', None, True, 'CIN', 'NL'),
    ('Cooper Early', None, True, None, None),
    ('Jace Williams', None, True, None, None),
    ('Jackson Arnold', None, True, None, None),
    ('Jackson Wiggins', None, True, None, None),
    ('Jhostynxon Garcia', None, True, 'PIT', 'NL'),
    ('Jurrangelo Cijntje', None, True, 'STL', 'NL'),
    ('Kazuma Okamoto', None, True, 'TOR', 'AL'),
    ('Kolek Anderson', None, True, None, None),
    ('Leodalis De Vries', None, True, None, None),
    ('Moisés Ballesteros', None, True, 'CHC', 'NL'),
    ('Munetaka Murakami', None, True, 'CHW', 'AL'),
    ('Quinn Mathews', None, True, 'STL', 'NL'),
    ('Ricky Tiedemann', None, True, 'TOR', 'AL'),
    ('Spencer Jones', None, True, 'NYY', 'AL'),
    ('Tomoyuki Imai', None, True, None, None),
    ('Trent Bremner', None, True, None, None),
    ('Zac Veen', None, True, 'COL', 'NL'),
]


# Lookup helpers built at import time
ROOKIE_BY_NORM = {}   # filled below


# ── Numeric / stat helpers ─────────────────────────────────────────────────
def _norm_name(s):
    if not isinstance(s, str): return ""
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = s.lower()
    for suf in (" jr.", " jr", " sr.", " sr", " iii", " ii"):
        if s.endswith(suf): s = s[:-len(suf)]
    return s.replace(".", "").replace("'", "").strip()


def _build_rookie_index():
    """ROOKIE_BY_NORM[normalized name] = {avg_rank, is_roy, team, league}."""
    for nm, rk, roy, tm, lg in ROOKIES_RAW:
        key = _norm_name(nm)
        if not key: continue
        ROOKIE_BY_NORM[key] = {
            "name": nm, "avg_rank": rk, "is_roy": bool(roy),
            "team_preseason": tm, "league_preseason": lg}


def _mean(xs):
    xs = [x for x in xs if isinstance(x, (int, float)) and not (isinstance(x, float) and math.isnan(x))]
    return (sum(xs) / len(xs)) if xs else None


def _stdev(xs, mu):
    xs = [x for x in xs if isinstance(x, (int, float)) and not (isinstance(x, float) and math.isnan(x))]
    if len(xs) < 2: return None
    var = sum((x - mu) ** 2 for x in xs) / (len(xs) - 1)
    return math.sqrt(var) if var > 0 else None


def _zscore(value, pool):
    if value is None: return 0.0
    mu = _mean(pool)
    if mu is None: return 0.0
    sigma = _stdev(pool, mu)
    if not sigma: return 0.0
    return (value - mu) / sigma


def _amer_to_prob(odds):
    if odds is None: return None
    o = int(odds)
    return 100 / (o + 100) if o > 0 else abs(o) / (abs(o) + 100)


def _better(cand, best):
    """American-odds best-price comparison (higher payout = better)."""
    def payout(o): return o / 100 if o > 0 else 100 / abs(o)
    return payout(cand) > payout(best)


def _safe(v, default=0.0):
    if v is None or (isinstance(v, float) and math.isnan(v)): return default
    return v


def _inv(v):
    """Invert a rate stat (ERA / WHIP) so higher = better. Returns None on missing."""
    if v is None or v == 0: return None
    return -v   # negate; z-score normalizes scale so sign-flip is enough


# ── Softmax + KL-divergence calibration ────────────────────────────────────
def _softmax(scores, temp):
    if not scores: return []
    m = max(scores)
    exps = [math.exp((s - m) / max(temp, 1e-3)) for s in scores]
    z = sum(exps)
    return [e / z for e in exps] if z else [0.0] * len(scores)


def _kl(p, q):
    """KL(p ‖ q) with tiny-prob floor."""
    eps = 1e-9
    return sum(pi * math.log((pi + eps) / (qi + eps)) for pi, qi in zip(p, q))


def _calibrate_temp(scores, market_p, lo=0.1, hi=5.0, iters=64):
    """Bounded scalar KL minimization via golden-section search."""
    phi = (math.sqrt(5) - 1) / 2
    a, b = lo, hi
    c = b - phi * (b - a); d = a + phi * (b - a)
    def loss(t):
        p = _softmax(scores, t)
        return _kl(market_p, p)
    fc, fd = loss(c), loss(d)
    for _ in range(iters):
        if fc < fd: b, d, fd = d, c, fc; c = b - phi * (b - a); fc = loss(c)
        else:       a, c, fc = c, d, fd; d = a + phi * (b - a); fd = loss(d)
        if abs(b - a) < 1e-3: break
    t = (a + b) / 2
    return t


# ── Per-player projection accessors ────────────────────────────────────────
def _eos(player, stat, default=None):
    """Get EOS stat, falling back to YTD if EOS missing."""
    eos = (player or {}).get("eos") or {}
    if eos.get(stat) is not None: return eos[stat]
    ytd = (player or {}).get("ytd") or {}
    return ytd.get(stat, default)


def _ros_blend(player, stat, default=None):
    ros = (player or {}).get("ros") or {}
    blend = ros.get("blend") or {}
    return blend.get(stat, default)


def _atc(player, mod):
    """mod ∈ {'vol','skew','dim'} — pulled from ROS-blend (ATC modifier copy)."""
    return _ros_blend(player, mod) or 0.0


def _proj_pa(player):
    """Total expected PA for a hitter — YTD PA + ROS blend PA."""
    return _eos(player, "pa") or 0


def _proj_ip(player):
    return _eos(player, "ip") or 0


# ── Pool filtering + score computation ─────────────────────────────────────
MVP_WEIGHTS = {"war": .791, "ops": .714, "r": .587, "hr": .561, "rbi": .450, "sb": .187}
CY_WEIGHTS  = {"war": .705, "era_inv": .409, "k_bb_pct": .350, "k": .368,
               "w": .300, "whip_inv": .243, "ip": .174}
ROY_HIT_WEIGHTS = {"war": .731, "r": .615, "rbi": .459, "h": .455, "hr": .419, "ops": .400}
ROY_PIT_WEIGHTS = {"war": .680, "w": .548, "k": .547, "ip": .428, "era_inv": .400}


def _weighted_z(pool_player_stats, weights):
    """pool_player_stats: list of dicts (one per player) with stat -> value.
    weights: stat -> weight. Returns list of composite z-scores."""
    if not pool_player_stats: return []
    wsum = sum(weights.values()) or 1.0
    norm = {k: v / wsum for k, v in weights.items()}
    composites = []
    pools = {stat: [p.get(stat) for p in pool_player_stats] for stat in weights}
    for p in pool_player_stats:
        s = 0.0
        for stat, w in norm.items():
            s += w * _zscore(p.get(stat), pools[stat])
        composites.append(s)
    return composites


def _mvp_pool(hitters, league, team_futures):
    """All hitters with PA ≥ 400 in this league, plus their stat dicts."""
    tf_by_abbr = (team_futures or {}).get("teams", {})
    pool = []
    for h in hitters.values():
        if h.get("league") != league: continue
        if _proj_pa(h) < 400: continue
        team = h.get("team_abbr")
        playoff_pct = (tf_by_abbr.get(team, {}).get("composite", {})
                       .get("playoff_pct") or 0)
        div_pct = (tf_by_abbr.get(team, {}).get("composite", {})
                   .get("div_pct") or 0)
        pool.append({
            "player": h,
            "stats": {
                "war": _eos(h, "war"),  "ops": _eos(h, "ops"),
                "r":   _eos(h, "r"),    "hr":  _eos(h, "hr"),
                "rbi": _eos(h, "rbi"),  "sb":  _eos(h, "sb"),
            },
            "atc": {"vol": _atc(h, "vol"), "skew": _atc(h, "skew"), "dim": _atc(h, "dim")},
            "team_bonus": 0.06 * (playoff_pct or 0) + 0.04 * (div_pct or 0),
        })
    return pool


def _cy_pool(pitchers, league):
    pool = []
    for p in pitchers.values():
        if p.get("league") != league: continue
        if _proj_ip(p) < 120: continue
        sv = _eos(p, "sv") or 0
        if sv >= 5: continue
        era = _eos(p, "era"); whip = _eos(p, "whip")
        pool.append({
            "player": p,
            "stats": {
                "war":      _eos(p, "war"),
                "era_inv":  _inv(era),
                "k_bb_pct": _eos(p, "k_bb_pct"),
                "k":        _eos(p, "k"),
                "w":        _eos(p, "w"),
                "whip_inv": _inv(whip),
                "ip":       _eos(p, "ip"),
            },
            "atc": {"vol": _atc(p, "vol"), "skew": _atc(p, "skew"), "dim": _atc(p, "dim")},
        })
    return pool


def _roy_pool(hitters, pitchers, league, odds_players):
    """Rookies (preseason list) + any new names on the odds board, league-filtered."""
    # Anyone on the odds board for this market is automatically eligible
    extra_names = {_norm_name(p["name"]) for p in (odds_players or [])}
    pool_h, pool_p = [], []
    # Hitters
    for h in hitters.values():
        if h.get("league") != league: continue
        nm_key = _norm_name(h.get("name"))
        if not nm_key: continue
        rook = ROOKIE_BY_NORM.get(nm_key)
        if not rook and nm_key not in extra_names: continue
        if _proj_pa(h) < 100: continue
        rank = (rook or {}).get("avg_rank")
        prospect_bonus = 0.30 * max(0, 1 - ((rank or 150) / 150))
        # Playing time factor: PA / 600, capped at 1
        pt_ratio = min(1.0, max(0, _proj_pa(h)) / 600.0)
        pool_h.append({
            "player": h, "side": "hit",
            "stats": {
                "war": _eos(h, "war"), "r": _eos(h, "r"),
                "rbi": _eos(h, "rbi"), "h": _eos(h, "h"),
                "hr": _eos(h, "hr"),   "ops": _eos(h, "ops"),
            },
            "talent_war": _eos(h, "war"),
            "pt_factor": math.sqrt(pt_ratio),
            "prospect_bonus": prospect_bonus,
            "atc": {"vol": _atc(h, "vol"), "skew": _atc(h, "skew"), "dim": _atc(h, "dim")},
            "pos": h.get("pos") or "OF",
        })
    # Pitchers
    for p in pitchers.values():
        if p.get("league") != league: continue
        nm_key = _norm_name(p.get("name"))
        if not nm_key: continue
        rook = ROOKIE_BY_NORM.get(nm_key)
        if not rook and nm_key not in extra_names: continue
        if _proj_ip(p) < 30: continue
        rank = (rook or {}).get("avg_rank")
        prospect_bonus = 0.30 * max(0, 1 - ((rank or 150) / 150))
        pt_ratio = min(1.0, max(0, _proj_ip(p)) / 200.0)
        era = _eos(p, "era")
        pool_p.append({
            "player": p, "side": "pit",
            "stats": {
                "war": _eos(p, "war"), "w": _eos(p, "w"),
                "k":   _eos(p, "k"),   "ip": _eos(p, "ip"),
                "era_inv": _inv(era),
            },
            "talent_war": _eos(p, "war"),
            "pt_factor": math.sqrt(pt_ratio),
            "prospect_bonus": prospect_bonus,
            "atc": {"vol": _atc(p, "vol"), "skew": _atc(p, "skew"), "dim": _atc(p, "dim")},
            "pos": "SP",
        })
    return pool_h, pool_p


# ── Compute composite scores per market ────────────────────────────────────
def _score_mvp(pool):
    if not pool: return []
    base_z = _weighted_z([p["stats"] for p in pool], MVP_WEIGHTS)
    vol_pool  = [p["atc"]["vol"]  for p in pool]
    skew_pool = [p["atc"]["skew"] for p in pool]
    dim_pool  = [p["atc"]["dim"]  for p in pool]
    out = []
    for i, p in enumerate(pool):
        atc_adj = (0.20 * _zscore(p["atc"]["dim"], dim_pool)
                  + 0.10 * _zscore(p["atc"]["skew"], skew_pool)
                  - 0.10 * _zscore(p["atc"]["vol"], vol_pool))
        score = base_z[i] + atc_adj + p["team_bonus"]
        out.append({**p, "score": score})
    return out


def _score_cy(pool):
    if not pool: return []
    base_z = _weighted_z([p["stats"] for p in pool], CY_WEIGHTS)
    vol_pool  = [p["atc"]["vol"]  for p in pool]
    skew_pool = [p["atc"]["skew"] for p in pool]
    dim_pool  = [p["atc"]["dim"]  for p in pool]
    out = []
    for i, p in enumerate(pool):
        atc_adj = (0.10 * _zscore(p["atc"]["skew"], skew_pool)
                  - 0.10 * _zscore(p["atc"]["vol"], vol_pool)
                  + 0.05 * _zscore(p["atc"]["dim"], dim_pool))
        score = base_z[i] + atc_adj
        out.append({**p, "score": score})
    return out


def _score_roy(pool_h, pool_p):
    """Score hitters + pitchers separately, then cross-pool merge:
       Final_Score = 0.60·z(within-pool) + 0.40·z(raw WAR)."""
    h_z = _weighted_z([p["stats"] for p in pool_h], ROY_HIT_WEIGHTS)
    p_z = _weighted_z([p["stats"] for p in pool_p], ROY_PIT_WEIGHTS)
    out = []
    # Hitters within-pool composite
    for i, p in enumerate(pool_h):
        talent_z = _zscore(p["talent_war"], [x["talent_war"] for x in pool_h])
        skew_z   = _zscore(p["atc"]["skew"], [x["atc"]["skew"] for x in pool_h])
        within = (0.40 * h_z[i]
                + 0.40 * (talent_z * p["pt_factor"])
                + 0.30 * p["prospect_bonus"]
                + 0.05 * skew_z)
        out.append({**p, "within_pool": within})
    # Pitchers within-pool composite
    for i, p in enumerate(pool_p):
        talent_z = _zscore(p["talent_war"], [x["talent_war"] for x in pool_p])
        skew_z   = _zscore(p["atc"]["skew"], [x["atc"]["skew"] for x in pool_p])
        within = (0.40 * p_z[i]
                + 0.40 * (talent_z * p["pt_factor"])
                + 0.30 * p["prospect_bonus"]
                + 0.05 * skew_z)
        out.append({**p, "within_pool": within})
    # Cross-pool z-merge
    within_all = [x["within_pool"] for x in out]
    war_all    = [x["talent_war"]  for x in out]
    for x in out:
        x["score"] = (0.60 * _zscore(x["within_pool"], within_all)
                     + 0.40 * _zscore(x["talent_war"], war_all))
    return out


# ── Render market output ───────────────────────────────────────────────────
def _render_market(scored, market_key, market_meta, top_n):
    """Take scored candidates, join with odds, calibrate temp, emit final list."""
    odds_by_norm = {_norm_name(p["name"]): p for p in market_meta.get("players", [])}
    # Attach market odds to scored list (zero-fill if not on board)
    enriched = []
    for x in scored:
        nm = (x["player"].get("name") or "")
        odds_rec = odds_by_norm.get(_norm_name(nm))
        x["best_odds"]    = (odds_rec or {}).get("best_odds")
        x["best_book"]    = (odds_rec or {}).get("best_book")
        x["all_book_odds"]= (odds_rec or {}).get("all_book_odds", {})
        enriched.append(x)

    # Sort by score desc, take top N for model probability
    enriched.sort(key=lambda x: x["score"], reverse=True)
    pool = enriched[:top_n]

    # Calibrate temperature against market — drop players with no odds for the
    # KL-min fit; they still get a model probability assigned post-hoc.
    cal_pool = [x for x in pool if x["best_odds"] is not None]
    if cal_pool:
        scores  = [x["score"] for x in cal_pool]
        mkt_p   = [_amer_to_prob(x["best_odds"]) for x in cal_pool]
        mkt_sum = sum(mkt_p) or 1
        mkt_p_n = [p / mkt_sum for p in mkt_p]   # normalize so dist sums to 1
        temp = _calibrate_temp(scores, mkt_p_n)
    else:
        temp = 1.0

    all_scores = [x["score"] for x in pool]
    model_p    = _softmax(all_scores, temp)

    results = []
    for x, p_mod in zip(pool, model_p):
        market_p = _amer_to_prob(x["best_odds"])
        edge = (p_mod - market_p) if market_p is not None else None
        if   edge is None:       stars = ""
        elif edge >= 0.04:       stars = "★★★"
        elif edge >= 0.02:       stars = "★★"
        elif edge >= 0.005:      stars = "★"
        else:                    stars = ""
        results.append({
            "name":         x["player"].get("name"),
            "team_abbr":    x["player"].get("team_abbr"),
            "league":       x["player"].get("league"),
            "pos":          x.get("pos") or x["player"].get("pos"),
            "p_war":        _eos(x["player"], "war"),
            "p_ops":        _eos(x["player"], "ops") if x["player"].get("ros", {}).get("blend", {}).get("ops") is not None or _eos(x["player"], "ops") else None,
            "p_fip":        _eos(x["player"], "fip"),
            "p_ip":         _eos(x["player"], "ip"),
            "model_p":      round(p_mod, 4),
            "market_p":     round(market_p, 4) if market_p is not None else None,
            "edge":         round(edge, 4)    if edge      is not None else None,
            "stars":        stars,
            "best_odds":    x["best_odds"],
            "best_book":    x["best_book"],
            "all_book_odds":x["all_book_odds"],
        })
    return {
        "label":        market_meta.get("label", market_key),
        "n_pool":       len(pool),
        "temperature":  round(temp, 3),
        "calibrated_against_n_books": len(cal_pool),
        "candidates":   results,
    }


# ── Main ───────────────────────────────────────────────────────────────────
def main():
    _build_rookie_index()

    if not ODDS_PATH.exists() or not WAR_PATH.exists():
        print(f"[player-futures] required input missing — {ODDS_PATH.name} or "
              f"{WAR_PATH.name}", file=sys.stderr)
        return 0

    odds = json.loads(ODDS_PATH.read_text())
    war  = json.loads(WAR_PATH.read_text())
    tf   = json.loads(TF_PATH.read_text()) if TF_PATH.exists() else {}

    hitters  = war.get("hitters",  {})
    pitchers = war.get("pitchers", {})
    markets_in = odds.get("markets", {})

    out_markets = {}
    for league in ("AL", "NL"):
        # MVP
        mvp_key = f"{league}_MVP"
        mvp_pool = _mvp_pool(hitters, league, tf)
        mvp_scored = _score_mvp(mvp_pool)
        out_markets[mvp_key] = _render_market(
            mvp_scored, mvp_key, markets_in.get(mvp_key, {"label": f"{league} MVP"}),
            top_n=30)

        # CY
        cy_key = f"{league}_CY"
        cy_pool = _cy_pool(pitchers, league)
        cy_scored = _score_cy(cy_pool)
        out_markets[cy_key] = _render_market(
            cy_scored, cy_key, markets_in.get(cy_key, {"label": f"{league} Cy Young"}),
            top_n=15)

        # ROY
        roy_key = f"{league}_ROY"
        odds_players = markets_in.get(roy_key, {}).get("players", [])
        pool_h, pool_p = _roy_pool(hitters, pitchers, league, odds_players)
        roy_scored = _score_roy(pool_h, pool_p)
        out_markets[roy_key] = _render_market(
            roy_scored, roy_key, markets_in.get(roy_key, {"label": f"{league} Rookie of the Year"}),
            top_n=12)

    payload = {
        "generated_at": datetime.datetime.utcnow().isoformat() + "Z",
        "season":       odds.get("season"),
        "model":        "Awards V5.1 (PT-adjusted ROY + ATC modifiers)",
        "markets":      out_markets,
    }

    # PRESERVE-ON-EMPTY guard
    has_any = any(m.get("candidates") for m in out_markets.values())
    if not has_any and OUTPUT.exists():
        print("[player-futures] empty result — keeping prior file", file=sys.stderr)
        return 0

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(json.dumps(payload, indent=2))
    print(f"[player-futures] wrote {len(out_markets)} markets → {OUTPUT}",
          file=sys.stderr)
    for k, m in out_markets.items():
        print(f"  {k:8} pool={m['n_pool']:>2}  temp={m['temperature']}  "
              f"calib_n={m['calibrated_against_n_books']}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
