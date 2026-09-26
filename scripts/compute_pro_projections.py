#!/usr/bin/env python3
"""
BARTOLO internal projection engine — a faithful Python port of the Google Sheet's
`Model` chain (§2–§6 of BRIEF_to_BARTOLO_projection_chain_2026-09-25, live sheet
as of 2026-09-25). The goal is to reproduce the sheet's per-game `pro` outputs
(R2/S2 F5 runs, R9/S9 full runs, T9 total, S10 win prob) WITHOUT the spreadsheet,
so the site can eventually generate its own projections and retire the GAME
UPLOADER dependency.

Two layers:
  1. project_game(inp)  — the exact formula tree. VALIDATED: reproduces the sheet's
     matchup-1 outputs to ~1e-4 (see _selftest below). This is frozen: it is the
     sheet's arithmetic, cell for cell.
  2. build_inputs(...)  — Phase B: assemble the ~two-dozen intermediates from the
     repo's own feeds (hitters.json, pitcher anchors, Bullpen RR, park/weather/ump,
     tilt). This is the automation layer; it is reconciled slate-by-slate against
     the sheet until every game matches, then wired into the daily workflow to emit
     data/pro_projections.json.

Constants live in the sheet's Adjustments!K2:K7 and are stamped into
data/sheet_projections.json (see refresh_sheet_projections.py); the engine reads
them from there so it re-centres automatically with the sheet.
"""
from __future__ import annotations
import json, os, math

# ── constants (fallbacks; live values come from the Adjustments stamp) ──────
DEFAULT_CONST = {
    "offense_elasticity":   0.75,   # K2  γ
    "lg_lineup_xr_full":    4.76,   # K3
    "lg_lineup_xr_f5":      4.80,   # K4
    "win_exp_base":         1.30,   # K5  e_base
    "win_exp_total_slope":  0.06,   # K6  e_slope
    "f5_exp":               1.00,   # K7
}

# Per-inning / per-side structural constants — these are the sheet's literals,
# frozen with the chain (BRIEF §5, level-restore constants removed per §9).
SP_F5_SHARE   = 0.555     # SP share of a 9-inning start's F5 (G10*0.555 = F5 SP RA)
PEN_L4_WINDOW = 0.455     # four-inning bullpen window weight (I36*0.455)
PEN_L4_MULT   = 1.12      # bullpen L4 offense-side multiplier
SP_F5_MULT    = 1.01      # F5 SP·offense multiplier
NINTH_HOME    = 0.936     # S9 = X9 * 0.936 (home doesn't bat the 9th when leading)
HFA_F5_AWAY   = 0.956     # F5 away HFA
HFA_F5_HOME   = 1.044     # F5 home HFA
HFA_L4_AWAY   = 0.96      # full-game (L4) away HFA
HFA_L4_HOME   = 1.04      # full-game (L4) home HFA
L4_CARRY      = 0.95      # 0.95 * L4 bracket added onto the F5 to reach the full game
DEF_F5_SHARE  = 0.555     # DEF weight in the F5 (0.555*(-FLD_opp + BSR_own))
DEF_L4_SHARE  = 0.445     # DEF weight in the L4
TILT_RUNS     = 3.3       # tilt -> runs, F5 side


def _clip(x, lo, hi): return max(lo, min(hi, x))


def _ip_factor(gs, ip):
    """SP innings-per-start factor, shrunk to 5.2 by sample (BRIEF §4)."""
    raw = (ip / gs) if gs and gs > 0 else 5.2
    w = 0.0 if (gs is None or gs < 5) else min(1.0, gs / 10.0)
    shrunk = raw * w + 5.2 * (1 - w)
    return _clip(1 - (shrunk - 5.2) / 4.0, 0.85, 1.15)


def project_game(g, const=None):
    """The sheet's Model chain, cell for cell. `g` carries the intermediates the
    sheet computes in each matchup block; see KEYS below. Returns the same numbers
    the GAME UPLOADER publishes.

    KEYS (all per-game):
      away_sp_ra, home_sp_ra          F10 / G10  (full-9 SP RA, wFIP unified_adj scale)
      away_stamina, home_stamina      F15 / G15  (SP F5 share of the start)
      away_off_f5, home_off_f5        G12=W36 / F12=W58  (F5 offense ratio, park-adj)
      away_off_fg, home_off_fg        AB36 / AB58        (full-game offense ratio)
      away_pen_ra, home_pen_ra        I35 / I56  (power-1.5 K-BB-weighted pen RA)
      away_fld, home_fld              S36 / S58  (lineup FLD sum, PA-weighted)
      away_bsr, home_bsr              AA36 / AA58 (lineup BSR sum, PA-weighted)
      away_sp_gs, away_sp_ip          for the away SP ip_factor (used in X9)
      home_sp_gs, home_sp_ip          for the home SP ip_factor (used in R9)
      away_fatigued, home_fatigued    count of FATIGUED pen arms (0.015 each)
      env                             J2  (park × weather × ump × K-BB environment)
      tilt_away, tilt_home            Tilt By Game (v3, deviation-only)
    """
    c = const or DEFAULT_CONST
    gamma = c["offense_elasticity"]
    e_base, e_slope, f5_exp = c["win_exp_base"], c["win_exp_total_slope"], c["f5_exp"]
    J2 = g["env"]

    # F5 SP shares
    F11 = g["away_sp_ra"] * SP_F5_SHARE
    G11 = g["home_sp_ra"] * SP_F5_SHARE
    # SP L4 shares from stamina
    F16 = (g["away_stamina"] - SP_F5_SHARE) / (1 - SP_F5_SHARE)
    G16 = (g["home_stamina"] - SP_F5_SHARE) / (1 - SP_F5_SHARE)
    F17, G17 = 1 - F16, 1 - G16

    # F5 offense-adjusted SP (First-5 Adj): SP_F5 · 1.01 · OFF_F5^γ
    G13 = G11 * SP_F5_MULT * (g["away_off_f5"] ** gamma)   # away offense vs home SP
    F13 = F11 * SP_F5_MULT * (g["home_off_f5"] ** gamma)   # home offense vs away SP

    # F5 runs = (SP·off + DEF_F5) · env · HFA + tilt
    R2 = (G13 + DEF_F5_SHARE * (-g["home_fld"] + g["away_bsr"])) * J2 * HFA_F5_AWAY + TILT_RUNS * g["tilt_away"]
    S2 = (F13 + DEF_F5_SHARE * (-g["away_fld"] + g["home_bsr"])) * J2 * HFA_F5_HOME + TILT_RUNS * g["tilt_home"]

    # Bullpen L4 combined RA per side: pen_RA·pen_share + SP_RA·SP_L4_share
    I36 = g["away_pen_ra"] * F17 + g["away_sp_ra"] * F16   # away pitching, innings 6-9
    I57 = g["home_pen_ra"] * G17 + g["home_sp_ra"] * G16   # home pitching, innings 6-9
    I37 = I36 * PEN_L4_WINDOW
    I58 = I57 * PEN_L4_WINDOW
    fat_away = 1 + g.get("away_fatigued", 0) * 0.015
    fat_home = 1 + g.get("home_fatigued", 0) * 0.015
    # L4 offense-adjusted pen: pen_L4 · 1.12 · OFF_FG^γ · fatigue
    I38 = (I37 * PEN_L4_MULT * (g["home_off_fg"] ** gamma)) * fat_away   # away pen vs HOME offense
    I59 = (I58 * PEN_L4_MULT * (g["away_off_fg"] ** gamma)) * fat_home   # home pen vs AWAY offense

    ipf_away = _ip_factor(g.get("away_sp_gs"), g.get("away_sp_ip"))
    ipf_home = _ip_factor(g.get("home_sp_gs"), g.get("home_sp_ip"))

    # L4 runs (away bats vs home pen; DEF is opp FLD − own BSR at 0.445)
    L4_away = (I59 * ipf_home + (-g["home_fld"] + g["away_bsr"]) * DEF_L4_SHARE) * J2 * HFA_L4_AWAY
    L4_home = (I38 * ipf_away + (-g["away_fld"] + g["home_bsr"]) * DEF_L4_SHARE) * J2 * HFA_L4_HOME

    R9 = R2 + L4_CARRY * L4_away          # full away
    X9 = S2 + L4_CARRY * L4_home          # full home (pre-9th-trim)
    S9 = X9 * NINTH_HOME                  # home posted (9th-inning trim)
    T9 = R9 + S9                          # posted total

    # Win prob (full game): total-dependent exponent
    e = e_base + e_slope * ((X9 + R9) - 8.6)
    S10 = (X9 ** e) / ((X9 ** e) + (R9 ** e))    # home WP
    # F5 win prob
    S3 = (S2 ** f5_exp) / ((S2 ** f5_exp) + (R2 ** f5_exp))

    return {
        "f5_away_runs": R2, "f5_home_runs": S2, "f5_total": (R2 + S2),
        "f5_home_wp": S3, "f5_away_wp": 1 - S3,
        "away_runs": R9, "home_runs": S9, "home_runs_full9": X9, "total": T9,
        "home_wp": S10, "away_wp": 1 - S10,
    }



# ── Phase B: offense + DEF from the Batter Projected bridge feed ────────────
import math as _math, re as _re, unicodedata as _ud

PA_F5 = [2.91, 2.78, 2.65, 2.51, 2.38, 2.27, 2.18, 2.10, 2.02]   # sum 21.8
PA_FG = [4.81, 4.69, 4.57, 4.46, 4.35, 4.25, 4.14, 4.03, 3.92]
BSR_PA_MULT = [1.104, 1.076, 1.049, 1.023, 0.998, 0.975, 0.95, 0.925, 0.9]  # mean-1


def _bp_norm(s):
    s = (s or "").lower()
    s = "".join(c for c in _ud.normalize("NFKD", s) if not _ud.combining(c))  # strip accents (Diaz->diaz, Pena->pena)
    s = s.replace(".", "").replace("'", "")
    s = _re.sub(r"\s+(jr|sr|ii|iii|iv)$", "", s)
    return _re.sub(r"\s+", " ", s).strip()


def build_offense_side(lineup, opp_hand, park_off, bp_players, const=None):
    """Offense ratio (F5 + full) and DEF sums for one batting side, reproducing
    the sheet's W36/AB36 (offense) and S36/AA36 (FLD/BSR).

    lineup: list of (name, position) in batting order (up to 9).
    opp_hand: "RHP" or "LHP" (the pitcher this side faces).
    park_off: the batting team's own park run factor (Team Modifiers PF-Adj).
    bp_players: batter_projected.json "players" dict (norm-key -> row).
    """
    c = const or DEFAULT_CONST
    lg_f5, lg_fg = c["lg_lineup_xr_f5"], c["lg_lineup_xr_full"]
    off5_num = offg_num = fld_sum = bsr_sum = 0.0
    for i, (name, pos) in enumerate(lineup[:9]):
        p = bp_players.get(_bp_norm(name))
        if not p:
            continue
        plat = p["plat_vsR"] if opp_hand == "RHP" else p["plat_vsL"]
        if plat is None:
            plat = 1.0
        run = p["xr"] * _math.sqrt(max(0.0, plat))
        off5_num += run * PA_F5[i]
        offg_num += run * PA_FG[i]
        # FLD is zeroed for the DH (the sheet's IF(pos="DH",0,...)); BSR always counts.
        if str(pos).upper() != "DH":
            fld_sum += (p.get("fld") or 0.0)
        bsr_sum += (p.get("bsr") or 0.0) * BSR_PA_MULT[i]
    off_f5 = off5_num / sum(PA_F5) / (lg_f5 * park_off)
    off_fg = offg_num / sum(PA_FG) / (lg_fg * park_off)
    return {"off_f5": off_f5, "off_fg": off_fg, "fld": fld_sum, "bsr": bsr_sum}



# ── Phase B: env (J2) + bullpen aggregate ──────────────────────────────────
def build_env(home_park_factor, weather_adj, ump_favor):
    """J2 = park (Adjustments 'New Factor' for the home park) + weather term +
    ump term. Validated: Fenway 1.04 + (-0.12 * 0.7/0.62) + 0 = 0.904516."""
    w = weather_adj or 0.0
    wterm = w * (0.3 / 0.36) if w >= 0 else w * (0.7 / 0.62)
    if ump_favor is None:
        uterm = 0.0
    else:
        uterm = max(-0.012, min(0.012, ump_favor))   # sheet L6 = MEDIAN(-0.012, favor, 0.012)
    return (home_park_factor or 1.0) + wterm + uterm


def build_bullpen(arms):
    """Team bullpen L4 RA = power-1.5 K-BB-weighted mean of the available arms
    (sheet I35/I56). arms: list of {ra, kbb, fatigued}. Fatigue bumps RA +0.4 and
    trims K-BB -0.04 (the sheet's IF(K=TRUE,...)). Excludes tonight's SP upstream."""
    num = den = 0.0
    n_fat = 0
    for a in arms:
        ra, kbb = a.get("ra"), a.get("kbb")
        if ra is None or kbb is None:
            continue
        if a.get("fatigued"):
            ra += 0.4; kbb = max(0.0, kbb - 0.04); n_fat += 1
        w = kbb ** 1.5
        num += ra * w; den += w
    return {"pen_ra": (num / den if den else None), "n_fatigued": n_fat}



# ── Phase B: full assembler ────────────────────────────────────────────────
def project_matchup(mu, const=None):
    """Assemble a full game projection from structured inputs and the bridge/repo
    feeds, then call project_game(). `mu` carries:
      away_lineup / home_lineup : [(name, pos), ...] batting order
      away_sp / home_sp         : {ra, stamina, gs, ip}
      away_arms / home_arms     : [{ra, kbb, fatigued}, ...]
      away_park_off / home_park_off : each team's own PF-Adj (offense divisor)
      home_park_factor          : Adjustments 'New Factor' for the game park
      weather_adj, ump_favor    : J4, ump
      tilt_away, tilt_home      : Tilt By Game (v3)
      away_sp_hand / home_sp_hand : 'RHP'/'LHP'
      bp_players                : batter_projected 'players' dict
    """
    c = const or DEFAULT_CONST
    aoff = build_offense_side(mu["away_lineup"], mu["home_sp_hand"], mu["away_park_off"], mu["bp_players"], c)
    hoff = build_offense_side(mu["home_lineup"], mu["away_sp_hand"], mu["home_park_off"], mu["bp_players"], c)
    apen = build_bullpen(mu["away_arms"])
    hpen = build_bullpen(mu["home_arms"])
    env = build_env(mu["home_park_factor"], mu.get("weather_adj"), mu.get("ump_favor"))
    g = {
        "away_sp_ra": mu["away_sp"]["ra"], "home_sp_ra": mu["home_sp"]["ra"],
        "away_stamina": mu["away_sp"]["stamina"], "home_stamina": mu["home_sp"]["stamina"],
        "away_off_f5": aoff["off_f5"], "home_off_f5": hoff["off_f5"],
        "away_off_fg": aoff["off_fg"], "home_off_fg": hoff["off_fg"],
        "away_pen_ra": apen["pen_ra"], "home_pen_ra": hpen["pen_ra"],
        "away_fld": aoff["fld"], "home_fld": hoff["fld"],
        "away_bsr": aoff["bsr"], "home_bsr": hoff["bsr"],
        "away_sp_gs": mu["away_sp"].get("gs"), "away_sp_ip": mu["away_sp"].get("ip"),
        "home_sp_gs": mu["home_sp"].get("gs"), "home_sp_ip": mu["home_sp"].get("ip"),
        "away_fatigued": apen["n_fatigued"], "home_fatigued": hpen["n_fatigued"],
        "env": env, "tilt_away": mu.get("tilt_away", 0.0), "tilt_home": mu.get("tilt_home", 0.0),
    }
    out = project_game(g, c)
    out["_intermediates"] = g
    return out


# ── self-test: reproduce the sheet's matchup-1 (gid 302442, CHC@BOS 2026-09-25) ──
_SELFTEST_INPUT = {
    "away_sp_ra": 3.91, "home_sp_ra": 3.85,
    "away_stamina": 0.63333333, "home_stamina": 0.555,
    "away_off_f5": 1.082083437, "home_off_f5": 0.9831625669,   # G12 / F12
    "away_off_fg": 1.084945313, "home_off_fg": 0.9824661091,   # AB36 / AB58
    "away_pen_ra": 3.963884433, "home_pen_ra": 3.54875,        # I35 / I56
    "away_fld": 0.4246, "home_fld": 0.2526,                    # S36 / S58
    "away_bsr": 0.0194044, "home_bsr": 0.0452126,              # AA36 / AA58
    "away_sp_gs": 27.5, "away_sp_ip": 151.4,                   # Clay Holmes
    "home_sp_gs": 0.0,  "home_sp_ip": 17.9,                    # Jake Bennett
    "away_fatigued": 0, "home_fatigued": 0,
    "env": 0.904516129, "tilt_away": 0.0, "tilt_home": 0.02014,
}
_SELFTEST_EXPECT = {   # sheet cached values
    "f5_away_runs": 1.867992493, "f5_home_runs": 1.911137824,
    "away_runs": 3.368274627, "home_runs": 3.183984929,
    "home_runs_full9": 3.401693301, "total": 6.552259556, "home_wp": 0.5029375846,
}


def _selftest(tol=2e-3):
    out = project_game(_SELFTEST_INPUT)
    ok = True
    print("  key                 engine        sheet         diff")
    for k, exp in _SELFTEST_EXPECT.items():
        got = out[k]
        d = abs(got - exp)
        flag = "" if d <= tol else "  <-- MISMATCH"
        if d > tol: ok = False
        print(f"  {k:18} {got:12.6f}  {exp:12.6f}  {d:9.2e}{flag}")
    print(f"  => {'PASS' if ok else 'FAIL'} (tol {tol})")
    return ok


if __name__ == "__main__":
    import sys
    print("[compute_pro_projections] self-test vs sheet matchup-1 (gid 302442):")
    sys.exit(0 if _selftest() else 1)
