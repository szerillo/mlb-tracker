"""
B.A.R.T.O.L.O. | Per-game descriptive stats engine.

Everything here is derived from the game's Statcast pitch-level DataFrame (the
same `gdf` the simulator already pulls) plus the MLB live feed (for LOB). No new
data sources.

Produces, per team (away = inning_topbot "Top", home = "Bot"):
  - xwOBA / xBA            (quality of contact, for display)
  - plate discipline       O-Swing, Z-Swing, whiff%, SwStr%, CSW%  (process)
  - RISP                   H/AB with a runner on 2nd/3rd (sequencing, display)
  - lucky / unlucky events high-xBA outs & low-xBA hits (luck callouts)
  - LOB                    left on base (from the live-feed boxscore)

It also exposes expected_bb_k_rate_from_discipline(), the process layer the
deserved-runs model uses to fold plate discipline into the Win Prob.

NA-safe throughout: Statcast nullable values are coerced to plain floats first
(float(pd.NA) raises) so comparisons never hit the "boolean value of NA is
ambiguous" trap.
"""
from __future__ import annotations
from typing import Optional

# ---- Statcast description / event vocabularies -----------------------------
_SWING_DESCR = {
    "swinging_strike", "swinging_strike_blocked", "foul", "foul_tip",
    "hit_into_play", "foul_bunt", "missed_bunt", "bunt_foul_tip",
}
_WHIFF_DESCR = {"swinging_strike", "swinging_strike_blocked", "missed_bunt"}
_CSW_DESCR = {"called_strike", "swinging_strike", "swinging_strike_blocked"}

_HIT_EVENTS = {"single", "double", "triple", "home_run"}
# Plate appearances that are NOT at-bats (excluded from AVG/xBA denominators).
_NON_AB_EVENTS = {
    "walk", "intent_walk", "hit_by_pitch", "sac_fly", "sac_bunt",
    "sac_fly_double_play", "sac_bunt_double_play", "catcher_interf",
    "batter_interference",
}

# League-average plate-discipline anchors (≈2024 MLB) used to center the
# process model so a team is graded relative to the league, not absolute rates.
_LG = {"csw": 0.290, "oswing": 0.310, "zswing": 0.690, "k": 0.222, "bb": 0.083}
# Sensitivities (Δrate per Δprocess), from empirical CSW→K and chase→BB slopes.
_K_PER_CSW = 1.30      # +1pp CSW ≈ +1.3pp K
_BB_PER_OSWING = -0.45  # +1pp chase ≈ -0.45pp BB
_BB_PER_ZSWING = -0.12  # over-swinging in-zone also trims walks slightly


def _f(x) -> Optional[float]:
    """NA/None/NaN-safe float coercion. Returns None if not a real number."""
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    return None if v != v else v  # drop NaN


def _is_in_zone(zone) -> Optional[bool]:
    z = _f(zone)
    if z is None:
        return None
    zi = int(round(z))
    if 1 <= zi <= 9:
        return True
    if 11 <= zi <= 14:
        return False
    return None  # unknown/other


def _team_discipline(df) -> dict:
    """Swing/contact rates over every pitch this team's batters saw."""
    pitches = swings = whiffs = csw = 0
    in_zone = z_sw = out_zone = o_sw = 0
    for desc, zone in zip(df.get("description", []), df.get("zone", [])):
        pitches += 1
        d = desc if isinstance(desc, str) else ""
        swung = d in _SWING_DESCR
        if swung:
            swings += 1
        if d in _WHIFF_DESCR:
            whiffs += 1
        if d in _CSW_DESCR:
            csw += 1
        iz = _is_in_zone(zone)
        if iz is True:
            in_zone += 1
            if swung:
                z_sw += 1
        elif iz is False:
            out_zone += 1
            if swung:
                o_sw += 1
    def rate(a, b):
        return round(a / b, 3) if b else None
    return {
        "pitches": pitches,
        "o_swing": rate(o_sw, out_zone),    # chase rate
        "z_swing": rate(z_sw, in_zone),
        "whiff": rate(whiffs, swings),       # per swing
        "swstr": rate(whiffs, pitches),      # per pitch
        "csw": rate(csw, pitches),
    }


def _team_contact_quality(df) -> dict:
    """Team xwOBA / xBA from estimated_woba/ba_using_speedangle + BB/K/HBP."""
    bip = bip_xwoba_sum = bip_xba_sum = 0
    ks = bb = hbp = 0
    for typ, ev, xw, xb in zip(
        df.get("type", []), df.get("events", []),
        df.get("estimated_woba_using_speedangle", []),
        df.get("estimated_ba_using_speedangle", []),
    ):
        e = ev if isinstance(ev, str) else None
        if typ == "X":  # batted ball in play
            bip += 1
            w = _f(xw)
            b = _f(xb)
            if w is not None:
                bip_xwoba_sum += w
            if b is not None:
                bip_xba_sum += b
        if e == "strikeout":
            ks += 1
        elif e in ("walk", "intent_walk"):
            bb += 1
        elif e == "hit_by_pitch":
            hbp += 1
    ab = bip + ks                      # AB ≈ balls in play + strikeouts
    pa = ab + bb + hbp                 # (ignoring sacs — negligible for team x-stats)
    return {
        "xba": round(bip_xba_sum / ab, 3) if ab else None,
        "xwoba": round((bip_xwoba_sum + 0.69 * bb + 0.72 * hbp) / pa, 3) if pa else None,
        "bip": bip, "k": ks, "bb": bb, "hbp": hbp, "ab": ab, "pa": pa,
    }


def _team_risp(df) -> dict:
    """H/AB with a runner on 2nd or 3rd, from on_2b/on_3b at each PA end."""
    ab = h = 0
    for ev, on2, on3 in zip(df.get("events", []), df.get("on_2b", []), df.get("on_3b", [])):
        e = ev if isinstance(ev, str) else None
        if not e:
            continue  # not a PA-ending pitch
        risp = (_f(on2) is not None) or (_f(on3) is not None)
        if not risp:
            continue
        if e in _NON_AB_EVENTS:
            continue
        ab += 1
        if e in _HIT_EVENTS:
            h += 1
    return {"ab": ab, "h": h, "avg": round(h / ab, 3) if ab else None}


def _base_state(on1, on2, on3) -> str:
    """Human base-occupancy string from on_1b/2b/3b (runner id present = occupied)."""
    b = (_f(on1) is not None, _f(on2) is not None, _f(on3) is not None)
    return {
        (False, False, False): "bases empty",
        (True, False, False): "runner on 1st",
        (False, True, False): "runner on 2nd",
        (False, False, True): "runner on 3rd",
        (True, True, False): "1st & 2nd",
        (True, False, True): "1st & 3rd",
        (False, True, True): "2nd & 3rd",
        (True, True, True): "bases loaded",
    }[b]


def _lucky_unlucky(df, team: str, hi: float = 0.50, lo: float = 0.150) -> list:
    """Batted balls whose result defied contact quality.
    unlucky_out: xBA >= hi but made an out;  lucky_hit: xBA <= lo but a hit.

    NOTE: Statcast's `player_name` is the PITCHER, not the batter, so we carry
    the `batter` MLBAM id (`_bid`) here and resolve it to the hitter's name in
    compute_game_stats() — the player who actually struck the ball is who we
    want to credit/blame for the luck. Each event also carries the game
    situation (inning/half, outs, base state) so the callout reads in context."""
    n = len(df.get("events", []))
    def _col(name):
        return df[name] if hasattr(df, "columns") and name in df.columns else [None] * n
    outs, hits = [], []
    for ev, xb, ls, la, bid, inn, tb, ow, o1, o2, o3 in zip(
        df.get("events", []), df.get("estimated_ba_using_speedangle", []),
        df.get("launch_speed", []), df.get("launch_angle", []),
        _col("batter"), _col("inning"), _col("inning_topbot"),
        _col("outs_when_up"), _col("on_1b"), _col("on_2b"), _col("on_3b"),
    ):
        e = ev if isinstance(ev, str) else None
        x = _f(xb)
        if e is None or x is None:
            continue
        is_hit = e in _HIT_EVENTS
        bidv = _f(bid)
        innv = _f(inn)
        owv = _f(ow)
        rec = {
            "team": team, "batter": "", "_bid": int(bidv) if bidv is not None else None,
            "ev": round(_f(ls), 1) if _f(ls) is not None else None,
            "la": round(_f(la), 0) if _f(la) is not None else None,
            "xba": round(x, 3), "result": e,
            "inning": int(innv) if innv is not None else None,
            "half": ("T" if tb == "Top" else "B" if tb == "Bot" else None),
            "outs": int(owv) if owv is not None else None,
            "bases": _base_state(o1, o2, o3),
        }
        if (not is_hit) and x >= hi:
            rec["kind"] = "unlucky_out"
            outs.append(rec)
        elif is_hit and x <= lo:
            rec["kind"] = "lucky_hit"
            hits.append(rec)
    outs.sort(key=lambda r: r["xba"], reverse=True)   # most-robbed first
    hits.sort(key=lambda r: r["xba"])                 # flukiest first
    return outs[:3] + hits[:3]


def _resolve_batter_names(ids) -> dict:
    """Map MLBAM batter ids -> "Last, First" (Savant style). Best-effort:
    uses pybaseball's Chadwick reverse lookup (cached after first call).
    Returns {} on any failure so luck events still render (sans name)."""
    uniq = sorted({int(i) for i in ids if i})
    if not uniq:
        return {}
    try:
        from pybaseball import playerid_reverse_lookup
        tbl = playerid_reverse_lookup(uniq, key_type="mlbam")
    except Exception:
        return {}
    out = {}
    for _, r in tbl.iterrows():
        try:
            mid = int(r["key_mlbam"])
        except (TypeError, ValueError, KeyError):
            continue
        last = str(r.get("name_last", "") or "").strip()
        first = str(r.get("name_first", "") or "").strip()
        name = f"{last.title()}, {first.title()}" if last else first.title()
        if name:
            out[mid] = name
    return out


def expected_bb_k_rate_from_discipline(disc: dict) -> tuple[Optional[float], Optional[float]]:
    """Process layer: expected (bb_rate, k_rate) implied by plate discipline,
    anchored on league average. Used by the deserved-runs model to credit a
    team's swing decisions, not just the BB/K that happened to occur.
    Returns (None, None) if discipline is too sparse to trust."""
    csw, osw, zsw = disc.get("csw"), disc.get("o_swing"), disc.get("z_swing")
    if csw is None or osw is None or zsw is None or disc.get("pitches", 0) < 40:
        return None, None
    k = _LG["k"] + _K_PER_CSW * (csw - _LG["csw"])
    bb = (_LG["bb"] + _BB_PER_OSWING * (osw - _LG["oswing"])
          + _BB_PER_ZSWING * (zsw - _LG["zswing"]))
    return max(0.0, min(0.60, bb)), max(0.0, min(0.55, k))


def extract_lob(pbp: Optional[dict]) -> dict:
    """(away_lob, home_lob) from the MLB live-feed boxscore."""
    out = {"away": None, "home": None}
    try:
        teams = pbp["liveData"]["boxscore"]["teams"]
        out["away"] = teams["away"]["teamStats"]["batting"].get("leftOnBase")
        out["home"] = teams["home"]["teamStats"]["batting"].get("leftOnBase")
    except Exception:
        pass
    return out


def compute_game_stats(statcast_df, pbp: Optional[dict] = None) -> dict:
    """Full per-game descriptive bundle for the Win Prob detail.
    Returns {} when the Statcast frame is unusable."""
    need = {"inning_topbot", "type", "events", "description", "zone"}
    if statcast_df is None or len(statcast_df) == 0 or not need.issubset(statcast_df.columns):
        return {}
    away = statcast_df[statcast_df["inning_topbot"] == "Top"]
    home = statcast_df[statcast_df["inning_topbot"] == "Bot"]
    lob = extract_lob(pbp)
    out = {}
    for side, df in (("away", away), ("home", home)):
        cq = _team_contact_quality(df)
        disc = _team_discipline(df)
        out[side] = {
            "xwoba": cq["xwoba"], "xba": cq["xba"],
            "discipline": {k: disc[k] for k in ("o_swing", "z_swing", "whiff", "swstr", "csw")},
            "risp": _team_risp(df),
            "lob": lob.get(side),
        }
    luck = _lucky_unlucky(away, "away") + _lucky_unlucky(home, "home")
    names = _resolve_batter_names([r.get("_bid") for r in luck])
    for r in luck:
        bid = r.pop("_bid", None)
        if bid is not None and not r.get("batter"):
            r["batter"] = names.get(bid, "")
    out["luck_events"] = luck
    return out
