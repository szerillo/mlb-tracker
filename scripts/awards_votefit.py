#!/usr/bin/env python3
"""
awards_votefit.py — Fable V1 vote-fit award boards (ROY / MVP / CY) + λ blend.
------------------------------------------------------------------------------
Object-driven: one code path for all three awards; each award's features,
weights, fixed SDs, temp and γ come from its *_votefit_V1.json object.

Cross-type z rule (per Fable, identical across awards):
  - WAR_z, ACC_z: centered on the WHOLE race field mean / fixed SD.
  - Hitter features (HR/RBI/OPS): centered on the race's HITTERS only; others 0.
  - Pitcher features (W/K/IP/ERA): centered on the race's PITCHERS only; others 0.
  - single-member group -> members' z = 0 (x==mean); missing group -> 0.
  - SDs ALWAYS the fixed historical SDs; means are per-race per-type.
  - Two-way (Ohtani-class) candidates belong to BOTH groups (all channels live).
  - ERA_z is SIGN-FLIPPED (lower ERA is better) so its positive weight rewards
    low ERA. (v1.2)

Win layer: p_model = softmax(share/temp + γ·ACC_z + pitcher_delta·[pure pitcher]).
  pitcher_delta (from the object, default 0) is a PURE-pitcher prior applied in
  MVP only; two-way (Ohtani-class) candidates are exempt. (v1.2)

Two numbers:
  * p_model   — pure vote-fit softmax. BET SELECTION reads this only.
  * p_display — conditional-λ log-pool of p_model with the de-vigged market
                (display board only; never bet off it).

λ policy (priors, pending next-season fit from archived daily market_p):
  λ = 0.5 ONLY when BOTH fire on the MODEL LEADER:
     (i)  |p_model_leader − p_market_leader| >= 0.35   (tripwire)
     (ii) leader idle >= 7 days (within last 21) or currently IL   (absence)
  else λ = 0.

Field rule (fixes small-field mean drift): field = top-8 by preliminary
share-score from the pool  ∪  anyone with market_p >= 0.5%, minimum 8.

Calibration note (do NOT bake a constant): p_model is monotone but mildly
underconfident 2–20% and ~7pts overconfident >70%. For EV: treat model edges
< ~0.08 as noise; haircut a model 0.90 toward ~0.84. Re-measure each season.

is_pit rule: two-way if PA>=200 AND IP>=50 (both groups); else P if IP*4.3>PA else H.
ACC (absence channel): Sept-to-date (FINAL games only, date>=YYYY-09-01) + FG-ROS
ATC row; PA for hitters, IP*4.3 for pitchers; missing-from-ROS => ROS 0 (shutdown
signal, do not impute). The 4.3 is batters-faced-per-inning.
"""
import math

FIELD_FEATS = {"WAR"}
HIT_FEATS = {"HR", "RBI", "OPS"}
PIT_FEATS = {"W", "K", "IP", "ERA"}
EDGE_NOISE = 0.08          # model edges below this are noise for bet purposes
TRIPWIRE_DIV = 0.35
ABSENCE_DAYS = 7
LAM_ABSENT = 0.5


def assign_groups(pa, ip):
    """{'H'}, {'P'}, or {'H','P'} (Ohtani-class) from season-to-date PA & IP."""
    pa = pa or 0
    ip = ip or 0
    if pa >= 200 and ip >= 50:
        return {"H", "P"}
    return {"P"} if (ip * 4.3 > pa) else {"H"}


def _mean(vals):
    return (sum(vals) / len(vals)) if vals else 0.0


def _base(feat):
    return feat[:-2] if feat.endswith("_z") else feat


def score_race(cands, obj):
    """Attach p_model + share to each candidate in a race (object-driven z rule)."""
    F = obj["share_features"]
    W = dict(zip(F, obj["share_weights"]))
    SD = obj["fixed_SDs"]
    for c in cands:
        if "groups" not in c:
            c["groups"] = {"P"} if c.get("is_pit") else {"H"}
    mwar = _mean([c.get("war", 0.0) for c in cands])
    macc = _mean([c.get("acc", 0.0) for c in cands])
    for c in cands:
        c["WAR_z"] = (c.get("war", 0.0) - mwar) / SD["war"]
        c["ACC_z"] = (c.get("acc", 0.0) - macc) / SD["acc"]
    for f in F:
        b = _base(f)
        if b == "WAR":
            continue
        grp = "H" if b in HIT_FEATS else ("P" if b in PIT_FEATS else None)
        sub = [c for c in cands if grp is None or grp in c["groups"]]
        m = _mean([c.get(b, 0.0) for c in sub]) if sub else 0.0
        for c in cands:
            inn = (grp is None) or (grp in c["groups"])
            if inn and sub:
                z = (c.get(b, 0.0) - m) / SD[b]
                c[f] = -z if b == "ERA" else z   # v1.2: ERA lower-is-better
            else:
                c[f] = 0.0
    pdelta = obj.get("pitcher_delta", 0.0)       # v1.2: pure-pitcher MVP prior
    for c in cands:
        c["share"] = sum(W[f] * c[f] for f in F)
        c["lin"] = c["share"] / obj["temp"] + obj["absence_gamma"] * c["ACC_z"]
        if pdelta and c["groups"] == {"P"}:       # pure pitcher only; two-way exempt
            c["lin"] += pdelta
    mx = max(c["lin"] for c in cands)
    Z = sum(math.exp(c["lin"] - mx) for c in cands)
    for c in cands:
        c["p_model"] = math.exp(c["lin"] - mx) / Z
    return cands


def build_field(pool, obj, market_by_key=None, min_n=8, mkt_floor=0.005):
    """top-min_n by preliminary share ∪ market_p>=mkt_floor. Each cand needs 'key'."""
    tmp = [dict(c) for c in pool]
    score_race(tmp, obj)
    ranked = sorted(tmp, key=lambda c: -c["share"])
    keys, chosen = set(), []
    for c in ranked[:min_n]:
        chosen.append(c)
        keys.add(c["key"])
    for c in ranked[min_n:]:
        mk = (market_by_key or {}).get(c["key"])
        if mk is not None and mk >= mkt_floor and c["key"] not in keys:
            chosen.append(c)
            keys.add(c["key"])
    # return original dicts for the chosen keys (fresh, unscored)
    by_key = {c["key"]: c for c in pool}
    return [dict(by_key[k]) for k in keys]


def devig(market_ps):
    s = sum(p for p in market_ps if p)
    return [(p / s if (p and s > 0) else None) for p in market_ps]


def logpool(p_models, p_markets_devig, lam):
    raw = []
    for pm, pk in zip(p_models, p_markets_devig):
        raw.append(pm if (not pk or pk <= 0 or lam <= 0) else (pm ** (1 - lam)) * (pk ** lam))
    s = sum(raw)
    return [r / s if s > 0 else 0.0 for r in raw]


def compute_lambda(cands, market_by_key, leader_days_idle, leader_il=False):
    """λ from the MODEL LEADER only: div>=0.35 AND (idle>=7d or IL)."""
    leader = max(cands, key=lambda c: c["p_model"])
    mk = (market_by_key or {}).get(leader["key"])
    tripwire = mk is not None and abs(leader["p_model"] - mk) >= TRIPWIRE_DIV
    absent = bool(leader_il) or (leader_days_idle is not None and leader_days_idle >= ABSENCE_DAYS)
    return (LAM_ABSENT if (tripwire and absent) else 0.0), leader, tripwire, absent


def build_board(cands, obj, market_by_key, leader_days_idle=None, leader_il=False,
                low_confidence=False):
    """Returns (cands with p_model+p_display, meta). market_by_key maps key->implied p."""
    score_race(cands, obj)
    lam, leader, tripwire, absent = compute_lambda(cands, market_by_key, leader_days_idle, leader_il)
    mkt = [market_by_key.get(c["key"]) if market_by_key else None for c in cands]
    disp = logpool([c["p_model"] for c in cands], devig(mkt), lam) if lam > 0 else [c["p_model"] for c in cands]
    for c, pd in zip(cands, disp):
        c["p_display"] = pd
        c["low_confidence"] = low_confidence
    meta = {"lambda": lam, "leader": leader["key"], "tripwire": tripwire,
            "absence": absent, "low_confidence": low_confidence,
            "tripwire_log": tripwire}  # always log any leader divergence>=0.35
    return sorted(cands, key=lambda c: -c["p_display"]), meta
