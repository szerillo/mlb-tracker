#!/usr/bin/env python3
"""grade_model.py - grade OUR (Bartolo/sheet) numbers vs the market, every game, every day.

For each slate date, join:
  data/archive/{date}/sheet_projections.json   our number: total, away_wp/home_wp
  data/archive/{date}/full_odds_open.json       OPEN full-game lines (write-once)
  data/archive/{date}/midnight_lines.json       MIDNIGHT-ET line + our number (capture_midnight_snapshot.py; going forward)
  data/odds_archive/{date}.json                 CLOSE (consensus) + actual runs

For every game we produce total + ML grades at each available snapshot (open,
midnight, close): our lean, our edge vs the book price on that side, whether it
cleared the 3% bettable bar, CLV open/midnight->close, and the result vs the
CLOSING line. Games are graded even when un-bettable so we can calibrate the raw
model; the bettable subset (|edge| >= 3% at midnight OR at close) is tracked
separately for ROI.

Edge math mirrors index.html exactly:
  * totals: pgPOver(line, our_total) empirical curve (full-game) -> P(over);
    edge = model_p - book_implied_p on the leaned side (raw price, juice included).
  * ML: our win% (sheet away_wp/home_wp) - book_implied_p on the leaned side.

Outputs:
  data/model_grades/{date}.json      per-game panel
  data/model_grades_summary.json     rolling summary (all / last7 / last14) x
                                     market x cohort (all, bettable@mid, bettable@close)

Idempotent: re-run grades every date that has our number + a close. Pure local read.
"""
from __future__ import annotations
import datetime, json, math, sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
DATA = REPO / "data"
ARCHIVE = DATA / "archive"
ODDS_ARCH = DATA / "odds_archive"
OUT_DIR = DATA / "model_grades"
SUMMARY = DATA / "model_grades_summary.json"

BET_THRESHOLD = 0.03   # |edge| >= 3% = bettable

# ── empirical total -> P(over) curves (ported verbatim from index.html) ────
_PG_TOT_LOW  = [0.859,0.815,0.784,0.709,0.667,0.617,0.565,0.500,0.447,0.403,0.349,0.307,0.268,0.245,0.205]
_PG_TOT_MID  = [0.833,0.785,0.741,0.702,0.644,0.591,0.542,0.500,0.451,0.401,0.364,0.325,0.290,0.255,0.231]
_PG_TOT_HIGH = [0.816,0.777,0.739,0.681,0.644,0.596,0.549,0.500,0.460,0.425,0.375,0.337,0.303,0.275,0.235]
_F5L = [0.829,0.778,0.744,0.667,0.591,0.500,0.404,0.321,0.266,0.218,0.163]
_F5M = [0.847,0.753,0.720,0.626,0.576,0.500,0.430,0.373,0.307,0.275,0.218]
_F5H = [0.830,0.737,0.690,0.614,0.554,0.500,0.445,0.403,0.338,0.295,0.232]


def pg_p_over(line, proj_total):
    """Full-game P(total > line). Mirrors index.html pgPOver()."""
    if line is None or proj_total is None or proj_total <= 0:
        return None
    col = _PG_TOT_LOW if line <= 7.5 else (_PG_TOT_MID if line < 9.5 else _PG_TOT_HIGH)
    kk = max(-3.5, min(3.5, line - proj_total))
    pos = (kk + 3.5) / 0.5
    i = min(14, int(math.floor(pos)))
    j = min(14, i + 1)
    return col[i] + (pos - i) * (col[j] - col[i])


def pg_p_over_f5(line, proj_total):
    """F5 P(total > line). Mirrors index.html pgPOverF5()."""
    if line is None or proj_total is None or proj_total <= 0:
        return None
    col = _F5L if line <= 4 else (_F5M if line < 5 else _F5H)
    kk = max(-2.5, min(2.5, line - proj_total))
    pos = (kk + 2.5) / 0.5
    i = min(10, int(math.floor(pos)))
    j = min(10, i + 1)
    return col[i] + (pos - i) * (col[j] - col[i])


# ── odds helpers ───────────────────────────────────────────────────────────
def implied(odds):
    """American odds -> implied prob (with vig). None-safe."""
    if odds is None:
        return None
    try:
        o = float(odds)
    except (TypeError, ValueError):
        return None
    if o == 0:
        return None
    return (-o) / ((-o) + 100) if o < 0 else 100 / (o + 100)


def payout(odds):
    """Profit per 1u stake at american `odds`."""
    if odds is None:
        return None
    o = float(odds)
    return o / 100.0 if o > 0 else 100.0 / abs(o)


def _load(path, default=None):
    p = Path(path)
    if not p.exists():
        return default
    try:
        return json.loads(p.read_text())
    except Exception:
        return default


# ── snapshot line extractors (normalise the 3 odds shapes to one) ──────────
def _lines_from_open(g):
    """full_odds_open.json game -> normalised {ml_away,ml_home,tot_line,over,under}."""
    ml = g.get("moneyline") or {}
    tot = g.get("total") or {}
    over = tot.get("over") or {}
    under = tot.get("under") or {}
    line = over.get("line")
    if line is None:
        line = under.get("line")
    return {
        "ml_away": (ml.get("away") or {}).get("odds"),
        "ml_home": (ml.get("home") or {}).get("odds"),
        "tot_line": line,
        "over": over.get("odds"),
        "under": under.get("odds"),
    }


def _lines_from_consensus(c):
    """odds_archive consensus/open block -> normalised."""
    if not c:
        return None
    tot = c.get("total") or {}
    return {
        "ml_away": c.get("ml_away"),
        "ml_home": c.get("ml_home"),
        "tot_line": tot.get("line"),
        "over": tot.get("over"),
        "under": tot.get("under"),
    }


def _lines_from_midnight(m):
    """midnight_lines.json market block -> normalised."""
    if not m:
        return None
    return {
        "ml_away": m.get("ml_away"),
        "ml_home": m.get("ml_home"),
        "tot_line": m.get("total_line"),
        "over": m.get("over"),
        "under": m.get("under"),
    }


# ── per-snapshot edge ──────────────────────────────────────────────────────
# `edge` is model_p − DE-VIGGED fair prob on the leaned side (the skill edge,
# comparable to CLV / fair-value tables). `ev_edge` is model_p − the raw
# vig-inclusive price you'd actually pay (the +EV read). bettable = fair edge
# >= 3%. Both are emitted so the two readings are never conflated.
def _devig2(pa, pb):
    if pa is None or pb is None:
        return (None, None)
    s = pa + pb
    if s <= 0:
        return (None, None)
    return (pa / s, pb / s)


def _pick_and_pack(cands):
    """cands: list of (label, model_p_side, raw_implied, fair, price, extra_dict).
    Pick the side with the biggest FAIR edge; return the packed dict."""
    def fair_edge(c):
        return (c[1] - c[3]) if c[3] is not None else (c[1] - c[2])
    lean, mp, raw_imp, fair, price, extra = max(cands, key=fair_edge)
    edge = (mp - fair) if fair is not None else (mp - raw_imp)
    out = {"lean": lean, "edge": round(edge, 4), "ev_edge": round(mp - raw_imp, 4),
           "fair_p": (round(fair, 4) if fair is not None else None),
           "mkt_p": round(raw_imp, 4), "price": price,
           "bettable": edge >= BET_THRESHOLD}
    out.update(extra)
    return out


def _total_edge(lines, our_total):
    line = lines.get("tot_line")
    p_over = pg_p_over(line, our_total)
    if p_over is None:
        return None
    io, iu = implied(lines.get("over")), implied(lines.get("under"))
    fair_o, fair_u = _devig2(io, iu)
    cands = []
    if io is not None:
        cands.append(("Over", p_over, io, fair_o, lines.get("over"), {}))
    if iu is not None:
        cands.append(("Under", 1 - p_over, iu, fair_u, lines.get("under"), {}))
    if not cands:
        return None
    out = _pick_and_pack(cands)
    out["line"] = line
    out["model_p"] = round(p_over, 4)
    return out


def _ml_edge(lines, away_wp, home_wp):
    if away_wp is None or home_wp is None:
        return None
    ia, ih = implied(lines.get("ml_away")), implied(lines.get("ml_home"))
    fair_a, fair_h = _devig2(ia, ih)
    cands = []
    if ia is not None:
        cands.append(("away", away_wp, ia, fair_a, lines.get("ml_away"), {}))
    if ih is not None:
        cands.append(("home", home_wp, ih, fair_h, lines.get("ml_home"), {}))
    if not cands:
        return None
    out = _pick_and_pack(cands)
    out["our_p"] = out.pop("model_p", None) or round(away_wp if out["lean"] == "away" else home_wp, 4)
    ref = out["fair_p"] if out["fair_p"] is not None else out["mkt_p"]
    out["side"] = "fav" if (ref is not None and ref >= 0.5) else "dog"
    return out


def _clv_total(open_line, close_line, lean):
    if open_line is None or close_line is None or lean is None:
        return None
    moved = close_line - open_line
    toward = (moved > 0) if lean == "Over" else (moved < 0)
    return {"open": open_line, "close": close_line, "clv_pts": round(moved if lean == "Over" else -moved, 2),
            "toward_us": (None if moved == 0 else toward)}


def _clv_ml(open_price, close_price, lean):
    io, ic = implied(open_price), implied(close_price)
    if io is None or ic is None:
        return None
    return {"open": open_price, "close": close_price, "clv_pct": round((ic - io) * 100, 2),
            "toward_us": (None if ic == io else ic > io)}


# ── grade one date ─────────────────────────────────────────────────────────
def grade_date(date):
    sp = _load(ARCHIVE / date / "sheet_projections.json")
    if not sp or not sp.get("games"):
        return None
    open_odds = _load(ARCHIVE / date / "full_odds_open.json") or {}
    mid = _load(ARCHIVE / date / "midnight_lines.json") or {}
    arch = _load(ODDS_ARCH / f"{date}.json") or {}

    open_by_pk = {str(g.get("game_pk")): g for g in (open_odds.get("games") or [])}
    mid_games = mid.get("games") or {}
    arch_by_pk = {str(g.get("game_pk")): g for g in (arch.get("games") or [])}

    rows = []
    for pk, s in (sp.get("games") or {}).items():
        our_total = s.get("total")
        away_wp, home_wp = s.get("away_wp"), s.get("home_wp")
        a = arch_by_pk.get(str(pk), {})
        aa, ah = a.get("actual_away_runs"), a.get("actual_home_runs")
        have_result = aa is not None and ah is not None
        total_runs = (aa + ah) if have_result else None
        away_win = (aa > ah) if have_result else None

        snaps = {}
        o = open_by_pk.get(str(pk))
        if o:
            snaps["open"] = _lines_from_open(o)
        m = mid_games.get(str(pk))
        if m:
            snaps["midnight"] = _lines_from_midnight(m)
        cl = _lines_from_consensus(a.get("consensus"))
        if cl:
            snaps["close"] = cl
        if not snaps:
            continue

        tot = {}
        mlg = {}
        for k, ln in snaps.items():
            te = _total_edge(ln, our_total)
            if te:
                # grade result vs THIS snapshot's line + lean (what you'd have bet)
                if have_result and te["line"] is not None:
                    if total_runs == te["line"]:
                        te["result"] = "push"
                    else:
                        over_hit = total_runs > te["line"]
                        te["result"] = "win" if (over_hit == (te["lean"] == "Over")) else "loss"
                else:
                    te["result"] = None
                tot[k] = te
            me = _ml_edge(ln, away_wp, home_wp)
            if me:
                if have_result:
                    won = (away_win and me["lean"] == "away") or ((not away_win) and me["lean"] == "home")
                    me["result"] = "win" if won else "loss"
                else:
                    me["result"] = None
                mlg[k] = me

        close_ln = snaps.get("close") or {}
        close_tot_line = close_ln.get("tot_line")
        tot_result = (tot.get("close") or {}).get("result")   # headline = vs close
        ml_result = (mlg.get("close") or {}).get("result")

        # CLV: prefer midnight->close, else open->close
        base = "midnight" if "midnight" in snaps else "open"
        tot_clv = None
        if tot.get(base) and tot.get("close"):
            tot_clv = _clv_total(snaps[base].get("tot_line"), close_tot_line, tot["close"]["lean"])
        ml_clv = None
        if mlg.get(base) and mlg.get("close"):
            lean = mlg["close"]["lean"]
            op = snaps[base].get(f"ml_{lean}")
            cpx = close_ln.get(f"ml_{lean}")
            ml_clv = _clv_ml(op, cpx, lean)

        bettable_total = any((tot.get(k) or {}).get("bettable") for k in ("midnight", "open", "close"))
        bettable_ml = any((mlg.get(k) or {}).get("bettable") for k in ("midnight", "open", "close"))

        rows.append({
            "game_pk": pk, "an_event_id": s.get("an_event_id"),
            "away": s.get("away_team"), "home": s.get("home_team"),
            "our_total": our_total, "our_away_wp": away_wp, "our_home_wp": home_wp,
            "final": (None if not have_result else f"{aa}-{ah}"),
            "total_runs": total_runs, "away_win": away_win,
            "total": tot, "ml": mlg,
            "total_result": tot_result, "ml_result": ml_result,
            "total_clv": tot_clv, "ml_clv": ml_clv,
            "bettable_total": bettable_total, "bettable_ml": bettable_ml,
        })

    rows.sort(key=lambda r: max([(r["total"].get(k) or {}).get("edge", -9) for k in ("close", "midnight", "open")] +
                                [(r["ml"].get(k) or {}).get("edge", -9) for k in ("close", "midnight", "open")]),
              reverse=True)
    return {"date": date, "generated_at": datetime.datetime.utcnow().isoformat(timespec="seconds") + "Z",
            "n_games": len(rows), "games": rows}


# ── rolling summary ────────────────────────────────────────────────────────
def _blank():
    return {"n": 0, "win": 0, "loss": 0, "push": 0, "units": 0.0,
            "clv_n": 0, "clv_sum": 0.0, "clv_toward": 0}


def _tally(agg, result, price, clv, clv_key):
    agg["n"] += 1
    if result == "win":
        agg["win"] += 1
        p = payout(price)
        agg["units"] += p if p is not None else 0.91
    elif result == "loss":
        agg["loss"] += 1
        agg["units"] -= 1
    elif result == "push":
        agg["push"] += 1
    if clv is not None and clv.get(clv_key) is not None:
        agg["clv_n"] += 1
        agg["clv_sum"] += clv[clv_key]
        if clv.get("toward_us"):
            agg["clv_toward"] += 1


def _fin(a):
    dec = a["win"] + a["loss"]
    return {"n": a["n"], "record": f"{a['win']}-{a['loss']}" + (f"-{a['push']}" if a["push"] else ""),
            "hit_pct": round(100 * a["win"] / dec, 1) if dec else None,
            "roi_pct": round(100 * a["units"] / dec, 1) if dec else None,
            "units": round(a["units"], 2),
            "avg_clv": round(a["clv_sum"] / a["clv_n"], 3) if a["clv_n"] else None,
            "clv_toward_pct": round(100 * a["clv_toward"] / a["clv_n"], 1) if a["clv_n"] else None}


def build_summary(all_dates_rows, today):
    windows = {"all": None, "last7": 7, "last14": 14}
    cohorts = ["all", "bettable_mid", "bettable_close"]
    out = {}
    for wname, wdays in windows.items():
        cutoff = None if wdays is None else (datetime.date.fromisoformat(today) - datetime.timedelta(days=wdays)).isoformat()
        agg = {c: {"total": _blank(), "ml": _blank(), "ml_fav": _blank(), "ml_dog": _blank()}
               for c in cohorts}
        for date, res in all_dates_rows:
            if cutoff and date < cutoff:
                continue
            for r in res["games"]:
                for m, clv_key in (("total", "clv_pts"), ("ml", "clv_pct")):
                    snaps = r.get(m) or {}
                    clv = r.get(m + "_clv")

                    def tal(cohort, snap):
                        _tally(agg[cohort][m], snap["result"], snap.get("price"), clv, clv_key)
                        if m == "ml" and snap.get("side") in ("fav", "dog"):
                            _tally(agg[cohort]["ml_" + snap["side"]], snap["result"], snap.get("price"), clv, clv_key)

                    sc = snaps.get("close")            # all games graded at CLOSE
                    if sc and sc.get("result") in ("win", "loss", "push"):
                        tal("all", sc)
                        if sc.get("bettable"):
                            tal("bettable_close", sc)
                    sm = snaps.get("midnight") or snaps.get("open")   # bettable at midnight/open
                    if sm and sm.get("bettable") and sm.get("result") in ("win", "loss", "push"):
                        tal("bettable_mid", sm)

        def _pack(c):
            d = {"total": _fin(agg[c]["total"]), "ml": _fin(agg[c]["ml"])}
            d["ml"]["by_side"] = {"fav": _fin(agg[c]["ml_fav"]), "dog": _fin(agg[c]["ml_dog"])}
            return d
        out[wname] = {c: _pack(c) for c in cohorts}
    return out


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    dates = sorted(p.stem for p in ODDS_ARCH.glob("*.json")) if ODDS_ARCH.exists() else []
    # also include archive dates that have our number even if odds_archive missing
    if ARCHIVE.exists():
        dates = sorted(set(dates) | {p.name for p in ARCHIVE.iterdir()
                                     if p.is_dir() and (p / "sheet_projections.json").exists()})
    graded = []
    all_rows = []
    for d in dates:
        res = grade_date(d)
        if not res or not res["games"]:
            continue
        (OUT_DIR / f"{d}.json").write_text(json.dumps(res, separators=(",", ":")))
        graded.append(d)
        all_rows.append((d, res))
    today = datetime.date.today().isoformat()
    summary = {"generated_at": datetime.datetime.utcnow().isoformat(timespec="seconds") + "Z",
               "dates_graded": graded, "n_dates": len(graded),
               "bet_threshold": BET_THRESHOLD, "windows": build_summary(all_rows, today)}
    SUMMARY.write_text(json.dumps(summary, indent=1))
    a = summary["windows"]["all"]
    print(f"[grade-model] {len(graded)} dates graded → {OUT_DIR}", file=sys.stderr)
    print(f"  ALL totals {a['all']['total']['record']} hit {a['all']['total']['hit_pct']}% "
          f"avgCLV {a['all']['total']['avg_clv']} | bettable@close totals {a['bettable_close']['total']['record']} "
          f"ROI {a['bettable_close']['total']['roi_pct']}%", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
