#!/usr/bin/env python3
"""compute_clv.py - grade Action PRO projections open->close for CLV + direction.

Joins, per date:
  data/clv/{date}.json            (pre-game snapshot: our PRO + opening Book-30 line + last line)
  data/odds_archive/{date}.json   (finalized: consensus close + actual results)

For each game with a stored PRO projection, computes for ML and total:
  - our lean side + edge%
  - open line/price, close line/price
  - directional correctness: did the close move TOWARD our projection vs open?
  - CLV: did our entry (open) beat the close?  (totals in run-line points,
    ML in implied-probability points)
  - result vs the actual final (W / L / push)

Writes data/clv_archive/{date}.json per date and a rolling data/clv_summary.json
(directional hit-rate + avg CLV + record, by market). Designed to be safe to
re-run; only dates with both a PRO snapshot and a finalized archive are graded.
"""
import datetime, json, sys, pathlib

REPO = pathlib.Path(__file__).resolve().parent.parent
CLV_DIR     = REPO / "data" / "clv"
ODDS_ARCH   = REPO / "data" / "odds_archive"
OUT_DIR     = REPO / "data" / "clv_archive"
SUMMARY     = REPO / "data" / "clv_summary.json"


def _implied(american):
    if american is None: return None
    a = float(american)
    return (-a) / ((-a) + 100) if a < 0 else 100 / (a + 100)


def _grade_total(pro, open_m, close_m, actual_total):
    proj = pro.get("over_proj")
    if proj is None: proj = pro.get("under_proj")
    ol = (open_m or {}).get("total", {}).get("line")
    cl = (close_m or {}).get("total", {}).get("line")
    if proj is None or ol is None or cl is None: return None
    if proj > ol:   lean = "Over"
    elif proj < ol: lean = "Under"
    else: return None
    edge_pct = pro.get("over_edge_pct") if lean == "Over" else pro.get("under_edge_pct")
    moved = cl - ol
    if moved == 0:        direction = "push"
    elif (lean == "Over") == (moved > 0): direction = "correct"
    else:                 direction = "wrong"
    clv = moved if lean == "Over" else -moved   # positive = entry (open) beat the close
    result = None
    if actual_total is not None:
        if actual_total == cl: result = "push"
        else:
            over_hit = actual_total > cl
            result = "win" if (over_hit == (lean == "Over")) else "loss"
    return {"market": "total", "lean": lean, "proj": round(proj, 2),
            "edge_pct": edge_pct, "open_line": ol, "close_line": cl,
            "direction": direction, "clv_pts": round(clv, 2), "result": result}


def _grade_ml(pro, open_m, close_m, away_win):
    aw_p, hm_p = pro.get("ml_away_proj"), pro.get("ml_home_proj")
    aw_e, hm_e = pro.get("ml_away_edge_pct"), pro.get("ml_home_edge_pct")
    if aw_e is None and hm_e is None: return None
    lean = "away" if (aw_e or -9) >= (hm_e or -9) else "home"
    edge_pct = aw_e if lean == "away" else hm_e
    o_open = (open_m or {}).get(f"ml_{lean}")
    o_close = (close_m or {}).get(f"ml_{lean}")
    io, ic = _implied(o_open), _implied(o_close)
    if io is None or ic is None: return None
    direction = "correct" if ic > io else ("wrong" if ic < io else "push")
    clv_pct = round((ic - io) * 100, 2)   # positive = our open price beat the close
    result = None
    if away_win is not None:
        won = (away_win and lean == "away") or ((not away_win) and lean == "home")
        result = "win" if won else "loss"
    return {"market": "ml", "lean": lean, "edge_pct": edge_pct,
            "open_odds": o_open, "close_odds": o_close,
            "direction": direction, "clv_pct": clv_pct, "result": result}


def grade_date(date):
    snap_p = CLV_DIR / f"{date}.json"
    arch_p = ODDS_ARCH / f"{date}.json"
    if not snap_p.exists(): return None
    snap = json.loads(snap_p.read_text()).get("games", {})
    arch = {}
    if arch_p.exists():
        for g in json.loads(arch_p.read_text()).get("games", []):
            arch[str(g.get("an_event_id"))] = g
    rows = []
    for eid, s in snap.items():
        pro = s.get("pro")
        if not pro: continue
        a = arch.get(eid, {})
        open_m  = s.get("open") or a.get("open")
        close_m = a.get("consensus") or s.get("last")
        aa, ah = a.get("actual_away_runs"), a.get("actual_home_runs")
        actual_total = (aa + ah) if (aa is not None and ah is not None) else None
        away_win = (aa > ah) if (aa is not None and ah is not None) else None
        base = {"an_event_id": s.get("an_event_id"), "game_pk": s.get("game_pk"),
                "away": s.get("away_team"), "home": s.get("home_team"),
                "start_time": s.get("start_time"), "final": (None if aa is None else f"{aa}-{ah}")}
        gt = _grade_total(pro, open_m, close_m, actual_total)
        gm = _grade_ml(pro, open_m, close_m, away_win)
        if gt or gm:
            rows.append({**base, "total": gt, "ml": gm})
    # sort biggest edge first
    def best_edge(r):
        e = 0
        for k in ("total", "ml"):
            v = r.get(k)
            if v and v.get("edge_pct") is not None: e = max(e, v["edge_pct"])
        return e
    rows.sort(key=best_edge, reverse=True)
    return {"date": date, "generated_at": datetime.datetime.utcnow().isoformat(timespec="seconds") + "Z",
            "n_games": len(rows), "games": rows}


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    dates = sorted(p.stem for p in CLV_DIR.glob("*.json")) if CLV_DIR.exists() else []
    agg = {"ml": {"dir_correct": 0, "dir_total": 0, "clv_sum": 0.0, "clv_n": 0, "win": 0, "loss": 0},
           "total": {"dir_correct": 0, "dir_total": 0, "clv_sum": 0.0, "clv_n": 0, "win": 0, "loss": 0}}
    graded_dates = []
    for d in dates:
        res = grade_date(d)
        if not res or not res["games"]: continue
        (OUT_DIR / f"{d}.json").write_text(json.dumps(res, indent=1))
        graded_dates.append(d)
        for r in res["games"]:
            for mk in ("ml", "total"):
                v = r.get(mk)
                if not v: continue
                if v.get("direction") in ("correct", "wrong"):
                    agg[mk]["dir_total"] += 1
                    if v["direction"] == "correct": agg[mk]["dir_correct"] += 1
                clv = v.get("clv_pct" if mk == "ml" else "clv_pts")
                if clv is not None:
                    agg[mk]["clv_sum"] += clv; agg[mk]["clv_n"] += 1
                if v.get("result") == "win": agg[mk]["win"] += 1
                elif v.get("result") == "loss": agg[mk]["loss"] += 1
    def summarize(a):
        return {"directional_pct": round(100 * a["dir_correct"] / a["dir_total"], 1) if a["dir_total"] else None,
                "dir_correct": a["dir_correct"], "dir_total": a["dir_total"],
                "avg_clv": round(a["clv_sum"] / a["clv_n"], 3) if a["clv_n"] else None,
                "record": f"{a['win']}-{a['loss']}", "win": a["win"], "loss": a["loss"]}
    summary = {"generated_at": datetime.datetime.utcnow().isoformat(timespec="seconds") + "Z",
               "dates_graded": graded_dates,
               "ml": summarize(agg["ml"]), "total": summarize(agg["total"])}
    SUMMARY.write_text(json.dumps(summary, indent=1))
    print(f"[compute-clv] graded {len(graded_dates)} dates: {graded_dates}", file=sys.stderr)
    print(f"  ML dir {summary['ml']['directional_pct']}% avgCLV {summary['ml']['avg_clv']} | "
          f"TOT dir {summary['total']['directional_pct']}% avgCLV {summary['total']['avg_clv']}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
