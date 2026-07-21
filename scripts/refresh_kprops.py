#!/usr/bin/env python3
"""
Archive pitcher STRIKEOUT props -> data/kprops.json (latest snapshot),
data/archive/{date}/kprops.json (daily, for the September wFIP re-run), and an
append-only data/kprops_archive.csv (line/odds movement w/ logged_at).

WHY: K-props are the pitcher-pure market — Ks are almost entirely pitcher-
determined and priced less efficiently than game totals, so per Fable's entry-
edge findings this is the market most likely to carry edge for wFIP's K-BB /
CSW / whiff signals. No archive existed; this creates it.

SOURCE: PropFinder's public props feed (same host as the weather archiver):
    https://api.propfinder.app/mlb/props?date=YYYY-MM-DD   (no auth)
Each pitching_strikeouts main-line row carries the line, PF rating/matchup, hit
rates, and a `markets` array of ~30 sportsbooks (price + points) + `bestMarket`.
Over and under are separate rows per pitcher; we fold them into one record with
consensus (median) and best odds per side.
"""
from __future__ import annotations
import csv, datetime as dt, json, os, statistics, sys, urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(HERE, "..", "data")
OUT_JSON = os.path.join(DATA, "kprops.json")
ARCHIVE_CSV = os.path.join(DATA, "kprops_archive.csv")
API = "https://api.propfinder.app/mlb/props?date={date}"
UA = {"User-Agent": "Mozilla/5.0 (mlb-tracker/kprops-archive)"}

CSV_COLS = ["date", "game_pk", "player_id", "name", "team", "opp", "is_home",
            "line", "over_median", "over_best", "over_books",
            "under_median", "under_best", "under_books",
            "pf_rating", "matchup_value", "matchup_label",
            "hit_l5", "hit_l10", "hit_season", "avg_l10",
            "pulled_for_date", "logged_at"]


def _et_today():
    return (dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=4)).date()


def fetch(date):
    req = urllib.request.Request(API.format(date=date), headers=UA)
    with urllib.request.urlopen(req, timeout=45) as r:
        return json.loads(r.read().decode("utf-8"))


def _med(rows):
    ps = [m.get("price") for m in rows.get("markets", []) if isinstance(m.get("price"), (int, float))]
    return (round(statistics.median(ps)) if ps else None, len(ps))


def build(date):
    try:
        data = fetch(date)
    except Exception as e:
        print(f"[kprops] fetch failed ({e})", file=sys.stderr)
        return None
    ks = [x for x in data if x.get("category") == "pitching_strikeouts" and not x.get("isAlternate")]
    # fold over+under per (player, game)
    byp = {}
    for x in ks:
        key = (x.get("playerId"), x.get("gameId"))
        byp.setdefault(key, {})[x.get("overUnder")] = x
    recs = []
    for (pid, gpk), sides in byp.items():
        base = sides.get("over") or sides.get("under")
        if not base:
            continue
        over, under = sides.get("over"), sides.get("under")
        om, ob = (_med(over) if over else (None, 0))
        um, ub = (_med(under) if under else (None, 0))
        recs.append({
            "date": date, "game_pk": gpk, "player_id": pid,
            "name": base.get("name"), "team": base.get("teamCode"),
            "opp": base.get("opposingTeamCode"), "is_home": base.get("isHome") or base.get("issHome") or base.get("isHome"),
            "line": base.get("line"),
            "over_median": om, "over_best": (over.get("bestMarket") or {}).get("price") if over else None, "over_books": ob,
            "under_median": um, "under_best": (under.get("bestMarket") or {}).get("price") if under else None, "under_books": ub,
            "pf_rating": base.get("pfRating"), "matchup_value": base.get("matchupValue"),
            "matchup_label": base.get("matchupLabel"),
            "hit_l5": base.get("hitRateL5"), "hit_l10": base.get("hitRateL10"),
            "hit_season": base.get("hitRateSeason"), "avg_l10": base.get("avgL10"),
        })
    return recs


def main():
    date = os.environ.get("KPROPS_DATE") or _et_today().isoformat()
    recs = build(date)
    if not recs:
        print("[kprops] no strikeout props (off day or feed empty) — leaving files", file=sys.stderr)
        return 0
    now = dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")
    payload = {"generated_at": now, "date": date, "source": "PropFinder /mlb/props pitching_strikeouts",
               "n_pitchers": len(recs), "props": recs}
    # latest snapshot
    with open(OUT_JSON, "w") as f:
        json.dump(payload, f, indent=1)
    # daily archive (for the re-run)
    adir = os.path.join(DATA, "archive", date)
    os.makedirs(adir, exist_ok=True)
    with open(os.path.join(adir, "kprops.json"), "w") as f:
        json.dump(payload, f, separators=(",", ":"))
    # append-only movement log
    new = os.path.exists(ARCHIVE_CSV) is False
    with open(ARCHIVE_CSV, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=CSV_COLS, extrasaction="ignore")
        if new:
            w.writeheader()
        for r in recs:
            r2 = dict(r, pulled_for_date=date, logged_at=now)
            w.writerow(r2)
    print(f"[kprops] wrote {len(recs)} pitcher K-props for {date}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
