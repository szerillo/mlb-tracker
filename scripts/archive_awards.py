#!/usr/bin/env python3
"""archive_awards.py - daily snapshot of the awards board for the vote-fit λ fit.

Reads data/player_futures.json (the live board) and writes a slim per-day copy
to data/awards_archive/{ET-date}.json capturing, for every candidate in all six
markets: model_p, display_p (λ-blended board value; falls back to model_p),
market_p (de-vig-free book implied), edge, best_odds, low_confidence.

This is the dataset Fable needs to (a) turn λ from a 0.5 prior into a real fit
and (b) grade awards-model calibration + CLV in October. One append/overwrite
per run; converges to the final pre-lock board for the day. Pure local read.
"""
from __future__ import annotations
import datetime, json, sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
DATA = REPO / "data"
SRC = DATA / "player_futures.json"
OUT_DIR = DATA / "awards_archive"

FIELDS = ("name", "team_abbr", "league", "rank", "model_p", "display_p",
          "market_p", "edge", "best_odds", "low_confidence")


def _et_today():
    return (datetime.datetime.utcnow() - datetime.timedelta(hours=4)).date().isoformat()


def main():
    if not SRC.exists():
        print("[awards-archive] no player_futures.json; skipping", file=sys.stderr)
        return 0
    try:
        pf = json.loads(SRC.read_text())
    except Exception as e:
        print(f"[awards-archive] unreadable player_futures.json: {e}", file=sys.stderr)
        return 0

    markets = pf.get("markets") or {}
    if not markets:
        print("[awards-archive] no markets; skipping", file=sys.stderr)
        return 0

    date = _et_today()
    out = {}
    n_cands = 0
    for mkt, blk in markets.items():
        rows = []
        for c in (blk.get("candidates") or []):
            row = {f: c.get(f) for f in FIELDS if f in c}
            # display_p falls back to model_p if the old engine is still live
            if "display_p" not in row and c.get("model_p") is not None:
                row["display_p"] = c.get("model_p")
            rows.append(row)
            n_cands += 1
        out[mkt] = {"label": blk.get("label"), "engine": blk.get("engine"),
                    "n_pool": blk.get("n_pool"), "candidates": rows}

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    payload = {"date": date,
               "generated_at": datetime.datetime.utcnow().isoformat(timespec="seconds") + "Z",
               "source_generated_at": pf.get("generated_at"),
               "model": pf.get("model"),
               "markets": out}
    (OUT_DIR / f"{date}.json").write_text(json.dumps(payload, separators=(",", ":")))
    print(f"[awards-archive] {date}: {len(out)} markets, {n_cands} candidates → {OUT_DIR / (date + '.json')}",
          file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
