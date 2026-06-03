#!/usr/bin/env python3
"""capture_clv_snapshot.py - daily PRE-GAME snapshot for CLV / directional tracking.

Action Network's PRO model projection (edge_projections.game) is only served for
UPCOMING games - it disappears once a game finalizes. To grade open->close line
movement against our number later, we must snapshot the PRO projection (and the
opening Book-30 line + the latest line seen) while games are still pending.

Runs in the daily cron during the pre-game window. Writes/merges
data/clv/{date}.json keyed by an_event_id so multiple runs keep the EARLIEST
pro/open and the LATEST line (closing proxy).
"""
import datetime, json, sys, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import scrape_action_archive as sa

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
OUT_DIR   = REPO_ROOT / "data" / "clv"


def main():
    today = datetime.date.today().isoformat()
    try:
        scraped = sa.scrape_date(today)
    except Exception as e:
        print(f"[capture-clv] scrape failed: {e}", file=sys.stderr)
        return 0
    games_in = scraped.get("games", []) or []
    now = datetime.datetime.utcnow().isoformat(timespec="seconds") + "Z"

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUT_DIR / f"{today}.json"
    prior = {}
    if out_path.exists():
        try:
            prior = json.loads(out_path.read_text()).get("games", {}) or {}
        except Exception:
            prior = {}

    merged = dict(prior)
    n_pro = 0
    for g in games_in:
        eid = str(g.get("an_event_id") or "")
        if not eid:
            continue
        cur = merged.get(eid, {})
        cur.update({
            "an_event_id": g.get("an_event_id"),
            "game_pk":     g.get("game_pk"),
            "away_team":   g.get("away_team"),
            "home_team":   g.get("home_team"),
            "start_time":  g.get("start_time"),
        })
        if g.get("open") and not cur.get("open"):
            cur["open"] = g["open"]
        if g.get("consensus"):
            cur["last"] = g["consensus"]
        pro = g.get("pro") or {}
        has_pro = any(v is not None for v in pro.values()) if pro else False
        if has_pro and not cur.get("pro"):
            cur["pro"] = pro
            cur["pro_first_seen"] = now
            n_pro += 1
        merged[eid] = cur

    payload = {"date": today, "captured_at": now, "n_games": len(merged), "games": merged}
    out_path.write_text(json.dumps(payload, indent=1))
    have_pro = sum(1 for v in merged.values() if v.get("pro"))
    print(f"[capture-clv] {today}: {len(merged)} games, {have_pro} with PRO (+{n_pro} new) -> {out_path}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
