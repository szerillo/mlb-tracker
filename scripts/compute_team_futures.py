#!/usr/bin/env python3
"""
Aggregate team_projections.json + team_futures_odds.json into a single
team_futures.json the frontend renders. For each of the 30 teams:

  • Composite projection = simple mean of the available source systems
    (PECOTA, FanGraphs, ATC, BAT, OOPSY). Equal-weighted so we don't get
    into the business of judging which system is best.
  • Composite *vs market* edges:
      win_total_edge  = composite_wins - market_line   (positive = lean Over)
      div_edge_pct    = composite_div_pct - market_implied_div_pct
      playoff_edge_pct = composite_playoff_pct - market_implied_playoff_pct
      ws_edge_pct      = composite_ws_pct - market_implied_ws_pct
  • Market implied probability: de-vig American odds against the FAVORITE side
    when we only have one side, or against the full field for division/WS
    (sum of all teams' implied probs, scale to 1.0).

We DO NOT compute fair odds from win totals via Monte Carlo here — the
public-system make_playoffs / win_ws columns are already the right ground
truth and they bake in schedule / standings / playoff structure properly.

USAGE:
    python scripts/compute_team_futures.py

Output keys per team (in addition to forwarding raw inputs):
    composite: { wins, losses, div_pct, wc_pct, playoff_pct, ws_pct, n_systems }
    sean:      { wins: null, ... }        # placeholder, will populate later
    market:    { win_total_line, win_total_implied,
                 div_odds, div_implied_pct,
                 playoff_odds, playoff_implied_pct,
                 ws_odds, ws_implied_pct }
    edges:     { win_total, div_pct, playoff_pct, ws_pct }
"""
from __future__ import annotations
import datetime, json, sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
PROJ_FILE = REPO_ROOT / "data" / "team_projections.json"
ODDS_FILE = REPO_ROOT / "data" / "team_futures_odds.json"
OUTPUT    = REPO_ROOT / "data" / "team_futures.json"
# Optional Rotowire snapshot of Miss-Playoffs (NO) odds across 4 books. When
# present, compute_team_futures merges the actual NO American + implied % per
# team into the market dict so the frontend can show real prices for the No
# side instead of the mathematical mirror of YES.
NO_PLAYOFFS_FILE = REPO_ROOT / "data" / "team_no_playoff_odds.json"


def american_to_prob(odds: int | float) -> float | None:
    if odds is None:
        return None
    try:
        odds = float(odds)
    except (TypeError, ValueError):
        return None
    if odds > 0:
        return 100.0 / (odds + 100.0)
    return -odds / (-odds + 100.0)


def mean(values):
    vs = [v for v in values if v is not None]
    return sum(vs) / len(vs) if vs else None


def composite_for_team(projs: dict) -> dict:
    """Equal-weight average across whichever source systems exist."""
    fields = ["wins", "losses", "div_pct", "wc_pct", "playoff_pct", "ws_pct"]
    out = {}
    for f in fields:
        vals = [p.get(f) for p in projs.values()]
        m = mean(vals)
        if m is not None:
            out[f] = round(m, 2)
    out["n_systems"] = len(projs)
    return out


def _star_pos(edge_pct):
    # Star only positive edges (no bettable opposite side, e.g. Division/WS).
    if edge_pct is None or edge_pct <= 0:
        return ""
    return _star_tier(edge_pct)


def _star_tier(edge_pct):
    """Star rating for a market edge. Edges in percentage points (e.g. 4.5)
    OR wins (for win_total). Uses ABS magnitude because either direction can
    be the bettable side (Under/No edges show up as negative). Matches
    player_futures.py: ★★★ 4%+, ★★ 2-4%, ★ 0.5-2%, blank ≤0.5%."""
    if edge_pct is None:
        return ""
    e = abs(edge_pct)
    if e >= 4.0:  return "★★★"
    if e >= 2.0:  return "★★"
    if e >= 0.5:  return "★"
    return ""


def devig_field(odds_by_team: dict[str, int]) -> dict[str, float]:
    """For division / playoffs / WS futures: sum the raw implied probabilities
    across all teams and scale each by that sum (de-vig). Returns each team's
    de-vigged probability in percent points (e.g. 8.3)."""
    raws = {abbr: american_to_prob(odds) for abbr, odds in odds_by_team.items()
            if odds is not None}
    raws = {k: v for k, v in raws.items() if v is not None}
    total = sum(raws.values())
    if total <= 0:
        return {}
    return {abbr: round(p / total * 100, 2) for abbr, p in raws.items()}


def _skip_daily(output_path, label, am_hour_et=9):
    """Recompute at most once per day — the first pipeline run at/after ~9 AM ET,
    so the published generated_at stays steady all day. FORCE_RUN bypasses."""
    import os, json, datetime
    if os.environ.get("FORCE_RUN"):
        return False
    try:
        if not output_path.exists():
            return False
        gen = json.loads(output_path.read_text()).get("generated_at", "") or ""
        g = gen.replace("Z", "")
        if "+" in g:
            g = g.split("+")[0]
        gen_dt = datetime.datetime.fromisoformat(g)
    except Exception:
        return False
    et_now = datetime.datetime.utcnow() - datetime.timedelta(hours=4)
    gen_et = gen_dt - datetime.timedelta(hours=4)
    if gen_et.date() >= et_now.date():
        print(f"[{label}] skip recompute: already refreshed today ({gen}); keeping steady.")
        return True
    if et_now.hour < am_hour_et:
        print(f"[{label}] skip recompute: before {am_hour_et}AM ET; holding until the AM data pull.")
        return True
    return False


def main():
    if _skip_daily(OUTPUT, "team-futures"):
        return 0
    if not PROJ_FILE.exists() or not ODDS_FILE.exists():
        print(f"[team-futures] missing inputs (proj={PROJ_FILE.exists()}, odds={ODDS_FILE.exists()})",
              file=sys.stderr)
        return 1

    proj_data = json.loads(PROJ_FILE.read_text())
    odds_data = json.loads(ODDS_FILE.read_text())
    # Optional NO playoff odds snapshot (Rotowire). Falls back to empty dict
    # when the file is missing — the rest of the pipeline still emits valid
    # output, just without the playoff_no_* fields.
    no_data = {}
    if NO_PLAYOFFS_FILE.exists():
        try:
            no_data = json.loads(NO_PLAYOFFS_FILE.read_text()).get("teams", {}) or {}
        except Exception as e:
            print(f"[team-futures] could not load {NO_PLAYOFFS_FILE.name}: {e}", file=sys.stderr)
            no_data = {}

    # PRESERVE-ON-EMPTY-ODDS: if the odds fetcher hit the BettingPros IP block
    # (Cloudflare on GH Actions runners), team_futures_odds.json may have
    # n_teams=0 or have teams with all-null market data. In that case do NOT
    # overwrite the existing team_futures.json — its market data is still
    # valid from a previous successful run. Without this guard, every blocked
    # run would wipe the frontend Futures tab back to "—" cells.
    odds_teams = odds_data.get("teams") or {}
    has_any_market_data = any(
        (t.get("win_total") and t["win_total"].get("line") is not None)
        or (t.get("division") and t["division"].get("odds") is not None)
        or (t.get("world_series") and t["world_series"].get("odds") is not None)
        for t in odds_teams.values()
    )
    if odds_data.get("n_teams", 0) == 0 or not has_any_market_data:
        if OUTPUT.exists():
            print("[team-futures] odds file is empty (IP block?) — "
                  "leaving existing team_futures.json unchanged", file=sys.stderr)
            return 0
        # If no existing output, fall through and produce best-effort

    teams_proj = proj_data["teams"]
    teams_odds = odds_data["teams"]

    # 1) Compute composites per team
    composites = {}
    for abbr, t in teams_proj.items():
        composites[abbr] = composite_for_team(t["projections"])

    # 2) De-vig the futures markets across the league
    div_odds = {a: ((teams_odds.get(a) or {}).get("division") or {}).get("odds")
                for a in teams_proj}
    playoff_odds = {a: ((teams_odds.get(a) or {}).get("playoffs") or {}).get("odds")
                    for a in teams_proj}
    ws_odds = {a: ((teams_odds.get(a) or {}).get("world_series") or {}).get("odds")
               for a in teams_proj}

    # Division: raw American-odds → implied probability per team. Previously
    # this de-vigged per division; user spec is raw across all markets for
    # consistency with Player Futures. Edges still meaningful since model_p is
    # fair, even if market_p carries the book's vig.
    div_implied = {a: round(american_to_prob(o) * 100, 2) if o is not None else None
                   for a, o in div_odds.items()}

    # Playoffs: ~6 teams will eventually be in (12 with WC), but market is
    # per-team yes/no, so we don't de-vig across teams — just compute the
    # raw implied probability of each yes side.
    playoff_implied = {a: round(american_to_prob(o) * 100, 2) if o is not None else None
                       for a, o in playoff_odds.items()}

    # WS: single championship market, all 30 teams compete, de-vig across all.
    # WS: raw implied per team (no de-vig, matches Player Futures)
    ws_implied_all = {a: round(american_to_prob(o) * 100, 2) if o is not None else None
                      for a, o in ws_odds.items()}

    # 3) Build per-team output
    teams_out = {}
    for abbr, info in teams_proj.items():
        comp = composites.get(abbr, {})
        market = teams_odds.get(abbr) or {}
        wt = market.get("win_total") or {}
        div = market.get("division") or {}
        po = market.get("playoffs") or {}
        ws = market.get("world_series") or {}

        # Win-total "fair line implied" — when over/under odds aren't a
        # symmetric -110/-110, the actual implied total is offset. We use
        # the simple de-vig over→under approach:
        #   over_p + under_p = sum; over_share = over_p/sum; line + (over_share - 0.5) * K
        # where K=12 is the same constant we use on the scoreboard tooltip.
        wt_implied_line = None
        if wt.get("line") is not None and wt.get("over_odds") is not None and wt.get("under_odds") is not None:
            op = american_to_prob(wt["over_odds"])
            up = american_to_prob(wt["under_odds"])
            if op and up:
                wt_implied_line = round(wt["line"] + ((op / (op + up)) - 0.5) * 12, 2)

        wt_edge = None
        if comp.get("wins") is not None and wt_implied_line is not None:
            wt_edge = round(comp["wins"] - wt_implied_line, 2)

        div_imp = div_implied.get(abbr)
        div_edge = round(comp["div_pct"] - div_imp, 2) if (comp.get("div_pct") is not None and div_imp is not None) else None

        po_imp = playoff_implied.get(abbr)
        po_edge = round(comp["playoff_pct"] - po_imp, 2) if (comp.get("playoff_pct") is not None and po_imp is not None) else None
        # Real NO (Miss Playoffs) odds from the optional Rotowire snapshot.
        # po_no_edge = composite_NO − market_NO_implied, where composite_NO
        # = 100 − composite_YES. Positive edge = lean NO (bet they miss).
        no_team = no_data.get(abbr) or {}
        po_no_odds = no_team.get("no_odds")
        po_no_book = no_team.get("no_book")
        po_no_imp  = round(american_to_prob(po_no_odds) * 100, 2) if po_no_odds is not None else None
        po_no_edge = None
        if comp.get("playoff_pct") is not None and po_no_imp is not None:
            po_no_edge = round((100.0 - comp["playoff_pct"]) - po_no_imp, 2)
        # Bettable side = larger |edge|. Pick that as the displayed binary edge
        # so frontend's binaryEdgeCell shows the side the user should actually
        # bet (instead of always reading YES even when NO has a bigger edge).
        # We keep edges.playoff_pct = YES edge (signed; negative = lean NO) so
        # the existing sign-based frontend keeps working unchanged. The NO
        # edge ships alongside in edges.playoff_no_pct for callers that want
        # both directly.
        # Bettable side = the side with a POSITIVE edge (model beats the price
        # you'd actually pay). A negative YES edge does NOT imply a bettable NO
        # unless we have real NO odds showing positive NO value — otherwise the
        # gap is just the book's vig, not an edge. Pick the largest positive
        # edge; if neither side is positive there's no bettable edge (keep the
        # signed YES edge for an honest display, but it won't be starred).
        _cands = []
        if po_edge is not None:    _cands.append(('Y', po_edge))
        if po_no_edge is not None: _cands.append(('N', po_no_edge))
        _pos = [c for c in _cands if c[1] is not None and c[1] > 0]
        if _pos:
            po_best_side, po_best_edge = max(_pos, key=lambda c: c[1])
        else:
            po_best_side, po_best_edge = ('Y', po_edge)

        ws_imp = ws_implied_all.get(abbr)
        ws_edge = round(comp["ws_pct"] - ws_imp, 2) if (comp.get("ws_pct") is not None and ws_imp is not None) else None

        teams_out[abbr] = {
            "abbr":         abbr,
            "name":         info["name"],
            "division":     info["division"],
            "projections":  info["projections"],
            "composite":    comp,
            "sean":         {"wins": None, "div_pct": None, "wc_pct": None,
                             "playoff_pct": None, "ws_pct": None},
            "market": {
                "win_total_line":    wt.get("line"),
                "win_total_implied": wt_implied_line,
                "win_total_over":    {"odds": wt.get("over_odds"), "book": wt.get("over_book")},
                "win_total_under":   {"odds": wt.get("under_odds"), "book": wt.get("under_book")},
                "div_odds":          div.get("odds"),
                "div_book":          div.get("book"),
                "div_implied_pct":   div_imp,
                "playoff_odds":      po.get("odds"),
                "playoff_book":      po.get("book"),
                "playoff_implied_pct": po_imp,
                "playoff_no_odds":     po_no_odds,
                "playoff_no_book":     po_no_book,
                "playoff_no_implied_pct": po_no_imp,
                "ws_odds":           ws.get("odds"),
                "ws_book":           ws.get("book"),
                "ws_implied_pct":    ws_imp,
            },
            "edges": {
                "win_total":      wt_edge,
                "div_pct":        div_edge,
                "playoff_pct":    po_edge,
                "playoff_no_pct": po_no_edge,
                "playoff_best_pct": po_best_edge,
                "playoff_best_side": po_best_side,
                "ws_pct":         ws_edge,
            },
            "stars": {
                "win_total":   _star_tier(wt_edge),
                "div_pct":     _star_pos(div_edge),
                "playoff_pct": _star_pos(po_best_edge),
                "ws_pct":      _star_pos(ws_edge),
            },
        }

    payload = {
        "generated_at":       datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds"),
        "season":             odds_data.get("season"),
        "projections_at":     proj_data.get("generated_at"),
        "odds_at":            odds_data.get("generated_at"),
        "n_teams":            len(teams_out),
        "sources": {
            **proj_data.get("sources", {}),
            "betting_pros": "https://api.bettingpros.com/v3/offers · best-available across DK/FD/MGM/Caesars/etc",
        },
        "teams":              teams_out,
    }
    OUTPUT.write_text(json.dumps(payload, separators=(",", ":")))
    print(f"[team-futures] wrote {OUTPUT} ({len(teams_out)} teams)", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
