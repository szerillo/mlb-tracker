#!/usr/bin/env python3
"""
Refresh team-level season projections from 5 public sources:

  • PECOTA          — Baseball Prospectus standings page
  • FanGraphs       — FG Depth Charts via FG playoff odds /fg/div
  • ATC             — FG playoff odds /atc/div
  • THE BAT X       — FG playoff odds /thebat/div
  • OOPSY           — FG playoff odds /oopsy/div

For each team, we capture:
  • proj_wins / proj_losses
  • win_div_pct
  • win_wc_pct (FG: "Clinch Wild Card", BP: "WC%")
  • make_playoffs_pct
  • win_ws_pct

Both sources are server-rendered HTML, so a simple urllib + html.parser pass
works (no Cloudflare auth flow needed).

Output: data/team_projections.json
  {
    "generated_at": ISO,
    "sources": { "pecota": "url", "fangraphs": ... },
    "teams": {
      "NYY": {
        "abbr": "NYY", "name": "Yankees", "division": "AL East",
        "projections": {
          "pecota":     { wins, losses, div_pct, wc_pct, playoff_pct, ws_pct },
          "fangraphs":  { ... },
          "atc":        { ... },
          "bat":        { ... },
          "oopsy":      { ... }
        }
      }, ...
    }
  }

The composite blend lives in compute_team_futures.py, not here — this script
just collects raw per-system numbers.

USAGE:
    python scripts/refresh_team_projections.py
"""
from __future__ import annotations
import datetime, json, re, sys, urllib.request
from html.parser import HTMLParser
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
OUTPUT = REPO_ROOT / "data" / "team_projections.json"

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120 Safari/537.36")

# Team-name normalization. FG uses short names ("Yankees", "Rays"), BP uses
# city+abbr concatenated ("New YorkNYY"). We standardize to the MLB team abbr
# so the composite join works downstream.
NAME_TO_ABBR = {
    # Full team names (FG)
    "Yankees": "NYY", "Red Sox": "BOS", "Blue Jays": "TOR", "Rays": "TB", "Orioles": "BAL",
    "Guardians": "CLE", "Twins": "MIN", "Tigers": "DET", "Royals": "KC", "White Sox": "CWS",
    "Astros": "HOU", "Mariners": "SEA", "Rangers": "TEX", "Angels": "LAA", "Athletics": "ATH",
    "Braves": "ATL", "Mets": "NYM", "Phillies": "PHI", "Nationals": "WSH", "Marlins": "MIA",
    "Brewers": "MIL", "Cubs": "CHC", "Reds": "CIN", "Cardinals": "STL", "Pirates": "PIT",
    "Dodgers": "LAD", "Padres": "SD", "Giants": "SF", "Diamondbacks": "ARI", "D-backs": "ARI",
    "Rockies": "COL",
    # BP variants (the "city" concat — we strip city by taking the last 2-3 chars
    # since BP renders "New YorkNYY". This is handled in the parser, not here.)
}
# Some BP abbrs differ from MLB; map to the standard 30 we use elsewhere
BP_ABBR_FIXUP = {"CWS": "CWS", "TB": "TB", "SD": "SD", "SF": "SF", "WSH": "WSH",
                 "KC": "KC", "ATH": "ATH"}


def _http_get(url: str, timeout: int = 30) -> str | None:
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": UA,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
        })
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.read().decode("utf-8", errors="replace")
    except Exception as e:
        print(f"  http err {url}: {e}", file=sys.stderr)
        return None


# ── HTML table extractor ──────────────────────────────────────────────────
class TableExtractor(HTMLParser):
    """Extract every table on the page as list[list[str]] (rows × cells)."""
    def __init__(self):
        super().__init__()
        self.tables: list[list[list[str]]] = []
        self._in_table = 0
        self._in_row = False
        self._in_cell = False
        self._cur_row: list[str] = []
        self._cur_cell: list[str] = []
        self._cur_table: list[list[str]] = []

    def handle_starttag(self, tag, attrs):
        if tag == "table":
            self._in_table += 1
            self._cur_table = []
        elif tag == "tr" and self._in_table:
            self._in_row = True
            self._cur_row = []
        elif tag in ("td", "th") and self._in_row:
            self._in_cell = True
            self._cur_cell = []

    def handle_endtag(self, tag):
        if tag == "table" and self._in_table:
            self._in_table -= 1
            if not self._in_table:
                self.tables.append(self._cur_table)
                self._cur_table = []
        elif tag == "tr" and self._in_row:
            self._in_row = False
            if self._cur_row:
                self._cur_table.append(self._cur_row)
        elif tag in ("td", "th") and self._in_cell:
            self._in_cell = False
            self._cur_row.append("".join(self._cur_cell).strip())
            self._cur_cell = []

    def handle_data(self, data):
        if self._in_cell:
            self._cur_cell.append(data)


def extract_tables(html: str) -> list[list[list[str]]]:
    p = TableExtractor()
    p.feed(html)
    return p.tables


def _pct(s: str) -> float | None:
    """Parse '57.1%' or '57.1' → 57.1.

    Both FG and BP return percent VALUES directly (FG with % suffix, BP
    without). We never see decimal probabilities here — '0.1' means 0.1%,
    not 10%. So just strip and parse — no auto-rescaling."""
    if not s:
        return None
    s = s.strip().replace("%", "").replace(",", "")
    try:
        return round(float(s), 2)
    except ValueError:
        return None


def _float(s: str) -> float | None:
    if not s:
        return None
    try:
        return float(s.replace(",", "").strip())
    except ValueError:
        return None


# ── FG playoff odds scraper ──────────────────────────────────────────────
# Page layout: 6 division tables (idx 14-25 on the playoff-odds page) with
# columns: Team | W | L | W% | GB | Proj W | Proj L | ROS W% | SoS | Win Div |
#           Clinch Bye | Clinch WC | Make Playoffs | Win WS

def _parse_next_data(html: str) -> dict[str, dict]:
    """Extract playoff-odds rows from the Next.js __NEXT_DATA__ blob.
    endData mapping: ExpW/ExpL = proj W/L; divTitle=Win Div; wcTitle=Clinch WC;
    poffTitle=Make Playoffs; wsWin=Win WS (all 0-1 fractions)."""
    m = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, re.S)
    if not m:
        return {}
    try:
        blob = json.loads(m.group(1))
        queries = blob["props"]["pageProps"]["dehydratedState"]["queries"]
    except Exception:
        return {}
    rows = None
    for q in queries:
        qk = q.get("queryKey") or []
        if qk and qk[0] == "playoff-odds":
            rows = (q.get("state") or {}).get("data")
            break
    if not isinstance(rows, list):
        return {}
    out = {}
    for r in rows:
        abbr = r.get("abbName")
        e = r.get("endData") or {}
        if not abbr or "ExpW" not in e:
            continue
        # Normalize FG abbreviations to ours
        abbr = {"CHW": "CWS", "WSN": "WSH", "TBR": "TB", "SDP": "SD",
                "SFG": "SF", "KCR": "KC", "OAK": "ATH"}.get(abbr, abbr)
        out[abbr] = {
            "wins":        round(e["ExpW"], 1),
            "losses":      round(e["ExpL"], 1),
            "div_pct":     round(e.get("divTitle", 0) * 100, 1),
            "wc_pct":      round(e.get("wcTitle", 0) * 100, 1),
            "playoff_pct": round(e.get("poffTitle", 0) * 100, 1),
            "ws_pct":      round(e.get("wsWin", 0) * 100, 1),
        }
    return out


def fetch_fg_projection_mode(mode: str) -> dict[str, dict]:
    """mode is one of 'fg', 'atc', 'thebat', 'oopsy'."""
    url = f"https://www.fangraphs.com/standings/playoff-odds/{mode}/div"
    html = _http_get(url)
    if not html:
        return {}
    # 2026-07-14: FG moved playoff-odds to client-rendered Next.js — the HTML
    # <table> is gone; data now lives in the __NEXT_DATA__ dehydrated queries.
    # Parse that first; fall back to the legacy table parser just in case.
    out = _parse_next_data(html)
    if out:
        print(f"  fg/{mode}: {len(out)} teams (__NEXT_DATA__)", file=sys.stderr)
        return out
    tables = extract_tables(html)
    out = {}
    # Find tables with our 14-column shape that have team rows
    for t in tables:
        if not t or len(t) < 2:
            continue
        # First row is headers
        headers = t[0]
        if not any("Win\nWS" in h or "Win WS" in h or h == "WS" for h in headers):
            continue
        # Confirm we have the right column count
        if len(headers) < 14:
            continue
        # Each subsequent row is a team
        for row in t[1:]:
            if len(row) < 14:
                continue
            # Team cell looks like "YankeesYankees36-23" — FG renders the name
            # twice (logo alt + display text). Edge case: Arizona is alt
            # "Diamondbacks" + display "D-backs" (NOT duplicated), so a strict
            # self-duplication regex misses them. Instead, scan the cell for
            # any known team name from NAME_TO_ABBR.
            team_cell = row[0]
            abbr = None
            for nm, ab in NAME_TO_ABBR.items():
                if nm in team_cell:
                    abbr = ab
                    break
            if not abbr:
                continue
            out[abbr] = {
                "wins":         _float(row[5]),
                "losses":       _float(row[6]),
                "div_pct":      _pct(row[9]),
                "wc_pct":       _pct(row[11]),
                "playoff_pct":  _pct(row[12]),
                "ws_pct":       _pct(row[13]),
            }
    print(f"  fg/{mode}: {len(out)} teams", file=sys.stderr)
    return out


# ── BP PECOTA scraper ──────────────────────────────────────────────────────
# BP standings page has 2 wide tables (AL + NL), 15 rows each (3 divisions of
# 5 teams stacked). Columns per team: City+Abbr, SimW, SimL, SimW%, DCRS,
# DCRA, Div%, WC%, Playoff%, PAdj%, WS%, D1%, D7%.
def fetch_pecota() -> dict[str, dict]:
    html = _http_get("https://www.baseballprospectus.com/standings/")
    if not html:
        return {}
    tables = extract_tables(html)
    out = {}
    for t in tables:
        if not t or len(t) < 2:
            continue
        # BP rows have 13 cols starting with team
        for row in t[1:]:
            if len(row) < 11:
                continue
            team_cell = row[0]
            # Last 2-3 uppercase letters are the abbr; everything before is city
            m = re.search(r"([A-Z]{2,3})$", team_cell)
            if not m:
                continue
            abbr = m.group(1)
            # Fix BP abbreviation oddities to match our standard set
            abbr = BP_ABBR_FIXUP.get(abbr, abbr)
            # SAC = Athletics (Sacramento) per BP; map to ATH
            if abbr == "SAC":
                abbr = "ATH"
            out[abbr] = {
                "wins":         _float(row[1]),    # SimW
                "losses":       _float(row[2]),    # SimL
                "div_pct":      _pct(row[6]),      # Div%
                "wc_pct":       _pct(row[7]),      # WC%
                "playoff_pct":  _pct(row[8]),      # Playoff%
                "ws_pct":       _pct(row[10]),     # WS% (after PAdj%)
            }
    print(f"  pecota: {len(out)} teams", file=sys.stderr)
    return out


# ── Per-team metadata (division, full name) ───────────────────────────────
# Used by the frontend so we don't have to maintain a separate roster file.
TEAMS_META = {
    "NYY": ("Yankees", "AL East"),  "BOS": ("Red Sox", "AL East"),
    "TOR": ("Blue Jays", "AL East"), "TB": ("Rays", "AL East"),
    "BAL": ("Orioles", "AL East"),
    "CLE": ("Guardians", "AL Central"), "MIN": ("Twins", "AL Central"),
    "DET": ("Tigers", "AL Central"), "KC": ("Royals", "AL Central"),
    "CWS": ("White Sox", "AL Central"),
    "HOU": ("Astros", "AL West"), "SEA": ("Mariners", "AL West"),
    "TEX": ("Rangers", "AL West"), "LAA": ("Angels", "AL West"),
    "ATH": ("Athletics", "AL West"),
    "ATL": ("Braves", "NL East"), "NYM": ("Mets", "NL East"),
    "PHI": ("Phillies", "NL East"), "WSH": ("Nationals", "NL East"),
    "MIA": ("Marlins", "NL East"),
    "MIL": ("Brewers", "NL Central"), "CHC": ("Cubs", "NL Central"),
    "CIN": ("Reds", "NL Central"), "STL": ("Cardinals", "NL Central"),
    "PIT": ("Pirates", "NL Central"),
    "LAD": ("Dodgers", "NL West"), "SD": ("Padres", "NL West"),
    "SF": ("Giants", "NL West"), "ARI": ("Diamondbacks", "NL West"),
    "COL": ("Rockies", "NL West"),
}


def main():
    print("[team-proj] fetching PECOTA…", file=sys.stderr)
    pecota = fetch_pecota()

    print("[team-proj] fetching FG / ATC / BAT X / OOPSY…", file=sys.stderr)
    fg_modes = {"fangraphs": "fg", "atc": "atc", "bat": "thebat", "oopsy": "oopsy"}
    fg_data = {}
    for key, mode in fg_modes.items():
        fg_data[key] = fetch_fg_projection_mode(mode)

    # Stitch by team abbr
    teams_out = {}
    for abbr, (name, division) in TEAMS_META.items():
        projs = {}
        if abbr in pecota:
            projs["pecota"] = pecota[abbr]
        for key, data in fg_data.items():
            if abbr in data:
                projs[key] = data[abbr]
        teams_out[abbr] = {
            "abbr": abbr,
            "name": name,
            "division": division,
            "projections": projs,
        }

    payload = {
        "generated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds"),
        "n_teams": len(teams_out),
        "sources": {
            "pecota":    "https://www.baseballprospectus.com/standings/",
            "fangraphs": "https://www.fangraphs.com/standings/playoff-odds/fg/div",
            "atc":       "https://www.fangraphs.com/standings/playoff-odds/atc/div",
            "bat":       "https://www.fangraphs.com/standings/playoff-odds/thebat/div",
            "oopsy":     "https://www.fangraphs.com/standings/playoff-odds/oopsy/div",
        },
        "teams": teams_out,
    }
    OUTPUT.write_text(json.dumps(payload, separators=(",", ":")))
    n_full = sum(1 for t in teams_out.values() if len(t["projections"]) >= 4)
    print(f"[team-proj] wrote {OUTPUT}  ({n_full}/30 teams with ≥4 systems)",
          file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
