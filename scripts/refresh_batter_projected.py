#!/usr/bin/env python3
"""
Bridge feed for the internal projection engine (compute_pro_projections.py).

Publishes the sheet's `Batter Projected` tab — the exact per-hitter inputs the
Model's offense chain uses — to data/batter_projected.json, so the engine
reproduces the sheet's offense ratios to the decimal without the spreadsheet in
the request path. This is the "bridge now, native later" step: the hitter inputs
still originate in the sheet, but everything downstream (offense ratio, DEF, the
whole run chain) is computed natively in Python. The native replacement is to
generate these five columns in the repo's own hitter pipeline.

Columns pulled per player (Batter Projected, live via gviz by tab name):
  A  Name        E  xR (=(woba/lg)^2 * run_scale)     F  FLD      G  BSR (x2)
  N  Name  R  platoon vs RHP (avg Steamer/2yr wRC+/100)
  S  Name  W  platoon vs LHP
Output: data/batter_projected.json = { "<normkey>": {name, xr, fld, bsr,
        plat_vsR, plat_vsL}, ... } plus a name index for joining lineups.
"""
from __future__ import annotations
import csv, io, json, os, re, sys, time, urllib.parse, urllib.request
import unicodedata

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
OUTPUT = os.path.join(REPO_ROOT, "data", "batter_projected.json")
SHEET_ID = "1Dq9ma3W_YPOJJzq6ZnqivfaniEk8wZJw3gZvuDrH6DE"
GVIZ = "https://docs.google.com/spreadsheets/d/{sid}/gviz/tq?tqx=out:csv&sheet={tab}&_cb={cb}"
UA = "Mozilla/5.0 (compatible; mlb-tracker/1.0)"


def _norm(s):
    s = (s or "").lower()
    s = "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))  # strip accents (Diaz/Diaz, Pena/Pena)
    s = s.replace(".", "").replace("'", "")
    s = re.sub(r"\s+(jr|sr|ii|iii|iv)$", "", s)
    return re.sub(r"\s+", " ", s).strip()


def _f(v):
    try:
        v = str(v).strip().replace(",", "")
        return float(v) if v not in ("", "#N/A", "None") else None
    except (ValueError, TypeError):
        return None


def fetch_tab(tab):
    url = GVIZ.format(sid=SHEET_ID, tab=urllib.parse.quote(tab), cb=int(time.time()))
    req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": "text/csv,*/*"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return list(csv.reader(io.StringIO(r.read().decode("utf-8", "replace"))))


def main():
    rows = fetch_tab("Batter Projected")
    # 0-indexed: A=0 name, E=4 xR, F=5 fld, G=6 bsr; N=13 name, R=17 platR; S=18 name, W=22 platL
    players = {}
    platR, platL = {}, {}
    for row in rows[1:]:
        def cell(i): return row[i] if i < len(row) else ""
        a = cell(0)
        if a and _norm(a) and _norm(a) not in players:   # first-occurrence = VLOOKUP semantics
            xr = _f(cell(4))
            if xr is not None:
                players[_norm(a)] = {"name": a, "xr": xr, "fld": _f(cell(5)) or 0.0, "bsr": _f(cell(6)) or 0.0}
        n = cell(13)
        if n and _norm(n) and _norm(n) not in platR:
            pv = _f(cell(17))
            if pv is not None: platR[_norm(n)] = pv
        sname = cell(18)
        if sname and _norm(sname) and _norm(sname) not in platL:
            pv = _f(cell(22))
            if pv is not None: platL[_norm(sname)] = pv
    for k, p in players.items():
        p["plat_vsR"] = platR.get(k)
        p["plat_vsL"] = platL.get(k)
    n_full = sum(1 for p in players.values() if p["plat_vsR"] is not None and p["plat_vsL"] is not None)
    payload = {
        "generated_at": __import__("datetime").datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "source": "Google Sheet 'Batter Projected' tab (bridge feed for compute_pro_projections)",
        "n_players": len(players), "n_with_platoon": n_full,
        "players": players,
    }
    os.makedirs(os.path.dirname(OUTPUT), exist_ok=True)
    with open(OUTPUT, "w") as fh:
        json.dump(payload, fh, indent=1)
    print(f"[batter_projected] wrote {len(players)} players ({n_full} with platoon) -> {OUTPUT}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
