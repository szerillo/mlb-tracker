#!/usr/bin/env python3
"""
Bridge feed: SP stamina from the sheet's `Pitcher Projected` tab (H:I) ->
data/pitcher_projected.json, for compute_pro_projections.py. Stamina is the SP's
F5 share of the start (Holmes 0.633); the sheet's F15/G15. SP RA itself comes from
pitcher_stats.unified_adj (already in the repo), so this feed only carries stamina.
Bridge now, native later (derive stamina from expected IP/GS).
"""
from __future__ import annotations
import csv, io, json, os, re, sys, time, urllib.parse, urllib.request, datetime
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
OUTPUT = os.path.join(REPO_ROOT, "data", "pitcher_projected.json")
SHEET_ID = "1Dq9ma3W_YPOJJzq6ZnqivfaniEk8wZJw3gZvuDrH6DE"
GVIZ = "https://docs.google.com/spreadsheets/d/{sid}/gviz/tq?tqx=out:csv&sheet={tab}&_cb={cb}"
UA = "Mozilla/5.0 (compatible; mlb-tracker/1.0)"

def _norm(s):
    s = (s or "").lower().replace(".", "").replace("'", "")
    s = re.sub(r"\s+(jr|sr|ii|iii|iv)$", "", s)
    return re.sub(r"\s+", " ", s).strip()

def _f(v):
    try:
        v = str(v).strip().replace(",", "")
        return float(v) if v not in ("", "#N/A", "None") else None
    except (ValueError, TypeError):
        return None

def main():
    def tab(name):
        url = GVIZ.format(sid=SHEET_ID, tab=urllib.parse.quote(name), cb=int(time.time()))
        req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": "text/csv,*/*"})
        with urllib.request.urlopen(req, timeout=30) as r:
            return list(csv.reader(io.StringIO(r.read().decode("utf-8", "replace"))))
    # Stamina: Pitcher Projected H=col7 name, I=col8 stamina
    out = {}
    for row in tab("Pitcher Projected")[1:]:
        nm = row[7] if len(row) > 7 else ""
        st = _f(row[8]) if len(row) > 8 else None
        if nm and _norm(nm) and _norm(nm) not in out and st is not None:
            out[_norm(nm)] = {"name": nm, "stamina": st}
    # K-BB: Pitcher KBB A=col0 name, B=col1 kbb (first-occurrence = VLOOKUP)
    kbb_seen = set()
    for row in tab("Pitcher KBB")[1:]:
        nm = row[0] if len(row) > 0 else ""
        raw = row[1] if len(row) > 1 else ""
        kb = None
        if raw:
            m2 = str(raw).strip()
            pct = m2.endswith("%")
            kb = _f(m2.rstrip("%"))
            if kb is not None and (pct or kb > 1.5):   # percentage -> fraction
                kb = kb / 100.0
        k = _norm(nm)
        if nm and k and k not in kbb_seen and kb is not None:
            kbb_seen.add(k)
            out.setdefault(k, {"name": nm})["kbb"] = kb
    payload = {"generated_at": datetime.datetime.utcnow().isoformat(timespec="seconds") + "Z",
               "source": "Google Sheet 'Pitcher Projected' H:I (SP stamina bridge feed)",
               "n_pitchers": len(out), "n_stamina": sum(1 for p in out.values() if "stamina" in p), "n_kbb": sum(1 for p in out.values() if "kbb" in p), "pitchers": out}
    os.makedirs(os.path.dirname(OUTPUT), exist_ok=True)
    with open(OUTPUT, "w") as fh:
        json.dump(payload, fh, indent=1)
    print(f"[pitcher_projected] wrote {len(out)} pitchers (stamina+kbb) -> {OUTPUT}")
    return 0

if __name__ == "__main__":
    sys.exit(main())
