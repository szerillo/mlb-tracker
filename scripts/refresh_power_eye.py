#!/usr/bin/env python3
"""
Nightly Power+Eye refresh — pulls 3 sources, applies user's Orr-equivalent
formulas, writes data/power_eye_2026.csv (raw values + inputs).

Sources:
  1. Savant Exit Velocity leaderboard (Barrel% + HardHit%)
  2. Savant Swing/Take run values     (Heart/Shadow/Chase/Waste RV)
  3. FanGraphs Plate Discipline       (O-Swing/Z-Swing/SwStr/Zone)

Formulas:
  Power = -4.78 + 1.18 * Barrel%  + 0.39 * HardHit%       (raw % inputs)
  Eye   = 69.34 - 54.80*O-Sw + 21.01*Z-Sw + 29.54*SwStr
        - 134.00*Zone + 0.05*Heart_RV + 0.01*Shadow_RV
        + 0.06*Chase_RV + 0.06*Waste_RV                    (decimals + raw RV)

Source 3 (FanGraphs) is Cloudflare-protected — uses Playwright if installed,
falls back to writing an EMPTY row set and leaving Eye blank (Power still
populates). Sean drives FG via the admin browser bookmarklet when needed.
"""
from __future__ import annotations
import csv, datetime, json, os, sys, unicodedata, urllib.request
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR  = REPO_ROOT / "data"
OUT_PATH  = DATA_DIR / "power_eye_2026.csv"
FG_CACHE  = DATA_DIR / "fg_plate_disc_2026.csv"  # manually uploaded fallback

YEAR = datetime.date.today().year
UA   = "Mozilla/5.0 (mlb-tracker/power-eye)"

def _fetch(url, timeout=20):
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read().decode("utf-8-sig")

def _norm(s):
    if not s: return ""
    s = unicodedata.normalize("NFKD", s).encode("ascii","ignore").decode()
    return " ".join("".join(c for c in s.lower() if c.isalnum() or c == " ").split())

# ---- 1. Savant Exit Velocity ----
def pull_savant_ev():
    url = (f"https://baseballsavant.mlb.com/leaderboard/custom?year={YEAR}"
           f"&type=batter&filter=&min=10&selections=pa%2Cab%2Cbarrel_batted_rate%2Chard_hit_percent"
           "&chart=false&x=pa&y=pa&r=no&chartType=beeswarm&csv=true")
    try:
        txt = _fetch(url)
    except Exception as e:
        print(f"[power_eye] Savant EV fetch failed: {e}", file=sys.stderr)
        return {}
    out = {}
    rdr = csv.DictReader(txt.splitlines())
    for row in rdr:
        pid = (row.get("player_id") or "").strip()
        if not pid: continue
        last, first = [x.strip() for x in (row.get("last_name, first_name") or "").split(",", 1) + [""]][:2]
        try:
            out[pid] = {
                "name": f"{first} {last}".strip(),
                "pa":   int(row.get("pa") or 0),
                "barrel": float(row.get("barrel_batted_rate") or 0),
                "hh":     float(row.get("hard_hit_percent")   or 0),
            }
        except ValueError:
            pass
    print(f"[power_eye] Savant EV: {len(out)} hitters")
    return out

# ---- 2. Savant Swing/Take ----
def pull_savant_st():
    # Working URL format requires empty attemptType=
    url = f"https://baseballsavant.mlb.com/leaderboard/swing-take?year={YEAR}&attemptType=&min=10&csv=true"
    try:
        txt = _fetch(url)
    except Exception as e:
        print(f"[power_eye] Savant ST fetch failed: {e}", file=sys.stderr)
        return {}
    out = {}
    rdr = csv.DictReader(txt.splitlines())
    for row in rdr:
        pid = (row.get("player_id") or "").strip()
        if not pid: continue
        try:
            out[pid] = {
                "heart_rv":  float(row.get("runs_heart")  or 0),
                "shadow_rv": float(row.get("runs_shadow") or 0),
                "chase_rv":  float(row.get("runs_chase")  or 0),
                "waste_rv":  float(row.get("runs_waste")  or 0),
            }
        except ValueError:
            pass
    print(f"[power_eye] Savant ST: {len(out)} hitters")
    return out

# ---- 3. FanGraphs Plate Discipline (Cloudflare-protected) ----
def pull_fg_pd():
    # FG is behind CF — accept either a pre-cached CSV in data/ OR an empty
    # set. The admin browser bookmarklet refreshes the cache.
    if not FG_CACHE.exists():
        print(f"[power_eye] FG cache missing at {FG_CACHE} — Eye will be blank for all hitters", file=sys.stderr)
        return {}
    out = {}
    with open(FG_CACHE, encoding="utf-8") as f:
        rdr = csv.DictReader(f, delimiter="|")
        for row in rdr:
            nm = _norm(row.get("name") or "")
            try:
                out[nm] = {
                    "team":  row.get("team") or "",
                    "osw":   float(row.get("osw")   or 0),
                    "zsw":   float(row.get("zsw")   or 0),
                    "swstr": float(row.get("swstr") or 0),
                    "zone":  float(row.get("zone")  or 0),
            }
            except ValueError:
                pass
    print(f"[power_eye] FG plate disc (cached): {len(out)} hitters")
    return out

def main():
    ev = pull_savant_ev()
    st = pull_savant_st()
    fg = pull_fg_pd()
    rows = []
    all_pids = set(ev) | set(st)
    for pid in all_pids:
        e = ev.get(pid, {})
        s = st.get(pid, {})
        name = e.get("name") or ""
        if not name: continue
        f = fg.get(_norm(name))
        power = round(-4.78 + 1.18 * e["barrel"] + 0.39 * e["hh"], 2) if (e and "barrel" in e) else None
        eye = None
        if f and s:
            eye = round(
                69.34 - 54.80*(f["osw"]/100) + 21.01*(f["zsw"]/100)
                + 29.54*(f["swstr"]/100) - 134.00*(f["zone"]/100)
                + 0.05*s["heart_rv"] + 0.01*s["shadow_rv"]
                + 0.06*s["chase_rv"] + 0.06*s["waste_rv"], 2)
        rows.append({
            "name": name, "team": (f or {}).get("team") or "",
            "pa": e.get("pa") or 0, "power": power, "eye": eye,
            "barrel": e.get("barrel"), "hh": e.get("hh"),
            "osw":   (f or {}).get("osw"),
            "zsw":   (f or {}).get("zsw"),
            "swstr": (f or {}).get("swstr"),
            "zone":  (f or {}).get("zone"),
            "heart_rv":  s.get("heart_rv"),
            "shadow_rv": s.get("shadow_rv"),
            "chase_rv":  s.get("chase_rv"),
            "waste_rv":  s.get("waste_rv"),
        })
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT_PATH, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        cols = ["name","team","pa","power","eye","barrel","hh","osw","zsw","swstr","zone",
                "heart_rv","shadow_rv","chase_rv","waste_rv"]
        w.writerow(cols)
        for r in rows:
            w.writerow([r.get(c, "") for c in cols])
    n_p = sum(1 for r in rows if r["power"] is not None)
    n_e = sum(1 for r in rows if r["eye"]   is not None)
    print(f"[power_eye] wrote {OUT_PATH} — {len(rows)} hitters ({n_p} with Power, {n_e} with Eye)")

if __name__ == "__main__":
    sys.exit(main())
