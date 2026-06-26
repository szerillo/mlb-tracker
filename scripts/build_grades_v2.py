#!/usr/bin/env python3
"""
Build data/grades_v2.json — the single grade source the frontend reads.

Power + Eye are computed directly from Baseball Savant (no FanGraphs), Contact
from Savant's K% percentile; Def/BSR come from data/def_bsr_2026.csv (manual,
from the model). Each metric gets a rank-percentile (vs the hitter pool) and an
A+..D- letter via the shared ladder.

Power  : barrel% + hardhit% + bat-tracking blast% (hybrid — blast% where the
         hitter has bat-tracking, barrel+hardhit fallback otherwise).
Eye    : Savant plate discipline (O-Swing, Z-Swing, Zone, SwStr) + the 2026
         automated-zone chase percentile (Orr SEAGER proxy).
Contact: Savant automated K% percentile (lower K% = better).

Coefficients below were fit to Sean's calibrated example so output stays on the
same scale as the model's Power/Eye/SEAGER numbers.
"""
import csv, io, json, re, sys, unicodedata, datetime, urllib.request
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
DATA = REPO / "data"
OUT = DATA / "grades_v2.json"
DEF_BSR = DATA / "def_bsr_2026.csv"
YEAR = 2026
UA = "Mozilla/5.0 (mlb-tracker/grades)"

# --- fitted coefficients (vs Sean's example) ---
POWER_BASE = ([0.86064, 0.27265], 4.57515)                              # barrel, hardhit
POWER_BT   = ([1.22766, 0.28695, 13.61369], -0.75478)                   # barrel, hardhit, blast_per_swing
EYE_FULL   = ([-0.56281, 0.12522, -1.21835, 0.27193, -0.01273], 76.12834)  # oz_sw, z_sw, zone, swstr, chase_pctile
EYE_FB     = ([-0.51539, 0.13219, -1.16305, 0.24909], 71.34311)            # oz_sw, z_sw, zone, swstr

GRADE_LADDER = [(95,"A+"),(87,"A"),(80,"A-"),(72,"B+"),(64,"B"),(56,"B-"),
                (46,"C+"),(36,"C"),(26,"C-"),(17,"D+"),(8,"D"),(0,"D-")]
def grade(pct):
    if pct is None: return None
    for cut,l in GRADE_LADDER:
        if pct >= cut: return l
    return "D-"

SUF = re.compile(r" (jr|sr|iii|ii)\.?$")
def norm(s):
    if not s: return ""
    s = unicodedata.normalize("NFKD", str(s))
    s = "".join(c for c in s if not unicodedata.combining(c)).lower()
    s = SUF.sub("", s).replace(".", "").strip()
    return " ".join(s.split())
def sav_name(s):
    return norm((s.split(",")[1] + " " + s.split(",")[0])) if "," in str(s) else norm(s)

def fetch(u):
    return urllib.request.urlopen(urllib.request.Request(u, headers={"User-Agent": UA}), timeout=60).read().decode("utf-8-sig")
def lin(coefs, intercept, xs):
    return intercept + sum(c*x for c,x in zip(coefs, xs))
def rank_pct(vals):
    """percentile of score (higher value -> higher pct), among non-null."""
    arr = sorted(v for v in vals.values() if v is not None)
    n = len(arr); out = {}
    import bisect
    for k,v in vals.items():
        out[k] = None if v is None else round(100.0 * bisect.bisect_right(arr, v) / n, 1)
    return out

def main():
    import csv as _csv
    rows = {}  # norm -> dict of raw inputs/values

    # EV + discipline
    sel = "pa,barrel_batted_rate,hard_hit_percent,oz_swing_percent,z_swing_percent,in_zone_percent,whiff_percent,swing_percent"
    rdr = _csv.reader(io.StringIO(fetch(f"https://baseballsavant.mlb.com/leaderboard/custom?year={YEAR}&type=batter&filter=&min=10&selections={sel}&chart=false&x=pa&y=pa&r=no&csv=true")))
    data = list(rdr); hdr = [h.strip().replace("\n"," ").strip() for h in data[0]]
    ix = {h:i for i,h in enumerate(hdr)}; ncol = [h for h in hdr if "last_name" in h][0]
    def f(r,k):
        i = ix.get(k);
        try: return float(r[i]) if i is not None and r[i] not in ("","NA") else None
        except: return None
    for r in data[1:]:
        nm = r[ix[ncol]]; key = sav_name(nm)
        disp = (nm.split(",")[1].strip()+" "+nm.split(",")[0].strip()) if "," in nm else nm
        whiff, sw = f(r,"whiff_percent"), f(r,"swing_percent")
        rows[key] = {"name": disp, "barrel": f(r,"barrel_batted_rate"), "hardhit": f(r,"hard_hit_percent"),
                     "oz": f(r,"oz_swing_percent"), "zsw": f(r,"z_swing_percent"), "zone": f(r,"in_zone_percent"),
                     "swstr": (whiff*sw/100.0) if (whiff is not None and sw is not None) else None}

    # bat tracking (blast_per_swing)
    try:
        bt = list(_csv.reader(io.StringIO(fetch(f"https://baseballsavant.mlb.com/leaderboard/bat-tracking?minSwings=q&minGroupSwings=1&type=batter&year={YEAR}&csv=true"))))
        bh = {h.strip():i for i,h in enumerate(bt[0])}
        for r in bt[1:]:
            key = sav_name(r[bh["name"]])
            if key in rows:
                try: rows[key]["blast"] = float(r[bh["blast_per_swing"]])
                except: pass
    except Exception as e:
        print("bat-tracking pull failed:", e, file=sys.stderr)

    # percentile rankings: automated chase + K% percentiles
    try:
        pr = fetch(f"https://baseballsavant.mlb.com/leaderboard/percentile-rankings?type=batter&year={YEAR}")
        for p in json.loads(re.search(r"var leaderboard_data = (\[.*?\]);", pr, re.S).group(1)):
            key = sav_name(p.get("player_name",""))
            if key in rows:
                if p.get("percent_rank_chase_percent") not in (None,""): rows[key]["chase_p"] = float(p["percent_rank_chase_percent"])
                if p.get("percent_rank_k_percent") not in (None,""): rows[key]["k_p"] = float(p["percent_rank_k_percent"])
    except Exception as e:
        print("percentile-rankings pull failed:", e, file=sys.stderr)

    # season K% (fallback to fill Contact where Savant percentile is missing)
    kseason = {}
    try:
        H = json.load(open(DATA / "hitters.json")).get("hitters", {})
        for v in H.values():
            k = v.get("k_pct_actual") if v.get("k_pct_actual") is not None else v.get("k_pct")
            if isinstance(k, (int, float)): kseason[norm(v.get("name", ""))] = k
    except Exception as e:
        print("hitters.json read failed:", e, file=sys.stderr)
    # rank season K% -> percentile (lower K% = better = higher pct)
    kvals = sorted(kseason.values())
    import bisect as _bis
    def kpct_season(k):
        if k is None or not kvals: return None
        return round(100.0 * (1 - _bis.bisect_right(kvals, k) / len(kvals)) + 100.0/len(kvals), 1)

    # def/bsr (manual model export)
    fld_raw, bsr_raw = {}, {}
    try:
        for r in _csv.DictReader(open(DEF_BSR)):
            k = norm(r["name"])
            try: fld_raw[k] = float(r["fld"])
            except: pass
            try: bsr_raw[k] = float(r["bsr"])
            except: pass
    except Exception as e:
        print("def_bsr read failed:", e, file=sys.stderr)

    # compute raw Power / Eye / Contact
    for k,v in rows.items():
        b,h = v.get("barrel"), v.get("hardhit")
        if v.get("blast") is not None and b is not None and h is not None:
            v["power"] = round(lin(*POWER_BT, [b,h,v["blast"]]),2)
        elif b is not None and h is not None:
            v["power"] = round(lin(*POWER_BASE, [b,h]),2)
        else: v["power"] = None
        oz,zs,zn,ss,cp = v.get("oz"),v.get("zsw"),v.get("zone"),v.get("swstr"),v.get("chase_p")
        if None not in (oz,zs,zn,ss) and cp is not None:
            v["eye"] = round(lin(*EYE_FULL, [oz,zs,zn,ss,cp]),2)
        elif None not in (oz,zs,zn,ss):
            v["eye"] = round(lin(*EYE_FB, [oz,zs,zn,ss]),2)
        else: v["eye"] = None
        v["fld"] = fld_raw.get(k); v["bsr"] = bsr_raw.get(k)

    # percentiles
    p_pct = rank_pct({k:v["power"] for k,v in rows.items()})
    e_pct = rank_pct({k:v["eye"] for k,v in rows.items()})
    f_pct = rank_pct({k:v["fld"] for k,v in rows.items()})
    b_pct = rank_pct({k:v["bsr"] for k,v in rows.items()})

    out = {}
    for k,v in rows.items():
        rec = {"name": v["name"]}
        if v["power"] is not None: rec.update(power=v["power"], power_pct=p_pct[k], power_grade=grade(p_pct[k]))
        if v["eye"]   is not None: rec.update(eye=v["eye"], eye_pct=e_pct[k], eye_grade=grade(e_pct[k]))
        cpc = v.get("k_p")
        if cpc is None: cpc = kpct_season(kseason.get(k))
        if cpc is not None: rec.update(con_pct=round(cpc,1), con_grade=grade(cpc))
        if v["fld"]   is not None: rec.update(fld=v["fld"], fld_pct=f_pct[k], fld_grade=grade(f_pct[k]))
        if v["bsr"]   is not None: rec.update(bsr=v["bsr"], bsr_pct=b_pct[k], bsr_grade=grade(b_pct[k]))
        if len(rec) > 1: out[k] = rec

    payload = {"generated_at": datetime.datetime.utcnow().isoformat(timespec="seconds")+"Z",
               "n_players": len(out), "source": "Baseball Savant (EV + bat-tracking + plate discipline + percentiles); def/bsr from model",
               "by_name": out}
    OUT.write_text(json.dumps(payload, indent=2))
    have = lambda f: sum(1 for v in out.values() if f in v)
    print(f"wrote {len(out)} players -> {OUT}")
    print(f"  power_grade:{have('power_grade')} eye_grade:{have('eye_grade')} con_grade:{have('con_grade')} fld_grade:{have('fld_grade')} bsr_grade:{have('bsr_grade')}")

if __name__ == "__main__":
    main()
