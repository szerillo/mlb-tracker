#!/usr/bin/env python3
"""
Build data/grades_v2.json — the single hitter-grade source the frontend reads.

METHODOLOGY v2.0 (Sean's Orr-equivalent pipeline). Power/Eye/FLD/BSR are NOT
computed here anymore — they come straight from data/hitter_grades_v2.csv, which
Sean regenerates with the full v2.0 reproduction (Savant EV -> Damage w/ shrinkage
and FROZEN Orr-truth cutoffs; SEAGER-scaled Eye quantile-mapped to Orr truth;
FLD/BSR projection+FanGraphs blend). Those grades are ABSOLUTE (frozen cutoffs),
so they must NOT be re-ranked against the current pool — doing so mis-graded ~55%
of hitters by >=2 tiers in testing.

The frontend turns a percentile into a letter via a fixed ladder
(95=A+,87=A,80=A-,72=B+,64=B,56=B-,46=C+,36=C,26=C-,17=D+,8=D,else D-). To make it
display the CSV's ABSOLUTE letter unchanged, we emit a synthetic *_pct that lands
inside that letter's ladder band (spread within-band by the raw value so ordering
and color intensity are preserved). gradeLetter(synthetic_pct) == the CSV grade,
verified 100% across all four metrics, with zero frontend change.

CONTACT is the one grade the v2.0 CSV omits ("SPEC, NOT YET RUN"). We keep the
tool's existing Contact = Savant K% percentile (lower K% = better), sourced live.

Reads : data/hitter_grades_v2.csv  (Sean's v2.0 output; committed, re-uploaded on refresh)
        Savant custom leaderboard K%  +  data/hitters.json (season K% fallback)
Writes: data/grades_v2.json  (by_name -> power/eye/con/fld/bsr value+pct+grade)
"""
import csv, io, json, re, sys, unicodedata, datetime, urllib.request, bisect
from pathlib import Path

HERE = Path(__file__).resolve().parent
DATA = HERE / ".." / "data"
CSV_IN = DATA / "hitter_grades_v2.csv"
OUT = DATA / "grades_v2.json"
YEAR = 2026
UA = "Mozilla/5.0 (mlb-tracker/grades)"

# Must match the frontend gradeLetter() ladder EXACTLY.
GRADE_LADDER = [(95,"A+"),(87,"A"),(80,"A-"),(72,"B+"),(64,"B"),(56,"B-"),
                (46,"C+"),(36,"C"),(26,"C-"),(17,"D+"),(8,"D"),(0,"D-")]
BANDS = {}
for _i,(_cut,_l) in enumerate(GRADE_LADDER):
    BANDS[_l] = (_cut, 100 if _i == 0 else GRADE_LADDER[_i-1][0])

def grade_from_pct(p):
    if p is None: return None
    for cut,l in GRADE_LADDER:
        if p >= cut: return l
    return "D-"

def norm(s):
    s = unicodedata.normalize("NFD", s or "")
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    s = re.sub(r"\b(jr|sr|ii|iii|iv|v)\.?$", "", s.strip(), flags=re.I)
    return re.sub(r"\s+", " ", s).replace(".", "").lower().strip()

def fetch(u):
    return urllib.request.urlopen(urllib.request.Request(u, headers={"User-Agent": UA}),
                                  timeout=60).read().decode("utf-8-sig")

def _fnum(x):
    try: return float(x)
    except Exception: return None


def synthetic_pcts(rows, value_key, grade_key):
    """Per absolute grade, spread players across that grade's ladder band by
    value so gradeLetter(pct) reproduces the grade AND within-band order holds."""
    byg = {}
    for i, r in enumerate(rows):
        g = (r.get(grade_key) or "").strip()
        v = _fnum(r.get(value_key))
        if not g or v is None or g not in BANDS:
            continue
        byg.setdefault(g, []).append((v, i))
    out = {}
    for g, items in byg.items():
        lo, hi = BANDS[g]
        items.sort()                      # ascending value
        n = len(items)
        for rank, (_v, i) in enumerate(items):
            p = lo + (rank + 0.5) / n * (hi - lo)
            out[i] = round(min(hi - 0.01, max(lo, p)), 2)
    return out


def load_contact():
    """Contact percentile from Savant K% (lower K% -> better -> higher pct).
    Primary: custom leaderboard K% ranked across the pool; fallback: hitters.json
    season K%. Keyed by normalized name."""
    con = {}
    try:
        sel = "pa,k_percent"
        url = (f"https://baseballsavant.mlb.com/leaderboard/custom?year={YEAR}"
               f"&type=batter&filter=&min=10&selections={sel}&chart=false&x=pa&y=pa&r=no&csv=true")
        data = list(csv.reader(io.StringIO(fetch(url))))
        hdr = [h.strip().replace("\n", " ").strip() for h in data[0]]
        ix = {h: i for i, h in enumerate(hdr)}
        ncol = [h for h in hdr if "last_name" in h][0]
        kvals, tmp = [], {}
        for r in data[1:]:
            k = _fnum(r[ix["k_percent"]]) if "k_percent" in ix else None
            if k is None: continue
            tmp[norm(r[ix[ncol]])] = k
            kvals.append(k)
        kvals.sort()
        n = len(kvals)
        for nm, k in tmp.items():
            # lower K% -> higher percentile
            pct = 100.0 * (1 - bisect.bisect_right(kvals, k) / n) + 100.0 / n
            con[nm] = round(max(1.0, min(99.0, pct)), 1)
    except Exception as e:
        print(f"[grades_v2] Savant K% pull failed ({e}); contact from hitters.json only",
              file=sys.stderr)
    # season K% fallback for anyone the leaderboard missed
    try:
        H = json.load(open(DATA / "hitters.json")).get("hitters", {})
        season = {}
        for v in H.values():
            k = v.get("k_pct_actual") if v.get("k_pct_actual") is not None else v.get("k_pct")
            if isinstance(k, (int, float)):
                season[norm(v.get("name", ""))] = k
        svals = sorted(season.values())
        m = len(svals)
        for nm, k in season.items():
            if nm in con or not m: continue
            pct = 100.0 * (1 - bisect.bisect_right(svals, k) / m) + 100.0 / m
            con[nm] = round(max(1.0, min(99.0, pct)), 1)
    except Exception as e:
        print(f"[grades_v2] hitters.json K% fallback failed ({e})", file=sys.stderr)
    return con


def main():
    if not CSV_IN.exists():
        print(f"[grades_v2] no {CSV_IN}; nothing to build", file=sys.stderr)
        return 1
    rows = list(csv.DictReader(open(CSV_IN)))
    print(f"[grades_v2] read {len(rows)} hitters from {CSV_IN.name}", file=sys.stderr)

    METRICS = [  # (value_col, grade_col, out_val, out_pct, out_grade)
        ("Damage",  "Damage_Grade", "power", "power_pct", "power_grade"),
        ("Eye",     "Eye_Grade",    "eye",   "eye_pct",   "eye_grade"),
        ("FLD_150", "FLD_Grade",    "fld",   "fld_pct",   "fld_grade"),
        ("BSR_150", "BSR_Grade",    "bsr",   "bsr_pct",   "bsr_grade"),
    ]
    pcts = {vk: synthetic_pcts(rows, vk, gk) for vk, gk, *_ in METRICS}

    con = load_contact()

    out = {}
    for i, r in enumerate(rows):
        nm = (r.get("Player") or "").strip()
        if not nm: continue
        key = norm(nm)
        rec = {"name": nm}
        for vk, gk, ov, op, og in METRICS:
            v = _fnum(r.get(vk)); g = (r.get(gk) or "").strip()
            p = pcts[vk].get(i)
            if v is None or not g or p is None:
                continue
            rec[ov] = round(v, 2)
            rec[op] = p
            rec[og] = g
        # Contact (not in the v2.0 CSV) — keep the tool's Savant K% grade
        cp = con.get(key)
        if cp is not None:
            rec["con_pct"] = cp
            rec["con_grade"] = grade_from_pct(cp)
        if len(rec) > 1:
            out[key] = rec

    payload = {
        "generated_at": datetime.datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "n_players": len(out),
        "source": ("Hitter Grades METHODOLOGY v2.0 (Orr-equivalent): Power=Damage "
                   "(Savant EV, shrunk, FROZEN Orr cutoffs); Eye=SEAGER-scaled; "
                   "FLD/BSR=proj+FanGraphs blend. From data/hitter_grades_v2.csv. "
                   "Contact=Savant K% percentile (v2.0 Contact not yet run)."),
        "methodology_version": "2.0",
        "by_name": out,
    }
    OUT.write_text(json.dumps(payload, indent=2))
    have = lambda f: sum(1 for v in out.values() if f in v)
    print(f"[grades_v2] wrote {len(out)} players -> {OUT}", file=sys.stderr)
    print(f"[grades_v2]   power:{have('power_grade')} eye:{have('eye_grade')} "
          f"con:{have('con_grade')} fld:{have('fld_grade')} bsr:{have('bsr_grade')}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
