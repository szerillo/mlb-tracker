#!/usr/bin/env python3
"""
build_grades_v2.py — AUTOMATED hitter-grade pipeline (Methodology v2.0).

Computes Damage / Eye / FLD / BSR / Contact live every run — NO CSV upload.
Sources: Baseball Savant (EV leaderboard, statcast_search zone/count harvest,
OAA, K% leaderboard) + committed baselines data/fld_bsr_proj.csv (projection
component). Writes data/grades_v2.json in the exact schema the frontend reads.

Grades are FROZEN-cutoff (longitudinally comparable): coefficients/cutoffs are
the v2.0 truth-anchored values; counts float year to year. See METHODOLOGY.md.
"""
import csv, io, json, sys, unicodedata, re, datetime, urllib.request, bisect, statistics
from pathlib import Path

HERE = Path(__file__).resolve().parent
DATA = HERE / ".." / "data"
OUT = DATA / "grades_v2.json"
FLD_PROJ = DATA / "fld_bsr_proj.csv"
YEAR = 2026
UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36"

# Frontend display ladder (a pct -> letter). MUST match gradeLetter() in index.html.
GRADE_LADDER = [(95,"A+"),(87,"A"),(80,"A-"),(72,"B+"),(64,"B"),(56,"B-"),
                (46,"C+"),(36,"C"),(26,"C-"),(17,"D+"),(8,"D"),(0,"D-")]
BANDS = {}
for _i,(_c,_l) in enumerate(GRADE_LADDER):
    BANDS[_l] = (_c, 100 if _i==0 else GRADE_LADDER[_i-1][0])
def grade_from_pct(p):
    for c,l in GRADE_LADDER:
        if p>=c: return l
    return "D-"

# FROZEN value cutoffs (>= threshold -> grade). Damage/Eye anchored to 2024-25
# Orr truth percentiles; FLD/BSR from the blended pool (recovered from v2.0 freeze).
DAMAGE_CUTS = [(36.4,"A+"),(32.4,"A"),(30.5,"A-"),(27.5,"B+"),(25.0,"B"),(22.2,"B-"),
               (18.7,"C+"),(15.4,"C"),(12.4,"C-"),(9.2,"D+"),(6.1,"D"),(-9e9,"D-")]
FLD_CUTS = [(13.7,"A+"),(10.0,"A"),(8.4,"A-"),(6.1,"B+"),(3.4,"B"),(1.3,"B-"),
            (-0.7,"C+"),(-3.0,"C"),(-5.7,"C-"),(-8.9,"D+"),(-11.6,"D"),(-9e9,"D-")]
BSR_CUTS = [(5.0,"A+"),(3.3,"A"),(2.6,"A-"),(1.7,"B+"),(0.7,"B"),(0.1,"B-"),
            (-0.5,"C+"),(-1.1,"C"),(-1.6,"C-"),(-2.2,"D+"),(-3.0,"D"),(-9e9,"D-")]
# Eye: graded by within-season composite percentile against the design ladder
# (rank-equivalent to quantile-mapping onto the frozen truth). Display value is
# the frozen SEAGER cutoff at each ladder percentile, interpolated.
EYE_LADDER = [(98,"A+",21.5),(94,"A",19.3),(89,"A-",17.1),(81,"B+",15.6),(70,"B",14.2),
              (57,"B-",12.9),(43,"C+",11.5),(30,"C",10.1),(19,"C-",8.7),(10,"D+",6.9),(4,"D",4.0),(0,"D-",2.0)]

def norm(s):
    s = str(s or "")
    if "," in s:                                   # Savant "Last, First" -> "First Last"
        a = s.split(",",1); s = a[1].strip()+" "+a[0].strip()
    s = unicodedata.normalize("NFKD", s).encode("ascii","ignore").decode()
    s = re.sub(r"\b(jr|sr|ii|iii|iv)\b","",s.lower())
    return re.sub(r"[^a-z ]","",s).strip()

def fetch(u, timeout=60):
    return urllib.request.urlopen(urllib.request.Request(u, headers={"User-Agent":UA}),
                                  timeout=timeout).read().decode("utf-8-sig","ignore")
def _f(x):
    try: return float(x)
    except Exception: return None
def by_cuts(cuts, v):
    for c,l in cuts:
        if v>=c: return l
    return "D-"

# ---------------------------------------------------------------- DAMAGE
def compute_damage():
    url=(f"https://baseballsavant.mlb.com/leaderboard/statcast?type=batter&year={YEAR}"
         f"&min=50&csv=true")
    rows=list(csv.DictReader(io.StringIO(fetch(url))))
    nk=[k for k in rows[0] if "last_name" in k][0]
    out={}
    for r in rows:
        bbe=_f(r.get("attempts")); brl=_f(r.get("brl_percent")); ev95=_f(r.get("ev95percent")); air=_f(r.get("fbld"))
        if None in (bbe,brl,ev95,air): continue
        raw=-108.825 + 0.939*brl + 0.120*ev95 + 1.256*air
        shrunk=(bbe*raw + 68*20.3)/(bbe+68)
        out[norm(r[nk])]={"name":r[nk],"power":round(shrunk,1),"power_grade":by_cuts(DAMAGE_CUTS,shrunk)}
    return out

# ---------------------------------------------------------------- EYE
def compute_eye():
    base=("https://baseballsavant.mlb.com/statcast_search/csv?all=true&hfGT=R%7C&hfSea="
          f"{YEAR}%7C&player_type=batter&group_by=name&min_pitches=1&sort_col=pitches&sort_order=desc")
    def harv(extra):
        d=fetch(base+extra, 45)
        return {r["player_id"]:(int(r["pitches"]),int(r["takes"]),r["player_name"]) for r in csv.DictReader(io.StringIO(d))}
    Z=lambda c:"&hfNewZones="+"".join(f"{x}%7C" for x in c)
    C=lambda c:"&hfC="+"".join(f"{x}%7C" for x in c)
    tot=harv("")
    chase=harv(Z([21,22,23,24,26,27,28,29])); waste=harv(Z([31,32,33,34,36,37,38,39]))
    sh2k=harv(Z([11,12,13,14,16,17,18,19])+C(["02","12","22","32"]))
    heart=harv(Z([1,2,3,4,5,6,7,8,9])+C(["10","20","21","30","31","00","11"]))
    comp={}
    for pid,(tp,tt,nm) in tot.items():
        if tp<600: continue
        cp=chase.get(pid,(0,0))[0]+waste.get(pid,(0,0))[0]; ctk=chase.get(pid,(0,0))[1]+waste.get(pid,(0,0))[1]
        sp,stk=sh2k.get(pid,(0,0))[:2]; hp,htk=heart.get(pid,(0,0))[:2]
        if cp<150 or sp<40 or hp<100: continue
        comp[pid]={"name":nm,"ct":ctk/cp,"ed":stk/sp,"ha":1-htk/hp}
    if not comp: return {}
    for key in ("ct","ed","ha"):
        vals=[v[key] for v in comp.values()]; m=statistics.mean(vals); sd=statistics.pstdev(vals) or 1
        for v in comp.values(): v["z"+key]=(v[key]-m)/sd
    for v in comp.values(): v["comp"]=0.45*v["zct"]+0.30*v["zed"]+0.25*v["zha"]
    ranked=sorted(comp.values(),key=lambda v:v["comp"]); n=len(ranked)
    def eye_val(pct):     # interpolate SEAGER display value between ladder anchors
        pts=sorted([(p,s) for p,_,s in EYE_LADDER])
        for i in range(len(pts)-1):
            (p0,s0),(p1,s1)=pts[i],pts[i+1]
            if p0<=pct<=p1: return round(s0+(s1-s0)*(pct-p0)/(p1-p0 or 1),1)
        return pts[-1][1] if pct>pts[-1][0] else pts[0][1]
    out={}
    for i,v in enumerate(ranked):
        pct=100.0*(i+0.5)/n
        g=next(gr for c,gr,_ in EYE_LADDER if pct>=c)
        out[norm(v["name"])]={"eye":eye_val(pct),"eye_grade":g}
    return out

# ---------------------------------------------------------------- FLD / BSR
def _season_games():
    try:
        d=json.loads(fetch("https://statsapi.mlb.com/api/v1/standings?leagueId=103,104&season="+str(YEAR)+"&standingsTypes=regularSeason",20))
        g=[t.get("gamesPlayed") for rec in d.get("records",[]) for t in rec.get("teamRecords",[]) if t.get("gamesPlayed")]
        return statistics.median(g) if g else 100
    except Exception: return 100

def compute_fld_bsr():
    proj={}
    if FLD_PROJ.exists():
        for r in csv.DictReader(open(FLD_PROJ)):
            proj[r["name_key"]]={"name":r["name"],"fld":_f(r["fld_proj_pg"]),"bsr":_f(r["bsr_proj_pg"])}
    games=max(30,_season_games())
    # SABR Defensive Index (SDI) observed -> /150. Multi-system composite (the
    # Gold-Glove metric); rates ALL positions incl. catchers. Auto-discovers the
    # latest published date. No positional adjustment (SDI is position-contextual).
    oaa={}
    try:
        import html as _html
        land=fetch("https://sabr.org/sdi/", 25)
        dates=sorted(set(re.findall(r"/sdi/(\d{4}-\d{2}-\d{2})", land)))
        latest=dates[-1] if dates else "2026-07-12"
        page=fetch(f"https://sabr.org/sdi/{latest}", 25)
        rows=re.findall(r"<tr>\s*<td>(.*?)</td>\s*<td>(.*?)</td>\s*<td>(.*?)</td>\s*<td>(.*?)</td>\s*</tr>", page, re.S)
        for nm,tm,pos,val in rows:
            if pos.strip()=="P": continue                 # exclude pitchers
            v=_f(val)
            if v is None: continue
            k=norm(_html.unescape(nm))
            oaa[k]=oaa.get(k,0.0)+v*150.0/games           # sum multi-position, season-to-date -> /150
        print(f"[grades_v2] SDI {latest}: {len(oaa)} position players", file=sys.stderr)
    except Exception as e:
        print(f"[grades_v2] SDI pull failed ({e})", file=sys.stderr)
    # Baserunning observed (runner_runs) -> /150 (via a nominal 650 PA season already? keep proj-led)
    brun={}
    try:
        for r in csv.DictReader(io.StringIO(fetch(f"https://baseballsavant.mlb.com/leaderboard/baserunning?type=runner&year={YEAR}&min=1&csv=true"))):
            rr=_f(r.get("runner_runs"))
            if rr is None: continue
            brun[norm(r.get("entity_name",""))]=rr*150.0/games
    except Exception as e:
        print(f"[grades_v2] baserunning pull failed ({e})", file=sys.stderr)
    out={}
    for k,p in proj.items():
        fld_proj150 = (p["fld"] or 0)*150.0
        bsr_proj150 = (p["bsr"] or 0)*150.0
        # FLD: 0.75 proj + 0.25 obs (OAA); obs weight 0 if no OAA (catcher / unqualified)
        o=oaa.get(k)
        fld = 0.75*fld_proj150 + 0.25*o if o is not None else fld_proj150
        # BSR: 0.65 proj + 0.35 obs (Savant runner runs)
        b=brun.get(k)
        bsr = 0.65*bsr_proj150 + 0.35*b if b is not None else bsr_proj150
        rec={"name":p["name"]}
        if p["fld"] is not None: rec["fld"]=round(fld,1); rec["fld_grade"]=by_cuts(FLD_CUTS,fld)
        if p["bsr"] is not None: rec["bsr"]=round(bsr,1); rec["bsr_grade"]=by_cuts(BSR_CUTS,bsr)
        out[k]=rec
    return out

# ---------------------------------------------------------------- CONTACT (CoE: pitch-level contact-over-expected)
# Methodology v2.0 sec.4. Pulls the full season of pitch-level Statcast, fits a
# league contact surface P(contact|swing) over normalized location x pitch-class
# x batter-side, then scores each hitter's actual-minus-expected contact rate
# (shrunk). Validated vs Orr CoE truth: Kwan +10.8 (truth +11.4), Yordan +5.5
# (+5.1), Arraez top, Gallo/Schwarber/Judge bottom.
_CONTACT_DESC={"hit_into_play","foul","foul_tip","foul_bunt"}
_WHIFF_DESC={"swinging_strike","swinging_strike_blocked","missed_bunt","swinging_pitchout"}
_FB={"FF","FA","FT","SI","FC"}; _BR={"SL","CU","KC","SV","ST","CS","SC","KN"}; _OS={"CH","FS","FO","EP"}
_XED=[0.5,1.0,1.5,2.0]; _ZED=[0.5,1.0,1.5,2.0]
def _pclass(pt):
    if pt in _FB: return "FB"
    if pt in _BR: return "BR"
    if pt in _OS: return "OS"
    return None
def _lbin(v,ed):
    for i,e in enumerate(ed):
        if v<e: return i
    return len(ed)
def load_contact(min_sw=150, k=120.0, pseudo=50.0):
    """Return {norm_name: percentile(1-99)} from the pitch-level CoE model."""
    con={}
    try:
        start=datetime.date(YEAR,3,15); today=datetime.date.today()
        cellC={}; cellN={}; margC={}; margN={}; rows=[]; id2name={}
        d=start
        while d<=today:
            d2=min(d+datetime.timedelta(days=3), today)
            url=("https://baseballsavant.mlb.com/statcast_search/csv?all=true&type=details"
                 f"&player_type=batter&hfSea={YEAR}%7C&game_date_gt={d.isoformat()}"
                 f"&game_date_lt={d2.isoformat()}&min_pitches=0")
            d=d2+datetime.timedelta(days=1)
            try:
                data=list(csv.reader(io.StringIO(fetch(url, timeout=120))))
            except Exception as e:
                print(f"[grades_v2] contact window {url[-30:]} failed ({e})", file=sys.stderr); continue
            if not data or len(data)<2: continue
            hdr=[h.strip() for h in data[0]]; ix={h:i for i,h in enumerate(hdr)}
            need=("description","pitch_type","plate_x","plate_z","sz_top","sz_bot","stand","batter","player_name")
            if any(c not in ix for c in need): continue
            for r in data[1:]:
                try:
                    dsc=r[ix["description"]]
                    if dsc in _CONTACT_DESC: c=1
                    elif dsc in _WHIFF_DESC: c=0
                    else: continue
                    pc=_pclass(r[ix["pitch_type"]])
                    if not pc: continue
                    px=float(r[ix["plate_x"]]); pz=float(r[ix["plate_z"]])
                    st=float(r[ix["sz_top"]]); sb=float(r[ix["sz_bot"]])
                    half=(st-sb)/2.0
                    if half<=0: continue
                    side=r[ix["stand"]]; pid=r[ix["batter"]]
                    if side not in ("R","L") or not pid: continue
                except Exception:
                    continue
                xb=_lbin(abs(px)/0.83,_XED); zb=_lbin(abs(pz-(st+sb)/2.0)/half,_ZED)
                key=(side,pc,xb,zb); mk=(side,pc)
                cellC[key]=cellC.get(key,0)+c; cellN[key]=cellN.get(key,0)+1
                margC[mk]=margC.get(mk,0)+c; margN[mk]=margN.get(mk,0)+1
                rows.append((pid,side,pc,xb,zb,c))
                if pid not in id2name: id2name[pid]=r[ix["player_name"]]
        if not rows:
            print("[grades_v2] contact: no pitch rows pulled", file=sys.stderr); return con
        prob={}
        for key in cellN:
            side,pc,xb,zb=key; mrate=margC[(side,pc)]/margN[(side,pc)]
            prob[key]=(cellC[key]+pseudo*mrate)/(cellN[key]+pseudo)
        pl={}
        for pid,side,pc,xb,zb,c in rows:
            a=pl.setdefault(pid,[0,0,0.0]); a[0]+=c; a[1]+=1; a[2]+=prob[(side,pc,xb,zb)]
        recs=[(pid, 100.0*(C-E)/(N+k)) for pid,(C,N,E) in pl.items() if N>=min_sw]
        if not recs:
            print("[grades_v2] contact: no players >= min_sw", file=sys.stderr); return con
        # Bell-shaped design ladder (same allocation as Eye): rank CoE, map to
        # letter via design percentiles, emit the band-midpoint pct so the
        # frontend's grade_from_pct recovers the intended letter.
        DESIGN=[(98,"A+"),(94,"A"),(89,"A-"),(81,"B+"),(70,"B"),(57,"B-"),
                (43,"C+"),(30,"C"),(19,"C-"),(10,"D+"),(4,"D"),(0,"D-")]
        vals=sorted(v for _,v in recs); n=len(vals)
        for pid,coe in recs:
            p=100.0*bisect.bisect_right(vals,coe)/n
            letter=next(l for c,l in DESIGN if p>=c)
            lo,hi=BANDS[letter]
            con[norm(id2name.get(pid,pid))]=round((lo+hi)/2.0,1)
        print(f"[grades_v2] contact CoE: {len(con)} hitters ({len(rows)} swings)", file=sys.stderr)
    except Exception as e:
        print(f"[grades_v2] contact CoE failed ({e})", file=sys.stderr)
    return con

# ---------------------------------------------------------------- synthetic pcts
def synth(items):
    """items: list of (key, value, grade). Return {key: pct} inside the letter band, ordered by value."""
    byg={}
    for k,v,g in items:
        if g in BANDS and v is not None: byg.setdefault(g,[]).append((v,k))
    out={}
    for g,arr in byg.items():
        lo,hi=BANDS[g]; arr.sort(); n=len(arr)
        for rank,(_v,k) in enumerate(arr):
            out[k]=round(min(hi-0.01,max(lo, lo+(rank+0.5)/n*(hi-lo))),2)
    return out

def main():
    dmg=compute_damage();  print(f"[grades_v2] Damage: {len(dmg)}", file=sys.stderr)
    eye=compute_eye();     print(f"[grades_v2] Eye: {len(eye)}", file=sys.stderr)
    fb=compute_fld_bsr();  print(f"[grades_v2] FLD/BSR: {len(fb)}", file=sys.stderr)
    con=load_contact();    print(f"[grades_v2] Contact: {len(con)}", file=sys.stderr)

    keys=set(dmg)|set(eye)|set(fb)|set(con)
    # synthetic pcts per metric
    p_pow=synth([(k,dmg[k]["power"],dmg[k]["power_grade"]) for k in dmg])
    p_eye=synth([(k,eye[k]["eye"],eye[k]["eye_grade"]) for k in eye])
    p_fld=synth([(k,fb[k]["fld"],fb[k]["fld_grade"]) for k in fb if "fld" in fb[k]])
    p_bsr=synth([(k,fb[k]["bsr"],fb[k]["bsr_grade"]) for k in fb if "bsr" in fb[k]])

    out={}
    for k in keys:
        rec={"name": (dmg.get(k) or eye.get(k) or fb.get(k) or {}).get("name","")}
        if k in dmg and k in p_pow: rec.update(power=dmg[k]["power"], power_pct=p_pow[k], power_grade=dmg[k]["power_grade"])
        if k in eye and k in p_eye: rec.update(eye=eye[k]["eye"], eye_pct=p_eye[k], eye_grade=eye[k]["eye_grade"])
        if k in fb and k in p_fld:  rec.update(fld=fb[k]["fld"], fld_pct=p_fld[k], fld_grade=fb[k]["fld_grade"])
        if k in fb and k in p_bsr:  rec.update(bsr=fb[k]["bsr"], bsr_pct=p_bsr[k], bsr_grade=fb[k]["bsr_grade"])
        if k in con: rec.update(con_pct=con[k], con_grade=grade_from_pct(con[k]))
        if len(rec)>1: out[k]=rec

    payload={"generated_at":datetime.datetime.utcnow().isoformat(timespec="seconds")+"Z",
             "n_players":len(out),
             "source":"AUTO v2.0: Damage(Savant EV+shrink+frozen cutoffs), Eye(Savant zone/count composite -> ladder), FLD/BSR(proj baseline + SABR SDI (fielding) + Savant runner_runs (baserunning)), Contact(pitch-level CoE: contact-over-expected).",
             "methodology_version":"2.0-auto","by_name":out}
    OUT.write_text(json.dumps(payload,indent=2))
    have=lambda f: sum(1 for v in out.values() if f in v)
    print(f"[grades_v2] wrote {len(out)} players -> power:{have('power_grade')} eye:{have('eye_grade')} "
          f"fld:{have('fld_grade')} bsr:{have('bsr_grade')} con:{have('con_grade')}", file=sys.stderr)
    return 0

if __name__=="__main__":
    sys.exit(main())
