"""finalize_awards.py: awards-week pass over data/player_futures.json (frozen end-of-season vote-fit board).

What it changes (Sean, 2026-11-09):
  1. Finalists. BBWAA announced the top three in points per award on 11/2. First-place share outside the
     three finalists is floored (0.1% each); they stay in the field for down-ballot points.
  2. Market weight. Final first-place share among the finalists is a linear blend:
     share = (1-W_MKT)*vote-fit first-place share + W_MKT*market P(win), W_MKT = 0.75 (old board: 0.25, Kalshi only).
     market P(win) = mean of Kalshi (yes bid/ask mid, *_dollars fields) and the sportsbook de-vig
     (data/player_futures_odds.json, BetMGM excluded), each normalized over the finalists. Linear, not a log-pool:
     the vote-fit gave Stewart 0.8% and Sanchez/Messick the lead, and a geometric pool lets one extreme input veto the other.
  3. P(win), not first-place share. The old board labelled first-place share as "win". Now 20k simulated
     elections: 30 Plackett-Luce ballots per election, each candidate's utility = lin + a common shock
     N(0, TAU) shared by all 30 voters (voters are correlated), plus per-ballot Gumbel noise. First place on each
     ballot comes from the blended finalist shares; places 2..k are drawn from the remaining field with weights =
     blended share (finalists) or the vote-fit share score, capped at 30% of the weakest finalist (non-finalists), so the down-ballot follows the model.
     p_win = share of elections won on points. TAU = 0.5 (a 2:1 first-place favourite wins about 80%).
     Also emitted: p_unanimous, first_p10 / first_p90 (10th and 90th percentile of first-place votes).
Outputs per candidate: proj_first, proj_points, proj_vote_share, p_win (model), p_win_mkt, kalshi_p, books_p,
pure_model_p (untouched vote-fit first-place share). Writes data/player_futures.json in place.
Run from repo root:  python3 scripts/finalize_awards.py
"""
import json, math, random, sys, urllib.request, datetime, unicodedata, pathlib

ROOT = pathlib.Path(__file__).resolve().parents[1]
PF = ROOT / "data" / "player_futures.json"
ODDS = ROOT / "data" / "player_futures_odds.json"
W_MKT = 0.75
W_MKT_BY = {"NL_ROY": 0.65}   # Sean 11/9: NL ROY anchored closer to the model (Benge) than the other five
ROY_POOL = ROOT / "data" / "awards" / "roy_pool_2026.json"   # full-rookie vote-fit share scores, down-ballot only
DOWN_T = {"ROY": 0.07}   # down-ballot temperature on the roy_pool share score (2x the first-place temp)
TAU = 0.5
N_SIM = 20000
VOTERS = 30
FLOOR = 0.001
BALLOT_PTS = {"MVP": [14, 9, 8, 7, 6, 5, 4, 3, 2, 1], "CY": [7, 4, 3, 2, 1], "ROY": [5, 3, 1]}
KALSHI = {"AL_MVP": "KXMLBALMVP", "NL_MVP": "KXMLBNLMVP", "AL_CY": "KXMLBALCY",
          "NL_CY": "KXMLBNLCY", "AL_ROY": "KXMLBALROTY", "NL_ROY": "KXMLBNLROTY"}
FINALISTS = {  # BBWAA, announced 2026-11-02
    "AL_MVP": ["Yordan Alvarez", "Junior Caminero", "Bobby Witt Jr."],
    "NL_MVP": ["Pete Crow-Armstrong", "Shohei Ohtani", "Brice Turang"],
    "AL_CY": ["Dylan Cease", "Parker Messick", "Cam Schlittler"],
    "NL_CY": ["Jacob Misiorowski", "Chris Sale", "Cristopher Sanchez"],
    "AL_ROY": ["Kevin McGonigle", "Parker Messick", "Payton Tolle"],
    "NL_ROY": ["Carson Benge", "Sal Stewart", "JJ Wetherholt"],
}
FIRST_OVERRIDE = {}   # optional manual first-place share, e.g. {"NL_CY": {"Jacob Misiorowski": 0.90}}; the rest of the finalists split the remainder pro rata
ALIAS = {"cameron schlittler": "cam schlittler", "robert witt": "bobby witt jr", "jonathan wetherholt": "jj wetherholt",
         "christopher sale": "chris sale", "peter alonso": "pete alonso", "toby rumfield": "tj rumfield"}


def norm(s):
    s = unicodedata.normalize("NFKD", s or "").encode("ascii", "ignore").decode().lower().replace(".", "").replace(",", "").strip()
    return ALIAS.get(s, s)


def amer(o):
    o = float(o); return 100 / (o + 100) if o > 0 else -o / (-o + 100)


def kalshi(series):
    try:
        d = json.load(urllib.request.urlopen(f"https://api.elections.kalshi.com/trade-api/v2/markets?series_ticker={series}&limit=500", timeout=20))
    except Exception as e:
        print("  kalshi fetch failed", series, e, file=sys.stderr); return {}
    out = {}
    for m in d.get("markets", []):
        b = float(m.get("yes_bid_dollars") or 0); a = float(m.get("yes_ask_dollars") or 0)
        mid = (b + a) / 2 if (b > 0 and 0 < a <= 1) else (b or a or 0)
        out[norm(m.get("yes_sub_title"))] = mid
    return out


def books(market):
    try:
        d = json.load(ODDS.read_text() and open(ODDS))
    except Exception:
        return {}
    out = {}
    for p in d.get("markets", {}).get(market, {}).get("players", []):
        o = p.get("all_book_odds") or {}
        o = {k: v for k, v in o.items() if k != "BetMGM"} or o   # BetMGM excluded (stale), fallback if it is the only book
        if o:
            out[norm(p["name"])] = sum(amer(v) for v in o.values()) / len(o)
    return out


def main():
    D = json.loads(PF.read_text())
    rng = random.Random(7)
    summary = {}
    for mk, M in D["markets"].items():
        award = mk.split("_")[1]
        fin = [norm(n) for n in FINALISTS[mk]]
        km, bm = kalshi(KALSHI[mk]), books(mk)
        cands = M["candidates"]
        wm = W_MKT_BY.get(mk, W_MKT)
        # ROY: bring in every rookie the vote-fit scores (the frozen board only carried its top 8-9)
        pool = (json.loads(ROY_POOL.read_text()).get(mk) if (award == "ROY" and ROY_POOL.exists()) else None) or []
        dsh = {norm(r["name"]): r for r in pool}
        have0 = {norm(c["name"]) for c in cands}
        for r in pool:
            if norm(r["name"]) not in have0 and norm(r["name"]) not in {norm(x) for x in FINALISTS[mk]}:
                cands.append({"name": r["name"], "team_abbr": r.get("team_abbr"), "league": mk[:2], "model_p": 0.0,
                              "pure_model_p": 0.0, "proj_share": None, "cats": [], "from_pool": True})
        # make sure every finalist is on the board
        have = {norm(c["name"]) for c in cands}
        for n in FINALISTS[mk]:
            if norm(n) not in have:
                cands.append({"name": n, "team_abbr": None, "league": mk[:2], "model_p": FLOOR, "pure_model_p": FLOOR, "cats": []})
        # market P(win) over finalists: mean of Kalshi-normalized and books-normalized
        def nz(src):
            v = {f: max(src.get(f, 0.0), 0.0) for f in fin}; t = sum(v.values())
            return {f: (x / t if t > 0 else None) for f, x in v.items()}
        kn, bn = nz(km), nz(bm)
        mkt = {}
        for f in fin:
            parts = [x for x in (kn[f], bn[f]) if x is not None]
            mkt[f] = sum(parts) / len(parts) if parts else 1 / 3
        t = sum(mkt.values()); mkt = {f: v / t for f, v in mkt.items()}
        # log-pool first-place utility
        for c in cands:
            n = norm(c["name"]); s = max(c.get("pure_model_p") or c.get("model_p") or FLOOR, 1e-6)
            c["kalshi_p"] = round(km.get(n), 4) if n in km else None
            c["books_p"] = round(bm.get(n), 4) if n in bm else None
            if n in fin:
                m = max(mkt[n], 1e-4)
                c["p_win_mkt"] = round(mkt[n], 4)
                c["_lin"] = math.log((1 - wm) * s + wm * m)
                c["finalist"] = True
            else:
                c["p_win_mkt"] = 0.0
                c["_lin"] = math.log(FLOOR) + 0.25 * math.log(s)   # out of the top three: no realistic first-place share, keep order for down-ballot
                c["finalist"] = False
        ov = {norm(k): v for k, v in FIRST_OVERRIDE.get(mk, {}).items()}
        if ov:
            fin_c = [c for c in cands if c["finalist"]]; rest = [c for c in fin_c if norm(c["name"]) not in ov]
            left = max(1 - sum(ov.values()), 1e-3); rz = sum(math.exp(c["_lin"]) for c in rest) or 1.0
            for c in fin_c:
                c["_lin"] = math.log(ov[norm(c["name"])] if norm(c["name"]) in ov else left * math.exp(c["_lin"]) / rz)
        z = sum(math.exp(c["_lin"]) for c in cands)
        for c in cands:
            c["model_p"] = c["display_p"] = round(math.exp(c["_lin"]) / z, 4)   # final first-place share
        # simulate elections
        pts = BALLOT_PTS[award]; n = len(cands); k = len(pts)
        lin = [c["_lin"] for c in cands]
        # down-ballot weights: finalists keep their blended share; non-finalists are ordered by the vote-fit share
        # score (proj_share, more resolution than first-place probability) and capped at 30% of the weakest finalist
        nonf = [c for c in cands if not c["finalist"]]
        top_sh = max((c.get("proj_share") or 0.0) for c in nonf) if nonf else 0.0
        min_fin = min(math.exp(c["_lin"]) for c in cands if c["finalist"])
        if dsh:   # ROY: down-ballot from the full-rookie vote-fit share (+ NPB/KBO veteran discount)
            tops = max((dsh[norm(c["name"])]["share"] for c in nonf if norm(c["name"]) in dsh), default=0.0)
            def _dw(c):
                r = dsh.get(norm(c["name"]))
                if r is None: return 1e-6
                return 0.3 * min_fin * math.exp((r["share"] - tops) / DOWN_T["ROY"] + (-1.5 if r.get("npb_vet") else 0.0))
            wdown = [math.exp(c["_lin"]) if c["finalist"] else _dw(c) for c in cands]
        else:
            wdown = [math.exp(c["_lin"]) if c["finalist"] else 0.3 * min_fin * math.exp(((c.get("proj_share") or -0.2) - top_sh) / 0.015) for c in cands]
        first = [0.0] * n; points = [0.0] * n; wins = [0.0] * n; unan = [0.0] * n; fdist = [[] for _ in range(n)]
        for _ in range(N_SIM):
            shock = [rng.gauss(0, TAU) for _ in range(n)]
            tot = [0.0] * n; f = [0] * n
            for _b in range(VOTERS):
                u = [lin[i] + shock[i] - math.log(-math.log(rng.random() or 1e-12)) for i in range(n)]
                top = max(range(n), key=lambda i: u[i])
                f[top] += 1; tot[top] += pts[0]
                rest = [i for i in range(n) if i != top]
                ud = [math.log(wdown[i]) + shock[i] - math.log(-math.log(rng.random() or 1e-12)) for i in rest]
                order = [rest[j] for j in sorted(range(len(rest)), key=lambda j: -ud[j])]
                for place, i in enumerate(order[:k - 1]):
                    tot[i] += pts[place + 1]
            best = max(tot); w = [i for i in range(n) if tot[i] == best]
            for i in w: wins[i] += 1 / len(w)
            for i in range(n):
                points[i] += tot[i]; first[i] += f[i]; fdist[i].append(f[i])
                if f[i] == VOTERS: unan[i] += 1
        for i, c in enumerate(cands):
            fd = sorted(fdist[i])
            c["proj_first"] = round(first[i] / N_SIM, 1)
            c["first_p10"] = fd[int(0.10 * N_SIM)]; c["first_p90"] = fd[int(0.90 * N_SIM)]
            if c["finalist"]:   # full distribution of 1st-place votes (share of sims with k votes, k = 0..30)
                h = [0] * (VOTERS + 1)
                for v in fdist[i]: h[v] += 1
                c["first_hist"] = [round(x / N_SIM, 5) for x in h]
            c["p_unanimous"] = round(unan[i] / N_SIM, 4)
            c["proj_points"] = round(points[i] / N_SIM, 1)
            c["proj_vote_share"] = round(points[i] / N_SIM / (VOTERS * pts[0]), 4)
            c["p_win"] = round(wins[i] / N_SIM, 4)
            c.pop("_lin", None)
        fins = [c for c in cands if c["finalist"]]
        base = [int(c["proj_first"]) for c in fins]; rem = VOTERS - sum(base)
        for i in sorted(range(len(fins)), key=lambda i: -(fins[i]["proj_first"] - base[i]))[:max(rem, 0)]: base[i] += 1
        for c, b in zip(fins, base): c["proj_first_r"] = b
        for c in cands:
            if not c["finalist"]: c["proj_first_r"] = 0
        cands.sort(key=lambda c: -c["proj_points"])
        for i, c in enumerate(cands): c["rank"] = i + 1
        M["finalists"] = FINALISTS[mk]; M["market_blend"] = {"w_mkt": wm, "tau": TAU, "kalshi": bool(km), "books": bool(bm)}
        summary[mk] = [(c["name"], c["proj_first"], c["proj_points"], c["proj_vote_share"], c["p_win"], c["p_win_mkt"], c.get("pure_model_p")) for c in cands[:4]]
    # MVP down-ballot pitchers follow the Cy Young board: the league's Cy Young winner cannot rank below another
    # pitcher on the MVP ballot. Insert him at the top pitcher's slot if the MVP field lacks him.
    for lg in ("AL", "NL"):
        cy = D["markets"][f"{lg}_CY"]["candidates"]; mvp = D["markets"][f"{lg}_MVP"]["candidates"]
        cy_order = [norm(c["name"]) for c in cy]
        pitchers = [c for c in mvp if norm(c["name"]) in cy_order and not c.get("finalist")]
        if not pitchers: continue
        slots = sorted([(c["proj_points"], c["proj_vote_share"]) for c in pitchers], reverse=True)
        names = sorted({norm(c["name"]) for c in pitchers} | {cy_order[0]}, key=cy_order.index)
        for i, nm in enumerate(names):
            c = next((x for x in mvp if norm(x["name"]) == nm), None)
            if c is None:
                src = next(x for x in cy if norm(x["name"]) == nm)
                c = {"name": src["name"], "team_abbr": src.get("team_abbr"), "league": lg, "finalist": False, "proj_first": 0.0,
                     "p_win": 0.0, "p_win_mkt": 0.0, "pure_model_p": 0.0, "model_p": 0.0, "display_p": 0.0, "cats": []}
                mvp.append(c)
            if i < len(slots): c["proj_points"], c["proj_vote_share"] = slots[i]
            else:  # displaced below the last pitcher slot: just under the next non-pitcher
                lower = [x["proj_points"] for x in mvp if x["proj_points"] < slots[-1][0] and norm(x["name"]) not in names]
                c["proj_points"] = round((lower[0] if lower else slots[-1][0] * 0.9) - 1, 1)
                c["proj_vote_share"] = round(c["proj_points"] / (VOTERS * BALLOT_PTS["MVP"][0]), 4)
        mvp.sort(key=lambda c: -c["proj_points"])
        for i, c in enumerate(mvp): c["rank"] = i + 1
    D["model"] = D["model"].split(" | FINAL")[0] + " | FINAL (frozen end-of-season stats; finalists 11/2; first-place share among finalists = 25% vote-fit model + 75% market (NL ROY 35/65) [Kalshi + books]; P(win) from 20k simulated 30-ballot elections, voter shock 0.5)"
    D["finalized_at"] = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    PF.write_text(json.dumps(D, indent=1))
    for mk, rows in summary.items():
        print(mk)
        for r in rows: print("   %-22s 1st %5.1f  pts %6.1f  share %5.1f%%  P(win) model %5.1f%%  market %5.1f%%  (pure vote-fit 1st share %s)" % (r[0], r[1], r[2], 100 * r[3], 100 * r[4], 100 * r[5], r[6]))


if __name__ == "__main__":
    main()
