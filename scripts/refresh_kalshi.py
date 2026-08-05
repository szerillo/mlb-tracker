#!/usr/bin/env python3
"""Live Kalshi MLB prediction-market prices -> data/kalshi.json (keyed by gamePk).

Public GetMarkets endpoint (no auth). Series:
  KXMLBGAME      game winner   (one market per side, yes = that team wins)
  KXMLBTOTAL     game total    (one market per strike, yes = over X.5)
  KXMLBTEAMTOTAL team total    (one market per team+strike, yes = team over X.5)

Prices come back in dollars (0-1); we store cents (0-100). We record yes bid/ask
and a mid for each side/strike, keyed by our MLB gamePk so the scoreboard can join
directly. Fair-cents pricing (our model vs these market cents) is done in the
frontend, which already has the projection + probability functions.
"""
import json
import os
import sys
import time
import urllib.request

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SHEET = os.path.join(REPO_ROOT, "data", "sheet_projections.json")
OUT = os.path.join(REPO_ROOT, "data", "kalshi.json")
BASE = "https://api.elections.kalshi.com/trade-api/v2/markets"
UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36")
MONTHS = {"JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
          "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12}

# Canonical team codes. Tokens match either our full names or Kalshi's short
# forms (which abbreviate the multi-team cities: "Chicago WS", "Los Angeles D").
CANON_TEAMS = [
    ("ARI", ["arizona", "diamondbacks"]),
    ("ATL", ["atlanta", "braves"]),
    ("ATH", ["athletics", "oakland", "sacramento"]),
    ("BAL", ["baltimore", "orioles"]),
    ("BOS", ["boston", "red sox"]),
    ("CHC", ["cubs", "chicago c"]),
    ("CWS", ["white sox", "chicago w"]),
    ("CIN", ["cincinnati", "reds"]),
    ("CLE", ["cleveland", "guardians"]),
    ("COL", ["colorado", "rockies"]),
    ("DET", ["detroit", "tigers"]),
    ("HOU", ["houston", "astros"]),
    ("KC", ["kansas", "royals"]),
    ("LAA", ["angels", "los angeles a"]),
    ("LAD", ["dodgers", "los angeles d"]),
    ("MIA", ["miami", "marlins"]),
    ("MIL", ["milwaukee", "brewers"]),
    ("MIN", ["minnesota", "twins"]),
    ("NYM", ["mets", "new york m"]),
    ("NYY", ["yankees", "new york y"]),
    ("PHI", ["philadelphia", "phillies"]),
    ("PIT", ["pittsburgh", "pirates"]),
    ("SD", ["san diego", "padres"]),
    ("SF", ["san francisco", "giants"]),
    ("SEA", ["seattle", "mariners"]),
    ("STL", ["st louis", "cardinals"]),
    ("TB", ["tampa", "rays"]),
    ("TEX", ["texas", "rangers"]),
    ("TOR", ["toronto", "blue jays"]),
    ("WSH", ["washington", "nationals"]),
]


def canon(name):
    """Map any team display form (full or Kalshi short) to a canonical code."""
    n = (name or "").lower().replace(".", "").replace("'", "")
    if n.strip() == "as":  # Kalshi renders the Athletics as "A's"
        return "ATH"
    for code, toks in CANON_TEAMS:
        for t in toks:
            if t in n:
                return code
    return None


def _get(url, tries=3):
    last = None
    for _ in range(tries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": "application/json"})
            with urllib.request.urlopen(req, timeout=30) as r:
                return json.loads(r.read().decode("utf-8", errors="replace"))
        except Exception as e:
            last = e
            time.sleep(1.0)
    raise last


def fetch_series(series):
    out = []
    cursor = ""
    for _ in range(25):
        url = BASE + "?series_ticker=%s&status=open&limit=1000" % series
        if cursor:
            url += "&cursor=%s" % cursor
        try:
            d = _get(url)
        except Exception as e:
            print("[kalshi] %s fetch failed: %s" % (series, e), file=sys.stderr)
            break
        out.extend(d.get("markets", []) or [])
        cursor = d.get("cursor") or ""
        if not cursor:
            break
        time.sleep(0.2)
    return out


def _cents(v):
    try:
        return round(float(v) * 100)
    except Exception:
        return None


def _mid(bid, ask):
    b, a = _cents(bid), _cents(ask)
    if b is None and a is None:
        return None
    if b is None:
        return a
    if a is None:
        return b
    return round((b + a) / 2.0)


def _suffix(event_ticker):
    """KXMLBTOTAL-26AUG051435SFTEX -> '26AUG051435SFTEX' (shared across series)."""
    parts = (event_ticker or "").split("-", 1)
    return parts[1] if len(parts) == 2 else None


def _date_from_suffix(suf):
    suf = suf or ""
    if len(suf) < 7:
        return None
    yy, mon, dd = suf[0:2], suf[2:5], suf[5:7]
    if mon not in MONTHS or not yy.isdigit() or not dd.isdigit():
        return None
    return "20%s-%02d-%s" % (yy, MONTHS[mon], dd)


def _fp(v):
    try:
        return float(v)
    except Exception:
        return None


def main():
    with open(SHEET) as f:
        sheet = json.load(f)
    slate_date = sheet.get("date")
    games = sheet.get("games") or {}
    # canonical (away_code, home_code) per gamePk, for matching
    ours = []
    for pk, g in games.items():
        ca, ch = canon(g.get("away_team")), canon(g.get("home_team"))
        if ca and ch:
            ours.append((pk, ca, ch))

    def match_pk(away, home, date):
        ca, ch = canon(away), canon(home)
        if not ca or not ch:
            return None
        for pk, oa, oh in ours:
            if oa == ca and oh == ch:
                return pk
        return None

    # 1) game winner -> suffix -> gamePk, and ML cents
    winners = fetch_series("KXMLBGAME")
    suf2pk = {}
    out = {}
    by_event = {}
    for m in winners:
        by_event.setdefault(m.get("event_ticker"), []).append(m)
    for ev, mks in by_event.items():
        suf = _suffix(ev)
        title = (mks[0].get("title") or "")
        base = title.split(" Winner?")[0]
        if " vs " not in base:
            continue
        away, home = [s.strip() for s in base.split(" vs ", 1)]
        pk = match_pk(away, home, _date_from_suffix(suf))
        if not pk:
            continue
        suf2pk[suf] = pk
        ml = {}
        for m in mks:
            side = (m.get("yes_sub_title") or "").strip()
            entry = {"team": side,
                     "bid": _cents(m.get("yes_bid_dollars")),
                     "ask": _cents(m.get("yes_ask_dollars")),
                     "mid": _mid(m.get("yes_bid_dollars"), m.get("yes_ask_dollars")),
                     "vol": _fp(m.get("volume_fp"))}
            cs = canon(side)
            key = None
            if cs and cs == canon(away):
                key = "away"
            elif cs and cs == canon(home):
                key = "home"
            if key:
                ml[key] = entry
        out[pk] = {"kalshi_event": ev, "ml": ml, "total": [], "team_total": {"away": [], "home": []}}

    # 2) game totals -> strikes
    for m in fetch_series("KXMLBTOTAL"):
        pk = suf2pk.get(_suffix(m.get("event_ticker")))
        if not pk or pk not in out:
            continue
        fs = _fp(m.get("floor_strike"))
        if fs is None:
            continue
        out[pk]["total"].append({
            "strike": fs,
            "over_bid": _cents(m.get("yes_bid_dollars")),
            "over_ask": _cents(m.get("yes_ask_dollars")),
            "over_mid": _mid(m.get("yes_bid_dollars"), m.get("yes_ask_dollars")),
            "vol": _fp(m.get("volume_fp")),
        })

    # 3) team totals -> per team strikes
    for m in fetch_series("KXMLBTEAMTOTAL"):
        pk = suf2pk.get(_suffix(m.get("event_ticker")))
        if not pk or pk not in out:
            continue
        fs = _fp(m.get("floor_strike"))
        if fs is None:
            continue
        # sub like 'Texas over 7.5 runs scored' -> canonical team -> side
        cs = canon(m.get("yes_sub_title"))
        g = games.get(pk, {})
        side = None
        if cs and cs == canon(g.get("away_team")):
            side = "away"
        elif cs and cs == canon(g.get("home_team")):
            side = "home"
        if side is None:
            continue
        out[pk]["team_total"][side].append({
            "strike": fs,
            "over_bid": _cents(m.get("yes_bid_dollars")),
            "over_ask": _cents(m.get("yes_ask_dollars")),
            "over_mid": _mid(m.get("yes_bid_dollars"), m.get("yes_ask_dollars")),
            "vol": _fp(m.get("volume_fp")),
        })

    for pk in out:
        out[pk]["total"].sort(key=lambda x: x["strike"])
        out[pk]["team_total"]["away"].sort(key=lambda x: x["strike"])
        out[pk]["team_total"]["home"].sort(key=lambda x: x["strike"])

    payload = {
        "generated_at": __import__("datetime").datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "date": slate_date,
        "source": "Kalshi public API (KXMLBGAME / KXMLBTOTAL / KXMLBTEAMTOTAL)",
        "n_games": len(out),
        "games": out,
    }
    if not out:
        print("[kalshi] no games matched; leaving previous feed intact.", file=sys.stderr)
        sys.exit(1)
    with open(OUT, "w") as f:
        json.dump(payload, f, indent=2)
    tot = sum(len(v["total"]) for v in out.values())
    tt = sum(len(v["team_total"]["away"]) + len(v["team_total"]["home"]) for v in out.values())
    print("[kalshi] matched %d games, %d total strikes, %d team-total strikes -> %s"
          % (len(out), tot, tt, OUT), file=sys.stderr)


if __name__ == "__main__":
    main()
