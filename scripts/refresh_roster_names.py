#!/usr/bin/env python3
"""
Daily clean-name feed for active MLB rosters.

Pulls every team's ACTIVE (26-man) roster from the MLB Stats API and writes a
canonical, de-formatted name for each player so neither BARTOLO nor the external
model has to hand-fix names.

MLB's `fullName` already uses the name a player GOES BY (e.g. "Aroldis Chapman",
not legal "Albertin"; "Emmet Sheehan", not "George") — the legal first name is in
a separate `firstName` field. What MLB leaves in are suffixes and disambiguation
middle-initials ("Daniel Lynch IV", "Jose A. Ferrer"). clean_name() strips those
and normalizes to ASCII.

Outputs:
  data/roster_names.json  { generated_at, players:[{mlbam_id, team, team_abbr,
                            pos, hand, name, name_raw}] , by_team:{abbr:[...]} }
  data/roster_names.csv   mlbam_id,team_abbr,pos,hand,name,name_raw   (IMPORTDATA-friendly)
"""
import json, csv, os, re, unicodedata, datetime, urllib.request

SUFFIXES = {"jr","sr","ii","iii","iv","v"}

def clean_name(name: str) -> str:
    if not name:
        return ""
    # 1) strip accents -> ASCII
    s = unicodedata.normalize("NFKD", name)
    s = "".join(c for c in s if not unicodedata.combining(c))
    # 2) tokenize; drop internal periods so "P.J." -> "PJ", "A." -> "A"
    toks = [t.replace(".", "") for t in s.split()]
    toks = [t for t in toks if t]
    # 3) strip trailing suffix tokens (Jr/Sr/II-V)
    while len(toks) > 2 and toks[-1].lower().strip(",") in SUFFIXES:
        toks.pop()
    # 4) drop single-letter INTERIOR tokens (middle initials); keep first & last
    if len(toks) > 2:
        toks = [toks[0]] + [t for t in toks[1:-1] if len(t) > 1] + [toks[-1]]
    return " ".join(toks)

def fetch(u):
    req = urllib.request.Request(u, headers={"User-Agent": "roster-names/1.0"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read())

def main():
    here = os.path.dirname(__file__)
    teams = fetch("https://statsapi.mlb.com/api/v1/teams?sportId=1")["teams"]
    players, by_team = [], {}
    for t in teams:
        tid, abbr, tname = t["id"], t.get("abbreviation", ""), t.get("name", "")
        try:
            d = fetch(f"https://statsapi.mlb.com/api/v1/teams/{tid}/roster?rosterType=active&hydrate=person")
        except Exception as e:
            print(f"  roster fail {abbr}: {e}"); continue
        for e in d.get("roster", []):
            p = e.get("person", {})
            raw = p.get("fullName", "")
            rec = {
                "mlbam_id": p.get("id"),
                "team": tname, "team_abbr": abbr,
                "pos": e.get("position", {}).get("abbreviation", ""),
                "hand": p.get("pitchHand", {}).get("code", ""),
                "name": clean_name(raw), "name_raw": raw,
            }
            players.append(rec)
            by_team.setdefault(abbr, []).append(rec)
    payload = {"generated_at": datetime.datetime.utcnow().isoformat() + "Z",
               "source": "MLB Stats API active rosters", "n_players": len(players),
               "players": players, "by_team": by_team}
    with open(os.path.join(here, "..", "data", "roster_names.json"), "w") as f:
        json.dump(payload, f, indent=1)
    with open(os.path.join(here, "..", "data", "roster_names.csv"), "w", newline="") as f:
        w = csv.writer(f); w.writerow(["mlbam_id","team_abbr","pos","hand","name","name_raw"])
        for r in players:
            w.writerow([r["mlbam_id"], r["team_abbr"], r["pos"], r["hand"], r["name"], r["name_raw"]])
    print(f"wrote {len(players)} players across {len(by_team)} teams")
    # show that the known problem cases clean up correctly
    for raw in ["Daniel Lynch IV","Jose A. Ferrer","Mark Leiter Jr.","P.J. Higgins",
                "J.P. France","Andres Munoz","Hyun Jin Ryu","Michael A. Taylor"]:
        print(f"   {raw!r:24} -> {clean_name(raw)!r}")

if __name__ == "__main__":
    main()
