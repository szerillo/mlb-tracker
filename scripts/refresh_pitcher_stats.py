"""
Refresh current-season pitcher stats from Baseball Savant + MLB Stats API.

- Savant expected_statistics endpoint: ERA, xERA, wOBA, xwOBA (season-to-date)
- MLB Stats API: K%, BB% (derived from pitching stats)
- ATC-projected FIP: loaded from the user-uploaded pitchers.json

Writes data/pitcher_stats.json keyed by normalized name.
"""
import json, os, sys, csv, io, datetime, urllib.request, unicodedata
from concurrent.futures import ThreadPoolExecutor

OUTPUT = os.path.join(os.path.dirname(__file__), "..", "data", "pitcher_stats.json")
PITCHERS_JSON = os.path.join(os.path.dirname(__file__), "..", "data", "pitchers.json")

SAVANT_URL = "https://baseballsavant.mlb.com/leaderboard/expected_statistics?year={year}&type=pitcher&csv=true"

sys.path.insert(0, os.path.dirname(__file__))
from _common import skip_if_not_in_window


def strip_accents(s):
    if not isinstance(s, str): return s
    return "".join(c for c in unicodedata.normalize('NFKD', s) if not unicodedata.combining(c))


def norm_name(s):
    if not isinstance(s, str): return ""
    s = strip_accents(s).lower()
    for suffix in [' jr.', ' jr', ' sr.', ' sr', ' iii', ' ii']:
        if s.endswith(suffix): s = s[:-len(suffix)]
    return s.replace('.', '').strip()


def savant_to_key(last_first):
    parts = [p.strip() for p in (last_first or "").split(",", 1)]
    if len(parts) != 2: return norm_name(last_first)
    last, first = parts
    return norm_name(f"{first} {last}")


def fetch_savant(year):
    url = SAVANT_URL.format(year=year)
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0", "Accept": "text/csv"})
    with urllib.request.urlopen(req, timeout=60) as r:
        return r.read().decode("utf-8-sig")


def fetch_mlb_kbb_and_hand(person_ids, season):
    """Fetch K/BB counts + TBF + pitchHand for many pitchers in batch."""
    if not person_ids: return {}, {}
    kbb_result = {}
    hand_result = {}
    ids_list = list(person_ids)
    for i in range(0, len(ids_list), 100):
        chunk = ids_list[i:i+100]
        url = (f"https://statsapi.mlb.com/api/v1/people?personIds={','.join(str(x) for x in chunk)}"
               f"&hydrate=stats(group=pitching,type=season,season={season})")
        try:
            req = urllib.request.Request(url, headers={"User-Agent":"u/1.0"})
            with urllib.request.urlopen(req, timeout=30) as r:
                d = json.load(r)
        except Exception as e:
            print(f"  MLB K/BB batch failed: {e}")
            continue
        for p in d.get("people", []):
            pid = p["id"]
            hand_result[pid] = (p.get("pitchHand") or {}).get("code")
            for s in p.get("stats", []):
                for sp in s.get("splits", []):
                    st = sp.get("stat", {})
                    ks = st.get("strikeOuts")
                    bbs = st.get("baseOnBalls")
                    tbf = st.get("battersFaced")
                    if ks is not None and tbf and tbf > 0:
                        kbb_result[pid] = {
                            "k_pct": round(100 * ks / tbf, 1),
                            "bb_pct": round(100 * (bbs or 0) / tbf, 1),
                            "tbf": tbf,
                        }
    return kbb_result, hand_result


def main():
    if skip_if_not_in_window("refresh_pitcher_stats", overnight_only=True):
        return

    year = datetime.date.today().year
    print(f"Fetching Savant expected_statistics for {year}...")
    try:
        text = fetch_savant(year)
    except Exception as e:
        print(f"  ERR: {e}"); return

    reader = csv.DictReader(io.StringIO(text))
    savant_stats = {}
    pid_to_key = {}
    def _f(v):
        try: return float(v) if v and v != "null" else None
        except (ValueError, TypeError): return None
    for row in reader:
        key = savant_to_key(row.get("last_name, first_name",""))
        if not key: continue
        pid = row.get("player_id")
        try: pid_int = int(pid) if pid else None
        except ValueError: pid_int = None
        savant_stats[key] = {
            "era": _f(row.get("era")),
            "xera": _f(row.get("xera")),
            "woba": _f(row.get("woba")),
            "xwoba": _f(row.get("est_woba")),
            "pa": _f(row.get("pa")),
            "mlbam_id": pid_int,
        }
        if pid_int: pid_to_key[pid_int] = key
    print(f"  Savant entries: {len(savant_stats)}")

    # Fetch K%/BB% + pitchHand from MLB Stats API for all Savant pitchers
    print(f"Fetching K%/BB% + pitchHand from MLB Stats API for {len(pid_to_key)} pitchers...")
    kbb, hand_by_pid = fetch_mlb_kbb_and_hand(list(pid_to_key.keys()), year)
    for pid, stats in kbb.items():
        key = pid_to_key.get(pid)
        if key and key in savant_stats:
            savant_stats[key].update(stats)
    for pid, hand in hand_by_pid.items():
        key = pid_to_key.get(pid)
        if key and key in savant_stats:
            savant_stats[key]["hand"] = hand
    print(f"  K/BB resolved: {sum(1 for v in savant_stats.values() if 'k_pct' in v)}")
    print(f"  pitchHand resolved: {sum(1 for v in savant_stats.values() if v.get('hand'))}")

    # Load projected FIP
    try:
        proj = json.load(open(PITCHERS_JSON))
        fip_map = {k: v["fip"] for k, v in proj.get("pitchers", {}).items()}
    except Exception:
        fip_map = {}
    print(f"  ATC FIP projections: {len(fip_map)}")

    # Combine
    combined = {}
    all_keys = set(savant_stats.keys()) | set(fip_map.keys())
    for k in all_keys:
        s = savant_stats.get(k, {})
        entry = {
            "fip_proj": fip_map.get(k),
            "era": s.get("era"),
            "xera": s.get("xera"),
            "woba": s.get("woba"),
            "xwoba": s.get("xwoba"),
            "k_pct": s.get("k_pct"),
            "bb_pct": s.get("bb_pct"),
            "k_bb_pct": round(s["k_pct"] - s["bb_pct"], 1) if s.get("k_pct") is not None and s.get("bb_pct") is not None else None,
            "hand": s.get("hand"),
            "mlbam_id": s.get("mlbam_id"),
        }
        combined[k] = entry

    payload = {
        "generated_at": datetime.datetime.utcnow().isoformat() + "Z",
        "sources": [
            "Baseball Savant (xERA, xwOBA, ERA, wOBA — season to date)",
            "MLB Stats API (K%, BB% from season stats)",
            "ATC (projected FIP from user's Excel upload)",
        ],
        "thresholds": {
            "fip":   {"elite": 3.25, "good": 3.75, "bad": 4.25, "worst": 4.75},
            "xera":  {"elite": 3.25, "good": 3.75, "bad": 4.25, "worst": 4.75},
            "k_bb":  {"elite": 18.0, "good": 12.0, "bad": 7.0, "worst": 4.0},
            "k_pct": {"elite": 28.0, "good": 24.0, "bad": 18.0, "worst": 15.0},
            "xwoba": {"elite": 0.290, "good": 0.310, "bad": 0.340, "worst": 0.360},
        },
        "pitchers": combined,
    }
    os.makedirs(os.path.dirname(OUTPUT), exist_ok=True)
    with open(OUTPUT, "w") as f:
        json.dump(payload, f, indent=2)
    matched = sum(1 for v in combined.values() if v["xera"] is not None and v["fip_proj"] is not None)
    print(f"  wrote {len(combined)} pitchers  ({matched} with both Savant + projected FIP)")


if __name__ == "__main__":
    main()
