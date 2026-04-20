"""Shared helpers for refresh scripts."""
import datetime, json, urllib.request, os


def get_today_games():
    """Return today's games from MLB Stats API with ISO game times."""
    today = datetime.date.today().isoformat()
    url = f"https://statsapi.mlb.com/api/v1/schedule?sportId=1&date={today}"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "mlb-tracker/1.0"})
        with urllib.request.urlopen(req, timeout=20) as r:
            d = json.load(r)
    except Exception as e:
        print(f"  schedule fetch failed: {e}")
        return []
    return d.get("dates", [{}])[0].get("games", []) if d.get("dates") else []


def earliest_first_pitch():
    """Return the earliest non-Final game time today, or None if all games are done/no games."""
    games = get_today_games()
    now = datetime.datetime.now(datetime.timezone.utc)
    future = []
    for g in games:
        status = g.get("status", {}).get("detailedState", "")
        if status in ("Final", "Game Over", "Completed Early", "Postponed", "Cancelled"):
            continue
        try:
            gt = datetime.datetime.fromisoformat(g["gameDate"].replace("Z", "+00:00"))
            future.append(gt)
        except Exception:
            continue
    return min(future) if future else None


def within_game_window(hours_before=4, hours_after=5):
    """
    True if any of today's games is within `hours_before` future OR `hours_after` past
    of the current moment. In-progress games count as within window.
    """
    games = get_today_games()
    now = datetime.datetime.now(datetime.timezone.utc)
    for g in games:
        try:
            gt = datetime.datetime.fromisoformat(g["gameDate"].replace("Z", "+00:00"))
        except Exception:
            continue
        delta_hours = (gt - now).total_seconds() / 3600
        # delta > 0 = game is in future; delta < 0 = game started that many hours ago
        if -hours_after <= delta_hours <= hours_before:
            return True
        # In-progress games always count
        status = g.get("status", {}).get("detailedState", "")
        if status in ("In Progress", "Warmup", "Pre-Game"):
            return True
    return False


def is_overnight_anchor(tolerance_min=20):
    """True if current UTC hour matches one of our overnight anchor slots (12am/2am/8am ET)."""
    now = datetime.datetime.now(datetime.timezone.utc)
    # EDT anchors: 04:00 UTC (12am ET), 06:00 UTC (2am ET), 12:00 UTC (8am ET)
    for anchor_hour in (4, 6, 12):
        target = now.replace(hour=anchor_hour, minute=0, second=0, microsecond=0)
        diff_min = abs((now - target).total_seconds() / 60)
        if diff_min <= tolerance_min:
            return True
    return False


def should_run(window_hours_before=4, window_hours_after=5, force_env="FORCE_RUN"):
    """
    Decide whether a refresh should proceed.

    Returns True if:
      - FORCE_RUN env var is set (manual dispatch will set this), or
      - We're at an overnight anchor, or
      - Any game is within the game window.
    """
    if os.environ.get(force_env):
        return True
    if is_overnight_anchor():
        return True
    return within_game_window(window_hours_before, window_hours_after)


def skip_if_not_in_window(name, overnight_only=False):
    """Print a skip message and return True if we should NOT proceed."""
    if overnight_only:
        if is_overnight_anchor() or os.environ.get("FORCE_RUN"):
            return False
        print(f"[{name}] skip: not an overnight anchor, and this script only runs then.")
        return True
    if should_run():
        return False
    print(f"[{name}] skip: no game within window and not an overnight anchor.")
    return True
