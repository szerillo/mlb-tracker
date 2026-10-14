"""Normalisation helpers: odds conversions, team codes, player keys, canonical market/stat names."""
import re, unicodedata, datetime as dt

ET = dt.timezone(dt.timedelta(hours=-4))   # EDT; the logs run through October, DST ends 2026-11-01 -> see et_date()

TEAM_AB = {'Arizona Diamondbacks': 'ARI', 'Atlanta Braves': 'ATL', 'Baltimore Orioles': 'BAL', 'Boston Red Sox': 'BOS', 'Chicago Cubs': 'CHC',
           'Chicago White Sox': 'CWS', 'Cincinnati Reds': 'CIN', 'Cleveland Guardians': 'CLE', 'Colorado Rockies': 'COL', 'Detroit Tigers': 'DET',
           'Houston Astros': 'HOU', 'Kansas City Royals': 'KC', 'Los Angeles Angels': 'LAA', 'Los Angeles Dodgers': 'LAD', 'Miami Marlins': 'MIA',
           'Milwaukee Brewers': 'MIL', 'Minnesota Twins': 'MIN', 'New York Mets': 'NYM', 'New York Yankees': 'NYY', 'Athletics': 'ATH',
           'Oakland Athletics': 'ATH', 'Philadelphia Phillies': 'PHI', 'Pittsburgh Pirates': 'PIT', 'San Diego Padres': 'SD',
           'San Francisco Giants': 'SF', 'Seattle Mariners': 'SEA', 'St. Louis Cardinals': 'STL', 'Tampa Bay Rays': 'TB', 'Texas Rangers': 'TEX',
           'Toronto Blue Jays': 'TOR', 'Washington Nationals': 'WSH'}
AB_FIX = {'CHW': 'CWS', 'WAS': 'WSH', 'AZ': 'ARI', 'OAK': 'ATH'}
NICK = {v.split()[-1]: ab for v, ab in TEAM_AB.items()}; NICK.update({'Sox': None, 'Jays': 'TOR', 'Athletics': 'ATH'})


def team(x):
    if not x: return None
    x = str(x).strip()
    if x in TEAM_AB: return TEAM_AB[x]
    if x.upper() in AB_FIX: return AB_FIX[x.upper()]
    if len(x) <= 3 and x.upper() in set(TEAM_AB.values()): return x.upper()
    if x in ('Red Sox',): return 'BOS'
    if x in ('White Sox',): return 'CWS'
    return NICK.get(x)


def american_to_decimal(a):
    a = float(a)
    return 1 + a / 100 if a > 0 else 1 + 100 / -a


def decimal_to_american(d):
    d = float(d)
    if d <= 1: return None
    return round((d - 1) * 100) if d >= 2 else -round(100 / (d - 1))


def implied_from_american(a):
    return 1 / american_to_decimal(a)


def prob_to_american(p):
    if p is None or p <= 0 or p >= 1: return None
    return -round(100 * p / (1 - p)) if p >= .5 else round(100 * (1 - p) / p)


def player_key(name):
    """lowercase ascii, no punctuation, no suffix: 'Michael Harris II' -> 'michael harris'."""
    if not name: return None
    s = unicodedata.normalize('NFKD', str(name)).encode('ascii', 'ignore').decode().lower()
    s = re.sub(r"[.'’\-]", ' ', s)
    s = re.sub(r'\b(jr|sr|ii|iii|iv)\b', '', s)
    return re.sub(r'\s+', ' ', s).strip()


# canonical stat codes: K pitcher Ks, OUTS pitching outs, HA hits allowed, BBA walks allowed, ER earned runs,
# H hits, TB total bases, HR home runs, RBI, HRR hits+runs+rbi, R runs, BK batter Ks, BBB batter walks
STAT_KALSHI = {'KXMLBKS': 'K', 'KXMLBOUTS': 'OUTS', 'KXMLBHA': 'HA', 'KXMLBHIT': 'H', 'KXMLBHR': 'HR', 'KXMLBTB': 'TB', 'KXMLBRBI': 'RBI',
               'KXMLBHRR': 'HRR', 'KXMLBWA': 'BBA', 'KXMLBRFI': 'RFI', 'KXMLBRUNS': 'R'}
STAT_NOVIG = {'PITCHER_STRIKEOUTS': 'K', 'PITCHER_OUTS': 'OUTS', 'HITS_ALLOWED': 'HA', 'WALKS': 'BBA', 'EARNED_RUNS': 'ER', 'HITS': 'H',
              'TOTAL_BASES': 'TB', 'HITS_RUNS_RBIS': 'HRR', 'HOME_RUNS': 'HR', 'RBIS': 'RBI', 'RUNS': 'R', 'BATTING_STRIKEOUTS': 'BK',
              'BATTING_WALKS': 'BBB'}
STAT_AN = {'core_bet_type_37_strikeouts': 'K', 'core_bet_type_42_pitching_outs': 'OUTS', 'core_bet_type_72_hits_allowed': 'HA',
           'core_bet_type_76_walks': 'BBA', 'core_bet_type_74_earned_runs': 'ER', 'core_bet_type_36_hits': 'H', 'core_bet_type_77_total_bases': 'TB',
           'core_bet_type_33_hr': 'HR', 'core_bet_type_431_hits_runs_rbis': 'HRR', 'core_bet_type_34_rbi': 'RBI', 'core_bet_type_78_runs_scored': 'R',
           'core_bet_type_709_hitter_strikeouts': 'BK', 'core_bet_type_710_hitter_walks': 'BBB'}
STAT_PINNACLE = {'K': 'K', 'OUTS': 'OUTS', 'HA': 'HA', 'BB': 'BBA', 'ER': 'ER', 'H': 'H', 'TB': 'TB', 'HR': 'HR', 'RBI': 'RBI', 'HRR': 'HRR', 'R': 'R',
                 'BK': 'BK', 'BBB': 'BBB', 'HITS': 'H', 'SO': 'K', 'STRIKEOUTS': 'K'}
# boxscore field per stat: (group, key)
STAT_BOX = {'K': ('pitching', 'strikeOuts'), 'OUTS': ('pitching', 'outs'), 'HA': ('pitching', 'hits'), 'BBA': ('pitching', 'baseOnBalls'),
            'ER': ('pitching', 'earnedRuns'), 'H': ('batting', 'hits'), 'TB': ('batting', 'totalBases'), 'HR': ('batting', 'homeRuns'),
            'RBI': ('batting', 'rbi'), 'R': ('batting', 'runs'), 'BK': ('batting', 'strikeOuts'), 'BBB': ('batting', 'baseOnBalls'),
            'HRR': ('batting', 'hits+runs+rbi')}
GAME_MARKETS = {'ml', 'rl', 'total', 'tt_home', 'tt_away', 'f5_ml', 'f5_rl', 'f5_total', 'f5_tt_home', 'f5_tt_away', 'rfi'}
MARKET_CLASS = lambda m: 'game' if m in GAME_MARKETS else 'prop'
AN_BOOK_GAME = {'Consensus': 'AN_Consensus', 'Open': 'AN_Open', 'DK': 'DK', 'FD': 'FD', 'CZR': 'Caesars', 'BetRivers': 'BetRivers',
                'Fanatics': 'Fanatics', 'theScore': 'theScore', 'MGM': 'BetMGM'}


def parse_ts(x):
    """-> aware UTC datetime from iso string, epoch seconds, or epoch ms."""
    if x is None: return None
    if isinstance(x, (int, float)):
        if x > 1e11: x = x / 1000
        return dt.datetime.fromtimestamp(x, dt.timezone.utc)
    s = str(x).replace('Z', '+00:00')
    d = dt.datetime.fromisoformat(s)
    return d if d.tzinfo else d.replace(tzinfo=dt.timezone.utc)


def et_date(d_utc):
    """ET calendar date with the DST rule applied (2026: EDT until Nov 1 06:00 UTC)."""
    off = -4 if d_utc < dt.datetime(2026, 11, 1, 6, tzinfo=dt.timezone.utc) else -5
    return (d_utc + dt.timedelta(hours=off)).date()


def iso(d):
    return d.astimezone(dt.timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')


MON = ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC']


def kalshi_event(ev):
    """'26OCT142000MILLAD' -> (et_date, 'MIL', 'LAD', 'HHMM'); the time is ET, the team codes are Kalshi's.
    Team split: known 2-3 letter codes; try every split of the trailing letters."""
    m = re.match(r'^(\d\d)([A-Z]{3})(\d\d)(\d{4})([A-Z]+)$', ev)
    if not m: return None
    yy, mon, dd, hhmm, teams = m.groups()
    date = dt.date(2000 + int(yy), MON.index(mon) + 1, int(dd))
    codes = set(TEAM_AB.values())
    for i in (2, 3):
        a, h = teams[:i], teams[i:]
        a, h = AB_FIX.get(a, a), AB_FIX.get(h, h)
        if a in codes and h in codes: return date, a, h, hhmm
    return None
