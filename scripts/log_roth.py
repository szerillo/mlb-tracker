#!/usr/bin/env python3
"""Log Kevin Roth's MLB game forecasts (mysportsweather.com) for the weather-source accuracy test.

Reads https://mysportsweather.com/mlb/today and /mlb/tomorrow (robots.txt allows crawling) a few times a day from the
weather workflow and appends one row per game per run to data/weather_sources/roth.jsonl:
  {logged_at, game_date, game_id, away, home, park, first_pitch_et, ou, ou_open, first_pitch_temp_f, rain_pct, wind_mph,
   wind_rel: in|out|cross|calm (his field arrow), wind_rel_deg (arrow rotation, 0 = blowing out to CF, 180 = in from CF),
   hours:     [{hour, game_hour, precip, temp_f, wind_mph, wind_dir, dewpoint_f}]   his main forecast
   hours_nbm: [...]                                                                  the NBM 2.5 km table under it}
Nothing here changes a live number; the grader joins these to observed weather next to ours and BallparkPal.
"""
import os, re, json, html, time, datetime as dt, urllib.request
OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'data', 'weather_sources', 'roth.jsonl')
BASE = 'https://mysportsweather.com/mlb/'
UA = {'User-Agent': 'bartolo-weather-log (personal research; a few requests per day)'}
ET = dt.timezone(dt.timedelta(hours=-4))

def get(u):
    for i in range(3):
        try: return urllib.request.urlopen(urllib.request.Request(u, headers=UA), timeout=30).read().decode('utf-8', 'replace')
        except Exception: time.sleep(3 * (i + 1))
    return ''

txt = lambda s: re.sub(r'\s+', ' ', html.unescape(re.sub(r'<[^>]+>', ' ', s or ''))).strip()
def num(s):
    m = re.search(r'-?\d+(\.\d+)?', s or '')
    return float(m.group()) if m else None

def table(t):
    hours = [txt(x) for x in re.findall(r'<div class="hourly__time">(.*?)</div>', t, re.S)]
    gh = [('is-game-hour' in x) for x in re.findall(r'<th class="hourly__hour-col([^"]*)"', t)]
    rows = {}
    for tr in re.findall(r'<tr>(.*?)</tr>', t, re.S):
        lab = re.search(r'class="hourly__label">(.*?)</td>', tr, re.S)
        if not lab: continue
        rows[txt(lab.group(1))] = [txt(c) for c in re.findall(r'<td class="hourly__cell[^"]*">(.*?)</td>', tr, re.S)]
    g = lambda k, x: (rows.get(k) or [None] * 30)[x] if x < len(rows.get(k) or []) else None
    return [{'hour': h, 'game_hour': gh[x] if x < len(gh) else None, 'precip': num(g('Precip', x)), 'temp_f': num(g('Temp', x)),
             'wind_mph': num(g('Wind', x)), 'wind_dir': g('Wind dir', x), 'dewpoint_f': num(g('Dewpoint', x))} for x, h in enumerate(hours)]

def parse(page, game_date):
    out = []
    for m in re.finditer(r'<article class="mlb-game-block[^"]*" id="([^"]+)">(.*?)</article>', page, re.S):
        gid, b = m.group(1), m.group(2)
        teams = [txt(x) for x in re.findall(r'<span class="matchup-team">.*?<span>(.*?)</span></span>', b, re.S)]
        arrow = re.search(r'fd-arrow--(\w+)"(?: style="transform: rotate\(([-\d.]+)deg)?', b)
        tabs = re.findall(r'<table class="hourly__table">(.*?)</table>', b, re.S)
        ou = re.search(r'O/U ([\d.]+)', b); op = re.search(r'Opened ([\d.]+)', b)
        vals = [txt(x) for x in re.findall(r'class="game-summary__(?:primary-)?value[^"]*">(.*?)</span>\s*<span class="game-summary__(?:primary-label|cell-label)', b, re.S)]
        labs = [txt(x) for x in re.findall(r'class="game-summary__(?:primary-label|cell-label)">(.*?)</span>', b, re.S)]
        summ = dict(zip(labs, vals))
        out.append({'game_date': game_date, 'game_id': gid, 'away': teams[0] if teams else None, 'home': teams[1] if len(teams) > 1 else None,
                    'park': txt((re.search(r'mlb-game-block__venue">(.*?)</div>', b, re.S) or [None, ''])[1]).split('·')[0].strip() or None,
                    'first_pitch_et': txt((re.search(r'mlb-game-block__time">(.*?)</div>', b, re.S) or [None, ''])[1]).split('·')[-1].strip() or None,
                    'ou': float(ou.group(1)) if ou else None, 'ou_open': float(op.group(1)) if op else None,
                    'first_pitch_temp_f': num(summ.get('At first pitch')), 'rain_pct': num(summ.get('Rain')), 'wind_mph': num(summ.get('Wind')),
                    'wind_rel': arrow.group(1) if arrow else None, 'wind_rel_deg': float(arrow.group(2)) if arrow and arrow.group(2) else None,
                    'hours': table(tabs[0]) if tabs else [], 'hours_nbm': table(tabs[1]) if len(tabs) > 1 else []})
    return out

def main():
    now = dt.datetime.now(dt.timezone.utc); today = now.astimezone(ET).date()
    rows = []
    for path, d in (('today', today), ('tomorrow', today + dt.timedelta(days=1))):
        p = get(BASE + path)
        if p: rows += parse(p, d.isoformat())
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(OUT, 'a') as f:
        for r in rows: f.write(json.dumps({'logged_at': now.isoformat(timespec='seconds'), **r}) + '\n')
    print(f'[roth] logged {len(rows)} games:', ', '.join(f"{r['away']} @ {r['home']} {r['first_pitch_temp_f']}F {r['wind_mph']} mph {r['wind_rel']}" for r in rows))

if __name__ == '__main__':
    main()
