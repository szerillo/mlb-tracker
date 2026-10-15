#!/usr/bin/env python3
"""Log Kevin Roth's game forecasts (pasted text) for the weather-source accuracy test.

Usage: python3 scripts/log_roth_paste.py roth_paste.txt [YYYY-MM-DD]
Reads the text as copied from his game cards ("Away at Home / Park / First pitch · 1:00 PM ET / ... hourly table with
Precip, Temp, Wind, Wind dir, Dewpoint") and appends one row per game to data/weather_sources/roth.jsonl:
  {logged_at, game_date, home, away, park, first_pitch_et, first_pitch_temp_f, hours:[{hour, precip, temp_f, wind_mph, wind_dir, dewpoint_f}]}
The grader joins these to PropFinder observed weather (weather_forecast_accuracy.json) by date + park, next to ours and BallparkPal.
"""
import sys, os, re, json, datetime as dt
OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'data', 'weather_sources', 'roth.jsonl')

def num(s):
    m = re.search(r'-?\d+(\.\d+)?', s or '')
    return float(m.group()) if m else None

def parse(text):
    lines = [l.strip() for l in text.splitlines()]
    games, i = [], 0
    while i < len(lines):
        if lines[i] == 'at' and i >= 1 and i + 1 < len(lines):
            h = i + 1
            while h < len(lines) and not lines[h]: h += 1
            g = {'away': lines[i - 1], 'home': lines[h]}
            j = h + 1
            while j < len(lines) and not lines[j]: j += 1
            g['park'] = lines[j].split('·')[0].strip() if j < len(lines) else None
            k = j
            while k < len(lines) and not lines[k].startswith('First pitch'): k += 1
            if k < len(lines): g['first_pitch_et'] = lines[k].split('·', 1)[-1].strip()
            m = k
            while m < len(lines) and not lines[m].endswith('°'): m += 1
            g['first_pitch_temp_f'] = num(lines[m]) if m < len(lines) else None
            hrs = []; n = m
            while n < len(lines) and not re.match(r'^Precip\b', lines[n]):
                if re.match(r'^\d{1,2} (AM|PM)$', lines[n]): hrs.append(lines[n])
                n += 1
            rows = {}
            while n < len(lines) and lines[n] and not lines[n] == 'at':
                parts = [p for p in re.split(r'\t+|\s{2,}', lines[n]) if p]
                if parts and parts[0] in ('Precip', 'Temp', 'Wind', 'Dewpoint', 'Wind dir'): rows[parts[0]] = parts[1:]
                elif parts[:2] == ['Wind', 'dir']: rows['Wind dir'] = parts[2:]
                n += 1
            g['hours'] = [{'hour': h, 'precip': num((rows.get('Precip') or [None] * 9)[x]), 'temp_f': num((rows.get('Temp') or [None] * 9)[x]),
                           'wind_mph': num((rows.get('Wind') or [None] * 9)[x]), 'wind_dir': (rows.get('Wind dir') or [None] * 9)[x],
                           'dewpoint_f': num((rows.get('Dewpoint') or [None] * 9)[x])} for x, h in enumerate(hrs)]
            games.append(g); i = n
        else: i += 1
    return games

if __name__ == '__main__':
    text = open(sys.argv[1]).read(); gd = sys.argv[2] if len(sys.argv) > 2 else dt.date.today().isoformat()
    G = parse(text); now = dt.datetime.now(dt.timezone.utc).isoformat(timespec='seconds')
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(OUT, 'a') as f:
        for g in G: f.write(json.dumps({'logged_at': now, 'game_date': gd, **g}) + '\n')
    for g in G: print(g['away'], '@', g['home'], g.get('first_pitch_et'), [(h['hour'], h['temp_f'], h['wind_mph'], h['wind_dir']) for h in g['hours']])
