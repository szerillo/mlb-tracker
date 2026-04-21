# New refresh scripts

Three scripts to drop into `scripts/` in your repo:

## 1. `scrape_projected_lineups.py`

Produces `data/lineups.json`. Pulls **confirmed lineups from MLB Stats API**
(authoritative when batting orders are posted) and **projected lineups from
Rotowire** as a fallback. Respects `data/_catcher_dan_flags.json` if your
existing pipeline writes it.

**Install:**
```bash
pip install requests beautifulsoup4 unidecode
```

**Run:**
```bash
python scripts/scrape_projected_lineups.py > data/lineups.json
```

**Output statuses** (front-end color-codes):
- `confirmed` — green pill; came from MLB Stats API live feed
- `projected` — red pill; came from Rotowire
- `tbd` — gray pill

## 2. `refresh_odds.py`

Produces `data/odds.json` using [The Odds API](https://the-odds-api.com).
Free tier = 500 requests/month. One request covers the whole slate, so
you'll burn ~30/month on a morning + evening refresh.

**Setup:**
1. Sign up at the-odds-api.com (30 seconds, no credit card).
2. Add the key as a GitHub Actions secret named `ODDS_API_KEY`.

**Install:**
```bash
pip install requests
```

**Run:**
```bash
export ODDS_API_KEY=xxxxxxxx
python scripts/refresh_odds.py > data/odds.json
```

Pulls best price per market across DK / FanDuel / BetMGM / Caesars /
BetRivers / bet365 / Fanatics, matching your existing schema.

## 3. `refresh_hitter_stats_enrich.py`

Adds projected **OPS** (and ISO / PA) to each hitter in `hitters.json`. Uses
Fangraphs' ATC projections by default; flip the `type=` query string in
`FG_URL` to `steamer` or `zips` if you prefer those.

**Install:**
```bash
pip install requests unidecode
```

**Run:**
```bash
python scripts/refresh_hitter_stats_enrich.py data/hitters.json > /tmp/h.json
mv /tmp/h.json data/hitters.json
```

The front-end's Lineups tab already has an OPS column — once this script
runs, the values light up (.329 wOBA chip stays, OPS chip fills in).

## 4. `refresh_pitcher_stats_enrich.py`

Adds **xFIP**, **Stuff+**, **Pitching+**, and **IP** to each pitcher in
`pitcher_stats.json`. Runs after your existing refresh script.

**Install:**
```bash
pip install requests unidecode
```

**Run:**
```bash
python scripts/refresh_pitcher_stats_enrich.py data/pitcher_stats.json \
  > data/pitcher_stats.new.json && mv data/pitcher_stats.new.json data/pitcher_stats.json
```

Fangraphs occasionally shuffles their leaderboard JSON column indices.
If the script returns 0 enriched entries, open
`https://www.fangraphs.com/leaders/major-league?stats=pit` in DevTools →
Network → filter `api/leaders` — copy the `type=...` query string into
`FG_URL` in the script.

### Front-end integration (once xFIP lands)

Update `index.html` `spMiniCell()` and `renderSPBox()`:

```js
// Prefer xFIP when we have a decent sample, otherwise fall back to pFIP
function primaryFipStat(s) {
  if (s?.xfip != null && (s?.ip ?? 0) >= 30) {
    return { label: "xFIP", val: s.xfip };
  }
  return { label: "pFIP", val: s.fip_proj };
}
```

Then replace `cell("pFIP", s.fip_proj, ...)` with the dynamic label/value.
Add Stuff+/Pitching+ chips under the SP box in the modal.

## GitHub Actions wiring

In `.github/workflows/refresh.yml`, add the steps:

```yaml
- name: Refresh lineups (MLB + Rotowire)
  run: python scripts/scrape_projected_lineups.py > data/lineups.json

- name: Refresh odds (The Odds API)
  env:
    ODDS_API_KEY: ${{ secrets.ODDS_API_KEY }}
  run: python scripts/refresh_odds.py > data/odds.json

- name: Enrich pitcher_stats with xFIP / Stuff+ / Pitching+
  run: |
    python scripts/refresh_pitcher_stats_enrich.py data/pitcher_stats.json > /tmp/ps.json
    mv /tmp/ps.json data/pitcher_stats.json
```

Run these **after** any existing step that produces the base `pitcher_stats.json`.
