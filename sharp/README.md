# Sharp tracker (MLB first)

In-house build of SHARP_TRACKER_BUILD_SPEC_2026-10-14 §1–§12: a price-history view plus a grading engine, no projection anywhere in the ruler.
Order of work is strict: **log → test → weight → bet**. Nothing here produces a bet on its own.

## What runs

| step | file | writes |
|---|---|---|
| fold the append-only logs into `events` + `ticks` | `sharp/build_db.py` | `data/sharp/sharp.sqlite` (derived, gitignored), `schedule_cache.json`, `kalshi_markets.json` |
| results + box scores from statsapi | `sharp/results.py` | `data/sharp/results.jsonl` |
| closing-line table (open, close, T-24h/6h/1h, outcome) | `sharp/grade.py` | table `closes`, `data/sharp/closes.jsonl` (gitignored) |
| bet ledger | `sharp/fills.py` | `data/sharp/fills.jsonl` |
| Tests A–D, opener (gated) | `sharp/tests.py` | nothing; read by export |
| ruler weights (gated ridge fit) | `sharp/weights_fit.py` | `data/sharp/weights_fitted.json` |
| dashboard data | `sharp/export.py` | `data/sharp/dashboard.json` → `sharp_dashboard.html` |

Workflows: `sharp_tracker.yml` runs the whole chain every 20 min (offset :10). `sharp_fill.yml` logs a fill from the Actions UI.
`sharp_prices.yml` is the light 20-minute price logger (see "Cadence" below).

Local: `python3 sharp/build_db.py && python3 sharp/results.py && python3 sharp/grade.py && python3 -m sharp.fills grade && python3 sharp/weights_fit.py && python3 sharp/export.py`,
then open `sharp_dashboard.html` from a local server (`python3 -m http.server`) or the Pages site. `SHARP_OFFLINE=1` skips every network call.

## Sources folded (all already logged by the existing jobs)

| log | venue(s) | class |
|---|---|---|
| `data/game_lines/pinnacle_log.jsonl` | Pinnacle (with max-risk limits) | game |
| `data/game_lines/an_log.jsonl` | DK, FD, Caesars, BetRivers, Fanatics, theScore; AN_Consensus (public %), AN_Open (reference only) | game |
| `data/game_lines/kalshi_log.jsonl` | Kalshi (bid/ask/last/vol per rung; F5 tie rung kept as `draw`) | game |
| `data/kalshi_oblog/*.jsonl` | Kalshi prop order books | prop |
| `data/novig_log/*.jsonl` | Novig (best resting bid per outcome; ask = 1 − other side's bid) | game + prop |
| `data/an_props_log/*.jsonl` | DK, FD, Caesars, BetRivers, Fanatics, theScore, BetMGM | prop |
| `data/prop_scan/pinnacle_log.jsonl` | Pinnacle props (with limits); matched to a game via the exchanges' player→game map | prop |

Canonical event id = statsapi gamePk, matched by participants + ET date (+ nearest start for doubleheaders), never by name string.
Every tick stores both sides as rows. Exchanges store bid/ask/mid; `implied` is the ask (cost to take). Flags: `open` (first row per venue×market×line×side),
`wide` (ask − bid > 0.04, excluded from the ruler), `oneside` (no complement quote), `pulled` (venue stopped quoting 60+ min before the others), `threeway`.

## The ruler

`fair.py`: per venue, devig (power by default; proportional and Shin implemented, chosen by calibration at 2,000 events) or the exchange mid when the
spread is ≤ 4¢; weighted average with the prior vector from `constants.py` (the thread's hand-set weights, renormalised, every venue capped at 60%).
Books older than 45 min and exchange quotes older than 45 min do not enter. A fair is shown on the board only with ≥3 ruler venues.
Every fair carries `status` = `prior` until `weights_fit.py` passes its gate (1,500 settled games for game lines, 20,000 settled prop rows) and writes a fitted vector.

Best bettable price excludes BetMGM and adds Kalshi's taker fee to the ask; Novig charges no pregame fee.

## Tests (pre-registered, gated)

A accuracy (Brier + deciles) · B speed (gap closure at 24h/6h/1h, with the reaction-speed control; the feed-lag control waits on measured lags) ·
C head-to-head edge conversion on props (3,000 pairs per venue) · D the over tax (and C split by side) · opener test. Steam test: not yet (needs a tick-level pass).
Below the gate the export carries the sample count only. `python3 sharp/tests.py --preview` prints below-gate numbers locally, labelled PREVIEW.

## Fills

`python3 -m sharp.fills add --game MIL@LAD --market ml --side home --venue FD --price -174 --stake 1 --tag "rule:…"`. The fair at fill is computed from the
DB at that moment; a row written after first pitch is tagged `late` and excluded from CLV. "Discussed" and "would have" are not fills.

## Cadence (read this first)

The log currently averages 3–6 snapshots per game per venue against ~40 expected from a 20-minute job. `kalshi_oblog.yml` shares a concurrency group with
the prop scan, takes 10+ minutes (two 5-minute sleeps in `game_lines_log.py`), and GitHub drops scheduled runs while one is queued. Until that is fixed,
T-6h snapshots are mostly empty, most closes are older than 20 minutes, and every gate takes several times longer to reach. `sharp_prices.yml` runs the
four loggers alone every 20 minutes; once it is on, remove those four steps from `kalshi_oblog.yml`.

## Constants

`sharp/constants.py`. Every entry has `status` (rule / prior / fitted) and a `source`. An entry without one is a bug. The dashboard's Constants tab lists them.

## Not yet done (spec items deferred)

Feed-lag measurement per venue (spec §2, weekly spot check); devig method calibration harness (§5, needs 2,000 settled events); steam-chasing test (§6);
limit-growth test (§6; the raw material is in `pinnacle_first.json` and the ticks); Circa, ProphetX, Polymarket feeds; other sports (the schema is sport-agnostic:
`sport`/`league` columns, `events` keyed by any id); folding Bartolo's older `clv_archive` into `fills` (those rows are model calls, not fills, so they stay out by rule).
