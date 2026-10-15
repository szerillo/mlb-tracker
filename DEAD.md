# DEAD.md: tests that are closed. Check here before proposing a test. Seeded 2026-10-14 by Fable from METHODOLOGY_INDEX §2 and the October docs.

Rule: a row here is not retested without a stated reason (new data source, new definition, or a pre-registered 2027 refit named in PINNED §D). To reopen, add a row to PINNED with the reason; do not delete rows here. Owner: Fable. Bartolo and the prop model append rows for their own nulls using the same columns.

Columns: test | definition and sample | result | date | ran by | doc

## Hitter inputs (regular season)

| test | definition and sample | result | date | by | doc |
|---|---|---|---|---|---|
| Observed-season wOBA blended into ROS projection | k in PA/(PA+k); next-30d wOBA; 13 weekly snapshots 2026 | k to infinity in 13/13 folds; weight 0 | 9/23 | Fable | HITTER_BLEND_REFIT_AND_HOTCOLD_2026-09-23 |
| Hot/cold rolling 7/14/30d, 8 stats, 3 horizons | residual vs projection; 2026 + 2025 OOS, 26k rows | null all horizons | 9/23 | Fable | same |
| Hitter process index (whiff, chase, zone-swing, K, BB, EV, HH, bat speed; 14d vs 46d) | 2021 to 2026, 18,865 rows, next-30 wOBA | null for wOBA (max t 1.5); K-rate persistence only (K props) | 9/24 | Fable | HITTER_PROCESS_INDEX_SIX_SEASONS_2026-09-24 |
| Plate-discipline change as leading indicator | ctrl for contemporaneous xwOBA | null; 2025 whiff spike t −3.8 did not replicate in 2026 | 9/23 | Fable | HOTCOLD_SHORT_HORIZON_AND_FBPOWER_2026-09-23 |
| FB-power streakiness ("Alonso type") | HR/PA top-q × FB tendency | 2026 +0.28 (t 2.7), 2025 −0.19 (t −1.8); sign flip | 9/23 | Fable | same |
| Age residual on projection | linear, quadratic, buckets | null (t −1.85) | 9/23 | Fable | WOBA_INCREMENTS_AGE_BATSPEED_GRADES_2026-09-23 |
| Bat speed level | season to date | already in projection (3.6 pts/mph) | 9/23 | Fable | same |
| Damage / Eye grade components | increments to projection | null or negative (over-crediting) | 9/23 | Fable | same |
| Swing path levels and deltas (attack angle, tilt, ideal rate, intercept, direction, chase, zone-swing) | 33 tests, 2026 | null; 30d attack-angle and bat-speed deltas t 2.0 logged for 2027 | 9/24 | Fable | MECHANICS_AND_PITCHER_STREAKS_2026-09-24 |
| Components-over-xwOBA gate; p_Damage / p_SwStr as 6th wFIP input | leakage-free retest | reject | 9/8 | Fable | PSWSTR_GATE_RESULTS_2026-09-08 |
| Sheet platoon Y/Z construction (absolute wRC+ as a split) | corr with wRC+ .995 | reject; replaced by league split by bats | 9/25 | Fable | PLATOON_DOUBLE_COUNT_FINDING_2026-09-25 |
| Defense × BIP / pitching quality (sheet bipf) | 28,690 RS team-games; SwStr, CSW, Stuff proxies | null ×3; bipf removed | 9/25 | Fable | SHEET_EDITS_v3_TESTS_AND_PASTE_2026-09-25 |
| Hitter bat speed tonight (first two PAs vs own norm) | 34,186 batter-games 2026 | null on hits, TB, HR, K, xwOBA | 10/14 | Fable | LIVE_BATTERY_2_AND_APRIL_ITEMS_2026-10-14 |
| Hitter's own as-of rate vs the Kalshi mid | slopes .01 / .05 / −.09 | market knows the season line | 10/14 | prop model | ALPHA_DIG_2026-10-14 |
| Rust (days since last start) | gaps 4 to 9 days n 314 | −3 hits / −2 TB, t −1; 10+ days nothing | 10/14 | prop model | same |
| Platoon part-timers in the regular season | hits 1+, n 933 | +0.9; LHB platoon −0.6; priced in season (October lift is October only) | 10/14 | prop model | same |
| Sibling-rung relative value (TB 2+ off hits 1+ × own ratio) | as-of ratios | full-season version was lookahead (t 4.5); as-of slope .14 / −.17 | 10/14 | prop model | same |
| ENG2 hitter grade vs Kalshi closes | 56,516 rungs Sep 2026 | slope −0.02, Brier worse than close every series; hitter props zero model weight | 10/6 | Fable | REPLY3_to_BARTOLO_eng2_signoff_hitters_hr_2026-10-06 |

## Pitcher inputs

| test | definition and sample | result | date | by | doc |
|---|---|---|---|---|---|
| 30 anchor fields as increments to pitcher base (Pitching+, Stuff+, velo_delta, L5 CSW, ...) | residual vs projection, SP n 840 | none clear (max t 1.9 vs bar 2.9); stuff family logged for 2027 | 9/24 | Fable | october/pitcher_increment_sweep.csv |
| Pitcher mechanics deltas: arm angle, IVB, location, release SD, extension, zone, FPS, mix change, velo fade | 176 tests, 14/30d | null; only the SP process index (velo, whiff, CSW, K-BB) survived | 9/24 | Fable | MECHANICS_AND_PITCHER_STREAKS_2026-09-24 |
| Pitcher results streak (14d xwOBA against) | | null (t 1.8) | 9/24 | Fable | same |
| TTO3 as a pitcher trait | | control artifact | 9/24 | Fable | SP_PROCESS_INDEX_SIX_SEASONS_2026-09-24 |
| Fastball shape by park (IVB) as pitcher up/downgrade | 451k four-seamers 2025 to 2026 | park IVB real, whiff +0.5 pp/inch, 0.03 runs per start; no upgrade | 10/5 | Fable | FASTBALL_SHAPE_BY_PARK_2026-10-05 |
| Archive features vs Kalshi K/outs/HA closes (xERA, SIERA, Stuff+, Location+, L5 stuff, CSW, barrel, splits, wx v8, pen RR flags, opposing 14d xwOBA) | Aug to Sep 2026 | all null; velo delta vs K ladder Sept-only (April candidate) | 10/3 | Fable | KALSHI_PROPS_ALPHA_SCAN_2026-10-03 §10 |
| Our wFIP / K-BB / lineup / pen inputs beyond Kalshi closes | | all t ≤ 1.3 | 10/3 | Fable | same |
| Outs props off the exit hazard vs Kalshi close (RS) | 1,488 markets | Brier .263 vs close .241; band edge = entry reversion | 10/3 | Fable | REPLY_to_BARTOLO_kalshi_props_backtest_2026-10-03 |
| K props: multi-factor model (lineup K%, whiff, SP K%, CSW, velo L3, umpire, home, day) | fit Aug score Sep | slope −0.68; every coefficient flips; K market calibrated at every level | 10/14 | prop model | ALPHA_DIG_2026-10-14 |
| K Poisson beyond the close | | anti-informative (z −3.9) | 10/3 | Fable | KALSHI_PROPS_ALPHA_SCAN_2026-10-03 §12 |
| Pitch-mix dropout in game (usual pitch at a quarter of expected through 3) | 2026 and 2025 | 2026 first-half artifact (+70 % then −16 %); 2025 z 0.3 | 10/14 | Fable | LIVE_BATTERY_2_AND_APRIL_ITEMS_2026-10-14 |
| Live starter contact and discipline signals (whiff, chase, CSR, zone, foul EV, hard fouls, BIP EV, hard-hit, xwOBA, bat speed) on next-inning runs | 24,748 starter-innings 2026 | all |z| < 1.6; signals predict the hook, not the runs | 10/12 | Fable | LIVE_SIGNALS_PITCHER_CONTACT_2026-10-12 |
| Openers / short starters vs hitter rungs | | nothing | 10/14 | prop model | ALPHA_DIG_2026-10-14 |

## Game environment and schedule

| test | definition and sample | result | date | by | doc |
|---|---|---|---|---|---|
| Day game after night game: totals, NRFI, K | both-teams ET (10/4) and per-team local (10/14); RS 2024 to 2026, 1,334 games / 2,635 team-games; PS 33 games | null both definitions (runs −0.07 se 0.06 per team-game); general day-game dip already priced | 10/4, 10/14 | Fable | DAY_AFTER_NIGHT_RESEARCH_2026-10-04, NOTE_dan_team_level_recheck_2026-10-14 |
| Day/night effect as shadows | domes vs open air, 1 pm vs 4 pm | dip exists under domes and at 1 pm; it is temperature outdoors, not shadows | 10/4 | Fable | DAY_AFTER_NIGHT_RESEARCH addendum |
| Start time / twilight / west-coast team in eastern day game vs totals, F5, RFI | 2026 Kalshi | within noise; twilight overs +10 on 30 games (opposite of shadow story) | 10/14 | prop model | ALPHA_DIG_2026-10-14 |
| Umpire live zone (edge calls through 3 innings → innings 4+) | 2,387 games | within-game persistence r .05; no K/BB effect | 10/14 | Fable | LIVE_BATTERY_2_AND_APRIL_ITEMS_2026-10-14 |
| Umpire as-of called-strike residual vs K props, outs, totals | 360k taken pitches | K +0.4 pts/SD t 0.8; outs nothing; totals one-sided and noisy | 10/14 | prop model | ALPHA_DIG_2026-10-14 |
| Umpire BB% vs walks market | 672 markets | z 0.0 | 10/6 | Fable | REPLY3_to_BARTOLO_eng2_signoff_hitters_hr_2026-10-06 §4 |
| Bullpen fatigue (prev 1d/3d usage, b2b relievers) vs hook, pen runs, R/BF, totals, late-innings runs | 2026 RS | null everywhere (R/BF .1174 vs .1182) | 10/3, 10/14 | Fable, prop model | REPLY_to_BARTOLO_kalshi_game_markets_2026-10-03; ALPHA_DIG |
| Weather vs Kalshi totals and TB rungs | 868 games with game-time weather, open air | market prices it; residual flips by month | 10/14 | prop model | reply 10/14 evening |
| HR 1+ vs temperature beyond the close | 11,904 markets | temperature −0.08 logit (market over-prices heat); only wind +0.12 survives, expressible at the bid only | 10/6 | Fable | REPLY_to_BARTOLO_velo_hr_weather_2026-10-06 |
| Off-day-tomorrow usage in the postseason | 156 off_next starts | +0.11 logit z 0.9; BF 19.7 vs 19.4 (RS getaway-day term is real, PS is not) | 10/4 | Fable | REPLY_to_BARTOLO_offday_usage_2026-10-04 |
| Rest / travel / schedule spots vs close | | per doc | 9/8 | Fable | SCHEDULE_SPOTS_RESULTS_2026-09-08 |

## Market structure

| test | definition and sample | result | date | by | doc |
|---|---|---|---|---|---|
| Sheet beyond the sportsbook close (totals, ML) | 936 games Jul to Sep | totals t −2.1, ML z −1.8; value at the open only | 9/27 | Fable | PRO_VS_MARKET_FULL_TEST_2026-09-27 |
| Sheet beyond Kalshi game closes (winner, total, TT, F5, RFI) | 853 games | null; opens too wide to monetize except ML at open | 10/3 | Fable | REPLY_to_BARTOLO_kalshi_game_markets_2026-10-03 |
| Sheet at 3h or the close (any game market) | | 71 to 90 % of the lean absorbed by 3h; nothing after | 10/3 | Fable | ALPHA_BEFORE_THE_CLOSE_2026-10-03 |
| Props beat the open (K, outs, HA on two-sided opens) | | outs t 4.3 on open to close but zero CLV; K anti-informative; HA nothing | 10/3 | Fable | KALSHI_PROPS_ALPHA_SCAN §12 |
| Move direction since open as a gate on sheet leans | 850 games | adds 0.00 to 0.04; nothing on results; against-us moves do not raise the bar | 10/7 | Fable | REPLY_sharp_sides_totals_2026-10-07_v2 |
| Kalshi flow: $1k prints, hourly 3+ pt moves, taker imbalance | 3.73M trades | moves price (t 4 to 8), results z ≤ 1.2, −2 to −4 after fee; 3-pt moves reverse −1.8 | 10/7 | Fable | same |
| Steam following on sportsbook ML and totals | close vs own history | ML −8 / −3 %, totals −4 / −6 %; close efficient | 10/7 | Fable | same |
| Pregame momentum on hitter rungs and game markets (3h vs last) | | hitters barely move; game moves of 3+ n 84 / 113 continue +5 either direction se 8 | 10/14 | prop model | ALPHA_DIG_2026-10-14 |
| Liquidity (pregame volume terciles) on central totals | | low +3.7, high +0.9, se 2.2; not separable from September overs | 10/14 | prop model | same |
| Team-total leans from sheet team runs | | sheet 0.19 runs above lines; leans lose at every size over 0.2 (−8 to −19 %) | 10/7 | Fable | REPLY_sharp_sides_totals_2026-10-07_v2 |
| Kalshi F5 ML at the open | 34c spread | p=.5 control passes; unusable | 10/3 | Fable | ALPHA_BEFORE_THE_CLOSE |
| HR anchor (Kalshi mid − as-of HR rate ≥ .08) | 87 triggers | Aug +12.1c, Sep −3.2c; paper only, re-read at 200 | 10/6 | Fable | REPLY3 §3 |
| Hits-allowed YES at the ask | | dead at the ask (11.7c spread); limit at mid only | 10/3 | Fable | REPLY_to_BARTOLO_kalshi_props_backtest |
| RFI vs F5-implied | n 156 | +4.7 YES t 1.6 cheap side, 0 rich side; weak | 10/14 | prop model | ALPHA_DIG |
| Stolen bases 1+, earned runs, inning totals on Kalshi | | SB nothing (.082 → .083); ER and innings too thin | 10/14 | prop model | OTHER_MARKETS_SWEEP_2026-10-14 |
| Moneyline, spreads, F5 spread, F3/F5/F7 winners, inning winners in October vs priors | 1,323 rungs | calibrated; no shift | 10/14 | prop model | same |

## October / series model

| test | definition and sample | result | date | by | doc |
|---|---|---|---|---|---|
| Bullpen (9 constructions incl. Stuff+, leverage, exposure-weighted) at series level | 2012 to 2025 | null | 9/22 | Fable | OCTOBER_RESEARCH_METHODOLOGY_2026-09-22 |
| EV90 / contact composite; bullpen Stuff+; defense × BIP | | retracted ×3 (subgroup, multiple testing) | 9/22 | Fable | same |
| Half-season and rolling splits, power/HR/pull, RISP, two-out, one-run and extras records, streakiness, September W%, Pythag, DER, seeding, park/SOS, interactions, GLM forms | | null (§6.5) | 9/22 | Fable | same |
| Within-series momentum, series lead, elimination games, HFA by game number | | null (t 0.24 / 0.77 / −0.29); game independence holds | 9/23 | Fable | UNTESTED_FACTORS_AUDIT_2026-09-23 |
| Postseason experience (prior-October PA) | | null (t −0.65, wrong sign) | 9/23 | Fable | same |
| Bye-team rust; rested-team G1 offense | 2012 to 2025 | rested 18–16 vs WC winners; G1 is the rested side's best game; the real effect is the G2 rotation flip, in lane 2 | 10/1 | Fable | BYE_TEAM_RUST_RESEARCH_2026-10-01 |
| Per-appearance RP repeat-exposure penalty | 3rd appearance +7 %, p .39 | null; b2b +10 to 13 % and three straight days are real and in the datapoints | 9/27 | Fable | PLAYOFF_BETTING_DATAPOINTS_2026-09-27 |
| Lowering the 4.5 environment by round | | the SP/pen selection is the whole discount; given pitchers used, October is 1.04× | 9/27 | Fable | same |
| October first-inning intercept | pooled 2021 to 2026, 237 games | −0.13 logit, 1σ; not shipped; YRFI live +3.5 is the only term | 10/14 | Fable | OCTOBER_MIDPOINT_CHECK_2026-10-14 |
| 2025 ace regime (P(18+) .61) as the 2026 normal | 22 ace starts 2026 PS | .32; 2021 to 2024 regime; ACE_MULT 1.0 | 10/14 | Fable | same |
| October game total as a prior-supported under | 351 PS games 2016 to 2025 | market already below the October norm; 2026 unders = 2026 running cold | 10/14 | prop model | OTHER_MARKETS_SWEEP_2026-10-14 |
| Hitter-quality tags as sizing inputs on October TB unders | terciles by RS rate vs xwOBA | two cuts disagree; display only | 10/14 | prop model | FABLE_REPLY_october_TB_unders_2026-10-14 |
