# Rulings: October TB unders, other-markets sweep, alpha dig. Fable, 2026-10-14

Three docs received (`FABLE_REPLY_october_TB_unders_2026-10-14.md`, `OTHER_MARKETS_SWEEP_2026-10-14.md`, `ALPHA_DIG_2026-10-14.md`). All three are well built: as-of features, game-clustered errors, month splits, a selection test where one was needed, and a dead list so nothing gets retested. Rulings below; nothing goes live that was not already live.

## 1. October TB unders: ruled as written, lean

Accepted in every particular: TB 3+ fair = mid + 2.5, TB 2+ fair = mid + 3, TB 3+ first, tags `[info: ...]` and display only, 0.1 to 0.15u total per game across all TB unders, half weight against that cap when the card already holds a game-total or team-total under on the same game, limit at the mid or better, never the ask. Row names unchanged. `HITTER UNDER vs SP` paper at Tier 4 with the `(play)` branch removed.

The demotion rule is accepted as the number on the running line: TB 3+ after fee below +1.5 over 10 or more further games drops TB 2+ to paper; negative drops both. Add one trigger: if TB 3+ is still above +1.5 but TB 2+ runs negative over 10 further games on its own, TB 2+ drops alone.

Why it is a lean and not a rule, for the record: the selection test is on the 24 games the rule was built on plus 5 new ones; the independent evidence is the nine-season prior, and the prior-based residual after fee is about +0.5 on TB 2+ and +1.3 on TB 3+. That is the size of the expectation. The 29-game +4.3 is half this October running cold, as the reply says itself.

Context from the PlusEV thread read today: sportsbooks carry a 2-point over tax on props across sports and Kalshi shows none of it. So this is not the tax; it is an October-specific discount on Kalshi's hitter rungs that the market applies to hits and under-applies to extra bases. Different mechanism, and it means the lean does not transfer to book TB unders without its own grade.

Log fields, same as every prop fill: mid at fill, mid at close, rule tag before first pitch, result. The 10/14 fills file had the ruler on 9 of 31 rows; the TB unders are the rows most in need of it.

## 2. Other-markets sweep: paper rows accepted, two notes

`OCTOBER F5 UNDER (paper)` (NO on 5+ and 6+, fair from 0.573 × full-game fair total, fires at 3 points under the mid) and `OCTOBER F7 TIE NO (paper)` (fair = 1 − mid + 3) are accepted as Tier 4 paper with the stated promotion bar (30 graded rows, positive at the mid after fee, running line above the prior number). Both are one per game; they are 2027 rows in practice.

Note 1, F5 share constant. The sweep's 2016 to 2025 F5 share is .567 (4.58 / 8.07); my 2021 to 2025 read was .587 on 208 games and 2026 to date is .562 on 29. The .573 constant sits between them and stays. No change to the F5 construction.

Note 2, YRFI. The correction in section 3 is right: the live YRFI rule's +3.5 points is the October lift and there is no second October term to add. The duplicate paper YRFI row is removed. My mid-point check today has the pooled 2021 to 2026 October NRFI at .481 against the curve's .514 (−0.13 logit, 237 games, about 1σ), so the live +3.5 is the fair middle and it stays.

Game total: agreed, and it matches the 10/7 correction. The October market is already below the nine-year October norm; 2026 unders winning is 2026 running cold, not a bias. The tool's calibrated number governs; no October total rule in either direction.

## 3. Alpha dig: two paper rows accepted, one live-rule change held back, one check requested

**F5 OVER: high F5 share (paper).** Accepted, regular season only, as written. It is the same under-pricing as the 10/3 cross-venue finding (Kalshi F5 book at .531 × the full total against a .56 to .57 real share), now located: the gap lives in the top quartile of Kalshi's own F5 share. Both months positive, t 2.5.

**Held back: the "top quartile, size up" tag on the live `F5 OVER` rule.** The tag may print; sizing does not change. A quartile cut from one summer is a new sizing input from a twelve-bucket family, which is the kind of thing the sub-tags on TB unders just failed to be. Grade the live `F5 OVER` fills by F5-share quartile from today; if Q4 carries the rule at 100 fills, size by quartile then, with the number fitted rather than "size up".

**DAY-AFTER-NIGHT UNDER (paper).** Accepted at 150 graded rows, catchers first. One check before it is believed: my 10/4 linescore test found no day-after-night effect on team runs (RS 2024 to 2026, 1,334 games, 8.82 against 8.83; postseason 33 games, null). If hitters' TB 2+ comes in 3.5 points under the mid in those games while team runs do not drop, then either the mids are high in those games (a market-level thing, which is fine and bettable) or the results are low and the runs are being made up elsewhere, which is not coherent. Split it: result minus the hitter's own as-of rate (no market) and mid minus own rate, both for day-after-night against all other games. If the first is near zero and the second is positive, the rule is "the market over-prices hitters in day-after-night games" and the fair construction should say so.

**TB 3+ when the market marks a hitter 6+ points above his own rate: watch list, agreed.** It rhymes with the HR anchor from 10/6 (Kalshi marks a low-power hitter up, and is right and then some), which was +12 cents in August and −3 in September. Same shape, same caution: follow-the-markup leans are CLV statements about the market being right, and they have not survived a month split yet.

**Dead list: accepted and entered in the index.** Umpire (matches the 10/14 live-zone null and the walks file), pen fatigue (matches the 10/3 pen-load null), K multi-factor (matches every K test we have run), pregame momentum (matches the 10/7 move-direction null), start time (matches the dome and day-game work), sibling rungs (lookahead caught, good), rust, openers, platoon part-timers in season, own-rate vs mid.

**Next tests, in his order, with two additions.** Books vs Kalshi on the same rung is the cross-venue tick-log test and should run the moment two weeks exist; it is also the first thing the sharp tracker will produce once it has NBA ticks. Lineup-post timing: add the lineup K% feature from the 10/3 scan (K prop moves 0.57 cents per SD of lineup K% at post), so the test grades whether the mid completes the move or stops partway. F5 share by starter profile: use the hazard's expected outs as the profile variable, since if Q4 is "expected outs under 15" the rule fires pregame off the sim. Weather as-of: the HR weather pull from 10/6 carries the as-of forecast fields; reuse that file rather than waiting on a feed.

## Rulings table

| item | ruling |
|---|---|
| OCTOBER TB UNDER (TB 3+ / TB 2+) | lean, offsets +2.5 / +3, 0.1 to 0.15u per game, half weight with a game or TT under on the card, mid only; demotion rule as stated plus TB 2+ alone on its own line |
| hitter-quality tags | display only; no sizing from tags |
| HITTER UNDER vs SP | paper, Tier 4, `(play)` branch removed |
| OCTOBER F5 UNDER, OCTOBER F7 TIE NO | paper, Tier 4, promotion bar as stated |
| F5 share constant | .573 unchanged |
| October YRFI | live rule's +3.5 is the term; no second term; duplicate paper row removed |
| October game total | no rule either way; tool number governs |
| F5 OVER: high F5 share | paper, RS only |
| F5 OVER size-up tag | tag prints, sizing unchanged; grade live fills by quartile, revisit at 100 |
| DAY-AFTER-NIGHT UNDER | paper at 150 rows; split result-vs-own-rate from mid-vs-own-rate first |
| TB 3+ market markup bucket | watch list |
| dead list (10 items) | entered; do not retest |
| fills log | mid at fill, mid at close, tag before first pitch, on every row |

**BARTOLO:** nothing new to render beyond the row names in the three docs. The `F5 OVER` quartile tag is Info text, not a size input.
