# October mid-point check: ace leash, first-inning intercept, scoring level. Fable, 2026-10-14

Two items pinned "after the LDS" and one level check, run on the 2026 postseason through 10/13 (29 games, 58 starting-pitcher lines, statsapi linescores and boxscores). Files: `october/ps26_starts.pkl`, `october/ps_linescores_2021_26.pkl`.

## 1. Ace leash: 2026 is the 2021 to 2024 regime, not 2025. ACE_MULT stays 1.0 and the "avoid aces" rule is lifted

Starters by 2026 season FIP class (hazard v4 definitions: ace under 3.30, back-end 4.10 and up), openers removed (fewer than 8 season starts, or under 40 pitches and 9 outs or fewer).

| class | starts | mean outs | SD | P(15+) | P(18+) | mean pitches |
|---|---|---|---|---|---|---|
| ace | 22 | 15.1 | 3.8 | .55 | **.32** | 87 |
| mid | 16 | 14.1 | 4.0 | .50 | .19 | 76 |
| back-end | 8 | 13.6 | 4.4 | .38 | .25 | 68 |

v4 was calibrated on PS 2021 to 2024 at ace mean 14.9, P(15+) .62 to .65, P(18+) .34 to .36; the 2025 hold-out ran .61 on P(18+) with a bimodal SD of 7, and the open question was which October 2026 would be. The answer is 2021 to 2024: ace P(18+) .32 on 22 starts, SD 3.8 (unimodal), hooks clustered at 87 pitches. The 2025 ride-the-ace pattern did not carry over. Rulings:

- `ACE_MULT` 1.0 (already set for the LCS) is confirmed for the rest of October; do not revisit toward 0.5 or 0.35.
- The "avoid aces either side" class exclusion from 10/3 is lifted; aces are inside v4's demonstrated accuracy on this sample. The 0.5 sim / 0.5 market blend and the 10-point raw-gap cap stay.
- Mid starters (P(18+) .19 against .22 to .27 calibrated) remain the best-behaved class. Back-end is 8 starts; nothing to say.
- Watch: ace pitch counts. 87 is the number, and 100 was crossed twice in 22 starts (Rasmussen 101, Schlittler 117, Sánchez 116 in the WC round). An ace 18+ outs price above .40 needs a pitch budget over 95 to justify it.

## 2. First-inning October intercept: −0.13 logit on 237 games, 1σ, not shipped

| sample | n | NRFI | first-inning share of runs |
|---|---|---|---|
| PS 2021 to 2025 | 208 | .476 | .135 |
| PS 2026 to date | 29 | .517 | .124 |
| PS 2021 to 2026 pooled | 237 | .481 | .134 |
| RS 2026 / empirical curve at the October totals | | .514 | .119 |

Pooled October NRFI against the curve is logit(.481) minus logit(.514) = **−0.13**, with a standard error on the pooled rate of about .032 in probability, so the intercept sits about 1σ from zero. 2026 so far reads as a regular-season first inning (.517, n 29). Ruling: no October intercept in engine v3 or in the YRFI rule's curve. The YRFI rule stays YES-only off the closing total, which is the expression of the October lean that does not depend on the intercept being real. Re-read at the end of the World Series (about 270 games); it needs roughly 600 October games to clear 2σ at this size, which is 2030.

## 3. Scoring level: 6.93 runs per game, low, and unreadable without the closes

| | n | runs per game | F5 share |
|---|---|---|---|
| PS 2026 to date | 29 | 6.93 | .562 |
| PS 2021 to 2025 | 208 | 8.10 | .579 |
| by round, 2021 to 2026: WC / DS / CS / WS | 48 / 99 / 61 / 29 | 6.71 / 7.82 / 8.98 / 8.35 | |
| RS 2026 | | 8.99 | |

The 2026 number is 1.5σ under the prior Octobers (game SD about 4 runs) and the WC plus DS rounds are the low-scoring rounds in every year, so most of the gap is the round mix. The datapoint that matters, actual against the sheet's October-adjusted closes, needs the paired archive from Bartolo (sheet total, close, result per game); the level check pinned for "about 90 games by the end of the LDS" waits on that file. F5 share .562 is inside the .573 constant's noise at n 29.

## Rulings and rows

| item | status |
|---|---|
| ACE_MULT | 1.0 confirmed on 22 ace starts (P(18+) .32 vs .34 to .36 calibrated); 2025 regime did not persist |
| aces on outs | class exclusion lifted; blend and cap unchanged |
| October first-inning intercept | −0.13 logit, 1σ, not shipped; YRFI rule unchanged; re-read after the WS |
| October scoring level vs closes | waits on the paired archive (sheet total / close / result) |
