# Pre-Wild-Card brief — locked bracket, both lanes. Fable, 2026-09-27 (Sunday night)

Bracket (locked): **AL** 1 TB · 2 CLE · 3 HOU · 4 NYY · 5 BOS · 6 CWS → WC: NYY hosts BOS, HOU hosts CWS; TB gets the NYY/BOS winner, CLE gets the HOU/CWS winner. **NL** 1 MIL · 2 LAD · 3 ATL · 4 SD · 5 CHC · 6 PHI → WC: SD hosts CHC, ATL hosts PHI; MIL gets SD/CHC winner, LAD gets ATL/PHI winner. File: `PLAYOFF_HANDOFF/code/bracket_final_2026.json`.

Both lanes run on this bracket tonight. Lane 1 = v2 October model (pre-registered, shipped) on the 9/24 frozen inputs. Lane 2 = pro chain rolled game-by-game (`lane2_pro_bracket.py` on Bartolo's `project_matchup`), **Sean's platoon construction (`AVERAGE(P,Q,Q)`), live constants 0.75 / 4.75 / 4.79 / 1.31 / 0.06 / 1.0** (matches the constants stamp now in `sheet_projections.json`), projected-October nines with accents fixed, full rested pens, weather/ump 0.

## 1. World Series and pennant, lane 1 vs lane 2

| | WS lane 1 | WS lane 2 | pennant lane 1 | pennant lane 2 |
|---|---|---|---|---|
| LAD | 29.8 | **33.9** | 39.7 | **44.9** |
| MIL | 20.0 | **13.9** | 28.4 | 23.0 |
| NYY | 10.9 | 6.5 | 26.8 | 17.0 |
| TB | 8.5 | 7.0 | 27.7 | 22.0 |
| CHC | 8.2 | 4.2 | 12.0 | 7.4 |
| PHI | 5.8 | 3.7 | 9.3 | 6.9 |
| CLE | 5.5 | 6.6 | 21.5 | 21.6 |
| SD | 4.1 | 4.4 | 7.1 | 8.2 |
| HOU | 2.2 | 6.2 | 9.6 | 17.1 |
| CWS | 1.8 | 1.5 | 7.6 | 5.8 |
| BOS | 1.6 | 6.6 | 6.8 | 16.6 |
| ATL | 1.6 | 5.6 | 3.4 | 9.7 |

Market 9/22 (last read): LAD 32, MIL 11, TB 9. Sean to refresh before pricing.

What moved lane 1 from the marginalized 42.4 (LAD) to 29.8: the locked bracket puts LAD as the 2-seed with the ATL/PHI winner in the LDS and MIL as the 1-seed drawing SD/CHC — the modal bracket had those flipped and LAD hosting the NLCS. Lane 1 now has MIL at 20.0, up from 14.7, for the same reason (1-seed, hosts the NLCS). Lane 2 barely moved (LAD 34.0 → 33.9, MIL 13.8 → 13.9) because it prices the actual pitchers, and the LAD–MIL game grid is the same whoever hosts.

## 2. Where the lanes disagree, and the read

**MIL: 20.0 vs 13.9.** This is the one that changed. Lane 1 rewards the 1-seed structurally (bye + home field in a seeding-standardised model); lane 2 says MIL is a .55 team in every LDS/LCS game not started by Misiorowski, and a bo7 with LAD is 35 % for them even hosting. Last night both lanes agreed at ~14 vs a market 11 and the ruling was "lean". Now lane 1 says 20 and lane 2 says 14 — the lean holds (both above market) but size to lane 2, not lane 1. Lane 1's seeding term is the validated one (99 series) so I don't dismiss it; the honest number is the pair.

**LAD: 29.8 vs 33.9, market ~32.** Both lanes inside the market's noise. No-bet, unchanged.

**BOS / HOU / ATL: lane 2 sees them at 6–7 % WS, lane 1 at ~2 %.** Same disagreement as before: lane 2 prices Suarez/Gray, Brown/Valdez, Sale as real per-game assets in short series; v2's field standardisation scores those lineups (BOS .007, ATL .117 off_fg — ATL is a good lineup, HOU 1.156 is *very* good on the projected nine) against the rotation October-weighting. HOU jumps most: lane 2 has them 63 % over CWS and 54 % over CLE in the LDS (Brown/Valdez vs Williams/Messick is close to even), pennant 17. If the market has HOU under 5 % WS / 12 % pennant, that is a lane-2-only look — log it, don't bet it, lane 2 has priced zero real series.

**CLE: 5.5 / 6.6 WS, 21.5 / 21.6 pennant.** The lanes now agree. Market pennant under 15 remains the look from 9/25.

**NYY: 10.9 vs 6.5.** Lane 2 has NYY only 53 % over BOS (Schlittler v Suarez .509 at the Stadium — the lineups are the reason, BOS off_fg 1.007 vs NYY 1.046 with Judge back, both league-average-plus) and 55 % over TB. Lane 1 likes NYY's lineup xwOBA. No position either way.

## 3. LAD, game by game (lane 2, MIL hosts G1–2, 6–7)

NLCS vs MIL: **LAD 65.2 %**. G1 @MIL Misiorowski v Skubal **.524** (tot 5.3) · G2 Harrison v Snell .546 · G3 @LAD Glasnow v May .617 · G4 Yamamoto v Patrick .626 (tot 7.3) · G5 Skubal v Misiorowski .571 · G6 @MIL .546 · G7 May v Glasnow .566. LAD favoured in all seven; G1 is the coin-flip. NLDS vs ATL 63.6 (Sale G1 .574 for LAD; G3 Holmes v Glasnow .60), vs PHI 68.8. NLCS vs SD 71.8, vs CHC 71.1. WS: vs TB 78.0, CLE 78.6, NYY 72.0.

Wild cards, lane 2 (higher seed hosts all three): NYY over BOS **52.9**; HOU over CWS 63.1; SD over CHC 53.7 (Pivetta v Gausman .544); ATL over PHI 53.0 (Sale v Sánchez .571, Mahle v Wheeler .474). Division series: TB over NYY 44.7 / over BOS 41.7 (TB is the dog to either — Rasmussen/Seymour vs Schlittler/Fried); CLE over HOU 45.6, over CWS 57.4; MIL over SD 55.7, over CHC 55.3.

Rotations used (announced where known): NYY **Schlittler / Fried / Cole** (announced today); SD **Pivetta** G1 (Stammen, 9/25), then King / Buehler / Mize (Mize's role "in jeopardy" — Buehler moved ahead of him); HOU Brown / Valdez / Imai / Gusto (auto — 9/25 pack had only Brown/Imai); the rest still auto-ranked from 9/25 (TB Rasmussen/Seymour/Peralta/Matz; CLE Williams/Messick/Griffin/Cantillo; BOS Suarez/Gray/Bello; ATL Sale/Mahle/Holmes; CHC Gausman/Holmes/Imanaga/Peterson; PHI Sánchez/Wheeler/Painter/Nola; MIL Misiorowski/Harrison/May/Patrick; LAD Skubal/Snell/Glasnow/Yamamoto). Replace as announcements land Monday; the WC grids re-price in seconds.

## 4. Board

- **LAD WS**: no-bet (29.8 / 33.9 vs ~32).
- **MIL WS**: lean holds, size to lane 2 (14 vs market 11); lane 1's 20 is the seeding term and I would not stake on it alone.
- **CLE pennant**: look if market < 15 (both lanes 21–22).
- **HOU pennant**: lane-2-only look at 17 if market < 10; log, don't stake.
- **TB**: both lanes have TB the dog in its own LDS whoever comes through (42–45 %); if the market has TB pennant above 25 (it was 26.5 on lane 1 last week), fade rather than buy.
- Half-stake ruling on the moderate-under band still Sean's.

## 4b. Roster update (Sean, Sunday night): Contreras back for BOS; Judge ambiguous for NYY

Lane 2 re-run with **Willson Contreras at DH for Gasper** (BOS off_fg 1.007 → 1.035) and NYY two ways — Judge in, or Judge out with Caballero back in the nine (the 9/24 patch reversed).

| | Judge in | Judge out |
|---|---|---|
| NYY over BOS (WC, bo3 in NY) | **51.6 %** | **45.8 %** |
| G1 Schlittler v Suarez, NYY | .503 | .460 |
| NYY pennant / WS | 16.5 / 6.3 | 11.1 / 3.5 |
| BOS pennant / WS | 18.1 / 7.6 | 20.3 / 8.5 |
| TB over NYY (LDS) | 44.7 | 51.6 |
| AL pennant TB / CLE / HOU | 21.5 / 21.2 / 16.9 | 23.0 / 22.1 / 17.5 |

Contreras alone makes NYY–BOS a coin flip even with Judge (.529 → .516); without Judge BOS is the favourite in the Bronx. Judge is worth ~6 pp of the series and ~5 pp of pennant to NYY — the single biggest lineup variable on the AL side. Rule stays: nothing is priced on NYY/BOS until the G1 card posts; the Judge-out grid is the one to hold if the market is still pricing him in and the news turns. Everything NL-side is unchanged to 0.1. Files: `lane2_bracket_FINAL_2026-09-27_judge_in.json`, `_judge_out.json` (both with Contreras).

## 5. Caveats and what happens next

Lane 1 is on the 9/24 frozen inputs; the four v2 CSVs get re-harvested on final rosters Monday and the WS table above is re-run (expected movement small — the bracket, not the inputs, moved it tonight). Lane 2 totals still run low on ace games (level, not spread). Every series gets logged with both lanes' p and graded in October; lane 2 stakes nothing until it has a record. Bartolo's side: the constants stamp in `sheet_projections.json` already reads 0.75/4.75/4.79/1.31/0.06/1.0 (confirmed tonight, refresh 19:23Z) — so the futures game-by-game path and the daily path are on the same constants; the remaining item is confirming `plat_vsR/plat_vsL` in `batter_projected.json` is Batter Projected R/W (Sean's construction), and the reconciliation residual at first lock.

Files: `PLAYOFF_HANDOFF/code/lane2_bracket_FINAL_2026-09-27.json` (full game grid, rotations, constants), `lane1_v2_final_bracket_2026-09-27.json`, `bracket_final_2026.json`.
