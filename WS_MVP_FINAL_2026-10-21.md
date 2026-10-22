# World Series MVP and series-leader props: what matters, what does not, the engine, the pre-Game-1 board. Fable, 2026-10-21, engine v2 2026-10-22 (consolidates and replaces the 10/20 reply and its four addenda; the archive is in `october/wsmvp/`)

Panel: every World Series 1969 to 2025 (56 series, 326 games, every player's series line from statsapi boxscores), each player's regular-season line that year, regular-season records, LCS boxscores 1985 to 2025, player hands, and 21 BetMGM pre-series MVP boards 2005 to 2025 (SportsOddsHistory). MVP recipients from the statsapi awards endpoint. 2026 inputs: LCS-final lineups, 2026 season rates, the rotations in Bartolo's brief, Sean's series grid (MIL 65.0) and the DK series markets (MIL 58.6 by correct score).

## 1. What matters, in order

1. **Which team wins.** 56 of 56 MVPs came from the winner (Richardson 1960 is the only exception ever). P(MVP) = P(team wins) × share. Nothing else is close.
2. **Whether the winner was the favourite or the dog.** "Dog" means the worse regular-season record (no odds back to 1969). The worse record won 28 of 56. When it did, the MVP was a pitcher 50 % of the time (14 of 28; 2009 to 2025: 3 of 6). When the better record won, 11 % (3 of 28; 2009 to 2025: **0 of 11**). The two-start arm on a worse-record winner takes MVP 21 % of the time (11 of 53); on a better-record winner 6 % (3 of 48). Favourites win with the lineup; dogs win because the ace shut the favourite down.
3. **Series length, through the same channel.** Pitcher MVPs in seven-game series 42 % (8 of 19), in shorter series 24 % (9 of 37). A third appearance by the ace is the mechanism (5 of 18 two-start arms with a third appearance won it; 9 of 83 without).
4. **Lineup slot.** MVP rate per winner bat (10+ series PA): slots 1 to 3 8.9 % (15 of 169), 4 to 6 7.9 % (15 of 191), 7 to 9 5.8 % (9 of 155). Modest, about 1.5× top to bottom, and it is plate appearances.
5. **The series line itself, once it exists, weighted like total bases.** Sean's question answered: splitting hits into singles, doubles-plus-triples and homers, the voters' weights per event come out at about 1 : 2 : 3 (share coefficients 10.0 / 5.0 / 4.9 on bases that are roughly 45 / 12 / 7 per team-series), which is total bases. A single rule, share of team total bases plus share of team RBI, fits as well as the three-stat version and does better out of sample on log-loss (0.869 vs 0.920). Fitted jointly with the pitcher terms on all 56 series: hitters 18.69 × share of team total bases + 6.47 × share of team RBI; pitchers 8.58 × share of team IP + 5.11 × share of team wins − 0.16 × series ERA, plus 2.15 if the winner had the worse record. Leave-one-year-out it names the MVP first 61 % of the time and top three 86 %. Runs scored add a little (z 2.0) and walks nothing (z 0.6). Ex post, 69 % of hitter MVPs led their team in hits, 69 % in HR, 56 % in RBI, 100 % top three in one of them.

## 2. What does not matter (tested, null)

| | result |
|---|---|
| Regular-season quality, given the series line | OPS z coefficient −0.13 (z −0.8); regular-season HR −0.1 (z −0.3). **No star bias.** Voters reward the series line and nothing about the name on it. Pre-G1, OPS is worth 0.34 logit per 80 points, which is mostly slot. |
| Pre-G1 within-team prediction of any kind (2009 to 2025) | LOYO log-loss 2.538 against 2.544 for "pick a regular at random". |
| The market's MVP board (21 boards, 14 with a series price) | log-loss 3.207 against 3.187 for uniform-over-regulars × market P(win) and 3.137 for our blend. Favourite won 4 of 21 at a mean 11 %; median winner 8th on the board. Names priced 6 to 10 % hit 4.9 %; names priced 2 to 4 % hit 4.6 %. |
| Board bias by team | boards put 57 % on the favourite's names against a 60 % series price; no team tilt to exploit. |
| Platoon exposure (share of opponent innings the bat has the advantage against) | null, wrong sign (z −0.6). LHB won 4.6 % per bat, RHB 8.5 %. The Rays' two-lefty pen does not help Mitchell or Yelich. |
| Hot hand entering (LCS production, z within team) | null (z 0.6); team's top-two LCS bats won 6 of 23, expectation 5.5. |
| Part-time and platoon bats (under 400 regular-season PA) | 7 of 121 (5.8 %) vs 32 of 394 regulars (8.1 %); live at roughly their playing-time rate (Pearce, Renteria, Freese). |
| Relievers | 3 of 86 relievers with 4+ appearances or 2+ saves (Fingers 1974, Wetteland 1996, Rivera 1999; all closers, none since 1999). Long relief is not voted. |
| Scoring environment | pitcher MVP 50 % in series under 7 R/G, 29 % above 9; mostly the dog and length effects restated. |
| Position | nothing beyond slot (positions available only as of today; not usable). |

**Not tested, and why:** WPA or clutch (no play-by-play back to 1969; the hits/HR/RBI shares carry it well enough to name the MVP 62 % ex post); umpire, weather, park for a seven-game series (no mechanism to the vote); manager usage patterns beyond the rotation (not observable pre-series); closing MVP boards at exactly G1 for every year (five of 21 snapshots are a game or two in, which favours the market and it still loses).

## 3. The engine (v2, `october/wsmvp/mvp_engine.py`)

Simulates the remaining games from a per-game MIL win probability solved to the series price (Sean 65.0 → .565 ± .03 home/road; market 58.6 → .534). Lineups by opposing starter hand from the last three postseason cards: MIL faces right-handers all series (Chourio, Turang, Contreras, Bauers, Yelich, Mitchell, Pratt, Frelick, Hamilton); TB faces Misiorowski and Henderson in Games 1, 2, 5, 6 (Díaz, Aranda, Hicks, Caminero, Simpson, Mesa, DeLuca, Palacios, Walls) and the left-handers Gasser and Drohan in Games 3, 4, 7 (Vilade and Williamson in for Mesa and Palacios). Bench bats get 0.6 PA a game when not starting. Pinch-hit and early-lift risk from the 2026 play-by-play: a start is a game where the hitter's first PA came by the second inning and not as a pinch hitter; the lift rate is the share of those starts ending at one or two PA, taken at 1.5× for October. True lift rates are small for the regulars (Bauers 1.5 %, Yelich 0.9 %, Frelick 5.0 %, Hamilton 5.8 %, Mitchell 8.2 %, Palacios 12.3 %), so the adjustment is a few tenths of a PA for Mitchell and Palacios and nothing for the others. (A first pass used game-log games with one or two PA, which counted late defensive entries as lifts and cut Bauers and Mitchell wrongly; corrected.) Each hitter's PA (slot rate), hits, doubles-plus-triples, HR and RBI at 2026 rates shrunk toward league; each starter's IP and ER by rotation slot; every reliever's innings by role (Ashby 0.9 a game); wins to the starter at five innings, otherwise the pen. The voter rule above scores the winner's pool with the fitted eligibility (hitters 8+ series PA, pitchers 3+ innings), softmax over all 49 rostered players, so the board sums to 100. Two calibration constants on the pitcher score (−0.13 MIL win, +0.04 TB win) set the pre-G1 pitcher share to history (favourite winner 8 %, dog winner 45 %).

Lane B: the same engine with each hitter's rates scaled halfway toward what DK's most-hits / most-HR / most-RBI boards imply for his share of team production (ratio of market to engine team-leader probability, square-rooted, capped 0.5 to 1.6). The blend is the average of the two lanes and is what the sizing uses; where the lanes disagree is the uncertainty.

**After each game:** series score and each player's line so far into `state.json` as `[h, 2b+3b, 0, hr, rbi, ip, er, w, pa]`, then `python3 mvp_engine.py --wins MIL=1,TB=0 --p_series 0.70 --state state.json` (add `--laneB 1` for the second lane) with the updated series price. The market's correct-score devig is the series input for "what the market should think"; Sean's grid for "what we think".

## 4. Pre-Game-1 board (`WS2026_MVP_PROJECTIONS_preG1.csv`, all 49 rostered players, sums to 100; both series prices, lane B, blend, Kalshi YES/NO asks, book consensus and devig, edges after fee)

| name | team | pure (Sean 65) | lane B | **blend** | market 58.6 | Kalshi YES ask | books devig | read |
|---|---|---|---|---|---|---|---|---|
| Chourio | MIL | 11.5 | 12.1 | **11.8** | 10.4 | 15 | 11.2 | Kalshi over; books fair |
| Contreras | MIL | 8.2 | 8.9 | **8.5** | 7.4 | 14 | 10.3 | Kalshi over |
| Turang | MIL | 9.1 | 8.1 | **8.6** | 8.2 | 9.6 | 8.3 | fair |
| Bauers | MIL | 8.5 | 7.8 | **8.1** | 7.7 | 7.5 | 6.0 | fair to slightly under |
| Yelich | MIL | 5.9 | 9.7 | **7.8** | 5.2 | 7 | **3.0** | books under on both lanes |
| **Rasmussen** | TB | 6.5 | 6.2 | **6.4** | 7.8 | 5.5 | **3.7** | under at the books, 1.8× |
| Caminero | TB | 3.9 | 6.6 | **5.2** | 4.6 | **10** | 9.3 | over at both |
| Mitchell | MIL | 4.9 | 3.5 | **4.2** | 4.4 | 2.8 | 2.1 | under |
| Frelick | MIL | 3.7 | 4.6 | **4.2** | 3.3 | 0.7 | 0.5 | under on both lanes (DK's own props rate him) |
| Pratt | MIL | 4.8 | 3.0 | **3.9** | 4.4 | 1.1 | 0.9 | under (thin) |
| Díaz | TB | 3.3 | 3.6 | **3.4** | 3.8 | 9 | 6.2 | over |
| Aranda | TB | 3.1 | 3.2 | **3.2** | 3.7 | 5.3 | 5.7 | over |
| Peralta | TB | 2.8 | 2.7 | **2.7** | 3.3 | 1.9 | 1.9 | slightly under |
| Hamilton | MIL | 3.2 | 2.1 | **2.6** | 2.9 | unlisted | 1.0 | under (thin) |
| **Misiorowski** | MIL | 2.4 | 2.4 | **2.4** | 2.1 | **12** | 8.6 | over at both; favourite's ace |
| Hicks | TB | 2.6 | 1.9 | **2.3** | 3.0 | unlisted | 1.5 | about fair |
| DeLuca | TB | 2.2 | 1.3 | **1.7** | 2.6 | unlisted | 0.6 | under (thin) |
| Jax | TB | 1.6 | 1.5 | **1.6** | 1.9 | unlisted | 0.6 | under (dog's third starter) |
| Simpson | TB | 1.6 | 1.0 | **1.3** | 1.9 | 1.7 | 1.6 | fair |
| Henderson | MIL | 1.1 | 1.0 | **1.1** | 0.9 | unlisted | 1.7 | over |
| TB relievers (Baker, Kelly, Wells, Booser, Seymour, Matz, Sulser) | | 0.3 to 0.7 each | | 3.4 total | | | 0.1 to 0.6 | nothing |
| Ashby | MIL | 0.4 | 0.4 | **0.4** | 0.3 | 2.2 | 0.6 | Kalshi over |
| Mesa, Vilade, Walls, Palacios, other MIL relievers, bench | | 0.1 to 0.9 each | | | | | | nothing |

What the total-bases rule changed against the hits rule: Bauers up (power), Mitchell and Turang up a little (doubles), Contreras and Chourio down a little; nothing crosses a line. What lane B changes: Yelich and Caminero up (the leader props rate both), Mitchell and Pratt down, Frelick up (DK's hits board has his share above Mitchell's).


**Confidence by tier:** team and dog/favourite structure, high; the total-bases voter rule, high ex post and moderate as a pre-series input; ordering among the top five bats on each side, moderate; bottom-of-order and bench names, low individually and moderate as a group (history gives a slot 7 to 9 bat 5.8 % per series, the books give these three about 0.7 %, the engine is between). Those are 0.05u stabs, not positions.

## 5. The book

| position | venue | price | blend | size |
|---|---|---|---|---|
| YES Rasmussen | books | +2000 | 6.4 | 0.15u |
| NO Misiorowski | Kalshi | NO ask 89 | NO fair 97.6 | 0.15u |
| NO Caminero | Kalshi | NO ask 91.3 | NO fair 94.8 | 0.1u |
| NO Contreras | Kalshi | NO ask 88 | NO fair 91.5 | 0.1u |
| NO Chourio | Kalshi | NO ask 86 | NO fair 87.5 | paper; no edge after fee |
| YES Yelich | books | +2500 | 7.8 | 0.1u |
| YES Mitchell | books | +3500 | 4.2 | 0.05u |
| YES Frelick | books | +14586 consensus, +4000 at DK | 4.2 | 0.05u at the consensus price only |
| YES Hicks | books | +5000 | 2.3 | paper |
| YES Peralta | books | +4000 | 2.7 | paper |
| Simpson most SB | DK | +450 | 21 vs 18 implied | 0.1u |
| Ashby, Pratt, Hamilton, Jax, DeLuca | | | thin | no |

Net MVP exposure about 0.8u. Chourio's NO at Kalshi is one point after fee and stays paper. Four of the positions pay if TB wins (Rasmussen, the three MIL NOs) and three if MIL wins (Yelich, Mitchell, the Caminero NO), so the book nets slightly TB, which is the right offset to a MIL series position. Series leaders other than Simpson SB: every DK line is above the exposure Monte Carlo under a 30 % overround; paper.

The series itself: Sean −180 (64.3) against −160 listed (58.6 by DK's correct score). The length markets match his grid within a point; MIL in six or fewer is 74 % of MIL wins on both. If the 64 holds, the MIL series line is the bet, not anything on the MVP board, which already leans MIL harder than the series market does.

## 6. Files

`october/wsmvp/`: `ws_panel_1969_2025.pkl` (every player-series line, regular-season stats, records, slot, appearances), `expost_beta.json` (voter rule), `expost_pool.pkl`, `mvp_engine.py`, `ws26_hitters.pkl`, `ws26_pitchers.json`, `ws26_engine_preG1.pkl`, `wsmvp_odds_covers.json` and `wsmvp_market_test.pkl` (21 boards), `lcs_box_1985_2025.pkl`, `hands.json`, `winners_enriched.pkl`, the v1 reply with addenda. Root: `WS2026_MVP_PROJECTIONS_preG1.csv`.
