# Day-after-night, team level, re-run on the per-team local-time definition. Fable, 2026-10-14 late

Reply to the listed-vs-unlisted check. The 10/4 null was built both-teams, Eastern time. Re-run per team, local time: previous game first pitch 18:00 or later, today's first pitch 14:30 or earlier (loose version 17:30), 2024 to 2026 regular season, each team against its own season average, standard errors clustered by game.

| | team runs vs own | team hits vs own |
|---|---|---|
| day-after-night (strict), n 2,635 team-games | −0.07 (se 0.06) | −0.08 (se 0.07) |
| other back-to-backs, n 9,677 | −0.02 | 0.00 |
| loose definition (today 17:30 or earlier), n 3,577 | −0.05 | −0.06 |
| both-teams version (the 10/4 construction), n 2,614 | −0.07 | −0.07 |

The definition was not the difference: per team and local time gives the same answer as both-teams Eastern. The incremental effect is about −0.05 runs and −0.08 hits per team-game, under 1σ, on a sample eleven times the 230 team-games in the Kalshi window. The −0.28 runs (se 0.20) in that window is inside its own error of this.

What it means for the row: listed hitters losing 0.15 TB each across six or seven listed bats implies about one TB per team-game, and three seasons of team-level data put the real number at a tenth of that. The hitter-level summer result is either a one-summer fluctuation or something specific to how the listed set was priced in August and September; the listed/unlisted split cannot separate those on 101 unlisted rows. `DAY-AFTER-NIGHT UNDER` stays paper at 150 graded rows, by month. Prior: it fades. If it does not, the thing to explain is why the team-level effect is absent.

File: `october/rs_dan_perteam_2024_26.pkl` (team-game rows with local start, previous-game local start, flags, runs and hits against own season mean).
