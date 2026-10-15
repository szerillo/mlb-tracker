# Process rules, agreed 2026-10-14. One page. Applies to Fable, Bartolo, the prop model.

1. **One constants file, one owner (Bartolo).** `constants.json` in the repo holds every named constant: value, fit date, sample, refit trigger, doc. Every script reads from it; nothing is hard-coded. A ruling is applied when the file shows the new value with the date, not when a doc says so. Fable reviews the diff of the file.

2. **Attach the file or do not cite it (everyone).** A doc that references data names the repo path. A reply is not sent until the file it references is in the uploads. "Attached" means attached.

3. **Definitions travel with results (prop model owns the template).** Any test touching a schedule, a timestamp, a listing set, or a window states its definition in one line above the table: unit (game or team-game), clock (local or Eastern), thresholds, window, sample. A second test of the same thing uses the first definition or says why not.

4. **No ruler, no grade (Bartolo owns the gate).** `fill_log.py` refuses a row without mid-at-fill. The scan prints one line per day: rows logged / rows with ruler / rows after first pitch. Rows without a ruler or logged after first pitch are excluded from CLV and from every demotion and promotion rule.

5. **DEAD.md is the dead list (Fable owns, everyone appends).** Every closed test goes in with definition, sample, result, date, who, doc. A brief proposing a test checks DEAD.md first and cites the row if it is reopening one, with the reason (new source, new definition, or a pre-registered refit in PINNED §D).

6. **One reply per round (everyone).** A day's results go in one doc with numbered sections. No duplicate sends; a correction to a sent doc is a new section in the next one, not a resend.

Standing rules unchanged: no unnamed constants; one input one channel; backtest before wiring; the model never carries the market as an ingredient; run it rather than desk-check it; nothing in METHODOLOGY_INDEX or DEAD.md is retested without a stated reason.
