-- Sharp tracker schema (SHARP_TRACKER_BUILD_SPEC_2026-10-14 §4).
-- The SQLite file is a derived view rebuilt from the append-only JSONL logs under data/; the logs are the record.

CREATE TABLE IF NOT EXISTS events (
  event_id     TEXT PRIMARY KEY,        -- MLB: statsapi gamePk as text
  sport        TEXT NOT NULL,
  league       TEXT NOT NULL,
  et_date      TEXT NOT NULL,           -- game date in ET
  start_utc    TEXT NOT NULL,           -- scheduled first pitch, last seen
  away         TEXT NOT NULL,
  home         TEXT NOT NULL,
  status       TEXT,                    -- scheduled / final / postponed
  venue_ids    TEXT                     -- json: {"an": 303345, "kalshi": ["26OCT142000MILLAD"], "novig": "..."}
);

CREATE TABLE IF NOT EXISTS ticks (
  ts_utc         TEXT NOT NULL,         -- observed by us
  vendor_ts      TEXT,                  -- vendor timestamp when given
  sport          TEXT NOT NULL,
  league         TEXT NOT NULL,
  event_id       TEXT NOT NULL,
  start_utc      TEXT,
  market         TEXT NOT NULL,         -- ml rl total tt_home tt_away f5_ml f5_rl f5_total f5_tt_* rfi prop_<STAT>
  mclass         TEXT NOT NULL,         -- game | prop
  participant    TEXT,                  -- team code or player_key
  player_name    TEXT,
  line           REAL,
  side           TEXT NOT NULL,         -- home away draw over under
  venue          TEXT NOT NULL,
  price_american INTEGER,
  price_decimal  REAL,
  implied        REAL,                  -- books: 1/decimal; exchanges: ask (cost to take)
  bid            REAL, ask REAL, mid REAL, last REAL, volume REAL,
  "limit"        REAL,
  feed           TEXT NOT NULL,
  flags          TEXT,                  -- comma list: open wide pulled oneside
  extra          TEXT,                  -- json: tickets_pct/money_pct, depth, qty
  src            TEXT NOT NULL          -- log file the row came from
);
CREATE INDEX IF NOT EXISTS ticks_ev ON ticks(event_id, market, line, side, venue, ts_utc);
CREATE INDEX IF NOT EXISTS ticks_ts ON ticks(ts_utc);

CREATE TABLE IF NOT EXISTS results (
  event_id     TEXT PRIMARY KEY,
  status       TEXT NOT NULL,           -- final postponed void
  actual_start TEXT,
  home_score   INTEGER, away_score INTEGER,
  f5_home      INTEGER, f5_away INTEGER,
  rfi          INTEGER,                 -- runs in the first inning (both halves)
  players      TEXT                     -- json: {player_key: {stat: value}}
);

CREATE TABLE IF NOT EXISTS fills (
  fill_id      TEXT PRIMARY KEY,
  ts_utc       TEXT NOT NULL,           -- when the fill row was written
  event_id     TEXT NOT NULL,
  start_utc    TEXT,
  market       TEXT NOT NULL, mclass TEXT NOT NULL,
  participant  TEXT, player_name TEXT, line REAL, side TEXT NOT NULL,
  venue        TEXT NOT NULL,
  price_american INTEGER, price_decimal REAL, implied REAL,
  stake        REAL NOT NULL,           -- units risked
  rule_tag     TEXT NOT NULL,           -- written at fill time
  fair_at_fill REAL, fair_at_fill_status TEXT, fair_at_fill_detail TEXT,
  fair_at_close REAL, fair_at_close_status TEXT,
  late         INTEGER NOT NULL DEFAULT 0,  -- written after start -> excluded from CLV
  result       TEXT,                    -- W L P void
  pnl_units    REAL,
  note         TEXT
);

CREATE TABLE IF NOT EXISTS log_health (
  et_date TEXT, venue TEXT, mclass TEXT, n_ticks INTEGER, n_events INTEGER, first_ts TEXT, last_ts TEXT,
  PRIMARY KEY (et_date, venue, mclass)
);
