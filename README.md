# BAR6 DataConvert

Universal market data → **BAR6** binary converter, built for use with [partiqon](https://partiqon.com).

Converts CSV/tick exports from common trading platforms into the compact BAR6
binary format used by Partiqon's backtesting engine — or use it standalone for
your own tooling.

## Supported input formats

| Format         | Source                                                        |
|----------------|-----------------------------------------------------------------|
| `dukascopy`    | Dukascopy tick CSV (`LocalTime,Ask,Bid,AskVolume,BidVolume`)     |
| `mt5ticks`     | MT5 tick export (`<DATE> <TIME> <BID> <ASK> <LAST> <VOLUME> <FLAGS>`) |
| `ninjatraderticks` | NinjaTrader 8 tick export (`yyyyMMdd HHmmss fffffff;last;bid;ask;volume`) |
| `genericticks` | Any tick CSV whose header names a time, a bid and an ask column  |
| `mt4`          | MT4 bar CSV (`<DATE>,<TIME>,<OPEN>,<HIGH>,<LOW>,<CLOSE>,...,<SPREAD>`) |
| `mt4hst`       | MT4 `.hst` binary history file (versions 400 and 401)            |
| `mt5`          | MT5 bar CSV (same layout as MT4, tab or comma delimited)          |
| `tradingview`  | TradingView export (`time,open,high,low,close,volume`)           |
| `ninjatrader`  | NinjaTrader bar CSV (intraday, or daily bars without a time)      |
| `tradestation` | TradeStation bar CSV                                             |
| `binance`      | Binance klines CSV from [data.binance.vision](https://data.binance.vision/) (12 columns, ms timestamps) |
| `bybit`        | Bybit / Kraken / Coinbase OHLCV CSV (Unix-second timestamps, `open_time` header) |
| `generic`      | Auto-detects any OHLCV CSV (column names or positional), with an optional manual column-mapping dialog in the GUI |

## Usage

### GUI (no arguments)

```bash
python dataconvert.py
```

Pick an input file, let it auto-detect the format (or choose manually), set
symbol + timeframe, and convert. Use **Map Columns...** for CSVs that don't
follow a known layout.

### CLI

```bash
python dataconvert.py INPUT OUTPUT --symbol SYM --tf MINUTES [--format FMT] [--tz ZONE]

# Examples
python dataconvert.py ticks.csv out.bin --symbol EURUSD --tf 5
python dataconvert.py bars.csv out.bin --symbol DEU.IDX-EUR --tf 1 --format mt4 --tz Europe/Helsinki
python dataconvert.py history.hst out.bin --symbol EURUSD --tf 5 --format mt4hst --tz Europe/Helsinki
python dataconvert.py BTCUSDT-1h-2024-01.csv out.bin --symbol BTCUSDT --tf 60 --format binance
python dataconvert.py BTCUSDT_1_2023.csv out.bin --symbol BTCUSDT --tf 1 --format bybit
```

### Source timezone (`--tz`)

A timestamp is only meaningful once you know what timezone it is in,
and that is a property of the SOURCE, not of the format. Formats fall into
three groups:

| Group | `--tz` | Formats |
|---|---|---|
| Each record carries its own offset, or the vendor pins the feed to UTC | not needed; refused | `dukascopy` |
| The value IS an instant (Unix seconds/ms) | not needed; refused | `tradingview`, `binance`, `bybit` |
| A bare wall clock, with nothing to anchor it | **required** | `mt4`, `mt5`, `mt4hst`, `mt5ticks`, `ninjatrader`, `ninjatraderticks`, `tradestation`, `generic`, `genericticks` |

For the third group the import **aborts** rather than guessing. Reading a
broker's EET wall clock as UTC is silently wrong by two or three hours, produces
a correct-looking bar count, and only surfaces later as signals an hour out of
place.

```bash
--tz Europe/Helsinki    # most MT4/MT5 brokers (EET/EEST, DST-aware)
--tz America/New_York   # US exchange local time
--tz Europe/Berlin      # German local time
--tz UTC                # the export is already UTC
--tz +02:00             # a fixed offset that never observes DST
```

Prefer an IANA zone name over a fixed offset unless the source genuinely does
not observe DST: only the zone name gets the transition weekends right.

Zone names work on every computer. Python reads them from the system's time
zone database, which Windows does not have; for that case DataConvert carries
the rules of these zones (from the IANA time zone database, version 2025b):
`Europe/Helsinki`, `Europe/Athens`, `Europe/Kyiv`, `Europe/Berlin`,
`Europe/Paris`, `Europe/Zurich`, `Europe/London`, `Europe/Moscow`,
`America/New_York`, `America/Chicago`, `Asia/Tokyo`, `Asia/Hong_Kong`,
`Asia/Singapore` and `Australia/Sydney`. The log says when it used them. For
any other zone name on such a computer, install the database once with
`python -m pip install tzdata`; a database that Python has is always used
first.

Note that MT4's `.hst` `CTM` field looks like a Unix timestamp and is not one —
it is the broker's server wall clock encoded as though it were UTC — which is
why `mt4hst` needs `--tz` like the CSV formats do.

Utility commands:

```bash
python dataconvert.py --info FILE.bin   # Show header info of a .bin file
python dataconvert.py --formats         # List all supported input formats
```

### Spreads

A backtest pays the spread when an order fills. DataConvert keeps every spread
the source quotes:

| Source | What is kept |
|---|---|
| `dukascopy`, `mt5ticks`, `ninjatraderticks`, `genericticks` | each tick's bid and ask: in raw ticks (`--ticks`) as they are, in bars (`--tf`) as the bar's closing bid and ask |
| `mt4` / `mt5` CSV with a `<SPREAD>` column, `mt4hst` version 401 | the bar's spread (see below) |
| everything else (`tradingview`, `binance`, `bybit`, `ninjatrader` and `tradestation` bars, `generic`, `mt4hst` version 400, MT4 CSVs without `<SPREAD>`) | nothing, because the source quotes none: bid = ask = close |

A file without spreads is still backtested; the backtest then charges the spread
it is configured with.

**Tick exports.** MT5 writes a tick row when the bid or the ask changes and
leaves the other field empty: that side keeps its last value. A row with no
quote at all (a trade on an exchange symbol) keeps the current quote. Rows
before the file has quoted both a bid and an ask cannot be priced and are
counted as skipped. A generic tick CSV needs a header: the bid column is the
one headed `bid`, else the first whose header contains `bid` and names no size
(`px_bid`, `best_bid_price`, not `Bid Volume`); the same for the ask; the time
is the column headed `date`, `datetime`, `timestamp` or `time`, plus a separate
`time` column when the date stands alone. Times keep their milliseconds.

**A spread per bar.** MetaTrader's bar prices are bid prices and its spread is
in points. A point is the symbol's smallest price step: the `.hst` header's
`digits` gives it (10^-digits); for a CSV it is read from the prices, because
MetaTrader writes every price with the symbol's digits (`1.10427`: five
decimals, a point of `0.00001`). The log says which point was used; `--point`
(in the GUI: **Spread point**) sets it. The bar is written the way a backtest
reads every bar:

| Field | Value |
|---|---|
| `open`, `high`, `low`, `close` | the exported price plus half the bar's spread (a mid price) |
| `bid` | the exported close |
| `ask` | the exported close plus the spread |

For example `1.10400,1.10500,1.10300,1.10450` with a spread of 6 points becomes
open 1.10403, high 1.10503, low 1.10303, close 1.10453, bid 1.10450, ask
1.10456. The export gives one spread for the whole bar, so that is the spread
of its every price. A bar whose spread is 0 has bid = ask; a backtest charges
its configured spread for such a bar.

**Wall-clock ticks and the autumn clock change.** Ticks may share a
millisecond, so in the hour that repeats when the clocks go back, the repeated
copy starts where the tick times step back by a second or more.

### NinjaTrader daily bars

A NinjaTrader daily export has no time of day:

```
20240102;16860.75;16878.50;16578.00;16632.50;575012
```

A row of exactly six fields whose first field is a bare date (`yyyymmdd`) is
read as one daily bar: the date, then open, high, low, close and volume.
**A daily bar's time is 00:00 of its date in the source timezone** (`--tz`):
with `--tz UTC` the bar of 2 January 2024 starts at 2024-01-02 00:00 UTC, with
`--tz America/New_York` at 05:00 UTC. Such a file is converted with
`--tf 1440` and is detected as `ninjatrader` automatically. The date and the
time in two fields (`20240102;070000;...`) makes seven fields and is read as an
intraday bar, as before; a six-digit open such as `123000.5` is a price, never
a time.

### Records that cannot be market data

Before any file is written, every record is checked, and the conversion is
**refused** (nothing is written) if one of these is found:

* a high below its low;
* an open or a close outside its own high and low;
* a price (open, high, low, close, bid, ask) of zero or less, or one that is not
  a number (`nan`, `inf`);
* a time before 1990 or after 2100.

The message names the row of the source file (a `.hst` file: the record number),
the rule and the values, so the source can be corrected. A bid above its ask is
possible data and is not refused. A file made by this tool is therefore not one
that Partiqon's upload refuses for its contents.

## Memory

DataConvert holds every row of the input in memory while it converts: about
**313 MB per million rows** (measured with Python 3.13 on tick files, for bar
and tick output alike), roughly 13 times the size of a tick CSV. A year of
EUR-USD ticks (about 28 million rows) needs about **8.6 GB**, a year of BTC
ticks about **15 GB**.

Before it converts, DataConvert estimates the rows of the file and says what
the conversion needs, for example:

```
Memory: about 27,600,000 rows, about 8.7 GB needed to convert (6.1 GB available)
```

When the need is more than the memory available, it stops there with a message
that names both, before reading the file and without writing anything. A file
that covers a shorter period needs less. `--ignore-memory-check` (in the GUI:
"Convert anyway?") converts all the same; a computer that pages to disk may
then still finish, slowly. Where the available memory cannot be read (macOS),
the estimate is shown and nothing is refused.

## Requirements

- Python 3.9+
- Standard library only (GUI requires `tkinter`, usually bundled with Python);
  zone names for `--tz` need nothing extra (see "Source timezone")
- Memory: about 313 MB per million input rows (see [Memory](#memory))

## Test files

Sample CSVs for each supported format are in [`testcsvs/`](testcsvs/), with
usage notes in [`testcsvs/README.md`](testcsvs/README.md). All sample data is
synthetic.

## BAR6 format

BAR6 is a compact little-endian binary format for storing either OHLC bars or
raw bid/ask ticks. Every file starts with a fixed 49-byte header, followed by
a flat array of fixed-size records (bars or ticks — never mixed).

### Header (49 bytes)

| Offset | Size | Field           | Type      | Notes                                  |
|--------|------|-----------------|-----------|-----------------------------------------|
| 0      | 4    | `magic`         | `uint32`  | `0x42415236` (ASCII `"BAR6"`)            |
| 4      | 4    | `version`       | `uint32`  | Currently `2`                            |
| 8      | 1    | `payload_type`  | `uint8`   | `0` = bars, `1` = ticks                  |
| 9      | 4    | `timeframe_min` | `uint32`  | Bar timeframe in minutes; `0` for ticks  |
| 13     | 4    | `record_count`  | `uint32`  | Number of records following the header   |
| 17     | 8    | `start_ts`      | `int64`   | Timestamp of first record, Unix ms       |
| 25     | 8    | `end_ts`        | `int64`   | Timestamp of last record, Unix ms        |
| 33     | 16   | `symbol`        | `char[16]`| ASCII, null-padded, truncated at 16 chars|

Struct format string: `<IIBIIqq16s`

The predecessor format, **BAR5** (`magic = 0x42415235`, `version = 1`), used a
shorter header without `payload_type` and `symbol`. `dataconvert.py --info`
recognizes a BAR5 file and says so; to get a BAR6 file, convert the original
source data again with this tool.

### Bar record (56 bytes) — `payload_type = 0`

| Field       | Type     |
|-------------|----------|
| `ts`        | `int64`  | Unix ms
| `open`      | `float64`|
| `high`      | `float64`|
| `low`       | `float64`|
| `close`     | `float64`|
| `bid`       | `float64`| close-time bid (equals `close` if the source quotes no spread)
| `ask`       | `float64`| close-time ask (equals `close` if the source quotes no spread)

Struct format string: `<qdddddd`

### Tick record (32 bytes) — `payload_type = 1`

| Field    | Type     |
|----------|----------|
| `ts`     | `int64`  | Unix ms
| `bid`    | `float64`|
| `ask`    | `float64`|
| `volume` | `float64`| reserved; always `0.0` in the current writer

Struct format string: `<qddd`

Only the tick formats (`dukascopy`, `mt5ticks`, `ninjatraderticks`,
`genericticks`) can produce tick records: use the `--ticks` CLI flag or the
**"Save as raw ticks"** GUI checkbox. All other formats always write bar
records.

### Bars made from ticks

A tick source converted with `--tf` becomes one bar per `--tf` minutes on the
UTC grid, stamped with the bar's open time. The ticks are put in time order
first; ticks with the same millisecond keep their order in the file. Then:

| Field         | Value                                              |
|---------------|----------------------------------------------------|
| `open`        | mid price, (bid + ask) / 2, of the bar's earliest tick |
| `high`, `low` | the highest and the lowest mid price of the bar's ticks |
| `close`       | mid price of the bar's latest tick                 |
| `bid`, `ask`  | the quote of the bar's latest tick                 |

Every price of such a bar is a mid price, so a backtest that pays the spread
when an order fills does not pay it a second time through the bar's range.

### Inspecting a file

```bash
python dataconvert.py --info FILE.bin
```

prints the parsed header (format version, symbol, payload, timeframe, record
count, time range) and verifies the file size against
`header_size + record_count * record_size`. A file of a format version or a
payload type this tool does not know, or one whose size does not match the
records its header states, is refused with a sentence that says which.

## Changes

### October 2026

* A zone name in `--tz` (`Europe/Berlin`) works on a Python without a time
  zone database, as on Windows: DataConvert carries the rules of the common
  zones and names `python -m pip install tzdata` for any other. Such a zone
  name used to be refused there.
* Tick exports of MT5 (`mt5ticks`) and NinjaTrader (`ninjatraderticks`), and
  any tick CSV whose header names a time, a bid and an ask column
  (`genericticks`), are read with their bid and ask, as raw ticks or as bars.
  Before, only the Dukascopy layout gave ticks, and an MT5 tick export was read
  as bars of the wrong columns.
* The `<SPREAD>` column of an MT4/MT5 bar CSV and the spread field of an
  `.hst` file (version 401) are kept: such a bar is written with mid prices and
  the closing bid and ask (see "Spreads"). They used to be dropped, with
  bid = ask = close. `--point` (GUI: **Spread point**) sets the price of a point.
* Bars made from ticks take their high and low from the mid price, like their
  open and close. They used to be the highest ask and the lowest bid, so the
  same ticks converted again give bars half a spread narrower at each end.
* Ticks that are not in time order in the file are sorted before they become
  bars: a bar's open and close are its earliest and its latest tick, not its
  first and last row.
* A Dukascopy tick file whose header starts with `UTC`, `Local`, `Local time`
  or `Gmt time` has its header recognised; it used to be counted as one
  unreadable row, also in the sample `testcsvs/dukascopy_eurusd.csv`.
* `--info` shows the format version, names the payload once and refuses a file
  it cannot read correctly (see above).
* NinjaTrader exports with the compact time stamp (`20240102 070000`, the date
  and the time in one field or in two) are read on Python 3.9 and 3.10. Every
  row used to be skipped there. A fraction of a second is kept.
* ISO 8601 time stamps are read the same way on every supported Python
  version: the basic form (`20240102T070000`), offsets without a colon
  (`+0100`, `+01`) and fractions of a second of any length, also with a comma.
  On Python 3.9 and 3.10 such rows used to be skipped. Nothing changes on
  Python 3.11 and newer.
* A NinjaTrader daily export (the date without a time, `20240102;O;H;L;C;V`)
  is read: one bar per row, at 00:00 of its date in the source timezone, and
  it is detected automatically. Its rows used to be skipped, and a six-digit
  open was read as a time of day.
* Before converting, DataConvert says how much memory the file needs and stops
  with a message when the computer does not have that much available
  (`--ignore-memory-check` converts anyway).

## License

MIT — see [LICENSE](LICENSE).
