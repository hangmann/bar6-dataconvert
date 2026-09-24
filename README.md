# BAR6 DataConvert

Universal market data → **BAR6** binary converter, built for use with [partiqon](https://partiqon.com).

Converts CSV/tick exports from common trading platforms into the compact BAR6
binary format used by Partiqon's backtesting engine — or use it standalone for
your own tooling.

## Supported input formats

| Format         | Source                                                        |
|----------------|-----------------------------------------------------------------|
| `dukascopy`    | Dukascopy tick CSV (`LocalTime,Ask,Bid,AskVolume,BidVolume`)     |
| `mt4`          | MT4 bar CSV (`<DATE>,<TIME>,<OPEN>,<HIGH>,<LOW>,<CLOSE>,...`)    |
| `mt4hst`       | MT4 `.hst` binary history file (versions 400 and 401)            |
| `mt5`          | MT5 bar CSV (same layout as MT4, tab or comma delimited)          |
| `tradingview`  | TradingView export (`time,open,high,low,close,volume`)           |
| `ninjatrader`  | NinjaTrader bar CSV                                              |
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
| A bare wall clock, with nothing to anchor it | **required** | `mt4`, `mt5`, `mt4hst`, `ninjatrader`, `tradestation`, `generic` |

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

Note that MT4's `.hst` `CTM` field looks like a Unix timestamp and is not one —
it is the broker's server wall clock encoded as though it were UTC — which is
why `mt4hst` needs `--tz` like the CSV formats do.

Utility commands:

```bash
python dataconvert.py --info FILE.bin   # Show header info of a .bin file
python dataconvert.py --formats         # List all supported input formats
```

## Requirements

- Python 3.9+
- Standard library only (GUI requires `tkinter`, usually bundled with Python)

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
| `bid`       | `float64`| close-time bid (equals `close` if no bid/ask source data)
| `ask`       | `float64`| close-time ask (equals `close` if no bid/ask source data)

Struct format string: `<qdddddd`

### Tick record (32 bytes) — `payload_type = 1`

| Field    | Type     |
|----------|----------|
| `ts`     | `int64`  | Unix ms
| `bid`    | `float64`|
| `ask`    | `float64`|
| `volume` | `float64`| reserved; always `0.0` in the current writer

Struct format string: `<qddd`

Only tick-based input formats (currently `dukascopy`) can produce tick
records — use the `--ticks` CLI flag or the **"Save as raw ticks"** GUI
checkbox. All other formats always write bar records.

### Inspecting a file

```bash
python dataconvert.py --info FILE.bin
```

prints the parsed header (symbol, timeframe, record count, time range) and
verifies the file size against `header_size + record_count * record_size`.

## License

MIT — see [LICENSE](LICENSE).
