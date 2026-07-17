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
python dataconvert.py INPUT OUTPUT --symbol SYM --tf MINUTES [--format FMT]

# Examples
python dataconvert.py ticks.csv out.bin --symbol EURUSD --tf 5
python dataconvert.py bars.csv out.bin --symbol DEU.IDX-EUR --tf 1 --format mt4
python dataconvert.py history.hst out.bin --symbol EURUSD --tf 5 --format mt4hst
```

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
shorter header without `payload_type` and `symbol`. BAR5 files can be upgraded
with `migrate_to_v2.py` (not part of this repo); `dataconvert.py --info`
recognizes BAR5 files but only reads BAR6 for conversion.

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
