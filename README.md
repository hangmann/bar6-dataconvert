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

## License

MIT — see [LICENSE](LICENSE).
