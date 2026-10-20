# Test CSV Files

## Auto-detected formats (no manual mapping needed)

| File | Format | Notes |
|------|--------|-------|
| `dukascopy_eurusd.csv` | `dukascopy` | Tick CSV with Local,Ask,Bid,Ask Volume,Bid Volume |
| `mt5_ticks_eurusd.csv` | `mt5ticks` | MT5 tick export (tab separated); a row with one side empty keeps the other side's last quote |
| `mt4_dax.csv` | `mt4` | MT4 bar export with `<DATE>,<TIME>,<OPEN>,...` headers |
| `tradingview_nq.csv` | `tradingview` | TradingView export with `time,open,high,low,close,volume` |
| `generic_labelled.csv` | `generic` (auto) | Generic CSV with obvious column names |
| `generic_iso_datetime.csv` | `generic` (auto) | ISO datetime combined column, auto-detects positionally |
| `random_sensor_log.csv` | `genericticks` (auto) | Quotes: `timestamp_utc`, `px_bid` and `px_ask` are found by their names; a tick each row, bid and ask kept |

## Random / non-standard (need Map Columns dialog)

| File | Format | What to map |
|------|--------|-------------|
| `random_ab123.csv` | `generic` + manual map | Columns: a,b,c,d,e,f: map a→date, b→time, c→open, d→high, e→low, f→close |

The column mapping is in the window only (**Map Columns...**); the command line
has none, so these files are converted from the window.

## Usage

`--tz` is required for the wall-clock formats (mt4, mt5, mt4hst, mt5ticks,
ninjatrader, ninjatraderticks, tradestation, generic, genericticks) and refused
for the rest -- see the "Source timezone" section in [../README.md](../README.md).

```bash
# Auto-detected:
python dataconvert.py testcsvs/dukascopy_eurusd.csv out.bin --symbol EUR-USD --tf 5
python dataconvert.py testcsvs/mt4_dax.csv          out.bin --symbol DEU.IDX-EUR --tf 5 --tz Europe/Helsinki
python dataconvert.py testcsvs/tradingview_nq.csv   out.bin --symbol USATECH.IDX-USD --tf 5

# Generic with obvious labels (auto):
python dataconvert.py testcsvs/generic_labelled.csv out.bin --symbol TEST --tf 5 --tz UTC

# Quotes with bid and ask columns (auto), as raw ticks:
python dataconvert.py testcsvs/random_sensor_log.csv out.bin --symbol EUR-USD --ticks --tz UTC

# MT5 tick export (auto), as raw ticks and as M1 bars:
python dataconvert.py testcsvs/mt5_ticks_eurusd.csv out.bin --symbol EURUSD --ticks --tz Europe/Helsinki
python dataconvert.py testcsvs/mt5_ticks_eurusd.csv out.bin --symbol EURUSD --tf 1 --tz Europe/Helsinki
```

`random_ab123.csv` needs the window's **Map Columns...** (see above).
