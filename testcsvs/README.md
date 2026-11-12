# Test CSV Files

## Auto-detected formats (no manual mapping needed)

| File | Format | Notes |
|------|--------|-------|
| `dukascopy_eurusd.csv` | `dukascopy` | Tick CSV with Local,Ask,Bid,Ask Volume,Bid Volume |
| `SYN.IDX-USD_1Minute_ASK_2026-03-02_08_00-08_04_Etc_UTC.csv` | `dukascopyweb` | Dukascopy web export, five M1 bars of the ASK side; the name says the bar size and the side (made-up prices) |
| `SYN.IDX-USD_1Tick_BID_2026-03-02_12_00-12_01_Etc_UTC.csv` | `dukascopyweb` | Dukascopy web export, five ticks of the BID side, two sharing a second (made-up prices) |
| `mt5_ticks_eurusd.csv` | `mt5ticks` | MT5 tick export (tab separated); a row with one side empty keeps the other side's last quote |
| `mt4_dax.csv` | `mt4` | MT4 bar export with `<DATE>,<TIME>,<OPEN>,...` headers |
| `tradingview_nq.csv` | `tradingview` | TradingView export with `time,open,high,low,close,volume` |
| `generic_labelled.csv` | `generic` (auto) | Generic CSV with obvious column names |
| `generic_iso_datetime.csv` | `generic` (auto) | ISO datetime combined column, auto-detects positionally |
| `random_sensor_log.csv` | `genericticks` (auto) | Quotes: `timestamp_utc`, `px_bid` and `px_ask` are found by their names; a tick each row, bid and ask kept; `vol_lots` is not read as volume |

## Files with traded volume (made-up prices and volumes)

| File | Format | Volume written |
|------|--------|----------------|
| `SYN.IDX-USD_ninjatrader_M1.txt` | `ninjatrader` | four M1 bars: 120, 0, 35, 8 |
| `SYN.IDX-USD_ninjatrader_ticks.txt` | `ninjatraderticks` | ticks 2, 0, 3, 5, 1; as M1 bars 5 and 6 |
| `SYNUSDT-1m-2026-03-02.csv` | `binance` | three M1 bars: 1.25, 0.5, 2 (column 6) |
| `bybit_synusdt_1m.csv` | `bybit` | three M1 bars: 3.5, 0, 1.5 (header `volume`) |
| `generic_ticks_volume.csv` | `genericticks` | ticks 2, 0, 3, 4; as M1 bars 5 and 4 |

`tradingview_nq.csv` and `generic_labelled.csv` are written with their volume
column too. `mt4_dax.csv` (`<VOL>` 0 on every row) and `mt5_ticks_eurusd.csv`
(no volume on any tick) are written without volume, and the conversion says
why; so are the Dukascopy files, whose sizes are quoted sizes.

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

# Dukascopy web export (bars at their own size; ticks of one side):
python dataconvert.py testcsvs/SYN.IDX-USD_1Minute_ASK_2026-03-02_08_00-08_04_Etc_UTC.csv out.bin --symbol SYN.IDX-USD --tf 1
python dataconvert.py testcsvs/SYN.IDX-USD_1Tick_BID_2026-03-02_12_00-12_01_Etc_UTC.csv   out.bin --symbol SYN.IDX-USD --ticks

# Generic with obvious labels (auto):
python dataconvert.py testcsvs/generic_labelled.csv out.bin --symbol TEST --tf 5 --tz UTC

# Quotes with bid and ask columns (auto), as raw ticks:
python dataconvert.py testcsvs/random_sensor_log.csv out.bin --symbol EUR-USD --ticks --tz UTC

# With traded volume:
python dataconvert.py testcsvs/SYN.IDX-USD_ninjatrader_M1.txt    out.bin --symbol SYN.IDX-USD --tf 1 --format ninjatrader --tz UTC
python dataconvert.py testcsvs/SYN.IDX-USD_ninjatrader_ticks.txt out.bin --symbol SYN.IDX-USD --ticks --format ninjatraderticks --tz UTC
python dataconvert.py testcsvs/SYNUSDT-1m-2026-03-02.csv         out.bin --symbol SYNUSDT --tf 1 --format binance
python dataconvert.py testcsvs/bybit_synusdt_1m.csv              out.bin --symbol SYNUSDT --tf 1 --format bybit
python dataconvert.py testcsvs/generic_ticks_volume.csv          out.bin --symbol SYN --ticks --format genericticks --tz UTC

# MT5 tick export (auto), as raw ticks and as M1 bars:
python dataconvert.py testcsvs/mt5_ticks_eurusd.csv out.bin --symbol EURUSD --ticks --tz Europe/Helsinki
python dataconvert.py testcsvs/mt5_ticks_eurusd.csv out.bin --symbol EURUSD --tf 1 --tz Europe/Helsinki
```

`random_ab123.csv` needs the window's **Map Columns...** (see above).
