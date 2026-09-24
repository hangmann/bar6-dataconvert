# Test CSV Files

## Auto-detected formats (no manual mapping needed)

| File | Format | Notes |
|------|--------|-------|
| `dukascopy_eurusd.csv` | `dukascopy` | Tick CSV with LocalTime,Ask,Bid,AskVolume,BidVolume |
| `mt4_dax.csv` | `mt4` | MT4 bar export with `<DATE>,<TIME>,<OPEN>,...` headers |
| `tradingview_nq.csv` | `tradingview` | TradingView export with `time,open,high,low,close,volume` |
| `generic_labelled.csv` | `generic` (auto) | Generic CSV with obvious column names |
| `generic_iso_datetime.csv` | `generic` (auto) | ISO datetime combined column, auto-detects positionally |

## Random / non-standard (need Map Columns dialog)

| File | Format | What to map |
|------|--------|-------------|
| `random_ab123.csv` | `generic` + manual map | Columns: a,b,c,d,e,f — map: a→date, c→open, d→high, e→low, f→close |
| `random_sensor_log.csv` | `generic` + manual map | Columns: timestamp_utc,sensor_id,px_ask,px_bid,vol — tick-like, map: timestamp_utc→date, px_ask→open/high, px_bid→low/close |

## Usage

`--tz` is required for the wall-clock formats (mt4, mt5, mt4hst,
ninjatrader, tradestation, generic) and refused for the rest -- see the
"Source timezone" section in [../README.md](../README.md).

```bash
# Auto-detected:
python dataconvert.py testcsvs/dukascopy_eurusd.csv out.bin --symbol EUR-USD --tf 5
python dataconvert.py testcsvs/mt4_dax.csv          out.bin --symbol DEU.IDX-EUR --tf 5 --tz Europe/Helsinki
python dataconvert.py testcsvs/tradingview_nq.csv   out.bin --symbol USATECH.IDX-USD --tf 5

# Generic with obvious labels (auto):
python dataconvert.py testcsvs/generic_labelled.csv out.bin --symbol TEST --tf 5 --tz UTC

# Needs manual mapping — use GUI "Map Columns" button:
python dataconvert.py testcsvs/random_ab123.csv out.bin --symbol TEST --tf 5 --format generic --tz UTC
```
