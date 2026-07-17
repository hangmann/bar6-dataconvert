#!/usr/bin/env python3
"""
DataConvert - Universal market data -> BAR6 binary converter.

Supported input formats:
  dukascopy    Dukascopy tick CSV  (LocalTime, Ask, Bid, AskVolume, BidVolume)
  mt4          MT4 bar CSV  (<DATE>,<TIME>,<OPEN>,<HIGH>,<LOW>,<CLOSE>,...)
  mt4hst       MT4 .hst binary history file (versions 400 and 401)
  mt5          MT5 bar CSV  (same layout as MT4, tab or comma delimited)
  tradingview  TradingView export  (time, open, high, low, close, volume)
  ninjatrader  NinjaTrader bar CSV  (date+time semicolon or comma separated)
  tradestation TradeStation bar CSV  (Date, Time, Open, High, Low, Close, ...)
  generic      Auto-detect any OHLCV CSV (column names or positional)

CLI usage:
  python dataconvert.py INPUT OUTPUT --symbol SYM --tf MINUTES [--format FMT]
  python dataconvert.py ticks.csv out.bin --symbol DEU.IDX-EUR --tf 5
  python dataconvert.py bars.csv out.bin --symbol USATECH.IDX-USD --tf 1 --format mt4
  python dataconvert.py history.hst out.bin --symbol EURUSD --tf 5 --format mt4hst

Utility commands:
  python dataconvert.py --info FILE.bin       Show header info of a .bin file
  python dataconvert.py --formats             List all supported input formats

GUI (no arguments, or --gui):
  python dataconvert.py
  python dataconvert.py --gui
"""

import argparse
import csv
import os
import re
import struct
import sys
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

# =============================================================================
# Binary format constants - must match bar_binary.h
# =============================================================================
MAGIC_V1 = 0x42415235  # "BAR5"
MAGIC_V2 = 0x42415236  # "BAR6"
FORMAT_VERSION = 2
PAYLOAD_BARS  = 0
PAYLOAD_TICKS = 1

# FileHeaderV2: magic(4u) version(4u) payload_type(1u) timeframe_min(4u)
#               record_count(4u) start_ts(8i) end_ts(8i) symbol[16] = 49 bytes
HDR_V2_FMT = "<IIBIIqq16s"
HDR_V2_SIZE = struct.calcsize(HDR_V2_FMT)  # 49

# BinaryBarM5: timestamp(8i) open(8f) high(8f) low(8f) close(8f) bid(8f) ask(8f) = 56 bytes
BAR_FMT  = "<qdddddd"
BAR_SIZE = struct.calcsize(BAR_FMT)  # 56

# TickRecord: timestamp(8i) bid(8f) ask(8f) volume(8f) = 32 bytes  (payload_type=1)
# Matches BinaryFormat::TickRecord in bar_binary.h
TICK_FMT  = "<qddd"
TICK_SIZE = struct.calcsize(TICK_FMT)  # 32

assert HDR_V2_SIZE == 49, f"Header size mismatch: {HDR_V2_SIZE}"
assert BAR_SIZE    == 56, f"Bar size mismatch: {BAR_SIZE}"
assert TICK_SIZE   == 32, f"Tick size mismatch: {TICK_SIZE}"

Bar = Dict[str, Any]  # {ts: int, open: float, high: float, low: float, close: float, bid: float, ask: float}
ProgressFn = Optional[Callable[[str], None]]

TIMEFRAMES = [1, 2, 3, 4, 5, 10, 15, 30, 60, 120, 240, 1440]

# =============================================================================
# Timestamp parsers
# =============================================================================
_RE_DUKA = re.compile(
    r"^(\d{2})\.(\d{2})\.(\d{4}) (\d{2}):(\d{2}):(\d{2})\.(\d+)"
    r"(?: UTC([+-]\d{2}):(\d{2}))?$"
)
_RE_MT   = re.compile(r"^(\d{4})[.\-/](\d{2})[.\-/](\d{2})$")
_RE_US   = re.compile(r"^(\d{1,2})[/\-](\d{1,2})[/\-](\d{4})$")
_RE_NT   = re.compile(r"^(\d{8}) (\d{6})$")


def parse_ts(date_str: str, time_str: str = "") -> int:
    """Parse various date/time strings -> Unix milliseconds. Returns 0 on failure."""
    s = date_str.strip()
    t = time_str.strip()

    # Dukascopy: "01.01.2020 00:01:00.288" or "01.01.2020 00:01:00.288 UTC+01:00"
    m = _RE_DUKA.match(s)
    if m:
        dd, mm, yyyy, hh, mi, ss, ms_s, tz_h, tz_m = m.groups()
        ms = int(ms_s[:3].ljust(3, "0"))
        dt = datetime(int(yyyy), int(mm), int(dd), int(hh), int(mi), int(ss),
                      ms * 1000, tzinfo=timezone.utc)
        if tz_h is not None:
            # Subtract the UTC offset to convert local -> UTC
            from datetime import timedelta
            sign = 1 if tz_h.startswith("+") else -1
            offset = timedelta(hours=sign * int(tz_h.lstrip("+-")),
                               minutes=sign * int(tz_m))
            dt = dt - offset
        return int(dt.timestamp() * 1000)

    # NinjaTrader compact: "20200102 070500"
    m = _RE_NT.match(s)
    if m:
        d, ti = m.groups()
        return int(datetime(int(d[:4]), int(d[4:6]), int(d[6:8]),
                            int(ti[:2]), int(ti[2:4]), int(ti[4:6]),
                            tzinfo=timezone.utc).timestamp() * 1000)

    # MT4/MT5/ISO date: "2020.01.02" or "2020-01-02" with optional separate time
    m = _RE_MT.match(s)
    if m:
        yyyy, mm, dd = m.groups()
        tp = t.split(":") if t else ["0", "0", "0"]
        hh = int(tp[0]) if len(tp) > 0 else 0
        mi = int(tp[1]) if len(tp) > 1 else 0
        ss = int(float(tp[2])) if len(tp) > 2 else 0
        return int(datetime(int(yyyy), int(mm), int(dd), hh, mi, ss,
                            tzinfo=timezone.utc).timestamp() * 1000)

    # US date: "01/02/2020" or "01-02-2020"
    m = _RE_US.match(s)
    if m:
        mo, dd, yyyy = m.groups()
        tp = t.split(":") if t else ["0", "0", "0"]
        hh = int(tp[0]) if len(tp) > 0 else 0
        mi = int(tp[1]) if len(tp) > 1 else 0
        ss = int(float(tp[2])) if len(tp) > 2 else 0
        return int(datetime(int(yyyy), int(mo), int(dd), hh, mi, ss,
                            tzinfo=timezone.utc).timestamp() * 1000)

    # Unix timestamp (seconds or milliseconds)
    try:
        v = float(s)
        if 1e9 < v < 1e13:
            return int(v * 1000) if v < 1e12 else int(v)
    except ValueError:
        pass

    # ISO 8601 / generic fallback
    try:
        full = (s + " " + t).strip()
        dt = datetime.fromisoformat(full.rstrip("Z"))
        return int(dt.replace(tzinfo=timezone.utc).timestamp() * 1000)
    except Exception:
        pass

    return 0


def ts_to_str(ts_ms: int) -> str:
    dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
    return dt.strftime("%Y-%m-%d %H:%M UTC")


# =============================================================================
# Binary writer
# =============================================================================
def write_v2(path: str, bars: List[Bar], symbol: str, tf_min: int) -> int:
    """Write bars list to a BAR6 .bin file. Returns total bytes written."""
    if not bars:
        raise ValueError("No bars to write")
    sym_b = symbol.encode("ascii", errors="replace")[:16].ljust(16, b"\x00")
    hdr = struct.pack(HDR_V2_FMT,
                      MAGIC_V2, FORMAT_VERSION, PAYLOAD_BARS,
                      tf_min, len(bars),
                      bars[0]["ts"], bars[-1]["ts"],
                      sym_b)
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    with open(path, "wb") as f:
        f.write(hdr)
        for b in bars:
            f.write(struct.pack(BAR_FMT,
                                b["ts"], b["open"], b["high"],
                                b["low"], b["close"], b["bid"], b["ask"]))
    return HDR_V2_SIZE + len(bars) * BAR_SIZE


def write_ticks_v2(path: str, ticks: List[Dict], symbol: str) -> int:
    """Write raw ticks to a BAR6 file with payload_type=1 (32-byte TickRecords).

    Header: timeframe_min=0 (tick), payload_type=1.
    Record: ts(int64) bid(double) ask(double) flags(int64=0).
    """
    if not ticks:
        raise ValueError("No ticks to write")
    sym_b = symbol.encode("ascii", errors="replace")[:16].ljust(16, b"\x00")
    hdr = struct.pack(HDR_V2_FMT,
                      MAGIC_V2, FORMAT_VERSION, PAYLOAD_TICKS,
                      0,           # timeframe_min = 0 for ticks
                      len(ticks),
                      ticks[0]["ts"], ticks[-1]["ts"],
                      sym_b)
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    with open(path, "wb") as f:
        f.write(hdr)
        for tk in ticks:
            f.write(struct.pack(TICK_FMT, tk["ts"], tk["bid"], tk["ask"], 0.0))
    return HDR_V2_SIZE + len(ticks) * TICK_SIZE


# =============================================================================
# Tick aggregator
# =============================================================================
def aggregate_ticks(ticks: List[Dict], tf_min: int) -> List[Bar]:
    """(ts_ms, bid, ask) ticks -> OHLCV bars."""
    interval_ms = tf_min * 60 * 1000
    bar_map: Dict[int, Bar] = {}
    for tick in ticks:
        ts, bid, ask = tick["ts"], tick["bid"], tick["ask"]
        mid = (bid + ask) / 2.0
        bar_ts = (ts // interval_ms) * interval_ms
        if bar_ts not in bar_map:
            bar_map[bar_ts] = {"ts": bar_ts, "open": mid, "high": ask,
                               "low": bid, "close": mid, "bid": bid, "ask": ask}
        else:
            b = bar_map[bar_ts]
            if ask > b["high"]:  b["high"] = ask
            if bid < b["low"]:   b["low"]  = bid
            b["close"] = mid
            b["bid"]   = bid
            b["ask"]   = ask
    return sorted(bar_map.values(), key=lambda x: x["ts"])


# =============================================================================
# Format parsers
# =============================================================================
def _auto_delim(sample: str) -> str:
    return max(",", "\t", ";", key=sample.count)


def parse_dukascopy(path: str, tf_min: int, progress: ProgressFn = None) -> List[Bar]:
    """
    Dukascopy tick CSV:
      LocalTime,Ask,Bid,AskVolume,BidVolume
      01.01.2020 00:01:00.288,11348.0,11347.7,1.0,1.0
    Ticks are aggregated into OHLCV bars.
    """
    ticks = parse_dukascopy_ticks(path, progress)
    if progress:
        progress(f"  Aggregating {len(ticks):,} ticks -> M{tf_min} bars")
    return aggregate_ticks(ticks, tf_min)


def parse_dukascopy_ticks(path: str, progress: ProgressFn = None) -> List[Dict]:
    """Parse Dukascopy tick CSV and return raw tick list [{ts, bid, ask}, ]."""
    ticks: List[Dict] = []
    skipped = 0
    with open(path, newline="", encoding="utf-8", errors="replace") as f:
        reader = csv.reader(f)
        for i, row in enumerate(reader):
            if not row:
                continue
            if i == 0 and row[0].strip().lower() in ("localtime", "date", "time", "timestamp"):
                continue
            if len(row) < 3:
                skipped += 1
                continue
            ts = parse_ts(row[0].strip())
            if not ts:
                skipped += 1
                continue
            try:
                ask = float(row[1])
                bid = float(row[2])
                ticks.append({"ts": ts, "bid": bid, "ask": ask})
            except ValueError:
                skipped += 1
            if progress and i % 250_000 == 0 and i:
                progress(f"  Parsed {i:,} ticks")
    if progress:
        progress(f"  Loaded {len(ticks):,} ticks ({skipped:,} skipped)")
    return ticks


def _make_bar(ts: int, o: float, h: float, l: float, c: float) -> Bar:
    return {"ts": ts, "open": o, "high": h, "low": l, "close": c, "bid": c, "ask": c}


def parse_mt4(path: str, tf_min: int, progress: ProgressFn = None) -> List[Bar]:
    """
    MT4 / MT5 bar CSV:
      <DATE>,<TIME>,<OPEN>,<HIGH>,<LOW>,<CLOSE>,<TICKVOL>,<VOL>,<SPREAD>
      2020.01.02,07:05,11430.0,11439.0,11428.0,11430.0,234,0,0

    Also handles MT5 (same layout, tab- or comma-delimited).
    Handles optional combined datetime column (no separate time col).
    """
    bars: List[Bar] = []
    skipped = 0
    with open(path, newline="", encoding="utf-8", errors="replace") as f:
        sample = f.read(8192)
        f.seek(0)
        delim = _auto_delim(sample)
        reader = csv.reader(f, delimiter=delim)
        date_col, time_col, o_col, h_col, l_col, c_col = 0, 1, 2, 3, 4, 5
        for i, raw in enumerate(reader):
            row = [c.strip().strip("<>\"") for c in raw if raw]
            if not row:
                continue
            if i == 0:
                # Detect header and column layout
                hdrs = [h.upper() for h in row]
                if "DATE" in hdrs or "OPEN" in hdrs or "TIME" in hdrs:
                    # Might be combined datetime
                    if "DATETIME" in hdrs or (hdrs[0] in ("DATE", "DATETIME") and "TIME" not in hdrs[1:3]):
                        time_col = -1
                        o_col, h_col, l_col, c_col = 1, 2, 3, 4
                    # Locate columns by name for robustness
                    for ci, h in enumerate(hdrs):
                        if h == "DATE":     date_col = ci
                        elif h == "TIME":   time_col = ci
                        elif h == "OPEN":   o_col    = ci
                        elif h == "HIGH":   h_col    = ci
                        elif h == "LOW":    l_col    = ci
                        elif h == "CLOSE":  c_col    = ci
                    continue
            if len(row) <= max(o_col, h_col, l_col, c_col):
                skipped += 1
                continue
            try:
                t_str = row[time_col] if time_col >= 0 and time_col < len(row) else ""
                ts = parse_ts(row[date_col], t_str)
                if not ts:
                    skipped += 1
                    continue
                bars.append(_make_bar(ts, float(row[o_col]), float(row[h_col]),
                                      float(row[l_col]), float(row[c_col])))
            except (ValueError, IndexError):
                skipped += 1
            if progress and i % 100_000 == 0 and i:
                progress(f"  Parsed {i:,} rows")
    if progress:
        progress(f"  Loaded {len(bars):,} bars ({skipped:,} skipped)")
    return sorted(bars, key=lambda x: x["ts"])


def parse_mt4hst(path: str, tf_min: int, progress: ProgressFn = None) -> List[Bar]:
    """
    MetaTrader 4 binary .hst history file.
    Version 400: 148-byte header + 44-byte records  {time(u32), open, low, high, close, vol  (5xf64)}
    Version 401: 148-byte header + 60-byte records  {time(i64), open, high, low, close (4xf64),
                                                      tick_vol(u64), spread(i32), real_vol(u64)}
    Note: v400 stores LOW before HIGH; v401 stores HIGH before LOW.
    """
    HST_HDR = 148
    with open(path, "rb") as f:
        raw = f.read()

    if len(raw) < HST_HDR:
        raise ValueError(f"File too small for MT4 HST header ({len(raw)} bytes)")

    version = struct.unpack_from("<I", raw, 0)[0]
    if version not in (400, 401):
        # Some brokers write version 400 with 4-byte integers
        raise ValueError(f"Unrecognised MT4 HST version: {version} (expected 400 or 401)")

    data = raw[HST_HDR:]
    bars: List[Bar] = []

    if version == 400:
        # CTM(uint32) OPEN LOW HIGH CLOSE VOL  - all floats as double
        FMT, SZ = "<Iddddd", 44
        n = len(data) // SZ
        for i in range(n):
            time_s, o, low, high, c, _ = struct.unpack_from(FMT, data, i * SZ)
            bars.append(_make_bar(time_s * 1000, o, high, low, c))
    else:  # 401
        # CTM(int64) OPEN HIGH LOW CLOSE  tick_vol(uint64) spread(int32) real_vol(uint64)
        FMT, SZ = "<qddddQiQ", 60
        n = len(data) // SZ
        for i in range(n):
            time_s, o, high, low, c, _tv, _sp, _rv = struct.unpack_from(FMT, data, i * SZ)
            bars.append(_make_bar(time_s * 1000, o, high, low, c))

    if progress:
        symbol_raw = raw[4:68].rstrip(b"\x00").decode("ascii", errors="replace")
        progress(f"  Parsed {len(bars):,} MT4 v{version} bars  symbol={symbol_raw!r}")
    return sorted(bars, key=lambda x: x["ts"])


def parse_tradingview(path: str, tf_min: int, progress: ProgressFn = None) -> List[Bar]:
    """
    TradingView / Pine Script data export:
      time,open,high,low,close,volume
      1577836800,11396.6,11430.5,11391.2,11410.3,1234
    'time' is Unix seconds (int or float).
    """
    bars: List[Bar] = []
    skipped = 0
    with open(path, newline="", encoding="utf-8", errors="replace") as f:
        reader = csv.reader(f)
        for i, row in enumerate(reader):
            if not row:
                continue
            row = [c.strip() for c in row]
            if i == 0 and row[0].lower() in ("time", "date", "timestamp"):
                continue
            if len(row) < 5:
                skipped += 1
                continue
            try:
                t = float(row[0])
                ts = int(t * 1000) if t < 1e12 else int(t)
                bars.append(_make_bar(ts, float(row[1]), float(row[2]),
                                      float(row[3]), float(row[4])))
            except ValueError:
                skipped += 1
    if progress:
        progress(f"  Loaded {len(bars):,} TradingView bars ({skipped:,} skipped)")
    return sorted(bars, key=lambda x: x["ts"])


def parse_ninjatrader(path: str, tf_min: int, progress: ProgressFn = None) -> List[Bar]:
    """
    NinjaTrader bar export (several layouts):
      20200102 070500;11430;11439;11428;11430;100          (compact yyyymmdd hhmmss)
      01/02/2020 07:05:00;11430;11439;11428;11430;100      (US date)
      2020-01-02 07:05:00;11430;11439;11428;11430;100      (ISO date)
    Separator: ; or ,   |   OHLCV order: open, high, low, close
    """
    bars: List[Bar] = []
    skipped = 0
    with open(path, newline="", encoding="utf-8", errors="replace") as f:
        sample = f.read(2048)
        f.seek(0)
        delim = ";" if sample.count(";") >= sample.count(",") else ","
        reader = csv.reader(f, delimiter=delim)
        for i, row in enumerate(reader):
            row = [c.strip() for c in row]
            if not row or (i == 0 and not row[0][:1].isdigit()):
                continue
            if len(row) < 5:
                skipped += 1
                continue
            try:
                # NinjaTrader may combine date+time in col 0 with space
                if " " in row[0]:
                    parts = row[0].split(" ", 1)
                    ts = parse_ts(parts[0], parts[1])
                else:
                    ts = parse_ts(row[0], row[1] if len(row) > 5 else "")
                if not ts:
                    skipped += 1
                    continue
                # OHLCV starts at col 1 if datetime combined, else col 2
                ci = 1 if " " in row[0] else 2
                bars.append(_make_bar(ts, float(row[ci]), float(row[ci+1]),
                                      float(row[ci+2]), float(row[ci+3])))
            except (ValueError, IndexError):
                skipped += 1
    if progress:
        progress(f"  Loaded {len(bars):,} NinjaTrader bars ({skipped:,} skipped)")
    return sorted(bars, key=lambda x: x["ts"])


def parse_tradestation(path: str, tf_min: int, progress: ProgressFn = None) -> List[Bar]:
    """
    TradeStation bar CSV (EasyLanguage format):
      "Date","Time","Open","High","Low","Close","Up","Down"
      01/02/2020,07:05,11430.00,11439.00,11428.00,11430.00,100,50
    Date is MM/DD/YYYY; Time is HH:MM (24h).
    """
    bars: List[Bar] = []
    skipped = 0
    with open(path, newline="", encoding="utf-8", errors="replace") as f:
        reader = csv.reader(f)
        date_col, time_col, o_col, h_col, l_col, c_col = 0, 1, 2, 3, 4, 5
        for i, raw in enumerate(reader):
            row = [c.strip().strip('"') for c in raw]
            if not row:
                continue
            if i == 0:
                hdrs = [h.upper() for h in row]
                for ci, h in enumerate(hdrs):
                    if h == "DATE":   date_col = ci
                    elif h == "TIME": time_col = ci
                    elif h == "OPEN": o_col    = ci
                    elif h == "HIGH": h_col    = ci
                    elif h == "LOW":  l_col    = ci
                    elif h == "CLOSE": c_col   = ci
                continue
            if len(row) <= max(o_col, h_col, l_col, c_col):
                skipped += 1
                continue
            try:
                ts = parse_ts(row[date_col], row[time_col] if time_col < len(row) else "")
                if not ts:
                    skipped += 1
                    continue
                bars.append(_make_bar(ts, float(row[o_col]), float(row[h_col]),
                                      float(row[l_col]), float(row[c_col])))
            except (ValueError, IndexError):
                skipped += 1
    if progress:
        progress(f"  Loaded {len(bars):,} TradeStation bars ({skipped:,} skipped)")
    return sorted(bars, key=lambda x: x["ts"])


def parse_generic(path: str, tf_min: int, progress: ProgressFn = None,
                  col_map: Optional[Dict[str, int]] = None) -> List[Bar]:
    """
    Auto-detect OHLCV CSV.  Tries column name matching first, then positional.
    Falls back to Dukascopy tick parser if columns can't be resolved.

    col_map  optional manual override: {'date': 0, 'time': 1, 'open': 2, ...}
    Column indices are 0-based; 'time' may be -1 if combined with date.
    """
    with open(path, newline="", encoding="utf-8", errors="replace") as f:
        sample = f.read(8192)
        f.seek(0)
        delim = _auto_delim(sample)
        reader = csv.reader(f, delimiter=delim)
        rows = [r for r in reader if any(c.strip() for c in r)]

    if not rows:
        raise ValueError("File appears empty")

    raw_hdr = [c.strip().strip('"<>').lower() for c in rows[0]]

    if col_map is not None:
        # User-supplied mapping  skip auto-detection entirely
        date_col = col_map.get("date", 0)
        time_col = col_map.get("time", -1)
        o_col    = col_map.get("open",  -1)
        h_col    = col_map.get("high",  -1)
        l_col    = col_map.get("low",   -1)
        c_col    = col_map.get("close", -1)
        # Skip header row if first row looks non-numeric
        first_date = (rows[0][date_col].strip() if date_col < len(rows[0]) else "")
        data_rows = rows[1:] if re.search(r"[a-zA-Z]", first_date) else rows
        if any(c < 0 for c in [date_col, o_col, h_col, l_col, c_col]):
            raise ValueError("Incomplete column mapping  date, open, high, low, close are required")
    else:
        has_header = any(re.search(r"[a-z]", h) for h in raw_hdr)

        if has_header:
            def find(*names) -> int:
                for n in names:
                    for ci, h in enumerate(raw_hdr):
                        if n == h or n in h:
                            return ci
                return -1
            date_col = find("date", "datetime", "timestamp", "time")
            time_col = find("time")
            if time_col == date_col:
                time_col = -1
            o_col = find("open")
            h_col = find("high")
            l_col = find("low")
            c_col = find("close")
            data_rows = rows[1:]
        else:
            # Positional: date [time] open high low close [volume ...]
            if len(rows[0]) >= 6:
                date_col, time_col, o_col, h_col, l_col, c_col = 0, 1, 2, 3, 4, 5
            elif len(rows[0]) >= 5:
                date_col, time_col, o_col, h_col, l_col, c_col = 0, -1, 1, 2, 3, 4
            else:
                if progress:
                    progress("  Cannot detect OHLCV layout - trying tick parser")
                return parse_dukascopy(path, tf_min, progress)
            data_rows = rows

        if any(c < 0 for c in [date_col, o_col, h_col, l_col, c_col]):
            if progress:
                progress(f"  Cannot detect columns ({raw_hdr}) - trying tick parser")
            return parse_dukascopy(path, tf_min, progress)

    bars: List[Bar] = []
    skipped = 0
    for row in data_rows:
        try:
            t_str = row[time_col].strip() if 0 <= time_col < len(row) else ""
            ts = parse_ts(row[date_col].strip(), t_str)
            if not ts:
                skipped += 1
                continue
            bars.append(_make_bar(ts, float(row[o_col]), float(row[h_col]),
                                  float(row[l_col]), float(row[c_col])))
        except (ValueError, IndexError):
            skipped += 1
    if progress:
        progress(f"  Auto-detected {len(bars):,} bars ({skipped:,} skipped)")
    return sorted(bars, key=lambda x: x["ts"])


# =============================================================================
# Format registry
# =============================================================================
FORMATS: Dict[str, tuple] = {
    "dukascopy":    (parse_dukascopy,    "Dukascopy tick CSV  (LocalTime,Ask,Bid,AskVolume,BidVolume)"),
    "mt4":          (parse_mt4,          "MT4 bar CSV  (<DATE>,<TIME>,<OPEN>,<HIGH>,<LOW>,<CLOSE>,...)"),
    "mt4hst":       (parse_mt4hst,       "MT4 .hst binary history  (v400: 44-byte, v401: 60-byte records)"),
    "mt5":          (parse_mt4,          "MT5 bar CSV  (same layout as MT4, tab or comma delimited)"),
    "tradingview":  (parse_tradingview,  "TradingView export  (time,open,high,low,close,volume)"),
    "ninjatrader":  (parse_ninjatrader,  "NinjaTrader bar CSV  (yyyymmdd hhmmss or date+time ; sep)"),
    "tradestation": (parse_tradestation, "TradeStation bar CSV  (Date,Time,Open,High,Low,Close,...)"),
    "generic":      (parse_generic,      "Auto-detect any OHLCV CSV  (column names or positional)"),
}


def detect_format(path: str) -> str:
    """Heuristically guess the input format from file extension and content."""
    ext = Path(path).suffix.lower()
    if ext == ".hst":
        return "mt4hst"

    # Check binary MT4 HST magic
    try:
        with open(path, "rb") as f:
            first4 = f.read(4)
        if len(first4) == 4:
            ver = struct.unpack_from("<I", first4, 0)[0]
            if ver in (400, 401):
                return "mt4hst"
    except OSError:
        pass

    # Text-based detection
    try:
        with open(path, encoding="utf-8", errors="replace") as f:
            lines = [f.readline() for _ in range(4)]
        sample = " ".join(lines).lower()

        if "localtime" in sample or re.search(r"\d{2}\.\d{2}\.\d{4} \d{2}:\d{2}:\d{2}\.\d+", sample):
            return "dukascopy"
        if "<date>" in sample or "<open>" in sample:
            return "mt4"
        # TradingView: first column is "time" (Unix seconds), no separate "date" column
        if (re.search(r"time,open,high,low,close", sample.replace(" ", ""))
                and not re.search(r"\bdate\b", sample[:200].lower())):
            return "tradingview"
        if re.search(r"\d{8} \d{6}", sample):
            return "ninjatrader"
        if re.search(r'"date".*"time".*"open"', sample):
            return "tradestation"
    except OSError:
        pass

    return "generic"


# =============================================================================
# Main convert function
# =============================================================================
def convert(input_path: str, output_path: str, symbol: str, tf_min: int,
            fmt: Optional[str] = None, progress: ProgressFn = None,
            as_ticks: bool = False,
            col_map: Optional[Dict[str, int]] = None) -> dict:
    """Convert input_path -> BAR6 output_path.

    as_ticks=True: write raw ticks (payload_type=1, 32-byte TickRecord).
    col_map: manual column index mapping for generic CSVs,
             e.g. {'date': 0, 'time': 1, 'open': 2, 'high': 3, 'low': 4, 'close': 5}
    """
    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Input not found: {input_path}")

    if fmt is None:
        fmt = detect_format(input_path)
        if progress:
            progress(f"Auto-detected format: {fmt}")

    if fmt not in FORMATS:
        raise ValueError(f"Unknown format {fmt!r}. Run --formats to list valid options.")

    # Tick output path
    if as_ticks:
        if fmt not in ("dukascopy",):
            raise ValueError(
                f"--ticks / 'Save as ticks' is only supported for tick-based input formats "
                f"(currently: dukascopy). Got: {fmt!r}. "
                f"Bar-based formats do not carry per-tick bid/ask data."
            )
        if progress:
            progress(f"Parsing {os.path.basename(input_path)} as [{fmt}] (raw ticks)")
        ticks = parse_dukascopy_ticks(input_path, progress)
        if not ticks:
            raise ValueError("No ticks could be parsed from the input file")
        if progress:
            progress(f"Writing {len(ticks):,} raw ticks -> {os.path.basename(output_path)}")
        n_bytes = write_ticks_v2(output_path, ticks, symbol)
        if progress:
            progress(f"DONE  {len(ticks):,} ticks  |  "
                     f"{ts_to_str(ticks[0]['ts'])} -> {ts_to_str(ticks[-1]['ts'])}"
                     f"  |  {n_bytes / 1024 / 1024:.2f} MB written")
        return {
            "ticks":  len(ticks),
            "bars":   len(ticks),
            "bytes":  n_bytes,
            "start":  ticks[0]["ts"],
            "end":    ticks[-1]["ts"],
            "format": fmt,
            "symbol": symbol,
            "tf_min": 0,
        }

    # Bar output path (original)
    parser_fn, _ = FORMATS[fmt]
    if progress:
        progress(f"Parsing {os.path.basename(input_path)} as [{fmt}]")

    if col_map and fmt in ("generic", None):
        bars = parse_generic(input_path, tf_min, progress, col_map=col_map)
    else:
        bars = parser_fn(input_path, tf_min, progress)
    if not bars:
        raise ValueError("No bars could be parsed from the input file")

    if progress:
        progress(f"Writing {len(bars):,} M{tf_min} bars -> {os.path.basename(output_path)}")

    n_bytes = write_v2(output_path, bars, symbol, tf_min)

    if progress:
        progress(f"DONE  {len(bars):,} bars  |  {ts_to_str(bars[0]['ts'])} -> {ts_to_str(bars[-1]['ts'])}"
                 f"  |  {n_bytes / 1024 / 1024:.2f} MB written")

    return {
        "bars":   len(bars),
        "bytes":  n_bytes,
        "start":  bars[0]["ts"],
        "end":    bars[-1]["ts"],
        "format": fmt,
        "symbol": symbol,
        "tf_min": tf_min,
    }


# =============================================================================
# --info: inspect an existing .bin file
# =============================================================================
def cmd_info(path: str) -> None:
    with open(path, "rb") as f:
        raw = f.read(max(HDR_V2_SIZE, 32))

    magic = struct.unpack_from("<I", raw, 0)[0]

    if magic == MAGIC_V2:
        mg, ver, pt, tf, count, ts0, ts1, sym_b = struct.unpack_from(HDR_V2_FMT, raw)
        sym = sym_b.rstrip(b"\x00").decode("ascii", errors="replace")
        rec_size = TICK_SIZE if pt == PAYLOAD_TICKS else BAR_SIZE
        print(f"File:        {path}")
        print(f"Format:      BAR6")
        print(f"Symbol:      {sym or '(empty)'}")
        print(f"Payload:     {'ticks (type=1, 32 B)' if pt == PAYLOAD_TICKS else 'bars (type=0, 56 B)'}")
        print(f"Timeframe:   {'tick (M0)' if tf == 0 else f'M{tf}'}")
        print(f"Records:     {count:,}")
        print(f"Start:       {ts_to_str(ts0)}")
        print(f"End:         {ts_to_str(ts1)}")
        print(f"Payload:     {'bars' if pt == 0 else 'ticks'}")
        expected = HDR_V2_SIZE + count * rec_size
        actual   = os.path.getsize(path)
        ok = "OK" if actual == expected else f"MISMATCH (expected {expected:,}, got {actual:,})"
        print(f"File size:   {actual:,} bytes  {ok}")
    elif magic == MAGIC_V1:
        V1_FMT = "<IIIIqq"
        mg, ver, tf, count, ts0, ts1 = struct.unpack_from(V1_FMT, raw)
        print(f"File:        {path}")
        print(f"Format:      BAR5  (run migrate_to_v2.py to upgrade)")
        print(f"Timeframe:   M{tf}")
        print(f"Bars:        {count:,}")
        print(f"Start:       {ts_to_str(ts0)}")
        print(f"End:         {ts_to_str(ts1)}")
    else:
        print(f"File:        {path}")
        print(f"Format:      UNKNOWN  (magic 0x{magic:08X})")
        sys.exit(1)


# =============================================================================
# --formats: list all supported formats
# =============================================================================
def cmd_formats() -> None:
    print("Supported input formats:\n")
    for name, (_, desc) in FORMATS.items():
        print(f"  {name:<14}  {desc}")
    print()
    print("Use --format <name> on the CLI, or select in the GUI dropdown.")
    print("Omit --format to auto-detect from file extension and content.")


# =============================================================================
# GUI
# =============================================================================
def run_gui() -> None:
    try:
        import tkinter as tk
        from tkinter import filedialog, messagebox, ttk
    except ImportError:
        print("ERROR: tkinter not available. Use CLI mode.", file=sys.stderr)
        sys.exit(1)

    C = {
        "bg":      "#0d0f14",
        "surf":    "#13161e",
        "surf2":   "#1a1e2a",
        "border":  "#1e2330",
        "accent":  "#00d4aa",
        "accent2": "#00a882",
        "text":    "#e2e6f0",
        "dim":     "#8891a8",
        "muted":   "#4a5166",
        "red":     "#f04e5e",
        "green":   "#3dd68c",
        "mono":    ("Consolas", 9),
        "ui":      ("Segoe UI", 9),
    }

    root = tk.Tk()
    root.title("DataConvert - BAR6")
    root.geometry("740x580")
    root.minsize(640, 480)
    root.configure(bg=C["bg"])

    style = ttk.Style()
    style.theme_use("clam")
    style.configure(".", background=C["bg"], foreground=C["text"],
                    font=C["ui"], fieldbackground=C["surf2"], borderwidth=0)
    style.configure("TFrame",    background=C["bg"])
    style.configure("TLabel",    background=C["bg"], foreground=C["text"])
    style.configure("TButton",   background=C["accent"], foreground="#000",
                    font=(C["ui"][0], C["ui"][1], "bold"), padding=(10, 5))
    style.map("TButton",
              background=[("active", C["accent2"]), ("disabled", C["muted"])],
              foreground=[("disabled", C["surf"])])
    style.configure("TCombobox", selectbackground=C["surf2"], padding=(4, 4))
    style.map("TCombobox", fieldbackground=[("readonly", C["surf2"])])
    style.configure("TScrollbar", background=C["surf2"], troughcolor=C["bg"],
                    arrowcolor=C["dim"])

    # == Header bar ===========================================================
    hdr = tk.Frame(root, bg=C["surf"], pady=10)
    hdr.pack(fill="x")
    tk.Label(hdr, text="DataConvert", bg=C["surf"], fg=C["accent"],
             font=(C["ui"][0], 15, "bold")).pack(side="left", padx=16)
    tk.Label(hdr, text="CSV / HST -> BAR6 binary", bg=C["surf"],
             fg=C["dim"]).pack(side="left")
    tk.Label(hdr, text="Dukascopy - MT4/MT5 - TradingView - NinjaTrader - TradeStation - Generic",
             bg=C["surf"], fg=C["muted"], font=(C["ui"][0], 8)).pack(side="right", padx=16)

    # == Content ==============================================================
    main = tk.Frame(root, bg=C["bg"], padx=18, pady=10)
    main.pack(fill="both", expand=True)

    def lbl(parent, text, **kw):
        tk.Label(parent, text=text, bg=C["bg"], fg=C["dim"],
                 font=(C["ui"][0], 8)).pack(**{"anchor": "w", **kw})

    def entry_widget(parent, var, width=None):
        e = tk.Entry(parent, textvariable=var, bg=C["surf2"], fg=C["text"],
                     insertbackground=C["text"], relief="flat",
                     font=C["mono"], highlightthickness=1,
                     highlightcolor=C["accent"], highlightbackground=C["border"])
        if width:
            e.configure(width=width)
        return e

    # Row: Input file + Format
    r1 = tk.Frame(main, bg=C["bg"])
    r1.pack(fill="x", pady=(0, 8))
    r1.columnconfigure(0, weight=3)
    r1.columnconfigure(1, weight=1)

    v_input  = tk.StringVar()
    v_format = tk.StringVar(value="auto")

    lbl(r1, "Input File")
    inp_row = tk.Frame(r1, bg=C["bg"])
    inp_row.pack(fill="x")
    inp_row.columnconfigure(0, weight=1)
    entry_widget(inp_row, v_input).grid(row=0, column=0, sticky="ew", ipady=4, padx=(0, 6))
    ttk.Button(inp_row, text="Browse", command=lambda: browse_input()).grid(row=0, column=1)

    # Format + description
    fmt_row = tk.Frame(main, bg=C["bg"])
    fmt_row.pack(fill="x", pady=(0, 8))
    lbl(fmt_row, "Format")
    fmt_inner = tk.Frame(fmt_row, bg=C["bg"])
    fmt_inner.pack(fill="x")
    fmt_opts = ["auto"] + list(FORMATS.keys())
    fmt_combo = ttk.Combobox(fmt_inner, textvariable=v_format, values=fmt_opts,
                             state="readonly", width=18)
    fmt_combo.pack(side="left")
    v_fmt_desc = tk.StringVar(value="Auto-detect from file extension and content")
    tk.Label(fmt_inner, textvariable=v_fmt_desc, bg=C["bg"], fg=C["muted"],
             font=(C["ui"][0], 8)).pack(side="left", padx=8)

    def on_fmt_change(*_):
        val = v_format.get()
        if val == "auto":
            v_fmt_desc.set("Auto-detect from file extension and content")
        elif val in FORMATS:
            v_fmt_desc.set(FORMATS[val][1])

    v_format.trace_add("write", on_fmt_change)

    # Column mapping (populated by the "Map Columns" dialog)
    _col_map: Dict[str, int] = {}   # mutable dict, shared via closure

    # "Map Columns" button  only useful for generic/auto with a CSV loaded
    map_btn = ttk.Button(fmt_inner, text="Map Columns...",
                         command=lambda: open_col_map_dialog())
    map_btn.pack(side="left", padx=(8, 0))
    v_map_label = tk.StringVar(value="")
    tk.Label(fmt_inner, textvariable=v_map_label, bg=C["bg"], fg=C["accent"],
             font=(C["ui"][0], 7)).pack(side="left", padx=4)

    def _update_map_btn(*_):
        fmt = v_format.get()
        # Only show Map Columns for generic/auto formats
        if fmt in ("auto", "generic"):
            map_btn.configure(state="normal")
        else:
            map_btn.configure(state="disabled")
            _col_map.clear()
            v_map_label.set("")

    v_format.trace_add("write", _update_map_btn)
    _update_map_btn()

    def open_col_map_dialog():
        """Open a dialog to manually map CSV columns to OHLCV fields."""
        inp = v_input.get().strip()
        if not inp or not os.path.exists(inp):
            messagebox.showerror("Error", "Select an input CSV file first")
            return

        # Read CSV headers + first few rows for preview
        try:
            with open(inp, newline="", encoding="utf-8", errors="replace") as f:
                sample = f.read(8192)
                f.seek(0)
                delim = _auto_delim(sample)
                reader = csv.reader(f, delimiter=delim)
                all_rows = [r for r in reader if any(c.strip() for c in r)]
        except Exception as exc:
            messagebox.showerror("Error", f"Cannot read file: {exc}")
            return

        if not all_rows:
            messagebox.showerror("Error", "File appears empty")
            return

        ncols = max(len(r) for r in all_rows[:5])
        # Build column options: "0: <header>" or "0: (col 0)"
        first_row = all_rows[0]
        col_labels = []
        for i in range(ncols):
            val = first_row[i].strip() if i < len(first_row) else ""
            col_labels.append(f"{i}: {val}" if val else f"col {i}")
        none_option = "(none)"
        options = [none_option] + col_labels

        dlg = tk.Toplevel(root)
        dlg.title("Map CSV Columns")
        dlg.configure(bg=C["bg"])
        dlg.resizable(False, False)
        dlg.grab_set()

        # Header
        tk.Label(dlg, text="Map CSV columns to required fields",
                 bg=C["bg"], fg=C["text"],
                 font=(C["ui"][0], 10, "bold")).pack(padx=20, pady=(14, 4))
        tk.Label(dlg, text="First row of your CSV:",
                 bg=C["bg"], fg=C["dim"], font=(C["ui"][0], 8)).pack(padx=20, anchor="w")
        preview = "  " + "  |  ".join(
            (first_row[i].strip() if i < len(first_row) else "") or f"(col {i})"
            for i in range(min(ncols, 10))
        )
        tk.Label(dlg, text=preview, bg=C["surf2"], fg=C["accent"],
                 font=C["mono"], relief="flat", anchor="w",
                 padx=8, pady=4).pack(fill="x", padx=20, pady=(2, 10))

        # Mapping rows
        fields = [
            ("date",  "Date / Datetime *",  True),
            ("time",  "Time (optional, if separate column)", False),
            ("open",  "Open *",  True),
            ("high",  "High *",  True),
            ("low",   "Low *",   True),
            ("close", "Close *", True),
        ]
        vars_: Dict[str, tk.StringVar] = {}
        grid = tk.Frame(dlg, bg=C["bg"])
        grid.pack(padx=20, pady=4, fill="x")

        for i, (key, label, required) in enumerate(fields):
            color = C["text"] if required else C["dim"]
            tk.Label(grid, text=label, bg=C["bg"], fg=color,
                     font=(C["ui"][0], 9), anchor="w", width=34).grid(
                         row=i, column=0, sticky="w", pady=2)
            # Pre-fill from existing mapping or auto-detect
            existing = _col_map.get(key)
            if existing is not None:
                default = col_labels[existing] if existing < len(col_labels) else none_option
            else:
                default = none_option
                if key == "time":
                    default = none_option  # optional
            v = tk.StringVar(value=default)
            vars_[key] = v
            cb = ttk.Combobox(grid, textvariable=v, values=options,
                              state="readonly", width=22)
            cb.grid(row=i, column=1, sticky="w", padx=(8, 0), pady=2)

        def apply_map():
            new_map: Dict[str, int] = {}
            for key, v in vars_.items():
                val = v.get()
                if val == none_option:
                    if key in ("date", "open", "high", "low", "close"):
                        messagebox.showerror("Missing", f"'{key}' is required")
                        return
                    continue  # optional field skipped
                # Parse index from "N: label" format
                try:
                    idx = int(val.split(":")[0])
                    new_map[key] = idx
                except ValueError:
                    messagebox.showerror("Error", f"Invalid selection for '{key}'")
                    return
            _col_map.clear()
            _col_map.update(new_map)
            mapped = ", ".join(f"{k}->col{v}" for k, v in sorted(new_map.items()))
            v_map_label.set(f"OK {mapped}")
            dlg.destroy()

        def clear_map():
            _col_map.clear()
            v_map_label.set("")
            dlg.destroy()

        btn_frame = tk.Frame(dlg, bg=C["bg"])
        btn_frame.pack(pady=(10, 16), padx=20, fill="x")
        ttk.Button(btn_frame, text="Apply Mapping", command=apply_map).pack(side="left")
        ttk.Button(btn_frame, text="Clear (auto-detect)", command=clear_map).pack(side="left", padx=(8, 0))
        ttk.Button(btn_frame, text="Cancel", command=dlg.destroy).pack(side="right")

        dlg.wait_window()

    # Symbol + Timeframe row
    r2 = tk.Frame(main, bg=C["bg"])
    r2.pack(fill="x", pady=(0, 8))
    r2.columnconfigure(0, weight=2)
    r2.columnconfigure(1, weight=0)

    v_symbol = tk.StringVar(value="SYMBOL")
    v_tf     = tk.StringVar(value="5")

    sym_frame = tk.Frame(r2, bg=C["bg"])
    sym_frame.grid(row=0, column=0, sticky="ew", padx=(0, 12))
    lbl(sym_frame, "Symbol (max 15 chars)")
    entry_widget(sym_frame, v_symbol).pack(fill="x", ipady=4)

    tf_frame = tk.Frame(r2, bg=C["bg"])
    tf_frame.grid(row=0, column=1, sticky="w")
    lbl(tf_frame, "Timeframe (min)")
    ttk.Combobox(tf_frame, textvariable=v_tf,
                 values=[str(t) for t in TIMEFRAMES], width=8).pack(ipady=2)

    # Output file
    lbl(main, "Output File", pady=(0, 0))
    out_row = tk.Frame(main, bg=C["bg"])
    out_row.pack(fill="x", pady=(0, 6))
    out_row.columnconfigure(0, weight=1)
    v_output = tk.StringVar()
    entry_widget(out_row, v_output).grid(row=0, column=0, sticky="ew", ipady=4, padx=(0, 6))
    ttk.Button(out_row, text="Browse", command=lambda: browse_output()).grid(row=0, column=1)

    # "Save as ticks" checkbox (only useful for tick-based formats)
    v_as_ticks = tk.BooleanVar(value=False)
    ticks_row = tk.Frame(main, bg=C["bg"])
    ticks_row.pack(fill="x", pady=(0, 8))
    ticks_chk = tk.Checkbutton(
        ticks_row, text="Save as raw ticks  (payload_type=1, 32 B/tick - Dukascopy only)",
        variable=v_as_ticks, bg=C["bg"], fg=C["text"],
        activebackground=C["bg"], activeforeground=C["accent"],
        selectcolor=C["surf2"], font=(C["ui"][0], 8),
        command=lambda: _on_ticks_toggle(),
    )
    ticks_chk.pack(side="left")
    tk.Label(ticks_row, text="  <- Timeframe ignored when ticked",
             bg=C["bg"], fg=C["muted"], font=(C["ui"][0], 7)).pack(side="left")

    def _on_ticks_toggle():
        """Auto-update output filename suffix when toggling tick mode."""
        out = v_output.get().strip()
        if not out:
            return
        p = Path(out)
        stem = p.stem
        if v_as_ticks.get():
            # Remove _M<n> suffix if present, add _TICKS
            stem = re.sub(r"_M\d+$", "", stem) + "_TICKS"
        else:
            stem = re.sub(r"_TICKS$", "", stem) + f"_M{v_tf.get()}"
        v_output.set(str(p.parent / (stem + p.suffix)))

    def _update_ticks_state(*_):
        """Disable tick checkbox for bar-only formats."""
        fmt = v_format.get()
        tick_formats = {"dukascopy", "auto"}
        if fmt in tick_formats:
            ticks_chk.configure(state="normal")
        else:
            v_as_ticks.set(False)
            ticks_chk.configure(state="disabled")

    v_format.trace_add("write", _update_ticks_state)

    # Browse callbacks
    def browse_input():
        path = filedialog.askopenfilename(
            title="Select input file",
            filetypes=[
                ("Market data", "*.csv *.txt *.hst"),
                ("CSV files", "*.csv"),
                ("Text files", "*.txt"),
                ("MT4 History", "*.hst"),
                ("All files", "*.*"),
            ])
        if not path:
            return
        v_input.set(path)
        p = Path(path)
        sym = v_symbol.get() or p.stem.split("_")[0]
        tf  = v_tf.get() or "5"
        if not v_output.get():
            v_output.set(str(p.parent / f"{sym}_M{tf}.bin"))
        # Auto-detect format
        detected = detect_format(path)
        if detected in FORMATS:
            v_format.set(detected)

    def browse_output():
        sym = v_symbol.get().strip() or "output"
        tf  = v_tf.get() or "5"
        path = filedialog.asksaveasfilename(
            title="Save binary file as",
            initialfile=f"{sym}_M{tf}.bin",
            defaultextension=".bin",
            filetypes=[("Binary bar file", "*.bin"), ("All files", "*.*")])
        if path:
            v_output.set(path)

    # Convert button + status
    btn_row = tk.Frame(main, bg=C["bg"])
    btn_row.pack(fill="x", pady=(0, 8))
    convert_btn = ttk.Button(btn_row, text="Convert & Write .bin",
                             command=lambda: start_convert())
    convert_btn.pack(side="left")
    info_btn = ttk.Button(btn_row, text="Inspect .bin",
                          command=lambda: do_info())
    info_btn.pack(side="left", padx=(8, 0))
    v_status = tk.StringVar(value="Ready")
    status_lbl = tk.Label(btn_row, textvariable=v_status, bg=C["bg"],
                          fg=C["dim"], font=(C["ui"][0], 9))
    status_lbl.pack(side="left", padx=12)

    # Log area
    lbl(main, "Log")
    log_frame = tk.Frame(main, bg=C["bg"])
    log_frame.pack(fill="both", expand=True)
    log_text = tk.Text(log_frame, bg=C["surf"], fg=C["text"],
                       insertbackground=C["text"], relief="flat",
                       font=C["mono"], height=10, wrap="word",
                       highlightthickness=1, highlightbackground=C["border"],
                       state="disabled")
    log_sb = ttk.Scrollbar(log_frame, orient="vertical", command=log_text.yview)
    log_text.configure(yscrollcommand=log_sb.set)
    log_sb.pack(side="right", fill="y")
    log_text.pack(fill="both", expand=True)
    log_text.tag_config("green",  foreground=C["green"])
    log_text.tag_config("red",    foreground=C["red"])
    log_text.tag_config("accent", foreground=C["accent"])
    log_text.tag_config("dim",    foreground=C["dim"])

    def log(msg: str, tag: str = ""):
        log_text.configure(state="normal")
        log_text.insert("end", msg + "\n", tag)
        log_text.see("end")
        log_text.configure(state="disabled")
        root.update_idletasks()

    def clear_log():
        log_text.configure(state="normal")
        log_text.delete("1.0", "end")
        log_text.configure(state="disabled")

    def start_convert():
        inp = v_input.get().strip()
        out = v_output.get().strip()
        sym = v_symbol.get().strip()
        fmt = v_format.get() if v_format.get() != "auto" else None
        try:
            tf = int(v_tf.get())
        except ValueError:
            messagebox.showerror("Error", "Timeframe must be a whole number of minutes")
            return
        if not inp:  messagebox.showerror("Error", "No input file selected"); return
        if not out:  messagebox.showerror("Error", "No output file specified"); return
        if not sym:  messagebox.showerror("Error", "Symbol name is required"); return

        clear_log()
        convert_btn.configure(state="disabled")
        status_lbl.configure(fg=C["dim"])
        v_status.set("Converting")
        as_ticks = v_as_ticks.get()
        active_col_map = dict(_col_map) if _col_map else None

        def worker():
            try:
                result = convert(inp, out, sym, tf, fmt, log,
                                 as_ticks=as_ticks, col_map=active_col_map)
                root.after(0, lambda: on_done(result))
            except Exception as exc:
                root.after(0, lambda e=exc: on_error(str(e)))

        threading.Thread(target=worker, daemon=True).start()

    def on_done(result):
        convert_btn.configure(state="normal")
        n = result.get("ticks") or result.get("bars", 0)
        unit = "ticks" if result.get("tf_min") == 0 else "bars"
        v_status.set(f"Done - {n:,} {unit}")
        status_lbl.configure(fg=C["green"])
        log(f"Output: {v_output.get()}", "accent")

    def on_error(msg):
        convert_btn.configure(state="normal")
        v_status.set("Error")
        status_lbl.configure(fg=C["red"])
        log(f"ERROR: {msg}", "red")

    def do_info():
        path = v_output.get().strip() or filedialog.askopenfilename(
            filetypes=[("Binary bar file", "*.bin"), ("All files", "*.*")])
        if not path or not os.path.exists(path):
            messagebox.showerror("Error", f"File not found: {path}")
            return
        clear_log()
        try:
            with open(path, "rb") as f:
                raw = f.read(HDR_V2_SIZE)
            magic = struct.unpack_from("<I", raw, 0)[0]
            if magic == MAGIC_V2:
                _, ver, pt, tf, count, ts0, ts1, sym_b = struct.unpack_from(HDR_V2_FMT, raw)
                sym = sym_b.rstrip(b"\x00").decode("ascii", errors="replace")
                rec_size = TICK_SIZE if pt == PAYLOAD_TICKS else BAR_SIZE
                log(f"Format:    BAR6", "accent")
                log(f"Symbol:    {sym or '(empty)'}")
                log(f"Payload:   {'ticks (payload_type=1, 32 B)' if pt == PAYLOAD_TICKS else 'bars (payload_type=0, 56 B)'}")
                log(f"Timeframe: {'tick (M0)' if tf == 0 else f'M{tf}'}")
                log(f"{'Ticks' if pt == PAYLOAD_TICKS else 'Bars'}:      {count:,}")
                log(f"Start:     {ts_to_str(ts0)}")
                log(f"End:       {ts_to_str(ts1)}")
                exp = HDR_V2_SIZE + count * rec_size
                act = os.path.getsize(path)
                log(f"Size:      {act:,} bytes  {'OK' if act == exp else 'MISMATCH'}")
            elif magic == MAGIC_V1:
                log(f"Format:    BAR5  (use migrate_to_v2.py to upgrade)", "dim")
            else:
                log(f"Unknown magic: 0x{magic:08X}", "red")
        except Exception as exc:
            log(f"ERROR: {exc}", "red")

    root.mainloop()


# =============================================================================
# CLI entry point
# =============================================================================
def main() -> None:
    # No arguments on GUI launch
    if len(sys.argv) == 1:
        run_gui()
        return

    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("input",   nargs="?", help="Input file (CSV or .hst)")
    parser.add_argument("output",  nargs="?", help="Output .bin file path")
    parser.add_argument("--symbol",  "-s",  help="Symbol name (e.g. DEU.IDX-EUR)")
    parser.add_argument("--tf",      "-t",  type=int, help="Timeframe in minutes (1, 5, 15, 60, )")
    parser.add_argument("--format",  "-f",  choices=list(FORMATS.keys()),
                        help="Input format (default: auto-detect)")
    parser.add_argument("--ticks",  action="store_true",
                        help="Write raw ticks (payload_type=1, 32-byte TickRecord). "
                             "Only for tick-based inputs (dukascopy). Ignores --tf.")
    parser.add_argument("--gui",    action="store_true", help="Launch GUI")
    parser.add_argument("--info",   metavar="FILE",      help="Print header info of a .bin file")
    parser.add_argument("--formats", action="store_true", help="List all supported input formats")

    args = parser.parse_args()

    if args.gui:
        run_gui()
        return

    if args.formats:
        cmd_formats()
        return

    if args.info:
        cmd_info(args.info)
        return

    # Conversion mode - require input / output / symbol / tf
    missing = []
    if not args.input:   missing.append("INPUT")
    if not args.output:  missing.append("OUTPUT")
    if not args.symbol:  missing.append("--symbol")
    if not args.ticks and not args.tf:
        missing.append("--tf  (or use --ticks to write raw tick file)")
    if missing:
        parser.error(f"Missing required arguments: {', '.join(missing)}")

    try:
        convert(args.input, args.output, args.symbol, args.tf or 0,
                fmt=args.format, progress=print, as_ticks=args.ticks)
        sys.exit(0)
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
