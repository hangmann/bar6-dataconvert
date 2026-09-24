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
  binance      Binance historical klines (data.binance.vision ZIP/CSV, ms timestamps)
  bybit        Bybit / Kraken / generic crypto exchange OHLCV CSV (Unix seconds)
  generic      Auto-detect any OHLCV CSV (column names or positional)

CLI usage:
  python dataconvert.py INPUT OUTPUT --symbol SYM --tf MINUTES [--format FMT]
  python dataconvert.py ticks.csv out.bin --symbol DEU.IDX-EUR --tf 5
  python dataconvert.py bars.csv out.bin --symbol USATECH.IDX-USD --tf 1 --format mt4
  python dataconvert.py history.hst out.bin --symbol EURUSD --tf 5 --format mt4hst
  python dataconvert.py BTCUSDT-1h-2023-01.csv out.bin --symbol BTCUSDT --tf 60 --format binance

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
from datetime import datetime, timedelta, timezone, tzinfo
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

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

_RE_FIXED_OFFSET = re.compile(r"^(?:UTC|GMT)?([+-])(\d{1,2})(?::?(\d{2}))?$", re.IGNORECASE)


# =============================================================================
# The source timezone is a property of the SOURCE, not of the format
# =============================================================================
#
# Dukascopy has confirmed in writing that its historical raw data is UTC with no
# broker offset. Other exports do not behave that way: MT4/MT5 timestamps are
# broker *server* time, which for most brokers is EET with a DST jump, and
# NinjaTrader / TradeStation stamp whatever display timezone the user had set.
#
# Every non-Dukascopy branch of parse_ts() used to build the datetime with
# `tzinfo=timezone.utc` and no offset logic at all -- so a wall clock was simply
# relabelled as UTC. That is silently wrong by however many hours the source was
# offset, and nothing reported it: bars land in the file, the count is right,
# and the error only surfaces later as signals an hour out of place.
#
# What follows makes the timezone explicit. A format whose timestamps are
# self-describing keeps working with no argument; one whose timestamps are a
# bare wall clock now REQUIRES --tz and aborts with a message when it is absent.
# Nothing guesses.

#: How each input format's timestamps are anchored in time.
#:
#:   "epoch"   the value IS an instant (Unix seconds/ms). No timezone to supply,
#:             and supplying one would be meaningless.
#:   "in_data" each record carries its own offset, or the vendor has pinned the
#:             feed to UTC. Dukascopy is both: "…UTC+02:00" when the suffix is
#:             present, UTC when it is not.
#:   "wall"    a bare wall clock with no anchor. --tz is REQUIRED.
#:
#: Unlisted formats are treated as "wall", so a format added later demands an
#: explicit timezone rather than quietly inheriting the bug this replaces.
#: Every FORMATS key is expected to have an entry here, so the fallback is a
#: safety net and not the normal path.
TZ_POLICY: Dict[str, str] = {
    "dukascopy":    "in_data",
    "mt4":          "wall",
    "mt4hst":       "wall",
    "mt5":          "wall",
    "tradingview":  "epoch",
    "ninjatrader":  "wall",
    "tradestation": "wall",
    "binance":      "epoch",
    "bybit":        "epoch",
    "generic":      "wall",
}

_TZ_EXAMPLES = (
    "  --tz Europe/Helsinki     most MT4/MT5 brokers (EET/EEST, DST-aware)\n"
    "  --tz America/New_York    US exchange local time\n"
    "  --tz Europe/Berlin       German local time\n"
    "  --tz UTC                 the export is already UTC\n"
    "  --tz +02:00              a fixed offset that never observes DST"
)


class SourceTimezoneRequired(ValueError):
    """Raised when a format's timestamps are a bare wall clock and no --tz was given.

    If the source timezone cannot be determined, abort with a clear message
    instead of guessing.
    """


class RecordsNotInOrder(ValueError):
    """Raised when a payload's timestamps do not ascend, so nothing is written.

    A tick payload written in arrival order and two bars stamped the same
    minute are the two halves of one output contract: a BAR6 payload ascends,
    and for bars it ascends STRICTLY. It is the same policy the BAR6 format's
    other writers enforce -- many ticks share a millisecond, so ticks may
    repeat a timestamp; two bars never may -- applied here as well.

    Attributes:
      offenders  list of (kind, i, ts_i, j, ts_j); kind is "descent" or
                 "duplicate", i/j are 0-based positions in the payload.
      counts     the conversion counts, attached by `convert()` so a refusal
                 still carries the conversion counts that explain it.
    """

    def __init__(self, message, offenders=None, counts=None):
        super().__init__(message)
        self.offenders = list(offenders or ())
        self.counts = counts


def resolve_tz(spec: Optional[str]) -> Optional[tzinfo]:
    """Turn a --tz string into a tzinfo. None stays None.

    Accepts an IANA zone name ("Europe/Helsinki"), which is what makes DST
    handling correct across a transition, or a fixed offset ("+02:00", "-5",
    "UTC+02:00") for sources that genuinely never shift.
    """
    if spec is None:
        return None
    spec = spec.strip()
    if not spec:
        return None
    if spec.upper() in ("UTC", "GMT", "Z"):
        return timezone.utc

    m = _RE_FIXED_OFFSET.match(spec)
    if m:
        sign, hh, mm = m.groups()
        delta = timedelta(hours=int(hh), minutes=int(mm or 0))
        if sign == "-":
            delta = -delta
        if abs(delta) > timedelta(hours=18):
            raise ValueError(f"Timezone offset out of range: {spec!r}")
        return timezone(delta)

    try:
        return ZoneInfo(spec)
    except (ZoneInfoNotFoundError, ValueError) as exc:
        raise ValueError(
            f"Unknown timezone {spec!r}: {exc}\n"
            f"Give an IANA zone name or a fixed offset. Examples:\n{_TZ_EXAMPLES}"
        ) from exc


def require_tz(fmt: str, tz: Optional[tzinfo]) -> Optional[tzinfo]:
    """Enforce TZ_POLICY for `fmt`. Returns the tz the parsers should use.

    For "wall" formats a missing tz is fatal. For the other two a tz is not
    merely unnecessary, it is a claim about the data that is not true, so
    anything other than UTC is refused rather than silently ignored -- a
    caller who passes --tz America/New_York to a Binance file has a
    misunderstanding worth stopping for.
    """
    policy = TZ_POLICY.get(fmt, "wall")

    if policy == "wall":
        if tz is None:
            raise SourceTimezoneRequired(
                f"Format {fmt!r} stores a bare wall-clock timestamp with no timezone "
                f"in it, so the source timezone cannot be derived from the file.\n"
                f"Pass --tz to say what the timestamps mean. Examples:\n{_TZ_EXAMPLES}\n"
                f"Reading them as UTC is what this check exists to prevent: it is "
                f"silently wrong by the source's offset and shows up much later as "
                f"signals an hour out of place."
            )
        return tz

    if tz is not None and tz is not timezone.utc and tz != timezone.utc:
        kind = ("carry their own UTC offset" if policy == "in_data"
                else "are Unix epoch values, which are UTC by definition")
        raise ValueError(
            f"--tz cannot be applied to format {fmt!r}: its timestamps {kind}, "
            f"so an external timezone would be applied on top of an anchor that "
            f"is already correct. Drop --tz (or pass --tz UTC)."
        )
    return None


def wallclock_to_utc_ms(dt_naive: datetime, tz: Optional[tzinfo]) -> int:
    """Interpret a naive wall clock as being in `tz` and return Unix ms.

    `tz=None` means UTC, which is only reached through paths that have already
    been through require_tz().

    A ZoneInfo carries the whole DST history, so 2024-03-31 02:00 in
    Europe/Helsinki is +02:00 and 04:00 the same morning is +03:00 -- the two
    sides of that spring transition, from one unchanged call.
    """
    return int(dt_naive.replace(tzinfo=tz or timezone.utc).timestamp() * 1000)


class SourceClock:
    """One source file's wall clock, read in ROW ORDER.

    WHAT IT IS FOR
    --------------
    A zone that falls back repeats an hour of local labels. Europe/Helsinki
    goes back at 01:00 UTC on 27 October 2024, 04:00 local becoming 03:00, so
    03:00-03:59 local happens TWICE and a broker export whose server time
    observes DST contains both copies, both labelled the same.

    `wallclock_to_utc_ms` above leaves `fold=0`, which resolves BOTH copies to
    the first, still-DST occurrence -- one instant for two rows. `write_v2`
    refuses that rather than writing it, which is honest but leaves a user with
    a legitimate multi-year MT4 export unable to convert it at all.

    HOW THE TWO ARE TOLD APART
    --------------------------
    The information is in the file, in the ROW ORDER: within one ambiguous
    hour, the run of rows before the clock went back is the first occurrence
    and the run after it is the second. The design decision: resolve by
    position, because the wall clock alone cannot distinguish them and the
    order is the only surviving record of the difference.

    So this object is stateful and per FILE. It sees each row's wall clock once,
    in the order the parser read it, and the second appearance of an ambiguous
    label gets `fold=1` -- the repeated hour, one hour later in UTC.

    HOW AMBIGUOUS AND NONEXISTENT ARE DECIDED
    -----------------------------------------
    By asking the zone, not by knowing transition dates. For a naive local time
    with a zone, PEP 495 gives `fold=0` the offset in force BEFORE the nearest
    transition and `fold=1` the one after, so:

        off0 == off1   an ordinary local time
        off0 >  off1   the clocks went back: this label happens twice
        off0 <  off1   the clocks went forward: this label never happened

    Measured against Europe/Helsinki 2024 in both directions before this class
    was written, and all three cases are covered by tests.

    WHAT IT DOES NOT CLAIM
    ----------------------
    * A file whose FIRST rows already sit inside an ambiguous hour -- an export
      that begins mid-transition -- has no way to say whether its first copy is
      the first occurrence. It is read as the first, which is the documented
      `fold=0` assumption and the only one a forward-running file supports.
    * A label appearing a THIRD time is not DST; it is a genuine duplicate, and
      it keeps `fold=1` so that `write_v2` refuses the payload as it should.
    * A fixed offset (`--tz +02:00`) has no transitions, so nothing is resolved
      and nothing is claimed: `isinstance(tz, timezone)` short-circuits the
      whole check. A broker that DOES observe DST read through a fixed offset
      still collides, and `write_v2`'s refusal message says so.
    """

    def __init__(self, tz: Optional[tzinfo] = None):
        self.tz = tz
        #: Rows this clock has resolved. Also the ordinal in `nonexistent`.
        self.rows = 0
        #: Rows resolved into the REPEATED copy of an ambiguous hour.
        self.repeated_hour_rows = 0
        #: [(row ordinal, naive datetime)] for labels the zone never had.
        self.nonexistent: List[Tuple[int, datetime]] = []
        # Only a zone with transitions can be ambiguous. A fixed offset and
        # plain UTC cannot, and probing them per row would cost two utcoffset()
        # calls on every row of every file for an answer that is always "no".
        self._dst_aware = tz is not None and not isinstance(tz, timezone)
        self._seen: Dict[datetime, int] = {}

    def to_utc_ms(self, dt_naive: datetime) -> int:
        """This row's wall clock as Unix ms, resolved against the rows before it."""
        self.rows += 1
        if not self._dst_aware:
            return wallclock_to_utc_ms(dt_naive, self.tz)

        tz = self.tz
        off0 = dt_naive.replace(tzinfo=tz, fold=0).utcoffset()
        off1 = dt_naive.replace(tzinfo=tz, fold=1).utcoffset()

        if off0 == off1:
            # An ordinary local time ends any ambiguous window: the labels seen
            # inside one cannot be confused with the next transition's, which is
            # months of rows away. Clearing here is what keeps the map to at
            # most one hour of labels rather than the whole file's.
            self._seen.clear()
            return int(dt_naive.replace(tzinfo=tz).timestamp() * 1000)

        if off0 < off1:
            self.nonexistent.append((self.rows, dt_naive))
            # Still returns a number: the refusal is raised once, by the parser,
            # with every offending row named. Raising from here would land in
            # the per-row `except (ValueError, IndexError)` every parser has and
            # be reported as an unreadable row instead.
            return int(dt_naive.replace(tzinfo=tz, fold=0).timestamp() * 1000)

        seen = self._seen.get(dt_naive, 0)
        self._seen[dt_naive] = seen + 1
        if seen:
            self.repeated_hour_rows += 1
            return int(dt_naive.replace(tzinfo=tz, fold=1).timestamp() * 1000)
        return int(dt_naive.replace(tzinfo=tz, fold=0).timestamp() * 1000)

    def refuse_nonexistent(self, path: str, limit: int = 5) -> None:
        """Raise NonexistentWallClock if any row named a local time the zone skipped.

        Called by the parser AFTER the whole file, for the reason given above:
        the per-row handler swallows ValueError, so a refusal has to happen
        where nothing is catching one. Nothing is written either way -- the
        parsers return to `convert()`, which writes only after they return.
        """
        if not self.nonexistent:
            return
        shown = self.nonexistent[:limit]
        lines = ["  row %d names %s, which %s never had"
                 % (i, dt.strftime("%Y-%m-%d %H:%M:%S"), self.tz)
                 for i, dt in shown]
        if len(self.nonexistent) > limit:
            lines.append("  ... and %d more" % (len(self.nonexistent) - limit))
        raise NonexistentWallClock(
            "Refusing to convert %s: %d row(s) name a local time that does not "
            "exist in %s.\n%s\n"
            "On the morning the clocks go forward an hour of local labels is "
            "skipped, so no clock in that zone ever read them. They cannot be "
            "converted, only invented -- and the instant invented for them is "
            "the one the real row an hour later already has.\n"
            "The usual cause is a --tz that is not the file's zone: a broker "
            "whose server clock does NOT observe DST, read as a zone that "
            "does. Pass that server's fixed offset instead (--tz +02:00), or "
            "the zone the file was really exported in."
            % (path, len(self.nonexistent), self.tz, "\n".join(lines)),
            offenders=list(self.nonexistent))


def _wall(dt_naive: datetime, tz: Optional[tzinfo],
          clock: Optional[SourceClock]) -> int:
    """One row's wall clock -> Unix ms, through the file's clock when it has one.

    Without a clock this is exactly `wallclock_to_utc_ms`, i.e. the behaviour
    every direct `parse_ts(date, time, tz)` caller gets. The
    resolution by row position needs state that a single call cannot have, so a
    caller with no file to read in order keeps the documented `fold=0` reading.
    """
    return (clock.to_utc_ms(dt_naive) if clock is not None
            else wallclock_to_utc_ms(dt_naive, tz))


def parse_ts(date_str: str, time_str: str = "", tz: Optional[tzinfo] = None,
             clock: Optional[SourceClock] = None) -> int:
    """Parse various date/time strings -> Unix milliseconds. Returns 0 on failure.

    `tz` is the source timezone for the branches that read a bare wall clock.
    It is deliberately ignored by the branches whose input already fixes an
    instant (Dukascopy's own offset suffix, Unix epochs).

    `clock` is the file's `SourceClock`. Given one, the wall-clock branches
    resolve a repeated DST hour by the position of the row within the file;
    without one they keep the documented `fold=0` reading, which is the only
    thing a single call can decide.
    """
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
            sign = 1 if tz_h.startswith("+") else -1
            offset = timedelta(hours=sign * int(tz_h.lstrip("+-")),
                               minutes=sign * int(tz_m))
            dt = dt - offset
        return int(dt.timestamp() * 1000)

    # NinjaTrader compact: "20200102 070500" — wall clock, needs `tz`.
    m = _RE_NT.match(s)
    if m:
        d, ti = m.groups()
        return _wall(
            datetime(int(d[:4]), int(d[4:6]), int(d[6:8]),
                     int(ti[:2]), int(ti[2:4]), int(ti[4:6])), tz, clock)

    # MT4/MT5/ISO date: "2020.01.02" or "2020-01-02" with optional separate time
    # — wall clock in the broker's server timezone, needs `tz`.
    m = _RE_MT.match(s)
    if m:
        yyyy, mm, dd = m.groups()
        tp = t.split(":") if t else ["0", "0", "0"]
        hh = int(tp[0]) if len(tp) > 0 else 0
        mi = int(tp[1]) if len(tp) > 1 else 0
        ss = int(float(tp[2])) if len(tp) > 2 else 0
        return _wall(
            datetime(int(yyyy), int(mm), int(dd), hh, mi, ss), tz, clock)

    # US date: "01/02/2020" or "01-02-2020" — wall clock, needs `tz`.
    m = _RE_US.match(s)
    if m:
        mo, dd, yyyy = m.groups()
        tp = t.split(":") if t else ["0", "0", "0"]
        hh = int(tp[0]) if len(tp) > 0 else 0
        mi = int(tp[1]) if len(tp) > 1 else 0
        ss = int(float(tp[2])) if len(tp) > 2 else 0
        return _wall(
            datetime(int(yyyy), int(mo), int(dd), hh, mi, ss), tz, clock)

    # Unix timestamp (seconds or milliseconds)
    try:
        v = float(s)
        if 1e9 < v < 1e13:
            return int(v * 1000) if v < 1e12 else int(v)
    except ValueError:
        pass

    # ISO 8601 / generic fallback. A trailing "Z" or an explicit offset already
    # fixes the instant; anything else is a wall clock and needs `tz`.
    naive_iso = None
    try:
        full = (s + " " + t).strip()
        had_zulu = full.endswith("Z")
        dt = datetime.fromisoformat(full.rstrip("Z"))
        if had_zulu:
            return int(dt.replace(tzinfo=timezone.utc).timestamp() * 1000)
        if dt.tzinfo is not None:
            return int(dt.timestamp() * 1000)
        naive_iso = dt
    except Exception:
        pass
    if naive_iso is not None:
        # Resolved OUTSIDE the try. `except Exception` above would
        # swallow anything the clock raises and this function would answer 0 --
        # "unreadable row", which is a quieter and more misleading failure than
        # the one being reported.
        return _wall(naive_iso, tz, clock)

    return 0


def ts_to_str(ts_ms: int) -> str:
    dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
    return dt.strftime("%Y-%m-%d %H:%M UTC")


def ts_to_str_exact(ts_ms: int) -> str:
    """Millisecond-precision rendering, for messages that must name a record.

    `ts_to_str` truncates to the minute, which is right for a range a user reads
    and useless for telling two ticks 300 ms apart apart.
    """
    dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
    return dt.strftime("%Y-%m-%d %H:%M:%S.") + "%03d UTC" % (ts_ms % 1000)


# =============================================================================
# Binary writer
# =============================================================================
class NonexistentWallClock(ValueError):
    """Raised when a row names a local time its source timezone never had.

    The other half of DST handling (see SourceClock). A zone that springs
    forward skips an hour of local
    labels: Europe/Helsinki jumps 03:00 -> 04:00 on 31 March 2024, so no clock
    in that zone ever read 03:30 that morning.

    `dt.replace(tzinfo=tz)` does not refuse such a label -- it INVENTS an
    instant for it, the one the pre-transition offset gives, which is the same
    instant the real row an hour later maps to. So a complete export produces
    two rows on one instant and `write_v2` refuses it while naming autumn; an
    export that only has the invented rows is shifted by an hour and nothing
    says so at all.

    There is no right answer to recover: the label does not denote a moment.
    The realistic cause is a --tz that is not the file's zone -- a server on a
    fixed offset read as a DST zone -- and that is worth stopping for, in the
    same spirit as SourceTimezoneRequired: a conversion that cannot timestamp
    its rows must not produce a file full of plausible ones.

    Attributes:
      offenders  [(row ordinal, naive datetime)], 1-based over the rows that
                 PARSED, in the order they were read.
    """

    def __init__(self, message, offenders=None):
        super().__init__(message)
        self.offenders = list(offenders or ())


def _payload_span(records: List[Dict]) -> Tuple[int, int]:
    """(earliest, latest) timestamp over `records` -- min and max, not first and last.

    The header used to carry `records[0]["ts"]` and
    `records[-1]["ts"]`. For a payload that ascends those are the same two
    numbers; for one that does not they are a range that can be INVERTED, with
    `start` later than `end`, so a consumer computing a span gets a negative
    one. `_refuse_unordered` below now makes the unordered case unwritable, so
    the two agree for every file this module produces -- but the header is
    derived from the extremes rather than from two positions, so it states
    something that is true of the payload rather than something that happens to
    be true of a sorted one.

    Streamed, not sorted: a Dukascopy tick export is tens of millions of records
    and already holds a dict each, so an extra list of timestamps is not
    affordable here.
    """
    if not records:
        raise ValueError("No records to measure")
    lo = hi = records[0]["ts"]
    for r in records:
        ts = r["ts"]
        if ts < lo:
            lo = ts
        elif ts > hi:
            hi = ts
    return lo, hi


def _refuse_unordered(records: List[Dict], path: str, what: str,
                      allow_repeats: bool, limit: int = 5) -> None:
    """Raise RecordsNotInOrder unless `records` ascend. Writes nothing either way.

    `allow_repeats` is the one difference between the two payload types, and it
    is not a convenience: many ticks share a millisecond, so a repeated tick
    timestamp is the instrument, not a defect. Two bars stamped one minute are
    always a defect -- they double-count that minute for every consumer -- so the
    bar payload must ascend strictly. Same rule, same reasoning and very nearly
    the same message as the BAR6 format's other writers use.

    Called BEFORE the output file is created, so a refusal leaves no truncated
    .bin behind for the next run to read as real.
    """
    offenders = []
    total = len(records)
    prev = None
    for i in range(total):
        ts = records[i]["ts"]
        if prev is not None:
            if ts < prev:
                offenders.append(("descent", i - 1, prev, i, ts))
            elif ts == prev and not allow_repeats:
                offenders.append(("duplicate", i - 1, prev, i, ts))
        prev = ts
    if not offenders:
        return

    descents = sum(1 for o in offenders if o[0] == "descent")
    repeats = len(offenders) - descents
    lines = []
    for kind, i, ts_i, j, ts_j in offenders[:limit]:
        if kind == "descent":
            lines.append("  record %d at %s is followed by record %d at %s, "
                         "which is earlier" % (i + 1, ts_to_str_exact(ts_i),
                                               j + 1, ts_to_str_exact(ts_j)))
        else:
            lines.append("  record %d and record %d are both stamped %s"
                         % (i + 1, j + 1, ts_to_str_exact(ts_i)))
    if len(offenders) > limit:
        lines.append("  ... and %d more" % (len(offenders) - limit))

    found = []
    if descents:
        found.append("%d record%s earlier than the record before %s" % (
            descents, " arrives" if descents == 1 else "s arrive",
            "it" if descents == 1 else "them"))
    if repeats:
        found.append("%d record%s a timestamp with the record before %s" % (
            repeats, " shares" if repeats == 1 else "s share",
            "it" if repeats == 1 else "them"))

    why = ("A BAR6 payload is read as ascending, and the file's header states "
           "its first and last timestamp as the range it covers.")
    if allow_repeats:
        why += ("\nAn unordered tick payload therefore reads time backwards and "
                "claims a range it does not have.\n"
                "Sort the export by timestamp and convert again. Converting the "
                "same file to bars instead (--tf) sorts for you.")
    else:
        why += ("\nTwo bars at one timestamp would count that moment twice in "
                "every backtest that reads this file.\n"
                "The usual cause is two exports concatenated over an "
                "overlapping range.\n"
                "The other one is a wall-clock export spanning the autumn DST "
                "transition, where the repeated local hour carries one label "
                "twice. That is resolved by row position when "
                "--tz is a zone NAME (Europe/Helsinki), which knows where its "
                "transitions are; a fixed offset (--tz +02:00) has none, so a "
                "broker whose server clock DOES observe DST still collides. "
                "Pass the zone name instead.")

    # A repeated bar timestamp is in order but not STRICTLY in order, and
    # saying so is the difference between a message a user can act on and
    # one that looks wrong to them because their rows plainly do ascend.
    order = "in time order" if allow_repeats else "in strictly ascending time order"
    positions = ("Positions count the records that PARSED, not lines in the "
                 "source file." if allow_repeats else
                 "Positions are in the sorted payload, not lines in the source "
                 "file -- the parser sorts before writing.")
    raise RecordsNotInOrder(
        "Refusing to write %s: %d %s records are not %s.\n"
        "%s.\n%s\n%s\n%s" % (path, total, what, order,
                             " and ".join(found), "\n".join(lines),
                             positions, why),
        offenders=offenders)


def write_v2(path: str, bars: List[Bar], symbol: str, tf_min: int) -> int:
    """Write bars list to a BAR6 .bin file. Returns total bytes written.

    Refuses a payload that does not ascend strictly. A bar source is
    sorted by every parser here, but nothing de-duplicates it, so two CSV rows
    stamped the same minute used to become two records in the file.
    """
    if not bars:
        raise ValueError("No bars to write")
    _refuse_unordered(bars, path, "bar", allow_repeats=False)
    first_ts, last_ts = _payload_span(bars)
    sym_b = symbol.encode("ascii", errors="replace")[:16].ljust(16, b"\x00")
    hdr = struct.pack(HDR_V2_FMT,
                      MAGIC_V2, FORMAT_VERSION, PAYLOAD_BARS,
                      tf_min, len(bars),
                      first_ts, last_ts,
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

    Refuses a payload whose timestamps descend. `parse_dukascopy_ticks`
    returns arrival order and has never sorted, so a shuffled or concatenated
    export used to be written out of order, with a header whose start was later
    than its end. Repeated timestamps are still accepted: many ticks share a
    millisecond.
    """
    if not ticks:
        raise ValueError("No ticks to write")
    _refuse_unordered(ticks, path, "tick", allow_repeats=True)
    first_ts, last_ts = _payload_span(ticks)
    sym_b = symbol.encode("ascii", errors="replace")[:16].ljust(16, b"\x00")
    hdr = struct.pack(HDR_V2_FMT,
                      MAGIC_V2, FORMAT_VERSION, PAYLOAD_TICKS,
                      0,           # timeframe_min = 0 for ticks
                      len(ticks),
                      first_ts, last_ts,
                      sym_b)
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    with open(path, "wb") as f:
        f.write(hdr)
        for tk in ticks:
            f.write(struct.pack(TICK_FMT, tk["ts"], tk["bid"], tk["ask"], 0.0))
    return HDR_V2_SIZE + len(ticks) * TICK_SIZE


# =============================================================================
# Conversion counts
# =============================================================================
#
# Every parser below drops rows it cannot read and repairs rows that arrive out
# of order. Both are the right behaviour -- the alternative to skipping an
# unreadable row is refusing a whole broker export over one bad line, and the
# alternative to sorting is a file whose bars go backwards. What was wrong is
# that neither left a trace a user could see: the counts lived in a local
# variable, went into a console line the GUI never shows, and never reached
# `convert()`'s return value. A CSV where half the rows failed to parse
# converted "successfully" and produced a smaller file, and nothing said so.
#
# A .bin file is checked when a user uploads it. This is the step before that:
# what the converter threw away on the way to making the file. The principle
# is to count at write time, so a data defect cannot reach a .bin silently.
#
# NOTHING IN THIS SECTION CHANGES WHAT IS WRITTEN. These are measurements.
#
# What they measure is now also acted on, one layer down: the two writers
# refuse a payload that does not ascend. The counts and the
# refusal are deliberately separate -- the counts describe the INPUT and are
# reported for every conversion including the successful ones, while the
# refusal is about the FILE and happens only when one would be wrong. A refusal
# carries the counts with it (see `RecordsNotInOrder.counts`), because "this
# export is shuffled" is the explanation for "this file was not written".


def new_conversion_counts() -> Dict[str, int]:
    """A fresh accumulator for one conversion.

    rows_parsed           records the parser accepted (ticks for a tick
                          source, bars otherwise) -- the denominator that
                          makes the other three readable.
    skipped_rows          input rows no parser could read.
    out_of_order_rows     rows that arrived with a timestamp EARLIER than the
                          row before them. This says how shuffled the source
                          was. On a bar path the sort repairs them; on the raw
                          tick path nothing sorts, so the writer refuses the
                          conversion instead of writing a backwards file.
    duplicate_timestamps  records sharing a timestamp with another record. For
                          a tick source these collapse into one bar during
                          aggregation, and are legal in a raw tick payload --
                          many ticks share a millisecond. For a bar source they
                          are a defect and the writer refuses the conversion.
    dst_repeated_rows     rows a wall-clock source placed in the
                          REPEATED copy of an autumn DST hour, resolved by
                          their position in the file. Structurally zero for
                          every source that is not a bare wall clock in a zone
                          with transitions, and that zero is worth printing:
                          it is the difference between "this file did not span
                          a transition" and "nobody looked".
    """
    return {
        "rows_parsed": 0,
        "skipped_rows": 0,
        "out_of_order_rows": 0,
        "duplicate_timestamps": 0,
        "dst_repeated_rows": 0,
    }


def _measure_timestamps(records: List[Dict]) -> tuple:
    """(out-of-order rows, duplicate timestamps) for one parsed record list.

    Out-of-order is counted over the order the records ARRIVED in, which is the
    only place that information exists -- after the sort it is gone. Duplicates
    are counted over sorted timestamps, so a shuffled source cannot hide a
    duplicate by separating the two rows.

    Both passes are O(1) in extra memory beyond one list of timestamps: a
    Dukascopy tick export is tens of millions of records and already holds a
    dict per record, so a set keyed on every timestamp is not affordable here.
    """
    out_of_order = 0
    stamps = []
    prev = None
    for r in records:
        ts = r["ts"]
        stamps.append(ts)
        if prev is not None and ts < prev:
            out_of_order += 1
        prev = ts
    stamps.sort()
    duplicates = 0
    for i in range(1, len(stamps)):
        if stamps[i] == stamps[i - 1]:
            duplicates += 1
    return out_of_order, duplicates


def _record(stats: Optional[Dict[str, int]], records: List[Dict], skipped: int,
            clock: Optional["SourceClock"] = None) -> None:
    """Add one parser's measurements to the conversion's accumulator."""
    if stats is None:
        return
    out_of_order, duplicates = _measure_timestamps(records)
    stats["rows_parsed"] += len(records)
    stats["skipped_rows"] += skipped
    stats["out_of_order_rows"] += out_of_order
    stats["duplicate_timestamps"] += duplicates
    if clock is not None:
        # The clock is the only thing that knows -- after the sort the
        # two copies of a repeated hour are just two ordinary ascending rows.
        stats["dst_repeated_rows"] = (stats.get("dst_repeated_rows", 0)
                                      + clock.repeated_hour_rows)


def _measure_and_sort(records: List[Dict], stats: Optional[Dict[str, int]],
                      skipped: int = 0,
                      clock: Optional["SourceClock"] = None) -> List[Dict]:
    """Count what the sort is about to hide, then sort exactly as before."""
    _record(stats, records, skipped, clock)
    return sorted(records, key=lambda x: x["ts"])


def counts_summary(stats: Optional[Dict[str, int]], tick_payload: bool = False) -> str:
    """One line naming every count, including the zeros, and what happens to it.

    The zeros are the point. "0 rows skipped" is a measurement a user can rely
    on; a line that appears only when something went wrong is indistinguishable
    from a line nobody remembered to print.

    `tick_payload` is not decoration: the two payload types answer the same two
    counts differently, and the line has to say which answer applies or it is a
    false claim about the file.

      out of order   bar path: every parser ends in a sort, so it was repaired.
                     raw tick path (`--ticks`): nothing sorts, so the writer
                     refuses the conversion -- a successful one therefore has
                     none, and the parenthetical states the policy, not a
                     repair that happened.
      duplicates     bar path: refused, for the same reason.
                     raw tick path: kept, because many ticks genuinely share a
                     millisecond and collapsing them would delete real data.
      repeated DST   resolved, on both paths -- the second copy of an
                     ambiguous local hour is the repeated one. Named here for
                     the same reason as the zeros: a file whose hour WAS moved
                     an hour later than its label reads should say so.

    `.get()` on the last count, not `[...]`: a stats dict built by an older
    version -- a caller holding one across an upgrade, or a refusal's attached copy --
    has four keys, and this line must describe it rather than raise on it.
    """
    if not stats:
        return ""
    order_fate = "refused" if tick_payload else "sorted"
    dupe_fate = ("kept -- ticks may share a millisecond" if tick_payload
                 else "refused")
    return (f"{stats['rows_parsed']:,} rows parsed  |  "
            f"{stats['skipped_rows']:,} unreadable rows skipped  |  "
            f"{stats['out_of_order_rows']:,} rows out of order ({order_fate})  |  "
            f"{stats['duplicate_timestamps']:,} duplicate timestamps ({dupe_fate})"
            f"  |  {stats.get('dst_repeated_rows', 0):,} rows in a repeated DST "
            f"hour (resolved by row order)")


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


def parse_dukascopy(path: str, tf_min: int, progress: ProgressFn = None,
                    tz: Optional[tzinfo] = None,
                    stats: Optional[Dict[str, int]] = None) -> List[Bar]:
    """
    Dukascopy tick CSV:
      LocalTime,Ask,Bid,AskVolume,BidVolume
      01.01.2020 00:01:00.288,11348.0,11347.7,1.0,1.0
    Ticks are aggregated into OHLCV bars.
    """
    ticks = parse_dukascopy_ticks(path, progress, stats=stats)
    if progress:
        progress(f"  Aggregating {len(ticks):,} ticks -> M{tf_min} bars")
    # aggregate_ticks is NOT measured. Bucketing many ticks into one
    # bar is what this format is, not a repair or a loss -- counting it would
    # report every normal conversion as having merged millions of "duplicates".
    # The duplicate tick timestamps that DO collapse are counted above, on the
    # ticks themselves.
    return aggregate_ticks(ticks, tf_min)


def parse_dukascopy_ticks(path: str, progress: ProgressFn = None,
                          stats: Optional[Dict[str, int]] = None) -> List[Dict]:
    """Parse Dukascopy tick CSV and return raw tick list [{ts, bid, ask}, ].

    The ticks are returned in ARRIVAL order -- this parser has never
    sorted, and does not start now. The counts are still taken, so a shuffled
    tick export is reported rather than silently written out of order.
    """
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
    _record(stats, ticks, skipped)
    if progress:
        progress(f"  Loaded {len(ticks):,} ticks ({skipped:,} skipped)")
    return ticks


def _make_bar(ts: int, o: float, h: float, l: float, c: float) -> Bar:
    return {"ts": ts, "open": o, "high": h, "low": l, "close": c, "bid": c, "ask": c}


def parse_mt4(path: str, tf_min: int, progress: ProgressFn = None,
              tz: Optional[tzinfo] = None,
              stats: Optional[Dict[str, int]] = None) -> List[Bar]:
    """
    MT4 / MT5 bar CSV:
      <DATE>,<TIME>,<OPEN>,<HIGH>,<LOW>,<CLOSE>,<TICKVOL>,<VOL>,<SPREAD>
      2020.01.02,07:05,11430.0,11439.0,11428.0,11430.0,234,0,0

    Also handles MT5 (same layout, tab- or comma-delimited).
    Handles optional combined datetime column (no separate time col).
    """
    bars: List[Bar] = []
    skipped = 0
    clock = SourceClock(tz)          # one per FILE, read in row order
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
                ts = parse_ts(row[date_col], t_str, tz, clock)
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
    clock.refuse_nonexistent(path)
    return _measure_and_sort(bars, stats, skipped, clock)


def parse_mt4hst(path: str, tf_min: int, progress: ProgressFn = None,
                 tz: Optional[tzinfo] = None,
                 stats: Optional[Dict[str, int]] = None) -> List[Bar]:
    """
    MetaTrader 4 binary .hst history file.
    Version 400: 148-byte header + 44-byte records  {time(u32), open, low, high, close, vol  (5xf64)}
    Version 401: 148-byte header + 60-byte records  {time(i64), open, high, low, close (4xf64),
                                                      tick_vol(u64), spread(i32), real_vol(u64)}
    Note: v400 stores LOW before HIGH; v401 stores HIGH before LOW.

    MT4's CTM field looks like a Unix timestamp and is not one. It is
    the broker's SERVER wall clock encoded as though that wall clock were UTC,
    so a v401 bar stamped 1711843200 means "2024-03-31 00:00 server time", not
    that instant in UTC. Reading it as an epoch is the same silent offset error
    as reading an MT4 CSV's wall clock as UTC, which is why "mt4hst" is a
    "wall" format in TZ_POLICY and gets the same --tz treatment: decode to the
    naive wall clock first, then anchor it with `tz`.
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
    clock = SourceClock(tz)          # the records are in file order

    def _ctm_to_utc_ms(time_s: int) -> int:
        """Server wall clock encoded as a pseudo-epoch -> real UTC ms."""
        wall = datetime.fromtimestamp(time_s, tz=timezone.utc).replace(tzinfo=None)
        return clock.to_utc_ms(wall)

    if version == 400:
        # CTM(uint32) OPEN LOW HIGH CLOSE VOL  - all floats as double
        FMT, SZ = "<Iddddd", 44
        n = len(data) // SZ
        for i in range(n):
            time_s, o, low, high, c, _ = struct.unpack_from(FMT, data, i * SZ)
            bars.append(_make_bar(_ctm_to_utc_ms(time_s), o, high, low, c))
    else:  # 401
        # CTM(int64) OPEN HIGH LOW CLOSE  tick_vol(uint64) spread(int32) real_vol(uint64)
        FMT, SZ = "<qddddQiQ", 60
        n = len(data) // SZ
        for i in range(n):
            time_s, o, high, low, c, _tv, _sp, _rv = struct.unpack_from(FMT, data, i * SZ)
            bars.append(_make_bar(_ctm_to_utc_ms(time_s), o, high, low, c))

    if progress:
        symbol_raw = raw[4:68].rstrip(b"\x00").decode("ascii", errors="replace")
        progress(f"  Parsed {len(bars):,} MT4 v{version} bars  symbol={symbol_raw!r}")
    clock.refuse_nonexistent(path)
    return _measure_and_sort(bars, stats, 0, clock)   # binary: no row can be unreadable


def parse_tradingview(path: str, tf_min: int, progress: ProgressFn = None,
                      tz: Optional[tzinfo] = None,
                      stats: Optional[Dict[str, int]] = None) -> List[Bar]:
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
    return _measure_and_sort(bars, stats, skipped)


def parse_ninjatrader(path: str, tf_min: int, progress: ProgressFn = None,
                      tz: Optional[tzinfo] = None,
                      stats: Optional[Dict[str, int]] = None) -> List[Bar]:
    """
    NinjaTrader bar export (several layouts):
      20200102 070500;11430;11439;11428;11430;100          (compact yyyymmdd hhmmss)
      01/02/2020 07:05:00;11430;11439;11428;11430;100      (US date)
      2020-01-02 07:05:00;11430;11439;11428;11430;100      (ISO date)
    Separator: ; or ,   |   OHLCV order: open, high, low, close
    """
    bars: List[Bar] = []
    skipped = 0
    clock = SourceClock(tz)          # one per FILE, read in row order
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
                    ts = parse_ts(parts[0], parts[1], tz, clock)
                else:
                    ts = parse_ts(row[0], row[1] if len(row) > 5 else "", tz, clock)
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
    clock.refuse_nonexistent(path)
    return _measure_and_sort(bars, stats, skipped, clock)


def parse_tradestation(path: str, tf_min: int, progress: ProgressFn = None,
                       tz: Optional[tzinfo] = None,
                       stats: Optional[Dict[str, int]] = None) -> List[Bar]:
    """"
      "Date","Time","Open","High","Low","Close","Up","Down"
      01/02/2020,07:05,11430.00,11439.00,11428.00,11430.00,100,50
    Date is MM/DD/YYYY; Time is HH:MM (24h).
    """
    bars: List[Bar] = []
    skipped = 0
    clock = SourceClock(tz)          # one per FILE, read in row order
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
                ts = parse_ts(row[date_col],
                              row[time_col] if time_col < len(row) else "", tz, clock)
                if not ts:
                    skipped += 1
                    continue
                bars.append(_make_bar(ts, float(row[o_col]), float(row[h_col]),
                                      float(row[l_col]), float(row[c_col])))
            except (ValueError, IndexError):
                skipped += 1
    if progress:
        progress(f"  Loaded {len(bars):,} TradeStation bars ({skipped:,} skipped)")
    clock.refuse_nonexistent(path)
    return _measure_and_sort(bars, stats, skipped, clock)


def parse_binance(path: str, tf_min: int, progress: ProgressFn = None,
                  tz: Optional[tzinfo] = None,
                  stats: Optional[Dict[str, int]] = None) -> List[Bar]:
    """
    Binance historical klines CSV from data.binance.vision (monthly ZIP/CSV downloads)
    and Binance API OHLCV export.

    Official 12-column format:
      open_time,open,high,low,close,volume,close_time,quote_asset_volume,
      number_of_trades,taker_buy_base_volume,taker_buy_quote_volume,ignore
      1577836800000,7186.30,7214.58,7172.00,7200.85,4123.47,...

    open_time is Unix **milliseconds** (13 digits).
    Files downloadable from: https://data.binance.vision/?prefix=data/spot/monthly/klines/

    Example CLI:
      python dataconvert.py BTCUSDT-1h-2023-01.csv BTCUSDT_H1.bin \\
             --symbol BTCUSDT --tf 60 --format binance
    """
    bars: List[Bar] = []
    skipped = 0
    with open(path, newline="", encoding="utf-8", errors="replace") as f:
        reader = csv.reader(f)
        for i, row in enumerate(reader):
            if not row:
                continue
            # Skip header row if present ("open_time", "timestamp", etc.)
            first = row[0].strip()
            if i == 0 and not first.lstrip('-').replace('.', '', 1).isdigit():
                continue
            if len(row) < 5:
                skipped += 1
                continue
            try:
                t = float(first)
                # Binance timestamps are ms (13 digits); guard against raw seconds
                ts = int(t) if t > 1e12 else int(t * 1000)
                bars.append(_make_bar(
                    ts,
                    float(row[1]),  # open
                    float(row[2]),  # high
                    float(row[3]),  # low
                    float(row[4]),  # close
                ))
            except (ValueError, IndexError):
                skipped += 1
            if progress and i % 100_000 == 0 and i:
                progress(f"  Parsed {i:,} rows")
    if progress:
        progress(f"  Loaded {len(bars):,} Binance bars ({skipped:,} skipped)")
    return _measure_and_sort(bars, stats, skipped)


def parse_bybit(path: str, tf_min: int, progress: ProgressFn = None,
                tz: Optional[tzinfo] = None,
                stats: Optional[Dict[str, int]] = None) -> List[Bar]:
    """
    Bybit / Kraken / generic crypto exchange kline CSV.
    Covers exchanges that export OHLCV data with Unix **seconds** timestamps
    and a header row using common column names.

    Bybit historical klines (data.bybit.com):
      open_time,open,high,low,close,volume,turnover
      1640995200,46223.30,47183.45,46148.86,47022.00,3892.21,1.83e+08

    Kraken OHLC (api.kraken.com or kraken.com/history):
      <time>,<open>,<high>,<low>,<close>,<vwap>,<volume>,<count>
      1609459200,29388.0,29600.0,29000.0,29400.0,29194.7,1234.5,5678

    Coinbase Advanced Trade CSV export:
      start,open,high,low,close,volume
      2023-01-01T00:00:00Z,16541.77,16547.70,16530.04,16543.34,123.45

    All columns auto-detected by header name.  Falls back to positional
    (col 0=ts, col 1=open, col 2=high, col 3=low, col 4=close) if headers
    aren't recognised.

    Example CLI:
      python dataconvert.py BTCUSDT_1_2023.csv BTCUSDT_M1.bin \\
             --symbol BTCUSDT --tf 1 --format bybit
    """
    bars: List[Bar] = []
    skipped = 0
    with open(path, newline="", encoding="utf-8", errors="replace") as f:
        sample = f.read(4096)
        f.seek(0)
        delim = _auto_delim(sample)
        reader = csv.reader(f, delimiter=delim)

        # Column index defaults (positional)
        ts_col, o_col, h_col, l_col, c_col = 0, 1, 2, 3, 4
        has_header = False

        for i, raw in enumerate(reader):
            row = [c.strip().strip('"<>') for c in raw]
            if not row:
                continue

            if i == 0:
                hdrs = [h.lower() for h in row]
                # Check for header row
                if not row[0].lstrip('-').replace('.', '', 1).isdigit():
                    has_header = True
                    def _find(*names) -> int:
                        for n in names:
                            for ci, h in enumerate(hdrs):
                                if h == n or h.startswith(n):
                                    return ci
                        return -1
                    ts_col = _find("open_time", "time", "timestamp", "start", "date")
                    o_col  = _find("open")
                    h_col  = _find("high")
                    l_col  = _find("low")
                    c_col  = _find("close")
                    if any(c < 0 for c in [ts_col, o_col, h_col, l_col, c_col]):
                        # Fall back to positional
                        ts_col, o_col, h_col, l_col, c_col = 0, 1, 2, 3, 4
                    continue

            if len(row) <= max(ts_col, o_col, h_col, l_col, c_col):
                skipped += 1
                continue
            try:
                ts_raw = row[ts_col].strip()
                # Try numeric Unix timestamp first
                try:
                    t = float(ts_raw)
                    ts = int(t) if t > 1e12 else int(t * 1000)
                except ValueError:
                    # ISO 8601 / human-readable date (e.g. Coinbase)
                    ts = parse_ts(ts_raw)
                if not ts:
                    skipped += 1
                    continue
                bars.append(_make_bar(
                    ts,
                    float(row[o_col]),
                    float(row[h_col]),
                    float(row[l_col]),
                    float(row[c_col]),
                ))
            except (ValueError, IndexError):
                skipped += 1
            if progress and i % 100_000 == 0 and i:
                progress(f"  Parsed {i:,} rows")
    if progress:
        progress(f"  Loaded {len(bars):,} bars [{path}] ({skipped:,} skipped)")
    return _measure_and_sort(bars, stats, skipped)


def parse_generic(path: str, tf_min: int, progress: ProgressFn = None,
                  col_map: Optional[Dict[str, int]] = None,
                  tz: Optional[tzinfo] = None,
                  stats: Optional[Dict[str, int]] = None) -> List[Bar]:
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
                return parse_dukascopy(path, tf_min, progress, stats=stats)
            data_rows = rows

        if any(c < 0 for c in [date_col, o_col, h_col, l_col, c_col]):
            if progress:
                progress(f"  Cannot detect columns ({raw_hdr}) - trying tick parser")
            return parse_dukascopy(path, tf_min, progress, stats=stats)

    bars: List[Bar] = []
    skipped = 0
    clock = SourceClock(tz)          # one per FILE, read in row order
    for row in data_rows:
        try:
            t_str = row[time_col].strip() if 0 <= time_col < len(row) else ""
            ts = parse_ts(row[date_col].strip(), t_str, tz, clock)
            if not ts:
                skipped += 1
                continue
            bars.append(_make_bar(ts, float(row[o_col]), float(row[h_col]),
                                  float(row[l_col]), float(row[c_col])))
        except (ValueError, IndexError):
            skipped += 1
    if progress:
        progress(f"  Auto-detected {len(bars):,} bars ({skipped:,} skipped)")
    clock.refuse_nonexistent(path)
    return _measure_and_sort(bars, stats, skipped, clock)


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
    "binance":      (parse_binance,      "Binance klines CSV  (data.binance.vision, 12 columns, ms timestamps)"),
    "bybit":        (parse_bybit,        "Bybit / Kraken / Coinbase OHLCV CSV  (Unix seconds, open_time header)"),
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

        # Binance: first data row has a 13-digit ms timestamp, or header is "open_time,..."
        # and there are ≥ 6 columns
        if re.search(r"open_time,open,high,low,close", sample.replace(" ", "")):
            # Bybit uses Unix seconds, Binance uses ms
            # Distinguish by first data value: Binance ms = 13 digits
            for ln in lines[1:]:
                if ln.strip() and ln.strip()[0].isdigit():
                    first_val = ln.split(",")[0].strip()
                    return "binance" if len(first_val) >= 13 else "bybit"
            return "binance"

        # Binance without header: 12-column CSV with large ms timestamp
        for ln in lines:
            stripped = ln.strip()
            if stripped and stripped[0].isdigit():
                parts = stripped.split(",")
                if len(parts) >= 11:
                    try:
                        if float(parts[0]) > 1e12:
                            return "binance"
                    except ValueError:
                        pass
                break
    except OSError:
        pass

    return "generic"


# =============================================================================
# Main convert function
# =============================================================================
def convert(input_path: str, output_path: str, symbol: str, tf_min: int,
            fmt: Optional[str] = None, progress: ProgressFn = None,
            as_ticks: bool = False,
            col_map: Optional[Dict[str, int]] = None,
            tz: Optional[str] = None) -> dict:
    """Convert input_path -> BAR6 output_path.

    as_ticks=True: write raw ticks (payload_type=1, 32-byte TickRecord).
    col_map: manual column index mapping for generic CSVs,
             e.g. {'date': 0, 'time': 1, 'open': 2, 'high': 3, 'low': 4, 'close': 5}
    tz: source timezone for formats that store a bare wall clock.
        An IANA name or a fixed offset. Required for those formats, refused
        for the ones whose timestamps already fix an instant.

    The returned dict carries `rows_parsed`, `skipped_rows`,
    `out_of_order_rows`, `duplicate_timestamps` and `dst_repeated_rows` on
    every path. They are measurements of the
    INPUT, not of the file written -- a caller that wants to tell a user what
    the conversion threw away has to be able to read them. See
    new_conversion_counts() for what each one means.

    For a wall-clock format read through a zone NAME, the repeated
    hour of the autumn transition is resolved by the position of each row in
    the file rather than mapped twice onto one instant, and a row naming a
    local time the zone SKIPPED raises NonexistentWallClock before anything is
    written. See SourceClock.

    Raises RecordsNotInOrder, with those same counts attached, when the payload
    would not ascend: a raw tick export written in arrival order or two bars
    stamped one instant. No output file is created in that
    case. `start` and `end` in the returned dict are the payload's earliest and
    latest timestamp, not its first and last record.
    """
    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Input not found: {input_path}")

    if fmt is None:
        fmt = detect_format(input_path)
        if progress:
            progress(f"Auto-detected format: {fmt}")

    if fmt not in FORMATS:
        raise ValueError(f"Unknown format {fmt!r}. Run --formats to list valid options.")

    # Settle the source timezone BEFORE any parsing. require_tz raises
    # SourceTimezoneRequired for a wall-clock format with no --tz, so an import
    # that cannot be timestamped correctly stops here instead of producing a
    # file full of plausible, silently-shifted bars.
    source_tz = require_tz(fmt, resolve_tz(tz))
    stats = new_conversion_counts()
    if progress and source_tz is not None:
        progress(f"Source timezone: {tz} (timestamps converted to UTC on import)")

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
        ticks = parse_dukascopy_ticks(input_path, progress, stats=stats)
        if not ticks:
            raise ValueError("No ticks could be parsed from the input file")
        if progress:
            progress(f"Writing {len(ticks):,} raw ticks -> {os.path.basename(output_path)}")
        # write_ticks_v2 refuses a payload that descends. The counts
        # travel with the refusal, and out through `progress`, because they are
        # what explains it: "2 rows out of order" on a four-tick export names
        # the source's problem, where the exception alone names only ours.
        try:
            n_bytes = write_ticks_v2(output_path, ticks, symbol)
        except RecordsNotInOrder as exc:
            exc.counts = dict(stats)
            if progress:
                progress(counts_summary(stats, tick_payload=True))
            raise
        start, end = _payload_span(ticks)
        if progress:
            progress(f"DONE  {len(ticks):,} ticks  |  "
                     f"{ts_to_str(start)} -> {ts_to_str(end)}"
                     f"  |  {n_bytes / 1024 / 1024:.2f} MB written")
            progress(counts_summary(stats, tick_payload=True))
        return {
            "ticks":  len(ticks),
            "bars":   len(ticks),
            "bytes":  n_bytes,
            "start":  start,
            "end":    end,
            "format": fmt,
            "symbol": symbol,
            "tf_min": 0,
            **stats,
        }

    # Bar output path (original)
    parser_fn, _ = FORMATS[fmt]
    if progress:
        progress(f"Parsing {os.path.basename(input_path)} as [{fmt}]")

    if col_map and fmt in ("generic", None):
        bars = parse_generic(input_path, tf_min, progress, col_map=col_map,
                             tz=source_tz, stats=stats)
    else:
        bars = parser_fn(input_path, tf_min, progress, tz=source_tz, stats=stats)
    if not bars:
        raise ValueError("No bars could be parsed from the input file")

    if progress:
        progress(f"Writing {len(bars):,} M{tf_min} bars -> {os.path.basename(output_path)}")

    # write_v2 refuses two bars stamped the same instant. Same
    # reasoning as the tick path above -- the counts explain the refusal.
    try:
        n_bytes = write_v2(output_path, bars, symbol, tf_min)
    except RecordsNotInOrder as exc:
        exc.counts = dict(stats)
        if progress:
            progress(counts_summary(stats))
        raise
    start, end = _payload_span(bars)

    if progress:
        progress(f"DONE  {len(bars):,} bars  |  {ts_to_str(start)} -> {ts_to_str(end)}"
                 f"  |  {n_bytes / 1024 / 1024:.2f} MB written")
        progress(counts_summary(stats))

    return {
        "bars":   len(bars),
        "bytes":  n_bytes,
        "start":  start,
        "end":    end,
        "format": fmt,
        "symbol": symbol,
        "tf_min": tf_min,
        **stats,
    }


# =============================================================================
# --info: inspect an existing .bin file
# =============================================================================
class NotABarFile(ValueError):
    """Raised by cmd_info for a file too short to hold its format's header."""


#: The BAR5 header: magic, version, timeframe, count, start, end.
_HDR_V1_FMT = "<IIIIqq"
_HDR_V1_SIZE = struct.calcsize(_HDR_V1_FMT)  # 32


def cmd_info(path: str) -> None:
    """Print a .bin file's header.

    Raises OSError when the file cannot be read, and NotABarFile when it is
    shorter than the header its magic announces; main() reports either as a
    one-line error rather than a traceback.
    """
    with open(path, "rb") as f:
        raw = f.read(max(HDR_V2_SIZE, _HDR_V1_SIZE))

    if len(raw) < 4:
        raise NotABarFile(f"{path} is {len(raw)} bytes long -- too short to be a "
                          f"BAR file")
    magic = struct.unpack_from("<I", raw, 0)[0]
    need = {MAGIC_V2: (HDR_V2_SIZE, "BAR6"), MAGIC_V1: (_HDR_V1_SIZE, "BAR5")}.get(magic)
    if need and len(raw) < need[0]:
        raise NotABarFile(f"{path} starts like a {need[1]} file but holds only "
                          f"{len(raw)} of the {need[0]} header bytes -- it is "
                          f"truncated")

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
        mg, ver, tf, count, ts0, ts1 = struct.unpack_from(_HDR_V1_FMT, raw)
        print(f"File:        {path}")
        print(f"Format:      BAR5  (older format -- convert the source data again to get BAR6)")
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
    tk.Label(hdr, text="Dukascopy · MT4/MT5 · TradingView\nNinjaTrader · TradeStation · Binance · Bybit",
             bg=C["surf"], fg=C["dim"], font=(C["ui"][0], 7), justify="right").pack(side="right", padx=16)

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

    # Source timezone. Editable rather than a fixed list because the
    # right answer is a property of the broker the export came from, not
    # something this tool can enumerate; the common ones are offered as values.
    v_tz = tk.StringVar(value="")
    tz_row = tk.Frame(main, bg=C["bg"])
    tz_row.pack(fill="x", pady=(0, 8))
    lbl(tz_row, "Source Timezone")
    tz_inner = tk.Frame(tz_row, bg=C["bg"])
    tz_inner.pack(fill="x")
    ttk.Combobox(tz_inner, textvariable=v_tz, width=22,
                 values=["", "UTC", "Europe/Helsinki", "Europe/Berlin",
                         "Europe/London", "America/New_York", "+02:00",
                         "+03:00"]).pack(side="left")
    v_tz_desc = tk.StringVar(value="")
    tk.Label(tz_inner, textvariable=v_tz_desc, bg=C["bg"], fg=C["muted"],
             font=(C["ui"][0], 8), justify="left").pack(side="left", padx=8)

    def on_fmt_change(*_):
        val = v_format.get()
        if val == "auto":
            v_fmt_desc.set("Auto-detect from file extension and content")
        elif val in FORMATS:
            v_fmt_desc.set(FORMATS[val][1])

        # Say up front whether a timezone is needed, so the requirement is not
        # discovered only as an error after picking a file.
        policy = TZ_POLICY.get(val, "wall") if val in FORMATS else None
        if policy == "wall":
            v_tz_desc.set("REQUIRED - these timestamps are a bare wall clock "
                          "(e.g. Europe/Helsinki for most MT4/MT5 brokers)")
        elif policy == "in_data":
            v_tz_desc.set("Not needed - each record carries its own offset, "
                          "or the feed is UTC")
        elif policy == "epoch":
            v_tz_desc.set("Not needed - Unix epoch timestamps are UTC by definition")
        else:
            v_tz_desc.set("Set once the format is known")

    v_format.trace_add("write", on_fmt_change)
    on_fmt_change()

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
        tz_spec = v_tz.get().strip() or None

        def worker():
            try:
                result = convert(inp, out, sym, tf, fmt, log,
                                 as_ticks=as_ticks, col_map=active_col_map,
                                 tz=tz_spec)
                root.after(0, lambda: on_done(result))
            except Exception as exc:
                root.after(0, lambda e=exc: on_error(str(e)))

        threading.Thread(target=worker, daemon=True).start()

    def on_done(result):
        convert_btn.configure(state="normal")
        n = result.get("ticks") or result.get("bars", 0)
        unit = "ticks" if result.get("tf_min") == 0 else "bars"
        # The counts belong ON the result, not only in the scrolling
        # log -- a GUI user reads the status line and the last few lines, and
        # the console line the parsers already printed is not a surface at all.
        skipped = result.get("skipped_rows", 0)
        shuffled = result.get("out_of_order_rows", 0)
        dupes = result.get("duplicate_timestamps", 0)
        if skipped:
            # Named first and in the status line: rows that are NOT in the file
            # are the only one of the three that is a loss.
            v_status.set(f"Done - {n:,} {unit}, {skipped:,} rows skipped")
            status_lbl.configure(fg=C["red"])
        else:
            v_status.set(f"Done - {n:,} {unit}")
            status_lbl.configure(fg=C["green"])
        log(f"Output: {v_output.get()}", "accent")
        log(f"Rows parsed:          {result.get('rows_parsed', 0):,}")
        log(f"Unreadable, skipped:  {skipped:,}", "red" if skipped else "dim")
        # The fate of the last two depends on the payload, the same way
        # counts_summary()'s does -- a tick file is never sorted (the writer
        # refuses an unordered one instead) and its duplicates are legal, while
        # a bar file is sorted and its duplicates are refused.
        ticks_written = result.get("tf_min") == 0
        log(f"Out of order, {'refused' if ticks_written else 'sorted':<7}: {shuffled:,}",
            "dim" if not shuffled else "")
        log(f"Duplicate timestamps: {dupes:,}"
            + ("  (legal in a tick file)" if ticks_written else ""),
            "dim" if not dupes else "")
        if skipped:
            log("Those rows are not in the output file. Check the source "
                "export's delimiter, date format and column order.", "red")

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
                log(f"Format:    BAR5  (older format -- convert the source data again to get BAR6)", "dim")
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
    parser.add_argument("--tz",
                        help="Source timezone of the input timestamps. "
                             "An IANA zone name such as Europe/Helsinki (DST-aware) "
                             "or a fixed offset such as +02:00. REQUIRED for formats "
                             "that store a bare wall clock (mt4, mt5, mt4hst, "
                             "ninjatrader, tradestation, generic); refused for formats "
                             "whose timestamps already fix an instant (dukascopy, "
                             "binance, bybit, tradingview).")
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
        try:
            cmd_info(args.info)
        except OSError as exc:
            parser.error(f"cannot read {args.info}: {exc.strerror or exc}")
        except NotABarFile as exc:
            parser.error(str(exc))
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
        result = convert(args.input, args.output, args.symbol, args.tf or 0,
                         fmt=args.format, progress=print, as_ticks=args.ticks,
                         tz=args.tz)
        # `convert` already printed the full count line through
        # `progress`. This repeats the one count that is a LOSS, on stderr, so
        # it survives a run whose stdout is piped into a log nobody reads and
        # cannot be mistaken for part of the progress chatter. The exit code is
        # deliberately unchanged: a broker export with a few unreadable rows is
        # still a successful conversion, and failing it would break every
        # caller that converts real-world CSVs.
        if result.get("skipped_rows"):
            print(f"WARNING: {result['skipped_rows']:,} of "
                  f"{result['rows_parsed'] + result['skipped_rows']:,} input rows "
                  f"could not be read and are NOT in {args.output}",
                  file=sys.stderr)
        sys.exit(0)
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
