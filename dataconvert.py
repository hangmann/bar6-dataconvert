#!/usr/bin/env python3
"""
DataConvert - Universal market data -> BAR6 binary converter.

Supported input formats:
  dukascopy    Dukascopy tick CSV  (LocalTime, Ask, Bid, AskVolume, BidVolume)
  dukascopyweb Dukascopy web export  (Etc/UTC, Open, High, Low, Close, Volume):
               bars, or ticks of one side; the file name says which side
  mt5ticks     MT5 tick export  (<DATE> <TIME> <BID> <ASK> <LAST> <VOLUME> <FLAGS>)
  ninjatraderticks  NinjaTrader tick export  (yyyyMMdd HHmmss fffffff;last;bid;ask;volume)
  genericticks Any tick CSV with a header naming a time, a bid and an ask column
  mt4          MT4 bar CSV  (<DATE>,<TIME>,<OPEN>,<HIGH>,<LOW>,<CLOSE>,...,<SPREAD>)
  mt4hst       MT4 .hst binary history file (versions 400 and 401)
  mt5          MT5 bar CSV  (same layout as MT4, tab or comma delimited)
  tradingview  TradingView export  (time, open, high, low, close, volume)
  ninjatrader  NinjaTrader bar CSV  (date+time, or a daily bar's date alone; ; or , separated)
  tradestation TradeStation bar CSV  (Date, Time, Open, High, Low, Close, ...)
  binance      Binance historical klines (data.binance.vision ZIP/CSV, ms timestamps)
  bybit        Bybit / Kraken / generic crypto exchange OHLCV CSV (Unix seconds)
  generic      Auto-detect any OHLCV CSV (column names or positional)

Spreads: the four two-sided tick formats carry a bid and an ask per tick, so
their ticks (--ticks) and the bars made from them (--tf) keep the quoted
spread. An MT4/MT5 bar CSV with a <SPREAD> column and an MT4 .hst file of
version 401 carry one spread per bar, which is kept too. The other formats carry
no quotes: their bars are written with bid = ask = close. A Dukascopy web export
holds one side of the quote: its bars and ticks are written with bid = ask.

CLI usage:
  python dataconvert.py INPUT OUTPUT --symbol SYM --tf MINUTES [--format FMT] [--tz ZONE]
  python dataconvert.py ticks.csv out.bin --symbol DEU.IDX-EUR --tf 5
  python dataconvert.py bars.csv out.bin --symbol USATECH.IDX-USD --tf 1 --format mt4 --tz Europe/Helsinki
  python dataconvert.py history.hst out.bin --symbol EURUSD --tf 5 --format mt4hst --tz Europe/Helsinki
  python dataconvert.py BTCUSDT-1h-2024-01.csv out.bin --symbol BTCUSDT --tf 60 --format binance

  --tz is required for the formats that store a local clock (mt4, mt5, mt4hst,
  mt5ticks, ninjatrader, ninjatraderticks, tradestation, generic, genericticks).

Utility commands:
  python dataconvert.py --info FILE.bin       Show header info of a .bin file
  python dataconvert.py --formats             List all supported input formats

GUI (no arguments, or --gui):
  python dataconvert.py
  python dataconvert.py --gui

Memory: a tick file (dukascopy, a dukascopyweb tick export, mt5ticks,
ninjatraderticks, genericticks) converts in at most about 200 MB of memory,
whatever its size; it needs free disk space beside the output, about 32 bytes
per tick (twice that while ticks that are not in time order are sorted). A
bar file is held in memory while it converts, 490 to 1,180 MB per million rows
depending on its format. Before converting, the tool says
what the file needs and stops with a message when that is more than the
computer has available (--ignore-memory-check converts anyway).

One input file per conversion: a history kept in many files (one per hour or
day) is joined into one file first, the files in time order and the header
line once.
"""

import argparse
import base64
import csv
import heapq
import io
import math
import os
import re
import secrets
import struct
import sys
import threading
import zipfile
from operator import itemgetter
from datetime import datetime, timedelta, timezone, tzinfo
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, Iterator, List, NamedTuple, Optional, Tuple
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

# =============================================================================
# Binary format constants - must match bar_binary.h
# =============================================================================
MAGIC_V1 = 0x42415235  # "BAR5"
MAGIC_V2 = 0x42415236  # "BAR6"
FORMAT_VERSION = 2
# payload_type: bit 0 = 0 bars, 1 ticks; bit 1 = the volume field holds the
# source's volume. A file without bit 1 holds 0.0 in every volume field.
PAYLOAD_BARS  = 0
PAYLOAD_TICKS = 1
PAYLOAD_VOLUME_BIT = 2

# FileHeaderV2: magic(4u) version(4u) payload_type(1u) timeframe_min(4u)
#               record_count(4u) start_ts(8i) end_ts(8i) symbol[16] = 49 bytes
HDR_V2_FMT = "<IIBIIqq16s"
HDR_V2_SIZE = struct.calcsize(HDR_V2_FMT)  # 49

# BinaryBarM5: timestamp(8i) open high low close bid ask volume(8f each) = 64 bytes
BAR_FMT  = "<qddddddd"
BAR_SIZE = struct.calcsize(BAR_FMT)  # 64

# A bar record of the earlier layout (before the volume field): 56 bytes.
# Files of it are not read; --info names them.
EARLIER_BAR_SIZE = 56

# TickRecord: timestamp(8i) bid(8f) ask(8f) volume(8f) = 32 bytes  (payload_type=1)
# Matches BinaryFormat::TickRecord in bar_binary.h
TICK_FMT  = "<qddd"
TICK_SIZE = struct.calcsize(TICK_FMT)  # 32

assert HDR_V2_SIZE == 49, f"Header size mismatch: {HDR_V2_SIZE}"
assert BAR_SIZE    == 64, f"Bar size mismatch: {BAR_SIZE}"
assert TICK_SIZE   == 32, f"Tick size mismatch: {TICK_SIZE}"

# {ts: int, open: float, high: float, low: float, close: float, bid: float, ask: float}
# and "volume": float when the source has a traded-volume column (see "Volume").
Bar = Dict[str, Any]
ProgressFn = Optional[Callable[[str], None]]

TIMEFRAMES = [1, 2, 3, 4, 5, 10, 15, 30, 60, 120, 240, 1440]

# A record time outside this window is a corrupt field, not history: 1990-01-01
# and 2100-01-01 in epoch milliseconds. Partiqon's upload holds a file to the
# same window, so a file this tool writes is one it accepts.
PLAUSIBLE_TS_MIN_MS = 631_152_000_000
PLAUSIBLE_TS_MAX_MS = 4_102_444_800_000

# No instrument trades at a billion: a price above this is a corrupt field.
# Partiqon's upload and engine hold a file to the same ceiling.
MAX_PRICE = 1_000_000_000.0
# The largest volume a record may hold (shares, contracts, crypto base units,
# FX units all fit); Partiqon's upload and backtest apply the same ceiling.
MAX_VOLUME = 1e15

# =============================================================================
# Timestamp parsers
# =============================================================================
_RE_DUKA = re.compile(
    r"^(\d{2})\.(\d{2})\.(\d{4}) (\d{2}):(\d{2}):(\d{2})\.(\d+)"
    r"(?: UTC([+-]\d{2}):(\d{2}))?$"
)
_RE_MT   = re.compile(r"^(\d{4})[.\-/](\d{2})[.\-/](\d{2})$")
_RE_US   = re.compile(r"^(\d{1,2})[/\-](\d{1,2})[/\-](\d{4})$")
_RE_NT   = re.compile(r"^(\d{8}) (\d{6})(?:[.,](\d+))?$")
# A NinjaTrader daily bar's date alone: "20240102" (yyyymmdd, no time).
_RE_NT_DATE = re.compile(r"^\d{8}$")

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
    # Dukascopy's web export writes every stamp with its UTC offset.
    "dukascopyweb": "in_data",
    # MT5 stamps ticks with the broker's server clock, NinjaTrader with the
    # display zone set in the platform; a generic tick CSV says nothing.
    "mt5ticks":         "wall",
    "ninjatraderticks": "wall",
    "genericticks":     "wall",
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


class RecordsInvalid(ValueError):
    """Raised when records of the payload cannot be market data, so nothing is written.

    A bar whose high is below its low, whose open or close lies outside that
    range, a price of zero or less, a value that is not a number and a time
    that is not a plausible date are all things a file cannot contain and still
    be used: an engine reading it computes on impossible prices. The check is
    the same one Partiqon's upload makes of every record, made here so the
    source is corrected now rather than when the file is uploaded.

    Attributes:
      offenders  list of dicts {"where", "ts", "reason", "values"}; `where` is
                 "row N" (a line of the source file) or "bar N" / "tick N" (the
                 payload's own position, for a payload built from many rows).
                 Only the first few are kept.
      bad        how many records were refused.
      counts     the conversion counts, attached by `convert()`.
    """

    def __init__(self, message, offenders=None, bad=0, counts=None):
        super().__init__(message)
        self.offenders = list(offenders or ())
        self.bad = bad
        self.counts = counts


# ── Zone names on a Python without a time zone database ──────────────────────
#
# Python reads a zone name from the operating system's IANA time zone database
# or from the tzdata package. Windows has neither unless tzdata is installed,
# and there `--tz Europe/Berlin` used to be refused. DataConvert therefore
# carries the rules of the zones below: the IANA database's own files, byte for
# byte (version ZONES_VERSION; the database is in the public domain), stored at
# the end of this file. They are used only when Python has no database of its
# own; one that Python has is used first, because it may be newer. Any other
# zone name on such a Python is refused with a message saying how to install
# the database.

_CARRIED: Dict[str, tzinfo] = {}


def carried_zone(name: str) -> Optional[tzinfo]:
    """The zone `name` from the rules DataConvert carries, or None when it
    carries no such zone."""
    if name not in _CARRIED:
        data = _ZONES.get(name)
        if data is None:
            return None
        _CARRIED[name] = ZoneInfo.from_file(io.BytesIO(base64.b64decode("".join(data))),
                                            key=name)
    return _CARRIED[name]


def is_carried(tz: Optional[tzinfo]) -> bool:
    """True when `tz` came from the rules DataConvert carries."""
    return any(tz is z for z in _CARRIED.values())


def has_zone_database() -> bool:
    """True when this Python can read zone names by itself (the operating
    system's database, or the tzdata package)."""
    try:
        ZoneInfo("UTC")
        return True
    except ZoneInfoNotFoundError:
        return False


def _unknown_zone(spec: str, why: str) -> ValueError:
    if not has_zone_database():
        return ValueError(
            f"Unknown timezone {spec!r}: this Python has no time zone database "
            f"(Windows ships none), and DataConvert carries the rules of these "
            f"zones only:\n  {', '.join(sorted(_ZONES))}\n"
            f"To use any other zone name, install the database once:\n"
            f"  python -m pip install tzdata\n"
            f"or give a fixed offset that never observes DST, such as +05:30.")
    return ValueError(
        f"Unknown timezone {spec!r}: {why}\n"
        f"Give an IANA zone name or a fixed offset. Examples:\n{_TZ_EXAMPLES}")


def resolve_tz(spec: Optional[str]) -> Optional[tzinfo]:
    """Turn a --tz string into a tzinfo. None stays None.

    Accepts an IANA zone name ("Europe/Helsinki"), which is what makes DST
    handling correct across a transition, or a fixed offset ("+02:00", "-5",
    "UTC+02:00") for sources that genuinely never shift.

    A zone name is read from Python's own time zone database when it has one,
    else from the rules DataConvert carries (see carried_zone).
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
    except ZoneInfoNotFoundError as exc:
        carried = carried_zone(spec)
        if carried is not None:
            return carried
        raise _unknown_zone(spec, str(exc)) from exc
    except ValueError as exc:            # not a zone name at all ("../x")
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

    TICKS (`repeats=True`)
    ----------------------
    Many ticks share a millisecond, so for a tick source "this label was seen
    before" says nothing about which copy of the hour a row is in. There the
    repeated copy starts where the clock steps BACK inside the ambiguous hour,
    by at least `TICK_STEP_BACK`: the first copy runs forward, and the second
    starts again at the top of the hour. A step back shorter than that is
    read as the source's own jitter, not as the clocks going back. Every row
    after the step, up to the end of the ambiguous hour, is in the repeated
    copy.
    """

    #: The smallest step back in a tick source's wall clock that is read as
    #: the start of the repeated hour (see TICKS above).
    TICK_STEP_BACK = timedelta(seconds=1)

    def __init__(self, tz: Optional[tzinfo] = None, repeats: bool = False):
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
        # Tick sources: the previous label inside the ambiguous hour, and
        # whether the clock has stepped back into the repeated copy.
        self._repeats = repeats
        self._previous: Optional[datetime] = None
        self._second_copy = False

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
            self._previous = None
            self._second_copy = False
            return int(dt_naive.replace(tzinfo=tz).timestamp() * 1000)

        if off0 < off1:
            self.nonexistent.append((self.rows, dt_naive))
            # Still returns a number: the refusal is raised once, by the parser,
            # with every offending row named. Raising from here would land in
            # the per-row `except (ValueError, IndexError)` every parser has and
            # be reported as an unreadable row instead.
            return int(dt_naive.replace(tzinfo=tz, fold=0).timestamp() * 1000)

        if self._repeats:
            previous, self._previous = self._previous, dt_naive
            if (not self._second_copy and previous is not None
                    and previous - dt_naive >= self.TICK_STEP_BACK):
                self._second_copy = True
            if self._second_copy:
                self.repeated_hour_rows += 1
                return int(dt_naive.replace(tzinfo=tz, fold=1).timestamp() * 1000)
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


# ISO 8601 as exporters write it, read field by field. datetime.fromisoformat
# reads the basic format ("20240102T070000"), offsets without a colon ("+0100",
# "+01") and fractions of other than 3 or 6 digits only from Python 3.11 on,
# and DataConvert runs on 3.9 and 3.10 too. The date and the time each use one
# form, extended or basic, as 3.11+ requires; a fraction keeps microseconds.
_RE_ISO_DATE = re.compile(r"(\d{4})(-?)(\d{2})\2(\d{2})")
_RE_ISO_TIME = re.compile(r"(\d{2})(?:(:?)(\d{2})(?:\2(\d{2})(?:[.,](\d+))?)?)?"
                          r"(?:([+-])(\d{2})(?::?(\d{2}))?)?")


def _iso_datetime(text: str) -> datetime:
    """`text` read the way datetime.fromisoformat reads it on Python 3.11+.

    The spellings above are parsed here, so every Python version gives the
    same value; anything else (week dates, other separators, offsets with
    seconds) is left to datetime.fromisoformat, which raises ValueError where
    it cannot read it.
    """
    d = _RE_ISO_DATE.match(text)
    rest = text[d.end():] if d else ""
    t = (_RE_ISO_TIME.fullmatch(rest[1:])
         if d and len(rest) > 1 and rest[0] in "Tt " else None)
    if d and (rest == "" or t):
        hh, _, mi, ss, frac, sign, oh, om = t.groups() if t else (None,) * 8
        try:
            tzi = None
            if sign:
                offset = timedelta(hours=int(oh), minutes=int(om or 0))
                tzi = timezone(-offset if sign == "-" else offset)
            return datetime(int(d.group(1)), int(d.group(3)), int(d.group(4)),
                            int(hh or 0), int(mi or 0), int(ss or 0),
                            int((frac or "")[:6].ljust(6, "0")), tzinfo=tzi)
        except ValueError:
            pass        # no such date or time: fromisoformat gives its own answer
    return datetime.fromisoformat(text)


def _seconds(field: str, fraction: bool) -> Tuple[int, int]:
    """(seconds, microseconds) of a time's seconds field ("12", "12.108").

    Without `fraction` the fraction is dropped, as bar exports have always been
    read; with it (tick sources) the digits after the point are kept, up to
    six."""
    if not fraction:
        return int(float(field)), 0
    whole, _, frac = field.partition(".")
    if frac and not frac.isdigit():
        raise ValueError(f"not a seconds field: {field!r}")
    return int(whole), int(frac[:6].ljust(6, "0")) if frac else 0


def parse_ts(date_str: str, time_str: str = "", tz: Optional[tzinfo] = None,
             clock: Optional[SourceClock] = None, fraction: bool = False) -> int:
    """Parse various date/time strings -> Unix milliseconds. Returns 0 on failure.

    `tz` is the source timezone for the branches that read a bare wall clock.
    It is deliberately ignored by the branches whose input already fixes an
    instant (Dukascopy's own offset suffix, Unix epochs).

    `clock` is the file's `SourceClock`. Given one, the wall-clock branches
    resolve a repeated DST hour by the position of the row within the file;
    without one they keep the documented `fold=0` reading, which is the only
    thing a single call can decide.

    `fraction` keeps a fraction of a second ("08:00:12.108") in the MT and US
    date branches, which tick sources need; bar sources leave it off and read
    whole seconds, as before. The other branches keep a fraction either way.
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
    # Matched against the date and the time TOGETHER: the parsers split the
    # stamp ("20200102", "070500") or read it from two columns, so matching
    # the date alone never hit, and the layout was read only by the ISO
    # fallback below, whose datetime.fromisoformat reads this basic format
    # from Python 3.11 on. On Python 3.10 every row was skipped. Parsed here
    # field by field, so every Python version gives the same value. A
    # fraction of a second keeps the value 3.11+ gave: microseconds, digits
    # beyond six cut.
    m = _RE_NT.match(s + " " + t if t else s)
    if m:
        d, ti, frac = m.groups()
        return _wall(
            datetime(int(d[:4]), int(d[4:6]), int(d[6:8]),
                     int(ti[:2]), int(ti[2:4]), int(ti[4:6]),
                     int((frac or "")[:6].ljust(6, "0"))), tz, clock)

    # MT4/MT5/ISO date: "2020.01.02" or "2020-01-02" with optional separate time
    # — wall clock in the broker's server timezone, needs `tz`.
    m = _RE_MT.match(s)
    if m:
        yyyy, mm, dd = m.groups()
        tp = t.split(":") if t else ["0", "0", "0"]
        hh = int(tp[0]) if len(tp) > 0 else 0
        mi = int(tp[1]) if len(tp) > 1 else 0
        ss, us = _seconds(tp[2], fraction) if len(tp) > 2 else (0, 0)
        return _wall(
            datetime(int(yyyy), int(mm), int(dd), hh, mi, ss, us), tz, clock)

    # US date: "01/02/2020" or "01-02-2020" — wall clock, needs `tz`.
    m = _RE_US.match(s)
    if m:
        mo, dd, yyyy = m.groups()
        tp = t.split(":") if t else ["0", "0", "0"]
        hh = int(tp[0]) if len(tp) > 0 else 0
        mi = int(tp[1]) if len(tp) > 1 else 0
        ss, us = _seconds(tp[2], fraction) if len(tp) > 2 else (0, 0)
        return _wall(
            datetime(int(yyyy), int(mo), int(dd), hh, mi, ss, us), tz, clock)

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
        dt = _iso_datetime(full.rstrip("Z"))
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
    raise _unordered_refusal(path, total, what, allow_repeats, offenders,
                             descents, len(offenders) - descents, limit)


def _unordered_refusal(path: str, total: int, what: str, allow_repeats: bool,
                       offenders: List[tuple], descents: int, repeats: int,
                       limit: int = 5) -> "RecordsNotInOrder":
    """The RecordsNotInOrder for `total` records of which `descents` go back
    in time and `repeats` repeat the time before them; `offenders` holds at
    least the first `limit` of them, in order. Shared by the check above and
    by the streamed tick payload, which keeps only the first ones."""
    lines = []
    for kind, i, ts_i, j, ts_j in offenders[:limit]:
        if kind == "descent":
            lines.append("  record %d at %s is followed by record %d at %s, "
                         "which is earlier" % (i + 1, ts_to_str_exact(ts_i),
                                               j + 1, ts_to_str_exact(ts_j)))
        else:
            lines.append("  record %d and record %d are both stamped %s"
                         % (i + 1, j + 1, ts_to_str_exact(ts_i)))
    if descents + repeats > limit:
        lines.append("  ... and %d more" % (descents + repeats - limit))

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
    return RecordsNotInOrder(
        "Refusing to write %s: %d %s records are not %s.\n"
        "%s.\n%s\n%s\n%s" % (path, total, what, order,
                             " and ".join(found), "\n".join(lines),
                             positions, why),
        offenders=offenders)


# =============================================================================
# Content checks
# =============================================================================
#
# What a record must be to be market data at all. Partiqon's upload checks every
# record of a file against exactly these rules, so a file that passes here is not
# refused there. Two writers below and the row audit in every parser use them.
#
#   a time inside PLAUSIBLE_TS_MIN_MS .. PLAUSIBLE_TS_MAX_MS
#   every price (bar: open, high, low, close, bid, ask; tick: bid, ask) a number
#   every price above zero, and not above MAX_PRICE
#   bar: high not below low, open and close inside [low, high]
#
# One reason per record, the first that applies, in that order. Quotes that are
# possible but poor -- a bid above its ask, gaps, spikes -- are not refused.

_BAR_PRICES = ("open", "high", "low", "close", "bid", "ask")
_TICK_PRICES = ("bid", "ask")

#: How many offending records a refusal names before it says "and N more".
REFUSAL_NAMES = 5


def record_problem(rec: Dict, tick: bool = False) -> Optional[str]:
    """Why `rec` (a bar dict, or a tick dict when `tick`) cannot be market data,
    or None when it can. A record carrying a "volume" is held to the volume
    rules too: a number, not negative, at most MAX_VOLUME."""
    ts = rec["ts"]
    if not (PLAUSIBLE_TS_MIN_MS <= ts <= PLAUSIBLE_TS_MAX_MS):
        return "its time is not a plausible date"
    names = _TICK_PRICES if tick else _BAR_PRICES
    for n in names + (("volume",) if "volume" in rec else ()):
        if not math.isfinite(rec[n]):
            return "it holds a value that is not a number"
    for n in names:
        if not rec[n] > 0.0:
            return "it holds a price of zero or less"
    for n in names:
        if rec[n] > MAX_PRICE:
            return "it holds a price above 1,000,000,000"
    if not tick:
        lo, hi = rec["low"], rec["high"]
        if hi < lo:
            return "its high is below its low"
        if rec["open"] < lo or rec["open"] > hi:
            return "it opens outside its own high and low"
        if rec["close"] < lo or rec["close"] > hi:
            return "it closes outside its own high and low"
    if "volume" in rec:
        if rec["volume"] < 0.0:
            return "its volume is negative"
        if rec["volume"] > MAX_VOLUME:
            return "it holds a volume above 1,000,000,000,000,000"
    return None


def _when_safe(ts) -> str:
    """A record's time for a message; a time no calendar holds is shown as the raw number."""
    try:
        return ts_to_str_exact(int(ts))
    except (OverflowError, OSError, ValueError):
        return "%s ms" % (ts,)


def _values_of(rec: Dict, tick: bool) -> str:
    names = (_TICK_PRICES if tick else ("open", "high", "low", "close")) + (
        ("volume",) if "volume" in rec else ())
    return ", ".join("%s %r" % (n, rec[n]) for n in names)


class _RowAudit:
    """The first few source rows a parser found that cannot be market data, and
    how many there were. O(1) memory: a tick export has tens of millions of rows."""

    def __init__(self, keep: int = REFUSAL_NAMES):
        self.keep = keep
        self.bad = 0
        self.first: List[Dict] = []

    def note(self, where: str, rec: Dict, reason: str, tick: bool) -> None:
        self.bad += 1
        if len(self.first) < self.keep:
            self.first.append({"where": where, "ts": rec["ts"], "reason": reason,
                               "values": _values_of(rec, tick)})


class _Counts(dict):
    """new_conversion_counts()'s dict: the counts, plus the row audit the parsers
    feed. Unpacking or copying it gives the counts alone."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.audit = _RowAudit()
        # What the parsers note about the SOURCE (not counts, so not keys):
        # whether its rows were ticks (counts_summary words duplicates by it),
        # and the quote side of a one-sided file.
        self.tick_source = False
        self.quote_side: Optional[str] = None
        # Whether this conversion writes the source's volume at all (see
        # "Volume"): when not, the parsers' records are checked and carried
        # without it, so a volume column never refuses a file written without.
        self.read_volume = True


def _audit(stats, rec: Dict, row: int, tick: bool = False, unit: str = "row") -> None:
    """Check the record a parser has just built from source row `row` (a
    conversion's counts carry the audit; a parser called without them is not
    audited, and the writers still check the payload)."""
    audit = getattr(stats, "audit", None)
    if audit is None:
        return
    if not getattr(stats, "read_volume", True):
        rec.pop("volume", None)
    reason = record_problem(rec, tick)
    if reason is not None:
        audit.note("%s %d" % (unit, row), rec, reason, tick)


def _refusal_lines(entries: List[Dict], bad: int) -> str:
    lines = ["  %s (%s): %s  [%s]" % (e["where"], _when_safe(e["ts"]), e["reason"], e["values"])
             for e in entries]
    if bad > len(entries):
        lines.append("  ... and %d more" % (bad - len(entries)))
    return "\n".join(lines)


def _refuse_audited(stats, path: str) -> None:
    """Raise RecordsInvalid, naming source rows, when the parsers found any that
    cannot be market data. Called after parsing and BEFORE the output file is
    created."""
    audit = getattr(stats, "audit", None)
    if audit is None or not audit.bad:
        return
    total = stats.get("rows_parsed", 0)
    raise RecordsInvalid(
        "Refusing to write %s: %d of %d source rows cannot be market data, and "
        "Partiqon's upload would refuse a file holding them.\n%s\n"
        "Row numbers are lines of the source file, the header included.\n"
        "Correct these rows in the source and convert it again. Nothing was written."
        % (path, audit.bad, total, _refusal_lines(audit.first, audit.bad)),
        offenders=audit.first, bad=audit.bad)


def _refuse_invalid(records: List[Dict], path: str, what: str) -> None:
    """Raise RecordsInvalid unless every record of the payload can be market
    data. Writes nothing either way; called BEFORE the output file is created,
    like _refuse_unordered. Names the payload's own positions -- for a payload
    built from many source rows (ticks aggregated into bars) there is no single
    source row to name."""
    tick = what == "tick"
    entries: List[Dict] = []
    bad = 0
    for i, rec in enumerate(records):
        reason = record_problem(rec, tick)
        if reason is None:
            continue
        bad += 1
        if len(entries) < REFUSAL_NAMES:
            entries.append(_invalid_entry(rec, i + 1, what, reason))
    if not bad:
        return
    raise _invalid_refusal(path, len(records), what, entries, bad)


def _invalid_entry(rec: Dict, position: int, what: str, reason: str) -> Dict:
    """How a refusal names record `position` (1-based) of a payload of `what`s."""
    return {"where": "%s %d" % (what, position), "ts": rec["ts"], "reason": reason,
            "values": _values_of(rec, what == "tick")}


def _invalid_refusal(path: str, total: int, what: str, entries: List[Dict],
                     bad: int) -> "RecordsInvalid":
    """The RecordsInvalid for `bad` of `total` records that cannot be market
    data, the first of them in `entries`. Shared by the check above and by
    the streamed payloads."""
    return RecordsInvalid(
        "Refusing to write %s: %d of %d %ss cannot be market data, and Partiqon's "
        "upload would refuse a file holding them.\n%s\n"
        "Positions are in the payload that would have been written (sorted by "
        "time; bars made from ticks hold many source rows), not lines of the "
        "source file.\nCorrect the source data and convert it again. Nothing was "
        "written." % (path, bad, total, what, _refusal_lines(entries, bad)),
        offenders=entries, bad=bad)


def write_v2(path: str, bars: List[Bar], symbol: str, tf_min: int,
             with_volume: bool = False) -> int:
    """Write bars list to a BAR6 .bin file. Returns total bytes written.

    with_volume: the file declares its volume (bit 1 of payload_type) and every
    bar's "volume" is written; without it every volume field is 0.0 (see
    "Volume" for which sources are written with it).

    Refuses a payload that does not ascend strictly. A bar source is
    sorted by every parser here, but nothing de-duplicates it, so two CSV rows
    stamped the same minute used to become two records in the file. Refuses a
    payload with a bar that cannot be market data (see the content checks).
    """
    if not bars:
        raise ValueError("No bars to write")
    _refuse_invalid(bars, path, "bar")
    _refuse_unordered(bars, path, "bar", allow_repeats=False)
    first_ts, last_ts = _payload_span(bars)
    sym_b = symbol.encode("ascii", errors="replace")[:16].ljust(16, b"\x00")
    hdr = struct.pack(HDR_V2_FMT,
                      MAGIC_V2, FORMAT_VERSION,
                      PAYLOAD_BARS | (PAYLOAD_VOLUME_BIT if with_volume else 0),
                      tf_min, len(bars),
                      first_ts, last_ts,
                      sym_b)
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    with open(path, "wb") as f:
        f.write(hdr)
        for b in bars:
            # A file that declares no volume holds 0.0 in the volume field.
            f.write(struct.pack(BAR_FMT,
                                b["ts"], b["open"], b["high"],
                                b["low"], b["close"], b["bid"], b["ask"],
                                b["volume"] if with_volume else 0.0))
    return HDR_V2_SIZE + len(bars) * BAR_SIZE


def write_ticks_v2(path: str, ticks: List[Dict], symbol: str,
                   with_volume: bool = False) -> int:
    """Write raw ticks to a BAR6 file with payload_type=1 (32-byte TickRecords).

    Header: timeframe_min=0 (tick), payload_type=1 (3 with_volume).
    Record: ts(int64) bid(double) ask(double) volume(double): the tick's
    "volume" with_volume, else 0.0.

    Refuses a payload whose timestamps descend. `parse_dukascopy_ticks`
    returns arrival order and has never sorted, so a shuffled or concatenated
    export used to be written out of order, with a header whose start was later
    than its end. Repeated timestamps are still accepted: many ticks share a
    millisecond. Refuses a payload with a tick that cannot be market data.
    """
    if not ticks:
        raise ValueError("No ticks to write")
    _refuse_invalid(ticks, path, "tick")
    _refuse_unordered(ticks, path, "tick", allow_repeats=True)
    first_ts, last_ts = _payload_span(ticks)
    sym_b = symbol.encode("ascii", errors="replace")[:16].ljust(16, b"\x00")
    hdr = struct.pack(HDR_V2_FMT,
                      MAGIC_V2, FORMAT_VERSION,
                      PAYLOAD_TICKS | (PAYLOAD_VOLUME_BIT if with_volume else 0),
                      0,           # timeframe_min = 0 for ticks
                      len(ticks),
                      first_ts, last_ts,
                      sym_b)
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    with open(path, "wb") as f:
        f.write(hdr)
        for tk in ticks:
            f.write(struct.pack(TICK_FMT, tk["ts"], tk["bid"], tk["ask"],
                                tk["volume"] if with_volume else 0.0))
    return HDR_V2_SIZE + len(ticks) * TICK_SIZE


# =============================================================================
# Volume
# =============================================================================
#
# A parser puts "volume" on its records when its source has a column of traded
# volume (each parser names its column). A file is written with it -- bit 1 of
# payload_type set, the values in the records' volume field -- when every
# record has one and one of them is above 0. Otherwise the file declares no
# volume and every volume field is 0.0, and the conversion says why:
#
#   quoted   Dukascopy's Ask Volume / Bid Volume / Volume are quoted sizes,
#            never traded volume; never written
#   off      written without volume, as asked (--no-volume)
#   none     the source has no volume column
#   zero     the column holds only 0 (an FX broker's <VOL>, quote ticks)
#   source   written from the source's volume column
#
# A volume that is not a number, negative or above MAX_VOLUME refuses the file
# and names the source row (record_problem); an empty cell is not a number.
# Never read as volume: MT4/MT5 <TICKVOL> and a .hst v400 record's volume (the
# number of price changes), a column not headed exactly "volume" or "vol"
# (trade_count, vol_lots, Ask Volume), TradeStation's Up and Down.

#: Formats whose size columns are quoted sizes: never written as volume.
QUOTED_SIZE_FORMATS = ("dukascopy", "dukascopyweb")

#: What a conversion says about the volume it wrote.
VOLUME_SAYS = {
    "source": "Volume: written from the source's volume column.",
    "zero":   "Volume: not written. The volume column holds only 0.",
    "quoted": "Volume: not written. The size columns of a Dukascopy export are quoted "
              "sizes, not traded volume.",
    "none":   "Volume: the source has no volume column; the file is written without volume.",
    "off":    "Volume: not written, as asked.",
}


def volume_of(records: List[Dict], fmt: str, no_volume: bool = False) -> str:
    """Which volume `records` are written with (a key of VOLUME_SAYS). A
    payload written without volume loses the records' "volume". A record
    without one among records that have it gets a volume that is not a
    number, so the writer refuses the payload and names it."""
    carrying, nonzero = 0, False
    if not (fmt in QUOTED_SIZE_FORMATS or no_volume):
        carrying = sum(1 for r in records if "volume" in r)
        nonzero = carrying == len(records) and any(r["volume"] != 0.0 for r in records)
    state = _volume_state(fmt, no_volume, carrying, len(records), nonzero)
    if state == "mixed":
        for r in records:
            r.setdefault("volume", math.nan)
        return "source"
    if state != "source":
        for r in records:
            r.pop("volume", None)
    return state


def _volume_state(fmt: str, no_volume: bool, carrying: int, total: int,
                  nonzero: bool) -> str:
    """volume_of's rule for `total` records of which `carrying` have a
    volume, `nonzero` when one of them is not 0: a key of VOLUME_SAYS, or
    "mixed" when only some records carry one (they are written as "source",
    and the records without a volume refuse the payload)."""
    if fmt in QUOTED_SIZE_FORMATS:
        return "quoted"
    if no_volume:
        return "off"
    if carrying == 0:
        return "none"
    if carrying < total:
        return "mixed"
    return "source" if nonzero else "zero"


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
    return _Counts({
        "rows_parsed": 0,
        "skipped_rows": 0,
        "out_of_order_rows": 0,
        "duplicate_timestamps": 0,
        "dst_repeated_rows": 0,
    })


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


def counts_summary(stats: Optional[Dict[str, int]], tick_payload: bool = False,
                   tick_source: Optional[bool] = None) -> str:
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
      duplicates     bar path from bar rows: refused, for the same reason.
                     bar path from TICKS (`tick_source`): ticks
                     sharing a time are aggregated into their bar like every
                     other tick; nothing is refused, and the line says so.
                     raw tick path: kept, because many ticks genuinely share a
                     millisecond and collapsing them would delete real data.
      repeated DST   resolved, on both paths -- the second copy of an
                     ambiguous local hour is the repeated one. Named here for
                     the same reason as the zeros: a file whose hour WAS moved
                     an hour later than its label reads should say so.

    `.get()` on the last count, not `[...]`: a stats dict built by an older
    version -- a caller holding one across an upgrade, or a refusal's attached copy --
    has four keys, and this line must describe it rather than raise on it.

    `tick_source` defaults to what the parsers noted on `stats` (the
    `tick_source` attribute _mark_tick_source sets); a plain dict says nothing,
    and its rows are read as bar rows.
    """
    if not stats:
        return ""
    if tick_source is None:
        tick_source = bool(getattr(stats, "tick_source", False))
    order_fate = "refused" if tick_payload else "sorted"
    if tick_payload:
        dupe_fate = "kept -- ticks may share a millisecond"
    elif tick_source:
        dupe_fate = "ticks aggregated into their bar"
    else:
        dupe_fate = "refused"
    return (f"{stats['rows_parsed']:,} rows parsed  |  "
            f"{stats['skipped_rows']:,} unreadable rows skipped  |  "
            f"{stats['out_of_order_rows']:,} rows out of order ({order_fate})  |  "
            f"{stats['duplicate_timestamps']:,} duplicate timestamps ({dupe_fate})"
            f"  |  {stats.get('dst_repeated_rows', 0):,} rows in a repeated DST "
            f"hour (resolved by row order)")


# =============================================================================
# Tick aggregator
# =============================================================================
def _in_time_order(ticks: List[Dict]) -> List[Dict]:
    """`ticks` in time order. Ticks sharing a millisecond keep their file order
    (Python's sort is stable), because nothing else says which came first. A
    list that already ascends is returned as it is, without a copy."""
    prev = None
    for tick in ticks:
        ts = tick["ts"]
        if prev is not None and ts < prev:
            return sorted(ticks, key=lambda t: t["ts"])
        prev = ts
    return ticks


def aggregate_ticks(ticks: List[Dict], tf_min: int) -> List[Bar]:
    """(ts_ms, bid, ask) ticks -> bars, one per `tf_min` minutes on the UTC grid.

    The rule, the same as the BAR6 format description and the backtest engine's
    own tick loop:

      ts          the bar's open time: tick time rounded down to the interval
      open        mid price, (bid + ask) / 2, of the bar's earliest tick
      high, low   the highest and lowest mid price of the bar's ticks
      close       mid price of the bar's latest tick
      bid, ask    the quote of the bar's latest tick
      volume      the sum of its ticks' volumes, when the ticks carry one

    "Earliest" and "latest" are in TIME order, not in the order the rows sit in
    the source: the ticks are sorted by time first. A position in a file is not
    a time, so a shuffled export gives the same bars as a sorted one. Ticks with
    the same millisecond keep their file order.

    High and low are mid prices because every other price of the bar is one.
    A stop or target is tested against them and the spread is paid once, when
    the order fills, at the bar's bid or ask; a high taken from the ask would
    reach a long's target half a spread before the mid did.
    """
    return list(_bars_in_order(_in_time_order(ticks), tf_min))


def _bars_in_order(ticks: Iterable[Dict], tf_min: int) -> Iterator[Bar]:
    """The bars of `ticks`, which are in time order already (ties in file
    order), one at a time and ascending: the rule of aggregate_ticks.

    In time order a bar's ticks follow one another, so one bar is open at a
    time and is complete when a tick of a later bar arrives. Each bar takes
    its ticks in the order they come, so its open, close, quote and the sum
    of its volumes are those of the time order, wherever the ticks came from;
    a conversion that streams its ticks gives the same bars as one that holds
    them all."""
    interval_ms = tf_min * 60 * 1000
    b: Optional[Bar] = None
    for tick in ticks:
        ts, bid, ask = tick["ts"], tick["bid"], tick["ask"]
        mid = (bid + ask) / 2.0
        bar_ts = (ts // interval_ms) * interval_ms
        if b is None or bar_ts != b["ts"]:
            if b is not None:
                yield b
            b = {"ts": bar_ts, "open": mid, "high": mid,
                 "low": mid, "close": mid, "bid": bid, "ask": ask}
            if "volume" in tick:
                b["volume"] = tick["volume"]
        else:
            if mid > b["high"]:  b["high"] = mid
            if mid < b["low"]:   b["low"]  = mid
            b["close"] = mid
            b["bid"]   = bid
            b["ask"]   = ask
            if "volume" in tick:
                b["volume"] = b.get("volume", 0.0) + tick["volume"]
    if b is not None:
        yield b


# =============================================================================
# Format parsers
# =============================================================================
def _auto_delim(sample: str) -> str:
    return max(",", "\t", ";", key=sample.count)


#: First cells that mark a Dukascopy tick CSV's first row as its header.
#: "etc/utc" heads Dukascopy's web export, which has a parser of its own
#: (parse_dukascopy_web); it is named here too, so that a web export handed to
#: this parser by --format dukascopy has its header never counted as an
#: unreadable row.
_DUKASCOPY_TIME_HEADERS = ("localtime", "local time", "local", "gmt time", "gmt", "utc",
                           "etc/utc", "date", "time", "timestamp")


def _mark_tick_source(stats) -> None:
    """Note on a conversion's counts that its rows were ticks, so that the bar
    path's summary says what became of ticks sharing a time (counts_summary)."""
    if stats is not None:
        try:
            stats.tick_source = True
        except AttributeError:      # a plain dict from a caller: nothing to note
            pass


def parse_dukascopy(path: str, tf_min: int, progress: ProgressFn = None,
                    tz: Optional[tzinfo] = None,
                    stats: Optional[Dict[str, int]] = None) -> List[Bar]:
    """
    Dukascopy tick CSV:
      LocalTime,Ask,Bid,AskVolume,BidVolume
      01.01.2020 00:01:00.288,11348.0,11347.7,1.0,1.0
    Ticks are aggregated into OHLCV bars.
    """
    _mark_tick_source(stats)
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
                          stats: Optional[Dict[str, int]] = None,
                          tz: Optional[tzinfo] = None) -> List[Dict]:
    """Parse Dukascopy tick CSV and return raw tick list [{ts, bid, ask}, ].

    `tz` is accepted so every tick parser takes the same arguments, and never
    used: a Dukascopy time is UTC or carries its own offset (TZ_POLICY).

    The ticks are returned in ARRIVAL order. The raw tick path writes them as
    they are and refuses a file that goes back in time; the bar path sorts them
    by time before it aggregates (see aggregate_ticks). The counts are taken
    either way, so a shuffled tick export is reported.

    A first row whose first cell names the time column is a header, not an
    unreadable row: "LocalTime", "Local time", "Local" and "Gmt time"
    (Dukascopy's own exports and the sample in testcsvs/), "UTC" (the header
    of a tick download stamped in UTC), and the generic "Date", "Time",
    "Timestamp".
    """
    return _collect_ticks(_dukascopy_ticks, path, progress, tz, stats)


class _ParseEnd:
    """What a tick parser knows once its file has ended: the rows it could not
    read, and the file's clock (None for a source that needs none)."""

    __slots__ = ("skipped", "clock")

    def __init__(self) -> None:
        self.skipped = 0
        self.clock: Optional[SourceClock] = None


def _collect_ticks(ticks_of: Callable[..., Iterator[Dict]], path: str,
                   progress: ProgressFn, tz: Optional[tzinfo],
                   stats: Optional[Dict[str, int]]) -> List[Dict]:
    """A tick parser's ticks as one list in arrival order, measured
    (_record): what the list-returning parsers below return. A conversion
    streams the same ticks instead (see "Streaming")."""
    end = _ParseEnd()
    ticks = list(ticks_of(path, progress, tz, stats, end))
    _record(stats, ticks, end.skipped, end.clock)
    return ticks


def _dukascopy_ticks(path: str, progress: ProgressFn, tz: Optional[tzinfo],
                     stats: Optional[Dict[str, int]], end: _ParseEnd) -> Iterator[Dict]:
    """parse_dukascopy_ticks's ticks, one at a time; the skipped rows go to `end`."""
    count = 0
    skipped = 0
    with open(path, newline="", encoding="utf-8", errors="replace") as f:
        reader = csv.reader(f)
        for i, row in enumerate(reader):
            if not row:
                continue
            if i == 0 and row[0].strip().lower() in _DUKASCOPY_TIME_HEADERS:
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
                tick = {"ts": ts, "bid": bid, "ask": ask}
                _audit(stats, tick, reader.line_num, tick=True)
            except ValueError:
                skipped += 1
            else:
                count += 1
                yield tick
            if progress and i % 250_000 == 0 and i:
                progress(f"  Parsed {i:,} ticks")
    end.skipped = skipped
    if progress:
        progress(f"  Loaded {count:,} ticks ({skipped:,} skipped)")


# ── The columns of a bar export, by its header ───────────────────────────────

def _bar_columns(hdrs: List[str]) -> Dict[str, int]:
    """The columns of a bar export by its header (`hdrs` lower-case, stripped):
    the one header lookup of the bar parsers that read one. An exact header
    name beats one that merely contains it: "open" is the price column, not
    "open_time". -1 where the header names no such column.

    "volume" is matched by its exact name only ("volume", "vol"): "Ask Volume"
    and "best_bid_qty" are quoted sizes, not a bar's volume. The volume column
    is read into the bar (key "volume"); which sources' volume is written to
    the file is decided where the file is written."""
    def find(*names) -> int:
        for n in names:
            for ci, h in enumerate(hdrs):
                if n == h:
                    return ci
            for ci, h in enumerate(hdrs):
                if n in h:
                    return ci
        return -1

    def exact(*names) -> int:
        for n in names:
            for ci, h in enumerate(hdrs):
                if n == h:
                    return ci
        return -1

    date_col = find("date", "datetime", "timestamp", "time")
    time_col = find("time")
    return {"date": date_col, "time": -1 if time_col == date_col else time_col,
            "open": find("open"), "high": find("high"), "low": find("low"),
            "close": find("close"), "volume": exact("volume", "vol")}


# ── Dukascopy's web export ───────────────────────────────────────────────────
#
# What a user downloads from Dukascopy's website is not the tick CSV above:
#
#   CNDX.GB-USD_1Minute_ASK_2026-10-28_23_59-23_59_Etc_UTC.csv
#     Etc/UTC,Open,High,Low,Close,Volume
#     2026-10-28T08:00:00+00:00,1713.633,1714.298,1713.403,1713.868,792000
#
# The first header cell is the time zone the stamps were exported in; every
# stamp carries its UTC offset, so no --tz is needed. One file holds ONE side
# of the quote, and only its name says which: "_BID_" or "_ASK_". The same
# header heads a bar file ("_1Minute_", "_1Hour_", "_1Day_") and a tick file
# ("_1Tick_", every row's open, high, low and close one price); only the name
# tells them apart too.
#
# A one-sided file has no spread. Its bars and ticks are written with
# bid = ask, which the backtest reads as "no quote", so a run on them charges
# the spread set for the run; the conversion says so, and which side it read.
# The Volume column is read into each bar and tick (key "volume"). Dukascopy's
# sizes are quoted sizes, not traded volume.

#: The name of a web export: symbol, size and unit of a bar (or "Tick"), side.
_RE_DUKA_WEB_NAME = re.compile(
    r"_(\d+)(tick|ticks|second|seconds|minute|minutes|hour|hours|day|days|week|weeks|"
    r"month|months)_(bid|ask)_", re.IGNORECASE)

_DUKA_WEB_UNIT_MINUTES = {"minute": 1, "minutes": 1, "hour": 60, "hours": 60,
                          "day": 1440, "days": 1440}


def _is_zone_header(cell: str) -> bool:
    """True for the first header cell of a web export: a time zone name
    ("Etc/UTC", "Europe/Berlin") or UTC / GMT."""
    cell = cell.strip().strip('"').lower()
    return cell in ("utc", "gmt") or bool(re.fullmatch(r"[a-z_]+(?:/[a-z0-9_+\-]+)+", cell))


def _is_duka_web_header(cells: List[str]) -> bool:
    """The header row of a web export: a zone, then open, high, low, close
    (and the volume)."""
    hdrs = [c.lstrip("﻿").strip().strip('"').lower() for c in cells]
    return (len(hdrs) >= 5 and _is_zone_header(hdrs[0])
            and hdrs[1:5] == ["open", "high", "low", "close"])


class DukaWebExport(NamedTuple):
    """What a web export's file name says: ticks or bars of how many minutes
    (0 for ticks, None when the name does not say), and the quote side
    ("bid", "ask", or None when the name does not say)."""
    ticks: bool
    bar_minutes: Optional[int]
    side: Optional[str]
    size_text: str


def duka_web_export_name(path: str) -> DukaWebExport:
    """Read a web export's file name. A name without the export's pattern (a
    renamed file) says nothing: bars of the requested timeframe, side unknown."""
    m = _RE_DUKA_WEB_NAME.search(os.path.basename(path))
    if not m:
        return DukaWebExport(False, None, None, "")
    count, unit, side = int(m.group(1)), m.group(2).lower(), m.group(3).lower()
    text = "%d %s" % (count, unit)
    if unit.startswith("tick"):
        return DukaWebExport(True, 0, side, text)
    per = _DUKA_WEB_UNIT_MINUTES.get(unit)
    if per is None:
        # seconds (no BAR6 timeframe is shorter than a minute), weeks, months
        raise ValueError(
            f"{os.path.basename(path)} holds bars of {text}. A bar file holds bars of one "
            f"of {', '.join(str(t) for t in TIMEFRAMES)} minutes; export the data "
            f"from Dukascopy again in one of those sizes, or as ticks.")
    return DukaWebExport(False, count * per, side, text)


class StampWithoutOffset(ValueError):
    """A web export's time without its UTC offset: the conversion stops."""


def default_output_name(input_path: str, symbol: str, tf, as_ticks: bool = False) -> str:
    """The output name the window offers: SYMBOL_M5.bin, SYMBOL_TICKS.bin; for
    a one-sided web export the side as well (SYMBOL_ASK_M1.bin), so that the
    BID and the ASK file of one symbol are not offered one name."""
    try:
        side = duka_web_export_name(input_path).side
    except ValueError:
        side = None
    stem = f"{symbol}_{side.upper()}" if side else symbol
    return f"{stem}_TICKS.bin" if as_ticks else f"{stem}_M{tf}.bin"


def _duka_web_stamp(field: str) -> int:
    """A web export's stamp, which carries its offset ("+00:00") or a "Z":
    Unix milliseconds, or 0 when it cannot be read. A stamp without an offset
    raises StampWithoutOffset: the instant it names is not known."""
    s = field.strip()
    try:
        if s.endswith("Z"):
            return int(_iso_datetime(s[:-1]).replace(tzinfo=timezone.utc).timestamp() * 1000)
        dt = _iso_datetime(s)
    except ValueError:
        return 0
    if dt.tzinfo is None:
        raise StampWithoutOffset(
            f"The time {s!r} carries no UTC offset, so the instant it names is not known. "
            f"Dukascopy's web export writes every time with its offset (for example "
            f"2026-10-28T08:00:00+00:00).")
    return int(dt.timestamp() * 1000)


def _read_duka_web_rows(path: str):
    """The rows of a web export as (line, ts, open, high, low, close, volume),
    and the number of rows that could not be read."""
    end = _ParseEnd()
    rows = list(_duka_web_rows(path, end))
    return rows, end.skipped


def _duka_web_rows(path: str, end: _ParseEnd) -> Iterator[tuple]:
    """_read_duka_web_rows's rows, one at a time; the skipped rows go to `end`."""
    with open(path, newline="", encoding="utf-8-sig", errors="replace") as f:
        reader = csv.reader(f)
        header = None
        skipped = 0
        for raw in reader:
            if not any(c.strip() for c in raw):
                continue
            if header is None:
                if not _is_duka_web_header(raw):
                    raise ValueError(
                        f"{os.path.basename(path)} does not start with the web export's header "
                        f"(a time zone, then Open, High, Low, Close, Volume); it starts with "
                        f"{raw!r}")
                header = _bar_columns([c.strip().strip('"').lower() for c in raw])
                header["date"] = 0
                continue
            try:
                ts = _duka_web_stamp(raw[0])
                vals = [float(raw[header[k]]) for k in ("open", "high", "low", "close")]
                vol_col = header["volume"]
                vol = float(raw[vol_col]) if 0 <= vol_col < len(raw) else None
            except StampWithoutOffset:
                raise
            except (ValueError, IndexError):
                skipped += 1
                continue
            if not ts:
                skipped += 1
                continue
            yield (reader.line_num, ts, vals[0], vals[1], vals[2], vals[3], vol)
    if header is None:
        raise ValueError("File appears empty")
    end.skipped = skipped


def _say_side(progress: ProgressFn, stats, export: DukaWebExport) -> None:
    """The lines a one-sided conversion owes its user: which side it read, and
    that the spread is not in the file."""
    side = export.side.upper() if export.side else None
    if stats is not None:
        try:
            stats.quote_side = export.side
        except AttributeError:
            pass
    if not progress:
        return
    if side:
        progress(f"  Prices: the {side} side only (from the file name). A file of one side "
                 f"holds no spread: the file is written with bid = ask, and a backtest on "
                 f"it charges the spread set for the run.")
    else:
        progress("  Prices: one side of the quote; the file name does not say which (the "
                 "export names it _BID_ or _ASK_). A file of one side holds no spread: the "
                 "file is written with bid = ask, and a backtest on it charges the spread "
                 "set for the run.")


def parse_dukascopy_web_ticks(path: str, progress: ProgressFn = None,
                              tz: Optional[tzinfo] = None,
                              stats: Optional[Dict[str, int]] = None) -> List[Dict]:
    """The ticks of a web export's tick file ("_1Tick_"), in arrival order:
    each row's close as both bid and ask (one side, see above), its volume
    read along. `tz` is accepted like every tick parser's and never used: the
    stamps carry their offset."""
    return _collect_ticks(_duka_web_ticks, path, progress, tz, stats)


def _duka_web_ticks(path: str, progress: ProgressFn, tz: Optional[tzinfo],
                    stats: Optional[Dict[str, int]], end: _ParseEnd) -> Iterator[Dict]:
    """parse_dukascopy_web_ticks's ticks, one at a time."""
    export = duka_web_export_name(path)
    if export.bar_minutes:
        raise ValueError(
            f"{os.path.basename(path)} holds bars of {export.size_text}, not ticks. "
            f"Convert it to bars (without --ticks).")
    _mark_tick_source(stats)
    count = 0
    for line, ts, _o, _h, _l, c, vol in _duka_web_rows(path, end):
        tick = {"ts": ts, "bid": c, "ask": c}
        if vol is not None:
            tick["volume"] = vol
        _audit(stats, tick, line, tick=True)
        count += 1
        yield tick
    if progress:
        progress(f"  Loaded {count:,} web export ticks ({end.skipped:,} skipped)")
    _say_side(progress, stats, export)


def parse_dukascopy_web(path: str, tf_min: int, progress: ProgressFn = None,
                        tz: Optional[tzinfo] = None,
                        stats: Optional[Dict[str, int]] = None) -> List[Bar]:
    """Dukascopy's web export: a bar file read as the bars it holds, a tick
    file ("_1Tick_") aggregated into bars of `tf_min` minutes.

    A bar file whose name gives its bar size is converted to that size only:
    asked for another timeframe it is refused (DataConvert does not resample
    bars). Prices are one side of the quote, written with bid = ask = close."""
    export = duka_web_export_name(path)
    if export.ticks:
        ticks = parse_dukascopy_web_ticks(path, progress, stats=stats)
        if progress:
            progress(f"  Aggregating {len(ticks):,} ticks -> M{tf_min} bars")
        return aggregate_ticks(ticks, tf_min)
    if export.bar_minutes is not None and export.bar_minutes != tf_min:
        raise ValueError(
            f"{os.path.basename(path)} holds bars of {export.size_text} "
            f"(M{export.bar_minutes}), so the timeframe must be {export.bar_minutes}; "
            f"got {tf_min}. DataConvert writes the bars a file holds and does not "
            f"combine them into longer ones.")
    rows, skipped = _read_duka_web_rows(path)
    bars: List[Bar] = []
    for line, ts, o, h, l, c, vol in rows:
        bar = _make_bar(ts, o, h, l, c)
        if vol is not None:
            bar["volume"] = vol
        bars.append(bar)
        _audit(stats, bar, line)
    if progress:
        progress(f"  Loaded {len(bars):,} web export bars ({skipped:,} skipped)")
    _say_side(progress, stats, export)
    return _measure_and_sort(bars, stats, skipped)


# ── Tick exports of other platforms ──────────────────────────────────────────
#
# Every tick parser returns [{ts, bid, ask}] in ARRIVAL order, like the
# Dukascopy one: the raw tick path refuses a file that goes back in time, the
# bar path sorts before it aggregates. The quote of each tick is the export's
# own bid and ask, so the spread a backtest pays on these ticks, and on the bars
# made from them, is the one the source quoted.
#
# A row may carry one side only. MT5 writes a tick row when the bid OR the ask
# changed and leaves the other field empty; the side that did not change is
# still quoted, so it is carried forward from the row before. A row carrying no
# quote at all (an exchange symbol's trade) keeps the current quote. Rows before
# both sides have been quoted once cannot be priced; they are counted as skipped.

def _quote_side(field: str, current: Optional[float]) -> Optional[float]:
    """The new value of one side of the quote: the field when it holds one,
    else the side's current value (an empty field means "unchanged")."""
    field = field.strip()
    return float(field) if field else current


class _QuoteTape:
    """One file's ticks, built row by row with the quote carried forward."""

    def __init__(self, stats):
        self.count = 0
        self.skipped = 0
        self.unpriced = 0         # rows before the first complete quote
        self.bid: Optional[float] = None
        self.ask: Optional[float] = None
        self._stats = stats

    def add(self, ts: int, bid_field: str, ask_field: str, line: int,
            volume: Optional[float] = None) -> Optional[Dict]:
        """One row: `ts` (0 when its time could not be read), its two quote
        fields as written in the file and, when the source has a volume
        column, the row's volume. Returns the row's tick, or None when the
        row gives none."""
        if not ts:
            self.skipped += 1
            return None
        try:
            bid = _quote_side(bid_field, self.bid)
            ask = _quote_side(ask_field, self.ask)
        except ValueError:
            self.skipped += 1
            return None
        self.bid, self.ask = bid, ask
        if bid is None or ask is None:
            self.skipped += 1
            self.unpriced += 1
            return None
        tick = {"ts": ts, "bid": bid, "ask": ask}
        if volume is not None:
            tick["volume"] = volume
        _audit(self._stats, tick, line, tick=True)
        self.count += 1
        return tick

    def finish(self, path: str, what: str, clock: SourceClock,
               progress: ProgressFn, end: _ParseEnd) -> None:
        clock.refuse_nonexistent(path)
        end.skipped, end.clock = self.skipped, clock
        if progress:
            progress(f"  Loaded {self.count:,} {what} ticks "
                     f"({self.skipped:,} skipped)")
            if self.unpriced:
                progress(f"  {self.unpriced:,} of the skipped rows came before the "
                         f"file had quoted both a bid and an ask")


def _header_index(hdrs: List[str], *names: str) -> int:
    """The column whose header is one of `names` exactly, else the first whose
    header has one of them as a WORD ("transaction_time", "Gmt time"), trying
    `names` in order; -1 when none does. A word, not a substring: "update_id"
    holds "date" and is no time."""
    for n in names:
        for ci, h in enumerate(hdrs):
            if h == n:
                return ci
        for ci, h in enumerate(hdrs):
            if n in re.split(r"[^a-z0-9]+", h):
                return ci
    return -1


#: Header words that mark a size, not a price: "Bid Volume", "best_bid_qty".
_NOT_A_PRICE = ("vol", "size", "qty", "quantity", "amount", "count")


def _quote_column(hdrs: List[str], side: str) -> int:
    """The column holding the `side` ("bid" or "ask") price, by its header:
    exactly "bid", else one containing "bid" that does not name a size."""
    for ci, h in enumerate(hdrs):
        if h == side:
            return ci
    for ci, h in enumerate(hdrs):
        if side in h and not any(w in h for w in _NOT_A_PRICE):
            return ci
    return -1


def parse_mt5_ticks(path: str, progress: ProgressFn = None,
                    tz: Optional[tzinfo] = None,
                    stats: Optional[Dict[str, int]] = None) -> List[Dict]:
    """MetaTrader 5 tick export (Symbols, Ticks, Export ticks):

      <DATE>	<TIME>	<BID>	<ASK>	<LAST>	<VOLUME>	<FLAGS>
      2024.01.02	00:00:12.108	1.10427	1.10481			6
      2024.01.02	00:00:14.513	1.10425				2

    Tab separated (a comma or semicolon is read too). The time is the broker's
    server clock with milliseconds, so --tz is required. A row whose BID or ASK
    is empty did not change that side; the side keeps its last value. Columns
    are found by their header names; a file without the header is read in the
    export's order (date, time, bid, ask).
    """
    return _collect_ticks(_mt5_ticks, path, progress, tz, stats)


def _mt5_ticks(path: str, progress: ProgressFn, tz: Optional[tzinfo],
               stats: Optional[Dict[str, int]], end: _ParseEnd) -> Iterator[Dict]:
    """parse_mt5_ticks's ticks, one at a time."""
    tape = _QuoteTape(stats)
    clock = SourceClock(tz, repeats=True)       # ticks: see SourceClock, TICKS
    with open(path, newline="", encoding="utf-8", errors="replace") as f:
        sample = f.read(8192)
        f.seek(0)
        reader = csv.reader(f, delimiter=_auto_delim(sample))
        cols = None
        for raw in reader:
            row = [c.strip().strip("<>\"") for c in raw]
            if not any(row):
                continue
            if cols is None:
                hdrs = [h.upper() for h in row]
                cols = (0, 1, 2, 3, -1)
                if "BID" in hdrs and "ASK" in hdrs:
                    # The traded volume: <VOLUME_REAL> where the export has it,
                    # else <VOLUME>. A quote tick's cell is empty: 0.
                    vol = ("VOLUME_REAL" if "VOLUME_REAL" in hdrs else
                           "VOLUME" if "VOLUME" in hdrs else None)
                    cols = (hdrs.index("DATE") if "DATE" in hdrs else 0,
                            hdrs.index("TIME") if "TIME" in hdrs else -1,
                            hdrs.index("BID"), hdrs.index("ASK"),
                            hdrs.index(vol) if vol else -1)
                    continue
            d, t, b, a, v = cols
            try:
                ts = parse_ts(row[d], row[t] if 0 <= t < len(row) else "", tz, clock,
                              fraction=True)
            except (ValueError, IndexError):
                ts = 0
            try:
                volume = (_volume_cell(row[v] if v < len(row) else "", empty=0.0)
                          if v >= 0 else None)
            except ValueError:
                tape.skipped += 1
                continue
            tick = tape.add(ts, row[b] if b < len(row) else "", row[a] if a < len(row) else "",
                            reader.line_num, volume)
            if tick is not None:
                yield tick
    tape.finish(path, "MT5", clock, progress, end)


#: A NinjaTrader tick stamp: date, time and the fraction in 100 ns steps.
_RE_NT_TICK = re.compile(r"^(\d{8}) (\d{6})(?: (\d{1,7}))?$")


def parse_ninjatrader_ticks(path: str, progress: ProgressFn = None,
                            tz: Optional[tzinfo] = None,
                            stats: Optional[Dict[str, int]] = None) -> List[Dict]:
    """NinjaTrader 8 tick export (Tools, Historical Data, Export, type Tick):

      20240102 070000 1230000;16860.75;16860.50;16861.00;1

    The stamp, then the last price, the bid, the ask and the volume, separated
    by ; (or ,). The fraction is in 100-nanosecond steps (1230000 = 0.123 s).
    The time is the zone set in NinjaTrader, so --tz is required.
    """
    return _collect_ticks(_ninjatrader_ticks, path, progress, tz, stats)


def _ninjatrader_ticks(path: str, progress: ProgressFn, tz: Optional[tzinfo],
                       stats: Optional[Dict[str, int]], end: _ParseEnd) -> Iterator[Dict]:
    """parse_ninjatrader_ticks's ticks, one at a time."""
    tape = _QuoteTape(stats)
    clock = SourceClock(tz, repeats=True)       # ticks: see SourceClock, TICKS
    with open(path, newline="", encoding="utf-8", errors="replace") as f:
        sample = f.read(2048)
        f.seek(0)
        delim = ";" if sample.count(";") >= sample.count(",") else ","
        reader = csv.reader(f, delimiter=delim)
        for i, raw in enumerate(reader):
            row = [c.strip() for c in raw]
            if not any(row) or (i == 0 and not row[0][:1].isdigit()):
                continue
            m = _RE_NT_TICK.match(row[0]) if len(row) >= 4 else None
            ts = 0
            if m:
                d, ti, frac = m.groups()
                try:
                    ts = _wall(datetime(int(d[:4]), int(d[4:6]), int(d[6:8]),
                                        int(ti[:2]), int(ti[2:4]), int(ti[4:6]),
                                        int((frac or "").ljust(7, "0")[:6])),
                               tz, clock)
                except ValueError:
                    ts = 0
            try:      # the volume, the trade's size: the field after the ask
                volume = _volume_cell(row[4]) if m and len(row) > 4 else None
            except ValueError:
                tape.skipped += 1
                continue
            tick = tape.add(ts, row[2] if m else "", row[3] if m else "", reader.line_num, volume)
            if tick is not None:
                yield tick
    tape.finish(path, "NinjaTrader", clock, progress, end)


def parse_generic_ticks(path: str, progress: ProgressFn = None,
                        tz: Optional[tzinfo] = None,
                        stats: Optional[Dict[str, int]] = None) -> List[Dict]:
    """Any tick CSV whose header names a time, a bid and an ask column:

      timestamp,bid,ask
      2024-01-02 13:00:00.250,1.11160,1.11180

    The bid column is the one headed "bid", else the first whose header
    contains "bid" and names no size ("px_bid", "best_bid_price", but not
    "Bid Volume"); the same for the ask. The time is the column headed "date",
    "datetime", "timestamp" or "time" (or with one of them as a word of its
    header, "transaction_time"), plus a separate "time" column when the date
    stands alone. Any time spelling
    DataConvert reads works, fractions of a second included; a wall clock needs
    --tz, a Unix time in seconds or milliseconds is an instant already.
    """
    return _collect_ticks(_generic_ticks, path, progress, tz, stats)


def _generic_ticks(path: str, progress: ProgressFn, tz: Optional[tzinfo],
                   stats: Optional[Dict[str, int]], end: _ParseEnd) -> Iterator[Dict]:
    """parse_generic_ticks's ticks, one at a time."""
    tape = _QuoteTape(stats)
    clock = SourceClock(tz, repeats=True)       # ticks: see SourceClock, TICKS
    with open(path, newline="", encoding="utf-8", errors="replace") as f:
        sample = f.read(8192)
        f.seek(0)
        reader = csv.reader(f, delimiter=_auto_delim(sample))
        cols = None
        for raw in reader:
            row = [c.strip().strip("\"<>") for c in raw]
            if not any(row):
                continue
            if cols is None:
                hdrs = [h.lower() for h in row]
                d = _header_index(hdrs, "date", "datetime", "timestamp", "time")
                t = _header_index(hdrs, "time")
                b, a = _quote_column(hdrs, "bid"), _quote_column(hdrs, "ask")
                if min(d, b, a) < 0:
                    raise ValueError(
                        "A tick CSV needs a header row naming its time, bid and ask "
                        f"columns; this one has {row!r}")
                cols = (d, t if t != d else -1, b, a, _bar_columns(hdrs)["volume"])
                continue
            d, t, b, a, v = cols
            try:
                ts = parse_ts(row[d], row[t] if 0 <= t < len(row) else "", tz, clock,
                              fraction=True)
            except (ValueError, IndexError):
                ts = 0
            try:      # a column headed exactly "volume" or "vol"
                volume = _volume_cell(row[v] if v < len(row) else "") if v >= 0 else None
            except ValueError:
                tape.skipped += 1
                continue
            tick = tape.add(ts, row[b] if b < len(row) else "", row[a] if a < len(row) else "",
                            reader.line_num, volume)
            if tick is not None:
                yield tick
    tape.finish(path, "CSV", clock, progress, end)


#: Every input format whose rows are quotes: its tick parser. These formats can
#: write raw ticks (--ticks), and their bars keep the quoted spread. The web
#: export's tick file is one side of the quote: its ticks have no spread.
TICK_PARSERS: Dict[str, Callable[..., List[Dict]]] = {
    "dukascopy":        parse_dukascopy_ticks,
    "dukascopyweb":     parse_dukascopy_web_ticks,
    "mt5ticks":         parse_mt5_ticks,
    "ninjatraderticks": parse_ninjatrader_ticks,
    "genericticks":     parse_generic_ticks,
}

#: The same parsers, one tick at a time: what a conversion streams (see
#: "Streaming"). Each takes (path, progress, tz, stats, end).
TICK_STREAMS: Dict[str, Callable[..., Iterator[Dict]]] = {
    "dukascopy":        _dukascopy_ticks,
    "dukascopyweb":     _duka_web_ticks,
    "mt5ticks":         _mt5_ticks,
    "ninjatraderticks": _ninjatrader_ticks,
    "genericticks":     _generic_ticks,
}


def _bars_from_ticks(tick_parser: Callable[..., List[Dict]], what: str):
    """The bar parser of a tick format: its ticks, aggregated (aggregate_ticks)."""
    def parse(path: str, tf_min: int, progress: ProgressFn = None,
              tz: Optional[tzinfo] = None,
              stats: Optional[Dict[str, int]] = None) -> List[Bar]:
        _mark_tick_source(stats)
        ticks = tick_parser(path, progress, tz=tz, stats=stats)
        if progress:
            progress(f"  Aggregating {len(ticks):,} ticks -> M{tf_min} bars")
        return aggregate_ticks(ticks, tf_min)
    parse.__name__ = f"parse_{what}_bars"
    parse.__doc__ = f"{what} ticks aggregated into bars; see aggregate_ticks."
    return parse


parse_mt5_tick_bars = _bars_from_ticks(parse_mt5_ticks, "mt5ticks")
parse_ninjatrader_tick_bars = _bars_from_ticks(parse_ninjatrader_ticks, "ninjatraderticks")
parse_generic_tick_bars = _bars_from_ticks(parse_generic_ticks, "genericticks")


def _make_bar(ts: int, o: float, h: float, l: float, c: float) -> Bar:
    return {"ts": ts, "open": o, "high": h, "low": l, "close": c, "bid": c, "ask": c}


def _volume_cell(field: str, empty: float = math.nan) -> float:
    """A volume cell as written. An empty cell is `empty`: not a number by
    default, so the row is refused and named (see "Volume" below); MT5's quote
    ticks, whose volume cell is empty by design, pass 0."""
    field = field.strip()
    return float(field) if field else empty


# ── A spread per bar (MT4/MT5) ───────────────────────────────────────────────
#
# MetaTrader's bar prices are BID prices, and its bar exports can carry one
# spread per bar, in points: the CSV's <SPREAD> column, the .hst v401 record's
# spread field. A point is the symbol's smallest price step, 10^-digits.
#
# The backtest reads every bar's open, high, low and close as MID prices and
# takes the half-spread a fill pays from the bar's closing quote, ask - mid
# (the engine's entry fill is open + (ask - mid) for a long). So a bar with a
# spread is written the same way a bar made from ticks is:
#
#   open, high, low, close   the bid price plus half the bar's spread
#   bid                      the close's bid, as exported
#   ask                      that bid plus the spread
#
# One spread stands for the whole bar: the export gives no other. A bar whose
# spread is 0 has bid = ask, which the backtest reads as "no quote", and it then
# charges the spread it is configured with for that bar.

def _decimals(field: str) -> int:
    """How many digits follow the decimal point in a price as written."""
    _, _, frac = field.strip().partition(".")
    return len(frac) if frac.isdigit() else 0


def _with_spread(bar: Bar, spread_price: float) -> Bar:
    """`bar` (bid prices) with `spread_price` applied as described above."""
    half = spread_price / 2.0
    bid = bar["close"]
    for k in ("open", "high", "low", "close"):
        bar[k] += half
    bar["bid"], bar["ask"] = bid, bid + spread_price
    return bar


def parse_mt4(path: str, tf_min: int, progress: ProgressFn = None,
              tz: Optional[tzinfo] = None,
              stats: Optional[Dict[str, int]] = None,
              point: Optional[float] = None) -> List[Bar]:
    """
    MT4 / MT5 bar CSV:
      <DATE>,<TIME>,<OPEN>,<HIGH>,<LOW>,<CLOSE>,<TICKVOL>,<VOL>,<SPREAD>
      2020.01.02,07:05,11430.0,11439.0,11428.0,11430.0,234,0,0

    Also handles MT5 (same layout, tab- or comma-delimited).
    Handles optional combined datetime column (no separate time col).

    A header naming a SPREAD column keeps the bar's spread (see "A spread per
    bar" above). The spread is in points; `point` is the price of one point.
    Without it, the point is read from the prices: MetaTrader writes every
    price with the symbol's digits, so the most decimals any price of the file
    has are the digits (1.10427: five, a point of 0.00001). The log says which.
    """
    bars: List[Bar] = []
    spreads: List[float] = []        # the SPREAD field of each bar, in points
    lines: List[int] = []            # the source line of each bar
    decimals = 0                     # the most decimals any price is written with
    skipped = 0
    clock = SourceClock(tz)          # one per FILE, read in row order
    with open(path, newline="", encoding="utf-8", errors="replace") as f:
        sample = f.read(8192)
        f.seek(0)
        delim = _auto_delim(sample)
        reader = csv.reader(f, delimiter=delim)
        date_col, time_col, o_col, h_col, l_col, c_col = 0, 1, 2, 3, 4, 5
        sp_col = -1
        vol_col = -1          # <VOL>, the real volume; never <TICKVOL> (price changes)
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
                        elif h == "SPREAD": sp_col   = ci
                        elif h == "VOL":    vol_col  = ci
                    continue
            if len(row) <= max(o_col, h_col, l_col, c_col, sp_col):
                skipped += 1
                continue
            try:
                t_str = row[time_col] if time_col >= 0 and time_col < len(row) else ""
                ts = parse_ts(row[date_col], t_str, tz, clock)
                if not ts:
                    skipped += 1
                    continue
                prices = [row[o_col], row[h_col], row[l_col], row[c_col]]
                bar = _make_bar(ts, *(float(p) for p in prices))
                if vol_col >= 0:
                    bar["volume"] = _volume_cell(row[vol_col] if vol_col < len(row) else "")
                if sp_col >= 0:
                    spread = float(row[sp_col])
                    if not spread >= 0:
                        raise ValueError("a spread below zero")
                    spreads.append(spread)
                    decimals = max([decimals] + [_decimals(p) for p in prices])
                bars.append(bar)
                lines.append(reader.line_num)
            except (ValueError, IndexError):
                skipped += 1
            if progress and i % 100_000 == 0 and i:
                progress(f"  Parsed {i:,} rows")
    if sp_col >= 0:
        unit = point if point is not None else 10.0 ** -decimals
        for bar, spread in zip(bars, spreads):
            _with_spread(bar, spread * unit)
        if progress:
            progress(f"  Spread column kept: a point is {unit:g} "
                     + ("(given)" if point is not None else
                        f"({decimals} decimals in the prices)"))
    # Checked as written: after the spread, which moves every price.
    for bar, line in zip(bars, lines):
        _audit(stats, bar, line)
    if progress:
        progress(f"  Loaded {len(bars):,} bars ({skipped:,} skipped)")
    clock.refuse_nonexistent(path)
    return _measure_and_sort(bars, stats, skipped, clock)


def parse_mt4hst(path: str, tf_min: int, progress: ProgressFn = None,
                 tz: Optional[tzinfo] = None,
                 stats: Optional[Dict[str, int]] = None,
                 point: Optional[float] = None) -> List[Bar]:
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

    A v401 record's spread (in points) is kept (see "A spread per bar"); the
    header's digits field (offset 84) gives the point, 10^-digits, unless
    `point` is given. Header: version(4) copyright(64) symbol(12) period(4)
    digits(4) timesign(4) last_sync(4) unused(52).
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
            _audit(stats, bars[-1], i + 1, unit="record")
    else:  # 401
        # CTM(int64) OPEN HIGH LOW CLOSE  tick_vol(uint64) spread(int32) real_vol(uint64)
        digits = struct.unpack_from("<i", raw, 84)[0]
        unit = point if point is not None else (
            10.0 ** -digits if 0 <= digits <= 10 else None)
        if progress:
            progress("  Spread field kept: a point is %g (%s)" % (
                unit, "given" if point is not None else "%d digits in the header" % digits)
                if unit is not None else
                "  Spread field NOT kept: the header's digits (%d) is not a number "
                "of decimals; give the point to keep it" % digits)
        FMT, SZ = "<qddddQiQ", 60
        n = len(data) // SZ
        for i in range(n):
            time_s, o, high, low, c, _tv, sp, real_vol = struct.unpack_from(FMT, data, i * SZ)
            bars.append(_make_bar(_ctm_to_utc_ms(time_s), o, high, low, c))
            bars[-1]["volume"] = float(real_vol)     # never tick_vol (price changes)
            if unit is not None and sp > 0:
                _with_spread(bars[-1], sp * unit)
            _audit(stats, bars[-1], i + 1, unit="record")

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
    vol_col = -1      # the column headed exactly "volume"; an export without a header has none
    with open(path, newline="", encoding="utf-8", errors="replace") as f:
        reader = csv.reader(f)
        for i, row in enumerate(reader):
            if not row:
                continue
            row = [c.strip() for c in row]
            if i == 0 and row[0].lower() in ("time", "date", "timestamp"):
                vol_col = _bar_columns([c.lower() for c in row])["volume"]
                continue
            if len(row) < 5:
                skipped += 1
                continue
            try:
                t = float(row[0])
                ts = int(t * 1000) if t < 1e12 else int(t)
                bars.append(_make_bar(ts, float(row[1]), float(row[2]),
                                      float(row[3]), float(row[4])))
                if vol_col >= 0:
                    bars[-1]["volume"] = _volume_cell(row[vol_col] if vol_col < len(row) else "")
                _audit(stats, bars[-1], reader.line_num)
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
      20200102;070500;11430;11439;11428;11430;100          (date and time in two fields)
      01/02/2020 07:05:00;11430;11439;11428;11430;100      (US date)
      2020-01-02 07:05:00;11430;11439;11428;11430;100      (ISO date)
      20200102;11430;11439;11428;11430;100                 (daily bar: the date alone)
    Separator: ; or ,   |   OHLCV order: open, high, low, close

    A row of exactly six fields whose first field is a bare yyyymmdd date is a
    DAILY bar (date;open;high;low;close;volume): NinjaTrader writes no time for
    it. Its time is 00:00 of that date in the source timezone (--tz). The date
    and the time in two fields always has seven, so the two never meet; and a
    six-digit open (123000.5) is a price, never a time.
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
                    ci = 1          # OHLCV after the combined stamp
                elif len(row) == 6 and _RE_NT_DATE.match(row[0]):
                    # A daily bar: the date alone, then OHLCV. Its time is
                    # the start of that date in the source zone.
                    d = row[0]
                    ts = _wall(datetime(int(d[:4]), int(d[4:6]), int(d[6:8])), tz, clock)
                    ci = 1
                else:
                    ts = parse_ts(row[0], row[1] if len(row) > 5 else "", tz, clock)
                    ci = 2          # OHLCV after the date and the time
                if not ts:
                    skipped += 1
                    continue
                bars.append(_make_bar(ts, float(row[ci]), float(row[ci+1]),
                                      float(row[ci+2]), float(row[ci+3])))
                if len(row) > ci + 4:          # the volume, the field after close
                    bars[-1]["volume"] = _volume_cell(row[ci + 4])
                _audit(stats, bars[-1], reader.line_num)
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
                _audit(stats, bars[-1], reader.line_num)
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
                if len(row) > 5:
                    bars[-1]["volume"] = _volume_cell(row[5])   # base asset volume
                _audit(stats, bars[-1], reader.line_num)
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
        vol_col = -1      # by its exact header name only, never by position
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
                        # An exact header name beats a longer one that merely
                        # starts with it: "open" is the price column, not
                        # "open_time", which comes first in a Bybit export.
                        for n in names:
                            for ci, h in enumerate(hdrs):
                                if h == n:
                                    return ci
                            for ci, h in enumerate(hdrs):
                                if h.startswith(n):
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
                    vol_col = _bar_columns(hdrs)["volume"]
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
                if vol_col >= 0:
                    bars[-1]["volume"] = _volume_cell(row[vol_col] if vol_col < len(row) else "")
                _audit(stats, bars[-1], reader.line_num)
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
    Falls back to the Dukascopy tick parser when the file shows no bar layout
    (a header naming no open, high, low and close columns, or fewer than five
    columns without a header). A header that names the four prices but no time
    column is refused.

    col_map  optional manual override: {'date': 0, 'time': 1, 'open': 2, ...}
    Column indices are 0-based; 'time' may be -1 if combined with date.
    """
    with open(path, newline="", encoding="utf-8", errors="replace") as f:
        sample = f.read(8192)
        f.seek(0)
        delim = _auto_delim(sample)
        reader = csv.reader(f, delimiter=delim)
        numbered = [(reader.line_num, r) for r in reader if any(c.strip() for c in r)]
        rows = [r for _, r in numbered]
        line_of = [n for n, _ in numbered]      # the line each kept row ends on

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
        v_col    = col_map.get("volume", -1)
        # Skip header row if first row looks non-numeric
        first_date = (rows[0][date_col].strip() if date_col < len(rows[0]) else "")
        data_rows = rows[1:] if re.search(r"[a-zA-Z]", first_date) else rows
        if any(c < 0 for c in [date_col, o_col, h_col, l_col, c_col]):
            raise ValueError("Incomplete column mapping  date, open, high, low, close are required")
    else:
        has_header = any(re.search(r"[a-z]", h) for h in raw_hdr)

        if has_header:
            cols = _bar_columns(raw_hdr)
            date_col, time_col = cols["date"], cols["time"]
            o_col, h_col, l_col, c_col = cols["open"], cols["high"], cols["low"], cols["close"]
            v_col = cols["volume"]
            # A header that names the four prices is a bar file. Read
            # by the tick parser it became wrong bars (open read as the ask,
            # high as the bid) and a successful conversion; without a time
            # column it is refused instead.
            if date_col < 0 and min(o_col, h_col, l_col, c_col) >= 0:
                raise ValueError(
                    f"The header names open, high, low and close columns but no time "
                    f"column ({', '.join(raw_hdr)}). Name the time column date, time, "
                    f"datetime or timestamp, or choose the columns by hand.")
            data_rows = rows[1:]
        else:
            # Positional: date [time] open high low close [volume ...]
            v_col = -1
            if len(rows[0]) >= 6:
                date_col, time_col, o_col, h_col, l_col, c_col = 0, 1, 2, 3, 4, 5
                if len(rows[0]) >= 7:
                    v_col = 6          # the column after close
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
    first_data = len(rows) - len(data_rows)     # data_rows is rows without its header, or all
    for k, row in enumerate(data_rows):
        try:
            t_str = row[time_col].strip() if 0 <= time_col < len(row) else ""
            ts = parse_ts(row[date_col].strip(), t_str, tz, clock)
            if not ts:
                skipped += 1
                continue
            bars.append(_make_bar(ts, float(row[o_col]), float(row[h_col]),
                                  float(row[l_col]), float(row[c_col])))
            if v_col >= 0:
                bars[-1]["volume"] = _volume_cell(row[v_col] if v_col < len(row) else "")
            _audit(stats, bars[-1], line_of[first_data + k])
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
    "dukascopyweb": (parse_dukascopy_web,
                     "Dukascopy web export  (Etc/UTC,Open,High,Low,Close,Volume; bars, or ticks of one side)"),
    "mt5ticks":     (parse_mt5_tick_bars, "MT5 tick export  (<DATE> <TIME> <BID> <ASK> <LAST> <VOLUME> <FLAGS>)"),
    "ninjatraderticks": (parse_ninjatrader_tick_bars,
                     "NinjaTrader tick export  (yyyyMMdd HHmmss fffffff;last;bid;ask;volume)"),
    "genericticks": (parse_generic_tick_bars, "Any tick CSV with a header naming a time, a bid and an ask column"),
    "mt4":          (parse_mt4,          "MT4 bar CSV  (<DATE>,<TIME>,<OPEN>,<HIGH>,<LOW>,<CLOSE>,...,<SPREAD>)"),
    "mt4hst":       (parse_mt4hst,       "MT4 .hst binary history  (v400: 44-byte, v401: 60-byte records)"),
    "mt5":          (parse_mt4,          "MT5 bar CSV  (same layout as MT4, tab or comma delimited)"),
    "tradingview":  (parse_tradingview,  "TradingView export  (time,open,high,low,close,volume)"),
    "ninjatrader":  (parse_ninjatrader,  "NinjaTrader bar CSV  (yyyymmdd hhmmss or date+time, or a daily yyyymmdd ; sep)"),
    "tradestation": (parse_tradestation, "TradeStation bar CSV  (Date,Time,Open,High,Low,Close,...)"),
    "binance":      (parse_binance,      "Binance klines CSV  (data.binance.vision, 12 columns, ms timestamps)"),
    "bybit":        (parse_bybit,        "Bybit / Kraken / Coinbase OHLCV CSV  (Unix seconds, open_time header)"),
    "generic":      (parse_generic,      "Auto-detect any OHLCV CSV  (column names or positional)"),
}

#: The bar formats that can carry one spread per bar, in points (see "A spread
#: per bar"): the MT4/MT5 CSV's SPREAD column and the .hst v401 spread field.
SPREAD_FORMATS = ("mt4", "mt5", "mt4hst")


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

        # Dukascopy's web export: a time zone, then Open, High, Low, Close.
        if _is_duka_web_header(lines[0].strip().split(",")):
            return "dukascopyweb"
        if "localtime" in sample or re.search(r"\d{2}\.\d{2}\.\d{4} \d{2}:\d{2}:\d{2}\.\d+", sample):
            return "dukascopy"
        # An MT5 tick export: the bar export's <DATE> header with <BID> and <ASK>.
        if "<bid>" in sample and "<ask>" in sample:
            return "mt5ticks"
        if "<date>" in sample or "<open>" in sample:
            return "mt4"
        # A NinjaTrader tick export: the compact stamp with a seven-digit fraction.
        if re.search(r"\d{8} \d{6} \d{7}[;,]", sample):
            return "ninjatraderticks"
        # A header naming a bid and an ask column and no open or close: quotes.
        head = [h.strip().strip("\"<>") for h in re.split(r"[,;\t]", lines[0].lower())]
        if (_quote_column(head, "bid") >= 0 and _quote_column(head, "ask") >= 0
                and not any(h in ("open", "close") for h in head)):
            return "genericticks"
        # Binance and Bybit with their header "open_time,open,...". Tested before
        # TradingView, whose "time,open,high,low,close" matches inside it: a
        # Bybit export used to be read as TradingView, its header counted as an
        # unreadable row and its volume column not found.
        if re.search(r"open_time,open,high,low,close", sample.replace(" ", "")):
            # Bybit uses Unix seconds, Binance uses ms
            # Distinguish by first data value: Binance ms = 13 digits
            for ln in lines[1:]:
                if ln.strip() and ln.strip()[0].isdigit():
                    first_val = ln.split(",")[0].strip()
                    return "binance" if len(first_val) >= 13 else "bybit"
            return "binance"
        # TradingView: first column is "time" (Unix seconds), no separate "date" column
        if (re.search(r"time,open,high,low,close", sample.replace(" ", ""))
                and not re.search(r"\bdate\b", sample[:200].lower())):
            return "tradingview"
        if re.search(r"\d{8} \d{6}", sample):
            return "ninjatrader"
        # A NinjaTrader daily export: yyyymmdd;open;high;low;close;volume.
        if any(re.match(r"\d{8}[;,]", ln) and len(re.split(r"[;,]", ln.strip())) == 6
               for ln in lines):
            return "ninjatrader"
        if re.search(r'"date".*"time".*"open"', sample):
            return "tradestation"

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
def _refuse_audited_with_counts(stats, output_path: str, progress: ProgressFn,
                                tick_payload: bool = False) -> None:
    """_refuse_audited, with the conversion counts attached to the refusal and
    reported, as for the ordering refusal."""
    try:
        _refuse_audited(stats, output_path)
    except RecordsInvalid as exc:
        exc.counts = dict(stats)
        if progress:
            progress(counts_summary(stats, tick_payload=tick_payload))
        raise


# ── Memory a conversion needs ───────────────────────────────────────────────
#
# A tick source streams (see "Streaming" below): its conversion needs at most
# STREAMING_MEMORY_MB, whatever the size of the file.
#
# A bar source is held in memory: its parser builds every row before anything
# is written (sorting by time needs the whole list), and what a row costs
# depends on the parser. Measured with Python 3.13 on Windows (peak working set
# of a fresh process, MB of 10^6 bytes, one-minute bars with volume), per
# million rows on top of about 20 MB for Python itself:
#
#   tradingview 497, tradestation 463, ninjatrader 496, binance 496, bybit 496,
#   mt4 640, mt5 642, mt4hst 678, dukascopyweb 634, generic 1,124
#
# linear in the rows (two million: mt4 638, dukascopyweb 630, generic 1,120).
# BAR_MB_PER_MILLION_ROWS holds them with 5 % added, rounded up to 10 MB. MB
# and GB here are 10^6 and 10^9 bytes.
BAR_MB_PER_MILLION_ROWS: Dict[str, int] = {
    "tradingview": 530, "tradestation": 490, "ninjatrader": 530, "binance": 530,
    "bybit": 530, "mt4": 680, "mt5": 680, "mt4hst": 720, "dukascopyweb": 670,
    "generic": 1180,
}
#: The figure for a format without one of its own: the largest.
MEMORY_MB_PER_MILLION_ROWS = max(BAR_MB_PER_MILLION_ROWS.values())
MEMORY_BASE_MB = 20
_ROW_SAMPLE_BYTES = 1 << 20      # the first MiB of a text file gives its mean row length


class NotEnoughMemory(Exception):
    """The input needs more memory to convert than the computer has available.

    Raised before the input is parsed, so nothing is written. Carries `rows`,
    `need_mb`, `available_mb` and `reason`: the message without the
    command-line switch, for the GUI, which asks "Convert anyway?" instead.
    `streaming`: the input is a tick file, whose need is the ceiling.
    `fmt`: the bar format, whose figure per million rows the message names.
    """

    def __init__(self, rows: int, need_mb: float, available_mb: float,
                 streaming: bool = False, fmt: Optional[str] = None):
        self.rows, self.need_mb, self.available_mb = rows, need_mb, available_mb
        self.streaming = streaming
        per_million = memory_per_million_rows(fmt)
        if streaming:
            reason = (
                f"Converting a tick file needs up to about {memory_text(need_mb)} of "
                f"memory, whatever its size, and about {memory_text(available_mb)} is "
                f"available on this computer")
        else:
            reason = (
                f"This file has about {rows:,} rows. Converting it needs about "
                f"{memory_text(need_mb)} of memory, and about {memory_text(available_mb)} "
                f"is available on this computer. DataConvert holds every row of a bar file "
                f"in memory while it converts (about {per_million:,} MB per million rows "
                f"of this format), so the conversion would run out of memory before it "
                f"ends. A file that covers a shorter period needs less")
        self.reason = reason + "."
        super().__init__(reason + "; --ignore-memory-check starts the conversion anyway.")


def memory_text(mb: float) -> str:
    """'640 MB' or '8.6 GB'."""
    return f"{mb / 1000:.1f} GB" if mb >= 1000 else f"{mb:.0f} MB"


def memory_per_million_rows(fmt: Optional[str] = None) -> int:
    """The memory a million rows of a bar file of `fmt` take, in MB (measured,
    see above); the largest figure for a format without one."""
    return BAR_MB_PER_MILLION_ROWS.get(fmt, MEMORY_MB_PER_MILLION_ROWS)


def memory_estimate_mb(rows: int, fmt: Optional[str] = None) -> float:
    """The memory a conversion of `rows` rows of a bar file of `fmt` needs,
    in MB (measured, see above)."""
    return MEMORY_BASE_MB + memory_per_million_rows(fmt) * rows / 1_000_000


def _rows_in_text(size: int, head: bytes) -> int:
    """Rows of a text file of `size` bytes whose first bytes are `head`:
    counted when `head` is the whole file, else estimated from the mean
    length of its complete rows (the sample's last row is usually cut)."""
    if not head:
        return 0
    lines = head.count(b"\n")
    if len(head) >= size:
        return lines + (0 if head.endswith(b"\n") else 1)
    if not lines:
        return 1
    return round(size * lines / (head.rindex(b"\n") + 1))


def estimate_rows(path: str, fmt: str) -> int:
    """About how many rows `path` holds, without reading more than a sample:
    an MT4 .hst file from its record size, a ZIP from its largest member's
    uncompressed size, a text file from the length of its first rows."""
    size = os.path.getsize(path)
    with open(path, "rb") as f:
        head = f.read(_ROW_SAMPLE_BYTES)
    if fmt == "mt4hst" and len(head) >= 4:
        version = struct.unpack_from("<I", head, 0)[0]
        record = 60 if version == 401 else 44
        return max(0, (size - 148) // record)
    if zipfile.is_zipfile(path):
        with zipfile.ZipFile(path) as z:
            members = [i for i in z.infolist() if not i.is_dir()]
            if not members:
                return 0
            biggest = max(members, key=lambda i: i.file_size)
            with z.open(biggest) as f:
                return _rows_in_text(biggest.file_size, f.read(_ROW_SAMPLE_BYTES))
    return _rows_in_text(size, head)


def available_memory_mb() -> Optional[float]:
    """The physical memory available to a new process now, in MB, or None
    where it cannot be read (then the estimate is reported, not enforced)."""
    try:
        if sys.platform == "win32":
            import ctypes

            class _MemoryStatus(ctypes.Structure):
                _fields_ = [("dwLength", ctypes.c_ulong), ("dwMemoryLoad", ctypes.c_ulong),
                            ("ullTotalPhys", ctypes.c_ulonglong),
                            ("ullAvailPhys", ctypes.c_ulonglong),
                            ("ullTotalPageFile", ctypes.c_ulonglong),
                            ("ullAvailPageFile", ctypes.c_ulonglong),
                            ("ullTotalVirtual", ctypes.c_ulonglong),
                            ("ullAvailVirtual", ctypes.c_ulonglong),
                            ("ullAvailExtendedVirtual", ctypes.c_ulonglong)]

            status = _MemoryStatus()
            status.dwLength = ctypes.sizeof(status)
            if ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(status)):
                return status.ullAvailPhys / 1_000_000
            return None
        with open("/proc/meminfo", encoding="ascii") as f:
            for line in f:
                if line.startswith("MemAvailable:"):
                    return int(line.split()[1]) * 1024 / 1_000_000
    except (OSError, ValueError, AttributeError):
        return None
    return None


def check_memory(input_path: str, fmt: str, progress: ProgressFn = None,
                 ignore: bool = False,
                 as_ticks: bool = False) -> Tuple[int, float, Optional[float]]:
    """Say the memory the conversion needs; raise NotEnoughMemory when it is
    more than is available (unless `ignore`). A conversion that streams
    (streams()) needs STREAMING_MEMORY_MB whatever the rows; any other the
    estimate for its rows. Returns (rows, need_mb, available_mb)."""
    rows = estimate_rows(input_path, fmt)
    streaming = streams(input_path, fmt, as_ticks)
    need = STREAMING_MEMORY_MB if streaming else memory_estimate_mb(rows, fmt)
    available = available_memory_mb()
    if progress:
        shown = f" ({memory_text(available)} available)" if available is not None else ""
        if streaming:
            progress(f"Memory: about {rows:,} rows, converted in at most about "
                     f"{memory_text(need)} of memory{shown}")
        else:
            progress(f"Memory: about {rows:,} rows, about {memory_text(need)} needed to "
                     f"convert{shown}")
    if available is not None and need > available and not ignore:
        raise NotEnoughMemory(rows, need, available, streaming=streaming, fmt=fmt)
    return rows, need, available


# ── Streaming ────────────────────────────────────────────────────────────────
#
# A tick export holds tens or hundreds of millions of rows, and a conversion of
# one does not hold them. Every tick goes to a scratch file of 32-byte records
# (the layout of a tick record) as it is parsed, in the order of the file, and
# what the conversion must know about the ticks is counted on the way. Then:
#
#   raw ticks   the scratch file is the output: once the ticks are known to
#               be writable, the header goes in front of them and the file
#               takes the output's name.
#   bars        the ticks are read back in time order and aggregated as they
#               come (_bars_in_order): one bar is open at a time and is
#               written when the next one starts.
#
# Time order. Ticks that never go back in the file are in time order already
# and are read back as they are. Otherwise the scratch file is sorted on disk:
# runs of SORT_RUN_TICKS consecutive ticks are sorted in memory by time, ties
# in file order, and written out; then the runs are merged, MERGE_FAN_IN at a
# time, a tick of an earlier run before a tick of a later run of the same time.
# A run holds consecutive ticks, so a merge of runs in their order is the
# stable sort of everything they hold: the stream is exactly the list that
# sorting all ticks by time gives, and so are the bars. A run is an artifact of
# the sort only; the aggregation never sees one.
#
# Nothing is written under the output's name before the ticks have passed every
# check: a refused conversion leaves the file that was there, and removes its
# scratch files and any folder it made for the output. The scratch files lie
# beside the output and need about 32 bytes per tick (twice that while ticks
# that are not in time order are sorted).
#
# The memory this takes does not grow with the file. Measured with Python 3.13
# on Windows (peak working set, MB of 10^6 bytes): about 28 MB for ticks in
# time order, raw or as bars; about 129 MB for shuffled ticks, the same for one
# and for two million of them, the most being one full sort run in memory.
# Held in memory, as before, the same million ticks took 338 MB and two million
# 652 MB. STREAMING_MEMORY_MB is the stated ceiling, with room for the window
# (Tk) and other platforms.

#: Ticks sorted in memory at once when a file's ticks are not in time order.
SORT_RUN_TICKS = 1_000_000
#: Sorted runs merged at once.
MERGE_FAN_IN = 32
#: Records read from a scratch file at a time (a merge reads its runs in
#: blocks of STREAM_BLOCK_RECORDS / MERGE_FAN_IN).
STREAM_BLOCK_RECORDS = 1 << 15
#: The most memory a conversion of a tick file needs, in MB: measured (see
#: above) with a margin.
STREAMING_MEMORY_MB = 200

_TICK_RECORD = struct.Struct(TICK_FMT)
_BAR_RECORD = struct.Struct(BAR_FMT)
_WRITE_BYTES = 1 << 20          # written out once a buffer holds this much


def streams(input_path: str, fmt: Optional[str], as_ticks: bool = False) -> bool:
    """True when converting `input_path` as `fmt` streams its ticks: every
    tick format, except a Dukascopy web export of bars, which is read as the
    bars it holds."""
    if fmt not in TICK_STREAMS:
        return False
    if as_ticks or fmt != "dukascopyweb":
        return True
    try:
        return duka_web_export_name(input_path).ticks
    except ValueError:
        return False


class _Scratch:
    """The scratch files of one conversion, beside its output, and the folders
    made for the output. close() removes the files left in it and, after a
    conversion that failed, those folders (when empty)."""

    def __init__(self, output_path: str):
        self.dir = os.path.dirname(os.path.abspath(output_path))
        self._base = os.path.basename(output_path)
        self._files: List[str] = []
        missing, d = [], self.dir
        while not os.path.isdir(d):
            missing.append(d)
            parent = os.path.dirname(d)
            if parent == d:
                break
            d = parent
        os.makedirs(self.dir, exist_ok=True)
        self._made = missing              # deepest first

    def new(self, what: str) -> str:
        """A new, empty scratch file. Created like an output file (the
        permissions a new file gets here), since one may become the output."""
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_BINARY", 0)
        while True:
            path = os.path.join(self.dir, ".%s.%s-%d-%s.part" % (
                self._base, what, os.getpid(), secrets.token_hex(4)))
            try:
                os.close(os.open(path, flags, 0o666))
            except FileExistsError:
                continue
            self._files.append(path)
            return path

    def drop(self, path: str) -> None:
        """Remove a scratch file that is no longer needed."""
        self._files.remove(path)
        os.remove(path)

    def becomes(self, path: str, output_path: str) -> None:
        """Give scratch file `path` the output's name."""
        os.replace(path, output_path)
        self._files.remove(path)

    def close(self, succeeded: bool) -> None:
        for path in self._files:
            try:
                os.remove(path)
            except OSError:
                pass
        self._files = []
        if not succeeded:
            for d in self._made:
                try:
                    os.rmdir(d)
                except OSError:
                    break


class _Spilled:
    """What the ticks of a scratch file are, counted as they were written:
    count, earliest and latest time (lo, hi), descents (a tick earlier than
    the one before it, the first ones in `first_descents` as the order check
    names them), repeats (a tick at the time of the one before it), and their
    volumes: how many carry one, whether one is not 0, whether one is -0.0,
    how many carry none and the first of those (position, tick)."""

    __slots__ = ("count", "lo", "hi", "descents", "first_descents", "repeats",
                 "carrying", "nonzero", "negative_zero", "lacking", "first_lacking")


def _spill_ticks(ticks: Iterator[Dict], f, missing_volume: float) -> _Spilled:
    """Write `ticks` to the open scratch file `f` as 32-byte records, in the
    order they come, `missing_volume` in the volume field of a tick without
    one, and count what _Spilled says."""
    pack = _TICK_RECORD.pack
    buf = bytearray()
    count = descents = repeats = carrying = lacking = 0
    nonzero = negative_zero = False
    first_descents: List[tuple] = []
    first_lacking: List[Tuple[int, Dict]] = []
    lo = hi = prev = 0
    for tick in ticks:
        ts = tick["ts"]
        if count:
            if ts < prev:
                descents += 1
                if len(first_descents) < 5:
                    first_descents.append(("descent", count - 1, prev, count, ts))
                if ts < lo:
                    lo = ts
            elif ts == prev:
                repeats += 1
            elif ts > hi:
                hi = ts
        else:
            lo = hi = ts
        prev = ts
        volume = tick.get("volume")
        if volume is None:
            lacking += 1
            if len(first_lacking) < REFUSAL_NAMES:
                first_lacking.append((count + 1, dict(tick)))
            volume = missing_volume
        else:
            carrying += 1
            if volume != 0.0:
                nonzero = True
            elif math.copysign(1.0, volume) < 0.0:
                negative_zero = True
        buf += pack(ts, tick["bid"], tick["ask"], volume)
        count += 1
        if len(buf) >= _WRITE_BYTES:
            f.write(buf)
            buf.clear()
    f.write(buf)
    s = _Spilled()
    s.count, s.lo, s.hi = count, lo, hi
    s.descents, s.first_descents, s.repeats = descents, first_descents, repeats
    s.carrying, s.nonzero, s.negative_zero = carrying, nonzero, negative_zero
    s.lacking, s.first_lacking = lacking, first_lacking
    return s


def _records_of(path: str, count: int, offset: int = 0,
                block: Optional[int] = None) -> Iterator[tuple]:
    """The `count` tick records of scratch file `path` from byte `offset`, as
    (ts, bid, ask, volume), read `block` records at a time."""
    size = _TICK_RECORD.size
    block = max(1, block or STREAM_BLOCK_RECORDS)
    with open(path, "rb") as f:
        f.seek(offset)
        left = count
        while left:
            k = min(block, left)
            data = f.read(k * size)
            if len(data) != k * size:
                raise OSError(f"The scratch file {path} is shorter than the ticks written to it")
            left -= k
            yield from _TICK_RECORD.iter_unpack(data)


def _merge_runs(runs: List[Tuple[str, int]], dst: Optional[str]) -> int:
    """Merge sorted runs [(path, records)], in their order, into `dst` (or
    nowhere): a tick of an earlier run comes before a tick of a later run of
    the same time. Returns how many ticks share the time of the tick before
    them in the merged order."""
    block = max(1, STREAM_BLOCK_RECORDS // max(MERGE_FAN_IN, 1))
    streams_ = [_records_of(p, n, block=block) for p, n in runs]
    pack = _TICK_RECORD.pack
    duplicates, prev, first = 0, 0, True
    out = open(dst, "wb") if dst is not None else None
    try:
        buf = bytearray()
        # heapq.merge is sorted(itertools.chain(*runs), key=...): stable, so
        # ties keep the order of the runs and, inside a run, their own.
        for rec in heapq.merge(*streams_, key=itemgetter(0)):
            ts = rec[0]
            if ts == prev and not first:
                duplicates += 1
            prev, first = ts, False
            if out is not None:
                buf += pack(*rec)
                if len(buf) >= _WRITE_BYTES:
                    out.write(buf)
                    buf.clear()
        if out is not None:
            out.write(buf)
    finally:
        if out is not None:
            out.close()
    return duplicates


def _sort_spill(src: str, count: int, dst: Optional[str], workdir: str,
                offset: int = 0) -> int:
    """Write the `count` tick records of `src` (from byte `offset`) to `dst`
    in time order, ties in file order -- what sorted(ticks, key=time) gives
    the whole list -- holding at most SORT_RUN_TICKS of them in memory; with
    `dst` None only count. Returns the ticks that share a time with the tick
    before them in that order. The runs live in `workdir` and are removed."""
    size = _TICK_RECORD.size
    run = max(1, SORT_RUN_TICKS)
    fan_in = max(2, MERGE_FAN_IN)
    made: List[str] = []

    def new_run() -> str:
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_BINARY", 0)
        while True:
            path = os.path.join(workdir, ".dataconvert-run-%d-%s.part"
                                % (os.getpid(), secrets.token_hex(4)))
            try:
                os.close(os.open(path, flags, 0o600))
            except FileExistsError:
                continue
            made.append(path)
            return path

    def remove(path: str) -> None:
        os.remove(path)
        made.remove(path)

    try:
        runs: List[Tuple[str, int]] = []
        with open(src, "rb") as f:
            f.seek(offset)
            left = count
            while left:
                k = min(run, left)
                data = f.read(k * size)
                if len(data) != k * size:
                    raise OSError(f"The scratch file {src} is shorter than the ticks written to it")
                left -= k
                # One int per tick, its time above its position: sorting
                # them sorts by time, ties by position, without a key.
                shift = max(1, (k - 1).bit_length())
                mask = (1 << shift) - 1
                keys = [(ts << shift) | i
                        for i, ts in enumerate(memoryview(data).cast("q")[::4])]
                keys.sort()
                path = new_run()
                with open(path, "wb") as out:
                    for j in range(0, k, 1 << 16):
                        out.write(b"".join([data[p:p + size] for p in
                                            [(key & mask) * size for key in keys[j:j + (1 << 16)]]]))
                del keys, data
                runs.append((path, k))
        while len(runs) > fan_in:
            merged: List[Tuple[str, int]] = []
            for g in range(0, len(runs), fan_in):
                group = runs[g:g + fan_in]
                if len(group) == 1:
                    merged.append(group[0])
                    continue
                path = new_run()
                _merge_runs(group, path)
                for p, _n in group:
                    remove(p)
                merged.append((path, sum(n for _p, n in group)))
            runs = merged
        return _merge_runs(runs, dst)
    finally:
        for path in list(made):
            try:
                os.remove(path)
            except OSError:
                pass


def _refuse_streamed_unordered(path: str, ticks: _Spilled) -> None:
    """_refuse_unordered for a streamed tick payload: raise RecordsNotInOrder,
    naming the first descents, when a tick is earlier than the one before it
    (ticks sharing a millisecond are legal)."""
    if ticks.descents:
        raise _unordered_refusal(path, ticks.count, "tick", True, ticks.first_descents,
                                 ticks.descents, 0)


def _spilled_ticks(path: str, count: int) -> Iterator[Dict]:
    """The ticks of a scratch file written for bars, whose volume field is not
    a number where the tick has none."""
    for ts, bid, ask, volume in _records_of(path, count):
        if volume == volume:
            yield {"ts": ts, "bid": bid, "ask": ask, "volume": volume}
        else:
            yield {"ts": ts, "bid": bid, "ask": ask}


class _Refusals:
    """The records of a payload that cannot be market data: how many, and the
    first REFUSAL_NAMES as the content check names them."""

    __slots__ = ("bad", "entries")

    def __init__(self) -> None:
        self.bad = 0
        self.entries: List[Dict] = []

    def check(self, rec: Dict, position: int, what: str) -> None:
        reason = record_problem(rec, what == "tick")
        if reason is not None:
            self.bad += 1
            if len(self.entries) < REFUSAL_NAMES:
                self.entries.append(_invalid_entry(dict(rec), position, what, reason))


class _BarsWritten:
    """What the bars written to a scratch file are: count, first and last
    time, their volumes (as _Spilled counts them), the bars that cannot be
    market data when written with volume (a bar without one as not a number,
    as volume_of makes it) and without, and any bar not later than the one
    before it (descents, repeats, the first of them as write_v2 names them;
    bars made in time order have none)."""

    __slots__ = ("count", "first", "last", "carrying", "nonzero", "negative_zero",
                 "with_volume", "without_volume", "descents", "repeats", "unordered")


def _write_bars(bars: Iterator[Bar], f) -> _BarsWritten:
    """Write `bars` (ascending) to the open file `f` as bar records, each
    with its volume (0.0 for a bar without one), and say what they are."""
    pack = _BAR_RECORD.pack
    buf = bytearray()
    w = _BarsWritten()
    w.count, w.first, w.last = 0, 0, 0
    w.carrying, w.nonzero, w.negative_zero = 0, False, False
    w.with_volume, w.without_volume = _Refusals(), _Refusals()
    w.descents, w.repeats, w.unordered = 0, 0, []
    for b in bars:
        w.count += 1
        if w.count == 1:
            w.first = b["ts"]
        elif b["ts"] <= w.last:
            kind = "descent" if b["ts"] < w.last else "duplicate"
            if kind == "descent":
                w.descents += 1
            else:
                w.repeats += 1
            if len(w.unordered) < 5:
                w.unordered.append((kind, w.count - 2, w.last, w.count - 1, b["ts"]))
        w.last = b["ts"]
        if "volume" in b:
            volume = b["volume"]
            w.carrying += 1
            if volume != 0.0:
                w.nonzero = True
            elif math.copysign(1.0, volume) < 0.0:
                w.negative_zero = True
            w.with_volume.check(b, w.count, "bar")
            del b["volume"]
            w.without_volume.check(b, w.count, "bar")
        else:
            volume = 0.0
            w.without_volume.check(b, w.count, "bar")
            b["volume"] = math.nan
            w.with_volume.check(b, w.count, "bar")
        buf += pack(b["ts"], b["open"], b["high"], b["low"], b["close"],
                    b["bid"], b["ask"], volume)
        if len(buf) >= _WRITE_BYTES:
            f.write(buf)
            buf.clear()
    f.write(buf)
    return w


def _zero_volumes(path: str, offset: int, count: int, size: int) -> None:
    """Set the volume field (a record's last 8 bytes) of the `count` records
    of `size` bytes from byte `offset` of `path` to 0.0: a file written
    without volume holds 0.0 there, and a -0.0 of the source is not 0.0."""
    block = max(1, STREAM_BLOCK_RECORDS)
    zero = bytes(8)
    with open(path, "r+b") as f:
        done = 0
        while done < count:
            k = min(block, count - done)
            f.seek(offset + done * size)
            data = bytearray(f.read(k * size))
            for i in range(k):
                data[i * size + size - 8:(i + 1) * size] = zero
            f.seek(offset + done * size)
            f.write(data)
            done += k


def _write_header(path: str, payload_type: int, tf_min: int, count: int,
                  first_ts: int, last_ts: int, symbol: str) -> None:
    """The BAR6 header, written over the first bytes of `path` (kept free
    for it), exactly as write_v2 and write_ticks_v2 write it."""
    sym_b = symbol.encode("ascii", errors="replace")[:16].ljust(16, b"\x00")
    with open(path, "r+b") as f:
        f.write(struct.pack(HDR_V2_FMT, MAGIC_V2, FORMAT_VERSION, payload_type,
                            tf_min, count, first_ts, last_ts, sym_b))


def _convert_streamed(input_path: str, output_path: str, symbol: str, tf_min: int,
                      fmt: str, progress: ProgressFn, as_ticks: bool,
                      source_tz: Optional[tzinfo], stats, no_volume: bool) -> dict:
    """convert() for a tick source (streams()): the same file, the same
    returned dict, the same refusals and the same progress lines as the
    conversion that holds every tick in memory, plus a line when ticks are
    sorted on disk. See "Streaming"."""
    scratch = _Scratch(output_path)
    succeeded = False
    try:
        result = _stream(scratch, input_path, output_path, symbol, tf_min, fmt, progress,
                         as_ticks, source_tz, stats, no_volume)
        succeeded = True
        return result
    finally:
        scratch.close(succeeded)


def _stream(scratch: _Scratch, input_path: str, output_path: str, symbol: str,
            tf_min: int, fmt: str, progress: ProgressFn, as_ticks: bool,
            source_tz: Optional[tzinfo], stats, no_volume: bool) -> dict:
    if not as_ticks and fmt != "dukascopyweb":
        _mark_tick_source(stats)      # as the bar parsers of a tick format do
    end = _ParseEnd()
    spill = scratch.new("ticks")
    header = HDR_V2_SIZE if as_ticks else 0
    with open(spill, "r+b") as f:
        f.write(bytes(header))
        ticks = _spill_ticks(TICK_STREAMS[fmt](input_path, progress, source_tz, stats, end),
                             f, 0.0 if as_ticks else math.nan)
    n = ticks.count
    if not as_ticks and progress:
        progress(f"  Aggregating {n:,} ticks -> M{tf_min} bars")
    if not n:
        raise ValueError("No ticks could be parsed from the input file" if as_ticks else
                         "No bars could be parsed from the input file")

    # The counts of _record: duplicates are counted in time order.
    if ticks.descents:
        if progress:
            progress(f"  Sorting {n:,} ticks by time on disk")
        if as_ticks:
            duplicates = _sort_spill(spill, n, None, scratch.dir, offset=header)
        else:
            in_order = scratch.new("sorted")
            duplicates = _sort_spill(spill, n, in_order, scratch.dir)
            scratch.drop(spill)
            spill = in_order
    else:
        duplicates = ticks.repeats
    stats["rows_parsed"] += n
    stats["skipped_rows"] += end.skipped
    stats["out_of_order_rows"] += ticks.descents
    stats["duplicate_timestamps"] += duplicates
    if end.clock is not None:
        stats["dst_repeated_rows"] = (stats.get("dst_repeated_rows", 0)
                                      + end.clock.repeated_hour_rows)

    _refuse_audited_with_counts(stats, output_path, progress, tick_payload=as_ticks)

    if as_ticks:
        state = _volume_state(fmt, no_volume, ticks.carrying, n, ticks.nonzero)
        volume = "source" if state == "mixed" else state
        _say_replacing(output_path, progress)
        if progress:
            progress(f"Writing {n:,} raw ticks -> {os.path.basename(output_path)}")
        try:
            if state == "mixed":
                entries = []
                for position, tick in ticks.first_lacking:
                    tick["volume"] = math.nan
                    entries.append(_invalid_entry(tick, position, "tick",
                                                  record_problem(tick, tick=True)))
                raise _invalid_refusal(output_path, n, "tick", entries, ticks.lacking)
            _refuse_streamed_unordered(output_path, ticks)
        except (RecordsNotInOrder, RecordsInvalid) as exc:
            exc.counts = dict(stats)
            if progress:
                progress(counts_summary(stats, tick_payload=True))
            raise
        if volume != "source" and ticks.negative_zero:
            _zero_volumes(spill, header, n, TICK_SIZE)
        _write_header(spill, PAYLOAD_TICKS | (PAYLOAD_VOLUME_BIT if volume == "source" else 0),
                      0, n, ticks.lo, ticks.hi, symbol)
        scratch.becomes(spill, output_path)
        n_bytes = HDR_V2_SIZE + n * TICK_SIZE
        if progress:
            progress(f"DONE  {n:,} ticks  |  "
                     f"{ts_to_str(ticks.lo)} -> {ts_to_str(ticks.hi)}"
                     f"  |  {n_bytes / 1024 / 1024:.2f} MB written")
            progress(VOLUME_SAYS[volume])
            progress(counts_summary(stats, tick_payload=True))
        return {
            "ticks":  n,
            "bars":   n,
            "bytes":  n_bytes,
            "start":  ticks.lo,
            "end":    ticks.hi,
            "format": fmt,
            "symbol": symbol,
            "tf_min": 0,
            "tick_source": True,
            "quote_side": stats.quote_side,
            "volume": volume,
            **stats,
        }

    out = scratch.new("bars")
    with open(out, "r+b") as f:
        f.write(bytes(HDR_V2_SIZE))
        bars = _write_bars(_bars_in_order(_spilled_ticks(spill, n), tf_min), f)
    scratch.drop(spill)
    state = _volume_state(fmt, no_volume, bars.carrying, bars.count, bars.nonzero)
    volume = "source" if state == "mixed" else state
    _say_replacing(output_path, progress)
    if progress:
        progress(f"Writing {bars.count:,} M{tf_min} bars -> {os.path.basename(output_path)}")
    # A bar's problems as write_v2 checks them: with its volume when the file
    # is written with volume, else without (volume_of drops it).
    refused = bars.with_volume if volume == "source" else bars.without_volume
    exc: Optional[ValueError] = None
    if refused.bad:
        exc = _invalid_refusal(output_path, bars.count, "bar", refused.entries, refused.bad)
    elif bars.descents or bars.repeats:
        exc = _unordered_refusal(output_path, bars.count, "bar", False, bars.unordered,
                                 bars.descents, bars.repeats)
    if exc is not None:
        exc.counts = dict(stats)
        if progress:
            progress(counts_summary(stats))
        raise exc
    if volume != "source" and bars.negative_zero:
        _zero_volumes(out, HDR_V2_SIZE, bars.count, BAR_SIZE)
    _write_header(out, PAYLOAD_BARS | (PAYLOAD_VOLUME_BIT if volume == "source" else 0),
                  tf_min, bars.count, bars.first, bars.last, symbol)
    scratch.becomes(out, output_path)
    n_bytes = HDR_V2_SIZE + bars.count * BAR_SIZE
    if progress:
        progress(f"DONE  {bars.count:,} bars  |  {ts_to_str(bars.first)} -> "
                 f"{ts_to_str(bars.last)}  |  {n_bytes / 1024 / 1024:.2f} MB written")
        progress(VOLUME_SAYS[volume])
        progress(counts_summary(stats))
    return {
        "bars":   bars.count,
        "bytes":  n_bytes,
        "start":  bars.first,
        "end":    bars.last,
        "format": fmt,
        "symbol": symbol,
        "tf_min": tf_min,
        "tick_source": stats.tick_source,
        "quote_side": stats.quote_side,
        "volume": volume,
        **stats,
    }


def may_replace(output_path: str, ask: Callable[[str, str], bool]) -> bool:
    """The window's question before it writes over a file that is there
    (a BID and an ASK export of one symbol used to replace each other). True
    when there is no such file, else the user's answer."""
    if not os.path.exists(output_path):
        return True
    return bool(ask("Replace file?",
                    f"{os.path.basename(output_path)} exists already.\n\nReplace it?"))


def _say_replacing(output_path: str, progress: ProgressFn) -> None:
    """Say it when the output replaces a file that is there (a BID and an ASK
    export of one symbol converted to one name used to replace each other
    without a word). Said after every check passed, just before the write."""
    if progress and os.path.exists(output_path):
        progress(f"Replacing the existing file {os.path.basename(output_path)}")


def convert(input_path: str, output_path: str, symbol: str, tf_min: int,
            fmt: Optional[str] = None, progress: ProgressFn = None,
            as_ticks: bool = False,
            col_map: Optional[Dict[str, int]] = None,
            tz: Optional[str] = None,
            ignore_memory_check: bool = False,
            point: Optional[float] = None,
            no_volume: bool = False) -> dict:
    """Convert input_path -> BAR6 output_path.

    no_volume: write the file without volume even when the source has a
             volume column (see "Volume"). The returned dict's `volume` says
             which volume was written: source, zero, quoted, none or off.

    as_ticks=True: write raw ticks (payload_type=1, 32-byte TickRecord), for
             the formats in TICK_PARSERS.
    point: the price of one point of an MT4/MT5 spread (mt4, mt5, mt4hst);
           without it the point is read from the file (see parse_mt4,
           parse_mt4hst). Refused for the other formats, which have no spread
           column it could apply to.
    col_map: manual column index mapping for generic CSVs,
             e.g. {'date': 0, 'time': 1, 'open': 2, 'high': 3, 'low': 4, 'close': 5}
    tz: source timezone for formats that store a bare wall clock.
        An IANA name or a fixed offset. Required for those formats, refused
        for the ones whose timestamps already fix an instant.
    ignore_memory_check: convert even when the memory the conversion needs
        is more than is available (see check_memory). Without it such an
        input raises NotEnoughMemory before it is parsed.

    A tick source streams (see "Streaming"): its ticks go through scratch
    files beside the output and the conversion holds at most
    STREAMING_MEMORY_MB, with the same output, returned dict and refusals as
    holding them all. A bar source is parsed into memory, as before.

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
    stamped one instant. Raises RecordsInvalid, with the counts too, when a
    record cannot be market data (a high below its low, a price of zero or
    less, a value that is not a number): it names the source rows. No output
    file is created in either case, and a file already at the output's name is
    left as it was. A streamed refusal's `offenders` holds the records it
    names (the first ones), not all of them. `start` and `end` in the
    returned dict are the payload's earliest and latest timestamp, not its
    first and last record.
    """
    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Input not found: {input_path}")

    # A column mapping belongs to the generic reader. With the format on auto
    # it chooses that reader; with any other format it is refused. It used to
    # be dropped without a word when auto detected another format.
    if col_map:
        if fmt is None:
            fmt = "generic"
            if progress:
                progress("Format: generic, read with the column mapping you made")
        elif fmt != "generic":
            raise ValueError(
                f"A column mapping is used with the generic format only; format {fmt!r} "
                f"reads its own columns. Choose generic (or auto) to use the mapping.")

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
    if point is not None:
        if fmt not in SPREAD_FORMATS:
            raise ValueError(
                f"A point applies to the spread column of {', '.join(SPREAD_FORMATS)} "
                f"exports; format {fmt!r} has no spread column.")
        if not (point > 0 and math.isfinite(point)):
            raise ValueError(f"A point is a price step above zero; got {point!r}.")
    # The memory the conversion will need, said before it starts, and refused
    # when the computer does not have it: an out-of-memory error after a long
    # run tells the user nothing they could have acted on.
    check_memory(input_path, fmt, progress, ignore=ignore_memory_check, as_ticks=as_ticks)
    stats = new_conversion_counts()
    stats.read_volume = not (no_volume or fmt in QUOTED_SIZE_FORMATS)
    if progress and source_tz is not None:
        carried = (f"zone rules carried by DataConvert, IANA {ZONES_VERSION}; "
                   if is_carried(source_tz) else "")
        progress(f"Source timezone: {tz} ({carried}timestamps converted to UTC on import)")

    # Tick output path
    if as_ticks:
        if fmt not in TICK_PARSERS:
            raise ValueError(
                f"Raw ticks can only be written from a tick format "
                f"({', '.join(TICK_PARSERS)}). Got: {fmt!r}. "
                f"A bar format does not carry a bid and an ask per tick."
            )
        if progress:
            progress(f"Parsing {os.path.basename(input_path)} as [{fmt}] (raw ticks)")
        # The ticks stream into the output (see "Streaming"); a payload that
        # descends is refused, and the counts travel with the refusal and out
        # through `progress`, because they are what explains it: "2 rows out
        # of order" on a four-tick export names the source's problem, where
        # the exception alone names only ours.
        return _convert_streamed(input_path, output_path, symbol, 0, fmt, progress,
                                 True, source_tz, stats, no_volume)

    # Bar output path
    parser_fn, _ = FORMATS[fmt]
    if progress:
        progress(f"Parsing {os.path.basename(input_path)} as [{fmt}]")
    if streams(input_path, fmt):
        return _convert_streamed(input_path, output_path, symbol, tf_min, fmt, progress,
                                 False, source_tz, stats, no_volume)

    if col_map and fmt == "generic":
        bars = parse_generic(input_path, tf_min, progress, col_map=col_map,
                             tz=source_tz, stats=stats)
    elif fmt in SPREAD_FORMATS:
        bars = parser_fn(input_path, tf_min, progress, tz=source_tz, stats=stats,
                         point=point)
    else:
        bars = parser_fn(input_path, tf_min, progress, tz=source_tz, stats=stats)
    if not bars:
        raise ValueError("No bars could be parsed from the input file")
    _refuse_audited_with_counts(stats, output_path, progress)
    volume = volume_of(bars, fmt, no_volume)
    _say_replacing(output_path, progress)

    if progress:
        progress(f"Writing {len(bars):,} M{tf_min} bars -> {os.path.basename(output_path)}")

    # write_v2 refuses two bars stamped the same instant. Same
    # reasoning as the tick path above -- the counts explain the refusal.
    try:
        n_bytes = write_v2(output_path, bars, symbol, tf_min,
                           with_volume=volume == "source")
    except (RecordsNotInOrder, RecordsInvalid) as exc:
        exc.counts = dict(stats)
        if progress:
            progress(counts_summary(stats))
        raise
    start, end = _payload_span(bars)

    if progress:
        progress(f"DONE  {len(bars):,} bars  |  {ts_to_str(start)} -> {ts_to_str(end)}"
                 f"  |  {n_bytes / 1024 / 1024:.2f} MB written")
        progress(VOLUME_SAYS[volume])
        progress(counts_summary(stats))

    return {
        "bars":   len(bars),
        "bytes":  n_bytes,
        "start":  start,
        "end":    end,
        "format": fmt,
        "symbol": symbol,
        "tf_min": tf_min,
        "tick_source": stats.tick_source,
        "quote_side": stats.quote_side,
        "volume": volume,
        **stats,
    }


# =============================================================================
# --info: inspect an existing .bin file
# =============================================================================
class NotABarFile(ValueError):
    """Raised by describe_bin (and so by cmd_info) for a file it cannot
    describe: too short for its format's header, of an unknown version or
    payload type, or not holding the records its header states."""


#: The BAR5 header: magic, version, timeframe, count, start, end.
_HDR_V1_FMT = "<IIIIqq"
_HDR_V1_SIZE = struct.calcsize(_HDR_V1_FMT)  # 32


class BinInfo(NamedTuple):
    """What a .bin file's header says (describe_bin): the lines shown, the
    refusal that follows them when the file does not hold what its header
    states (or None), and `known`, False when the file is no BAR file."""
    lines: List[str]
    problem: Optional[NotABarFile]
    known: bool


def cmd_info(path: str) -> None:
    """Print a .bin file's header (describe_bin).

    Raises OSError when the file cannot be read, and NotABarFile when it is
    shorter than the header its magic announces, when a BAR6 header names a
    version or a payload type this tool does not know, or when the file's size
    does not match the records its header states (the header is printed
    first, then the sentence); main() reports either as a one-line error
    rather than a traceback. Exits with 1 for a file of no BAR format. Reads
    the header only, never more.
    """
    info = describe_bin(path)
    for line in info.lines:
        print(line)
    if not info.known:
        sys.exit(1)
    if info.problem is not None:
        raise info.problem


def describe_bin(path: str) -> BinInfo:
    """A .bin file's header as --info and the window's "Inspect .bin" show
    it: the kind from bit 0 of payload_type, the volume from bit 1, the
    record size of that kind, and the file size checked against the records.
    Raises OSError and NotABarFile as cmd_info describes, before any line
    when nothing of the header can be shown."""
    lines: List[str] = []
    problem: Optional[NotABarFile] = None
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
        # The header is only meaningful in the version and payload this tool
        # writes. Anything else is said in one sentence, before a single field
        # of it is shown as if it could be read.
        if ver != FORMAT_VERSION:
            raise NotABarFile(f"{path} is a BAR6 file of version {ver}; this tool "
                              f"reads version {FORMAT_VERSION} only, so its header "
                              f"cannot be read")
        if pt > (PAYLOAD_TICKS | PAYLOAD_VOLUME_BIT):
            raise NotABarFile(f"{path} is a BAR6 file with payload type {pt}; "
                              f"only 0 (bars), 1 (ticks), 2 (bars with volume) and "
                              f"3 (ticks with volume) exist, so its records cannot be read")
        sym = sym_b.rstrip(b"\x00").decode("ascii", errors="replace")
        ticks = bool(pt & PAYLOAD_TICKS)
        rec_size = TICK_SIZE if ticks else BAR_SIZE
        with_volume = " with volume" if pt & PAYLOAD_VOLUME_BIT else ""
        lines.append(f"File:        {path}")
        lines.append(f"Format:      BAR6")
        lines.append(f"Version:     {ver}")
        lines.append(f"Symbol:      {sym or '(empty)'}")
        lines.append(f"Payload:     {'ticks' if ticks else 'bars'}{with_volume} "
                     f"(type={pt}, {rec_size} B)")
        lines.append(f"Timeframe:   {'tick (M0)' if tf == 0 else f'M{tf}'}")
        lines.append(f"Records:     {count:,}")
        lines.append(f"Start:       {ts_to_str(ts0)}")
        lines.append(f"End:         {ts_to_str(ts1)}")
        expected = HDR_V2_SIZE + count * rec_size
        actual   = os.path.getsize(path)
        ok = "OK" if actual == expected else f"MISMATCH (expected {expected:,}, got {actual:,})"
        lines.append(f"File size:   {actual:,} bytes  {ok}")
        if (actual != expected and not ticks and count > 0
                and actual == HDR_V2_SIZE + count * EARLIER_BAR_SIZE):
            problem = NotABarFile(
                f"{path} was written in the earlier BAR6 bar layout ({EARLIER_BAR_SIZE}-byte "
                f"bar records, before the volume field). Convert the source data again "
                f"with this DataConvert")
        elif actual != expected:
            whole = max(actual - HDR_V2_SIZE, 0) // rec_size
            problem = NotABarFile(
                f"{path} does not hold the records its header states: the header "
                f"names {count:,} record(s) of {rec_size} bytes, the file holds "
                f"{whole:,} whole record(s) and {actual:,} bytes in all. It was cut "
                f"short or changed after it was written; convert the source data again")
        return BinInfo(lines, problem, True)
    if magic == MAGIC_V1:
        mg, ver, tf, count, ts0, ts1 = struct.unpack_from(_HDR_V1_FMT, raw)
        lines.append(f"File:        {path}")
        lines.append(f"Format:      BAR5  (older format -- convert the source data again "
                     f"to get BAR6)")
        lines.append(f"Timeframe:   M{tf}")
        lines.append(f"Bars:        {count:,}")
        lines.append(f"Start:       {ts_to_str(ts0)}")
        lines.append(f"End:         {ts_to_str(ts1)}")
        return BinInfo(lines, None, True)
    lines.append(f"File:        {path}")
    lines.append(f"Format:      UNKNOWN  (magic 0x{magic:08X})")
    return BinInfo(lines, None, False)


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
            ("volume", "Volume (optional, traded volume only)", False),
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

    # The price of one point of an MT4/MT5 spread column. Empty: read from the
    # file (the .hst header's digits, or the decimals of the CSV's prices).
    v_point = tk.StringVar(value="")
    point_frame = tk.Frame(r2, bg=C["bg"])
    point_frame.grid(row=0, column=2, sticky="w", padx=(12, 0))
    lbl(point_frame, "Spread point (optional)")
    entry_widget(point_frame, v_point, width=10).pack(ipady=4)

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
        ticks_row, text="Save as raw ticks  (payload_type=1, 32 B/tick - tick formats only)",
        variable=v_as_ticks, bg=C["bg"], fg=C["text"],
        activebackground=C["bg"], activeforeground=C["accent"],
        selectcolor=C["surf2"], font=(C["ui"][0], 8),
        command=lambda: _on_ticks_toggle(),
    )
    ticks_chk.pack(side="left")
    tk.Label(ticks_row, text="  <- Timeframe ignored when ticked",
             bg=C["bg"], fg=C["muted"], font=(C["ui"][0], 7)).pack(side="left")

    # Write without volume (the command line's --no-volume)
    v_no_volume = tk.BooleanVar(value=False)
    volume_row = tk.Frame(main, bg=C["bg"])
    volume_row.pack(fill="x", pady=(0, 8))
    tk.Checkbutton(
        volume_row, text="Write without volume  (even when the source has a volume column)",
        variable=v_no_volume, bg=C["bg"], fg=C["text"],
        activebackground=C["bg"], activeforeground=C["accent"],
        selectcolor=C["surf2"], font=(C["ui"][0], 8),
    ).pack(side="left")

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
        if fmt == "auto" or fmt in TICK_PARSERS:
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
            v_output.set(str(p.parent / default_output_name(path, sym, tf)))
        # Auto-detect format
        detected = detect_format(path)
        if detected in FORMATS:
            v_format.set(detected)

    def browse_output():
        sym = v_symbol.get().strip() or "output"
        tf  = v_tf.get() or "5"
        inp = v_input.get().strip()
        path = filedialog.asksaveasfilename(
            title="Save binary file as",
            initialfile=default_output_name(inp, sym, tf) if inp else f"{sym}_M{tf}.bin",
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
        if not may_replace(out, messagebox.askyesno):
            return
        point = None
        if v_point.get().strip():
            try:
                point = float(v_point.get().strip())
            except ValueError:
                messagebox.showerror("Error", "Spread point must be a number, for example 0.00001")
                return

        clear_log()
        convert_btn.configure(state="disabled")
        status_lbl.configure(fg=C["dim"])
        v_status.set("Converting")
        as_ticks = v_as_ticks.get()
        no_volume = v_no_volume.get()
        active_col_map = dict(_col_map) if _col_map else None
        tz_spec = v_tz.get().strip() or None

        def worker(ignore_memory=False):
            try:
                result = convert(inp, out, sym, tf, fmt, log,
                                 as_ticks=as_ticks, col_map=active_col_map,
                                 tz=tz_spec, ignore_memory_check=ignore_memory,
                                 point=point, no_volume=no_volume)
                root.after(0, lambda: on_done(result))
            except NotEnoughMemory as exc:
                root.after(0, lambda e=exc: on_memory(e))
            except Exception as exc:
                root.after(0, lambda e=exc: on_error(str(e)))

        def on_memory(exc):
            # The estimate is said, and the user decides: a computer that can
            # page to disk may still finish, slowly. The window asks, so its
            # text names no command-line switch.
            if messagebox.askyesno("Not enough memory", f"{exc.reason}\n\nConvert anyway?"):
                log("Converting anyway, without the memory check.", "dim")
                threading.Thread(target=worker, args=(True,), daemon=True).start()
            else:
                on_error(exc.reason)

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
        # Bars made from ticks take in every tick of their period,
        # those sharing a time included; nothing was refused.
        log(f"Duplicate timestamps: {dupes:,}"
            + ("  (legal in a tick file)" if ticks_written
               else "  (ticks aggregated into their bar)" if result.get("tick_source")
               else ""),
            "dim" if not dupes else "")
        if result.get("quote_side") or (result.get("format") == "dukascopyweb"):
            side = (result.get("quote_side") or "").upper()
            log(f"Prices: {'the ' + side + ' side' if side else 'one side of the quote'} "
                f"only. The file holds no spread; a backtest on it charges the spread "
                f"set for the run.")
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
        # The same reading of the header as --info (describe_bin), so the
        # window and the command line say the same about one file.
        try:
            info = describe_bin(path)
        except Exception as exc:
            log(f"ERROR: {exc}", "red")
            return
        for line in info.lines:
            log(line, "accent" if line.startswith("Format:") and info.known
                else "" if info.known else "red")
        if info.problem is not None:
            log(f"ERROR: {info.problem}", "red")

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
                             "Only for tick formats (dukascopy, dukascopyweb tick "
                             "exports, mt5ticks, ninjatraderticks, genericticks). "
                             "Ignores --tf.")
    parser.add_argument("--tz",
                        help="Source timezone of the input timestamps. "
                             "An IANA zone name such as Europe/Helsinki (DST-aware) "
                             "or a fixed offset such as +02:00. REQUIRED for formats "
                             "that store a bare wall clock (mt4, mt5, mt4hst, "
                             "mt5ticks, ninjatrader, ninjatraderticks, tradestation, "
                             "generic, genericticks); refused for formats whose "
                             "timestamps already fix an instant (dukascopy, "
                             "dukascopyweb, binance, bybit, tradingview).")
    parser.add_argument("--point", type=float,
                        help="The price of one point of an MT4/MT5 spread column "
                             "(mt4, mt5, mt4hst), e.g. 0.00001 for EURUSD at five "
                             "digits. Default: the .hst header's digits, or the most "
                             "decimals the CSV's prices are written with.")
    parser.add_argument("--ignore-memory-check", action="store_true",
                        help="Convert even when the file needs more memory than is available")
    parser.add_argument("--no-volume", action="store_true",
                        help="Write the file without volume, even when the source has a "
                             "volume column")
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
                         tz=args.tz, ignore_memory_check=args.ignore_memory_check,
                         point=args.point, no_volume=args.no_volume)
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


# =============================================================================
# The zone rules DataConvert carries (see carried_zone): the IANA time zone
# database's TZif files of these zones, base64. Public domain.
# =============================================================================
# -- carried zone rules: generated, do not edit by hand ---------------------
ZONES_VERSION = "2025b"
_ZONES = {
    "Europe/Helsinki": (
        "VFppZjIAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAQAAAAEAAAAAAAAAVFpp"
        "ZjIAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAjAAAABAAAABH/////U7omm/////+k"
        "c28b/////8vOUWD/////zMDlYAAAAAAVI92AAAAAABYTzoAAAAAAFwO/gAAAAAAX87CAAAAA"
        "ABjjr5AAAAAAGdOgkAAAAAAaw5GQAAAAABu8vRAAAAAAHKyuEAAAAAAdnJ8QAAAAAB6MkBAA"
        "AAAAH3yBEAAAAAAgbHIQAAAAACFcYxAAAAAAIkxUEAAAAAAjPEUQAAAAACQsNhAAAAAAJRwn"
        "EAAAAAAmDBgQAAAAACcFQ5AAAAAAJ/U0kAAAAAAo5SWQAAAAACnVFpAAAAAAKsUHkAAAAAAr"
        "tPiQAAAAACyk6ZAAAAAALZTakAAAAAAuhMuQAAAAAC90vJAAAAAAMGStkAAAAAAxXdkQAQMC"
        "AwIDAgMCAwIDAgMCAwIDAgMCAwIDAgMCAwIDAgMCAwIAABdlAAAAABdlAAQAACowAQgAABwg"
        "AA1MTVQASE1UAEVFU1QARUVUAApFRVQtMkVFU1QsTTMuNS4wLzMsTTEwLjUuMC80Cg=="),
    "Europe/Athens": (
        "VFppZjIAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAQAAAAEAAAAAAAAAVFpp"
        "ZjIAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA3AAAABgAAABr/////dD+YRP////+b"
        "gCGA/////7l86eD/////ucav0P/////J8mPg/////8oQqFD/////zOdLEP/////Nqkzw////"
        "/86iGOD/////z5NpcP/////fE55g/////9+3ClAAAAAACexeYAAAAAALGPRgAAAAAAvNrgAA"
        "AAAADL2fAAAAAAANpFWAAAAAAA6MXYAAAAAAD4Q3gAAAAAAQavwQAAAAABFke/AAAAAAElKq"
        "8AAAAAATRoJgAAAAABQzwlAAAAAAFSPrkAAAAAAWE9yQAAAAABcDzZAAAAAAF/O+kAAAAAAY"
        "46+QAAAAABnToJAAAAAAGsORkAAAAAAbvL0QAAAAABysrhAAAAAAHZyfEAAAAAAejJAQAAAA"
        "AB98gRAAAAAAIGxyEAAAAAAhXGMQAAAAACJMVBAAAAAAIzxFEAAAAAAkLDYQAAAAACUcJxAA"
        "AAAAJgwYEAAAAAAnBUOQAAAAACf1NJAAAAAAKOUlkAAAAAAp1RaQAAAAACrFB5AAAAAAK7T4"
        "kAAAAAAspOmQAAAAAC2U2pAAAAAALoTLkAAAAAAvdLyQAAAAADBkrZAAAAAAMV3ZEAEDAgMC"
        "BQQFBAMCAwIDAgMCAwIDAgMCAwIDAgMCAwIDAgMCAwIDAgMCAwIDAgMCAwIDAgMCAwIAABY8"
        "AAAAABY8AAQAACowAQgAABwgAA0AAA4QABEAABwgARVMTVQAQU1UAEVFU1QARUVUAENFVABD"
        "RVNUAApFRVQtMkVFU1QsTTMuNS4wLzMsTTEwLjUuMC80Cg=="),
    "Europe/Kyiv": (
        "VFppZjIAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAQAAAAEAAAAAAAAAVFpp"
        "ZjIAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAnAAAACAAAACL/////VrbHZP////+q"
        "Gadk/////7WkGWD/////ys0u0P/////M50sQ/////82pF5D/////zqJDEP/////OzahwAAAA"
        "ABUnp9AAAAAAFhjcQAAAAAAXCNtQAAAAABf6D8AAAAAAGOoO0AAAAAAZ20NAAAAAABrMk9AA"
        "AAAAG7yg8AAAAAAcrJHwAAAAAB2cgvAAAAAAHoxz8AAAAAAffGTwAAAAACBsVfAAAAAAIVxG"
        "8AAAAAAiTDfwAAAAACM8KPAAAAAAJCwZ8AAAAAAlHArwAAAAACYL+/AAAAAAJo0g4AAAAAAo"
        "5ReAAAAAACnVCIAAAAAAKsT5gAAAAAArtOqAAAAAACyk24AAAAAALZTMgAAAAAAuhL2AAAAA"
        "AC90roAAAAAAMGSfgAAAAAAxXcsAAAAAADGWUVABAgMFBAUEAwYDBgMGAwYDBgMGAwYDBgMG"
        "AwYHAgcCBwIHAgcCBwcAABycAAAAABycAAQAABwgAAgAACowAAwAAA4QABAAABwgARQAADhA"
        "ARkAACowAR1MTVQAS01UAEVFVABNU0sAQ0VUAENFU1QATVNEAEVFU1QACkVFVC0yRUVTVCxN"
        "My41LjAvMyxNMTAuNS4wLzQK"),
    "Europe/Berlin": (
        "VFppZjIAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAQAAAAEAAAAAAAAAVFpp"
        "ZjIAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA8AAAABAAAABL/////b6Jh+P////+b"
        "DBdg/////5vV2vD/////nNmukP////+dpLWQ/////565kJD/////n4SXkP/////ICXGQ////"
        "/8znSxD/////zakXkP/////OokMQ/////8+SNBD/////0IIlEP/////RchYQ/////9G2lgD/"
        "////0li+gP/////SoU8Q/////9NjG5D/////1EsjkP/////VOdEg/////9Vn55D/////1ahz"
        "AP/////WKbQQ/////9csGhD/////2AmWEP/////ZAsGQ/////9npeBAAAAAAE01EEAAAAAAU"
        "M/qQAAAAABUj65AAAAAAFhPckAAAAAAXA82QAAAAABfzvpAAAAAAGOOvkAAAAAAZ06CQAAAA"
        "ABrDkZAAAAAAG7y9EAAAAAAcrK4QAAAAAB2cnxAAAAAAHoyQEAAAAAAffIEQAAAAACBschAA"
        "AAAAIVxjEAAAAAAiTFQQAAAAACM8RRAAAAAAJCw2EAAAAAAlHCcQAAAAACYMGBAAAAAAJwVD"
        "kAAAAAAn9TSQAAAAACjlJZAAAAAAKdUWkAAAAAAqxQeQAAAAACu0+JAAAAAALKTpkAAAAAAt"
        "lNqQAAAAAC6Ey5AAAAAAL3S8kAAAAAAwZK2QAAAAADFd2RACAQIBAgECAQIBAgECAQMBAgEC"
        "AQMBAgECAQIBAgECAQIBAgECAQIBAgECAQIBAgECAQIBAgECAQIBAgEAAAyIAAAAABwgAQQA"
        "AA4QAAkAACowAQ1MTVQAQ0VTVABDRVQAQ0VNVAAKQ0VULTFDRVNULE0zLjUuMCxNMTAuNS4w"
        "LzMK"),
    "Europe/Paris": (
        "VFppZjIAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAQAAAAEAAAAAAAAAVFpp"
        "ZjIAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABlAAAABwAAAB//////a8mbz/////+R"
        "YFBP/////5tHePD/////m9cscP////+cvJFw/////53ASPD/////non+cP////+foCrw////"
        "/6BgpfD/////oYAM8P////+iLhLw/////6N6TPD/////pDWB8P////+lXiNw/////6YlNfD/"
        "////pyeb8P////+oWCZw/////6kHffD/////qe40cP////+q51/w/////6vXUPD/////rMdB"
        "8P////+tyafw/////66nI/D/////r6BPcP////+whwXw/////7GJa/D/////snAicP////+z"
        "cohw/////7RQBHD/////tUkv8P////+2L+Zw/////7cyTHD/////uA/IcP////+4/7lw////"
        "/7nvqnD/////utZg8P////+72Mbw/////7zIt/D/////vbio8P////++n19w/////7+YivD/"
        "////wJrw8P/////BeGzw/////8JoXfD/////w1hO8P/////EPwVw/////8U4MPD/////xjqW"
        "8P/////HWKxw/////8faCaD/////yGwn4P/////M50sQ/////82pF5D/////zqJDEP/////P"
        "kjQQ/////9BP4eD/////0Inx8P/////RchYQ/////9JOQJAAAAAAC7s5AAAAAAAMqxvwAAAA"
        "AA2kY5AAAAAADosaEAAAAAAPhEWQAAAAABB0NpAAAAAAEWQnkAAAAAASVBiQAAAAABNNRBAA"
        "AAAAFDP6kAAAAAAVI+uQAAAAABYT3JAAAAAAFwPNkAAAAAAX876QAAAAABjjr5AAAAAAGdOg"
        "kAAAAAAaw5GQAAAAABu8vRAAAAAAHKyuEAAAAAAdnJ8QAAAAAB6MkBAAAAAAH3yBEAAAAAAg"
        "bHIQAAAAACFcYxAAAAAAIkxUEAAAAAAjPEUQAAAAACQsNhAAAAAAJRwnEAAAAAAmDBgQAAAA"
        "ACcFQ5AAAAAAJ/U0kAAAAAAo5SWQAAAAACnVFpAAAAAAKsUHkAAAAAArtPiQAAAAACyk6ZAA"
        "AAAALZTakAAAAAAuhMuQAAAAAC90vJAAAAAAMGStkAAAAAAxXdkQAQMCAwIDAgMCAwIDAgMC"
        "AwIDAgMCAwIDAgMCAwIDAgMCAwIDAgMCAwIDAgMCAwIDAgMCBQQFBAUGAgYEBQQFBAUEBQQF"
        "BAUEBQQFBAUEBQQFBAUEBQQFBAUEBQQFBAUEBQQFBAUAAAIxAAAAAAIxAAQAAA4QAQgAAAAA"
        "AA0AAA4QABEAABwgARUAABwgARpMTVQAUE1UAFdFU1QAV0VUAENFVABDRVNUAFdFTVQACkNF"
        "VC0xQ0VTVCxNMy41LjAsTTEwLjUuMC8zCg=="),
    "Europe/Zurich": (
        "VFppZjIAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAQAAAAEAAAAAAAAAVFpp"
        "ZjIAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAlAAAABAAAABH/////JPDqgP////9x"
        "1AaG/////8oXagD/////yuJxAP/////L90wA/////8zCUwAAAAAAFSPrkAAAAAAWE9yQAAAA"
        "ABcDzZAAAAAAF/O+kAAAAAAY46+QAAAAABnToJAAAAAAGsORkAAAAAAbvL0QAAAAABysrhAA"
        "AAAAHZyfEAAAAAAejJAQAAAAAB98gRAAAAAAIGxyEAAAAAAhXGMQAAAAACJMVBAAAAAAIzxF"
        "EAAAAAAkLDYQAAAAACUcJxAAAAAAJgwYEAAAAAAnBUOQAAAAACf1NJAAAAAAKOUlkAAAAAAp"
        "1RaQAAAAACrFB5AAAAAAK7T4kAAAAAAspOmQAAAAAC2U2pAAAAAALoTLkAAAAAAvdLyQAAAA"
        "ADBkrZAAAAAAMV3ZEAEDAgMCAwIDAgMCAwIDAgMCAwIDAgMCAwIDAgMCAwIDAgMCAwIAAAgA"
        "AAAAAAb6AAQAABwgAQgAAA4QAA1MTVQAQk1UAENFU1QAQ0VUAApDRVQtMUNFU1QsTTMuNS4w"
        "LE0xMC41LjAvMwo="),
    "Europe/London": (
        "VFppZjIAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAQAAAAEAAAAAAAAAVFpp"
        "ZjIAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAACfAAAABQAAABH/////Gl0Jy/////+b"
        "Jq2g/////5vWBSD/////nM8woP////+dpMOg/////56cnaD/////n5caoP////+ghbog////"
        "/6F2/KD/////omWcIP////+je8ig/////6ROuKD/////pT/7IP////+mJWAg/////6cnxiD/"
        "////qCosIP////+o6/ig/////6oA06D/////qtUVIP////+r6fAg/////6zHbCD/////rcnS"
        "IP////+up04g/////6+geaD/////sIcwIP////+xktCg/////7JwTKD/////s3KyoP////+0"
        "UC6g/////7VJWiD/////tjAQoP////+3Mnag/////7gP8qD/////uRJYoP////+579Sg////"
        "/7rpACD/////u9jxIP////+821cg/////7240yD/////vrH+oP////+/mLUg/////8CbGyD/"
        "////wXiXIP/////Cev0g/////8NYeSD/////xFGkoP/////FOFsg/////8Y6wSD/////x1jW"
        "oP/////H2gmg/////8oWJpD/////ypdZkP/////L0R6Q/////8x3O5D/////zbEAkP/////O"
        "YFgQ/////8+Q4pD/////0G5ekP/////RchYQ/////9H7MhD/////0mn+IP/////TYymg////"
        "/9RJ4CD/////1R4hoP/////VQv2Q/////9Xf4BD/////1k6sIP/////W/gOg/////9gujiD/"
        "////2PmVIP/////aDnAg/////9rr7CD/////2+UXoP/////cy84g/////93E+aD/////3rTq"
        "oP/////frhYg/////+CUzKD/////4XJIoP/////ia3Qg/////+NSKqD/////5FSQoP/////l"
        "Mgyg/////+Y9rSD/////5xspIP/////oFFSg/////+j7CyD/////6f1xIP/////q2u0g////"
        "/+vdUyD/////7LrPIP/////ts/qg/////+6asSD/////74FnoP/////wn30g//////FhSaD/"
        "////8n9fIP/////zSmYg//////RfQSD/////9SENoP/////2PyMg//////cA76D/////+B8F"
        "IP/////44NGg//////n+5yD/////+sCzoP/////76AOg//////x7q6D//////ce7cAAAAAAD"
        "cMYgAAAAAAQpWCAAAAAABVCoIAAAAAAGCTogAAAAAAcwiiAAAAAAB+kcIAAAAAAJEGwgAAAA"
        "AAnI/iAAAAAACvBOIAAAAAALshqgAAAAAAzQMCAAAAAADZH8oAAAAAAOsBIgAAAAAA9x3qAA"
        "AAAAEJkuoAAAAAARUcCgAAAAABJ5EKAAAAAAEzGioAAAAAAUWPKgAAAAABUj65AAAAAAFjjG"
        "kAAAAAAXA82QAAAAABgYqJAAAAAAGOOvkAAAAAAZ+IqQAAAAABrDkZAAAAAAG+GnEAAAAAAc"
        "rK4QAAAAAB3BiRAAAAAAHoyQEAAAAAAfoWsQAAAAACBschAAAAAAIYFNEAAAAAAiTFQQAAAA"
        "ACNhLxAAAAAAJCw2EAAAAAAlSkuQAAAAACYMGBAAAAAAJyotkAAAAAAn9TSQAAAAACkKD5AA"
        "AAAAKdUWkAAAAAAq6fGQAAAAACu0+JAAAAAALMnTkAAAAAAtlNqQAAAAAC6ptZAAAAAAL3S8"
        "kAAAAAAwiZeQAAAAADDnJAACAQIBAgECAQIBAgECAQIBAgECAQIBAgECAQIBAgECAQIBAgEC"
        "AQIBAgECAQIBAgECAQMBAwEDAQMBAwECAQIBAwECAQIBAgECAQIBAgECAQIBAgECAQIBAgEC"
        "AQIBAgECAQIBAgECAQIBAgEEAgECAQIBAgECAQIBAgECAQIBAgECAQIBAgECAQIBAgECAQIB"
        "AgECAQIBAgECAQIBAgL///+1AAAAAA4QAQQAAAAAAAgAABwgAQwAAA4QAARMTVQAQlNUAEdN"
        "VABCRFNUAApHTVQwQlNULE0zLjUuMC8xLE0xMC41LjAK"),
    "Europe/Moscow": (
        "VFppZjIAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAQAAAAEAAAAAAAAAVFpp"
        "ZjIAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABOAAAACwAAACb/////VrbAx/////+b"
        "Xx7H/////50+8nn/////niru+f////+e9zlp/////5+EV/n/////oNhs6f////+hADmA////"
        "/6E8pkD/////pBBtwP////+kPTKw/////6UVaLD/////pT0DwP////+nHkVQ/////7WkGWAA"
        "AAAAFSen0AAAAAAWGNxAAAAAABcI21AAAAAAF/oPwAAAAAAY6g7QAAAAABnbQ0AAAAAAGsyT"
        "0AAAAAAbvKDwAAAAAByskfAAAAAAHZyC8AAAAAAejHPwAAAAAB98ZPAAAAAAIGxV8AAAAAAh"
        "XEbwAAAAACJMN/AAAAAAIzwo8AAAAAAkLBnwAAAAACUcCvAAAAAAJgv78AAAAAAnBSdwAAAA"
        "ACf1GHAAAAAAKOUXgAAAAAApeL+AAAAAACnU+nAAAAAAKsTrcAAAAAArtNxwAAAAACykzXAA"
        "AAAALZS+cAAAAAAuhK9wAAAAAC90oHAAAAAAMGSRcAAAAAAxXbzwAAAAADJyl/AAAAAAMz2e"
        "8AAAAAA0UnnwAAAAADUdgPAAAAAANjJb8AAAAAA2/WLwAAAAADgbeHAAAAAAON1E8AAAAAA5"
        "+1pwAAAAADq9JvAAAAAAO9s8cAAAAAA8pkNwAAAAAD27HnAAAAAAPoYlcAAAAAA/mwBwAAAA"
        "AEBmB3AAAAAAQYQc8AAAAABCRelwAAAAAENj/vAAAAAARCXLcAAAAABFQ+DwAAAAAEYFrXAA"
        "AAAARyPC8AAAAABH7snwAAAAAEkDpPAAAAAASc6r8AAAAABK44bwAAAAAEuujfAAAAAATMyj"
        "cAAAAABNjm/wAAAAAFRMHWABAwIDBAIEBQYFBwUGCAYFBgUGBQYFBgUGBQYFBgUGBQYFBgkI"
        "BgUGBQYFBgUGBQYFBgUGBQYFBgUGBQYFBgUGBQYFBgUGBQYFBgUGCgYAACM5AAAAACM5AAQA"
        "ADGHAQgAACN3AAQAAD+XAQwAADhAAREAACowABUAAEZQARkAABwgAB0AACowASEAADhAABVM"
        "TVQATU1UAE1TVABNRFNUAE1TRABNU0sAKzA1AEVFVABFRVNUAApNU0stMwo="),
    "America/New_York": (
        "VFppZjIAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAQAAAAEAAAAAAAAAVFpp"
        "ZjIAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAACvAAAABQAAABT/////XgPwkP////+e"
        "ph5w/////5+662D/////oIYAcP////+hms1g/////6Jl4nD/////o4Pp4P////+kaq5w////"
        "/6U1p2D/////plPK8P////+nFYlg/////6gzrPD/////qP6l4P////+qE47w/////6reh+D/"
        "////q/Nw8P////+svmng/////63TUvD/////rp5L4P////+vszTw/////7B+LeD/////sZxR"
        "cP////+yZ0pg/////7N8M3D/////tEcsYP////+1XBVw/////7YnDmD/////tzv3cP////+4"
        "BvBg/////7kb2XD/////uebSYP////+7BPXw/////7vGtGD/////vOTX8P////+9r9Dg////"
        "/77EufD/////v4+y4P/////ApJvw/////8FvlOD/////woR98P/////DT3bg/////8RkX/D/"
        "////xS9Y4P/////GTXxw/////8cPOuD/////yC1ecP/////I+Fdg/////8oNQHD/////ytg5"
        "YP/////LiPBw/////9Ij9HD/////0mD74P/////TdeTw/////9RA3eD/////1VXG8P/////W"
        "IL/g/////9c1qPD/////2ACh4P/////ZFYrw/////9ngg+D/////2v6ncP/////bwGXg////"
        "/9zeiXD/////3amCYP/////evmtw/////9+JZGD/////4J5NcP/////haUZg/////+J+L3D/"
        "////40koYP/////kXhFw/////+VXLuD/////5kct8P/////nNxDg/////+gnD/D/////6Rby"
        "4P/////qBvHw/////+r21OD/////6+bT8P/////s1rbg/////+3GtfD/////7r/TYP/////v"
        "r9Jw//////CftWD/////8Y+0cP/////yf5dg//////NvlnD/////9F95YP/////1T3hw////"
        "//Y/W2D/////9y9acP/////4KHfg//////kPPHD/////+ghZ4P/////6+Fjw//////voO+D/"
        "/////Ng68P/////9yB3g//////64HPD//////6f/4AAAAAAAl/7wAAAAAAGH4eAAAAAAAnfg"
        "8AAAAAADcP5gAAAAAARg/XAAAAAABVDgYAAAAAAGQN9wAAAAAAcwwmAAAAAAB40ZcAAAAAAJ"
        "EKRgAAAAAAmtlPAAAAAACvCGYAAAAAAL4IVwAAAAAAzZouAAAAAADcBncAAAAAAOuYTgAAAA"
        "AA+pg/AAAAAAEJlm4AAAAAARiWXwAAAAABJ5SOAAAAAAE2lH8AAAAAAUWSrgAAAAABVJKfAA"
        "AAAAFjkM4AAAAAAXKQvwAAAAABgiKWAAAAAAGQjt8AAAAAAaAgtgAAAAABryCnAAAAAAG+Ht"
        "YAAAAAAc0exwAAAAAB3Bz2AAAAAAHrHOcAAAAAAfobFgAAAAACB2APAAAAAAIYGTYAAAAAAi"
        "VeLwAAAAACNqr+AAAAAAJDXE8AAAAAAlSpHgAAAAACYVpvAAAAAAJypz4AAAAAAn/sNwAAAA"
        "ACkKVeAAAAAAKd6lcAAAAAAq6jfgAAAAACu+h3AAAAAALNNUYAAAAAAtnmlwAAAAAC6zNmAA"
        "AAAAL35LcAAAAAAwkxhgAAAAADFnZ/AAAAAAMnL6YAAAAAAzR0nwAAAAADRS3GAAAAAANScr"
        "8AAAAAA2Mr5gAAAAADcHDfAAAAAAOBva4AAAAAA45u/wAAAAADn7vOAAAAAAOsbR8AAAAAA7"
        "257gAAAAADyv7nAAAAAAPbuA4AAAAAA+j9BwAAAAAD+bYuAAAAAAQG+ycAAAAABBhH9gAAAA"
        "AEJPlHAAAAAAQ2RhYAAAAABEL3ZwAAAAAEVEQ2AAAAAARfOo8AIBAgECAQIBAgECAQIBAgEC"
        "AQIBAgECAQIBAgECAQIBAgECAQIBAgECAQIBAgECAQIDBAIBAgECAQIBAgECAQIBAgECAQIB"
        "AgECAQIBAgECAQIBAgECAQIBAgECAQIBAgECAQIBAgECAQIBAgECAQIBAgECAQIBAgECAQIB"
        "AgECAQIBAgECAQIBAgECAQIBAgECAQIBAgECAQIBAgECAQIBAgECAQIBAgECAQIBAgH//7qe"
        "AAD//8fAAQT//7mwAAj//8fAAQz//8fAARBMTVQARURUAEVTVABFV1QARVBUAApFU1Q1RURU"
        "LE0zLjIuMCxNMTEuMS4wCg=="),
    "America/Chicago": (
        "VFppZjIAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAQAAAAEAAAAAAAAAVFpp"
        "ZjIAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAACvAAAABgAAABj/////XgP+oP////+e"
        "piyA/////5+6+XD/////oIYOgP////+hmttw/////6LLdAD/////o4P38P////+kRdKA////"
        "/6Vj2fD/////plPZAP////+nFZdw/////6gzuwD/////qP6z8P////+qE50A/////6relfD/"
        "////q/N/AP////+svnfw/////63TYQD/////rp5Z8P////+vs0MA/////7B+O/D/////sZxf"
        "gP////+yZ1hw/////7N8QYD/////tEc6cP////+1XCOA/////7YnHHD/////tzwFgP////+4"
        "Bv5w/////7kb54D/////uebgcP////+7BQQA/////7vGwnD/////vOTmAP////+9r97w////"
        "/77EyAD/////v4/A8P/////AWtYA/////8GwPHD/////woSMAP/////DT4Tw/////8RkbgD/"
        "////xS9m8P/////GTYqA/////8cPSPD/////yC1sgP/////I+GVw/////8oNToD/////ythH"
        "cP/////LiP6A/////9Ij9HD/////0mEJ8P/////TdfMA/////9RA6/D/////1VXVAP/////W"
        "IM3w/////9c1twD/////2ACv8P/////ZFZkA/////9ngkfD/////2v61gP/////bwHPw////"
        "/9zel4D/////3amQcP/////evnmA/////9+JcnD/////4J5bgP/////haVRw/////+J+PYD/"
        "////40k2cP/////kXh+A/////+VXPPD/////5kc8AP/////nNx7w/////+gnHgD/////6RcA"
        "8P/////qBwAA/////+r24vD/////6+biAP/////s1sTw/////+3GxAD/////7r/hcP/////v"
        "r+CA//////Cfw3D/////8Y/CgP/////yf6Vw//////NvpID/////9F+HcP/////1T4aA////"
        "//Y/aXD/////9y9ogP/////4KIXw//////kPSoD/////+ghn8P/////6+GcA//////voSfD/"
        "/////NhJAP/////9yCvw//////64KwD//////6gN8AAAAAAAmA0AAAAAAAGH7/AAAAAAAnfv"
        "AAAAAAADcQxwAAAAAARhC4AAAAAABVDucAAAAAAGQO2AAAAAAAcw0HAAAAAAB40ngAAAAAAJ"
        "ELJwAAAAAAmtowAAAAAACvCUcAAAAAAL4JOAAAAAAAzZsPAAAAAADcB1gAAAAAAOuZLwAAAA"
        "AA+pkgAAAAAAEJl08AAAAAARiXQAAAAAABJ5VvAAAAAAE2lWAAAAAAAUWTjwAAAAABVJOAAA"
        "AAAAFjka8AAAAAAXKRoAAAAAABgiN3AAAAAAGQj8AAAAAAAaAhlwAAAAABryGIAAAAAAG+H7"
        "cAAAAAAc0fqAAAAAAB3B3XAAAAAAHrHcgAAAAAAfob9wAAAAACB2DwAAAAAAIYGhcAAAAAAi"
        "VfEAAAAAACNqvfAAAAAAJDXTAAAAAAAlSp/wAAAAACYVtQAAAAAAJyqB8AAAAAAn/tGAAAAA"
        "ACkKY/AAAAAAKd6zgAAAAAAq6kXwAAAAACu+lYAAAAAALNNicAAAAAAtnneAAAAAAC6zRHAA"
        "AAAAL35ZgAAAAAAwkyZwAAAAADFndgAAAAAAMnMIcAAAAAAzR1gAAAAAADRS6nAAAAAANSc6"
        "AAAAAAA2MsxwAAAAADcHHAAAAAAAOBvo8AAAAAA45v4AAAAAADn7yvAAAAAAOsbgAAAAAAA7"
        "26zwAAAAADyv/IAAAAAAPbuO8AAAAAA+j96AAAAAAD+bcPAAAAAAQG/AgAAAAABBhI1wAAAA"
        "AEJPooAAAAAAQ2RvcAAAAABEL4SAAAAAAEVEUXAAAAAARfO3AAIBAgECAQIBAgECAQIBAgEC"
        "AQIBAgECAQIBAgECAQIBAgECAQIDAgECAQIBAgECAQIEBQIBAgECAQIBAgECAQIBAgECAQIB"
        "AgECAQIBAgECAQIBAgECAQIBAgECAQIBAgECAQIBAgECAQIBAgECAQIBAgECAQIBAgECAQIB"
        "AgECAQIBAgECAQIBAgECAQIBAgECAQIBAgECAQIBAgECAQIBAgECAQIBAgECAQIBAgH//63U"
        "AAD//7mwAQT//6ugAAj//7mwAAz//7mwARD//7mwARRMTVQAQ0RUAENTVABFU1QAQ1dUAENQ"
        "VAAKQ1NUNkNEVCxNMy4yLjAsTTExLjEuMAo="),
    "Asia/Tokyo": (
        "VFppZjIAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAQAAAAEAAAAAAAAAVFpp"
        "ZjIAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAJAAAAAwAAAAz/////ZcKkcP/////X"
        "PgJw/////9ftWfD/////2Pj6cP/////ZzTvw/////9sHAPD/////260d8P/////c5uLw////"
        "/92M//ACAQIBAgECAQIAAIMDAAAAAIygAQQAAH6QAAhMTVQASkRUAEpTVAAKSlNULTkK"),
    "Asia/Hong_Kong": (
        "VFppZjIAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAQAAAAEAAAAAAAAAVFpp"
        "ZjIAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABFAAAABQAAABb/////hWljkP/////K"
        "TTEw/////8rbkzD/////y0txeP/////SoN6Q/////9Nr14D/////1JNYuP/////VQrA4////"
        "/9ZzOrj/////1z5BuP/////YLjK4/////9j5Obj/////2g4UuP/////a2Ru4/////9vt9rj/"
        "////3Lj9uP/////dzdi4/////96iGjj/////37b1OP/////ggfw4/////+GWySj/////4k9p"
        "OP/////jdqso/////+QvSzj/////5V/HqP/////mDy04/////+c/qaj/////5/hJuP/////p"
        "H4uo/////+nYK7j/////6v9tqP/////ruA24/////+zfT6j/////7ZfvuP/////uyGwo////"
        "/+930bj/////8KhOKP/////xV7O4//////KIMCj/////80DQOP/////0aBIo//////Ugsjj/"
        "////9kf0KP/////3JX44//////gVYSj/////+QVgOP/////59UMo//////rlQjj/////+95f"
        "qP/////8zl64//////2+Qaj//////q5AuP//////niOoAAAAAACOIrgAAAAAAX4FqAAAAAAC"
        "bgS4AAAAAANd56gAAAAABE3muAAAAAAFRwQoAAAAAAY3AzgAAAAABybmKAAAAAAHgz04AAAA"
        "AAkGyCgAAAAACfbHOAAAAAAK5qooAAAAAAvWqTgAAAAADMaMKAAAAAARmzk4AAAAABJvbKgB"
        "AgMEAQIBAgECAQIBAgECAQIBAgECAQIBAgECAQIBAgECAQIBAgECAQIBAgECAQIBAgECAQIB"
        "AgECAQIBAgECAQIBAgEAAGsKAAAAAHCAAAQAAH6QAQgAAHeIAQ0AAH6QABJMTVQASEtUAEhL"
        "U1QASEtXVABKU1QACkhLVC04Cg=="),
    "Asia/Singapore": (
        "VFppZjIAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAQAAAAEAAAAAAAAAVFpp"
        "ZjIAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAIAAAACAAAACD/////fjZTo/////+G"
        "g4Wj/////7pnTpD/////wArkYP/////Ks+Vg/////8uRXwj/////0kht8AAAAAAWke4AAQID"
        "BAUGBQcAAGFdAAAAAGFdAAQAAGJwAAgAAGcgAQwAAGcgAAwAAGl4ABIAAH6QABgAAHCAABxM"
        "TVQAU01UACswNwArMDcyMAArMDczMAArMDkAKzA4AAo8KzA4Pi04Cg=="),
    "Australia/Sydney": (
        "VFppZjIAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAQAAAAEAAAAAAAAAVFpp"
        "ZjIAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABTAAAAAwAAAA7/////cxZ/PP////+c"
        "TsKA/////5y8LwD/////y1SzAP/////Lx2WA/////8y3VoD/////zadHgP/////OoHMA////"
        "/8+HKYAAAAAAA3A5gAAAAAAEDRwAAAAAAAVQG4AAAAAABfY4gAAAAAAHL/2AAAAAAAfWGoAA"
        "AAAACQ/fgAAAAAAJtfyAAAAAAArvwYAAAAAAC58ZAAAAAAAM2N4AAAAAAA1++wAAAAAADrjA"
        "AAAAAAAPXt0AAAAAABCYogAAAAAAET6/AAAAAAASeIQAAAAAABMeoQAAAAAAFFhmAAAAAAAU"
        "/oMAAAAAABY4SAAAAAAAFwyJgAAAAAAYIWSAAAAAABjHgYAAAAAAGgFGgAAAAAAap2OAAAAA"
        "ABvhKIAAAAAAHIdFgAAAAAAdwQqAAAAAAB55nIAAAAAAH5eyAAAAAAAgWX6AAAAAACGAzoAA"
        "AAAAIkKbAAAAAAAjaesAAAAAACQifQAAAAAAJUnNAAAAAAAl7+oAAAAAACcprwAAAAAAJ8/M"
        "AAAAAAApCZEAAAAAACmvrgAAAAAAKulzAAAAAAArmMqAAAAAACzSj4AAAAAALXisgAAAAAAu"
        "snGAAAAAAC9YjoAAAAAAMJJTgAAAAAAxXVqAAAAAADJyNYAAAAAAMz08gAAAAAA0UheAAAAA"
        "ADUdHoAAAAAANjH5gAAAAAA2/QCAAAAAADgbFgAAAAAAONzigAAAAAA5p+mAAAAAADq8xIAA"
        "AAAAO9raAAAAAAA8peEAAAAAAD26vAAAAAAAPoXDAAAAAAA/mp4AAAAAAEBlpQAAAAAAQYO6"
        "gAAAAABCRYcAAAAAAENjnIAAAAAARC6jgAAAAABFQ36AAAAAAEYFSwAAAAAARyNggAAAAABH"
        "96IAAgECAQIBAgECAQIBAgECAQIBAgECAQIBAgECAQIBAgECAQIBAgECAQIBAgECAQIBAgEC"
        "AQIBAgECAQIBAgECAQIBAgECAQIBAgECAQIBAgECAQIAAI3EAAAAAJqwAQQAAIygAAlMTVQA"
        "QUVEVABBRVNUAApBRVNULTEwQUVEVCxNMTAuMS4wLE00LjEuMC8zCg=="),
}
# -- end of the carried zone rules -------------------------------------------
if __name__ == "__main__":
    main()
