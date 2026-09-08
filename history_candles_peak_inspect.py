# history_candles_peak_inspect.py
"""
LLM-friendly candle-history browser & limit-order level tester for the T-Trader
sandbox. Use it to (a) find the most significant price peaks of a ticker,
(b) understand the fine (1M/5M) structure of a peak, and (c) answer the
question that matters for the trading strategy in t_order_manager.py:

    "If my resting limit order sits at price X / at MA(6) +/- P%, would it
     have fired during this period, and how deep did the market go past it?"

The strategy places limit brackets a small band away from the MA(6) of 5M
candles (SELL above, BUY below). Whether an order fires is a property of the
candles: a candle whose HIGH reaches your resting SELL price (or whose LOW
reaches your resting BUY price) means the market traded through your level.
Use --profile to choose the band width and --level/--band to verify a level
against history before you trust the live brackets with it.

=============================================================================
QUICK START / WORKFLOW FOR AN LLM
=============================================================================
1. FIND PEAKS (scan, default). Top-N candles with the largest deviation of
   their range from a 6-bar SMA, over the requested period:
       poetry run python history_candles_peak_inspect.py --tickers SAFE
   Add --interval D|30M|5M... to scan a different bar size; add --from/--to to
   restrict; --top N to change the count. Each printed peak is followed by a
   ready-to-copy "zoom" command.

2. ZOOM INTO A PEAK at fine detail (1M/5M). Full candle-by-candle table with
   SMA(6) and deviations:
       poetry run python history_candles_peak_inspect.py --tickers SAFE \
           --interval 1M --raw --from 2025-10-30 --to 2025-10-30
   A date-only --from/--to means "the whole day". This shows whether a peak is
   a single 1-minute flash or a multi-minute, high-volume move (tradable).

3. CHOOSE THE BRACKET WIDTH (--profile). Distribution of how far 5M extremes
   stray beyond the *preceding* SMA(6) (this is the level a resting bracket is
   measured against), over the last year by default:
       poetry run python history_candles_peak_inspect.py --tickers SAFE \
           --interval 5M --profile
   Read it as: "with a SELL band of X%, how many candles/days per year would
   the bracket fire, and how far past X% did the market run?" Custom levels:
       ... --profile --bands "0.15 0.5 1 2 5"
   Wider bands fire less often but collect more per event; pick the width from
   the table (see the excursion-profile section in the trade-off notes).

4. TEST A CONCRETE LEVEL (--level or --band). Replay a resting limit over a
   period and list every candle whose range would have filled it, plus how far
   the market ran past your level (the profit/risk you leave on the table):
       # would a resting SELL at 16.60 have fired during the 2025-10-20 spike?
       poetry run python history_candles_peak_inspect.py --tickers SAFE \
           --interval 5M --side sell --level 16.60 \
           --from 2025-10-19 --to 2025-10-22
       # same, but the level = MA(6) + 0.5% (as the live brackets do)
       poetry run python history_candles_peak_inspect.py --tickers SAFE \
           --interval 5M --side sell --band 0.5 \
           --from 2025-10-19 --to 2025-10-22
   For a dip (BUY bracket below MA) pass --side buy.

=============================================================================
ALL OPTIONS
=============================================================================
  --tickers T1 [T2...]   Tickers to analyse (default: SAFE).
  --interval IV           Bar size used for scanning AND zooming AND testing:
                          1M 2M 3M 5M 10M 15M 30M H 2H 4H D (default D).
  --from YYYY-MM-DD[ HH:MM:SS]   Start of period, inclusive (default: 1y back).
  --to   YYYY-MM-DD[ HH:MM:SS]   End of period, inclusive (default: now).
  --detail-interval IV    Bar size suggested in each peak's zoom command
                          (default 5M); does not affect computation.
  --raw                  ZOOM mode: print every candle of the period (OHLC +
                         SMA6 + deviations + volume) instead of the top peaks.
  --top N                How many peaks to report in scan mode (default 10).
  --profile              Print the excursion distribution of candle extremes
                         vs the preceding SMA(6) (use with --interval).
  --bands "P1 P2 ..."    Threshold percentages for --profile (default
                         "0.15 0.3 0.5 1 2 3 5 10 20").
  --level PRICE          Test a resting limit at this absolute price over the
                         period (see --side).
  --band PCT             Test a resting limit at SMA(6) +/- PCT% over the
                         period (level moves with the MA, like the live
                         brackets). Mutually exclusive with --level.
  --side buy|sell        Which resting order the test assumes (default sell).
                         sell  -> fires when a candle HIGH >= level (price rose
                                  to your sell; you get filled at the level)
                         buy   -> fires when a candle LOW  <= level
  --tick STEP            Price step for rounding band levels (default 0.01).
  --cap N                Max matched candles to print in a level test
                         (default 500; the summary counts are always full).

=============================================================================
NOTES / LIMITS
=============================================================================
* Data: sandbox (T_INVEST_TOKEN_SANDBOX). 1M candles are the finest history
  available; the tick tape endpoint returns nothing in the sandbox, so a
  candle whose range straddles your level with meaningful volume is the best
  possible "would it fill?" evidence.
* The fetcher never asks for more than the API allows (1000 candles/request)
  and never drops data: ranges are fetched in calendar slices that fit, run
  concurrently, and split if a slice comes back full. A full year of 5M
  (~33k candles) takes ~40s because the sandbox generates candles slowly;
  narrow zoom windows take under a second of fetching. Prefer --from/--to.
* Runtimes are dominated by t_tech import + ticker resolution (~4s fixed).
* "Preceding SMA(6)" = average of the 6 closes BEFORE a candle, i.e. the level
  a resting order would have been sitting at while that candle traded. Peaks
  in scan mode instead use the SMA that includes the candle (indicator view).
"""

import argparse
import asyncio
import os
from datetime import datetime, timezone, timedelta
from decimal import Decimal

from dotenv import load_dotenv
from t_tech.invest.sandbox.async_client import AsyncSandboxClient
from t_tech.invest.exceptions import AioRequestError
from t_tech.invest import CandleInterval
from t_tech.invest.utils import now, candle_interval_to_timedelta, quotation_to_decimal

from regime import resolve_instrument

# The sandbox refuses/truncates requests that would return more than this.
MAX_PER_REQUEST = 1000
# Recursion splits a slice in two whenever a response hits these limits, so no
# candles can be silently dropped no matter how dense the instrument history is.
MAX_CONCURRENT_REQUESTS = 30

# Conservative calendar-days per request per interval so slices normally stay
# well under the API limit without any splitting (values from measurements:
# ~351 1M / 108 5M / 37 15M / 19 30M candles per day for SAFE).
_SLICE_DAYS = {
    CandleInterval.CANDLE_INTERVAL_1_MIN: 1,
    CandleInterval.CANDLE_INTERVAL_2_MIN: 1,
    CandleInterval.CANDLE_INTERVAL_3_MIN: 1,
    CandleInterval.CANDLE_INTERVAL_5_MIN: 8,
    CandleInterval.CANDLE_INTERVAL_10_MIN: 15,
    CandleInterval.CANDLE_INTERVAL_15_MIN: 20,
    CandleInterval.CANDLE_INTERVAL_30_MIN: 30,
    CandleInterval.CANDLE_INTERVAL_HOUR: 45,
    CandleInterval.CANDLE_INTERVAL_2_HOUR: 60,
    CandleInterval.CANDLE_INTERVAL_4_HOUR: 90,
    CandleInterval.CANDLE_INTERVAL_DAY: 366,
}

_INTERVAL_NAMES = {
    CandleInterval.CANDLE_INTERVAL_1_MIN: "1M",
    CandleInterval.CANDLE_INTERVAL_2_MIN: "2M",
    CandleInterval.CANDLE_INTERVAL_3_MIN: "3M",
    CandleInterval.CANDLE_INTERVAL_5_MIN: "5M",
    CandleInterval.CANDLE_INTERVAL_10_MIN: "10M",
    CandleInterval.CANDLE_INTERVAL_15_MIN: "15M",
    CandleInterval.CANDLE_INTERVAL_30_MIN: "30M",
    CandleInterval.CANDLE_INTERVAL_HOUR: "H",
    CandleInterval.CANDLE_INTERVAL_2_HOUR: "2H",
    CandleInterval.CANDLE_INTERVAL_4_HOUR: "4H",
    CandleInterval.CANDLE_INTERVAL_DAY: "D",
}

DEFAULT_BANDS = ["0.15", "0.3", "0.5", "1", "2", "3", "5", "10", "20"]


def _fmt(d):
    """Format a Decimal price; None renders as a dash."""
    if d is None:
        return "—"
    return f"{d:,.2f}".replace(",", " ")


def _pct(d):
    """Decimal fraction -> '+1.23%' string."""
    if d is None:
        return "—"
    return f"{d * 100:+.2f}%"


def _parse_date(s):
    """Convert ISO date string to timezone-aware UTC datetime."""
    if not s:
        return None
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _parse_interval(s):
    mapping = {v: k for k, v in _INTERVAL_NAMES.items()}
    key = s.upper()
    if key not in mapping:
        raise ValueError(f"Unsupported interval: {s}. Supported: {', '.join(sorted(mapping, key=lambda x: -len(x)))}")
    return mapping[key]


def _format_datetime(dt, interval):
    if interval == CandleInterval.CANDLE_INTERVAL_DAY:
        return dt.strftime("%Y-%m-%d")
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def _median(vals):
    if not vals:
        return None
    s = sorted(vals)
    n = len(s)
    m = n // 2
    if n % 2:
        return s[m]
    return (s[m - 1] + s[m]) / 2


def _pctile(vals, p):
    if not vals:
        return None
    s = sorted(vals)
    k = (len(s) - 1) * p
    lo = int(k)
    hi = min(lo + 1, len(s) - 1)
    return s[lo] + (s[hi] - s[lo]) * (k - lo)


def _to_row(c):
    return {
        "t": c.time,
        "o": quotation_to_decimal(c.open),
        "h": quotation_to_decimal(c.high),
        "l": quotation_to_decimal(c.low),
        "c": quotation_to_decimal(c.close),
        "v": int(c.volume),
    }


async def _try_fetch(client, uid, interval, from_dt, to_dt, sem):
    async def do():
        resp = await client.market_data.get_candles(
            instrument_id=uid,
            from_=from_dt,
            to=to_dt,
            interval=interval,
            limit=MAX_PER_REQUEST,
        )
        return [_to_row(c) for c in resp.candles]

    try:
        async with sem:
            return await do()
    except AioRequestError as e:
        meta = getattr(e, "metadata", None)
        print(f"    [fetch {from_dt}..{to_dt} rejected: {getattr(meta, 'message', e)}]",
              file=os.sys.stderr)
        return None
    except Exception as e:
        print(f"    [fetch {from_dt}..{to_dt} failed: {e}]", file=os.sys.stderr)
        return None


async def _collect_slice(client, uid, interval, start, end, sem):
    """Fetch [start, end) fully. A full/rejected response is halved recursively."""
    if end <= start:
        return []
    rows = await _try_fetch(client, uid, interval, start, end, sem)
    if rows is not None and len(rows) < MAX_PER_REQUEST:
        return rows
    if end - start < timedelta(minutes=5):
        return []
    mid = start + (end - start) / 2
    lo = await _collect_slice(client, uid, interval, start, mid, sem)
    hi = await _collect_slice(client, uid, interval, mid, end, sem)
    return lo + hi


async def fetch_range(client, uid, interval, start, end):
    """Fetch every candle between start and end at the given interval.

    The range is split into non-overlapping calendar slices that fit a single
    request, fetched concurrently, and each slice is recursively halved if the
    API truncates or rejects it. Returns all candles sorted by time.
    """
    step = timedelta(days=_SLICE_DAYS.get(interval, 6))
    sem = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    slices = []
    cursor = start
    while cursor < end:
        nxt = min(cursor + step, end)
        slices.append((cursor, nxt))
        cursor = nxt
    parts = await asyncio.gather(*[
        _collect_slice(client, uid, interval, a, b, sem) for a, b in slices
    ])
    rows = [r for part in parts for r in part]
    return sorted(rows, key=lambda r: r["t"])


def prior_ma6(rows):
    """For every candle, the SMA(6) of the closes BEFORE it (resting level ref).

    Returns a list aligned with rows; entry is None until 6 earlier candles
    exist in the fetched range.
    """
    out = []
    for i in range(len(rows)):
        if i >= 6:
            out.append(sum(rows[j]["c"] for j in range(i - 6, i)) / Decimal(6))
        else:
            out.append(None)
    return out


def add_sma6_columns(rows):
    """Attach SMA(6) of close and its deviations to every row (None until 6 seen)."""
    out = []
    window = []
    for row in rows:
        window.append(row["c"])
        window = window[-6:]
        if len(window) == 6:
            sma = sum(window) / Decimal(6)
            diff_h = row["h"] - sma
            diff_l = row["l"] - sma
            row["sma6"] = sma
            row["diff_h"] = diff_h
            row["diff_l"] = diff_l
            row["abs_diff_max"] = max(abs(diff_h), abs(diff_l))
        else:
            row["sma6"] = row["diff_h"] = row["diff_l"] = row["abs_diff_max"] = None
        out.append(row)
    return out


def print_table(rows, interval):
    print(
        f"{'Date':<20} {'Open':>10} {'High':>10} {'Low':>10} {'Close':>10} "
        f"{'SMA6':>10} {'High-SMA':>10} {'Low-SMA':>10} {'|max|':>10} {'Vol':>9}"
    )
    print("-" * 115)
    for row in rows:
        print(
            f"{_format_datetime(row['t'], interval):<20} "
            f"{_fmt(row['o']):>10} {_fmt(row['h']):>10} {_fmt(row['l']):>10} {_fmt(row['c']):>10} "
            f"{_fmt(row['sma6']):>10} {_fmt(row['diff_h']):>10} {_fmt(row['diff_l']):>10} "
            f"{_fmt(row['abs_diff_max']):>10} {row['v']:>9}"
        )


def zoom_hint(ticker, peak_time, scan_interval, detail_interval):
    """A copy-pasteable command that zooms into the candle around a peak."""
    hw = timedelta(days=1)
    if scan_interval == CandleInterval.CANDLE_INTERVAL_DAY:
        hw = timedelta(days=3)
    frm = (peak_time - hw).strftime("%Y-%m-%d")
    to = (peak_time + hw).strftime("%Y-%m-%d")
    script = os.path.basename(__file__)
    return (f"# zoom into this peak at {_INTERVAL_NAMES.get(detail_interval, '5M')} detail:\n"
            f"poetry run python {script} --tickers {ticker} --interval "
            f"{_INTERVAL_NAMES.get(detail_interval, '5M')} --raw --from {frm} --to {to}")


def run_profile(rows, sel, interval, band_pcts, ma_prev):
    """Excursion distribution of candle extremes vs the preceding SMA(6)."""
    seen = [r for _, r in sel]
    n = len(seen)
    if not n:
        print("  No candles in the selected date range.")
        return
    dev_up = []   # Decimal fraction high/ma - 1 (only bars with a reference MA)
    dev_dn = []
    up_days = {}
    dn_days = {}
    for idx, row in sel:
        ma = ma_prev[idx]
        if ma is None:
            continue
        du = row["h"] / ma - 1
        dd = row["l"] / ma - 1
        dev_up.append(du)
        dev_dn.append(dd)
        up_days.setdefault(row["t"].date(), []).append(du)
        dn_days.setdefault(row["t"].date(), []).append(dd)
    n_ref = len(dev_up)

    print(f"Range: {_format_datetime(seen[0]['t'], interval)} .. {_format_datetime(seen[-1]['t'], interval)} "
          f"({n} {_INTERVAL_NAMES.get(interval, interval)} candles, {n_ref} with a preceding SMA(6))")
    print("Excursion = how far a candle extreme strayed from the SMA(6) that PRECEDES it — "
          "the level a resting bracket is measured against.\n")

    print(f"UP   (candle HIGH vs preceding SMA): bars poked above it = {sum(1 for x in dev_up if x > 0)}")
    print(f"     {'band':<10}{'bars':>8}{'days':>7}{'max':>10}")
    for b in band_pcts:
        bars = sum(1 for x in dev_up if x >= b)
        days = sum(1 for v in up_days.values() if any(x >= b for x in v))
        mx = max((x for x in dev_up if x >= b), default=None)
        print(f"     >{b * 100:<7.2f}%{bars:>8}{days:>7}{_pct(mx) if mx is not None else '—':>10}")
    pos = [float(x) for x in dev_up if x > 0]
    print(f"     distribution: "
          f"p50={_pct(_median(pos)) if pos else '—'} | p90={_pct(_pctile(pos, 0.9)) if pos else '—'} | "
          f"p99={_pct(_pctile(pos, 0.99)) if pos else '—'} | p99.9={_pct(_pctile(pos, 0.999)) if pos else '—'}")

    print(f"\nDOWN (candle LOW vs preceding SMA): bars poked below it = {sum(1 for x in dev_dn if x < 0)}")
    print(f"     {'band':<10}{'bars':>8}{'days':>7}{'max':>10}")
    for b in band_pcts:
        bars = sum(1 for x in dev_dn if x <= -b)
        days = sum(1 for v in dn_days.values() if any(x <= -b for x in v))
        mn = min((x for x in dev_dn if x <= -b), default=None)
        print(f"     <-{b * 100:<6.2f}%{bars:>8}{days:>7}{_pct(mn) if mn is not None else '—':>10}")
    neg = [float(x) for x in dev_dn if x < 0]
    print(f"     distribution: "
          f"p50={_pct(_median(neg)) if neg else '—'} | p90={_pct(_pctile(neg, 0.9)) if neg else '—'} | "
          f"p99={_pct(_pctile(neg, 0.99)) if neg else '—'} | p99.9={_pct(_pctile(neg, 0.999)) if neg else '—'}")


def run_level_test(rows, sel, interval, side, level_price, band_pct, tick, cap, ma_prev):
    """Replay a resting limit and list the candles that would have filled it."""
    seen = [r for _, r in sel]
    n = len(seen)
    if not n:
        print("  No candles in the selected date range.")
        return
    side_name = "SELL" if side == "sell" else "BUY"
    if level_price is not None:
        desc = f"fixed level {_fmt(level_price)}"
        is_fixed = True
    else:
        sign = 1 if side == "sell" else -1
        desc = f"MA(6) {sign * band_pct * 100:+.3f}%"
        is_fixed = False

    print(f"=== {side_name} resting limit test | {desc} | "
          f"{_format_datetime(seen[0]['t'], interval)} .. {_format_datetime(seen[-1]['t'], interval)} ===")
    print(f"({n} candles scanned; a {side_name} limit fills when the market trades to it)\n")

    matched = []
    days = set()
    for idx, row in sel:
        if not is_fixed:
            ma = ma_prev[idx]
            if ma is None:
                continue
            if side == "sell":
                level = (ma * (1 + band_pct) // tick) * tick
            else:
                level = (ma * (1 - band_pct) // tick) * tick
        else:
            level = level_price
        if side == "sell":
            fires = row["h"] >= level
            past = (row["h"] - level) / level if fires else None
        else:
            fires = row["l"] <= level
            past = (level - row["l"]) / level if fires else None
        if fires:
            matched.append({"row": row, "level": level, "past": past})
            days.add(row["t"].date())

    n_ev = len(matched)
    print(f"Level reached on {n_ev} candle(s) across {len(days)} day(s).")
    if not n_ev:
        print("No candle traded through the level in this period — the order would not have fired.")
        return
    past_vals = [m["past"] for m in matched]
    print(f"When it fires the market ran past the level by: "
          f"avg {_pct(sum(past_vals) / len(past_vals))} | "
          f"median {_pct(_median(past_vals))} | "
          f"max {_pct(max(past_vals))}")
    print("A resting limit fills AT the level; the amount it ran past is the profit a wider "
          "band would have collected before the market reverted.")
    print(f"\nMatched candles (first {min(cap, n_ev)} of {n_ev}):")
    print(f"{'Date':<20} {'Open':>9} {'High':>9} {'Low':>9} {'Close':>9} {'Vol':>9} {'Level':>9} {'past':>9}")
    print("-" * 95)
    for m in matched[:cap]:
        r = m["row"]
        print(f"{_format_datetime(r['t'], interval):<20} "
              f"{_fmt(r['o']):>9} {_fmt(r['h']):>9} {_fmt(r['l']):>9} {_fmt(r['c']):>9} {r['v']:>9} "
              f"{_fmt(m['level']):>9} {_pct(m['past']):>9}")
    if n_ev > cap:
        print(f"… and {n_ev - cap} more matched candles (raise --cap or narrow --from/--to).")


async def main(tickers, from_date, to_date, interval_str, detail_str, raw, top,
               profile, bands_str, level_str, band_str, side, cap, tick_str):
    token = os.environ.get("T_INVEST_TOKEN_SANDBOX") or os.environ.get("T_INVEST_TOKEN")
    if not token:
        print("[ERROR] T_INVEST_TOKEN_SANDBOX not set in environment or .env")
        return

    from_dt = _parse_date(from_date)
    to_dt = _parse_date(to_date)
    interval = _parse_interval(interval_str)
    detail_interval = _parse_interval(detail_str)
    end = to_dt if to_dt else now()
    # a date-only --to (midnight) is read as "the whole day is included"
    if end.hour == end.minute == end.second == end.microsecond == 0:
        end += timedelta(days=1) - timedelta(microseconds=1)
    if not from_dt:
        from_dt = end - timedelta(days=366)
    pad = candle_interval_to_timedelta(interval) * 40
    fetch_from = from_dt - pad
    fetch_to = end + candle_interval_to_timedelta(interval)

    level_price = Decimal(level_str) if level_str is not None else None
    band_pct = Decimal(band_str) / Decimal(100) if band_str is not None else None
    tick = Decimal(tick_str)
    band_pcts = [Decimal(b) / Decimal(100) for b in (bands_str.split() if bands_str else DEFAULT_BANDS)]

    mode = "profile" if profile else ("level" if (level_price is not None or band_pct is not None) else
                                      ("raw" if raw else "scan"))

    async with AsyncSandboxClient(token) as client:
        for ticker in tickers:
            if mode != "raw":
                print(f"\n=== {ticker} ({_INTERVAL_NAMES.get(interval, interval)}) ===")
            info = await resolve_instrument(client, ticker)
            if not info:
                print(f"  [ticker {ticker} not resolved]")
                continue

            rows = await fetch_range(client, info["uid"], interval, fetch_from, fetch_to)
            if not rows:
                print("  No candles fetched for the range.")
                continue
            prior_ma6_cache = prior_ma6(rows)
            rows = add_sma6_columns(rows)

            # candles belonging to the requested period (pad rows only seed the SMA)
            sel = [(i, r) for i, r in enumerate(rows) if from_dt <= r["t"] <= end]
            if not sel:
                print("  No candles in the selected date range.")
                continue

            if mode == "profile":
                run_profile(rows, sel, interval, band_pcts, prior_ma6_cache)
                continue
            if mode == "level":
                run_level_test(rows, sel, interval, side, level_price, band_pct, tick, cap, prior_ma6_cache)
                continue

            windowed = [r for _, r in sel]
            if mode == "raw":
                if len(windowed) > MAX_PER_REQUEST * 4:
                    print(f"  {len(windowed)} candles is too wide for one table — "
                          f"narrow the range with --from/--to, or use a larger interval.")
                    continue
                print(f"--- full table: {len(windowed)} candles, "
                      f"{_format_datetime(windowed[0]['t'], interval)} .. "
                      f"{_format_datetime(windowed[-1]['t'], interval)} ---")
                print_table(windowed, interval)
                continue

            # --- scan: top-N most significant deviations from SMA(6) ---
            valid = [r for r in windowed if r["sma6"] is not None]
            if not valid:
                print("  No candles with valid SMA(6) (need at least 6 bars before the range).")
                continue
            valid.sort(key=lambda r: r["abs_diff_max"], reverse=True)
            peaks = valid[:top]

            print(f"Scanned {len(windowed)} {_INTERVAL_NAMES.get(interval, interval)} candles "
                  f"({_format_datetime(windowed[0]['t'], interval)} .. "
                  f"{_format_datetime(windowed[-1]['t'], interval)}); "
                  f"{len(valid)} with a valid SMA(6).")
            print(f"Most significant peak(s) by deviation from SMA(6): "
                  f"{', '.join(_format_datetime(r['t'], interval) for r in peaks)}")
            print_table(peaks, interval)
            for i, row in enumerate(peaks, 1):
                print(zoom_hint(ticker, row["t"], interval, detail_interval))


if __name__ == "__main__":
    load_dotenv()
    parser = argparse.ArgumentParser(
        description="Candle-history browser and limit-order level tester "
                    "(find peaks, zoom into them, profile excursions, test a resting level). "
                    "See the module docstring for the full LLM guide."
    )
    parser.add_argument(
        "--tickers", nargs="+", default=["SAFE"], help="List of tickers (e.g. SBER SAFE)"
    )
    parser.add_argument(
        "--from", dest="from_date", default=None,
        help="Start of the scanned/zoomed period, YYYY-MM-DD[ HH:MM:SS] (inclusive). Default: 1 year back.",
    )
    parser.add_argument(
        "--to", dest="to_date", default=None,
        help="End of the scanned/zoomed period, YYYY-MM-DD[ HH:MM:SS] (inclusive). Default: now.",
    )
    parser.add_argument(
        "--interval", default="D",
        help="Candle interval used for scanning AND zooming AND level tests: 1M 5M 15M 30M H 2H 4H D (default: D).",
    )
    parser.add_argument(
        "--detail-interval", default="5M",
        help="Candle interval suggested in each peak's zoom command (default: 5M).",
    )
    parser.add_argument(
        "--raw", action="store_true",
        help="Zoom mode: print every candle of the period instead of the top peaks.",
    )
    parser.add_argument(
        "--top", type=int, default=10, help="How many peaks to report in scan mode (default: 10)."
    )
    parser.add_argument(
        "--profile", action="store_true",
        help="Print the excursion distribution of candle extremes vs the preceding SMA(6).",
    )
    parser.add_argument(
        "--bands", default=None,
        help="Space-separated thresholds (in percent) for --profile "
             "(default: 0.15 0.3 0.5 1 2 3 5 10 20).",
    )
    parser.add_argument(
        "--level", default=None,
        help="Test a resting limit at this absolute price over the period (with --side).",
    )
    parser.add_argument(
        "--band", default=None,
        help="Test a resting limit at SMA(6) +/- this many percent over the period (with --side).",
    )
    parser.add_argument(
        "--side", choices=["buy", "sell"], default="sell",
        help="Side of the resting order assumed by --level/--band (default: sell).",
    )
    parser.add_argument(
        "--tick", default="0.01", help="Price step used to round band levels (default: 0.01)."
    )
    parser.add_argument(
        "--cap", type=int, default=500,
        help="Max matched candles to print in a level test (default: 500).",
    )
    args = parser.parse_args()
    asyncio.run(main(args.tickers, args.from_date, args.to_date,
                     args.interval, args.detail_interval, args.raw, args.top,
                     args.profile, args.bands, args.level, args.band, args.side,
                     args.cap, args.tick))
