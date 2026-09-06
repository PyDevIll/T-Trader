# history_candles_peak_inspect.py
"""
Inspect candles and compare each candle's high/low to a 6‑period SMA.
Accepts --from and --to to filter the date range, and --interval for candle size.
Outputs only the top 10 rows with the biggest absolute difference (high-SMA or low-SMA).

Usage:
    poetry run python history_candles_peak_inspect.py --tickers SBER --from 2024-01-01 --to 2024-12-31 --interval H
"""

import argparse
import asyncio
import os
from datetime import datetime, timezone
from decimal import Decimal

from dotenv import load_dotenv
from t_tech.invest.sandbox.async_client import AsyncSandboxClient
from t_tech.invest.exceptions import AioRequestError
from t_tech.invest import CandleInterval

# Import data helpers from regime.py
from regime import resolve_instrument


def _fmt(d):
    """Format a Decimal price; None renders as a dash."""
    if d is None:
        return "—"
    return f"{d:,.2f}".replace(",", " ")


def _parse_date(s):
    """Convert ISO date string to timezone‑aware UTC datetime."""
    if not s:
        return None
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _parse_interval(s):
    """
    Convert string like "1M", "5M", "15M", "30M", "H", "2H", "4H", "D"
    to CandleInterval enum.
    """
    mapping = {
        "1M": CandleInterval.CANDLE_INTERVAL_1_MIN,
        "2M": CandleInterval.CANDLE_INTERVAL_2_MIN,
        "3M": CandleInterval.CANDLE_INTERVAL_3_MIN,
        "5M": CandleInterval.CANDLE_INTERVAL_5_MIN,
        "10M": CandleInterval.CANDLE_INTERVAL_10_MIN,
        "15M": CandleInterval.CANDLE_INTERVAL_15_MIN,
        "30M": CandleInterval.CANDLE_INTERVAL_30_MIN,
        "H": CandleInterval.CANDLE_INTERVAL_HOUR,
        "2H": CandleInterval.CANDLE_INTERVAL_2_HOUR,
        "4H": CandleInterval.CANDLE_INTERVAL_4_HOUR,
        "D": CandleInterval.CANDLE_INTERVAL_DAY
    }
    key = s.upper()
    if key not in mapping:
        raise ValueError(f"Unsupported interval: {s}. Supported: {', '.join(mapping.keys())}")
    return mapping[key]


async def fetch_history(client, uid, years, interval):
    """
    Fetch candles with the given interval. Uses the same pattern as regime.py
    but with configurable interval.
    """
    from t_tech.invest.utils import now

    # Approximate number of candles based on interval
    # Day: 252 per year, Hour: ~6.5 * 252 per year, Minute: more
    if interval == CandleInterval.CANDLE_INTERVAL_DAY:
        count = min(int(years * 252) + 30, 2400)
    elif interval in (CandleInterval.CANDLE_INTERVAL_HOUR, CandleInterval.CANDLE_INTERVAL_2_HOUR,
                      CandleInterval.CANDLE_INTERVAL_4_HOUR):
        # Trading hours: ~6.5 hours per day, ~252 trading days
        hours_per_day = 6.5
        if interval == CandleInterval.CANDLE_INTERVAL_HOUR:
            candles_per_day = int(hours_per_day)
        elif interval == CandleInterval.CANDLE_INTERVAL_2_HOUR:
            candles_per_day = int(hours_per_day / 2)
        else:  # 4H
            candles_per_day = int(hours_per_day / 4)
        count = min(int(years * 252 * candles_per_day) + 30, 2400)
    else:
        # Minute intervals: assume full trading day ~6.5 hours = 390 minutes
        minutes_per_day = 390
        if interval == CandleInterval.CANDLE_INTERVAL_1_MIN:
            candles_per_day = minutes_per_day
        elif interval == CandleInterval.CANDLE_INTERVAL_2_MIN:
            candles_per_day = minutes_per_day / 2
        elif interval == CandleInterval.CANDLE_INTERVAL_3_MIN:
            candles_per_day = minutes_per_day / 3
        elif interval == CandleInterval.CANDLE_INTERVAL_5_MIN:
            candles_per_day = minutes_per_day / 5
        elif interval == CandleInterval.CANDLE_INTERVAL_10_MIN:
            candles_per_day = minutes_per_day / 10
        elif interval == CandleInterval.CANDLE_INTERVAL_15_MIN:
            candles_per_day = minutes_per_day / 15
        elif interval == CandleInterval.CANDLE_INTERVAL_30_MIN:
            candles_per_day = minutes_per_day / 30
        else:
            candles_per_day = 390  # fallback
        count = min(int(years * 252 * candles_per_day) + 30, 2400)

    from_ = now() - timedelta(days=int(years * 366))
    resp = await client.market_data.get_candles(
        instrument_id=uid,
        from_=from_,
        to=now() + timedelta(minutes=1),
        interval=interval,
        limit=count,
    )
    candles = resp.candles
    rows = [{
        "t": c.time,
        "o": quotation_to_decimal(c.open),
        "h": quotation_to_decimal(c.high),
        "l": quotation_to_decimal(c.low),
        "c": quotation_to_decimal(c.close),
        "v": int(c.volume),
    } for c in candles]
    return rows


# Need to import quotation_to_decimal for fetch_history
from t_tech.invest.utils import quotation_to_decimal
from datetime import timedelta


async def main(tickers, from_date, to_date, interval_str):
    load_dotenv()
    token = os.environ.get("T_INVEST_TOKEN_SANDBOX")
    if not token:
        print("[ERROR] T_INVEST_TOKEN_SANDBOX not set in environment or .env")
        return

    from_dt = _parse_date(from_date)
    to_dt = _parse_date(to_date)
    interval = _parse_interval(interval_str)

    interval_name = {
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
        CandleInterval.CANDLE_INTERVAL_DAY: "D"
    }.get(interval, str(interval))

    async with AsyncSandboxClient(token) as client:
        for ticker in tickers:
            print(f"\n=== {ticker} ({interval_name}) ===")

            info = await resolve_instrument(client, ticker)
            if not info:
                continue

            try:
                rows = await fetch_history(client, info["uid"], 1, interval)
            except AioRequestError as e:
                print(f"[ERROR] History failed for {ticker}: {e.metadata.message}")
                continue

            if not rows:
                print("No data fetched.")
                continue

            # ------------------------------------------------------------
            # Compute SMA(6) for each row (full history)
            # ------------------------------------------------------------
            closes = [row["c"] for row in rows]
            sma6_values = []
            for i in range(len(rows)):
                if i >= 5:   # need 6 values: indices i-5 .. i
                    sma = sum(closes[i-5:i+1]) / Decimal(6)
                else:
                    sma = None
                sma6_values.append(sma)

            # Attach SMA(6) and compute diffs
            for row, sma in zip(rows, sma6_values):
                row["sma6"] = sma
                if sma is not None:
                    row["diff_h"] = row["h"] - sma
                    row["diff_l"] = row["l"] - sma
                    row["abs_diff_max"] = max(abs(row["diff_h"]), abs(row["diff_l"]))
                else:
                    row["diff_h"] = row["diff_l"] = row["abs_diff_max"] = None

            # ------------------------------------------------------------
            # Filter rows by date range (if provided)
            # ------------------------------------------------------------
            filtered = []
            for row in rows:
                dt = row["t"]
                if from_dt and dt < from_dt:
                    continue
                if to_dt and dt > to_dt:
                    continue
                # Only keep rows that have a valid SMA (i.e., at least 6 bars)
                if row["sma6"] is not None:
                    filtered.append(row)

            if not filtered:
                print("No candles in the selected date range (or insufficient data for SMA).")
                continue

            # ------------------------------------------------------------
            # Sort by absolute max difference descending and take top 10
            # ------------------------------------------------------------
            filtered.sort(key=lambda r: r["abs_diff_max"], reverse=True)
            top10 = filtered[:10]

            # ------------------------------------------------------------
            # Display results
            # ------------------------------------------------------------
            print(
                f"{'Date':<20} {'Open':>10} {'High':>10} {'Low':>10} {'Close':>10} "
                f"{'SMA6':>10} {'High-SMA':>10} {'Low-SMA':>10} {'|max|':>10}"
            )
            print("-" * 120)

            for row in top10:
                dt_str = row["t"].strftime("%Y-%m-%d %H:%M")
                print(
                    f"{dt_str:<20} "
                    f"{_fmt(row['o']):>10} {_fmt(row['h']):>10} {_fmt(row['l']):>10} {_fmt(row['c']):>10} "
                    f"{_fmt(row['sma6']):>10} {_fmt(row['diff_h']):>10} {_fmt(row['diff_l']):>10} "
                    f"{_fmt(row['abs_diff_max']):>10}"
                )

            print(f"\nTop 10 rows shown (out of {len(filtered)} filtered).")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Display top 10 candles with largest deviation from SMA(6)."
    )
    parser.add_argument(
        "--tickers",
        nargs="+",
        default=["SAFE"],
        help="List of tickers (e.g. SBER SAFE)"
    )
    parser.add_argument(
        "--from",
        dest="from_date",
        type=str,
        default=None,
        help="Start date in YYYY-MM-DD format (inclusive)"
    )
    parser.add_argument(
        "--to",
        dest="to_date",
        type=str,
        default=None,
        help="End date in YYYY-MM-DD format (inclusive)"
    )
    parser.add_argument(
        "--interval",
        type=str,
        default="D",
        help="Candle interval: 1M, 5M, 15M, 30M, H, 2H, 4H, D (default: D)"
    )

    args = parser.parse_args()
    asyncio.run(main(args.tickers, args.from_date, args.to_date, args.interval))
