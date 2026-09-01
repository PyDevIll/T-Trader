# t_regime.py
"""
Regime profile for an instrument, computed from years of daily history.

Used by InstrumentMonitor to adapt the MA bracket: the band expands with
intraday ATR during volatility, buying is gated on an intact uptrend, and a
"regime break" fires when the current drawdown from the reference high is
deeper than the worst dip that historically recovered.
"""

import math
import statistics
from dataclasses import dataclass
from datetime import timedelta
from decimal import Decimal

from t_tech.invest import CandleInterval
from t_tech.invest.utils import now, quotation_to_decimal
from t_tech.invest.exceptions import AioRequestError

TREND_WINDOW = 120
DIP_THRESHOLD_PCT = -1.0
DIP_EXIT_THRESHOLD_PCT = -0.2
MIN_SLOPE_ANNUAL = 0.0
MIN_R2 = 0.3
MAX_DIPS = 40


@dataclass
class RegimeProfile:
	ticker: str
	slope_annual_pct: float
	r2: float
	ref_high: Decimal
	worst_dd: Decimal
	median_recovery_bars: int
	vol20_pct: float
	atr14_daily: Decimal
	z_now: float
	trend_ok: bool

	def describe(self) -> str:
		return (
			f"{self.ticker}: slope={self.slope_annual_pct:.1f}%/yr r2={self.r2:.2f} "
			f"ref_high={self.ref_high} worst_dd={self.worst_dd} "
			f"median_recovery={self.median_recovery_bars}d vol20={self.vol20_pct:.2f}% "
			f"z_now={self.z_now:.2f} trend_ok={self.trend_ok}"
		)


def _trailing_fit(logc, window):
	n = len(logc)
	zs = [None] * n
	slope_last = None
	r2_last = None
	for i in range(window - 1, n):
		win = logc[i - window + 1:i + 1]
		xs = list(range(window))
		mx = (window - 1) / 2.0
		my = sum(win) / window
		sxx = sum((x - mx) ** 2 for x in xs)
		sxy = sum((x - mx) * (y - my) for x, y in zip(xs, win))
		slope = sxy / sxx
		intercept = my - slope * mx
		path_i = slope * (window - 1) + intercept
		res = [win[j] - (slope * xs[j] + intercept) for j in range(window)]
		sigma = statistics.pstdev(res)
		zs[i] = (win[-1] - path_i) / sigma if sigma > 0 else 0.0
		if i == n - 1:
			slope_last = slope
			ss_res = sum(r ** 2 for r in res)
			ss_tot = sum((y - my) ** 2 for y in win)
			r2_last = 1.0 - ss_res / ss_tot if ss_tot > 0 else 0.0
	return zs, slope_last, r2_last


def _extract_dips(closes, threshold=DIP_THRESHOLD_PCT, exit_thresh=DIP_EXIT_THRESHOLD_PCT):
	cummax = -1.0
	dd = []
	for c in closes:
		f = float(c)
		if f > cummax:
			cummax = f
		dd.append((f / cummax - 1.0) * 100 if cummax > 0 else 0.0)

	dips = []
	start = None
	cur_min = 0.0
	for i, d in enumerate(dd):
		if start is None:
			if d < threshold:
				start = i
				cur_min = d
		else:
			cur_min = min(cur_min, d)
			if d >= exit_thresh:
				dips.append({"depth": cur_min, "bars": i - start, "recovered": True})
				start = None
				cur_min = 0.0
	if start is not None:
		dips.append({"depth": cur_min, "bars": len(dd) - 1 - start, "recovered": False})
	return dips


def _atr(closes, highs, lows, period=14):
	if len(closes) < period + 1:
		return None
	trs = []
	for i in range(1, len(closes)):
		trs.append(max(
			float(highs[i] - lows[i]),
			abs(float(highs[i]) - float(closes[i - 1])),
			abs(float(lows[i]) - float(closes[i - 1])),
		))
	return sum(trs[-period:]) / period


def _vol20_pct(closes):
	if len(closes) < 21:
		return None
	rets = [float(closes[j]) / float(closes[j - 1]) - 1.0 for j in range(len(closes) - 20, len(closes))]
	return statistics.pstdev(rets) * 100.0


async def load_regime_profile(client, instrument_id, ticker, years=5, min_slope=MIN_SLOPE_ANNUAL, min_r2=MIN_R2):
	"""Fetch daily history and build a RegimeProfile. Returns None if history is too short."""
	from_ = now() - timedelta(days=int(years * 366))
	count = min(int(years * 252) + 30, 2400)
	try:
		resp = await client.market_data.get_candles(
			instrument_id=instrument_id,
			from_=from_,
			to=now(),
			interval=CandleInterval.CANDLE_INTERVAL_DAY,
			limit=count,
		)
	except AioRequestError as e:
		raise RuntimeError(f"regime history failed for {ticker}: {e.metadata.message}") from e

	candles = resp.candles
	if len(candles) < TREND_WINDOW + 20:
		return None

	closes = [quotation_to_decimal(c.close) for c in candles]
	highs = [quotation_to_decimal(c.high) for c in candles]
	lows = [quotation_to_decimal(c.low) for c in candles]
	logc = [math.log(float(c)) for c in closes]

	zs, slope_last, r2_last = _trailing_fit(logc, TREND_WINDOW)

	recovered = [d for d in _extract_dips(closes) if d["recovered"]]
	worst_recovered = min((d["depth"] for d in recovered), default=None)

	running = -1.0
	worst_dd = 0.0
	for c in closes:
		f = float(c)
		running = max(running, f)
		worst_dd = min(worst_dd, (f / running - 1.0) * 100)

	ref_high = max(closes)
	# worst dip that still recovered; fall back to the overall worst drawdown
	worst_dd_frac = Decimal(str(worst_recovered)) / 100 if worst_recovered is not None else Decimal(str(worst_dd)) / 100

	atr14 = _atr(closes, highs, lows)
	vol20 = _vol20_pct(closes)
	median_rec = int(statistics.median([d["bars"] for d in recovered])) if recovered else None

	slope_annual = slope_last * 252 * 100 if slope_last is not None else 0.0
	r2 = r2_last or 0.0
	trend_ok = slope_annual >= min_slope and r2 >= min_r2

	return RegimeProfile(
		ticker=ticker,
		slope_annual_pct=round(slope_annual, 2),
		r2=round(r2, 3),
		ref_high=ref_high,
		worst_dd=worst_dd_frac,
		median_recovery_bars=median_rec,
		vol20_pct=round(vol20, 2) if vol20 is not None else 0.0,
		atr14_daily=Decimal(str(round(atr14, 4))) if atr14 else Decimal(0),
		z_now=round(zs[-1], 2) if zs[-1] is not None else 0.0,
		trend_ok=trend_ok,
	)
