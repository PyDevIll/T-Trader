# llm_playground.py
"""
Playground: see what market data for an instrument actually looks like
when fetched via the T-Invest SDK, in two forms:

  RAW  - dump everything the SDK returns (raw fields, verbatim)
  LLM  - a compact, token-efficient "analyst snapshot" that we would
         actually feed to an LLM for entry-point analysis

Run (from project root):
    poetry run python llm_playground.py                 # LLM snapshots
    poetry run python llm_playground.py --raw           # + raw dumps
    poetry run python llm_playground.py --tickers SBER SAFE TGLD@
"""

import argparse
import asyncio
import os
from datetime import timedelta
from decimal import Decimal, ROUND_HALF_UP

from dotenv import load_dotenv
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from t_tech.invest.sandbox.async_client import AsyncSandboxClient
from t_tech.invest.utils import (
	now,
	candle_interval_to_timedelta,
	quotation_to_decimal,
	money_to_decimal,
)
from t_tech.invest import (
	CandleInterval,
	InstrumentIdType,
	InstrumentType,
	SecurityTradingStatus,
)
from t_tech.invest.schemas import (
	CandleSource,
	GetAssetFundamentalsRequest,
	GetDividendsRequest,
	GetForecastRequest,
	GetTechAnalysisRequest,
	IndicatorInterval,
	IndicatorType,
	TypeOfPrice,
)
from t_tech.invest.exceptions import AioRequestError

console = Console()


# ----------------------------------------------------------------------------
# tiny deterministic helpers (pure python, no pandas)
# ----------------------------------------------------------------------------

def dec(q):
	"""Quotation -> Decimal."""
	return quotation_to_decimal(q)


def rnd(x, n=2):
	"""Round a Decimal/float to n decimal places."""
	if x is None:
		return None
	return Decimal(str(x)).quantize(Decimal("1e-%d" % n), rounding=ROUND_HALF_UP)


def pct_change(old, new):
	if old in (None, 0):
		return None
	return (Decimal(new) - Decimal(old)) / Decimal(old) * 100


def sma(closes, period):
	if len(closes) < period:
		return None
	return sum(closes[-period:]) / period


def ema(closes, period):
	if len(closes) < period:
		return None
	k = Decimal(2) / (period + 1)
	e = sum(closes[:period]) / period
	for c in closes[period:]:
		e = c * k + e * (1 - k)
	return e


def rsi(closes, period=14):
	if len(closes) <= period:
		return None
	gains, losses = [], []
	for i in range(1, len(closes)):
		d = closes[i] - closes[i - 1]
		gains.append(max(d, Decimal(0)))
		losses.append(max(-d, Decimal(0)))
	avg_gain = sum(gains[-period:]) / period
	avg_loss = sum(losses[-period:]) / period
	if avg_loss == 0:
		return Decimal(100)
	rs = avg_gain / avg_loss
	return Decimal(100) - Decimal(100) / (1 + rs)


def atr(closes, highs, lows, period=14):
	"""Average True Range over the last `period` bars."""
	if len(closes) < period + 1:
		return None
	trs = []
	for i in range(1, len(closes)):
		trs.append(max(
			highs[i] - lows[i],
			abs(highs[i] - closes[i - 1]),
			abs(lows[i] - closes[i - 1]),
		))
	return sum(trs[-period:]) / period


def daily_vol(closes, period=20):
	"""Stdev of daily % returns over last `period` bars (volatility, in %)."""
	if len(closes) < 2:
		return None
	rets = []
	for i in range(1, len(closes)):
		c0 = closes[i - 1]
		if c0:
			rets.append((closes[i] - c0) / c0 * 100)
	window = rets[-period:]
	if len(window) < 2:
		return None
	mean = sum(window) / len(window)
	var = sum((r - mean) ** 2 for r in window) / (len(window) - 1)
	return Decimal(var).sqrt()


def tick_size(info):
	"""Instrument tick size as Decimal (from min_price_increment)."""
	v = info.get("min_price_increment")
	if isinstance(v, Decimal):
		return v
	return Decimal("0.01")


def fmt_price(x, tick=None, n=2):
	"""Round a price to the instrument tick size (or n decimals)."""
	if x is None:
		return None
	d = Decimal(str(x))
	if tick:
		t = tick.normalize()
		return (d / t).to_integral_value() * t
	return d.quantize(Decimal("1e-%d" % n), rounding=ROUND_HALF_UP)


# ----------------------------------------------------------------------------
# T-Invest data fetching
# ----------------------------------------------------------------------------

async def resolve_instrument(client, ticker):
	"""
	Find instrument by ticker and load its full instrument card.
	Returns a dict with short + full info, or None.
	"""
	try:
		found = await client.instruments.find_instrument(
			query=ticker, api_trade_available_flag=True
		)
	except AioRequestError as e:
		console.print(f"[red]find_instrument failed for {ticker}: {e.metadata.message}[/]")
		return None

	candidates = [i for i in found.instruments if i.ticker.upper() == ticker.upper()]
	if not candidates:
		console.print(f"[yellow]No exact ticker match for {ticker}; showing all:[/]")
		candidates = found.instruments
	if not candidates:
		return None

	short = candidates[0]
	info = {
		"ticker": short.ticker,
		"name": short.name,
		"uid": short.uid,
		"figi": short.figi,
		"lot": short.lot,
		"kind": short.instrument_kind.name,
		"instrument_type": short.instrument_type,
	}

	# full card (sector, currency, min_price_increment, liquidity, flags...)
	try:
		if short.instrument_kind == InstrumentType.INSTRUMENT_TYPE_SHARE:
			inst = (await client.instruments.share_by(
				id=short.uid, id_type=InstrumentIdType.INSTRUMENT_ID_TYPE_UID
			)).instrument
		elif short.instrument_kind == InstrumentType.INSTRUMENT_TYPE_ETF:
			inst = (await client.instruments.etf_by(
				id=short.uid, id_type=InstrumentIdType.INSTRUMENT_ID_TYPE_UID
			)).instrument
		else:
			inst = (await client.instruments.get_instrument_by(
				id=short.uid, id_type=InstrumentIdType.INSTRUMENT_ID_TYPE_UID
			)).instrument
		for attr in ("sector", "currency", "exchange", "min_price_increment",
					 "liquidity_flag", "api_trade_available_flag",
					 "buy_available_flag", "sell_available_flag",
					 "for_qual_investor_flag", "trading_status", "asset_uid"):
			if hasattr(inst, attr):
				val = getattr(inst, attr)
				if attr == "min_price_increment":
					info[attr] = dec(val) if val else None
				else:
					info[attr] = val.name if hasattr(val, "name") else val
	except AioRequestError as e:
		console.print(f"[yellow]full card failed for {ticker}: {e.metadata.message}[/]")

	return info


async def fetch_candles(client, instrument_id, interval, count):
	"""Last `count` complete/partial candles as list of closes + raw candles."""
	from_ = now() - candle_interval_to_timedelta(interval) * (count + 5)
	to = now() + timedelta(minutes=1)
	# NB: candle_source_type conflicts with `limit` (API error 30220) -> omit it
	resp = await client.market_data.get_candles(
		instrument_id=instrument_id,
		from_=from_,
		to=to,
		interval=interval,
		limit=count,
	)
	candles = resp.candles
	closes = [dec(c.close) for c in candles]
	return candles, closes


async def fetch_snapshot(client, info):
	uid = info["uid"]
	figi = info["figi"]
	snap = {"info": info}

	# last prices
	try:
		lp = (await client.market_data.get_last_prices(instrument_id=[uid])).last_prices
		snap["last_price"] = [{"price": dec(p.price), "time": p.time.isoformat(),
							   "type": p.last_price_type.name} for p in lp]
	except AioRequestError as e:
		snap["last_price"] = [{"error": e.metadata.message}]

	# orderbook
	try:
		ob = await client.market_data.get_order_book(instrument_id=uid, depth=10)
		snap["orderbook"] = {
			"bids": [(dec(o.price), o.quantity) for o in ob.bids],
			"asks": [(dec(o.price), o.quantity) for o in ob.asks],
			"last_price": dec(ob.last_price),
			"close_price": dec(ob.close_price),
			"limit_up": dec(ob.limit_up),
			"limit_down": dec(ob.limit_down),
			"ts": ob.orderbook_ts.isoformat(),
		}
	except AioRequestError as e:
		snap["orderbook"] = {"error": e.metadata.message}

	# trading status
	try:
		st = await client.market_data.get_trading_status(instrument_id=uid)
		snap["trading_status"] = st.trading_status.name
	except AioRequestError as e:
		snap["trading_status"] = {"error": e.metadata.message}

	# candles on several timeframes
	timeframes = {
		"1m": (CandleInterval.CANDLE_INTERVAL_1_MIN, 60),
		"5m": (CandleInterval.CANDLE_INTERVAL_5_MIN, 96),
		"1d": (CandleInterval.CANDLE_INTERVAL_DAY, 90),
	}
	snap["candles"] = {}
	for label, (interval, count) in timeframes.items():
		try:
			candles, closes = await fetch_candles(client, uid, interval, count)
			snap["candles"][label] = {
				"raw": candles, "closes": closes, "count": len(closes)
			}
		except AioRequestError as e:
			snap["candles"][label] = {"error": e.metadata.message}

	# enrichment: fundamentals / analyst target / dividends / recent tape
	snap["fundamentals"] = await fetch_fundamentals(client, uid, info.get("asset_uid"))
	snap["forecast"] = await fetch_forecast(client, uid)
	snap["dividends"] = await fetch_dividends(client, figi, uid)
	snap["last_trades"] = await fetch_last_trades(client, uid)
	snap["fetched_at"] = now().isoformat()

	return snap


async def fetch_tech_analysis(client, uid):
	"""Server-side indicators (BB/EMA/RSI/MACD/SMA). May be unsupported."""
	out = {}
	params = {
		"RSI": (IndicatorType.INDICATOR_TYPE_RSI, {"length": 14}),
		"SMA_20": (IndicatorType.INDICATOR_TYPE_SMA, {"length": 20}),
	}
	for label, (itype, extra) in params.items():
		try:
			req = GetTechAnalysisRequest(
				indicator_type=itype,
				instrument_uid=uid,
				from_=now() - timedelta(days=90),
				to=now() + timedelta(minutes=1),
				interval=IndicatorInterval.INDICATOR_INTERVAL_ONE_DAY,
				type_of_price=TypeOfPrice.TYPE_OF_PRICE_CLOSE,
				**extra,
			)
			resp = await client.market_data.get_tech_analysis(request=req)
			items = resp.technical_indicators
			vals = []
			for it in items:
				v = it.signal or it.middle_band or it.macd
				if v is not None:
					vals.append((it.timestamp.isoformat(), dec(v)))
			out[label] = vals[-5:] if vals else None
		except AioRequestError as e:
			out[label] = {"error": e.metadata.message}
	return out


async def fetch_fundamentals(client, uid, asset_uid=None):
	"""P/E, market cap, beta, margins, 52w range, dividend yield..."""
	try:
		resp = await client.instruments.get_asset_fundamentals(
			request=GetAssetFundamentalsRequest(assets=[asset_uid or uid])
		)
		if not resp.fundamentals:
			return None
		f = resp.fundamentals[0]
		return {
			"market_cap": f.market_capitalization,
			"pe_ttm": f.pe_ratio_ttm,
			"ps_ttm": f.price_to_sales_ttm,
			"pb_ttm": f.price_to_book_ttm,
			"beta": f.beta,
			"roe": f.roe,
			"net_margin": f.net_margin_mrq,
			"div_yield_ttm": f.dividend_yield_daily_ttm,
			"eps_ttm": f.eps_ttm,
			"free_float": f.free_float,
			"hi_52w": f.high_price_last_52_weeks,
			"lo_52w": f.low_price_last_52_weeks,
		}
	except AioRequestError as e:
		return {"error": e.metadata.message}


async def fetch_forecast(client, uid):
	"""Analyst target price + recommendation counts."""
	try:
		resp = await client.instruments.get_forecast_by(
			request=GetForecastRequest(instrument_id=uid)
		)
		if resp.targets:
			t = resp.targets[0]
			c = resp.consensus
			return {
				"target": dec(t.target_price) if t.target_price else None,
				"target_low": dec(c.min_target) if c and c.min_target else None,
				"target_high": dec(c.max_target) if c and c.max_target else None,
				"consensus": (c.recommendation.name if c and c.recommendation
							  else t.recommendation.name),
			}
	except AioRequestError as e:
		return {"error": e.metadata.message}
	return None


async def fetch_dividends(client, figi, uid):
	"""Last dividend: amount, ex-date, yield."""
	try:
		resp = await client.instruments.get_dividends(
			figi=figi, instrument_id=uid
		)
		if not resp.dividends:
			return None
		d = resp.dividends[-1]
		return {
			"per_share": dec(d.dividend_net) if d.dividend_net else None,
			"ex_date": d.last_buy_date.isoformat()[:10],
			"pay_date": d.payment_date.isoformat()[:10],
			# yield_value is already a percentage (e.g. 10.44)
			"yield": rnd(dec(d.yield_value), 2) if d.yield_value else None,
		}
	except AioRequestError as e:
		return {"error": e.metadata.message}


async def fetch_last_trades(client, uid, minutes=10):
	"""Recent tape: last up-to-6 trades (price, qty, side)."""
	try:
		resp = await client.market_data.get_last_trades(
			instrument_id=uid,
			from_=now() - timedelta(minutes=minutes),
			to=now() + timedelta(minutes=1),
		)
		trades = resp.trades

		out = []
		for t in trades[-6:]:
			out.append((t.time.strftime("%H:%M:%S"), str(rnd(dec(t.price))),
						t.quantity, t.direction.name.split("_")[-1]))
		return out
	except AioRequestError as e:
		return {"error": e.metadata.message}


# ----------------------------------------------------------------------------
# rendering
# ----------------------------------------------------------------------------

EXCHANGE_LABEL = {
	"moex_mrng_evng_e_wknd_dlr": "MOEX (morning+evening+wknd)",
	"moex_not_morning_not_evening": "MOEX (main session)",
	"spb_morning": "SPB (morning)",
}


def build_llm_snapshot(snap) -> str:
	"""The compact text we would actually put in an LLM prompt."""
	info = snap["info"]
	tick = tick_size(info)
	last = None
	if snap.get("last_price") and "price" in snap["last_price"][0]:
		last = snap["last_price"][0]["price"]
		last_ts = snap["last_price"][0]["time"]

	lines = []
	# -- identity + session context ---------------------------------------
	lines.append(f"## {info['ticker']} — {info['name']}")
	kind = info.get("instrument_type") or info.get("kind")
	exchange = EXCHANGE_LABEL.get(info.get("exchange"), info.get("exchange"))
	lines.append(
		f"kind={kind} currency={info.get('currency')} exchange={exchange} "
		f"sector={info.get('sector')} lot={info.get('lot')} "
		f"min_step={str(tick.normalize())} liquidity={info.get('liquidity_flag')}"
	)
	status = snap.get("trading_status", "?")
	status_note = ""
	if status == "SECURITY_TRADING_STATUS_NOT_AVAILABLE_FOR_TRADING":
		status_note = " (outside session or halted)"
	lines.append(f"trading={status}{status_note} api_trade={info.get('api_trade_available_flag')} "
				 f"buy={info.get('buy_available_flag')} sell={info.get('sell_available_flag')}")
	lines.append(f"snapshot_at={snap.get('fetched_at', '?')} last_price_at={last_ts if last is not None else 'n/a'}")

	# -- orderbook depth + imbalance ---------------------------------------
	if isinstance(snap.get("orderbook"), dict) and "bids" in snap["orderbook"]:
		ob = snap["orderbook"]
		bids, asks = ob["bids"], ob["asks"]
		bid, bid_qty = bids[0] if bids else (None, None)
		ask, ask_qty = asks[0] if asks else (None, None)
		if bid is not None and ask is not None:
			spread = fmt_price(ask - bid, tick)
			spread_pct = (ask - bid) / ask * 100
			tot_bid = sum(q for _, q in bids[:5])
			tot_ask = sum(q for _, q in asks[:5])
			imb = (tot_bid - tot_ask) / (tot_bid + tot_ask) * 100 if (tot_bid + tot_ask) else 0
			lines.append(f"orderbook(d10): bid={fmt_price(bid, tick)}x{bid_qty} | "
						 f"ask={fmt_price(ask, tick)}x{ask_qty} | spread={spread} "
						 f"({rnd(spread_pct, 2)}%)")
			lines.append(f"  depth top5: bid_qty={tot_bid} ask_qty={tot_ask} "
						 f"imbalance={rnd(imb, 1):+}% (buy vs sell pressure)")
		else:
			lines.append("orderbook: no live depth (market closed / after hours)")
		if ob.get("limit_up"):
			lines.append(f"  limits: up={fmt_price(ob.get('limit_up'), tick)} "
						 f"down={fmt_price(ob.get('limit_down'), tick)} "
						 f"prev_close={fmt_price(ob.get('close_price'), tick)}")

	# -- per-timeframe stats -----------------------------------------------
	for label in ("1d", "5m", "1m"):
		cd = snap["candles"].get(label)
		if not cd or "error" in cd:
			continue
		if "closes" not in cd or not cd["closes"]:
			lines.append(f"{label}: no recent bars (outside session)")
			continue
		raw = cd["raw"]
		closes = cd["closes"]
		last_c = closes[-1]
		prev_c = closes[-2] if len(closes) > 1 else None
		chg = pct_change(prev_c, last_c)
		hi = max(closes)
		lo = min(closes)
		s20, s50 = sma(closes, 20), sma(closes, 50)
		r = rsi(closes, 14)
		a = atr(closes, [dec(c.high) for c in raw], [dec(c.low) for c in raw], 14)
		vol = daily_vol(closes, 20)
		dist_s20 = pct_change(s20, last_c) if s20 else None
		avg_vol = sum(int(c.volume) for c in raw[-20:]) / min(20, len(raw))
		last_candle_t = raw[-1].time.strftime("%m-%d %H:%M")

		stats = (f"{label}: last={fmt_price(last_c, tick)} "
				 f"chg={rnd(chg, 2) if chg is not None else '?'}% "
				 f"sma20={fmt_price(s20, tick)} sma50={fmt_price(s50, tick)} "
				 f"rsi14={rnd(r, 1)} atr14={rnd(a)} vol20={rnd(vol, 1)}% "
				 f"vs_sma20={rnd(dist_s20, 2) if dist_s20 is not None else '?'}% "
				 f"hi={fmt_price(hi, tick)} lo={fmt_price(lo, tick)} "
				 f"avg_vol20={int(avg_vol):,} bars@={last_candle_t}")
		lines.append(stats)
		tail = [str(fmt_price(c, tick)) for c in closes[-8:]]
		lines.append(f"   {label} closes[{len(closes)}] = " + ", ".join(tail))

	# -- 52w range + analyst context ---------------------------------------
	fund = snap.get("fundamentals")
	if isinstance(fund, dict) and "error" not in fund and fund:
		lines.append(
			"fundamentals: mcap=" + _fmt_big(fund.get("market_cap")) +
			f" pe_ttm={_n(fund.get('pe_ttm'))} ps_ttm={_n(fund.get('ps_ttm'))} "
			f"beta={_n(fund.get('beta'))} roe={_n(fund.get('roe'))}% "
			f"net_margin={_n(fund.get('net_margin'))}% div_yield_ttm={_n(fund.get('div_yield_ttm'))}% "
			f"free_float={_n(fund.get('free_float'))}%"
		)
		if fund.get("hi_52w") and fund.get("lo_52w"):
			lo52, hi52 = Decimal(str(fund["lo_52w"])), Decimal(str(fund["hi_52w"]))
			pos52 = (last - lo52) / (hi52 - lo52) * 100 if lo52 else None
			lines.append(f"   range_52w: {fmt_price(lo52, tick)} .. {fmt_price(hi52, tick)} "
						 f"(current at {rnd(pos52, 0)}% of range)")

	fc = snap.get("forecast")
	if isinstance(fc, dict) and "error" not in fc and fc.get("target"):
		target = fc["target"]
		upside = pct_change(last, target) if last else None
		lines.append(f"analyst: {fc.get('consensus','?').replace('RECOMMENDATION_','')} "
					 f"target={fmt_price(target, tick)} "
					 f"[{fmt_price(fc.get('target_low'), tick)} .. {fmt_price(fc.get('target_high'), tick)}] "
					 f"implied={rnd(upside, 1) if upside is not None else '?'}%")

	div = snap.get("dividends")
	if isinstance(div, dict) and "error" not in div and div:
		lines.append(f"dividend: {fmt_price(div.get('per_share'), tick)}/share "
					 f"ex_date={div.get('ex_date')} pay_date={div.get('pay_date')} "
					 f"yield={div.get('yield')}%")

	# -- recent tape -------------------------------------------------------
	tape = snap.get("last_trades")
	if isinstance(tape, list) and tape:
		lines.append("tape(last6): " + " | ".join(
			f"{t[0]}({t[1]}x{t[2]:,}{t[3][0]})" for t in tape))
	elif isinstance(tape, list):
		lines.append("tape: no trades in last 10m (market closed?)")

	# -- server-side indicators (clean) ------------------------------------
	if isinstance(snap.get("tech_analysis"), dict):
		for label, vals in snap["tech_analysis"].items():
			if isinstance(vals, dict):  # error
				continue
			series = [str(v) for _, v in vals]
			if len(series) > 1:
				trend = "up" if float(vals[-1][1]) >= float(vals[-2][1]) else "down"
				lines.append(f"server_{label} = {', '.join(series)} "
							 f"(last={vals[-1][1]} {trend} d/d)")
			elif series:
				lines.append(f"server_{label} = {series[0]}")

	return "\n".join(lines)


def _fmt_big(x):
	"""Format a market cap / volume float into B/M/K."""
	if not x:
		return "?"
	try:
		x = float(x)
		for unit, div in (("T", 1e12), ("B", 1e9), ("M", 1e6), ("K", 1e3)):
			if abs(x) >= div:
				return f"{x/div:.2f}{unit}"
		return f"{x:.0f}"
	except (TypeError, ValueError):
		return "?"


def _n(x):
	"""Compact number formatting for float fields."""
	if x is None:
		return "?"
	try:
		return f"{float(x):.2f}".rstrip("0").rstrip(".")
	except (TypeError, ValueError):
		return "?"


def dump_raw(snap):
	"""Print raw values the SDK returned."""
	for label, cd in snap["candles"].items():
		if "closes" not in cd:
			continue
		raw = cd["raw"]
		table = Table(title=f"RAW candles {label} (n={len(raw)})", show_lines=False)
		table.add_column("time")
		table.add_column("open")
		table.add_column("high")
		table.add_column("low")
		table.add_column("close")
		table.add_column("vol")
		table.add_column("vol_buy")
		table.add_column("complete")
		for c in raw[-15:]:
			table.add_row(
				c.time.strftime("%m-%d %H:%M"), str(rnd(dec(c.open))), str(rnd(dec(c.high))),
				str(rnd(dec(c.low))), str(rnd(dec(c.close))), str(c.volume),
				str(c.volume_buy), str(c.is_complete),
			)
		console.print(table)

	console.print(Panel(
		f"trading_status = {snap.get('trading_status')}\n"
		f"last_price = {snap.get('last_price')}\n"
		f"orderbook = {snap.get('orderbook')}\n"
		f"tech_analysis = {snap.get('tech_analysis')}\n"
		f"fundamentals = {snap.get('fundamentals')}\n"
		f"forecast = {snap.get('forecast')}\n"
		f"dividends = {snap.get('dividends')}\n"
		f"last_trades = {snap.get('last_trades')}",
		title="RAW market snapshot",
		style="cyan",
	))


# ----------------------------------------------------------------------------
# main
# ----------------------------------------------------------------------------

def load_token():
	"""Load a T-Invest token: env var first, then .env (KEY=VALUE or bare token)."""
	for key in ("T_INVEST_TOKEN_SANDBOX", "SANDBOX_TOKEN", "T_INVEST_TOKEN"):
		val = os.environ.get(key)
		if val:
			return val
	path = os.path.join(os.path.dirname(__file__), ".env")
	if os.path.exists(path):
		for line in open(path):
			line = line.strip()
			if not line or line.startswith("#"):
				continue
			if "=" in line:
				k, _, v = line.partition("=")
				if k in ("T_INVEST_TOKEN_SANDBOX", "SANDBOX_TOKEN", "T_INVEST_TOKEN"):
					return v.strip()
			elif line.startswith("t."):
				return line
	return None


async def main(tickers, raw):
	load_dotenv()
	token = load_token()
	if not token:
		console.print("[red]No T-Invest token found in env or .env[/]")
		return

	async with AsyncSandboxClient(token) as client:
		for ticker in tickers:
			console.rule(f"[bold]{ticker}[/]")
			info = await resolve_instrument(client, ticker)
			if not info:
				console.print(f"[red]Could not resolve {ticker}[/]")
				continue
			console.print(f"resolved: {info}")

			snap = await fetch_snapshot(client, info)
			snap["tech_analysis"] = await fetch_tech_analysis(client, info["uid"])

			if raw:
				dump_raw(snap)

			console.print(Panel(
				build_llm_snapshot(snap),
				title=f"LLM snapshot: {ticker}",
				style="green",
			))


if __name__ == "__main__":
	parser = argparse.ArgumentParser(description="T-Invest LLM market data playground")
	parser.add_argument("--tickers", nargs="*", default=["SBER", "SAFE"],
						help="tickers to inspect (default: SBER SAFE)")
	parser.add_argument("--raw", action="store_true",
						help="also dump raw SDK responses")
	args = parser.parse_args()
	try:
		asyncio.run(main(args.tickers, args.raw))
	except KeyboardInterrupt:
		console.print("\nInterrupted")
