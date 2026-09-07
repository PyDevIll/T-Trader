# t_order_manager.py

import t_services
import asyncio
import sys
import os
import json
import threading
import termios
import tty
import select

from datetime import datetime, timedelta, time
from decimal import Decimal
from types import SimpleNamespace

from t_tech.invest.async_services import AsyncServices
from t_tech.invest.utils import (
	now,
	candle_interval_to_timedelta,
	quotation_to_decimal,
	decimal_to_quotation,
	decimal_to_money,
	money_to_decimal

)
from t_tech.invest import (
	InstrumentIdType,
	SecurityTradingStatus,
	Quotation,
)
from t_tech.invest.sandbox.async_client import AsyncSandboxClient
from t_tech.invest.schemas import (
	CandleInterval,
	CandleSource,
	InstrumentStatus,
	InstrumentIdType,
	OperationType,
	TradeInstrument,
	CandleInstrument,
	OrderBookInstrument,
	OrderType,
	OrderDirection,
	TimeInForceType,
	OrderExecutionReportStatus,
	OrderIdType,
	ReplaceOrderRequest,
	StopOrderDirection,
	StopOrderType
)
from t_tech.invest.exceptions import AioRequestError
_print = print
from rich import inspect, print
from rich.console import Console
from rich.live import Live
from rich.prompt import Prompt
from dotenv import load_dotenv
from functools import lru_cache, reduce
from t_services import (
	StreamMonitor,
	AccountManagerSandbox,
	OrderManagerSandbox,
	instruments_by_filter,
	etf_ticker_to_figi,
	share_ticker_to_figi
)
from t_services import ticker_figi_cache as ticker_figi
from t_regime import load_regime_profile
from t_dashboard import build_dashboard

LOG_FILE = "t_trader.log"


def log(message):
	with open(LOG_FILE, "a", encoding="utf-8") as f:
		f.write(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} {message}\n")


def MA(period, candle_history):
	if len(candle_history) < period:
		return None
	else:
		value = reduce(lambda a, v: a + v, [quotation_to_decimal(c.close) for c in candle_history[-period:]]) / period
		return value


_OPERATION_SIDES = {
	"OPERATION_TYPE_BUY": "BUY",
	"OPERATION_TYPE_SELL": "SELL",
	"OPERATION_TYPE_BROKER_FEE": "FEE",
	"OPERATION_TYPE_INPUT": "PAYIN",
	"OPERATION_TYPE_OUTPUT": "PAYOUT",
}


def _operation_view(op):
	side = _OPERATION_SIDES.get(op.operation_type.name, op.operation_type.name.replace("OPERATION_TYPE_", ""))
	return {
		"ticker": ticker_figi.ticker(op.figi) if op.figi else "",
		"side": side,
		"raw_type": op.type,
		"sum": money_to_decimal(op.payment),
		"price": money_to_decimal(op.price) if op.price.currency else None,
		"date": op.date.astimezone().strftime("%m-%d %H:%M") if op.date.tzinfo else op.date.strftime("%m-%d %H:%M"),
	}


# ---------------------------------------------------------------------------
# Per-ticker settings live in ticker_settings.json next to the repo root. The
# file is the source of truth for what the monitor boots: every top-level key
# is a ticker the OrderMonitor starts watching, its value is that ticker's
# InstrumentMonitor config. Recognized keys (any may be omitted -> defaults):
#   type                "etf" | "share"  (also selects the ticker->figi lookup)
#   candle_interval     "1M" "5M" "15M" "30M" "H" "2H" "4H" "D" (default "5M")
#   period              candles used for the moving average (default 6)
#   lots                order/position size (default 1)
#   allow_buying        allow the BUY bracket (default true)
#   allow_selling       allow the SELL bracket (default true)
#   load_regime         apply the regime guard before buying (default true)
#   band_atr_mult       ATR-scaled width multiplier (0 = off)
#   hard_stop_dev_k     hard stop distance in band-widths beyond avg entry
#   deviation_percent   base bracket width as a fraction of MA (0.0015 = 0.15%)
#   sell_band_factor    widen/narrow the SELL bracket vs the BUY one (1 = equal)
#   buy_band_factor     widen/narrow the BUY bracket vs the SELL one (1 = equal)
#
# Tickers WITHOUT a file entry keep the plain InstrumentMonitor defaults and are
# only watchable via the interactive "add TICKER" command. reload_ticker_settings()
# populates TICKER_SETTINGS at boot. Pick widths from the excursion profile tool
# (history_candles_peak_inspect.py --profile): it reports how often 5M prices
# move more than X% past the MA(6).
# ---------------------------------------------------------------------------
TICKER_SETTINGS_FILE = "ticker_settings.json"

TICKER_SETTINGS = {}  # {ticker: normalized InstrumentMonitor config}

_CANDLE_INTERVAL_BY_NAME = {
	"1M": CandleInterval.CANDLE_INTERVAL_1_MIN,
	"5M": CandleInterval.CANDLE_INTERVAL_5_MIN,
	"15M": CandleInterval.CANDLE_INTERVAL_15_MIN,
	"30M": CandleInterval.CANDLE_INTERVAL_30_MIN,
	"H": CandleInterval.CANDLE_INTERVAL_HOUR,
	"2H": CandleInterval.CANDLE_INTERVAL_2_HOUR,
	"4H": CandleInterval.CANDLE_INTERVAL_4_HOUR,
	"D": CandleInterval.CANDLE_INTERVAL_DAY,
}

_DECIMAL_FIELDS = {"deviation_percent", "band_atr_mult", "sell_band_factor", "buy_band_factor"}
_INT_FIELDS = {"lots", "period", "hard_stop_dev_k"}
_BOOL_FIELDS = {"allow_buying", "allow_selling", "load_regime"}


def _normalize_entry(raw_entry):
	"""Convert raw JSON values into the types InstrumentMonitor expects."""
	entry = {}
	for name, value in raw_entry.items():
		if name in _DECIMAL_FIELDS:
			entry[name] = Decimal(str(value))
		elif name in _INT_FIELDS:
			entry[name] = int(value)
		elif name in _BOOL_FIELDS:
			entry[name] = bool(value)
		elif name == "candle_interval" and isinstance(value, str):
			try:
				entry[name] = _CANDLE_INTERVAL_BY_NAME[value.upper()]
			except KeyError:
				raise ValueError(
					f"Unknown candle_interval {value!r} in ticker_settings.json "
					f"(use one of {sorted(_CANDLE_INTERVAL_BY_NAME)})")
		else:
			entry[name] = value
	return entry


def load_ticker_settings(path=None):
	"""Read ticker_settings.json and return {TICKER: normalized config}.

	The file decides which tickers the monitor boots with. Returns {} when the
	file is absent so an unconfigured checkout keeps working (no instruments).
	Raises on malformed JSON / bad values so config errors are visible at boot.
	"""
	path = path or TICKER_SETTINGS_FILE
	try:
		with open(path, encoding="utf-8") as f:
			raw = json.load(f)
	except FileNotFoundError:
		return {}
	if not isinstance(raw, dict):
		raise ValueError(f"{path}: top level must be a JSON object of tickers")
	return {str(t).upper(): _normalize_entry(entry) for t, entry in raw.items()}


def reload_ticker_settings(path=None):
	"""(Re)load ticker_settings.json into the module-global TICKER_SETTINGS."""
	global TICKER_SETTINGS
	TICKER_SETTINGS = load_ticker_settings(path)
	return TICKER_SETTINGS


def apply_ticker_settings(instrument_monitor, ticker):
	"""Overlay the ticker_settings.json entry for `ticker` onto an InstrumentMonitor."""
	overrides = TICKER_SETTINGS.get(str(ticker).upper())
	if not overrides:
		return
	for name, value in overrides.items():
		if hasattr(instrument_monitor, name):
			setattr(instrument_monitor, name, value)


class InstrumentMonitor:
	def __init__(self, client, figi, type, candle_interval, period, lots=1, allow_buying=True, allow_selling=True,
				 load_regime=False, band_atr_mult=Decimal(0), hard_stop_dev_k=2,
				 deviation_percent=Decimal("0.0015"),
				 sell_band_factor=Decimal(1), buy_band_factor=Decimal(1)):
		self.client = client
		self.figi = figi
		self.type = type
		self.candle_interval = candle_interval
		self.bid = None	#quotation
		self.ask = None #quotation
		self.period = period	# period = candle_count
		self.candle_history = []
		self.min_price_increment = None
		self.ma = None
		self.deviation_percent = deviation_percent
		#	 asymmetric bands: the SELL/BUY bracket width is deviation*factor,
		#	 so a ticker can sell peaks wider than it buys dips (or vice versa)
		self.sell_band_factor = sell_band_factor
		self.buy_band_factor = buy_band_factor
		self.lots = lots
		self.hi_order = None
		self.lo_order = None
		self.sell_limit = None
		self.buy_limit = None
		# self.is_trading = True
		self.order_manager = None
		#	 behaviour at peak values
		self.allow_buying = allow_buying
		self.allow_selling = allow_selling
		#	 regime-adaptive bracket
		self.load_regime = load_regime
		self.band_atr_mult = band_atr_mult
		self.regime = None
		self.regime_break = False
		self.trend_ok = True
		self.ref_high = None
		self.worst_dd = Decimal(0)
		#	 position-aware stops (set by OrderMonitor.reconcile_stops)
		self.hard_stop_dev_k = hard_stop_dev_k
		self.hold_side = None	# "long", "short" or None (flat)


	async def update_candles(self):
		# if not self.is_trading:
		# 	print(f"Trying to update not trading instrument ({self.figi}).")
		# 	return False

		candles = []
		from_ = now() - candle_interval_to_timedelta(self.candle_interval) * self.period
		candles = (await self.client.market_data.get_candles(
			instrument_id=self.figi,
			from_=from_,
			to=now(),
			interval=self.candle_interval,
			candle_source_type=CandleSource.CANDLE_SOURCE_INCLUDE_WEEKEND
		)).candles
		# if no enough candles - increase from_ until len(candle) = period
		while len(candles) < self.period:
			from_ -= candle_interval_to_timedelta(self.candle_interval)
			log(f"Not enough candles for {self.figi} ({len(candles)}). Getting candles from {from_}")
			candles = (await self.client.market_data.get_candles(
				instrument_id=self.figi,
				from_=from_,
				to=now(),
				interval=self.candle_interval,
				candle_source_type=CandleSource.CANDLE_SOURCE_INCLUDE_WEEKEND
			)).candles

		self.candle_history = candles
		self.ma = MA(self.period, self.candle_history)
		log(f"{ticker_figi.ticker(self.figi)}: close={quotation_to_decimal(self.candle_history[-1].close)} MA({self.period})={self.ma}")
		return True


	def update_bid_ask(self, orderbook):
		try:
			self.bid = orderbook.bids[0].price
			self.ask = orderbook.asks[0].price
			log(f"{ticker_figi.ticker(self.figi)} bid/ask = {quotation_to_decimal(self.bid)} / {quotation_to_decimal(self.ask)}")
		except Exception as e:
			log(f"Cannot update bid/ask for {ticker_figi.ticker(self.figi)}: {e}")


	def ATR(self, period=14):
		"""Average True Range in price units over the current candle history."""
		hist = self.candle_history
		if len(hist) < period + 1:
			return None
		trs = []
		for i in range(1, len(hist)):
			h = quotation_to_decimal(hist[i].high)
			l = quotation_to_decimal(hist[i].low)
			pc = quotation_to_decimal(hist[i - 1].close)
			trs.append(max(h - l, abs(h - pc), abs(l - pc)))
		return sum(trs[-period:]) / period


	def update_regime(self, profile):
		self.regime = profile
		if profile is None:
			self.trend_ok = True
			self.regime_break = False
			return
		self.trend_ok = profile.trend_ok
		self.ref_high = profile.ref_high
		self.worst_dd = profile.worst_dd
		self.regime_break = False
		log(f"[green]Regime[/] {profile.describe()}")


	def check_regime_break(self):
		"""Regime break = current drawdown from the reference high is deeper
		than the worst dip that historically recovered. Disables buying."""
		if self.regime is None or not self.ref_high:
			self.regime_break = False
			return
		price = quotation_to_decimal(self.bid) if self.bid else None
		if price is None and self.candle_history:
			price = quotation_to_decimal(self.candle_history[-1].close)
		if price is None:
			return
		drawdown = price / self.ref_high - 1
		broke = drawdown < self.worst_dd
		if broke and not self.regime_break:
			log(f"[red]REGIME BREAK[/] {ticker_figi.ticker(self.figi)} "
				  f"drawdown={drawdown} < worst_dd={self.worst_dd}. Cancelling BUY bracket.")
		self.regime_break = broke


	def quantize(self, price_decimal):
		quantized_price_decimal = (price_decimal // quotation_to_decimal(self.min_price_increment)) * quotation_to_decimal(self.min_price_increment)
		quantized_price_quotation = decimal_to_quotation(quantized_price_decimal)
		log(f"Before quantizing: {price_decimal} / after: {quantized_price_quotation}")
		return quantized_price_quotation


	async def _move_order(self, order_id, price_quotation, order_type_str):
		# returns order_id
		min_difference = quotation_to_decimal(self.min_price_increment) * 3
		if not order_id:
			if order_type_str == "SELL":
				order_id = await self.selllimit(price_quotation)
				return order_id
			if order_type_str == "BUY":
				order_id = await self.buylimit(price_quotation)
				return order_id
		else:
			order = await self.order_manager.get_order(order_id)
			if order and order.execution_report_status == OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_NEW:
				price_difference = abs(money_to_decimal(order.initial_security_price) - quotation_to_decimal(price_quotation))
				log(f" {order_type_str} LIMIT order for {ticker_figi.ticker(self.figi)} price diff = {price_difference}, min diff = {min_difference}")
				if price_difference > min_difference:
					log("Order should be moved...")
					order_id = await self.order_manager.change_order(order_id, price_quotation, self.lots)
					return order_id
			else:
				# if order execution report is not new (i.e. order cancelled)
				# make order = None so it will be replaced with new
				return None

		# return unchanged
		return order_id


	# moved to InstrumentMonitor
	def band_deviation(self, side_factor=Decimal(1)):
		"""Deviation band width as a fraction of MA.

		Base is deviation_percent (default 0.15%), ATR-scaled when enabled.
		side_factor lets SELL and BUY brackets use different widths
		(see sell_band_factor / buy_band_factor / TICKER_SETTINGS).
		"""
		deviation = self.deviation_percent
		if self.band_atr_mult:
			atr = self.ATR()
			if atr and self.ma:
				deviation = max(deviation, self.band_atr_mult * atr / self.ma)
		return deviation * side_factor


	# moved to InstrumentMonitor
	async def move_orders(self):
		# if not self.is_trading:
		# 	return

		self.check_regime_break()
		sell_deviation = self.band_deviation(self.sell_band_factor)
		buy_deviation = self.band_deviation(self.buy_band_factor)

		if self.allow_selling and self.hold_side != "long":
			sell_quotation = self.quantize(self.ma + self.ma * sell_deviation)
			self.hi_order = await self._move_order(self.hi_order, sell_quotation, "SELL")
			self.sell_limit = quotation_to_decimal(sell_quotation) if self.hi_order else None
		elif self.hold_side == "long" and self.hi_order:
			# opposite bracket would double-sell the position: keep it cancelled
			log(f"[yellow]CANCELLED[/] SELL bracket for {ticker_figi.ticker(self.figi)} "
				  f"(holding long, managed by MA stops)")
			await self.order_manager.cancel_order(self.hi_order)
			self.hi_order = None
			self.sell_limit = None

		if self.allow_buying and self.trend_ok and not self.regime_break and self.hold_side != "short":
			buy_quotation = self.quantize(self.ma - self.ma * buy_deviation)
			self.lo_order = await self._move_order(self.lo_order, buy_quotation, "BUY")
			self.buy_limit = quotation_to_decimal(buy_quotation) if self.lo_order else None
		elif self.hold_side == "short" and self.lo_order:
			log(f"[yellow]CANCELLED[/] BUY bracket for {ticker_figi.ticker(self.figi)} "
				  f"(holding short, managed by MA stops)")
			await self.order_manager.cancel_order(self.lo_order)
			self.lo_order = None
			self.buy_limit = None
		elif not (self.allow_buying and self.trend_ok and not self.regime_break):
			if self.lo_order:
				log(f"[yellow]CANCELLED[/] BUY bracket for {ticker_figi.ticker(self.figi)} "
					  f"(trend_ok={self.trend_ok}, regime_break={self.regime_break})")
				await self.order_manager.cancel_order(self.lo_order)
				self.lo_order = None
				self.buy_limit = None


	# moved to InstrumentMonitor
	async def buy(self):
		figi = self.figi
		price = self.bid
		lots = self.lots
		log(f"BUY {ticker_figi.ticker(figi)} for {quotation_to_decimal(price)}")
		await asyncio.sleep(5)
		order_id = await self.order_manager.post_order(figi, price, OrderType.ORDER_TYPE_MARKET, OrderDirection.ORDER_DIRECTION_BUY, lots)
		if order_id:
			return order_id


	# moved to InstrumentMonitor
	async def buylimit(self, price_quotation):
		figi = self.figi
		lots = self.lots
		log(f"BUY STOP {ticker_figi.ticker(figi)} at {quotation_to_decimal(price_quotation)}")
		await asyncio.sleep(5)
		order_id = await self.order_manager.post_order(figi, price_quotation, OrderType.ORDER_TYPE_LIMIT, OrderDirection.ORDER_DIRECTION_BUY, lots)
		if order_id:
			return order_id


	# moved to InstrumentMonitor
	async def sell(self):
		figi = self.figi
		price = self.ask
		lots = self.lots
		log(f"SELL {ticker_figi.ticker(figi)} for {quotation_to_decimal(price)}")
		await asyncio.sleep(5)
		order_id = await self.order_manager.post_order(figi, price, OrderType.ORDER_TYPE_MARKET, OrderDirection.ORDER_DIRECTION_SELL, lots)
		if order_id:
			return order_id


	# moved to InstrumentMonitor
	async def selllimit(self, price_quotation):
		figi = self.figi
		lots = self.lots
		log(f"SELL STOP {ticker_figi.ticker(figi)} at {quotation_to_decimal(price_quotation)}")
		await asyncio.sleep(5)
		order_id = await self.order_manager.post_order(figi, price_quotation, OrderType.ORDER_TYPE_LIMIT, OrderDirection.ORDER_DIRECTION_SELL, lots)
		if order_id:
			return order_id



class LineReader(threading.Thread):
	"""Reads command lines in cbreak (raw, no echo) mode and reports them
	to the event loop via the on_line callback."""

	def __init__(self, on_line):
		super().__init__(daemon=True, name="line-reader")
		self.on_line = on_line
		self.buffer = ""
		self.running = True

	def run(self):
		fd = sys.stdin.fileno()
		old_attr = termios.tcgetattr(fd)
		try:
			tty.setcbreak(fd)
			attr = termios.tcgetattr(fd)
			attr[3] = attr[3] & ~termios.ECHO
			termios.tcsetattr(fd, termios.TCSANOW, attr)
			while self.running:
				r, _, _ = select.select([fd], [], [], 0.1)
				if not r:
					continue
				data = os.read(fd, 1)
				if not data:
					continue
				ch = data.decode(errors="ignore")
				if ch in ("\r", "\n"):
					line = self.buffer
					self.buffer = ""
					if line.strip():
						self.on_line(line.strip())
				elif ch in ("\x7f", "\b"):
					self.buffer = self.buffer[:-1]
				elif ch == "\x03":
					self.on_line("q")
					self.running = False
				elif ch == "\x1b":
					pass
				elif ch.isprintable():
					self.buffer += ch
		finally:
			termios.tcsetattr(fd, termios.TCSADRAIN, old_attr)

	def stop(self):
		self.running = False


class OrderMonitor(StreamMonitor):
	def __init__(self, client, order_manager, portfolio_refresh_seconds=10, stop_reconcile_seconds=300):
		super().__init__(client)
		self.instrument_list_by_figi = {}	# {figi: InstrumentMonitor}
		self.order_manager = order_manager
		self.account_manager = order_manager.account_manager
		self.last_market_response = None
		self.console = Console()
		self.live = None
		self.balance = Decimal(0)
		self.positions = []
		self.operations = []
		self.stop_by_ticker = {}
		self.status = ""
		self.prompt = ""
		self.reader = None
		self.portfolio_refresh_seconds = portfolio_refresh_seconds
		self.stop_reconcile_seconds = stop_reconcile_seconds
		self.stop_task = None
		self.loop = None
		...


	def add_instrument(self, instrument_monitor, ticker=None):
		instrument_monitor.order_manager = self.order_manager
		if ticker:
			apply_ticker_settings(instrument_monitor, ticker)
		self.instrument_list_by_figi[instrument_monitor.figi] = instrument_monitor


	# async def check_trading_statuses(self):
	# 	wanted_status = [SecurityTradingStatus.SECURITY_TRADING_STATUS_NORMAL_TRADING, SecurityTradingStatus.SECURITY_TRADING_STATUS_DEALER_NORMAL_TRADING]
	# 	figis = [figi for figi, i in self.instrument_list_by_figi.items()]
	# 	statuses = await self.client.market_data.get_trading_statuses(instrument_ids=figis)
	# 	for status in statuses.trading_statuses:
	# 		self.instrument_list_by_figi[status.figi].is_trading = status.trading_status in wanted_status
	# 		print(f"Trading status of {ticker_figi.ticker(status.figi)} is {status.trading_status.name}. Tradable = {self.instrument_list_by_figi[status.figi].is_trading}")


	async def get_user_input(self):
		self.loop = asyncio.get_running_loop()
		self.reader = LineReader(self._submit_command)
		self.reader.start()


	def _submit_command(self, line):
		asyncio.run_coroutine_threadsafe(self.process_user_action(line), self.loop)


	async def process_user_action(self, user_input):
		parts = user_input.split()
		cmd = parts[0].lower()
		arg = parts[1] if len(parts) > 1 else None

		if cmd in ("add",):
			if not arg:
				self.status = "Usage: add TICKER"
			else:
				await self.add_ticker(arg)
		elif cmd == "rm":
			if not arg:
				self.status = "Usage: rm TICKER"
			else:
				await self.remove_ticker(arg)
		elif cmd in ("b", "s"):
			figi, _ = await self._resolve_figi(arg) if arg else (self._last_figi(), None)
			if not figi:
				self.status = "Unknown ticker. Usage: b TICKER"
			else:
				inst = self.instrument_list_by_figi.get(figi)
				ticker = ticker_figi.ticker(figi) or figi
				if inst is None:
					self.status = f"{ticker} is not in the monitor. Use: add {ticker}"
				elif cmd == "b":
					await inst.buy()
					self.status = f"BUY market {ticker} placed"
				else:
					await inst.sell()
					self.status = f"SELL market {ticker} placed"
		elif cmd == "r":
			figi, _ = await self._resolve_figi(arg) if arg else (self._last_figi(), None)
			inst = self.instrument_list_by_figi.get(figi) if figi else None
			if not figi or inst is None:
				self.status = "Unknown ticker. Usage: r TICKER"
			elif getattr(inst, "load_regime", False):
				try:
					profile = await load_regime_profile(self.client, figi, ticker_figi.ticker(figi))
				except Exception as e:
					log(f"Regime reload failed for {ticker_figi.ticker(figi)}: {e}")
					self.status = f"Regime reload failed: {e}"
					profile = None
				inst.update_regime(profile)
				self.status = f"Regime reloaded for {ticker_figi.ticker(figi)}"
			else:
				self.status = "Regime loading not enabled for this instrument"
		elif cmd == "+":
			try:
				amount = Decimal(arg) if arg else Decimal("10000")
			except Exception:
				self.status = "Usage: + AMOUNT"
				return
			await self.account_manager.pay_in(amount)
			await self.refresh_portfolio()
			self.status = f"Paid in {amount} RUB"
		elif cmd in ("q", "quit"):
			self.status = "Quitting..."
			self.stop = True
			if self.stream:
				self.stream.stop()
			if self.reader:
				self.reader.stop()
		elif cmd in ("h", "help"):
			self.status = "add TICKER | rm TICKER | b TICKER | s TICKER | r TICKER | + AMOUNT | q | h"
		else:
			self.status = f"Unknown command: {cmd} (try h)"
		self.refresh()


	def _last_figi(self):
		try:
			return (self.last_market_response.orderbook or self.last_market_response.last_price or self.last_market_response.candle).figi
		except Exception:
			return None


	async def _resolve_figi(self, ticker):
		ticker = ticker.upper()
		figi = await etf_ticker_to_figi(self.client, ticker, verbose=False)
		if figi:
			return figi, "etf"
		return await share_ticker_to_figi(self.client, ticker, verbose=False), "share"


	async def _build_instrument(self, ticker):
		"""Resolve TICKER and build its InstrumentMonitor.

		Uses the ticker_settings.json entry when one exists (asset type selects
		the ticker->figi lookup, flags/widths are constructor args); otherwise
		falls back to the plain monitor defaults. Raises LookupError when the
		ticker cannot be found."""
		ticker = ticker.upper()
		cfg = TICKER_SETTINGS.get(ticker, {})
		asset_type = cfg.get("type")
		if asset_type == "etf":
			figi = await etf_ticker_to_figi(self.client, ticker, verbose=False)
		elif asset_type == "share":
			figi = await share_ticker_to_figi(self.client, ticker, verbose=False)
		else:
			figi, asset_type = await self._resolve_figi(ticker)
		if not figi:
			raise LookupError(f"Ticker {ticker} not found")
		instrument = InstrumentMonitor(
			client=self.client,
			figi=figi,
			type=asset_type,
			candle_interval=cfg.get("candle_interval", CandleInterval.CANDLE_INTERVAL_5_MIN),
			period=int(cfg.get("period", 6)),
			lots=int(cfg.get("lots", 1)),
			allow_buying=cfg.get("allow_buying", True),
			allow_selling=cfg.get("allow_selling", True),
			load_regime=cfg.get("load_regime", True),
			band_atr_mult=cfg.get("band_atr_mult", Decimal("0.5")),
			hard_stop_dev_k=int(cfg.get("hard_stop_dev_k", 2)),
			deviation_percent=cfg.get("deviation_percent", Decimal("0.0015")),
			sell_band_factor=cfg.get("sell_band_factor", Decimal(1)),
			buy_band_factor=cfg.get("buy_band_factor", Decimal(1)),
		)
		return figi, instrument


	async def load_instruments_from_settings(self):
		"""Add every ticker listed in ticker_settings.json.

		The settings file is the boot source of truth: this is how the monitor
		decides which instruments to watch, instead of a hardcoded list."""
		for ticker in TICKER_SETTINGS:
			try:
				figi, instrument = await self._build_instrument(ticker)
			except LookupError as e:
				self.status = str(e)
				log(str(e))
				continue
			if figi in self.instrument_list_by_figi:
				continue
			self.add_instrument(instrument, ticker=ticker)
			log(f"Loaded {ticker} from {TICKER_SETTINGS_FILE}")
		return len(self.instrument_list_by_figi)


	async def add_ticker(self, ticker):
		ticker = ticker.upper()
		try:
			figi, instrument = await self._build_instrument(ticker)
		except LookupError as e:
			self.status = str(e)
			return
		if figi in self.instrument_list_by_figi:
			self.status = f"{ticker} is already in the monitor"
			return
		self.add_instrument(instrument, ticker=ticker)
		try:
			await self._init_single(instrument)
		except Exception as e:
			log(f"init failed for {ticker}: {e}")
			del self.instrument_list_by_figi[figi]
			self.status = f"init failed for {ticker}: {e}"
			return
		try:
			# prime hi/lo brackets immediately (candles then keep moving them)
			await instrument.move_orders()
		except Exception as e:
			log(f"Initial move_orders failed for {ticker}: {e}")
		self.status = f"Added {ticker}"
		if self.stream:
			self.stream.stop()
		# the new instrument may already have an open position: set up its
		# MA exits / hard stop right away instead of waiting for the loop
		try:
			await self.reconcile_stops()
		except Exception as e:
			log(f"reconcile after add failed for {ticker}: {e}")


	async def remove_ticker(self, ticker):
		"""Stop monitoring TICKER and cancel every order resting for it:
		the hi/lo brackets plus any MA-exit / hard-stop orders (whether or
		not a position is open). Leaves any position itself untouched."""
		ticker = ticker.upper()
		figi, _ = await self._resolve_figi(ticker)
		inst = self.instrument_list_by_figi.get(figi) if figi else None
		if not figi or inst is None:
			self.status = f"{ticker} is not in the monitor"
			return

		self.instrument_list_by_figi.pop(figi)
		cancelled = []

		if inst.hi_order:
			try:
				await self.order_manager.cancel_order_silent(inst.hi_order)
				cancelled.append("sell_limit")
			except Exception as e:
				log(f"rm {ticker}: cancel sell_limit failed: {e}")
			inst.hi_order = None
			inst.sell_limit = None
		if inst.lo_order:
			try:
				await self.order_manager.cancel_order_silent(inst.lo_order)
				cancelled.append("buy_limit")
			except Exception as e:
				log(f"rm {ticker}: cancel buy_limit failed: {e}")
			inst.lo_order = None
			inst.buy_limit = None

		try:
			active_stops = await self.order_manager.get_active_stop_orders()
		except Exception as e:
			log(f"rm {ticker}: cannot list stop orders: {e}")
			active_stops = []
		for so in active_stops:
			if so.figi == figi:
				try:
					await self.order_manager.cancel_stop_order(so.stop_order_id)
					cancelled.append(f"{so.order_type.name.replace('STOP_ORDER_TYPE_', '')}")
				except Exception as e:
					log(f"rm {ticker}: cancel stop {so.stop_order_id} failed: {e}")

		self.stop_by_ticker.pop(ticker, None)
		self.stop_by_ticker.pop(ticker_figi.ticker(figi), None)

		# reconnect the market stream without this instrument
		if self.stream:
			self.stream.stop()

		if cancelled:
			self.status = f"Removed {ticker} (cancelled: {', '.join(dict.fromkeys(cancelled))})"
		else:
			self.status = f"Removed {ticker} (no resting orders)"


	async def process_stream_response(self, market_response, figi):
		if figi not in self.instrument_list_by_figi:
			# instrument was removed while the stream was still subscribed
			return
		r = market_response

		if r.orderbook:
			self.instrument_list_by_figi[figi].update_bid_ask(r.orderbook)
			self.refresh()

		elif r.last_price:
			log(f"{ticker_figi.ticker(figi)}: last price = {quotation_to_decimal(r.last_price.price)}")
			self.refresh()

		elif r.candle:
			log(f"{ticker_figi.ticker(figi)} has finished candle ({r.candle.interval.name})")
			await self.instrument_list_by_figi[figi].update_candles()
			await self.instrument_list_by_figi[figi].move_orders()
			self.refresh()


	# ------------------------------------------------ dashboard

	def start_dashboard(self):
		self.loop = asyncio.get_running_loop()
		self.live = Live(self._render_dashboard(), console=self.console, refresh_per_second=5)
		self.live.start()
		self.refresh()


	def refresh(self):
		if self.live:
			self.live.update(self._render_dashboard())


	def _render_dashboard(self):
		views = []
		for figi, inst in self.instrument_list_by_figi.items():
			views.append(SimpleNamespace(
				ticker=ticker_figi.ticker(figi) or figi,
				period=inst.period,
				ma=inst.ma,
				sell_limit=inst.sell_limit,
				buy_limit=inst.buy_limit,
				bid=quotation_to_decimal(inst.bid) if inst.bid else None,
				ask=quotation_to_decimal(inst.ask) if inst.ask else None,
			))
		return build_dashboard(views, self.balance, self.positions, self.operations,
							   status=self.status, prompt=self.reader.buffer if self.reader else "")


	async def refresh_operations(self, days=7):
		try:
			operations = await self.account_manager.get_account_operations(
				from_=now() - timedelta(days=days),
				to=now()
			)
			operations = sorted(operations, key=lambda op: op.date, reverse=True)
			self.operations = [_operation_view(op) for op in operations]
		except Exception as e:
			log(f"Operations refresh failed: {e}")


	async def refresh_portfolio(self):
		try:
			self.balance = await self.account_manager.get_balance()
			positions = await self.account_manager.get_portfolio_positions()
			self.positions = [self._position_view(p) for p in positions
							  if p.instrument_type != "currency"]
			for view in self.positions:
				view["stop"] = self.stop_by_ticker.get(view["ticker"])
		except Exception as e:
			log(f"Portfolio refresh failed: {e}")


	def _position_view(self, p):
		avg = money_to_decimal(p.average_position_price_fifo)
		current = money_to_decimal(p.current_price)
		quantity = quotation_to_decimal(p.quantity)
		return {
			"ticker": p.ticker,
			"lots": quotation_to_decimal(p.quantity_lots),
			"avg": avg,
			"current": current,
			"profit": (current - avg) * quantity,
		}


	async def _portfolio_loop(self):
		while not self.stop:
			await self.refresh_portfolio()
			await self.refresh_operations()
			self.refresh()
			await asyncio.sleep(self.portfolio_refresh_seconds)


	# ------------------------------------------------ position exits

	async def reconcile_stops(self):
		"""Manage MA(6) exits and hard stops for open positions.

		Every cycle we fetch open positions and active stop orders, then keep
		the position protected around the current MA(6):
		- while the market is on the reversion side of the MA the MA level is
		  the take-profit target;
		- once the position is in profit beyond the MA the MA level becomes a
		  trailing stop-loss;
		- a hard stop-loss rests a few deviations beyond the average entry
		  price in the reversion zone.
		Stops that no longer correspond to an open position are cancelled and
		the opposite bracket is suppressed while the position is held.
		"""
		try:
			positions = await self.account_manager.get_portfolio_positions()
			active_stops = await self.order_manager.get_active_stop_orders()
		except Exception as e:
			log(f"Stop reconcile: cannot fetch state: {e}")
			return

		positions_by_figi = {p.figi: p for p in positions
							 if p.instrument_type != "currency"}
		stops_by_figi = {}
		for so in active_stops:
			stops_by_figi.setdefault(so.figi, []).append(so)

		figis = set(self.instrument_list_by_figi) | set(stops_by_figi)
		self.stop_by_ticker = {}
		for figi in figis:
			try:
				await self._reconcile_one_stop(
					figi,
					self.instrument_list_by_figi.get(figi),
					positions_by_figi.get(figi),
					stops_by_figi.get(figi, []),
				)
			except Exception as e:
				log(f"Stop reconcile failed for {figi}: {e}")


	async def _reconcile_one_stop(self, figi, inst, pos, stops):
		ticker = pos.ticker if pos and pos.ticker else (ticker_figi.ticker(figi) or figi)
		quantity = quotation_to_decimal(pos.quantity) if pos else Decimal(0)
		position_lots = quotation_to_decimal(pos.quantity_lots) if pos else Decimal(0)

		def _cancel(so, reason):
			log(f"Stop reconcile: {reason} {ticker} {so.direction.name} "
				f"{so.order_type.name} x{so.lots_requested} at {money_to_decimal(so.stop_price)}")

		if quantity == 0:
			# flat -> no MA exit / hard stop may linger
			if inst is not None:
				inst.hold_side = None
			for so in stops:
				await self.order_manager.cancel_stop_order(so.stop_order_id)
				_cancel(so, "cancelled orphan stop (no position)")
			return

		if inst is None:
			# position on an instrument we do not track the MA of: leave alone
			return

		lot_count = abs(position_lots)
		if lot_count == 0 or lot_count != lot_count.to_integral_value():
			log(f"Stop reconcile: cannot derive whole lot count for {ticker} "
				f"(quantity={quantity}, lots={position_lots})")
			return

		lots = int(lot_count)
		is_long = quantity > 0
		inst.hold_side = "long" if is_long else "short"

		# while a position is managed by MA stops, the opposite bracket could
		# double-close it later (fill the position, then the bracket re-sells):
		# cancel it here and keep it cancelled via move_orders while held.
		if is_long and inst.hi_order:
			await self.order_manager.cancel_order_silent(inst.hi_order)
			log(f"Stop reconcile: cancelled opposite SELL bracket for {ticker} (holding long)")
			inst.hi_order = None
			inst.sell_limit = None
		elif not is_long and inst.lo_order:
			await self.order_manager.cancel_order_silent(inst.lo_order)
			log(f"Stop reconcile: cancelled opposite BUY bracket for {ticker} (holding short)")
			inst.lo_order = None
			inst.buy_limit = None

		if inst.ma is None:
			log(f"Stop reconcile: no tracked MA for {ticker}, stops left as-is")
			return

		ma = inst.ma
		bid = quotation_to_decimal(inst.bid) if inst.bid else None
		ask = quotation_to_decimal(inst.ask) if inst.ask else None
		close = quotation_to_decimal(inst.candle_history[-1].close) if inst.candle_history else None
		if bid is None and ask is None and close is None:
			log(f"Stop reconcile: no market price for {ticker}")
			return
		low = bid if bid is not None else (ask if ask is not None else close)
		high = ask if ask is not None else (bid if bid is not None else close)
		mid = close if close is not None else (low + high) / 2

		avg = money_to_decimal(pos.average_position_price_fifo) if pos else Decimal(0)
		min_step = quotation_to_decimal(inst.min_price_increment) if inst.min_price_increment else Decimal("0.01")
		tolerance = min_step * 3
		dev = inst.band_deviation()

		def _away(level, above_market):
			# keep the resting order safely on its side of the market
			g = level // min_step * min_step
			if above_market and g != level:
				g += min_step
			return g

		# desired stops: (label, stop_order_type, direction, level)
		desired = []
		if is_long:
			if ma > mid:
				# dip zone: price is expected to revert back up to the MA
				if ma > high:
					desired.append(("MA exit", StopOrderType.STOP_ORDER_TYPE_TAKE_PROFIT,
									StopOrderDirection.STOP_ORDER_DIRECTION_SELL, _away(ma, True)))
				if avg > 0:
					hard = _away(avg - avg * dev * inst.hard_stop_dev_k, False)
					if hard < low:
						desired.append(("hard stop", StopOrderType.STOP_ORDER_TYPE_STOP_LOSS,
										StopOrderDirection.STOP_ORDER_DIRECTION_SELL, hard))
			elif ma < low:
				# in profit above the MA: trail out rather than give it back
				desired.append(("MA exit", StopOrderType.STOP_ORDER_TYPE_STOP_LOSS,
								StopOrderDirection.STOP_ORDER_DIRECTION_SELL, _away(ma, False)))
		else:
			if ma < mid:
				# rip zone: price is expected to revert back down to the MA
				if ma < low:
					desired.append(("MA exit", StopOrderType.STOP_ORDER_TYPE_TAKE_PROFIT,
									StopOrderDirection.STOP_ORDER_DIRECTION_BUY, _away(ma, False)))
				if avg > 0:
					hard = _away(avg + avg * dev * inst.hard_stop_dev_k, True)
					if hard > high:
						desired.append(("hard stop", StopOrderType.STOP_ORDER_TYPE_STOP_LOSS,
										StopOrderDirection.STOP_ORDER_DIRECTION_BUY, hard))
			elif ma > high:
				# in profit below the MA: trail out rather than give it back
				desired.append(("MA exit", StopOrderType.STOP_ORDER_TYPE_STOP_LOSS,
								StopOrderDirection.STOP_ORDER_DIRECTION_BUY, _away(ma, True)))

		active = [False] * len(desired)
		rest = list(stops)
		for i, (_label, otype, direction, level) in enumerate(desired):
			for j, so in enumerate(rest):
				if (so.direction == direction and so.order_type == otype
						and so.lots_requested == lots
						and abs(money_to_decimal(so.stop_price) - level) <= tolerance):
					active[i] = True
					del rest[j]
					break

		for so in rest:
			await self.order_manager.cancel_stop_order(so.stop_order_id)
			_cancel(so, "cancelled stale/duplicate stop")

		display = None
		for i, (label, otype, direction, level) in enumerate(desired):
			if active[i]:
				display = level if label == "MA exit" else (display or level)
				continue
			stop_id = await self.order_manager.post_stop_order(
				figi=figi, stop_price_decimal=level, direction=direction, lots=lots,
				stop_order_type=otype)
			if stop_id:
				active[i] = True
				display = level if label == "MA exit" else (display or level)
				log(f"Stop reconcile: placed {label} {direction.name} {otype.name} "
					f"x{lots} at {level} for {ticker}")
			else:
				log(f"Stop reconcile: FAILED to place {label} {direction.name} "
					f"{otype.name} x{lots} at {level} for {ticker}")
		if display is not None:
			self.stop_by_ticker[ticker] = display


	async def _stop_loop(self):
		while not self.stop:
			try:
				await self.reconcile_stops()
			except Exception as e:
				log(f"Stop loop error: {e}")
			await asyncio.sleep(self.stop_reconcile_seconds)


	async def _init_single(self, instrument):
		figi = instrument.figi
		await instrument.update_candles()
		try:
			if instrument.type == "etf":
				broker_instrument = (await self.client.instruments.etf_by(id=figi, id_type=InstrumentIdType.INSTRUMENT_ID_TYPE_FIGI)).instrument
			elif instrument.type == "share":
				broker_instrument = (await self.client.instruments.share_by(id=figi, id_type=InstrumentIdType.INSTRUMENT_ID_TYPE_FIGI)).instrument
			else:
				broker_instrument = None
		except Exception as e:
			log(f"_init_single: cannot set broker_instrument")
			raise(e)


		if broker_instrument:
			instrument.min_price_increment = broker_instrument.min_price_increment

		if getattr(instrument, "load_regime", False):
			try:
				profile = await load_regime_profile(self.client, figi, ticker_figi.ticker(figi))
			except Exception as e:
				log(f"Regime load failed for {ticker_figi.ticker(figi)}: {e}")
				profile = None
			instrument.update_regime(profile)


	async def init_instruments(self):
		# await self.check_trading_statuses()
		for figi, i in self.instrument_list_by_figi.items():
			await self._init_single(i)

		# get hi/lo orders
		orders = await self.order_manager.list_orders()
		for order in orders:
			if (order.order_type == OrderType.ORDER_TYPE_LIMIT and order.execution_report_status == OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_NEW):
				if not (order.figi in self.instrument_list_by_figi):
					log(f"[red bold]UNTRACKED[/] order for [bold]{ticker_figi.ticker(order.figi)}[/] {order.direction.name} at {order.initial_security_price} x {order.lots_requested}")
					continue

				instrument = self.instrument_list_by_figi[order.figi]
				if order.direction == OrderDirection.ORDER_DIRECTION_BUY:
					if not instrument.lo_order:
						if instrument.allow_buying:
							instrument.lo_order = order.order_id
							instrument.buy_limit = money_to_decimal(order.initial_security_price)
							log(f"[green bold]FOUND[/] lo_order ([bold blue]BUY[/]) for [bold]{ticker_figi.ticker(order.figi)}[/] at {order.initial_security_price} x {order.lots_requested}")
						else:
							await self.order_manager.cancel_order(order.order_id)
							log(f"[yellow bold]CANCELLED[/] [bold blue]BUY[/] for [bold]{ticker_figi.ticker(order.figi)}[/] at {order.initial_security_price} x {order.lots_requested}. Buying not allowed")
					else:
						# more than one BUY order
						await self.order_manager.cancel_order(order.order_id)
						log(f"[yellow bold]CANCELLED[/] dup lo_order ([bold blue]BUY[/]) for [bold]{ticker_figi.ticker(order.figi)}[/] at {order.initial_security_price} x {order.lots_requested}")
				elif order.direction == OrderDirection.ORDER_DIRECTION_SELL:
					if not instrument.hi_order:
						if instrument.allow_selling:
							instrument.hi_order = order.order_id
							instrument.sell_limit = money_to_decimal(order.initial_security_price)
							log(f"[green bold]FOUND[/] hi_order ([bold red]SELL[/]) for [bold]{ticker_figi.ticker(order.figi)}[/] at {order.initial_security_price} x {order.lots_requested}")
						else:
							await self.order_manager.cancel_order(order.order_id)
							log(f"[yellow bold]CANCELLED[/] [bold red]SELL[/] for [bold]{ticker_figi.ticker(order.figi)}[/] at {order.initial_security_price} x {order.lots_requested}. Selling not allowed")
					else:
						# more than one SELL order
						await self.order_manager.cancel_order(order.order_id)
						log(f"[yellow bold]CANCELLED[/] dup hi_order ([bold red]SELL[/]) for [bold]{ticker_figi.ticker(order.figi)}[/] at {order.initial_security_price} x {order.lots_requested}")
		# prime hi/lo brackets now instead of waiting for the first candle
		# close (may be minutes away / never on a closed market); candles keep
		# moving them afterwards
		for figi, instrument in self.instrument_list_by_figi.items():
			try:
				await instrument.move_orders()
			except Exception as e:
				log(f"Initial move_orders failed for {ticker_figi.ticker(figi)}: {e}")
		self.refresh()


	async def _monitor(self):
		self.stream = self.client.create_market_data_stream()
		trade_instruments = [TradeInstrument(instrument_id=figi) for figi in self.instrument_list_by_figi.keys()]
		candle_instruments = [CandleInstrument(instrument_id=figi, interval=i.candle_interval) for figi, i in self.instrument_list_by_figi.items()]
		orderbook_instruments = [OrderBookInstrument(instrument_id=figi, depth=1) for figi in self.instrument_list_by_figi.keys()]	# for real bid/ask prices

		self.stream.order_book.subscribe(orderbook_instruments)
		self.stream.last_price.subscribe(trade_instruments)
		self.stream.candles.waiting_close(enabled=True).subscribe(candle_instruments)	# cannot specify candle_source_type=CandleSource.CANDLE_SOURCE_INCLUDE_WEEKEND
		try:
			# first response returns value "SubscribeTradesResponse(..)"
			async for r in self.stream:
				if not self.stop:
					if (r.orderbook or r.last_price or r.candle):
						figi = (r.orderbook or r.last_price or r.candle).figi
					else:
						log("[red] Unsupported kind of stream data")
						continue

					self.last_market_response = r
					await self.process_stream_response(r, figi)

		# stream error shouldn't stop monitor
		# exceptions handled in parent class within while loop with retrying logic
		finally:
			self.last_market_response = None
			self.stream.stop()



async def test_order_monitor():
	load_dotenv()
	# ticker_settings.json decides which tickers (and their settings) to boot
	settings = reload_ticker_settings()
	if not settings:
		log("No tickers in ticker_settings.json - booting with an empty monitor "
			"(use the interactive \"add TICKER\" command at runtime).")
	async with AsyncSandboxClient(os.environ["T_INVEST_TOKEN_SANDBOX"]) as client:
		account_manager = await AccountManagerSandbox(client).connect()
		order_manager = OrderManagerSandbox(client, account_manager)
		order_monitor = OrderMonitor(client, order_manager)
		await order_monitor.load_instruments_from_settings()
		order_monitor.status = f"Initializing {len(order_monitor.instrument_list_by_figi)} instrument(s) from {TICKER_SETTINGS_FILE}..."
		order_monitor.start_dashboard()
		order_monitor.refresh()
		order_monitor.portfolio_task = asyncio.create_task(order_monitor._portfolio_loop())
		order_monitor.input_task = asyncio.create_task(order_monitor.get_user_input())

		async def boot():
			try:
				await order_monitor.init_instruments()
				order_monitor.status = "Starting market stream..."
				order_monitor.refresh()
				order_monitor.running_task = asyncio.create_task(order_monitor.monitor())
				order_monitor.stop_task = asyncio.create_task(order_monitor._stop_loop())
			except Exception as e:
				log(f"boot failed: {e}")
				order_monitor.status = f"boot failed: {e}"

		asyncio.create_task(boot())
		try:
			while True:
				await asyncio.sleep(1)
		finally:
			if order_monitor.live:
				order_monitor.live.stop()
			if order_monitor.reader:
				order_monitor.reader.stop()


if __name__ == "__main__":
	ticker_figi.init()
	try:
		# asyncio.run(test_order_manager())
		asyncio.run(test_order_monitor())
	except KeyboardInterrupt:
		print("KeyboardInterrupt handled")

"""
Currently moving stops are the only way to handle market spikes.
But there's no guarantee that such limit orders will fire.

28.04.2026
Candles are updated later, after trade moment. How to use trade events in trading?

07.05.2026
Instruments update their trading status only on startup and never refreshes it
Trading status is not needed? When the instrument is not trading - there's no any stream data for it

15.05.2026
Make closing positions at MA
Make limiting some instruments to only buy and only sell
Monitor limit orders firing by OrdersStreamService (order_state_stream (?))

16.05.2026
Widely test OrderManager on mock trading data before making TUI with Rich.table Rich.layout and Rich.live

"""
