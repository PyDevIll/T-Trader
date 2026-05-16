# t_order_manager.py

import t_services
import asyncio
import sys

from datetime import datetime, timedelta, time
from decimal import Decimal

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
	ReplaceOrderRequest
)
from t_tech.invest.exceptions import AioRequestError
import json
_print = print
from rich import inspect, print
from rich.prompt import Prompt
from dotenv import load_dotenv
import os
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


def MA(period, candle_history):
	if len(candle_history) < period:
		return None
	else:
		value = reduce(lambda a, v: a + v, [quotation_to_decimal(c.close) for c in candle_history[-period:]]) / period
		return value


class InstrumentMonitor:
	def __init__(self, client, figi, type, candle_interval, period, lots=1, allow_buying=True, allow_selling=True):
		self.client = client
		self.figi = figi
		self.type = type
		self.candle_interval = candle_interval
		self.bid = None	#quotation
		self.ask = None #quotation
		self.period = period	# period = candle_count
		self.candle_history = []
		self.min_price_increment = None
		self.ma = Decimal(0)
		self.deviation_percent = Decimal(0.0015) # 0.15%
		self.lots = lots
		self.hi_order = None
		self.lo_order = None
		# self.is_trading = True
		self.order_manager = None
		#	 behaviour at peak values
		self.allow_buying = allow_buying
		self.allow_selling = allow_selling


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
			print(f"Not enough candles for {self.figi} ({len(candles)}). Getting candles from {from_}")
			candles = (await self.client.market_data.get_candles(
				instrument_id=self.figi,
				from_=from_,
				to=now(),
				interval=self.candle_interval,
				candle_source_type=CandleSource.CANDLE_SOURCE_INCLUDE_WEEKEND
			)).candles

		self.candle_history = candles
		self.ma = MA(self.period, self.candle_history)
		print(f"{ticker_figi.ticker(self.figi)}: {quotation_to_decimal(self.candle_history[-1].close)}")
		print(f"MA({self.period}) = {self.ma}")
		return True


	def update_bid_ask(self, orderbook):
		try:
			self.bid = orderbook.bids[0].price
			self.ask = orderbook.asks[0].price
			print(f"{ticker_figi.ticker(self.figi)} bid/ask = {quotation_to_decimal(self.bid)} / {quotation_to_decimal(self.ask)}")
		except Exception as e:
			print(f"Cannot update bid/ask for {ticker_figi.ticker(self.figi)}: {e}")


	def quantize(self, price_decimal):
		quantized_price_decimal = (price_decimal // quotation_to_decimal(self.min_price_increment)) * quotation_to_decimal(self.min_price_increment)
		quantized_price_quotation = decimal_to_quotation(quantized_price_decimal)
		print(f"Before quantizing: {price_decimal} / after: {quantized_price_quotation}")
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
				print(f" {order_type_str} LIMIT order for {ticker_figi.ticker(self.figi)} price diff = {price_difference}, min diff = {min_difference}")
				if price_difference > min_difference:
					print("Order should be moved...")
					order_id = await self.order_manager.change_order(order_id, price_quotation, self.lots)
					return order_id
			else:
				# if order execution report is not new (i.e. order cancelled)
				# make order = None so it will be replaced with new
				return None

		# return unchanged
		return order_id


	# moved to InstrumentMonitor
	async def move_orders(self):
		# if not self.is_trading:
		# 	return

		sell_quotation = self.quantize(self.ma + self.ma * self.deviation_percent)
		buy_quotation = self.quantize(self.ma - self.ma * self.deviation_percent)
		if self.allow_selling:
			self.hi_order = await self._move_order(self.hi_order, sell_quotation, "SELL")
		if self.allow_buying:
			self.lo_order = await self._move_order(self.lo_order, buy_quotation, "BUY")


	# moved to InstrumentMonitor
	async def buy(self):
		figi = self.figi
		price = self.bid
		lots = self.lots
		print(f"BUY {ticker_figi.ticker(figi)} for {quotation_to_decimal(price)}")
		await asyncio.sleep(5)
		order_id = await self.order_manager.post_order(figi, price, OrderType.ORDER_TYPE_MARKET, OrderDirection.ORDER_DIRECTION_BUY, lots)
		if order_id:
			return order_id


	# moved to InstrumentMonitor
	async def buylimit(self, price_quotation):
		figi = self.figi
		lots = self.lots
		print(f"BUY STOP {ticker_figi.ticker(figi)} at {quotation_to_decimal(price_quotation)}")
		await asyncio.sleep(5)
		order_id = await self.order_manager.post_order(figi, price_quotation, OrderType.ORDER_TYPE_LIMIT, OrderDirection.ORDER_DIRECTION_BUY, lots)
		if order_id:
			return order_id


	# moved to InstrumentMonitor
	async def sell(self):
		figi = self.figi
		price = self.ask
		lots = self.lots
		print(f"SELL {ticker_figi.ticker(figi)} for {quotation_to_decimal(price)}")
		await asyncio.sleep(5)
		order_id = await self.order_manager.post_order(figi, price, OrderType.ORDER_TYPE_MARKET, OrderDirection.ORDER_DIRECTION_SELL, lots)
		if order_id:
			return order_id


	# moved to InstrumentMonitor
	async def selllimit(self, price_quotation):
		figi = self.figi
		lots = self.lots
		print(f"SELL STOP {ticker_figi.ticker(figi)} at {quotation_to_decimal(price_quotation)}")
		await asyncio.sleep(5)
		order_id = await self.order_manager.post_order(figi, price_quotation, OrderType.ORDER_TYPE_LIMIT, OrderDirection.ORDER_DIRECTION_SELL, lots)
		if order_id:
			return order_id



class OrderMonitor(StreamMonitor):
	def __init__(self, client, order_manager):
		super().__init__(client)
		self.instrument_list_by_figi = {}	# {figi: InstrumentMonitor}
		self.order_manager = order_manager
		self.account_manager = order_manager.account_manager
		self.last_market_response = None
		self.last_user_input = None
		...


	def add_instrument(self, instrument_monitor):
		instrument_monitor.order_manager = self.order_manager
		self.instrument_list_by_figi[instrument_monitor.figi] = instrument_monitor


	# async def check_trading_statuses(self):
	# 	wanted_status = [SecurityTradingStatus.SECURITY_TRADING_STATUS_NORMAL_TRADING, SecurityTradingStatus.SECURITY_TRADING_STATUS_DEALER_NORMAL_TRADING]
	# 	figis = [figi for figi, i in self.instrument_list_by_figi.items()]
	# 	statuses = await self.client.market_data.get_trading_statuses(instrument_ids=figis)
	# 	for status in statuses.trading_statuses:
	# 		self.instrument_list_by_figi[status.figi].is_trading = status.trading_status in wanted_status
	# 		print(f"Trading status of {ticker_figi.ticker(status.figi)} is {status.trading_status.name}. Tradable = {self.instrument_list_by_figi[status.figi].is_trading}")


	async def get_user_input(self):
		while True:
			user_input = await asyncio.to_thread(input, "")
			await self.process_user_action(user_input)


	async def process_user_action(self, user_input):
		if not self.last_market_response:
			return
		market_response = self.last_market_response

		try:
			figi = (market_response.orderbook or market_response.last_price or market_response.candle).figi
		except:
			print("Market response has no figi defined")
			inspect(market_response)
			input()

		print(f"You've entered '{user_input}'")
		if user_input == 'b':
			print(f'BUYING {ticker_figi.ticker(figi)}')
			await self.instrument_list_by_figi[figi].buy()
		elif user_input == 's':
			print(f'SELLING {ticker_figi.ticker(figi)}')
			await self.instrument_list_by_figi[figi].sell()
		elif user_input == 'l':
			print('LISTING ORDERS:')
			await self.order_manager.list_orders()
		elif user_input == '+':
			amount = Decimal(Prompt.ask("Pay in amount (10000)", default="10000"))
			await self.account_manager.pay_in(amount)
			print(await self.account_manager.get_balance())
		elif user_input == 'o':
			await self.account_manager.account_operations(from_=now() - timedelta(days=1))
		elif user_input == 'a':
			print(await self.account_manager.get_balance())
		elif user_input == 'p':
			await self.account_manager.get_positions()
		print("___\n")


	async def process_stream_response(self, market_response, figi):
		r = market_response

		if r.orderbook:
			self.instrument_list_by_figi[figi].update_bid_ask(r.orderbook)

		elif r.last_price:
			print(f"{ticker_figi.ticker(figi)}: last price = {quotation_to_decimal(r.last_price.price)}")

		elif r.candle:
			print(f"{ticker_figi.ticker(figi)} has finished candle ({r.candle.interval.name})")
			# just append?
			await self.instrument_list_by_figi[figi].update_candles()
			await self.instrument_list_by_figi[figi].move_orders()


	async def init_instruments(self):
		# await self.check_trading_statuses()
		for figi, i in self.instrument_list_by_figi.items():
			await i.update_candles()
			if i.type == "etf":
				broker_instrument = (await self.client.instruments.etf_by(id=figi, id_type=InstrumentIdType.INSTRUMENT_ID_TYPE_FIGI)).instrument
			elif i.type == "share":
				broker_instrument = (await self.client.instruments.share_by(id=figi, id_type=InstrumentIdType.INSTRUMENT_ID_TYPE_FIGI)).instrument

			i.min_price_increment = broker_instrument.min_price_increment
			print(f"{ticker_figi.ticker(figi)} min price increment = {i.min_price_increment}")

		# get hi/lo orders
		orders = await self.order_manager.list_orders()
		for order in orders:
			if (order.order_type == OrderType.ORDER_TYPE_LIMIT and order.execution_report_status == OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_NEW):
				if not (order.figi in self.instrument_list_by_figi):
					print(f"[red bold]UNTRACKED[/] order for [bold]{ticker_figi.ticker(order.figi)}[/] {order.direction.name} at {order.initial_security_price} x {order.lots_requested}")
					continue

				if order.direction == OrderDirection.ORDER_DIRECTION_BUY:
					if not self.instrument_list_by_figi[order.figi].lo_order:
						if self.allow_buying:
							self.instrument_list_by_figi[order.figi].lo_order = order.order_id
							print(f"[green bold]FOUND[/] lo_order ([bold blue]BUY[/]) for [bold]{ticker_figi.ticker(order.figi)}[/] at {order.initial_security_price} x {order.lots_requested}")
						else:
							await self.order_manager.cancel_order(order.order_id)
							print(f"[yellow bold]CANCELLED[/] [bold blue]BUY[/] for [bold]{ticker_figi.ticker(order.figi)}[/] at {order.initial_security_price} x {order.lots_requested}. Buying not allowed")
					else:
						# more than one BUY order
						await self.order_manager.cancel_order(order.order_id)
						print(f"[yellow bold]CANCELLED[/] dup lo_order ([bold blue]BUY[/]) for [bold]{ticker_figi.ticker(order.figi)}[/] at {order.initial_security_price} x {order.lots_requested}")
				elif order.direction == OrderDirection.ORDER_DIRECTION_SELL:
					if not self.instrument_list_by_figi[order.figi].hi_order:
						if self.allow_selling:
							self.instrument_list_by_figi[order.figi].hi_order = order.order_id
							print(f"[green bold]FOUND[/] hi_order ([bold red]SELL[/]) for [bold]{ticker_figi.ticker(order.figi)}[/] at {order.initial_security_price} x {order.lots_requested}")
						else:
							await self.order_manager.cancel_order(order.order_id)
							print(f"[yellow bold]CANCELLED[/] [bold red]SELL[/] for [bold]{ticker_figi.ticker(order.figi)}[/] at {order.initial_security_price} x {order.lots_requested}. Selling not allowed")
					else:
						# more than one SELL order
						await self.order_manager.cancel_order(order.order_id)
						print(f"[yellow bold]CANCELLED[/] dup hi_order ([bold red]SELL[/]) for [bold]{ticker_figi.ticker(order.figi)}[/] at {order.initial_security_price} x {order.lots_requested}")
		print("\n____\n\n")


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
						print("[red] Unsupported kind of stream data")
						inspect(r)
						continue

					self.last_market_response = r

					# erase menu line
					_print("\r" + ("             " * 10), end="\r")

					await self.process_stream_response(r, figi)

					# print menu line
					print(
						f"[white on grey11][bold green]{ticker_figi.ticker(figi)}[/]: [bold]b[/] - BUY, [bold]s[/] - SELL; \t" +
						f"[bold]General[/]: [bold]l[/] - LIST ORDERS, [bold]+[/] - PAY IN, " +
						f"[bold]a[/] - ACCOUNT BALANCE, [bold]o[/] - OPERATIONS, [bold]p[/] - POSITIONS", end='\r'
					)

		# stream error shouldn't stop monitor
		# exceptions handled in parent class within while loop with retrying logic
		finally:
			self.last_market_response = None
			self.stream.stop()



async def test_order_monitor():
	load_dotenv()
	async with AsyncSandboxClient(os.environ["T_INVEST_TOKEN_SANDBOX"]) as client:
		account_manager = await AccountManagerSandbox(client).connect()
		order_manager = OrderManagerSandbox(client, account_manager)
		order_monitor = OrderMonitor(client, order_manager)
		order_monitor.add_instrument(
			InstrumentMonitor(
				client=client,
				figi=await etf_ticker_to_figi(client, "TMON@"),
				type="etf",
				candle_interval=CandleInterval.CANDLE_INTERVAL_5_MIN,
				period=6,
				lots=1,
				allow_selling=False
			)
		)
		order_monitor.add_instrument(
			InstrumentMonitor(
				client=client,
				figi=await etf_ticker_to_figi(client, "SAFE"),
				type="etf",
				candle_interval=CandleInterval.CANDLE_INTERVAL_5_MIN,
				period=6,
				lots=1,
				allow_selling=False
			)
		)
		order_monitor.add_instrument(
			InstrumentMonitor(
				client=client,
				figi=await share_ticker_to_figi(client, "MRKS"),
				type="share",
				candle_interval=CandleInterval.CANDLE_INTERVAL_5_MIN,
				period=6,
				lots=1
			)
		)
		# order_monitor.add_instrument(
		# 	InstrumentMonitor(
		# 		client=client,
		# 		figi=await share_ticker_to_figi(client, "SBER"),
		# 		type="share",
		# 		candle_interval=CandleInterval.CANDLE_INTERVAL_5_MIN,
		# 		period=6,
		# 		lots=4
		# 	)
		# )
		await order_monitor.init_instruments()
		order_monitor.running_task = asyncio.create_task(order_monitor.monitor())
		order_monitor.input_task = asyncio.create_task(order_monitor.get_user_input())
		while True:
			await asyncio.sleep(1)


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
