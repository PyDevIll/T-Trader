# t_order_manager.py

import t_services
import asyncio

from datetime import datetime, timedelta
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
	OrderExecutionReportStatus
)
from t_tech.invest.exceptions import AioRequestError
import json
from rich import print, inspect
from dotenv import load_dotenv
import os
from functools import lru_cache, reduce
from t_services import (
	StreamMonitor,
	AccountManager,
	instruments_by_filter,
	etf_ticker_to_figi,
	share_ticker_to_figi
)
from t_services import ticker_figi_cache as ticker_figi
from uuid import uuid4 as uuid
import signal

def MA(period, candle_history):
	if len(candle_history) < period:
		return None
	else:
		value = reduce(lambda a, v: a + v, [quotation_to_decimal(c.close) for c in candle_history[-period:]]) / period
		return value


class InstrumentMonitor:
	def __init__(self, client, figi, type, candle_interval, period, lots=1):
		self.client = client
		self.figi = figi
		self.type = type
		self.candle_interval = candle_interval
		self.bid = None	#quotation
		self.ask = None #quotation
		self.limit_up = None
		self.limit_down = None
		self.period = period	# period = candle_count
		self.candle_history = []
		self.min_price_increment = None
		self.ma = 0
		self.deviation_percent = 0.1
		self.lots = lots
		self.hi_order = None
		self.lo_order = None
		self.is_trading = True


	async def update_candles(self):
		if not self.is_trading:
			print(f"Trying to update not trading instrument ({self.figi}).")
			return False

		candles = []
		from_ = now() - candle_interval_to_timedelta(self.candle_interval) * self.period
		candles = (await self.client.market_data.get_candles(
			instrument_id=self.figi,
			from_=from_,
			to=now(),
			interval=self.candle_interval,
			candle_source_type=CandleSource.CANDLE_SOURCE_INCLUDE_WEEKEND
		)).candles
		# if no enough candles increase from_ until len(candle) = period 
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
		self.bid = orderbook.bids[0].price
		self.ask = orderbook.asks[0].price
		self.limit_up = orderbook.limit_up
		self.limit_down = orderbook.limit_down
		print(f"{ticker_figi.ticker(self.figi)} bid/ask = {quotation_to_decimal(self.bid)} / {quotation_to_decimal(self.ask)}")
		print(f"Limit up/down = {quotation_to_decimal(self.limit_up)} / {quotation_to_decimal(self.limit_down)}")


class OrderManager:
	def __init__(self, client, account_id):
		self.account_id = account_id
		self.client = client

	async def _post_order(self, figi, price, order_type, order_direction, lots=1):
		order_id = str(uuid())
		print(f"Posting order (id = {order_id}; {args})")
		try:
			order_result = self.client.post_sandbox_order(
				figi=figi,
				quantity=lots,
				price=price,
				direction=order_direction,
				account_id=self.account_id,
				order_type=order_type,
				order_id=order_id,
				timeInForce=TimeInForceType.TIME_IN_FORCE_FILL_AND_KILL,
				confirmMarginTrade=True
			)
		except Exception as e:
			print(f"Cannot post order: {e}")
		if order_result.execution_report_status != OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_REJECTED:
			print("Success!")
			return order_result.order_id
		print("Order rejected!")
		return False	# order rejected


	async def change_order(self, order_id):
		...
		return order_id


	async def check_order(self, order_id):
		...


	async def list_orders(self):
		order_list_response = await client.sandbox.get_sandbox_orders(account_id=self.account_id)
		for order in order_list_response.orders:
			print(f"{ticker_figi.ticker(order.figi)}: {order.order_type.name} ,{order.direction.name} at {money_to_decimal(order.initial_order_price)} x {order.lots_requested}")
			print(f"\t\t Execution status: {order.execution_report_status.name}: {money_to_decimal(order.executed_order_price)} x {order.lots_executed}")
			print(f"\t\t Commission: {money_to_decimal(order.executed_commission)} + {money_to_decimal(order.service_commission)}")
			print("___\n")
		...

	async def buy(self, instrument_monitor):
		figi = instrument_monitor.figi
		price = instrument_monitor.bid
		lots = instrument_monitor.lots
		print(f"BUY {ticker_figi.ticker(figi)} for {Decimal(price)}")
		order_id = await self._post_order(figi, price, OrderType.ORDER_TYPE_MARKET, OrderDirection.ORDER_DIRECTION_BUY, lots)
		if order_id:
			return order_id


	async def buylimit(self, instrument_monitor, price):
		figi = instrument_monitor.figi
		lots = instrument_monitor.lots
		print(f"BUY STOP {ticker_figi.ticker(figi)} at {Decimal(price)}")
		order_id = await self._post_order(figi, price, OrderType.ORDER_TYPE_LIMIT, OrderDirection.ORDER_DIRECTION_BUY, lots)
		if order_id:
			return order_id


	async def sell(self, instrument_monitor):
		figi = instrument_monitor.figi
		price = instrument_monitor.ask 	# sell price /= buy price. should I get them for orderbook's bid/ask to be sure?
		lots = instrument_monitor.lots
		print(f"SELL {ticker_figi.ticker(figi)} for {Decimal(price)}")
		order_id = await self._post_order(figi, price, OrderType.ORDER_TYPE_MARKET, OrderDirection.ORDER_DIRECTION_SELL, lots)
		if order_id:
			return order_id


	async def selllimit(self, instrument_monitor, price):
		figi = instrument_monitor.figi
		lots = instrument_monitor.lots
		print(f"SELL STOP {ticker_figi.ticker(figi)} at {Decimal(price)}")
		order_id = await self._post_order(figi, price, OrderType.ORDER_TYPE_LIMIT, OrderDirection.ORDER_DIRECTION_SELL, lots)
		if order_id:
			return order_id


class OrderMonitor(StreamMonitor):
	def __init__(self, client, order_manager):
		super().__init__(client)
		self.instrument_list_by_figi = {}	# {figi: InstrumentMonitor}
		self.running_task = None
		self.order_manager = order_manager
		...


	def add_instrument(self, instrument_monitor):
		self.instrument_list_by_figi[instrument_monitor.figi] = instrument_monitor


	async def check_trading_statuses(self):
		wanted_status = [SecurityTradingStatus.SECURITY_TRADING_STATUS_NORMAL_TRADING, SecurityTradingStatus.SECURITY_TRADING_STATUS_DEALER_NORMAL_TRADING]
		figis = [figi for figi, i in self.instrument_list_by_figi.items()]
		statuses = await self.client.market_data.get_trading_statuses(instrument_ids=figis)
		for status in statuses.trading_statuses:
			self.instrument_list_by_figi[status.figi].is_trading = status.trading_status in wanted_status
			print(f"Trading status of {ticker_figi.ticker(status.figi)} is {status.trading_status.name}. Tradable = {self.instrument_list_by_figi[status.figi].is_trading}")
		...


	async def move_orders(self):
		for figi, i in self.instrument_list_by_figi.items():
			...

	async def request_action(self, market_response):
		def signal_timeout(num, frame):
			raise TimeoutError("Time is up")

		try:
			figi = (market_response.orderbook or market_response.last_price or market_response.candle).figi
		except:
			print("Market response has no figi defined")
			inspect(market_response)
			input()

		user_input = None
		signal.signal(signal.SIGALRM, signal_timeout)
		signal.alarm(1)
		print("Choose an operation")
		try:
			user_input = input(f"Operation on {ticker_figi.ticker(figi)} (b - BUY, s - SELL):")
			signal.alarm(0)
		except TimeoutError as e:
			print(e)

		if user_input:
			print(f"You've entered '{user_input}'")
			if user_input == 'b':
				print(f'BUYING {ticker_figi.ticker(figi)}')
				await self.order_manager.buy(self.instrument_list_by_figi[figi])
			if user_input == 's':
				print(f'SELLING {ticker_figi.ticker(figi)}')
				await self.order_manager.sell(self.instrument_list_by_figi[figi])
			if user_input == 'l':
				print('LISTING ORDERS:')
				await self.order_manager.list_orders()
			print("___\n")
		else:
			print("No operation selected")



	async def process_stream_response(self, market_response):
		await self.request_action(market_response)
		...


	async def init_instruments(self):
		await self.check_trading_statuses()
		for figi, i in self.instrument_list_by_figi.items():
			await i.update_candles()
			if i.type == "etf":
				broker_instrument = (await self.client.instruments.etf_by(id=figi, id_type=InstrumentIdType.INSTRUMENT_ID_TYPE_FIGI)).instrument
			elif i.type == "share":
				broker_instrument = (await self.client.instruments.share_by(id=figi, id_type=InstrumentIdType.INSTRUMENT_ID_TYPE_FIGI)).instrument

			i.min_price_increment = broker_instrument.min_price_increment
			print(f"{ticker_figi.ticker(figi)} min price increment = {quotation_to_decimal(i.min_price_increment)}")

		print("\n____\n\n")


	async def _monitor(self):
		self.stream = self.client.create_market_data_stream()
		trade_instruments = [TradeInstrument(instrument_id=figi) for figi in self.instrument_list_by_figi.keys()]
		candle_instruments = [CandleInstrument(instrument_id=figi, interval=i.candle_interval) for figi, i in self.instrument_list_by_figi.items()]
		orderbook_instruments = [OrderBookInstrument(instrument_id=figi, depth=1) for figi in self.instrument_list_by_figi.keys()]	# for real bid/ask prices
		# self.stream.trades.subscribe(trade_instruments)
		self.stream.order_book.subscribe(orderbook_instruments)
		self.stream.last_price.subscribe(trade_instruments)
		self.stream.candles.waiting_close(enabled=True).subscribe(candle_instruments)	# cannot specify candle_source_type=CandleSource.CANDLE_SOURCE_INCLUDE_WEEKEND
		try:
			# first response returns value "SubscribeTradesResponse(..)"
			async for r in self.stream:
				if not self.stop:
					# if r.trade:
					# 	print(f"{ticker_figi.ticker(r.trade.figi)}:      trade = {quotation_to_decimal(r.trade.price)} x {r.trade.quantity} ({r.trade.direction.name})")
					if r.orderbook:
						self.instrument_list_by_figi[r.orderbook.figi].update_bid_ask(r.orderbook)
						await self.process_stream_response(r)
					elif r.last_price:
						print(f"{ticker_figi.ticker(r.last_price.figi)}: last price = {quotation_to_decimal(r.last_price.price)}")
						# await self.process_stream_response()
					elif r.candle:
						print(f"{ticker_figi.ticker(r.candle.figi)} has finished candle ({r.candle.interval.name})")
						# just append?   won't work for weekends?
						await self.instrument_list_by_figi[r.candle.figi].update_candles()
		# except Exception as e:
		# 	print(f"Stream interrupted due to error: {e}")
		finally:
			self.stream.stop()
			self.stop = True
			print("Stream stopped. OK!")


async def test_order_manager():
	load_dotenv()
	async with AsyncSandboxClient(os.environ["T_INVEST_TOKEN_SANDBOX"]) as client:
		account_manager = await AccountManager(client).connect()
		order_manager = OrderManager(client, account_manager.account)


	...


async def test_order_monitor():
	load_dotenv()
	async with AsyncSandboxClient(os.environ["T_INVEST_TOKEN_SANDBOX"]) as client:
		account_manager = await AccountManager(client).connect()
		order_manager = OrderManager(client, account_manager.account)
		order_monitor = OrderMonitor(client, order_manager)
		order_monitor.add_instrument(
			InstrumentMonitor(
				client=client,
				figi=await etf_ticker_to_figi(client, "TMON@"),
				type="etf",
				candle_interval=CandleInterval.CANDLE_INTERVAL_5_MIN,
				period=6
			)
		)
		order_monitor.add_instrument(
			InstrumentMonitor(
				client=client,
				figi=await etf_ticker_to_figi(client, "SAFE"),
				type="etf",
				candle_interval=CandleInterval.CANDLE_INTERVAL_5_MIN,
				period=6
			)
		)
		order_monitor.add_instrument(
			InstrumentMonitor(
				client=client,
				figi=await share_ticker_to_figi(client, "SBER"),
				type="share",
				candle_interval=CandleInterval.CANDLE_INTERVAL_5_MIN,
				period=6
			)
		)
		await order_monitor.init_instruments()
		order_monitor.running_task = asyncio.create_task(order_monitor.monitor())

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

"""
