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
	CandleInterval, 
	InstrumentIdType,
	SecurityTradingStatus,
	Quotation,
)
from t_tech.invest.sandbox.async_client import AsyncSandboxClient
from t_tech.invest.schemas import (
	CandleSource,
	InstrumentStatus,
	OperationType,
	TradeInstrument
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
	share_ticker_to_figi,
	figi_to_ticker
)
from uuid import uuid4 as uuid


def MA(period, candle_history):
	if len(candle_history) < period:
		return None
	else:
		value = reduce(lambda a, v: a + v, [c.close for c in candle_history[-period:]]) / period
		return value


class InstrumentMonitor:
	def __init__(self, client, figi, candle_interval, period):
		self.client = client
		self.figi = figi
		self.candle_interval = candle_interval
		self.period = period	# period = candle_count
		self.candle_history = []
		self.ma = 0
		self.deviation_percent = 0.1
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
		print(f"{figi_to_ticker[self.figi]}: {quotation_to_decimal(self.candle_history[-1].close)}")
		# calculate MA
		...
		return True


class OrderManager:
	def __init__(self, client, account_id):
		self.account_id = account_id
		self.client = client

	async def post_order(self, figi, price, lots=1):
		order_id = str(uuid())
		print(order_id)
		...
		return order_id


	async def change_order(self, order_id):
		...
		return order_id

	async def check_order(self, order_id):
		...


class OrderMonitor(StreamMonitor):
	def __init__(self, client):
		super().__init__(client)
		self.instrument_list_by_figi = {}	# {figi: InstrumentMonitor}
		self.running_task = None
		...


	def add_instrument(self, instrument_monitor):
		self.instrument_list_by_figi[instrument_monitor.figi] = instrument_monitor


	async def check_trading_statuses(self):
		wanted_status = [SecurityTradingStatus.SECURITY_TRADING_STATUS_NORMAL_TRADING, SecurityTradingStatus.SECURITY_TRADING_STATUS_DEALER_NORMAL_TRADING]
		figis = [figi for figi, i in self.instrument_list_by_figi.items()]
		statuses = await self.client.market_data.get_trading_statuses(instrument_ids=figis)
		for status in statuses.trading_statuses:
			self.instrument_list_by_figi[status.figi].is_trading = status.trading_status in wanted_status
			print(f"Trading status of {figi_to_ticker[status.figi]} is {status.trading_status.name}. Tradable = {self.instrument_list_by_figi[status.figi].is_trading}")
		...


	async def move_orders(self):
		for figi, i in self.instrument_list_by_figi.items():
			...


	async def update_instruments(self):
		for figi, i in self.instrument_list_by_figi.items():
			await i.update_candles()
		print("\n____\n\n")


	async def _monitor(self):
		self.stream = self.client.create_market_data_stream()
		trade_instruments = [TradeInstrument(instrument_id=figi) for figi in self.instrument_list_by_figi.keys()]
		self.stream.trades.subscribe(trade_instruments)
		try:
			# first response returns value "SubscribeTradesResponse(..)"
			async for r in self.stream:
				if not self.stop:
					if r.trade:
						print(f"{figi_to_ticker[r.trade.figi]}: price = {quotation_to_decimal(r.trade.price)} x {r.trade.quantity} ({r.trade.direction.name})")
						await self.instrument_list_by_figi[r.trade.figi].update_candles()	# update in candles stream?
						print("\n____\n\n")
		finally:
			self.stream.stop()
			self.stop = True
			print("Stream stopped. OK!")


async def test_order_monitor():
	load_dotenv()
	async with AsyncSandboxClient(os.environ["T_INVEST_TOKEN_SANDBOX"]) as client:
		order_monitor = OrderMonitor(client)
		order_monitor.add_instrument(
			InstrumentMonitor(
				client=client,
				figi=await etf_ticker_to_figi(client, "TMON@"),
				candle_interval=CandleInterval.CANDLE_INTERVAL_5_MIN,
				period=5
			)
		)
		order_monitor.add_instrument(
			InstrumentMonitor(
				client=client,
				figi=await etf_ticker_to_figi(client, "SAFE"),
				candle_interval=CandleInterval.CANDLE_INTERVAL_5_MIN,
				period=5
			)
		)
		order_monitor.add_instrument(
			InstrumentMonitor(
				client=client,
				figi=await share_ticker_to_figi(client, "SBER"),
				candle_interval=CandleInterval.CANDLE_INTERVAL_5_MIN,
				period=5
			)
		)
		await order_monitor.check_trading_statuses()
		order_monitor.running_task = asyncio.create_task(order_monitor.monitor())
		while True:
			await asyncio.sleep(1)


if __name__ == "__main__":
	try:
		asyncio.run(test_order_monitor())
	except KeyboardInterrupt:
		print("KeyboardInterrupt handled")

"""
Currently moving stops are the only way to handle market spikes.
But there's no guarantee that such limit orders will fire.

28.04.2026
Candles are updated later, after of trade moment. How to use trade events in trading?

"""