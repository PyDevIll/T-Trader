# t-services.py
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
	Quotation
)
from t_tech.invest.sandbox.async_client import AsyncSandboxClient
from t_tech.invest.schemas import (
	CandleSource,
	InstrumentStatus,
	OperationType,
)
import json
from rich import print, inspect
from dotenv import load_dotenv
import os


def instruments_by_filter(instruments, filter_dict):
	instrument_by_ticker = {}
	for i in instruments:
		if (
			all(getattr(i, k) == v for k,v in filter_dict["pos"].items()) and
			not any(getattr(i, k) == v for k,v in filter_dict["neg"].items())
		):
			instrument_by_ticker[i.ticker] = {
				"name": i.name,
				"figi": i.figi,
				"country_of_risk": i.country_of_risk,
				"sector": i.sector,
				"liqudity_flag": i.liquidity_flag,
				"first_1min_candle_date": i.first_1min_candle_date.isoformat(),
				"first_1day_candle_date": i.first_1day_candle_date.isoformat(),
				"exchange": i.exchange,
				"currency": i.currency,
				"fixed_commission": getattr(i, "fixed_commission", None),
				"otc_flag": i.otc_flag,
				"buy_available_flag": i.buy_available_flag,
				"sell_available_flag": i.sell_available_flag,
				"min_price_increment": i.min_price_increment,
				"api_trade_available_flag": i.api_trade_available_flag,
				"for_qual_investor_flag": i.for_qual_investor_flag
			}
	return instrument_by_ticker


async def etf_ticker_to_figi(client, ticker):
	for etf in (await client.instruments.etfs()).instruments:
		if etf.ticker == ticker:
			return etf.figi


class TInvest_async_client():
	def __init__(self, client):
		self.client = client
		self.account = None


	async def get_account(self):
		accounts = await self.client.sandbox.get_sandbox_accounts()
		if not accounts.accounts:
			await client.sandbox.open_sandbox_account()
			accounts = await client.sandbox.get_sandbox_accounts()
		return accounts.accounts


	async def connect(self):
		self.account = (await self.get_account())[0]
		return self


	async def get_balance(self):
		balance = (await self.client.sandbox.get_sandbox_portfolio(account_id=self.account.id)).total_amount_portfolio 
		return money_to_decimal(balance)


	async def pay_in(self, amount_decimal):
		money_amount = decimal_to_money(amount_decimal, "rub")
		return await self.client.sandbox.sandbox_pay_in(account_id=self.account.id, amount=money_amount)


	async def account_operations(self):
		operations = (await self.client.sandbox.get_sandbox_operations(account_id=self.account.id)).operations
		inspect(operations[0])
		for op in operations:
			print(f"{op.operation_type.name};  {op.type}; {money_to_decimal(op.payment)}; {money_to_decimal(op.price)}; {op.date.isoformat()}")
			for tr in op.trades:
				print(f"	[{tr.quantity}; {money_to_decimal(tr.price)}; {tr.date_time.isoformat()}]")
			print("___\n\n")
		return operations


class Orderbook_monitor():
	def __init__(self, client):
		self.client = client
		self.instruments = []			# {figi: OrderBookInstrument}
		self.stream = None
		self.tracking_values_by_figi = {}		# {figi: {key: value, ...}}
		self.running_task = None


	def init_tracking_values(self, figi):
		self.tracking_values_by_figi[figi] = {
			"min_limit": decimal_to_quotation(Decimal(10000)),
			"max_limit": decimal_to_quotation(Decimal(0))
		}


	def _track_values(self, orderbook_data):
		figi = orderbook_data.figi
		if orderbook_data.limit_up > self.tracking_values_by_figi[figi]["max_limit"]:
			self.tracking_values_by_figi[figi]["max_limit"] = orderbook_data.limit_up
		if orderbook_data.limit_down < self.tracking_values_by_figi[figi]["min_limit"]:
			self.tracking_values_by_figi[figi]["min_limit"] = orderbook_data.limit_down

		print(f"max_limit: {self.tracking_values_by_figi[orderbook_data.figi]["max_limit"]}; min_limit: {self.tracking_values_by_figi[orderbook_data.figi]["min_limit"]}")


	def set_instruments(self, figi_list):
		if self.stream:
			self.stream.stop()
			self.stream = None
			print("Stream stopped. OK!")
			if not self.running_task.done():
				self.running_task.cancel()

		self.instruments = [
			OrderBookInstrument(
				figi=figi,
				depth=10
			) for figi in figi_list
		]


	async def monitor(self):
		self.stream = self.client.create_market_data_stream()
		self.stream.order_book.subscribe(self.instruments)
		try:
			async for response in self.stream:
				print(response.orderbook)
				if response.orderbook:
					self._track_values(response.orderbook)
		except asyncio.CancelledError:
			self.stream.stop()
			print("Stream stopped. OK!")


async def orderbook_streaming():
	load_dotenv()
	async with AsyncSandboxClient(os.environ["T_INVEST_TOKEN_SANDBOX"]) as client:
		figi = await etf_ticker_to_figi(client, "TMON@")
		orderbook_monitor = Orderbook_monitor(client)
		orderbook_monitor.init_tracking_values(figi)
		orderbook_monitor.set_instruments([figi])
		orderbook_monitor.running_task = asyncio.create_task(orderbook_monitor.monitor())
		while True:
			await asyncio.sleep(1)

if __name__ == "__main__":
	try:
		asyncio.run(orderbook_streaming())
	except KeyboardInterrupt:	# not triggered if last task is stopped
		print("KeyboardInterrupt handled")

