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
	Quotation,
	OrderBookInstrument
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
from rich import print, inspect
from dotenv import load_dotenv
import os
import random
import uuid


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


class ticker_figi_cache():
	_ticker_to_figi = {}
	_figi_to_ticker = {}

	@classmethod
	def init(cls):
		try:
			with open('ticker_figi_cache.txt', 'r') as f:
				cache = json.load(f)
		except:
			return
		else:
			cls._ticker_to_figi = cache["ticker_to_figi"]
			cls._figi_to_ticker = cache["figi_to_ticker"]

	@classmethod
	def save(cls):
		# recording duplicated data. list of single ticker:figi pairs is enough
		with open('ticker_figi_cache.txt', 'w') as f:
			json.dump({
				'ticker_to_figi': cls._ticker_to_figi,
				'figi_to_ticker': cls._figi_to_ticker
			}, f)

	@classmethod
	def figi(cls, ticker):
		return cls._ticker_to_figi[ticker] if ticker in cls._ticker_to_figi else None

	@classmethod
	def ticker(cls, figi):
		return cls._figi_to_ticker[figi] if figi in cls._figi_to_ticker else None

	@classmethod
	def update(cls, ticker, figi):
		cls._ticker_to_figi[ticker] = figi
		cls._figi_to_ticker[figi] = ticker
		cls.save()


async def etf_ticker_to_figi(client, ticker):
	if figi:=ticker_figi_cache.figi(ticker):
		return figi

	for etf in (await client.instruments.etfs()).instruments:
		if etf.ticker == ticker:
			print(f"For {ticker} figi = {etf.figi}")
			ticker_figi_cache.update(ticker, etf.figi)
			return etf.figi


async def share_ticker_to_figi(client, ticker):
	if figi:=ticker_figi_cache.figi(ticker):
		return figi

	for share in (await client.instruments.shares()).instruments:
		if share.ticker == ticker:
			print(f"For {ticker} figi = {share.figi}")
			ticker_figi_cache.update(ticker, share.figi)
			return share.figi



class AccountManagerSandbox():
	def __init__(self, client):
		self.client = client
		self.account = None


	async def open_account(self, name=""):
		await self.client.sandbox.open_sandbox_account(name=name)


	async def get_account(self, name=""):
		accounts = (await self.client.sandbox.get_sandbox_accounts()).accounts
		if not accounts:
			return None

		if not name:
			return accounts[0]
		else:
			for acc in accounts:
				if acc.name == name:
					return acc
			else:
				print(f"Account {name} is not found")
				return None


	async def connect(self):
		default_account = await self.get_account("default")
		if not default_account:
			await self.open_account("default")
			default_account = await self.get_account("default")

		self.account = default_account
		return self


	async def get_balance(self):
		balance_response = await self.client.sandbox.get_sandbox_withdraw_limits(account_id=self.account.id)
		balance = balance_response.money[0]
		return money_to_decimal(balance)


	async def pay_in(self, amount_decimal):
		money_amount = decimal_to_money(amount_decimal, "rub")
		return await self.client.sandbox.sandbox_pay_in(account_id=self.account.id, amount=money_amount)


	async def account_operations(self, from_=None, to=None):
		operations = (await self.client.sandbox.get_sandbox_operations(account_id=self.account.id, from_=from_, to=to)).operations
		for op in operations:
			print(f"{op.operation_type.name};  {op.type}; {money_to_decimal(op.payment):.2f}; {money_to_decimal(op.price):.2f}; {op.date.isoformat()}")
			for tr in op.trades:
				print(f"	[{tr.quantity}; {money_to_decimal(tr.price):.2f}; {tr.date_time.isoformat()}]")
			print("___\n\n")
		return operations


	async def get_positions(self):
		positions = (await self.client.sandbox.get_sandbox_portfolio(account_id=self.account.id)).positions
		for p in positions:
			profit = money_to_decimal(p.current_price) - money_to_decimal(p.average_position_price_fifo)
			green_color = profit >= Decimal(0)
			print(
				f"[bold green]{p.ticker}[/] x {quotation_to_decimal(p.quantity):.2f} = {money_to_decimal(p.average_position_price_fifo):.2f} (" +
				("[green]" if green_color else "[red]") + f"{profit:+.2f}[/] )"
			)
		return positions



class OrderManagerSandbox:
	def __init__(self, client, account_manager):
		self.account_manager = account_manager
		self.account_id = account_manager.account.id
		self.client = client

	async def _post_order(self, figi, price, order_type, order_direction, lots=1):
		order_id = str(uuid.uuid4())
		print(f"Posting order {self.account_id} ({locals()})")
		try:
			post_order_response = await self.client.sandbox.post_sandbox_order(
				figi=figi,
				quantity=lots,
				price=price,
				direction=order_direction,
				account_id=self.account_id,
				order_type=order_type,
				order_id=order_id,
				# time_in_force=TimeInForceType.TIME_IN_FORCE_FILL_AND_KILL
				# confirm_margin_trade=True
			)
		except AioRequestError as e:
			print(f"Cannot post order: {e.metadata.message}")
			return None

		inspect(post_order_response)
		if post_order_response.execution_report_status != OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_REJECTED:
			print(f"Success! ({post_order_response.execution_report_status.name})")
			return post_order_response.order_id
		print("Order rejected!")
		return None


	async def change_order(self, order_id, price_quotation, lots):
		new_order_id = str(uuid.uuid4())
		try:
			post_order_response = await self.client.sandbox.replace_sandbox_order(
				ReplaceOrderRequest(
					account_id=self.account_id,
					order_id_type=OrderIdType.ORDER_ID_TYPE_EXCHANGE,
					order_id=order_id,
					idempotency_key=new_order_id,
					quantity=lots,
					price=price_quotation
				)
			)
		except AioRequestError as e:
			print(f"Cannot post order: {e.metadata.message}")
			return None

		if post_order_response.execution_report_status != OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_REJECTED:
			print(f"Order changed! ({post_order_response.execution_report_status.name}) New price = {post_order_response.initial_security_price}")
			return post_order_response.order_id
		print("Order change rejected!")
		return None


	async def get_order(self, order_id):
		try:
			order = await self.client.sandbox.get_sandbox_order_state(
				account_id = self.account_id,
				order_id=order_id,
				order_id_type=OrderIdType.ORDER_ID_TYPE_EXCHANGE
			)
		except Exception as e:
			print(e)
			return None

		print(f"{ticker_figi_cache.ticker(order.figi)}: {order.order_type.name}, {order.direction.name} at {money_to_decimal(order.initial_security_price)} x {order.lots_requested}")
		print(f"\t\t Execution status: {order.execution_report_status.name}: {money_to_decimal(order.executed_order_price)} x {order.lots_executed}")
		print(f"\t\t Commission: {money_to_decimal(order.executed_commission)} + {money_to_decimal(order.service_commission)}")
		if order.stages:
			print(f"\t\t Execution stages:")
			for num, stage in enumerate(order.stages, start=1):
				print(f"\t\t\t {num}. {money_to_decimal(stage.price)} x {stage.quantity}")
		print("___\n")
		return order


	async def list_orders(self):
		order_list_response = await self.client.sandbox.get_sandbox_orders(account_id=self.account_id)
		for order in order_list_response.orders:
			print(f"{ticker_figi_cache.ticker(order.figi)}: {order.order_type.name}, {order.direction.name} at {money_to_decimal(order.initial_security_price)} x {order.lots_requested}")
			print(f"\t\t Execution status: {order.execution_report_status.name}: {money_to_decimal(order.executed_order_price)} x {order.lots_executed}")
			print(f"\t\t Commission: {money_to_decimal(order.executed_commission)} + {money_to_decimal(order.service_commission)}")
			print("___\n")
		return order_list_response.orders


	async def cancel_order(self, order_id):
		cancel_response = await self.client.sandbox.cancel_sandbox_order(
			account_id=self.account_id,
			order_id=order_id,
			order_id_type=OrderIdType.ORDER_ID_TYPE_EXCHANGE
		)
		inspect(cancel_response.response_metadata)



class StreamMonitor():
	def __init__(self, client):
		self.client = client
		self.stream = None
		self.running_task = None
		self.input_task = None
		self.stop = False
		self.show = True

	async def monitor(self):
		retry_count = 0
		max_retries = 10
		base_delay = 1
		max_delay = 60
		while not self.stop:
			if retry_count > 0:
				print(f"Reconnecting try: {retry_count}")
			try:
				await self._monitor()
			except AioRequestError as e:
				print(f"Stream interrupted: {e}")
				delay = min(base_delay * (2 ** retry_count), max_delay)
				delay += random.uniform(-delay*0.1, delay*0.1)
				print(f"Waiting before retry: {delay:.2f} sec.")
				await asyncio.sleep(delay)
				retry_count += 1
			except asyncio.exceptions.CancelledError:
				self.stop = True
			else:
				retry_count = 0
		print("Stream ended by setting stop flag")
		self.input_task.cancel()
		self.running_task.cancel()

	async def _monitor(self):
		...		# abstract



class OrderbookMonitor(StreamMonitor):
	def __init__(self, client):
		super().__init__(client)
		self.instruments = []			# {figi: OrderBookInstrument}
		self.tracking_values_by_figi = {}		# {figi: {key: value, ...}}


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

		print(f"max_limit: {self.tracking_values_by_figi[figi]["max_limit"]}; min_limit: {self.tracking_values_by_figi[figi]["min_limit"]}")


	def set_instruments(self, figi_list):
		if self.stream:
			self.stream.stop()
			self.stream = None
			print("Stream stopped. OK!")
			if not self.running_task.done():
				self.running_task.cancel()

		self.instruments = [
			OrderBookInstrument(
				instrument_id=figi,
				depth=10
			) for figi in figi_list
		]


	async def _monitor(self):
		self.stream = self.client.create_market_data_stream()
		self.stream.order_book.subscribe(self.instruments)
		try:
			async for response in self.stream:
				if not self.stop:
					print(response.orderbook)
					if response.orderbook:
						self._track_values(response.orderbook)
		finally:
			self.stream.stop()
			print("Stream stopped. OK!")


async def orderbook_streaming():
	load_dotenv()
	async with AsyncSandboxClient(os.environ["T_INVEST_TOKEN_SANDBOX"]) as client:
		figi = await etf_ticker_to_figi(client, "TMON@")
		orderbook_monitor = OrderbookMonitor(client)
		orderbook_monitor.init_tracking_values(figi)
		orderbook_monitor.set_instruments([figi])
		orderbook_monitor.running_task = asyncio.create_task(orderbook_monitor.monitor())
		while True:
			await asyncio.sleep(1)



if __name__ == "__main__":
	try:
		asyncio.run(orderbook_streaming())
	except KeyboardInterrupt:
		print("KeyboardInterrupt handled")

"""
Don't know how to use orderbook data for trading.
Maybe T-Invest plaform didn't implemented trading directly from orderbook yet
"""
