# t_scheduler.py

from t_services import (
	AccountManagerSandbox,
	OrderManagerSandbox,
	etf_ticker_to_figi,
	share_ticker_to_figi,
)
from t_services import ticker_figi_cache as ticker_figi

from t_tech.invest.sandbox.async_client import AsyncSandboxClient
from t_tech.invest.utils import now
from t_tech.invest.exceptions import AioRequestError
from t_tech.invest.schemas import (
	OrderType,
	OrderDirection,
	OrderExecutionReportStatus
)

from datetime import datetime, timedelta
from enum import Enum
import asyncio
import os
import json
_print = print
from rich import inspect, print
from rich.prompt import Prompt
from dotenv import load_dotenv
from decimal import Decimal

# datetime.weekday() = 0..6
weekday_str = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]

# check tickers for trading availablility
schedule_tab_shares = {
	"4":
		{
			"Mon": ["ABRD", "FIXR", "MBNK", "MRKP", "MRKU", "MSRS", "MSTT", "OZPH"],
			"Tue": ["ABIO", "ABRD", "DATA", "DOMRF", "ELFV", "EUTR", "GEMC", "LENT", "LSRG", "MDMG", "MRKP", "MSRS", "MVID", "OZON", "OZPH", "PIKK"],
			"Wed": ["ABRD", "ASTR", "CBOM", "DATA", "DOMRF", "FIXR", "LENT", "MDMG", "MRKP", "MSRS", "OGKB", "OZON", "OZPH", "POSI"],
			"Thu": ["ABRD", "APTK", "ASTR", "BSPB", "CBOM", "DATA", "DOMRF", "GEMC", "GMKN", "LENT", "MDMG", "MRKU", "MSRS", "OGKB", "OZPH", "POSI"],
			"Fri": ["BANE", "DOMRF", "ELFV", "LENT", "MDMG", "MRKP", "MRKU", "MSNG", "MSRS", "OZON", "OZPH", "POSI"],
			"Sat": ["BANE"],
			"Sun": []
		},
	"5":  { "Mon": [],						"Tue": ["ABRD"],		"Wed": [],				"Thu": [],				"Fri": [],				"Sat": [],				"Sun": []},
	"6":  { "Mon": [],						"Tue": ["ABRD"],		"Wed": [],				"Thu": [],				"Fri": [],				"Sat": [],				"Sun": []},
	"7":
	{
		"Mon": ["ABIO", "APTK", "MSTT", "NKHP", "NMTP"],
		"Tue": [],
		"Wed": ["ABRD", "BLNG", "MSRS", "OZON"],
		"Thu": [],
		"Fri": ["OZPH"],
		"Sat": [],
		"Sun": []
	},
	"8":  { "Mon": ["LIFE"],				"Tue": [],				"Wed": [],				"Thu": [],				"Fri": [],				"Sat": [],				"Sun": []},
	"9":  { "Mon": [],						"Tue": [],				"Wed": [],				"Thu": ["LENT"],		"Fri": [],				"Sat": [],				"Sun": []},
	"15": { "Mon": ["AKRN"],				"Tue": ["AKRN", "EUTR"],"Wed": [],				"Thu": [],				"Fri": [],				"Sat": [],				"Sun": []},
	"16":
	{
		"Mon": ["DOMRF","LIFE", "MRKU", "MRKZ"],
		"Tue": ["LIFE", "MRKZ"],
		"Wed": ["BLNG", "LIFE"],
		"Thu": ["LIFE", "MRKU", "MRKZ"],
		"Fri": ["LIFE"],
		"Sat": ["DOMRF"],
		"Sun": ["MRKZ", "NKHP"]
	},
	"17": { "Mon": [],						"Tue": ["AKRN"],		"Wed": [],				"Thu": [],				"Fri": [],				"Sat": [],				"Sun": []},
	"19": { "Mon": [],						"Tue": [],				"Wed": [],				"Thu": ["MRKZ"],		"Fri": [],				"Sat": [],				"Sun": []},
	"21":
	{
		"Mon": ["MSNG", "NKHP", "NMTP"],
		"Tue": ["BLNG", "NMTP"],
		"Wed": ["BLNG", "NKHP", "NMTP"],
		"Thu": ["NKHP"],
		"Fri": ["HYDR", "MTLR", "NKHP"],
		"Sat": ["MVID", "NKHP", "OGKB"],
		"Sun": []
	},
	"22": { "Mon": ["MVID"],				"Tue": [],				"Wed": ["MSRS"],		"Thu": [],				"Fri": ["CBOM"],		"Sat": ["MSNG"],		"Sun": []},
}
schedule_tab_etfs = {
	"4":  { "Mon": [],				"Tue": [],				"Wed": [],				"Thu": [],				"Fri": [],				"Sat": [],				"Sun": []},
	"5":  { "Mon": [],				"Tue": [],				"Wed": [],				"Thu": [],				"Fri": [],				"Sat": [],				"Sun": []},
	"6":  { "Mon": [],				"Tue": [],				"Wed": [],				"Thu": [],				"Fri": [],				"Sat": [],				"Sun": []},
	"7":  { "Mon": [],				"Tue": [],				"Wed": [],				"Thu": [],				"Fri": [],				"Sat": [],				"Sun": []},
	"8":  { "Mon": [],				"Tue": [],				"Wed": [],				"Thu": [],				"Fri": [],				"Sat": [],				"Sun": []},
	"9":  { "Mon": [],				"Tue": [],				"Wed": [],				"Thu": [],				"Fri": [],				"Sat": [],				"Sun": []},
	"15": { "Mon": [],				"Tue": [],				"Wed": [],				"Thu": [],				"Fri": [],				"Sat": [],				"Sun": []},
	"16": { "Mon": [],				"Tue": [],				"Wed": [],				"Thu": [],				"Fri": [],				"Sat": [],				"Sun": []},
	"17": { "Mon": [],				"Tue": [],				"Wed": [],				"Thu": [],				"Fri": [],				"Sat": [],				"Sun": []},
	"21": { "Mon": [],				"Tue": [],				"Wed": [],				"Thu": [],				"Fri": [],				"Sat": [],				"Sun": []},
	"22": { "Mon": [],				"Tue": [],				"Wed": [],				"Thu": [],				"Fri": [],				"Sat": [],				"Sun": []},
}

last_minute = now().minute

async def everyNMinutes(sleep_period=60, minutes=5):
	global last_minute

	while True:
		print("... ", end='', flush=True)
		await asyncio.sleep(sleep_period)

		if now().minute != last_minute:
			if (now().minute % minutes) == 0:
				last_minute = now().minute
				break
	return True


class Scheduler:
	def __init__(self, schedule_tab, order_manager):
		self.timer_task = None
		self.schedule_tab = schedule_tab
		self.orders_by_ticker = {}
		self.order_manager = order_manager
		self.ticker_list = []
		self.prev_ticker_list = None
		self.next_trade_time = now().replace(minute=0) + timedelta(hours=1)
		# at 15.55 UTC (22.55 NSK) no trading on the exchange
		if self.next_trade_time.hour == 16:
			self.next_trade_time += timedelta(minutes=5)


	async def timer(self):
		while True:
			await everyNMinutes()
		# try:
			await self.process_trades()
		# except Exception as e:
		# inspect(e)
		# self.timer_task.cancel()
		...


	async def process_trades(self):
		hour = str(self.next_trade_time.hour)
		weekday = weekday_str[self.next_trade_time.weekday()]
		print(f"Now: {now().strftime("%d, %H:%M")}. Next trade at: {self.next_trade_time.strftime("%d, %H:%M")} > {weekday}, {hour}")

		# 5 minutes before trade time
		if (now() < self.next_trade_time) and (self.next_trade_time - now() <= timedelta(minutes=5)):
			self.prev_ticker_list = self.ticker_list[:]
			if hour in self.schedule_tab:
				print(f"Some tickers are scheduled at {weekday}, {hour}")
				self.ticker_list = self.schedule_tab[hour][weekday]
				print(self.ticker_list)
				await self.check_trading_statuses()
			else:
				self.ticker_list = []
				print("No scheduled tickers")
			await self.sell_scheduled()

		# new hour started
		if (now() >= self.next_trade_time) and (now() - self.next_trade_time <= timedelta(minutes=5)):
			await self.buy_scheduled()
			self.next_trade_time = now().replace(minute=0) + timedelta(hours=1)
			# at 15.55 UTC (22.55 NSK) no trading on the exchange
			if self.next_trade_time.hour == 16:
				self.next_trade_time += timedelta(minutes=5)
			# at 3:55 UTC (10.55 NSK) is too early for trading
			if self.next_trade_time.hour == 4:
				self.next_trade_time += timedelta(minutes=5)
		...


	async def check_trading_statuses(self):
		figi_list = [await share_ticker_to_figi(self.order_manager.client, ticker) for ticker in self.ticker_list]
		tradable_figi_list = await self.order_manager.get_tradables_from(figi_list)
		self.ticker_list = [ticker_figi.ticker(figi) for figi in tradable_figi_list]
		print(f"Tradable scheduled tickers = {self.ticker_list}")


	async def buy_scheduled(self):
		print("Buying new scheduled shares...")
		if not self.ticker_list:
			print("No tickers scheduled")
			return

		for ticker in self.ticker_list:
			if ticker in self.prev_ticker_list:
				print(f"Keeping {ticker}. No BUY")
				continue
			print("Little delay between orders...")
			await asyncio.sleep(1)
			order_id = await self.buy(ticker)
			if order_id:
				self.orders_by_ticker[ticker] = order_id
				print("[green]Success!")
			else:
				print(f"[red]Couldn't BUY {ticker}.")
		print("___\n")
		...


	async def sell_scheduled(self):
		print("Closing earlier positions... ")
		if not self.orders_by_ticker:
			print("No opened positions!")
			return

		for ticker, order_id in self.orders_by_ticker.items():
			if ticker in self.ticker_list:
				print(f"Keeping {ticker}. No SELL")
				continue
			print("Little delay between orders...")
			await asyncio.sleep(1)
			order_id = await self.sell(ticker, order_id)
			if order_id:
				self.orders_by_ticker[ticker] = None	# to be deleted
				print("[green]Success!")
			else:
				print(f"[red]Couldn't SELL {ticker}.")

		# clean up nones from orders_by_ticker
		new_orders_by_ticker = {}
		for ticker, order_id in self.orders_by_ticker.items():
			if order_id:
				new_orders_by_ticker[ticker] = order_id

		self.orders_by_ticker = new_orders_by_ticker

		print("___\n")
		...


	async def buy(self, ticker):
		print(f"Buying {ticker}")
		figi = ticker_figi.figi(ticker)
		lots = 1
		try:
			bid, ask = await self.order_manager.get_bid_ask(figi)
		except:
			print("Failed to get BUY price")
		else:
			try:
				order_id = await self.order_manager.post_order(figi, bid, OrderType.ORDER_TYPE_MARKET, OrderDirection.ORDER_DIRECTION_BUY, lots)
				order = await self.order_manager.get_order(order_id)
				if order and (
					order.execution_report_status == OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_NEW or
					order.execution_report_status == OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_FILL
				):
					print(f"BUY order posted: {order.execution_report_status.name}")
					return order_id
			except Exception as e:
				print("Couldn't post BUY order")
				inspect(e)

		return None


	async def sell(self, ticker, order_id):
		# order_id is not needed?
		print(f"Selling {ticker}")
		figi = ticker_figi.figi(ticker)
		lots = 1
		try:
			bid, ask = await self.order_manager.get_bid_ask(figi)
		except:
			print("Failed to get SELL price")
		else:
			try:
				order_id = await self.order_manager.post_order(figi, ask, OrderType.ORDER_TYPE_MARKET, OrderDirection.ORDER_DIRECTION_SELL, lots)
				order = await self.order_manager.get_order(order_id)
				if order and (
					order.execution_report_status == OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_NEW or
					order.execution_report_status == OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_FILL
				):
					print(f"SELL order posted: {order.execution_report_status.name}")
					return order_id
			except Exception as e:
				print("Couldn't post SELL order")
				inspect(e)

		return None


async def scheduled_trading():
	load_dotenv()
	async with AsyncSandboxClient(os.environ["T_INVEST_TOKEN_SANDBOX"]) as client:
		account_manager = await AccountManagerSandbox(client).connect("schedule")
		order_manager = OrderManagerSandbox(client, account_manager)
		scheduler = Scheduler(schedule_tab_shares, order_manager)
		scheduler.timer_task = asyncio.create_task(scheduler.timer())
		while True:
			await asyncio.sleep(1)


_now = now
class now_mock():
	delta_time = (_now() - _now().replace(day=11, hour=3, minute=50))

	@classmethod
	def now(cls):
		return _now() - cls.delta_time

	@classmethod
	def set_time(cls, new_time):
		cls.delta_time = (_now() - new_time)
		return cls.now()


async def test_scheduler():
	load_dotenv()
	async with AsyncSandboxClient(os.environ["T_INVEST_TOKEN_SANDBOX"]) as client:
		account_manager = await AccountManagerSandbox(client).connect("schedule")
		order_manager = OrderManagerSandbox(client, account_manager)
		scheduler = Scheduler(schedule_tab_shares, order_manager)

		print(account_manager.account)
		print(await account_manager.get_balance_raw())
		balance = await account_manager.get_balance()
		print(f"Current balance = {balance}")
		if balance < 100000:
			await account_manager.pay_in(Decimal(100000))
			balance = await account_manager.get_balance()
			print(f"Paid in. Balance = {balance}")

		trade_times = []
		# 	now_mock.set_time(_now().replace(day=12, hour=3, minute=49)),
		# 	now_mock.set_time(_now().replace(day=12, hour=3, minute=50)),
		# 	now_mock.set_time(_now().replace(day=12, hour=3, minute=55)),
		# 	now_mock.set_time(_now().replace(day=12, hour=4, minute=0)),
		# 	now_mock.set_time(_now().replace(day=12, hour=4, minute=5)),

		# 	now_mock.set_time(_now().replace(day=12, hour=4, minute=49)),
		# 	now_mock.set_time(_now().replace(day=12, hour=4, minute=50)),
		# 	now_mock.set_time(_now().replace(day=12, hour=4, minute=55)),
		# 	now_mock.set_time(_now().replace(day=12, hour=5, minute=0)),
		# 	now_mock.set_time(_now().replace(day=12, hour=5, minute=5)),

		# 	now_mock.set_time(_now().replace(day=12, hour=5, minute=49)),
		# 	now_mock.set_time(_now().replace(day=12, hour=5, minute=50)),
		# 	now_mock.set_time(_now().replace(day=12, hour=5, minute=55)),
		# 	now_mock.set_time(_now().replace(day=12, hour=6, minute=0)),
		# 	now_mock.set_time(_now().replace(day=12, hour=6, minute=5)),

		# 	now_mock.set_time(_now().replace(day=12, hour=6, minute=49)),
		# 	now_mock.set_time(_now().replace(day=12, hour=6, minute=50)),
		# 	now_mock.set_time(_now().replace(day=12, hour=6, minute=55)),
		# 	now_mock.set_time(_now().replace(day=12, hour=7, minute=0)),
		# 	now_mock.set_time(_now().replace(day=12, hour=7, minute=5)),
		# ]


		trade_times_2 = [
			now_mock.set_time(_now().replace(day=11, hour=14, minute=55)),
			now_mock.set_time(_now().replace(day=11, hour=15, minute=0)),

			now_mock.set_time(_now().replace(day=11, hour=15, minute=49)),
			now_mock.set_time(_now().replace(day=11, hour=15, minute=50)),
			now_mock.set_time(_now().replace(day=11, hour=15, minute=55)),
			now_mock.set_time(_now().replace(day=11, hour=16, minute=0)),
			now_mock.set_time(_now().replace(day=11, hour=16, minute=5)),
			now_mock.set_time(_now().replace(day=11, hour=16, minute=10)),

			now_mock.set_time(_now().replace(day=11, hour=16, minute=49)),
			now_mock.set_time(_now().replace(day=11, hour=16, minute=50)),
			now_mock.set_time(_now().replace(day=11, hour=16, minute=55)),
			now_mock.set_time(_now().replace(day=11, hour=17, minute=0)),
			now_mock.set_time(_now().replace(day=11, hour=17, minute=5)),
			now_mock.set_time(_now().replace(day=11, hour=17, minute=10)),
		]

		if trade_times:
			scheduler.next_trade_time = trade_times[0].replace(minute=0) + timedelta(hours=1)
			for mock_time in trade_times:

				print(scheduler.ticker_list)
				figi_list = [await share_ticker_to_figi(client, ticker) for ticker in scheduler.ticker_list]
				print(figi_list)
				tradable_figi_list = await order_manager.get_tradables_from(figi_list)
				print(f"Tradable figis: {tradable_figi_list}")
				ticker_list = [ticker_figi.ticker(figi) for figi in tradable_figi_list]
				print(f"Tradable tickers: {ticker_list}")

				input("Moving to trades...")

				now_mock.set_time(mock_time)
				print(f"{now()}. Running trades...")
				await scheduler.process_trades()

				input("Moving to next time...")

		input("Morning test completed")
	# now = now_mock.now
	# asyncio.run(test_scheduler())

		scheduler.next_trade_time = trade_times_2[0].replace(minute=0) + timedelta(hours=1)
		for mock_time in trade_times_2:

			print(scheduler.ticker_list)
			figi_list = [await share_ticker_to_figi(client, ticker) for ticker in scheduler.ticker_list]
			print(figi_list)
			tradable_figi_list = await order_manager.get_tradables_from(figi_list)
			print(f"Tradable figis: {tradable_figi_list}")
			ticker_list = [ticker_figi.ticker(figi) for figi in tradable_figi_list]
			print(f"Tradable tickers: {ticker_list}")

			input("Moving to trades...")

			now_mock.set_time(mock_time)
			print(f"{now()}. Running trades...")
			await scheduler.process_trades()

			input("Moving to next time...")

		input("Test completed")


if __name__ == "__main__":
	ticker_figi.init()
	now = now_mock.now
	asyncio.run(test_scheduler())

# try:
	now = _now
	asyncio.run(scheduled_trading())
# except KeyboardInterrupt:
	print("KeyboardInterrupt handled")


"""
make posting multiple orders by gathering multiple async tasks with asyncio.gather(...)
and handle timeouts and limits inside every task as stream retrying with delay like in t_order_manager.py

"""