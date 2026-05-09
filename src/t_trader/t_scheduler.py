# t_scheduler.py

from t_services import (
	StreamMonitor,
	AccountManagerSandbox,
	OrderManagerSandbox,
	etf_ticker_to_figi,
	share_ticker_to_figi,
)
from t_services import 	ticker_figi_cache as ticker_figi

from t_tech.invest.sandbox.async_client import AsyncSandboxClient
from t_tech.invest.utils import now
from t_tech.invest.exceptions import AioRequestError

from datetime import datetime, timedelta
from enum import Enum
import asyncio
import os
import json
_print = print
from rich import inspect, print
from rich.prompt import Prompt
from dotenv import load_dotenv


# datetime.weekday() = 0..6
weekday_str = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]

class Weekday(Enum):
	Mon = 0
	Tue = 1
	Wed = 2
	Thu = 3
	Fri = 4
	Sat = 5
	Sun = 6

# fill-in figis
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

last_time = now()

async def every5Minutes():
	global last_time

	while True:
		print("... ", end='', flush=True)
		await asyncio.sleep(60)

		if (now().minute % 5) == 0:
			if now() > last_time:
				last_time = now()
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
		self.weekday = weekday_str[now().weekday()]
		self.last_hour = now().hour


	async def timer(self):
		# try:
		while True:
			await every5Minutes()

			# 5 minutes before hour end
			next_hour_in_5min = (now() + timedelta(minutes=5, seconds=59)).hour
			self.weekday = weekday_str[(now() + timedelta(minutes=5, seconds=59)).weekday()]
			print(f"{now().strftime("%H:%M")} > {self.weekday}, {now().hour}")

			if now().hour != next_hour_in_5min:
				self.prev_ticker_list = self.ticker_list[:]
				if str(next_hour_in_5min) in self.schedule_tab:
					self.ticker_list = self.schedule_tab[str(next_hour_in_5min)][self.weekday]
					await self.check_trading_statuses()
				else:
					self.ticker_list = []
					print("No scheduled tickers")
				await self.sell_scheduled()

			# new hour started
			if self.last_hour != now().hour:
				await self.buy_scheduled()
				self.last_hour = now().hour

		# except Exception as e:
			# inspect(e)
			# self.timer_task.cancel()
		...


	async def check_trading_statuses(self):
		tradable_ticker_list = await self.order_manager.get_tradables_from(self.ticker_list)
		self.ticker_list = tradable_ticker_list
		print(f"Tradable scheduled tickers = {tradable_ticker_list}")


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
			await asyncio.sleep(5)
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
			await asyncio.sleep(5)
			order_id = await self.sell(ticker, order_id)
			if order_id:
				self.orders_by_ticker[ticker] = order_id
				print("[green]Success!")
			else:
				print(f"[red]Couldn't SELL {ticker}.")
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
				order_id = await self.order_manager._post_order(figi, bid, OrderType.ORDER_TYPE_MARKET, OrderDirection.ORDER_DIRECTION_BUY, lots)
				order = await self.order_manager.get_order(order_id)
				if order and (
					order.execution_report_status == OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_NEW or
					order.execution_report_status == OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_FILL
				):
					print(f"BUY order posted: {order.execution_report_status.name}")
					return order_id
			except:
				print("Couldn't post BUY order")
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
				order_id = await self.order_manager._post_order(figi, ask, OrderType.ORDER_TYPE_MARKET, OrderDirection.ORDER_DIRECTION_SELL, lots)
				order = await self.order_manager.get_order(order_id)
				if order and (
					order.execution_report_status == OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_NEW or
					order.execution_report_status == OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_FILL
				):
					print(f"SELL order posted: {order.execution_report_status.name}")
					return order_id
			except:
				print("Couldn't post SELL order")
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



if __name__ == "__main__":
	ticker_figi.init()
	try:
		asyncio.run(scheduled_trading())
	except KeyboardInterrupt:
		print("KeyboardInterrupt handled")

