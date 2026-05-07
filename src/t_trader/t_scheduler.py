# t_scheduler.py

from t_services import (
	StreamMonitor,
	AccountManagerSandbox,
	OrderManagerSandbox,
	etf_ticker_to_figi,
	share_ticker_to_figi,
)
from t_services import 	ticker_figi_cache as ticker_figi
from t_order_manager import (
	OrderManagerSandbox,
)

from t_tech.invest.sandbox.async_client import AsyncSandboxClient
from t_tech.invest,utils import now

from datetime import datetime, timedelta
import pandas as pd
from enum import Enum
import os

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
			"Tue": ["ABIO", "ABRD", "DATA", "DOMRF", "ELFV", "EUTR", "GEMC", "LENT", "LSRG", "MDMG", "MRKP". "MSRS", "MVID", "OZON", "OZPH", "PIKK"],
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

async def every5Minutes(do_sleep=True):
	global last_time

	while True:
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
		self.ticker_queue = []
		self.weekday = ""
		self.hour = ""

	async def timer(self):
		print("...")
		await every5Minutes()
		print(now().strftime("%H : %M"))
		...


	async def check_trading_status(self, ticker):
		...


	async def buy_scheduled(self):
		...


	async def sell_scheduled(self):
		...


	async def buy(self, ticker):
		...



async def scheduled_trading():
	load_dotenv()
	async with AsyncSandboxClient(os.environ["T_INVEST_TOKEN_SANDBOX"]) as client:
		account_manager = AccountManagerSandbox(client).connect("schedule")
		order_manager = OrderManagerSandbox(client, account_manager)
		scheduler = Scheduler(schedule_tab_shares, order_manager)
		scheduler.timer_task = asyncio.create_task(scheduler.timer())


if __name__ == "__main__":
	ticker_figi.init()
	try:
		# asyncio.run(test_order_manager())
		asyncio.run(scheduled_trading())
	except KeyboardInterrupt:
		print("KeyboardInterrupt handled")
