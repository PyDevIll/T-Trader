# test_t_services.py
import pytest
import pytest_asyncio
from decimal import Decimal

from t_tech.invest.async_services import AsyncServices
from t_tech.invest.utils import (
	now,
	candle_interval_to_timedelta,
	quotation_to_decimal,
	decimal_to_money,
	money_to_decimal
)
from t_tech.invest import (
	CandleInterval, 
	InstrumentIdType,
	Quotation,
)
from t_tech.invest.sandbox.async_client import AsyncSandboxClient
from t_tech.invest.sandbox.client import SandboxClient
from t_tech.invest.schemas import (
	CandleSource,
	InstrumentStatus,
	OperationType,
)
import json

from src.t_trader.t_services import (
	instruments_by_filter,
	AccountManager,
	OrderbookMonitor
)
from rich import print, inspect
from datetime import datetime, timedelta

# _________________________________________________

@pytest.fixture(scope="session", autouse=True)
def load_dotenv():
	from dotenv import load_dotenv
	load_dotenv()


@pytest_asyncio.fixture
async def client():
	import os
	async with AsyncSandboxClient(os.environ["T_INVEST_TOKEN_SANDBOX"]) as client:
		yield client


# _________________________________________________


def test_get_instruments():
	import os
	with SandboxClient(os.environ["T_INVEST_TOKEN_SANDBOX"]) as client:
		etf_list = client.instruments.etfs(instrument_status=InstrumentStatus.INSTRUMENT_STATUS_BASE).instruments
		etf_by_ticker = instruments_by_filter(etf_list, {
			"pos": {
				"currency": "rub",
				"country_of_risk": "RU",
				"liquidity_flag": True,
				# "fixed_commission": Quotation(units=0, nano=0),
				"api_trade_available_flag": True,
			},
			"neg": {
				"exchange": "unknown"
			}
		})
	for k in etf_by_ticker.keys():
		print(k, etf_by_ticker[k]["fixed_commission"])

	assert {"TGLD@", "SAFE", "TMON@", "TPAY"}.issubset(set(etf_by_ticker.keys()))


@pytest.mark.asyncio
async def test_account_manager(client):
	account_manager = await AccountManager(client).connect()
	assert account_manager
	inspect(account_manager.account)

	fulfil_amount = Decimal("50.05")
	prev_balance = await account_manager.get_balance()
	await account_manager.pay_in(fulfil_amount)
	new_balance = await account_manager.get_balance()
	assert new_balance - prev_balance == fulfil_amount

	operations = await account_manager.account_operations(from_=now()-timedelta(days=1))
	assert operations
	inspect(operations[0])

