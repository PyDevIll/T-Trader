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
	TInvest_async_client
)
from rich import print, inspect

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


# @pytest_asyncio.fixture
# async def account(client):
# 	return await get_account(client)

# _________________________________________________

# @pytest.mark.asyncio
# async def test_account_pay_in(client, account):
# 	assert account
# 	prev_balance = await get_balance(client, account.id)
# 	new_balance = (await pay_in(client, account.id, "100.01")).balance
# 	new_balance = money_to_decimal(new_balance)
# 	print(f"prev_balance: {prev_balance}; new balance: {new_balance}")
# 	assert (new_balance - prev_balance) == Decimal("100.01")


# @pytest.mark.asyncio
# async def test_account_operations(client, account):
# 	assert account

# 	operations = (await client.sandbox.get_sandbox_operations(account_id=account.id)).operations
# 	assert operations
# 	inspect(operations[0])
# 	for op in operations:
# 		print(f"{op.operation_type.name};  {op.type}; {money_to_decimal(op.payment)}; {money_to_decimal(op.price)}; {op.date.isoformat()}")
# 		for tr in op.trades:
# 			print(f"	[{tr.quantity}; {money_to_decimal(tr.price)}; {tr.date_time.isoformat()}]")
# 		print("___\n\n")


def test_get_instruments():
	import os
	with SandboxClient(os.environ["T_INVEST_TOKEN_SANDBOX"]) as client:
		etf_list = client.instruments.etfs(instrument_status=InstrumentStatus(2)).instruments
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
async def test_TInvest_client(client):
	t_client = await TInvest_async_client(client).connect()

	prev_balance = await t_client.get_balance()
	await t_client.pay_in(Decimal("50.05"))
	new_balance = await t_client.get_balance()
	assert new_balance - prev_balance == Decimal("50.05")

	operations = await t_client.account_operations()
	assert operations

