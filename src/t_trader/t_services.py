# t-services.py
import asyncio
from datetime import datetime, timedelta
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
)
from t_tech.invest.sandbox.async_client import AsyncSandboxClient
from t_tech.invest.schemas import CandleSource
import json
from rich import print, inspect


async def get_account(client):
	accounts = await client.sandbox.get_sandbox_accounts()
	if not accounts.accounts:
		await client.sandbox.open_sandbox_account()
		accounts = await client.sandbox.get_sandbox_accounts()
	return accounts.accounts[0]


async def get_balance(client, account_id):
	balance = (await client.sandbox.get_sandbox_portfolio(account_id=account_id)).total_amount_portfolio 
	return money_to_decimal(balance)


async def pay_in(client, account_id, amount_decimal: str):
	money_amount = decimal_to_money(Decimal(amount_decimal), "rub")
	return await client.sandbox.sandbox_pay_in(account_id=account_id, amount=money_amount)


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

def trading_result():
	...


# if __name__ == "__main__":
# 	asyncio.run(general_test())

