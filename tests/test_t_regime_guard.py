# test_t_regime_guard.py
import asyncio
import os
import sys
from decimal import Decimal

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src", "t_trader"))

from t_tech.invest import CandleInterval, Quotation
from t_tech.invest.schemas import OrderExecutionReportStatus
from t_tech.invest.utils import decimal_to_money

from t_order_manager import InstrumentMonitor
from t_regime import RegimeProfile


class StubOrderManager:
	def __init__(self):
		self.cancelled = []
		self.posted = []

	async def post_order(self, figi, price, order_type, direction, lots):
		oid = f"ord-{len(self.posted)}"
		self.posted.append((figi, direction.name))
		return oid

	async def get_order(self, order_id):
		order = type("FakeOrder", (), {
			"execution_report_status": OrderExecutionReportStatus.EXECUTION_REPORT_STATUS_NEW,
			"initial_security_price": decimal_to_money(Decimal("18.27"), "rub"),
		})()
		return order

	async def change_order(self, order_id, price, lots):
		return order_id

	async def cancel_order(self, order_id):
		self.cancelled.append(order_id)


def make_monitor(stub, trend_ok=True):
	mon = InstrumentMonitor(
		client=None, figi="FIGI", type="etf",
		candle_interval=CandleInterval.CANDLE_INTERVAL_5_MIN,
		period=6, lots=1, allow_selling=False, allow_buying=True,
		load_regime=True,
	)
	mon.order_manager = stub
	mon.min_price_increment = Quotation(units=0, nano=10000000)
	mon.ma = Decimal("18.30")
	mon.update_regime(RegimeProfile(
		ticker="T", slope_annual_pct=10.0, r2=0.99, ref_high=Decimal("18.31"),
		worst_dd=Decimal("-0.14"), median_recovery_bars=33, vol20_pct=0.09,
		atr14_daily=Decimal("0.03"), z_now=-0.6, trend_ok=trend_ok,
	))
	return mon


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
	async def _noop(*a, **k):
		return None

	monkeypatch.setattr(asyncio, "sleep", _noop)


@pytest.mark.asyncio
async def test_buy_not_placed_when_trend_bad():
	stub = StubOrderManager()
	mon = make_monitor(stub, trend_ok=False)
	await mon.move_orders()
	assert mon.lo_order is None
	assert stub.cancelled == []
	assert stub.posted == []


@pytest.mark.asyncio
async def test_buy_placed_when_trend_ok():
	stub = StubOrderManager()
	mon = make_monitor(stub, trend_ok=True)
	await mon.move_orders()
	assert mon.lo_order is not None
	assert stub.posted and stub.posted[0][1] == "ORDER_DIRECTION_BUY"


@pytest.mark.asyncio
async def test_buy_cancelled_when_trend_flips():
	stub = StubOrderManager()
	mon = make_monitor(stub, trend_ok=True)
	mon.lo_order = "existing-buy"
	await mon.move_orders()
	assert mon.lo_order == "existing-buy"
	mon.trend_ok = False
	await mon.move_orders()
	assert mon.lo_order is None
	assert "existing-buy" in stub.cancelled


@pytest.mark.asyncio
async def test_regime_break_cancels_buy():
	stub = StubOrderManager()
	mon = make_monitor(stub, trend_ok=True)
	mon.lo_order = "existing-buy"
	mon.bid = Quotation(units=15, nano=0)
	await mon.move_orders()
	assert mon.regime_break is True
	assert mon.lo_order is None
	assert "existing-buy" in stub.cancelled
