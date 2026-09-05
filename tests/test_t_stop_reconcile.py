# test_t_stop_reconcile.py
import asyncio
import os
import sys
from decimal import Decimal
from types import SimpleNamespace

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src", "t_trader"))

from t_tech.invest import Quotation
from t_tech.invest.schemas import (
	StopOrderDirection,
	StopOrderType,
)
from t_tech.invest.utils import (
	decimal_to_money,
	decimal_to_quotation,
)

from t_order_manager import OrderMonitor


def make_pos(figi, ticker, qty, lots, itype="share"):
	return SimpleNamespace(
		figi=figi,
		ticker=ticker,
		instrument_type=itype,
		quantity=decimal_to_quotation(Decimal(qty)),
		quantity_lots=decimal_to_quotation(Decimal(lots)),
	)


def make_inst(figi, ma, bid=None, ask=None):
	return SimpleNamespace(
		figi=figi,
		ma=Decimal(str(ma)) if ma is not None else None,
		bid=decimal_to_quotation(Decimal(str(bid))) if bid is not None else None,
		ask=decimal_to_quotation(Decimal(str(ask))) if ask is not None else None,
		candle_history=[],
		min_price_increment=decimal_to_quotation(Decimal("0.01")),
	)


def make_stop(sid, figi, direction, lots, price,
			  order_type=StopOrderType.STOP_ORDER_TYPE_STOP_LOSS):
	return SimpleNamespace(
		stop_order_id=sid,
		figi=figi,
		ticker=None,
		direction=direction,
		lots_requested=lots,
		order_type=order_type,
		stop_price=decimal_to_money(Decimal(str(price)), "rub"),
	)


class FakeAccountManager:
	def __init__(self, positions):
		self.positions = positions

	async def get_portfolio_positions(self):
		return list(self.positions)


class FakeOrderManager:
	def __init__(self, stops):
		self.account_manager = FakeAccountManager([])
		self.stops = list(stops)
		self.cancelled = []
		self.posted = []

	async def get_active_stop_orders(self):
		return list(self.stops)

	async def cancel_stop_order(self, stop_order_id):
		self.cancelled.append(stop_order_id)

	async def post_stop_order(self, figi, stop_price_decimal, direction, lots):
		self.posted.append((figi, stop_price_decimal, direction, lots))
		return f"stop-{len(self.posted)}"


def make_monitor(positions, stops, instruments):
	fake_om = FakeOrderManager(stops)
	fake_om.account_manager = FakeAccountManager(positions)
	monitor = OrderMonitor(client=None, order_manager=fake_om)
	for figi, inst in instruments.items():
		monitor.instrument_list_by_figi[figi] = inst
	return monitor, fake_om


LONG = StopOrderDirection.STOP_ORDER_DIRECTION_SELL
SHORT = StopOrderDirection.STOP_ORDER_DIRECTION_BUY


@pytest.mark.asyncio
async def test_long_position_gets_sell_stop_at_ma():
	# long 2 lots of FIGI, MA 18.00 well below market 18.40 -> SELL stop
	mon, fake = make_monitor(
		positions=[make_pos("F1", "TMON@", "2", "2")],
		stops=[],
		instruments={"F1": make_inst("F1", "18.00", bid="18.40", ask="18.41")},
	)
	await mon.reconcile_stops()
	assert len(fake.posted) == 1
	figi, price, direction, lots = fake.posted[0]
	assert figi == "F1"
	assert direction == LONG
	assert lots == 2
	assert price == Decimal("18.00")
	assert mon.stop_by_ticker["TMON@"] == Decimal("18.00")


@pytest.mark.asyncio
async def test_short_position_gets_buy_stop_at_ma():
	# short 3 lots, MA 190.00 above market 189.15 -> BUY stop
	mon, fake = make_monitor(
		positions=[make_pos("F2", "MTSS", "-30", "-3")],
		stops=[],
		instruments={"F2": make_inst("F2", "190.00", bid="189.15", ask="189.16")},
	)
	await mon.reconcile_stops()
	assert len(fake.posted) == 1
	figi, price, direction, lots = fake.posted[0]
	assert direction == SHORT
	assert lots == 3
	assert price == Decimal("190.00")


@pytest.mark.asyncio
async def test_matching_stop_is_kept():
	existing = make_stop("s1", "F1", LONG, 2, "18.00")
	mon, fake = make_monitor(
		positions=[make_pos("F1", "TMON@", "2", "2")],
		stops=[existing],
		instruments={"F1": make_inst("F1", "18.00", bid="18.40", ask="18.41")},
	)
	await mon.reconcile_stops()
	assert fake.posted == []
	assert fake.cancelled == []
	assert mon.stop_by_ticker["TMON@"] == Decimal("18.00")


@pytest.mark.asyncio
async def test_stop_moved_along_ma():
	# resting stop at 17.90, MA now 18.05 -> replace at 18.05
	existing = make_stop("s1", "F1", LONG, 2, "17.90")
	mon, fake = make_monitor(
		positions=[make_pos("F1", "TMON@", "2", "2")],
		stops=[existing],
		instruments={"F1": make_inst("F1", "18.05", bid="18.40", ask="18.41")},
	)
	await mon.reconcile_stops()
	assert "s1" in fake.cancelled
	assert len(fake.posted) == 1
	assert fake.posted[0][1] == Decimal("18.05")


@pytest.mark.asyncio
async def test_wrong_direction_stop_replaced():
	# long position but resting BUY stop -> replace with SELL
	existing = make_stop("s1", "F1", SHORT, 2, "18.00")
	mon, fake = make_monitor(
		positions=[make_pos("F1", "TMON@", "2", "2")],
		stops=[existing],
		instruments={"F1": make_inst("F1", "18.00", bid="18.40", ask="18.41")},
	)
	await mon.reconcile_stops()
	assert "s1" in fake.cancelled
	assert len(fake.posted) == 1
	assert fake.posted[0][2] == LONG


@pytest.mark.asyncio
async def test_wrong_lot_count_stop_replaced():
	existing = make_stop("s1", "F1", LONG, 1, "18.00")
	mon, fake = make_monitor(
		positions=[make_pos("F1", "TMON@", "4", "4")],
		stops=[existing],
		instruments={"F1": make_inst("F1", "18.00", bid="18.40", ask="18.41")},
	)
	await mon.reconcile_stops()
	assert "s1" in fake.cancelled
	assert len(fake.posted) == 1
	assert fake.posted[0][3] == 4


@pytest.mark.asyncio
async def test_orphan_stop_cancelled_when_position_closed():
	existing = make_stop("s1", "F1", LONG, 2, "18.00")
	mon, fake = make_monitor(
		positions=[],  # no open position anymore
		stops=[existing],
		instruments={"F1": make_inst("F1", "18.00", bid="18.40", ask="18.41")},
	)
	await mon.reconcile_stops()
	assert "s1" in fake.cancelled
	assert fake.posted == []


@pytest.mark.asyncio
async def test_no_stop_when_ma_on_wrong_side_of_market():
	# MA 18.50 above market for a long -> a SELL stop there would be rejected
	mon, fake = make_monitor(
		positions=[make_pos("F1", "TMON@", "2", "2")],
		stops=[],
		instruments={"F1": make_inst("F1", "18.50", bid="18.40", ask="18.41")},
	)
	await mon.reconcile_stops()
	assert fake.posted == []
	assert mon.stop_by_ticker == {}


@pytest.mark.asyncio
async def test_no_ma_skips_stop():
	mon, fake = make_monitor(
		positions=[make_pos("F1", "TMON@", "2", "2")],
		stops=[],
		instruments={"F1": make_inst("F1", None, bid="18.40", ask="18.41")},
	)
	await mon.reconcile_stops()
	assert fake.posted == []


@pytest.mark.asyncio
async def test_unmonitored_position_with_no_stop_is_left_alone():
	# position on a ticker we do not track and no resting stop -> no action
	mon, fake = make_monitor(
		positions=[make_pos("FX", "GHOST", "5", "5")],
		stops=[],
		instruments={},  # not monitored
	)
	await mon.reconcile_stops()
	assert fake.posted == []
	assert fake.cancelled == []


@pytest.mark.asyncio
async def test_unmonitored_stop_for_closed_position_cancelled():
	# leftover stop on a figi we no longer monitor, position closed
	existing = make_stop("s1", "FX", LONG, 2, "18.00")
	mon, fake = make_monitor(
		positions=[],
		stops=[existing],
		instruments={},
	)
	await mon.reconcile_stops()
	assert "s1" in fake.cancelled


@pytest.mark.asyncio
async def test_reconcile_is_idempotent():
	mon, fake = make_monitor(
		positions=[make_pos("F1", "TMON@", "2", "2")],
		stops=[],
		instruments={"F1": make_inst("F1", "18.00", bid="18.40", ask="18.41")},
	)
	await mon.reconcile_stops()
	# second run must see the stop we just placed and keep it
	fake.stops = [make_stop("s1", "F1", LONG, 2, "18.00")]
	await mon.reconcile_stops()
	assert len(fake.posted) == 1
	assert fake.cancelled == []
