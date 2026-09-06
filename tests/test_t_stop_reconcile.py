# test_t_stop_reconcile.py
import os
import sys
from decimal import Decimal
from types import SimpleNamespace

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src", "t_trader"))

from t_tech.invest import CandleInterval
from t_tech.invest.schemas import (
	StopOrderDirection,
	StopOrderType,
)
from t_tech.invest.utils import (
	decimal_to_money,
	decimal_to_quotation,
)

from t_order_manager import InstrumentMonitor, OrderMonitor

SELL = StopOrderDirection.STOP_ORDER_DIRECTION_SELL
BUY = StopOrderDirection.STOP_ORDER_DIRECTION_BUY
TP = StopOrderType.STOP_ORDER_TYPE_TAKE_PROFIT
SL = StopOrderType.STOP_ORDER_TYPE_STOP_LOSS


def make_pos(figi, ticker, qty, lots, avg="0", itype="share"):
	return SimpleNamespace(
		figi=figi,
		ticker=ticker,
		instrument_type=itype,
		quantity=decimal_to_quotation(Decimal(qty)),
		quantity_lots=decimal_to_quotation(Decimal(lots)),
		average_position_price_fifo=decimal_to_money(Decimal(str(avg)), "rub") if avg else None,
	)


def make_inst(figi, ma, bid, ask):
	inst = InstrumentMonitor(
		client=None, figi=figi, type="etf",
		candle_interval=CandleInterval.CANDLE_INTERVAL_5_MIN,
		period=6, lots=1,
	)
	inst.ma = Decimal(str(ma)) if ma is not None else None
	inst.bid = decimal_to_quotation(Decimal(str(bid))) if bid is not None else None
	inst.ask = decimal_to_quotation(Decimal(str(ask))) if ask is not None else None
	inst.min_price_increment = decimal_to_quotation(Decimal("0.01"))
	return inst


def make_stop(sid, figi, direction, order_type, lots, price):
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
		self.cancelled_stops = []
		self.cancelled_limits = []
		self.posted = []

	async def get_active_stop_orders(self):
		return list(self.stops)

	async def cancel_stop_order(self, stop_order_id):
		self.cancelled_stops.append(stop_order_id)

	async def cancel_order_silent(self, order_id):
		self.cancelled_limits.append(order_id)

	async def post_stop_order(self, figi, stop_price_decimal, direction, lots,
							  stop_order_type=StopOrderType.STOP_ORDER_TYPE_STOP_LOSS):
		self.posted.append((figi, stop_price_decimal, direction, lots, stop_order_type))
		return f"stop-{len(self.posted)}"


def make_monitor(positions, stops, instruments):
	fake_om = FakeOrderManager(stops)
	fake_om.account_manager = FakeAccountManager(positions)
	monitor = OrderMonitor(client=None, order_manager=fake_om)
	for figi, inst in instruments.items():
		monitor.instrument_list_by_figi[figi] = inst
	return monitor, fake_om


# ---------------- long, market above MA (profit zone): trailing SELL SL at MA

@pytest.mark.asyncio
async def test_long_above_ma_trails_sell_stop_at_ma():
	mon, fake = make_monitor(
		positions=[make_pos("F1", "TMON@", "2", "2")],
		stops=[],
		instruments={"F1": make_inst("F1", "18.00", bid="18.40", ask="18.41")},
	)
	await mon.reconcile_stops()
	assert len(fake.posted) == 1
	figi, price, direction, lots, otype = fake.posted[0]
	assert figi == "F1"
	assert direction == SELL
	assert otype == SL
	assert lots == 2
	assert price == Decimal("18.00")
	assert mon.stop_by_ticker["TMON@"] == Decimal("18.00")


# ---------------- short, market below MA (profit zone): trailing BUY SL at MA

@pytest.mark.asyncio
async def test_short_below_ma_trails_buy_stop_at_ma():
	mon, fake = make_monitor(
		positions=[make_pos("F2", "MTSS", "-30", "-3")],
		stops=[],
		instruments={"F2": make_inst("F2", "190.00", bid="189.15", ask="189.16")},
	)
	await mon.reconcile_stops()
	assert len(fake.posted) == 1
	figi, price, direction, lots, otype = fake.posted[0]
	assert direction == BUY
	assert otype == SL
	assert lots == 3
	assert price == Decimal("190.00")


# ---------------- long, market below MA (dip zone): TP at MA + hard SL below avg

@pytest.mark.asyncio
async def test_long_dip_zone_places_take_profit_and_hard_stop():
	mon, fake = make_monitor(
		positions=[make_pos("F1", "TMON@", "2", "2", avg="17.97")],
		stops=[],
		instruments={"F1": make_inst("F1", "18.00", bid="17.95", ask="17.96")},
	)
	await mon.reconcile_stops()
	assert len(fake.posted) == 2
	types = {o for (_f, _p, _d, _l, o) in fake.posted}
	assert types == {TP, SL}
	by_type = {o: (d, p) for (_f, p, d, _l, o) in fake.posted}
	# take-profit SELL at MA (above market)
	assert by_type[TP][0] == SELL
	assert by_type[TP][1] == Decimal("18.00")
	# hard stop SELL below average entry (17.97 - 2 * 0.15%)
	assert by_type[SL][0] == SELL
	assert by_type[SL][1] <= Decimal("17.97")


# ---------------- short, market above MA (rip zone): TP at MA + hard SL above avg

@pytest.mark.asyncio
async def test_short_rip_zone_places_take_profit_and_hard_stop():
	mon, fake = make_monitor(
		positions=[make_pos("F2", "MTSS", "-30", "-3", avg="190.20")],
		stops=[],
		instruments={"F2": make_inst("F2", "190.00", bid="190.35", ask="190.36")},
	)
	await mon.reconcile_stops()
	assert len(fake.posted) == 2
	types = {o for (_f, _p, _d, _l, o) in fake.posted}
	assert types == {TP, SL}
	by_type = {o: (d, p) for (_f, p, d, _l, o) in fake.posted}
	assert by_type[TP][0] == BUY
	assert by_type[TP][1] == Decimal("190.00")
	assert by_type[SL][0] == BUY
	assert by_type[SL][1] >= Decimal("190.20")


# ---------------- matching stops are kept

@pytest.mark.asyncio
async def test_matching_stop_is_kept():
	existing = make_stop("s1", "F1", SELL, SL, 2, "18.00")
	mon, fake = make_monitor(
		positions=[make_pos("F1", "TMON@", "2", "2")],
		stops=[existing],
		instruments={"F1": make_inst("F1", "18.00", bid="18.40", ask="18.41")},
	)
	await mon.reconcile_stops()
	assert fake.posted == []
	assert fake.cancelled_stops == []
	assert mon.stop_by_ticker["TMON@"] == Decimal("18.00")


# ---------------- trailing stop moves along the MA

@pytest.mark.asyncio
async def test_stop_moved_along_ma():
	existing = make_stop("s1", "F1", SELL, SL, 2, "17.90")
	mon, fake = make_monitor(
		positions=[make_pos("F1", "TMON@", "2", "2")],
		stops=[existing],
		instruments={"F1": make_inst("F1", "18.05", bid="18.40", ask="18.41")},
	)
	await mon.reconcile_stops()
	assert "s1" in fake.cancelled_stops
	assert len(fake.posted) == 1
	assert fake.posted[0][1] == Decimal("18.05")
	assert fake.posted[0][4] == SL


# ---------------- stale stops of the wrong type/direction/lots get replaced

@pytest.mark.asyncio
async def test_wrong_direction_stop_replaced():
	existing = make_stop("s1", "F1", BUY, SL, 2, "18.00")
	mon, fake = make_monitor(
		positions=[make_pos("F1", "TMON@", "2", "2")],
		stops=[existing],
		instruments={"F1": make_inst("F1", "18.00", bid="18.40", ask="18.41")},
	)
	await mon.reconcile_stops()
	assert "s1" in fake.cancelled_stops
	assert len(fake.posted) == 1
	assert fake.posted[0][2] == SELL


@pytest.mark.asyncio
async def test_wrong_lot_count_stop_replaced():
	existing = make_stop("s1", "F1", SELL, SL, 1, "18.00")
	mon, fake = make_monitor(
		positions=[make_pos("F1", "TMON@", "4", "4")],
		stops=[existing],
		instruments={"F1": make_inst("F1", "18.00", bid="18.40", ask="18.41")},
	)
	await mon.reconcile_stops()
	assert "s1" in fake.cancelled_stops
	assert len(fake.posted) == 1
	assert fake.posted[0][3] == 4


@pytest.mark.asyncio
async def test_take_profit_downgraded_to_trailing_when_market_crosses_ma():
	# long bought in the dip with a resting TP above MA at 18.00; MA fell to
	# 17.80 and price is 18.40 -> market now above MA, exit is a trailing SL
	existing = make_stop("s1", "F1", SELL, TP, 2, "18.00")
	mon, fake = make_monitor(
		positions=[make_pos("F1", "TMON@", "2", "2")],
		stops=[existing],
		instruments={"F1": make_inst("F1", "17.80", bid="18.40", ask="18.41")},
	)
	await mon.reconcile_stops()
	assert "s1" in fake.cancelled_stops
	assert len(fake.posted) == 1
	assert fake.posted[0][1] == Decimal("17.80")
	assert fake.posted[0][4] == SL


# ---------------- flat / orphan handling

@pytest.mark.asyncio
async def test_orphan_stop_cancelled_when_position_closed():
	existing = make_stop("s1", "F1", SELL, SL, 2, "18.00")
	inst = make_inst("F1", "18.00", bid="18.40", ask="18.41")
	inst.hold_side = "long"
	mon, fake = make_monitor(
		positions=[],
		stops=[existing],
		instruments={"F1": inst},
	)
	await mon.reconcile_stops()
	assert "s1" in fake.cancelled_stops
	assert fake.posted == []
	assert inst.hold_side is None


@pytest.mark.asyncio
async def test_unmonitored_position_with_no_stop_is_left_alone():
	mon, fake = make_monitor(
		positions=[make_pos("FX", "GHOST", "5", "5")],
		stops=[],
		instruments={},
	)
	await mon.reconcile_stops()
	assert fake.posted == []
	assert fake.cancelled_stops == []


@pytest.mark.asyncio
async def test_unmonitored_stop_for_closed_position_cancelled():
	existing = make_stop("s1", "FX", SELL, SL, 2, "18.00")
	mon, fake = make_monitor(
		positions=[],
		stops=[existing],
		instruments={},
	)
	await mon.reconcile_stops()
	assert "s1" in fake.cancelled_stops


# ---------------- MA / market price edge cases

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
async def test_ma_pinned_at_market_places_nothing():
	# MA between bid/ask and effectively at the market: no resting order is
	# valid on either side, so the reconcile must not attempt to post
	mon, fake = make_monitor(
		positions=[make_pos("F1", "TMON@", "2", "2", avg="17.98")],
		stops=[],
		instruments={"F1": make_inst("F1", "18.00", bid="18.00", ask="18.01")},
	)
	await mon.reconcile_stops()
	assert fake.posted == []


# ---------------- opposite bracket suppression

@pytest.mark.asyncio
async def test_long_cancels_opposite_sell_bracket():
	inst = make_inst("F1", "18.00", bid="18.40", ask="18.41")
	inst.hi_order = "hi-1"
	inst.sell_limit = Decimal("18.20")
	mon, fake = make_monitor(
		positions=[make_pos("F1", "TMON@", "2", "2")],
		stops=[],
		instruments={"F1": inst},
	)
	await mon.reconcile_stops()
	assert "hi-1" in fake.cancelled_limits
	assert inst.hi_order is None
	assert inst.sell_limit is None
	assert inst.hold_side == "long"


@pytest.mark.asyncio
async def test_short_cancels_opposite_buy_bracket():
	inst = make_inst("F2", "190.00", bid="189.15", ask="189.16")
	inst.lo_order = "lo-1"
	inst.buy_limit = Decimal("189.80")
	mon, fake = make_monitor(
		positions=[make_pos("F2", "MTSS", "-30", "-3", avg="190.20")],
		stops=[],
		instruments={"F2": inst},
	)
	await mon.reconcile_stops()
	assert "lo-1" in fake.cancelled_limits
	assert inst.lo_order is None
	assert inst.buy_limit is None
	assert inst.hold_side == "short"


# ---------------- idempotence

@pytest.mark.asyncio
async def test_reconcile_is_idempotent():
	mon, fake = make_monitor(
		positions=[make_pos("F1", "TMON@", "2", "2")],
		stops=[],
		instruments={"F1": make_inst("F1", "18.00", bid="18.40", ask="18.41")},
	)
	await mon.reconcile_stops()
	fake.stops = [make_stop("s1", "F1", SELL, SL, 2, "18.00")]
	fake.posted.clear()
	await mon.reconcile_stops()
	assert fake.posted == []
	assert fake.cancelled_stops == []


@pytest.mark.asyncio
async def test_dip_zone_reconcile_is_idempotent():
	inst = make_inst("F1", "18.00", bid="17.95", ask="17.96")
	mon, fake = make_monitor(
		positions=[make_pos("F1", "TMON@", "2", "2", avg="17.97")],
		stops=[],
		instruments={"F1": inst},
	)
	await mon.reconcile_stops()
	assert len(fake.posted) == 2
	# restate exactly what the broker now holds and reconcile again
	hard = min(p for (_f, p, _d, _l, o) in fake.posted if o == SL)
	fake.stops = [
		make_stop("s1", "F1", SELL, TP, 2, "18.00"),
		make_stop("s2", "F1", SELL, SL, 2, str(hard)),
	]
	fake.posted.clear()
	await mon.reconcile_stops()
	assert fake.posted == []
	assert fake.cancelled_stops == []
