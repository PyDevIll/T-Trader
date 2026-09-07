# test_t_order_settings.py
import json
import os
import sys
from decimal import Decimal

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src", "t_trader"))

from t_tech.invest.schemas import CandleInterval

import t_order_manager as m


class FakeOrderManager:
	account_manager = None


def make_monitor():
	fake = FakeOrderManager()
	monitor = m.OrderMonitor(client=None, order_manager=fake)
	return monitor


def fake_resolvers(monkeypatch, figis):
	async def etf(client, ticker, verbose=False):
		return figis.get(ticker, {}).get("etf")

	async def share(client, ticker, verbose=False):
		return figis.get(ticker, {}).get("share")

	monkeypatch.setattr(m, "etf_ticker_to_figi", etf)
	monkeypatch.setattr(m, "share_ticker_to_figi", share)


# ---------------- loading / normalizing ticker_settings.json

def test_load_ticker_settings_normalizes_types(tmp_path):
	path = tmp_path / "ticker_settings.json"
	path.write_text(json.dumps({
		"safe": {
			"type": "etf",
			"candle_interval": "5M",
			"period": 6,
			"lots": 2,
			"allow_selling": False,
			"band_atr_mult": 0.5,
			"deviation_percent": 0.003,
			"sell_band_factor": 2,
			"hard_stop_dev_k": 3,
		},
	}))
	settings = m.load_ticker_settings(str(path))
	assert set(settings) == {"SAFE"}
	entry = settings["SAFE"]
	assert entry["type"] == "etf"
	assert entry["candle_interval"] == CandleInterval.CANDLE_INTERVAL_5_MIN
	assert entry["period"] == 6
	assert entry["lots"] == 2
	assert entry["allow_selling"] is False
	assert entry["band_atr_mult"] == Decimal("0.5")
	assert entry["deviation_percent"] == Decimal("0.003")
	assert entry["sell_band_factor"] == Decimal("2")
	assert entry["hard_stop_dev_k"] == 3


def test_load_ticker_settings_missing_file_returns_empty():
	assert m.load_ticker_settings("/nonexistent/ticker_settings.json") == {}


def test_load_ticker_settings_bad_candle_interval_raises(tmp_path):
	path = tmp_path / "ticker_settings.json"
	path.write_text(json.dumps({"SAFE": {"type": "etf", "candle_interval": "9M"}}))
	with pytest.raises(ValueError):
		m.load_ticker_settings(str(path))


def test_reload_ticker_settings_populates_global(tmp_path, monkeypatch):
	path = tmp_path / "ticker_settings.json"
	path.write_text(json.dumps({
		"AAA": {"type": "etf", "allow_selling": False},
		"bbb": {"type": "share", "lots": 4},
	}))
	monkeypatch.setattr(m, "TICKER_SETTINGS", {"old": {}})
	result = m.reload_ticker_settings(str(path))
	assert result is m.TICKER_SETTINGS
	assert set(m.TICKER_SETTINGS) == {"AAA", "BBB"}


# ---------------- building an instrument from a settings entry

@pytest.mark.asyncio
async def test_build_instrument_honors_settings_entry(monkeypatch):
	fake_resolvers(monkeypatch, {
		"SAFE": {"etf": "figi-SAFE"},
		"SBER": {"share": "figi-SBER"},
	})
	monkeypatch.setattr(m, "TICKER_SETTINGS", {
		"SAFE": {
			"type": "etf",
			"allow_selling": False,
			"deviation_percent": Decimal("0.003"),
			"sell_band_factor": Decimal("2"),
		},
		"SBER": {"type": "share", "lots": 4},
	})
	mon = make_monitor()

	figi, inst = await mon._build_instrument("SAFE")
	assert figi == "figi-SAFE"
	assert inst.type == "etf"
	assert inst.allow_selling is False
	assert inst.deviation_percent == Decimal("0.003")
	assert inst.sell_band_factor == Decimal("2")

	figi2, inst2 = await mon._build_instrument("SBER")
	assert figi2 == "figi-SBER"
	assert inst2.type == "share"
	assert inst2.lots == 4


@pytest.mark.asyncio
async def test_build_instrument_unknown_ticker_raises_lookup(monkeypatch):
	monkeypatch.setattr(m, "TICKER_SETTINGS", {})
	mon = make_monitor()

	async def no_resolve(ticker):
		return None, "etf"

	mon._resolve_figi = no_resolve
	with pytest.raises(LookupError):
		await mon._build_instrument("GHOST")


# ---------------- booting as many tickers as ticker_settings.json lists

@pytest.mark.asyncio
async def test_load_instruments_from_settings_boots_all_tickers(monkeypatch):
	fake_resolvers(monkeypatch, {
		"AAA": {"etf": "figi-AAA"},
		"BBB": {"share": "figi-BBB"},
		"CCC": {"etf": "figi-CCC"},
	})
	monkeypatch.setattr(m, "TICKER_SETTINGS", {
		"AAA": {"type": "etf", "allow_selling": False, "deviation_percent": Decimal("0.003")},
		"BBB": {"type": "share", "lots": 4},
		"CCC": {"type": "etf"},
	})
	mon = make_monitor()
	assert await mon.load_instruments_from_settings() == 3
	assert set(mon.instrument_list_by_figi) == {"figi-AAA", "figi-BBB", "figi-CCC"}

	aaa = mon.instrument_list_by_figi["figi-AAA"]
	assert aaa.allow_selling is False
	assert aaa.deviation_percent == Decimal("0.003")
	bbb = mon.instrument_list_by_figi["figi-BBB"]
	assert bbb.type == "share"
	assert bbb.lots == 4
	ccc = mon.instrument_list_by_figi["figi-CCC"]
	assert ccc.allow_selling is True


@pytest.mark.asyncio
async def test_load_instruments_skips_unresolvable_ticker(monkeypatch):
	figis = {"AAA": {"etf": "figi-AAA"}}

	async def etf(client, ticker, verbose=False):
		return figis.get(ticker, {}).get("etf")

	async def share(client, ticker, verbose=False):
		return None

	monkeypatch.setattr(m, "etf_ticker_to_figi", etf)
	monkeypatch.setattr(m, "share_ticker_to_figi", share)
	monkeypatch.setattr(m, "TICKER_SETTINGS", {
		"AAA": {"type": "etf"},
		"GHOST": {"type": "share"},
	})
	mon = make_monitor()
	assert await mon.load_instruments_from_settings() == 1
	assert set(mon.instrument_list_by_figi) == {"figi-AAA"}
