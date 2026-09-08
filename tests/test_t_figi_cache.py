# test_t_figi_cache.py
import os
import sys
from types import SimpleNamespace

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src", "t_trader"))

from t_services import (
	etf_ticker_to_figi,
	share_ticker_to_figi,
	ticker_figi_cache,
)


def inst(ticker, figi):
	return SimpleNamespace(ticker=ticker, figi=figi)


class FakeInstruments:
	def __init__(self, etfs=(), shares=()):
		self.etf_list = list(etfs)
		self.share_list = list(shares)
		self.calls = {"etfs": 0, "shares": 0}

	async def etfs(self):
		self.calls["etfs"] += 1
		return SimpleNamespace(instruments=self.etf_list)

	async def shares(self):
		self.calls["shares"] += 1
		return SimpleNamespace(instruments=self.share_list)


def make_client(etfs=(), shares=()):
	instruments = FakeInstruments(etfs, shares)
	return SimpleNamespace(instruments=instruments), instruments


@pytest.fixture(autouse=True)
def clean_cache(monkeypatch):
	ticker_figi_cache._ticker_to_figi = {}
	ticker_figi_cache._figi_to_ticker = {}
	ticker_figi_cache._type_by_ticker = {}
	monkeypatch.setattr(ticker_figi_cache, "save", lambda: None)


@pytest.mark.asyncio
async def test_legacy_cached_share_not_returned_by_etf_lookup():
	# MTSS regression: the cache holds a share figi without a type (legacy
	# entry). The etf lookup must NOT return it, otherwise the instrument
	# gets treated as an ETF and init fails with NOT_FOUND.
	ticker_figi_cache._ticker_to_figi["MTSS"] = "BBG004S681W1"
	ticker_figi_cache._figi_to_ticker["BBG004S681W1"] = "MTSS"
	client, _ = make_client(
		etfs=[inst("SBMX", "ETF-FIGI")],
		shares=[inst("MTSS", "BBG004S681W1")],
	)
	assert await etf_ticker_to_figi(client, "MTSS") is None
	assert await share_ticker_to_figi(client, "MTSS") == "BBG004S681W1"
	assert ticker_figi_cache.type("MTSS") == "share"


@pytest.mark.asyncio
async def test_typed_cached_share_uses_fast_path_without_scan():
	ticker_figi_cache.update("MTSS", "BBG004S681W1", "share")
	client, instruments = make_client()
	assert await etf_ticker_to_figi(client, "MTSS") is None
	assert instruments.calls["etfs"] == 0
	assert await share_ticker_to_figi(client, "MTSS") == "BBG004S681W1"
	assert instruments.calls["shares"] == 0


@pytest.mark.asyncio
async def test_typed_cached_etf_not_returned_by_share_lookup():
	ticker_figi_cache.update("TMON@", "TCS70A106DL2", "etf")
	client, instruments = make_client()
	assert await etf_ticker_to_figi(client, "TMON@") == "TCS70A106DL2"
	assert instruments.calls["etfs"] == 0
	assert await share_ticker_to_figi(client, "TMON@") is None
	assert instruments.calls["shares"] == 0


@pytest.mark.asyncio
async def test_uncached_share_is_found_and_typed():
	client, instruments = make_client(
		etfs=[inst("SBMX", "ETF-FIGI")],
		shares=[inst("MTSS", "BBG004S681W1")],
	)
	assert await etf_ticker_to_figi(client, "MTSS") is None
	assert await share_ticker_to_figi(client, "MTSS") == "BBG004S681W1"
	assert ticker_figi_cache.type("MTSS") == "share"


@pytest.mark.asyncio
async def test_legacy_cached_share_still_returns_none_when_not_in_lists():
	# a stale legacy figi that no longer appears in the listings must not be
	# returned either: the resolvers only trust a listing hit
	ticker_figi_cache._ticker_to_figi["GONE"] = "STALE-FIGI"
	ticker_figi_cache._figi_to_ticker["STALE-FIGI"] = "GONE"
	client, _ = make_client()
	assert await etf_ticker_to_figi(client, "GONE") is None
	assert await share_ticker_to_figi(client, "GONE") is None
	assert ticker_figi_cache.type("GONE") is None
