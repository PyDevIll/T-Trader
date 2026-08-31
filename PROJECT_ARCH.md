# T-Trader — Project Architecture

Reference for the T-Trader project: an LLM-managed trading bot built on the
T-Bank **T-Invest** API (https://developer.tbank.ru/invest/api), using the
official Python SDK `t-tech-investments`.

Everything currently runs against the **sandbox** (paper trading) API. The real
exchange API mirrors the sandbox one method-for-method.

---

## 1. Package layout

```
T-Trader/
├── pyproject.toml          # Poetry project; SDK pinned: t-tech-investments 1.49.3
├── .env                    # Secrets (T_INVEST_TOKEN_SANDBOX, etc.) — never commit
├── invest_token            # (token file, gitignored)
├── ticker_figi_cache.txt   # JSON cache: ticker <-> figi pairs
├── src/t_trader/
│   ├── __init__.py         # empty package marker
│   ├── t_services.py       # low-level wrappers: accounts, orders, streams, ticker cache
│   ├── t_order_manager.py  # trading logic: MA rebalancing, order monitor
│   └── t_scheduler.py      # scheduled (hourly) trading logic
└── tests/
    ├── __init__.py
    └── test_t_services.py  # integration tests against the sandbox
```

**Entry points** (each module's `if __name__ == "__main__"`):

| Module | Main entry | What it runs |
|---|---|---|
| `t_services.py` | `orderbook_streaming()` | subscribes to orderbook stream for one ETF |
| `t_order_manager.py` | `test_order_monitor()` | MA-rebalancing monitor for several instruments |
| `t_scheduler.py` | `scheduled_trading()` / `test_scheduler()` | hourly scheduled buy/sell |

---

## 2. `src/t_trader/t_services.py` — low-level service wrappers

The foundation layer. Knows how to talk to the SDK but contains **no trading
strategy**.

### Functions

- `instruments_by_filter(instruments, filter_dict)` — filters a list of
  instrument objects (`Etf`/`Share`/...) using a `{"pos": {...}, "neg": {...}}`
  dict of attribute values; returns `{ticker: {...}}`. Used by tests to build
  a curated universe.

- `etf_ticker_to_figi(client, ticker)` / `share_ticker_to_figi(client, ticker)`
  — async. Resolve a ticker to `figi` (checking `ticker_figi_cache` first,
  then scanning `client.instruments.etfs()` / `.shares()`). Caches on hit.

### Classes

- `ticker_figi_cache` — pure-class static cache.
  - State: class dicts `_ticker_to_figi`, `_figi_to_ticker`, persisted to
    `ticker_figi_cache.txt`.
  - API: `init()` (load from disk), `save()`, `figi(ticker)`, `ticker(figi)`,
    `update(ticker, figi)`.
  - Must call `ticker_figi_cache.init()` at startup (done in the main blocks).

- `AccountManagerSandbox` — wraps sandbox **account + money**.
  - `__init__(client)` stores the `AsyncSandboxClient`; `self.account = None`.
  - `connect(name="default")` → finds account by name or opens a new one;
    stores it in `self.account` and returns `self` (chainable).
  - `open_account(name)`, `get_account(name)`.
  - Money: `get_balance()` → `Decimal` RUB (via `get_sandbox_withdraw_limits`),
    `get_balance_raw()`, `pay_in(amount_decimal)`.
  - Reporting: `account_operations(from_, to)` (prints+returns ops),
    `get_positions()` (prints+returns portfolio positions).
  - **Used by** `OrderManagerSandbox` and `OrderMonitor`.

- `OrderManagerSandbox` — wraps sandbox **order lifecycle**.
  - `__init__(client, account_manager)`; takes `account_id` from the manager.
  - `post_order(figi, price, order_type, order_direction, lots)` — generates a
    UUID `order_id` for **idempotency**, calls `post_sandbox_order`, checks the
    `execution_report_status` against `EXECUTION_REPORT_STATUS_REJECTED`,
    returns the order id (or `None`).
  - `change_order(order_id, price_quotation, lots)` — `replace_sandbox_order`
    with a new idempotency key.
  - `get_order(order_id)` / `list_orders()` — status via `get_sandbox_order_state`
    / `get_sandbox_orders`; print rich status incl. stages and commissions.
  - `cancel_order(order_id)`.
  - Market checks: `get_tradables_from(figi_list)` (keeps only instruments in
    `NORMAL_TRADING` / `DEALER_NORMAL_TRADING`), `get_statuses_raw(figi_list)`,
    `get_bid_ask(figi)` (depth-1 orderbook → `(bid, ask)`).
  - **Used by** `InstrumentMonitor`, `OrderMonitor`, `Scheduler`.

- `StreamMonitor` (base class) — resilient streaming loop.
  - `__init__(client)`; holds `self.stream`, `self.running_task`,
    `self.input_task`, `self.stop`, `self.show`.
  - `monitor()` — `while not self.stop`: runs `_monitor()`, catches
    `AioRequestError`, applies **exponential backoff with jitter**
    (base 1s → max 60s, up to 10 retries), resets retry counter on success.
    Cancels tasks on `CancelledError`/exit.
  - `_monitor()` — abstract; subclasses implement the stream subscription loop.

- `OrderbookMonitor(StreamMonitor)` — orderbook stream consumer.
  - `self.instruments` (list of `OrderBookInstrument`), `self.tracking_values_by_figi`.
  - `init_tracking_values(figi)` — seeds `min_limit`/`max_limit` trackers.
  - `_track_values(orderbook_data)` — records min/max `limit_up`/`limit_down`.
  - `set_instruments(figi_list)` — stops any active stream, (re)builds
    `OrderBookInstrument(figi, depth=10)` subscriptions.
  - `_monitor()` — creates stream, subscribes orderbook, iterates responses,
    calls `_track_values`.

---

## 3. `src/t_trader/t_order_manager.py` — trading logic (MA rebalancer)

Depends on `t_services` for the wrappers above. Implements a mean-reversion-ish
**moving-average** strategy: keep a limit BUY slightly below MA and a limit SELL
slightly above MA; re-place them as MA drifts.

### Functions

- `MA(period, candle_history)` — simple mean of the last `period` candle closes
  (`Decimal`); returns `None` if insufficient data.

### Classes

- `InstrumentMonitor` — per-instrument strategy state.
  - `__init__(client, figi, type, candle_interval, period, lots=1,
    allow_buying=True, allow_selling=True)`.
    - `type` is `"etf"`/`"share"` (drives which SDK lookup to use).
    - `period` = candle count for MA.
    - `deviation_percent` = band width (default 0.15%).
    - `hi_order` / `lo_order` — current SELL/BUY order ids (`None` = none).
  - Data: `update_candles()` fetches history via `get_candles` and (re)computes
    MA; `update_bid_ask(orderbook)` reads best bid/ask from a streamed orderbook.
  - Pricing: `quantize(price_decimal)` rounds a price down to the instrument's
    `min_price_increment` (loaded from `etf_by`/`share_by` in `init_instruments`).
  - Order mgmt: `_move_order(order_id, price_quotation, order_type_str)` —
    creates a new limit order if none, or calls `replace_order` only when the
    price drifted more than `3 * min_price_increment`; returns `None` if the
    old order is gone (so a fresh one is placed). `move_orders()` computes
    SELL=MA×(1+dev), BUY=MA×(1−dev), quantizes, and moves both orders honoring
    `allow_buying`/`allow_selling`.
  - Actions: `buy()`, `buylimit()`, `sell()`, `selllimit()` (market/limit via
    `OrderManagerSandbox.post_order`).
  - **Owned by** `OrderMonitor` (via `add_instrument`); set `.order_manager`.

- `OrderMonitor(StreamMonitor)` — the orchestrator.
  - `self.instrument_list_by_figi` (`{figi: InstrumentMonitor}`),
    `self.order_manager`, `self.account_manager`, `self.last_market_response`.
  - `add_instrument(instrument_monitor)` — wires `.order_manager` and indexes it.
  - `init_instruments()` — loads candles + `min_price_increment` per instrument,
    then reconciles existing limit orders: re-adopts tracked BUY/SELL orders,
    cancels duplicates, cancels orders disallowed by flags.
  - `process_stream_response(market_response, figi)` — routes stream events:
    orderbook → `update_bid_ask`; last_price → log; finished candle →
    `update_candles()` + `move_orders()`.
  - `get_user_input()` / `process_user_action()` — interactive keyboard loop
    (b/s = buy/sell current, l = list, + = pay in, o/a/p = ops/balance/positions).
  - `_monitor()` — subscribes **orderbook (depth 1) + last_price + candles
    (`waiting_close`)** for all tracked instruments; iterates responses and
    calls `process_stream_response`. Also prints the interactive menu line.

---

## 4. `src/t_trader/t_scheduler.py` — scheduled trading

Automates hourly "call auction"-style trades: buy scheduled tickers at the top
of the hour, sell tickers that dropped off the schedule.

### Data

- `schedule_tab_shares`, `schedule_tab_etfs` — `{hour_str: {weekday: [tickers]}}`
  tables (auction-exchange-specific day/hour lists).

### Functions / Classes

- `everyNMinutes(sleep_period=60, minutes=5)` — async helper that sleeps and
  returns when the clock hits an `x:00`/`x:05`... boundary.

- `Scheduler` — the scheduling state machine.
  - `__init__(schedule_tab, order_manager)`; tracks `timer_task`,
    `orders_by_ticker` (`{ticker: order_id}`), `ticker_list`,
    `prev_ticker_list`, `next_trade_time`.
  - `timer()` — loop calling `everyNMinutes()` then `process_trades()`.
  - `process_trades()` — two windows:
    1. 5 min **before** `next_trade_time`: look up scheduled tickers for that
       hour/weekday, filter by trading status, sell tickers no longer scheduled.
    2. up to 5 min **at/after** `next_trade_time`: buy newly scheduled tickers,
       advance `next_trade_time` to the next hour (skipping the 16:00/04:00
       UTC closed windows).
  - `check_trading_statuses()` — resolve tickers→figi, keep only tradable ones.
  - `buy_scheduled()` / `sell_scheduled()` — iterate tickers, skip kept ones,
    place MARKET orders via `order_manager`.
  - `buy(ticker)` / `sell(ticker)` — resolve figi, get bid/ask, post MARKET
    order, verify status is NEW or FILL before returning the order id.

- `now_mock` — freezes "now" at a fixed delta from real time to replay scenarios
  in `test_scheduler()`.

---

## 5. `tests/test_t_services.py`

Integration tests hitting the live sandbox (needs `T_INVEST_TOKEN_SANDBOX` in
`.env`). Session-scoped dotenv load; `client` fixture yields an
`AsyncSandboxClient`.
- `test_get_instruments` — filters ETF universe, asserts known tickers present.
- `test_account_manager` — opens account, verifies `pay_in` moves balance by the
  exact amount, lists operations.

Note: this test imports `AccountManager` and `OrderbookMonitor` from
`t_services`; the current module defines `AccountManagerSandbox` and
`OrderbookMonitor` — reconcile names if the suite is to pass as-is.

---

## 6. Class dependency map

```
Scheduler ──uses──> OrderManagerSandbox ──uses──> AccountManagerSandbox
                        ▲                              ▲
                        │                              │
OrderMonitor(StreamMonitor) ──owns─[0..*]──> InstrumentMonitor
      │                                        │
      │                                        └──uses──> OrderManagerSandbox
      └──uses──> AccountManagerSandbox

StreamMonitor (base) ◄── OrderbookMonitor, OrderMonitor

ticker_figi_cache  ── used by everyone to map ticker <-> figi
etf_ticker_to_figi / share_ticker_to_figi ── populate the cache
```

All wrappers hold an `AsyncSandboxClient` / `AsyncSandboxServices` instance and
call methods on `client.sandbox.*`, `client.market_data.*`,
`client.instruments.*`.

---

## 7. Where to find T-Invest SDK modules to import

The SDK `t-tech-investments` is installed from the T-Bank opensource PyPI
mirror (`opensource.tbank.ru/api/v4/projects/238/packages/pypi/simple`).
Import root: **`t_tech.invest`**. Key locations:

| Import | Purpose |
|---|---|
| `from t_tech.invest.sandbox.async_client import AsyncSandboxClient` | Async sandbox client (`token=`). Entry point for all current code. |
| `from t_tech.invest.clients import AsyncClient, Client` | Real-exchange clients. `AsyncClient(token)` gives live trading; sandbox/real share the same service surface. |
| `from t_tech.invest.async_services import AsyncServices` | The `AsyncClient.__aenter__` result. Has `.instruments`, `.market_data`, `.operations`, `.orders`, `.users`, `.stop_orders`, `.sandbox`, `.signals`, `.create_market_data_stream()`. |
| `from t_tech.invest.services import Services` | Same layout, sync variant (used in tests with `SandboxClient`). |
| `from t_tech.invest.schemas import (...)` | **All request/response dataclasses + enums**: `CandleInterval`, `CandleSource`, `InstrumentStatus`, `InstrumentIdType`, `OrderType`, `OrderDirection`, `TimeInForceType`, `OrderExecutionReportStatus`, `OrderIdType`, `ReplaceOrderRequest`, `TradeInstrument`, `CandleInstrument`, `OrderBookInstrument`, `GetTechAnalysisRequest`, `GetMarketValuesRequest`, ... |
| `from t_tech.invest.utils import (...)` | `now`, `candle_interval_to_timedelta`, `quotation_to_decimal`, `decimal_to_quotation`, `decimal_to_money`, `money_to_decimal`. Always convert at the boundary — LLM/code work in `Decimal`/str. |
| `from t_tech.invest.exceptions import AioRequestError` | Async request errors (`.metadata.message`); used by stream retry logic. |
| `from t_tech.invest import (CandleInterval, InstrumentIdType, Quotation, OrderBookInstrument, SecurityTradingStatus)` | Re-exported conveniences. |
| `t_tech.invest.grpc.*` | Low-level protobuf stubs/messages (e.g. `common_pb2.CandleInterval`). Avoid unless needed. |
| `t_tech.invest.market_data_stream` | Stream managers behind `create_market_data_stream()` (bidirectional + server-side). |
| `t_tech.invest.strategies` | SDK-shipped example strategies (e.g. `moving_average` trader/supervisor/signal-executor) — reference patterns, not used here. |
| `t_tech.invest.caching` | Optional instruments/market-data caches to cut repeated calls. |

### Key service methods used by this project (sandbox)

- `client.sandbox`: `open_sandbox_account`, `get_sandbox_accounts`,
  `sandbox_pay_in`, `get_sandbox_withdraw_limits`, `post_sandbox_order`,
  `replace_sandbox_order`, `cancel_sandbox_order`, `get_sandbox_orders`,
  `get_sandbox_order_state`, `get_sandbox_operations`, `get_sandbox_portfolio`.
- `client.market_data`: `get_candles`, `get_last_prices`, `get_order_book`,
  `get_trading_statuses`, `get_tech_analysis` (request-object based),
  `get_market_values` (request-object based).
- `client.instruments`: `etfs`, `shares`, `etf_by`, `share_by`,
  `find_instrument`, `get_instrument_by`, `get_dividends`,
  `get_asset_fundamentals`, `get_consensus_forecasts`.

Full REST + gRPC reference (auth, schemas, per-method request/response fields):
https://developer.tbank.ru/invest/api — each method page lists the Bearer-token
REST endpoint and its JSON body. The gRPC service is
`tinkoff.public.invest.api.contract.v1.<Service>/<Method>`.

---

## 8. Gotchas / expert notes

- **Money everywhere**: prices are `units`+`nano`. Never compare raw values;
  always go through `utils` converters.
- **Order quantities are in lots**, not shares; validate with
  `get_max_lots`/`get_sandbox_max_lots` before posting.
- **Idempotency**: always pass a fresh UUID as `order_id`/`idempotency_key` —
  this is what makes an LLM retry safe.
- **Liquidity of statuses**: an instrument that isn't trading simply emits no
  stream data; refresh `get_trading_statuses` explicitly if you need it.
- **Stream resilience**: wrap streams in the `StreamMonitor` retry pattern
  (exponential backoff + jitter); a single `AioRequestError` must not kill the bot.
- **`get_tech_analysis`/`get_market_values`** are marked deprecated in the SDK
  but are current in the REST docs — prefer request-object calls or the REST
  endpoint; verify against the live docs before relying on them for the LLM layer.
- **Sandbox ≠ real**: `AsyncSandboxClient` points at the sandbox gRPC target.
  Swapping in `AsyncClient` + a real token + `client.orders.*` (non-sandbox)
  is the promotion path to live trading.
