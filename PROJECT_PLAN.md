Proposed plan
1. Verify the pattern on real data — extend the playground into a regime.py that fetches 2–5y daily history for SAFE/TMON@/TGLD@/TPAY, computes the feature digest (trend slope/R², dip depths, recovery times, z-scores), and prints a "regime card." Validates your hypothesis with numbers before anything else.
2. Regime characterization — deterministic digest → optional LLM call → structured regime JSON (pattern type, dip parameters, break flags).
3. Live regime-adaptive signal — replace the fixed ±0.15% bracket with: entry when price ≤ trend-path − k·σ while trend intact; target = trend path; stop/abort if regime break flag fires. Backtest on SAFE/TMON@ history first.
4. News gate — RSS/API fetcher, point-in-time gated; LLM verdict: "normal noise / skip / reduce size."
5. LLM decision loop — periodic review of regime brief + recent digest + news → structured {action, entry, stop, size, confidence, rationale}.


both horizons, no news (yet), DeepSeek via OpenAI-compatible API, standalone regime.py first. Here's the plan.
Plan: regime.py — history-driven regime analysis (Phase 1)
A standalone experiment module in the same style as llm_playground.py (tabs, rich, dotenv, T-Invest SDK, no pandas). It answers: does history actually show a "steady uptrend + recoverable dips" regime, and can an LLM characterize it?
Deliverables / files
- regime.py — four capabilities, CLI: poetry run python regime.py [--tickers SAFE TMON@ TGLD@ TPAY ...] [--years 5] [--k 2] [--llm]
1. History fetch: ~5y of daily candles (DAY interval, ≈1250 bars) per instrument.
2. Deterministic regime digest (no LLM): trend fit (log-linear slope + R², SMA200), drawdown analysis (rollback series → dip depths by percentile, dip durations, time-to-recover distribution, worst dip that still recovered), current z-score of price vs trend path, ATR14/vol20, vol clusters, avg liquidity. Output = compact "regime card" + raw JSON.
3. Dip-recovery backtest: simulate "buy when price drops k·σ below trend path while trend intact; exit at trend path or after N days" → per-ticker stats (n trades, win rate, avg hold, avg gain/loss, max adverse excursion). This is the number test of your SAFE hypothesis.
4. --llm mode — DeepSeek regime brief: post the digest to your OpenAI-compatible endpoint; ask for a structured JSON verdict (pattern type, dip params, risks, "buy the current dip?" with confidence). Rendered + saved. This validates LLM judgment on history without the LLM touching raw prices.
- .env additions: DEEPSEEK_API_KEY, DEEPSEEK_BASE_URL (default https://api.deepseek.com), DEEPSEEK_MODEL (default deepseek-v4-flash).
- pyproject.toml: add openai (canonical OpenAI-compatible client). If you'd rather not add a dependency, I'll use the async HTTP lib already in the venv — say the word.
Acceptance criteria (Phase 1)
- regime.py --tickers SAFE TMON@ TGLD@ prints a regime card each; backtest shows whether SAFE's dips provably recovered historically (win rate + hold times).
- --llm returns a well-formed JSON regime verdict that renders cleanly.
Phase 2 (only after validation, not now)
- Feed regime params into t_order_manager.InstrumentMonitor (replace fixed ±0.15% band with per-instrument σ/ATR-derived band; add a regime-break override that cancels the bracket if price breaks the historical-worst-recoverable dip or the trend flips) — this is the "both" layer (regime-aware intraday bracket + swing entries).
- Optional daily regime refresh via the scheduler.
Notes: news gate deferred as agreed; point-in-time gating will matter when we add it. The DeepSeek base URL/model name are configurable via env — if deepseek-v4-flash isn't exactly your endpoint model name, just set DEEPSEEK_MODEL.
