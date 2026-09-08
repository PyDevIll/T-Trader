# regime.py
"""
Regime analysis: does market history show a stable, exploitable pattern
(e.g. "steady uptrend with recoverable dips") for each instrument?

For every ticker we build a deterministic "regime digest" from years of
daily candles (trend fit, drawdown/dip statistics, volatility, residual
z-scores), run a dip-recovery pattern test, and optionally ask an LLM
(DeepSeek, OpenAI-compatible) to write a structured regime verdict.

Run (from project root):
    poetry run python regime.py                          # SAFE TMON@ TGLD@ TPAY
    poetry run python regime.py --tickers SBER SAFE      # custom list
    poetry run python regime.py --llm                    # + DeepSeek regime briefs
    poetry run python regime.py --k 2.5 --years 5        # tune dip threshold
"""

import argparse
import asyncio
import json
import math
import os
import statistics
from datetime import timedelta
from decimal import Decimal, ROUND_HALF_UP

from dotenv import load_dotenv
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from t_tech.invest.sandbox.async_client import AsyncSandboxClient
from t_tech.invest.utils import now, candle_interval_to_timedelta, quotation_to_decimal
from t_tech.invest import CandleInterval, InstrumentIdType, InstrumentType
from t_tech.invest.exceptions import AioRequestError

console = Console()


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def dec(q):
	return quotation_to_decimal(q)


def rnd(x, n=2):
	if x is None:
		return None
	return Decimal(str(x)).quantize(Decimal("1e-%d" % n), rounding=ROUND_HALF_UP)


def pct_change(old, new):
	if old in (None, 0):
		return None
	return (Decimal(str(new)) - Decimal(str(old))) / Decimal(str(old)) * 100


def load_token():
	for key in ("T_INVEST_TOKEN_SANDBOX", "SANDBOX_TOKEN", "T_INVEST_TOKEN"):
		val = os.environ.get(key)
		if val:
			return val
	path = os.path.join(os.path.dirname(__file__), ".env")
	if os.path.exists(path):
		for line in open(path):
			line = line.strip()
			if not line or line.startswith("#"):
				continue
			if "=" in line:
				k, _, v = line.partition("=")
				if k in ("T_INVEST_TOKEN_SANDBOX", "SANDBOX_TOKEN", "T_INVEST_TOKEN"):
					return v.strip()
			elif line.startswith("t."):
				return line
	return None


# ---------------------------------------------------------------------------
# T-Invest data
# ---------------------------------------------------------------------------

async def resolve_instrument(client, ticker):
	try:
		found = await client.instruments.find_instrument(
			query=ticker, api_trade_available_flag=True
		)
	except AioRequestError as e:
		console.print(f"[red]find_instrument failed for {ticker}: {e.metadata.message}[/]")
		return None

	candidates = [i for i in found.instruments if i.ticker.upper() == ticker.upper()]
	if not candidates:
		console.print(f"[yellow]No exact ticker match for {ticker}; using first result[/]")
		candidates = found.instruments
	if not candidates:
		return None

	short = candidates[0]
	info = {
		"ticker": short.ticker,
		"name": short.name,
		"uid": short.uid,
		"figi": short.figi,
		"lot": short.lot,
		"instrument_type": short.instrument_type,
	}
	try:
		if short.instrument_kind == InstrumentType.INSTRUMENT_TYPE_SHARE:
			inst = (await client.instruments.share_by(
				id=short.uid, id_type=InstrumentIdType.INSTRUMENT_ID_TYPE_UID
			)).instrument
		elif short.instrument_kind == InstrumentType.INSTRUMENT_TYPE_ETF:
			inst = (await client.instruments.etf_by(
				id=short.uid, id_type=InstrumentIdType.INSTRUMENT_ID_TYPE_UID
			)).instrument
		else:
			inst = (await client.instruments.get_instrument_by(
				id=short.uid, id_type=InstrumentIdType.INSTRUMENT_ID_TYPE_UID
			)).instrument
		for attr in ("currency", "exchange", "sector", "min_price_increment",
					 "liquidity_flag", "api_trade_available_flag"):
			if hasattr(inst, attr):
				val = getattr(inst, attr)
				if attr == "min_price_increment":
					info[attr] = dec(val) if val else None
				else:
					info[attr] = val.name if hasattr(val, "name") else val
	except AioRequestError as e:
		console.print(f"[yellow]full card failed for {ticker}: {e.metadata.message}[/]")
	return info


async def fetch_daily_history(client, uid, years):
	count = min(int(years * 252) + 30, 2400)
	from_ = now() - timedelta(days=int(years * 366))
	resp = await client.market_data.get_candles(
		instrument_id=uid,
		from_=from_,
		to=now() + timedelta(minutes=1),
		interval=CandleInterval.CANDLE_INTERVAL_DAY,
		limit=count,
	)
	candles = resp.candles
	rows = [{
		"t": c.time,
		"o": dec(c.open), "h": dec(c.high), "l": dec(c.low), "c": dec(c.close),
		"v": int(c.volume),
	} for c in candles]
	return rows


# ---------------------------------------------------------------------------
# regime features (deterministic, trailing-window only => no lookahead)
# ---------------------------------------------------------------------------

def _trailing_fit(logc, window):
	n = len(logc)
	paths = [None] * n
	sigmas = [None] * n
	zs = [None] * n
	r2_last = None
	slope_last = None
	for i in range(window - 1, n):
		win = logc[i - window + 1:i + 1]
		xs = list(range(window))
		mx = (window - 1) / 2.0
		my = sum(win) / window
		sxx = sum((x - mx) ** 2 for x in xs)
		sxy = sum((x - mx) * (y - my) for x, y in zip(xs, win))
		slope = sxy / sxx
		intercept = my - slope * mx
		path_i = slope * (window - 1) + intercept
		res = [win[j] - (slope * xs[j] + intercept) for j in range(window)]
		sigma = statistics.pstdev(res)
		paths[i] = math.exp(path_i)
		sigmas[i] = sigma
		zs[i] = (win[-1] - path_i) / sigma if sigma > 0 else 0.0
		if i == n - 1:
			slope_last = slope
			ss_res = sum(r ** 2 for r in res)
			ss_tot = sum((y - my) ** 2 for y in win)
			r2_last = 1.0 - ss_res / ss_tot if ss_tot > 0 else 0.0
	return paths, sigmas, zs, slope_last, r2_last


def _extract_dips(closes, threshold=-1.0, exit_thresh=-0.2):
	cummax = -1.0
	dd = []
	for c in closes:
		f = float(c)
		if f > cummax:
			cummax = f
		dd.append((f / cummax - 1.0) * 100 if cummax > 0 else 0.0)

	dips = []
	start = None
	cur_min = 0.0
	for i, d in enumerate(dd):
		if start is None:
			if d < threshold:
				start = i
				cur_min = d
		else:
			cur_min = min(cur_min, d)
			if d >= exit_thresh:
				dips.append({
					"start": start, "end": i, "depth": cur_min,
					"bars": i - start, "recovered": True,
				})
				start = None
				cur_min = 0.0
	if start is not None:
		dips.append({
			"start": start, "end": len(dd) - 1, "depth": cur_min,
			"bars": len(dd) - 1 - start, "recovered": False,
		})
	return dips


def _vols(closes, period=20):
	n = len(closes)
	out = []
	for i in range(period, n):
		rets = [(float(closes[j]) / float(closes[j - 1]) - 1.0) for j in range(i - period + 1, i + 1)]
		out.append(statistics.pstdev(rets) * 100.0)
	return out


def _sma(closes, period):
	if len(closes) < period:
		return None
	return sum(closes[-period:]) / period


def compute_digest(info, rows, trend_window=120):
	n = len(rows)
	closes = [r["c"] for r in rows]
	highs = [r["h"] for r in rows]
	lows = [r["l"] for r in rows]
	volumes = [r["v"] for r in rows]
	logc = [math.log(float(c)) for c in closes]

	paths, sigmas, zs, slope_last, r2_last = _trailing_fit(logc, trend_window)

	trs = []
	for i in range(1, n):
		trs.append(max(
			float(highs[i] - lows[i]),
			abs(float(highs[i]) - float(closes[i - 1])),
			abs(float(lows[i]) - float(closes[i - 1])),
		))
	atr14 = sum(trs[-14:]) / min(14, len(trs)) if trs else 0.0

	vols = _vols(closes)
	vol20 = vols[-1] if vols else None

	valid_zs = [z for z in zs if z is not None]
	zp = lambda q: (sorted(valid_zs)[int((len(valid_zs) - 1) * q)] if valid_zs else None)

	dips = _extract_dips(closes)
	recovered = [d for d in dips if d["recovered"]]
	ongoing = [d for d in dips if not d["recovered"]]
	depths = sorted(d["depth"] for d in dips)
	rec_times = [d["bars"] for d in recovered]

	running = -1.0
	worst_dd = 0.0
	for c in closes:
		f = float(c)
		running = max(running, f)
		worst_dd = min(worst_dd, (f / running - 1.0) * 100)

	digest = {
		"ticker": info["ticker"],
		"name": info["name"],
		"n": n,
		"first": rows[0]["t"].isoformat()[:10],
		"last": rows[-1]["t"].isoformat()[:10],
		"trend": {
			"slope_annual_pct": round(slope_last * 252 * 100, 2) if slope_last else None,
			"r2": round(r2_last, 3) if r2_last is not None else None,
			"sma200": float(_sma(closes, 200)) if _sma(closes, 200) else None,
		},
		"drawdown": {
			"max_dd_pct": round(worst_dd, 2),
			"n_dips": len(dips),
			"n_recovered": len(recovered),
			"dip_depth": {
				"median": round(statistics.median(depths), 2) if depths else None,
				"worst": round(depths[0], 2) if depths else None,
				"worst_recovered": round(min(d["depth"] for d in recovered), 2) if recovered else None,
				"ongoing": [round(d["depth"], 2) for d in ongoing],
			},
			"recovery": {
				"median_bars": int(statistics.median(rec_times)) if rec_times else None,
				"max_bars": max(rec_times) if rec_times else None,
				"p90_bars": int(sorted(rec_times)[int(len(rec_times) * 0.9)]) if rec_times else None,
			},
		},
		"volatility": {
			"atr14": round(atr14, 4),
			"vol20_pct": round(vol20, 2) if vol20 else None,
			"vol_p50": round(statistics.median(vols), 2) if vols else None,
			"vol_p90": round(sorted(vols)[int(len(vols) * 0.9)], 2) if vols else None,
		},
		"residual_z": {
			"now": round(zs[-1], 2) if zs[-1] is not None else None,
			"p1": round(zp(0.01), 2), "p5": round(zp(0.05), 2),
			"p50": round(zp(0.50), 2), "p95": round(zp(0.95), 2),
			"n_bars_lt_neg2": sum(1 for z in valid_zs if z <= -2.0),
		},
		"liquidity": {
			"avg_vol20": int(sum(volumes[-20:]) / min(20, len(volumes))) if volumes else None,
		},
	}
	return digest, closes, paths, zs


# ---------------------------------------------------------------------------
# pattern test: buy dips below k*sigma of the trailing trend path
# ---------------------------------------------------------------------------

def backtest_dip(times, closes, paths, zs, k, max_hold, trend_window):
	n = len(closes)
	trades = []
	pos = None
	for i in range(trend_window - 1, n):
		z = zs[i]
		if pos is None:
			if z is not None and z <= -k:
				pos = {
					"entry": i, "entry_z": z,
					"min_c": float(closes[i]), "max_c": float(closes[i]),
				}
			continue
		pos["min_c"] = min(pos["min_c"], float(closes[i]))
		pos["max_c"] = max(pos["max_c"], float(closes[i]))
		reason = None
		if z >= 0:
			reason = "REVERT"
		elif z <= pos["entry_z"] - 1.0:
			reason = "STOP"
		elif i - pos["entry"] >= max_hold:
			reason = "MAX_HOLD"
		if reason:
			entry_px = float(closes[pos["entry"]])
			exit_px = float(closes[i])
			trades.append({
				"entry_date": _date(times, pos["entry"]),
				"exit_date": _date(times, i),
				"entry": round(entry_px, 4),
				"exit": round(exit_px, 4),
				"ret_pct": round((exit_px / entry_px - 1) * 100, 2) if entry_px else None,
				"hold": i - pos["entry"],
				"mfe_pct": round((pos["max_c"] / entry_px - 1) * 100, 2),
				"mae_pct": round((pos["min_c"] / entry_px - 1) * 100, 2),
				"reason": reason,
			})
			pos = None
	if pos is not None:
		entry_px = float(closes[pos["entry"]])
		exit_px = float(closes[-1])
		trades.append({
			"entry_date": _date(times, pos["entry"]),
			"exit_date": _date(times, n - 1),
			"entry": round(entry_px, 4),
			"exit": round(exit_px, 4),
			"ret_pct": round((exit_px / entry_px - 1) * 100, 2) if entry_px else None,
			"hold": n - 1 - pos["entry"],
			"mfe_pct": round((pos["max_c"] / entry_px - 1) * 100, 2),
			"mae_pct": round((pos["min_c"] / entry_px - 1) * 100, 2),
			"reason": "OPEN",
		})
	return trades


def _date(items, idx):
	return items[idx].isoformat()[:10] if hasattr(items[idx], "isoformat") else str(idx)


def _trade_stats(trades):
	if not trades:
		return None
	rets = [t["ret_pct"] for t in trades if t["ret_pct"] is not None]
	wins = [r for r in rets if r > 0]
	return {
		"n": len(trades),
		"wins": len(wins),
		"win_rate": round(len(wins) / len(rets) * 100, 1) if rets else 0.0,
		"avg_ret": round(sum(rets) / len(rets), 2) if rets else 0.0,
		"median_ret": round(statistics.median(rets), 2) if rets else 0.0,
		"total_ret": round(sum(rets), 2),
		"avg_hold": round(statistics.mean([t["hold"] for t in trades]), 1),
		"avg_mfe": round(statistics.mean([t["mfe_pct"] for t in trades]), 2),
		"avg_mae": round(statistics.mean([t["mae_pct"] for t in trades]), 2),
		"best": max(rets) if rets else 0.0,
		"worst": min(rets) if rets else 0.0,
	}


# ---------------------------------------------------------------------------
# rendering
# ---------------------------------------------------------------------------

def regime_card(digest) -> str:
	d = digest
	t, dd, v, z = d["trend"], d["drawdown"], d["volatility"], d["residual_z"]
	ongoing = ", ".join(f"{x}%" for x in dd["dip_depth"]["ongoing"]) or "none"
	lines = []
	lines.append(f"## {d['ticker']} — {d['name']}  ({d['first']} .. {d['last']}, {d['n']} daily bars)")
	lines.append(
		f"trend: slope={t['slope_annual_pct']}%/yr  r2={t['r2']}  sma200={rnd(t['sma200'])}"
	)
	lines.append(
		f"drawdown: max_dd={dd['max_dd_pct']}%  dips={dd['n_dips']} "
		f"(recovered {dd['n_recovered']})  depth median={dd['dip_depth']['median']}% "
		f"worst={dd['dip_depth']['worst']}%  worst_recovered={dd['dip_depth']['worst_recovered']}%"
	)
	lines.append(
		f"recovery: median={dd['recovery']['median_bars']}d  "
		f"p90={dd['recovery']['p90_bars']}d  max={dd['recovery']['max_bars']}d"
	)
	lines.append(
		f"vol: atr14={v['atr14']}  vol20={v['vol20_pct']}%  "
		f"(p50={v['vol_p50']}% p90={v['vol_p90']}%)  avg_vol20={d['liquidity']['avg_vol20']:,}"
	)
	lines.append(
		f"residual_z: now={z['now']}  p1={z['p1']} p5={z['p5']} p50={z['p50']} "
		f"p95={z['p95']}  bars_z_le_-2={z['n_bars_lt_neg2']}"
	)
	lines.append(f"ongoing_dips: {ongoing}")
	return "\n".join(lines)


def print_trade_table(ticker, trades):
	table = Table(title=f"Per-trade pattern test: {ticker}", show_lines=False)
	for col in ("entry_date", "exit_date", "entry", "exit", "ret%", "hold_d", "mfe%", "mae%", "exit"):
		table.add_column(col)
	for t in trades:
		table.add_row(
			t["entry_date"], t["exit_date"], str(rnd(t["entry"])),
			str(rnd(t["exit"])), f"{t['ret_pct']:+.2f}", str(t["hold"]),
			f"{t['mfe_pct']:+.2f}", f"{t['mae_pct']:+.2f}", t["reason"],
		)
	console.print(table)


# ---------------------------------------------------------------------------
# persistence: per-trade and per-ticker stats ledger
# ---------------------------------------------------------------------------

def _write_csv(path, header, rows):
	exists = os.path.exists(path)
	with open(path, "a") as f:
		if not exists:
			f.write(",".join(header) + "\n")
		for r in rows:
			f.write(",".join(str(r.get(h, "")) for h in header) + "\n")


def persist(run_id, ticker, trades, stats, trade_path, stats_path):
	trade_rows = []
	for t in trades:
		row = {"run": run_id, "ticker": ticker}
		row.update(t)
		trade_rows.append(row)
	_write_csv(trade_path, ["run", "ticker", "entry_date", "exit_date", "entry",
							"exit", "ret_pct", "hold", "mfe_pct", "mae_pct", "reason"], trade_rows)
	if stats:
		s = {"run": run_id, "ticker": ticker}
		s.update(stats)
		_write_csv(stats_path, ["run", "ticker", "n", "wins", "win_rate", "avg_ret",
							   "median_ret", "total_ret", "avg_hold", "avg_mfe", "avg_mae",
							   "best", "worst"], [s])


# ---------------------------------------------------------------------------
# LLM regime brief (DeepSeek, OpenAI-compatible)
# ---------------------------------------------------------------------------

LLM_SYSTEM = (
	"You are a quantitative regime analyst. You receive a compact statistical digest "
	"of a financial instrument's market history and must characterize the underlying "
	"regime: whether price shows a persistent trend, whether drawdowns ('dips') are "
	"recoverable and mean-reverting, how to trade it, and the risks. "
	"Respond with strict JSON only, matching the requested schema. "
	"Never invent numbers that are not in the digest. Be concrete and sceptical."
)

LLM_SCHEMA = {
	"pattern": "one of: steady_uptrend_with_recoverable_dips, uptrend_with_break_risk, mean_reverting_flat, downtrend, other",
	"trend": {"slope_annual_pct": 0, "strength_0_1": 0},
	"dip_behavior": {"median_depth_pct": 0, "worst_recoverable_pct": 0, "median_recovery_days": 0},
	"current": {"zscore": 0, "in_dip": "bool", "verdict": "BUY_DIP|WAIT|AVOID|NO_POSITION"},
	"entry_plan": {"trigger": "e.g. price k*sigma below trend path", "stop_rule": "...", "target": "trend path"},
	"risks": ["..."],
	"rationale": "...",
}


async def llm_regime_brief(digest_card, ticker):
	from openai import AsyncOpenAI

	api_key = os.environ.get("DEEPSEEK_API_KEY")
	base_url = os.environ.get("DEEPSEEK_BASE_URL", "https://api.deepseek.com")
	model = os.environ.get("DEEPSEEK_MODEL", "deepseek-v4-flash")
	if not api_key:
		console.print("[red]DEEPSEEK_API_KEY not set[/]")
		return None

	client = AsyncOpenAI(api_key=api_key, base_url=base_url)
	prompt = (
		"Analyze this instrument's regime digest and return JSON matching this schema:\n"
		f"{json.dumps(LLM_SCHEMA, ensure_ascii=False)}\n\n"
		"DIGEST:\n" + digest_card
	)
	try:
		resp = await asyncio.wait_for(client.chat.completions.create(
			model=model,
			messages=[
				{"role": "system", "content": LLM_SYSTEM},
				{"role": "user", "content": prompt},
			],
			response_format={"type": "json_object"},
			temperature=0.2,
		), timeout=60)
		content = resp.choices[0].message.content
		data = json.loads(content)
		console.print(Panel(
			json.dumps(data, ensure_ascii=False, indent=2),
			title=f"LLM regime brief: {ticker} ({model})",
			style="magenta",
		))
	except asyncio.TimeoutError:
		console.print("[red]LLM timeout[/]")
		return None
	return data

# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

async def main(tickers, years, k, max_hold, trend_window, do_llm, csv_dir):
	token = load_token()
	if not token:
		console.print("[red]No T-Invest token found in env or .env[/]")
		return

	os.makedirs(csv_dir, exist_ok=True)
	trade_path = os.path.join(csv_dir, "regime_trades.csv")
	stats_path = os.path.join(csv_dir, "regime_stats.csv")
	run_id = now().strftime("%Y%m%d-%H%M%S")

	async with AsyncSandboxClient(token) as client:
		for ticker in tickers:
			console.print(Panel(f"[bold]{ticker}[/]", style="cyan"))
			info = await resolve_instrument(client, ticker)
			if not info:
				continue
			try:
				rows = await fetch_daily_history(client, info["uid"], years)
			except AioRequestError as e:
				console.print(f"[red]history failed for {ticker}: {e.metadata.message}[/]")
				continue
			if len(rows) < trend_window + 20:
				console.print(f"[yellow]{ticker}: only {len(rows)} bars, skipping[/]")
				continue

			digest, closes, paths, zs = compute_digest(info, rows, trend_window)
			console.print(regime_card(digest))

			times = [r["t"] for r in rows]
			trades = backtest_dip(times, closes, paths, zs, k, max_hold, trend_window)
			stats = _trade_stats(trades)
			print_trade_table(ticker, trades)
			if stats:
				console.print(
					f"[bold]{ticker}[/] pattern test (k={k}, hold<={max_hold}d, "
					f"window={trend_window}d): n={stats['n']} wins={stats['wins']} "
					f"win_rate={stats['win_rate']}% avg_ret={stats['avg_ret']:+.2f}% "
					f"median_ret={stats['median_ret']:+.2f}% total={stats['total_ret']:+.2f}% "
					f"avg_hold={stats['avg_hold']}d best={stats['best']:+.2f}% worst={stats['worst']:+.2f}%"
				)
			persist(run_id, ticker, trades, stats, trade_path, stats_path)

			if do_llm:
				await llm_regime_brief(regime_card(digest), ticker)
				await asyncio.sleep(2)

	console.print(f"trade ledger: [bold]{trade_path}[/]  stats ledger: [bold]{stats_path}[/]")
	console.print("NOTE: pattern test is in-sample with trailing features only — a sanity "
				  "check of the hypothesis, not a validated backtest.")


if __name__ == "__main__":
	load_dotenv()
	parser = argparse.ArgumentParser(description="Regime analysis over market history")
	parser.add_argument("--tickers", nargs="+", default=["SAFE", "TMON@", "TGLD@", "TPAY"])
	parser.add_argument("--years", type=int, default=5)
	parser.add_argument("--k", type=float, default=2.0, help="dip entry at k*sigma below trend path")
	parser.add_argument("--max-hold", type=int, default=60, help="max days in a dip trade")
	parser.add_argument("--trend-window", type=int, default=120, help="trailing regression window (days)")
	parser.add_argument("--llm", action="store_true", help="ask DeepSeek for a regime brief per ticker")
	parser.add_argument("--csv-dir", default="regime_cache", help="directory for trade/stats ledgers")
	args = parser.parse_args()
	asyncio.run(main(args.tickers, args.years, args.k, args.max_hold,
					 args.trend_window, args.llm, args.csv_dir))
