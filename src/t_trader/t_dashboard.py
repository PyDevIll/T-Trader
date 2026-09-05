# t_dashboard.py
# Static live dashboard for the order monitor.
# Renders a fixed-size cell per monitored ticker (3 per row) plus a
# portfolio footer (balance + opened positions) using rich.Layout.

from datetime import datetime
from decimal import Decimal

from rich.console import Console, Group
from rich.layout import Layout
from rich.panel import Panel
from rich.table import Table
from rich.text import Text


CELL_HEIGHT = 6
CELLS_PER_ROW = 3


def _fmt(d):
	"""Format a Decimal price; None renders as a dim dash."""
	if d is None:
		return "—"
	return f"{d:,.2f}".replace(",", " ")


def build_cell(ticker, period, ma, sell_limit, buy_limit, bid, ask, border_style="grey53"):
	t = Text()
	t.append("sell limit: ", style="dim")
	t.append(_fmt(sell_limit), style="green" if sell_limit is not None else "dim")
	t.append("\n")
	t.append(f"MA({period}): ", style="dim")
	t.append(_fmt(ma), style="bold cyan")
	t.append("\n")
	t.append("buy limit: ", style="dim")
	t.append(_fmt(buy_limit), style="blue" if buy_limit is not None else "dim")
	t.append("\n")
	t.append("BID: ", style="dim")
	t.append(_fmt(bid), style="bold white")
	t.append("   ASK: ", style="dim")
	t.append(_fmt(ask), style="bold white")
	return Panel(t, title=ticker, title_align="left", border_style=border_style,
				 height=CELL_HEIGHT, padding=(0, 1))


def build_grid(instruments):
	"""instruments: iterable of objects with figi/period/ma/sell_limit/buy_limit/bid/ask."""
	cells = []
	for i in instruments:
		cells.append(build_cell(
			ticker=i.ticker, period=i.period, ma=i.ma,
			sell_limit=i.sell_limit, buy_limit=i.buy_limit,
			bid=i.bid, ask=i.ask,
		))
	if not cells:
		return Panel(Text("No instruments. Type: add TICKER", justify="center"), height=CELL_HEIGHT)

	while len(cells) % CELLS_PER_ROW:
		cells.append(Panel(Text(""), height=CELL_HEIGHT, border_style="grey23"))

	row_count = len(cells) // CELLS_PER_ROW
	grid = Layout(size=row_count * CELL_HEIGHT)
	if len(cells) <= CELLS_PER_ROW:
		grid.split_row(*(Layout(renderable=c) for c in cells))
	else:
		rows = [cells[i:i + CELLS_PER_ROW] for i in range(0, len(cells), CELLS_PER_ROW)]
		row_layouts = []
		for row in rows:
			row_layout = Layout()
			row_layout.split_row(*(Layout(renderable=c) for c in row))
			row_layouts.append(Layout(renderable=row_layout, size=CELL_HEIGHT))
		grid.split_column(*row_layouts)
	return grid


def _fmt_signed(d):
	if d is None:
		return "—"
	return f"{d:+,.2f}".replace(",", " ")


MAX_OPS_ROWS = 6


def build_operation_list(operations):
	table = Table.grid(expand=True, padding=(0, 1))
	table.add_column("Date", no_wrap=True)
	table.add_column("Ticker", style="bold", no_wrap=True)
	table.add_column("Side", no_wrap=True)
	table.add_column("Price", justify="right", no_wrap=True)
	table.add_column("Sum", justify="right", no_wrap=True)

	if operations:
		table.add_row("Date", "Ticker", "Side", "Price", "Sum")
		for op in operations[:MAX_OPS_ROWS]:
			side = Text(op["side"], style={
				"BUY": "green", "SELL": "red", "PAYIN": "cyan", "PAYOUT": "cyan",
			}.get(op["side"], "dim"))
			sum_text = Text(_fmt_signed(op["sum"]), style=("green" if op["sum"] >= 0 else "red"))
			table.add_row(
				op["date"], op["ticker"] or "—", side, _fmt(op["price"]), sum_text,
			)
	else:
		table.add_row(Text("No recent operations", style="dim"), "", "", "", "")

	rows = min(len(operations), MAX_OPS_ROWS)
	height = rows + 3 if operations else 3
	return Panel(table, title="Operations", border_style="grey53", height=height)

def build_portfolio(balance, positions):
	"""balance: Decimal; positions: list of dicts with ticker/lots/avg/current/profit."""
	table = Table.grid(expand=True, padding=(0, 1))
	table.add_column("Ticker", style="bold", no_wrap=True)
	table.add_column("Lots", justify="right", no_wrap=True)
	table.add_column("Avg", justify="right", no_wrap=True)
	table.add_column("Now", justify="right", no_wrap=True)
	table.add_column("P/L", justify="right", no_wrap=True)

	if positions:
		table.add_row("Ticker", "Lots", "Avg", "Now", "P/L")
		for p in positions:
			profit_text = Text(f"{p['profit']:+.2f}", style=("green" if p["profit"] >= 0 else "red"))
			table.add_row(
				p["ticker"], _fmt(p["lots"]), _fmt(p["avg"]), _fmt(p["current"]), profit_text,
			)
	else:
		table.add_row(Text("No open positions", style="dim"), "", "", "", "")

	balance_line = Text()
	balance_line.append("Balance: ", style="bold")
	balance_line.append(_fmt(balance) + " RUB", style="bold green")

	return Panel(Group(balance_line, table), title="Portfolio", border_style="cyan", height=7)


def build_header(n_instruments):
	header = Text()
	header.append("T-Trader • regime-adaptive MA monitor", style="bold")
	header.append(f"   {n_instruments} instruments   ", style="dim")
	header.append(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), style="dim")
	return header


DEFAULT_HINT = "Commands: add TICKER | b TICKER | s TICKER | r TICKER | + AMOUNT | q | h"


def build_dashboard(instruments, balance, positions, operations, status=None, prompt=""):
	row_count = max(1, (len(instruments) + 2) // 3)
	ops_rows = min(len(operations), MAX_OPS_ROWS)
	ops_height = (ops_rows + 3) if operations else 3
	root = Layout()
	root.split_column(
		Layout(name="header", size=1),
		Layout(name="grid", size=row_count * CELL_HEIGHT),
		Layout(name="op_list", size=ops_height),
		Layout(name="portfolio", size=7),
		Layout(name="status", size=1),
		Layout(name="command", size=1),
	)
	root["header"].update(build_header(len(instruments)))
	root["grid"].update(build_grid(instruments))
	root["op_list"].update(build_operation_list(operations))
	root["portfolio"].update(build_portfolio(balance, positions))
	root["status"].update(build_status(status))
	root["command"].update(build_command_bar(prompt))
	return root


def build_status(status):
	line = Text()
	if status:
		line.append(status, style="yellow")
	else:
		line.append(DEFAULT_HINT, style="dim")
	return line


def build_command_bar(prompt):
	bar = Text()
	bar.append("> ", style="bold green")
	bar.append(prompt if prompt else " ", style="bold white")
	return bar


# ------------------------------------------------ mockup runner

class _MockInstrument:
	def __init__(self, ticker, ma, sell_limit, buy_limit, bid, ask):
		self.ticker = ticker
		self.period = 6
		self.ma = ma
		self.sell_limit = sell_limit
		self.buy_limit = buy_limit
		self.bid = bid
		self.ask = ask


if __name__ == "__main__":
	from rich.console import Console
	from rich.live import Live
	import time

	mock_instruments = [
		_MockInstrument("SBER", Decimal("294.52"), Decimal("298.53"), Decimal("290.41"), Decimal("294.60"), Decimal("294.75")),
		_MockInstrument("SAFE", Decimal("18.03"), None, Decimal("17.98"), Decimal("18.04"), Decimal("18.05")),
		_MockInstrument("TMON@", Decimal("161.41"), Decimal("161.90"), Decimal("160.91"), Decimal("161.44"), Decimal("161.46")),
		_MockInstrument("TGLD@", Decimal("412.30"), Decimal("414.40"), Decimal("410.20"), Decimal("412.28"), Decimal("412.35")),
	]
	mock_positions = [
		{"ticker": "SAFE", "lots": 10, "avg": Decimal("17.80"), "current": Decimal("18.04"), "profit": Decimal("0.24")},
		{"ticker": "TMON@", "lots": 2, "avg": Decimal("161.20"), "current": Decimal("161.44"), "profit": Decimal("-0.15")},
	]
	mock_operations = [
		{"date": "09-01 22:00", "ticker": "MRKS", "side": "SELL", "price": Decimal("0.40"), "sum": Decimal("396.00")},
		{"date": "09-01 21:31", "ticker": "MRKS", "side": "BUY", "price": Decimal("0.39"), "sum": Decimal("-389.00")},
		{"date": "09-01 21:00", "ticker": "", "side": "PAYIN", "price": None, "sum": Decimal("50.00")},
	]

	# one-shot render for a plain-text preview
	preview_console = Console(no_color=True, width=100)
	preview_console.print(build_dashboard(mock_instruments, Decimal("45830.12"), mock_positions,
										  mock_operations, status="", prompt=""))

	# interactive live preview that jitters the numbers
	console = Console()
	try:
		with Live(build_dashboard(mock_instruments, Decimal("45830.12"), mock_positions,
								  mock_operations, status="", prompt=""),
				  console=console, refresh_per_second=2, screen=True) as live:
			tick = 0
			while True:
				time.sleep(1)
				tick += 1
				for inst in mock_instruments:
					if inst.bid is not None:
						drift = Decimal("0.02") * (tick % 3)
						inst.bid = inst.bid + drift
						inst.ask = inst.ask + drift
				live.update(build_dashboard(mock_instruments, Decimal("45830.12"), mock_positions,
											mock_operations, status=f"tick {tick}", prompt=""))
	except KeyboardInterrupt:
		pass
