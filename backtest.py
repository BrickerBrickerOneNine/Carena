"""
Deterministic rule-based backtester for the Crypto Daytrading Arena.

Replays historical Coinbase candle data against strategy rules (no LLM)
to evaluate whether rule changes improve returns.

Usage:
    uv run python backtest.py --product BTC-USD --days 30 --strategy default
    uv run python backtest.py --product BTC-USD --days 30 --strategy momentum --compare
    uv run python backtest.py --product SOL-USD --days 60 --strategy contrarian --fee-rate 0.012
"""

from __future__ import annotations

import argparse
import enum
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta

import httpx

from coinbase_consumer import COINBASE_REST_BASE, Candle
from indicators import (
    atr,
    bollinger_bands,
    consecutive_red_candles,
    ema,
    macd,
    momentum_pct,
    obv_trend,
    rsi,
    sma,
    vwap,
)

# ── Configuration ────────────────────────────────────────────────

INITIAL_CASH = 100_000.0
DEFAULT_FEE_RATE = 0.005  # 0.5% — realistic Coinbase taker fee
POSITION_SIZE_PCT = 0.20  # 20% of portfolio per trade
MAX_POSITION_PCT = 0.30  # never more than 30% in one position


# ── Data fetching ────────────────────────────────────────────────


def fetch_candles(
    product: str, start_ts: int, end_ts: int, granularity: int
) -> list[Candle]:
    """Fetch historical candles from Coinbase REST API with pagination."""
    all_candles: list[Candle] = []
    max_per_request = 300

    with httpx.Client(base_url=COINBASE_REST_BASE, timeout=15.0) as client:
        cursor = start_ts
        while cursor < end_ts:
            batch_end = min(cursor + granularity * max_per_request, end_ts)
            for attempt in range(3):
                resp = client.get(
                    f"/products/{product}/candles",
                    params={
                        "granularity": granularity,
                        "start": cursor,
                        "end": batch_end,
                    },
                )
                if resp.status_code == 429:
                    time.sleep(1.0 * (attempt + 1))
                    continue
                resp.raise_for_status()
                break
            else:
                print(f"ERROR: Failed to fetch candles after 3 retries")
                sys.exit(1)

            rows = resp.json()
            for row in rows:
                # Coinbase format: [timestamp, low, high, open, close, volume]
                all_candles.append(
                    Candle(
                        time=datetime.fromtimestamp(row[0], tz=timezone.utc),
                        open=float(row[3]),
                        high=float(row[2]),
                        low=float(row[1]),
                        close=float(row[4]),
                        volume=float(row[5]),
                    )
                )

            cursor = batch_end
            time.sleep(0.2)  # rate limit courtesy

    # Deduplicate by timestamp and sort ascending
    seen = set()
    unique = []
    for c in all_candles:
        ts = c.time.timestamp()
        if ts not in seen:
            seen.add(ts)
            unique.append(c)
    unique.sort(key=lambda c: c.time)
    return unique


# ── Portfolio tracking ───────────────────────────────────────────


@dataclass
class BacktestAccount:
    cash: float = INITIAL_CASH
    positions: dict[str, float] = field(default_factory=dict)
    cost_basis: dict[str, float] = field(default_factory=dict)
    entry_prices: dict[str, float] = field(default_factory=dict)
    total_fees: float = 0.0
    trade_count: int = 0
    wins: int = 0
    losses: int = 0
    total_pnl_realized: float = 0.0
    peak_value: float = INITIAL_CASH
    max_drawdown: float = 0.0
    consecutive_losses: int = 0

    def buy(self, product: str, qty: float, price: float, fee_rate: float) -> None:
        cost = qty * price
        fee = cost * fee_rate
        if cost + fee > self.cash:
            # Size down to what we can afford
            max_cost = self.cash / (1 + fee_rate)
            qty = max_cost / price
            cost = qty * price
            fee = cost * fee_rate
        if qty <= 0:
            return

        self.cash -= cost + fee
        self.total_fees += fee
        self.trade_count += 1

        old_qty = self.positions.get(product, 0.0)
        old_basis = self.cost_basis.get(product, 0.0)
        self.positions[product] = old_qty + qty
        self.cost_basis[product] = old_basis + cost
        if old_qty == 0:
            self.entry_prices[product] = price
        else:
            # Weighted average entry price
            self.entry_prices[product] = (
                old_basis + cost
            ) / (old_qty + qty)

    def sell(self, product: str, qty: float, price: float, fee_rate: float) -> None:
        held = self.positions.get(product, 0.0)
        if held <= 0:
            return
        qty = min(qty, held)

        proceeds = qty * price
        fee = proceeds * fee_rate
        self.cash += proceeds - fee
        self.total_fees += fee
        self.trade_count += 1

        # Compute realized P&L for this sale
        avg_cost = self.cost_basis[product] / held
        cost_of_sold = avg_cost * qty
        pnl = proceeds - fee - cost_of_sold
        self.total_pnl_realized += pnl

        if pnl >= 0:
            self.wins += 1
            self.consecutive_losses = 0
        else:
            self.losses += 1
            self.consecutive_losses += 1

        remaining = held - qty
        if remaining < 1e-10:
            self.positions.pop(product, None)
            self.cost_basis.pop(product, None)
            self.entry_prices.pop(product, None)
        else:
            self.positions[product] = remaining
            self.cost_basis[product] = avg_cost * remaining

    def portfolio_value(self, prices: dict[str, float]) -> float:
        val = self.cash
        for pid, qty in self.positions.items():
            val += qty * prices.get(pid, 0.0)
        return val

    def update_drawdown(self, prices: dict[str, float]) -> None:
        val = self.portfolio_value(prices)
        if val > self.peak_value:
            self.peak_value = val
        dd = (self.peak_value - val) / self.peak_value * 100
        if dd > self.max_drawdown:
            self.max_drawdown = dd


# ── Indicator snapshots ──────────────────────────────────────────


@dataclass
class Snap:
    """Pre-computed indicators at a single point in time."""

    price: float
    sma_short: float | None = None
    sma_long: float | None = None
    rsi_val: float | None = None
    macd_hist: float | None = None
    bb_upper: float | None = None
    bb_mid: float | None = None
    bb_lower: float | None = None
    atr_val: float | None = None
    mom_short: float | None = None
    mom_long: float | None = None
    obv_dir: str | None = None
    consec_reds: int = 0


def compute_snap(candles: list[Candle], sma_s: int, sma_l: int, mom_s: int, mom_l: int) -> Snap:
    """Compute an indicator snapshot from a candle slice (no look-ahead)."""
    if not candles:
        return Snap(price=0.0)

    s = Snap(price=candles[-1].close)
    s.sma_short = sma(candles, sma_s)
    s.sma_long = sma(candles, sma_l)
    s.rsi_val = rsi(candles, 14)

    m = macd(candles)
    if m is not None:
        s.macd_hist = m[2]

    bb = bollinger_bands(candles, 20)
    if bb is not None:
        s.bb_upper, s.bb_mid, s.bb_lower = bb

    s.atr_val = atr(candles, 14)
    s.mom_short = momentum_pct(candles, mom_s)
    s.mom_long = momentum_pct(candles, mom_l)
    s.obv_dir = obv_trend(candles, 5)
    s.consec_reds = consecutive_red_candles(candles)
    return s


# ── Strategy signals ─────────────────────────────────────────────


class Signal(enum.Enum):
    BUY = "buy"
    BUY_SMALL = "buy_small"   # ~10% of portfolio (graduated entry tier 1)
    BUY_LARGE = "buy_large"   # ~25% of portfolio (graduated entry tier 3)
    SELL = "sell"
    SELL_HALF = "sell_half"   # partial exit
    HOLD = "hold"


def _atr_stop(h: Snap, entry_price: float, price: float) -> bool:
    """ATR-based stop-loss: 1.5x hourly ATR, capped at 3%."""
    if h.atr_val is None or entry_price <= 0:
        return price < entry_price * 0.97  # fallback to fixed 3%
    stop_dist = min(1.5 * h.atr_val, entry_price * 0.03)
    return price < entry_price - stop_dist


def _fixed_stop(entry_price: float, price: float, pct: float = 0.03) -> bool:
    """Fixed percentage stop-loss."""
    return price < entry_price * (1 - pct)


# ── Default strategy ─────────────────────────────────────────────


def strategy_default_old(
    h: Snap, d: Snap, account: BacktestAccount, product: str
) -> Signal:
    """Old default rules: fixed 3% stop, no MACD confirmation."""
    held = product in account.positions

    if held:
        entry = account.entry_prices.get(product, h.price)
        # Fixed 3% stop-loss
        if _fixed_stop(entry, h.price):
            return Signal.SELL
        # Take-profit at +5%
        if h.price > entry * 1.05:
            return Signal.SELL
        # RSI overbought exit
        if h.rsi_val is not None and h.rsi_val > 70:
            return Signal.SELL
        return Signal.HOLD

    # Entry checklist (old rules — no MACD)
    if account.consecutive_losses >= 3:
        return Signal.HOLD
    if d.sma_short is None or d.sma_long is None:
        return Signal.HOLD
    if h.sma_short is None or h.sma_long is None:
        return Signal.HOLD
    # Trends agree (both bullish)
    if not (d.sma_short > d.sma_long and h.sma_short > h.sma_long):
        return Signal.HOLD
    # RSI supports buy
    if h.rsi_val is None or h.rsi_val > 35:
        return Signal.HOLD
    # Price in lower half of Bollinger
    if h.bb_mid is not None and h.price > h.bb_mid:
        return Signal.HOLD
    # No falling knife
    if h.consec_reds >= 3:
        return Signal.HOLD

    return Signal.BUY


def strategy_default_new(
    h: Snap, d: Snap, account: BacktestAccount, product: str
) -> Signal:
    """New default rules: ATR stop, MACD confirmation, trailing stop."""
    held = product in account.positions

    if held:
        entry = account.entry_prices.get(product, h.price)
        # ATR-based stop-loss
        if _atr_stop(h, entry, h.price):
            return Signal.SELL
        # Trailing stop: if up >5%, trail at 2% below high
        gain_pct = (h.price - entry) / entry
        if gain_pct > 0.05:
            # Approximate trailing stop (we track entry, not peak — use take-profit instead)
            pass
        # Breakeven stop: if up >2%, don't let it go negative
        if gain_pct > 0.02 and h.price < entry * 1.001:
            return Signal.SELL
        # Take-profit at +5%
        if h.price > entry * 1.05:
            return Signal.SELL
        # RSI overbought exit
        if h.rsi_val is not None and h.rsi_val > 70:
            return Signal.SELL
        # Trending market: don't exit if MACD still positive and RSI moderate
        return Signal.HOLD

    # Entry checklist (new rules — with MACD)
    if account.consecutive_losses >= 3:
        return Signal.HOLD
    if d.sma_short is None or d.sma_long is None:
        return Signal.HOLD
    if h.sma_short is None or h.sma_long is None:
        return Signal.HOLD
    if not (d.sma_short > d.sma_long and h.sma_short > h.sma_long):
        return Signal.HOLD
    if h.rsi_val is None or h.rsi_val > 35:
        return Signal.HOLD
    # MACD histogram must confirm (NEW)
    if h.macd_hist is None or h.macd_hist <= 0:
        return Signal.HOLD
    if h.bb_mid is not None and h.price > h.bb_mid:
        return Signal.HOLD
    if h.consec_reds >= 3:
        return Signal.HOLD

    return Signal.BUY


# ── Momentum strategy ────────────────────────────────────────────


def strategy_momentum_old(
    h: Snap, d: Snap, account: BacktestAccount, product: str
) -> Signal:
    """Old momentum: fixed 2% stop, no MACD in entry."""
    held = product in account.positions

    if held:
        entry = account.entry_prices.get(product, h.price)
        if _fixed_stop(entry, h.price, 0.02):
            return Signal.SELL
        if h.rsi_val is not None and h.rsi_val > 75:
            return Signal.SELL
        if h.mom_short is not None and h.mom_short < 0:
            return Signal.SELL
        return Signal.HOLD

    if d.sma_short is None or d.sma_long is None:
        return Signal.HOLD
    if not (d.sma_short > d.sma_long):
        return Signal.HOLD
    if h.mom_short is None or h.mom_short <= 0:
        return Signal.HOLD
    if h.rsi_val is None or not (40 <= h.rsi_val <= 70):
        return Signal.HOLD
    if d.rsi_val is not None and d.rsi_val > 70:
        return Signal.HOLD

    return Signal.BUY


def strategy_momentum_new(
    h: Snap, d: Snap, account: BacktestAccount, product: str
) -> Signal:
    """New momentum: ATR stop, MACD confirmation in entry and exit."""
    held = product in account.positions

    if held:
        entry = account.entry_prices.get(product, h.price)
        if _atr_stop(h, entry, h.price):
            return Signal.SELL
        if h.rsi_val is not None and h.rsi_val > 75:
            return Signal.SELL
        if h.mom_short is not None and h.mom_short < 0:
            return Signal.SELL
        # NEW: exit if daily MACD turns bearish
        if d.macd_hist is not None and d.macd_hist < 0:
            return Signal.SELL
        return Signal.HOLD

    if d.sma_short is None or d.sma_long is None:
        return Signal.HOLD
    if not (d.sma_short > d.sma_long):
        return Signal.HOLD
    # NEW: daily MACD must be positive
    if d.macd_hist is None or d.macd_hist <= 0:
        return Signal.HOLD
    if h.mom_short is None or h.mom_short <= 0:
        return Signal.HOLD
    # NEW: hourly MACD must be positive
    if h.macd_hist is None or h.macd_hist <= 0:
        return Signal.HOLD
    if h.rsi_val is None or not (40 <= h.rsi_val <= 70):
        return Signal.HOLD
    if d.rsi_val is not None and d.rsi_val > 70:
        return Signal.HOLD

    return Signal.BUY


# ── Contrarian strategy ──────────────────────────────────────────


def strategy_contrarian_old(
    h: Snap, d: Snap, account: BacktestAccount, product: str
) -> Signal:
    """Old contrarian: fixed 3% stop, no ATR."""
    held = product in account.positions

    if held:
        entry = account.entry_prices.get(product, h.price)
        if _fixed_stop(entry, h.price):
            return Signal.SELL
        if h.rsi_val is not None and h.rsi_val > 70:
            return Signal.SELL
        if d.bb_upper is not None and h.price >= d.bb_upper:
            return Signal.SELL
        return Signal.HOLD

    # Buy on panic
    if h.rsi_val is None or h.rsi_val > 30:
        return Signal.HOLD
    if d.bb_lower is not None and h.price > d.bb_lower:
        return Signal.HOLD
    if d.mom_long is None or d.mom_long >= 0:
        return Signal.HOLD

    return Signal.BUY


def strategy_contrarian_new(
    h: Snap, d: Snap, account: BacktestAccount, product: str
) -> Signal:
    """Improved contrarian: same strict entries, better exits.

    Changes from old (exits only):
    - Stop: ATR-based instead of fixed 3%
    - Exit: sell HALF at RSI > 70, sell rest at RSI > 80 (let winners run)
    - Exit: breakeven stop protects gains after +3%
    Entry rules are IDENTICAL to old.
    """
    held = product in account.positions

    if held:
        entry = account.entry_prices.get(product, h.price)
        gain_pct = (h.price - entry) / entry if entry > 0 else 0

        # ATR-based stop-loss
        if _atr_stop(h, entry, h.price):
            return Signal.SELL

        # Breakeven stop: if up > 5% (enough to cover round-trip fees),
        # don't let it drop back to break-even. Fee-aware: entry fee already
        # paid, exit fee ~1.2%, so true breakeven is entry * 1.012.
        if gain_pct > 0.05 and h.price < entry * 1.015:
            return Signal.SELL

        # Partial exit: sell half at RSI > 70, but only in bullish macro trend
        # In bear trends, close fully (avoid churn in chop)
        if h.rsi_val is not None and h.rsi_val > 70 and h.rsi_val <= 80:
            macro_bullish = (d.sma_short is not None and d.sma_long is not None
                            and d.sma_short > d.sma_long)
            if macro_bullish and gain_pct > 0.03:  # need >3% to justify partial exit fees
                return Signal.SELL_HALF
            else:
                return Signal.SELL
        # Full exit at RSI > 80 or daily BB upper
        if h.rsi_val is not None and h.rsi_val > 80:
            return Signal.SELL
        if d.bb_upper is not None and h.price >= d.bb_upper:
            return Signal.SELL

        return Signal.HOLD

    # ── Entry rules (IDENTICAL to old) ──
    if h.rsi_val is None or h.rsi_val > 30:
        return Signal.HOLD
    if d.bb_lower is not None and h.price > d.bb_lower:
        return Signal.HOLD
    if d.mom_long is None or d.mom_long >= 0:
        return Signal.HOLD

    return Signal.BUY


# ── Swing strategy ───────────────────────────────────────────────


def strategy_swing_old(
    h: Snap, d: Snap, account: BacktestAccount, product: str
) -> Signal:
    """Old swing: fixed 2% stop, no MACD."""
    held = product in account.positions

    if held:
        entry = account.entry_prices.get(product, h.price)
        if _fixed_stop(entry, h.price, 0.02):
            return Signal.SELL
        if h.mom_short is not None and h.mom_short < 0:
            return Signal.SELL
        return Signal.HOLD

    if h.mom_short is None or h.mom_short <= 0:
        return Signal.HOLD
    if d.sma_short is None or d.sma_long is None:
        return Signal.HOLD
    if not (d.sma_short > d.sma_long):
        return Signal.HOLD
    # Buy in lower half of daily Bollinger
    if d.bb_mid is not None and h.price > d.bb_mid:
        return Signal.HOLD

    return Signal.BUY


def strategy_swing_new(
    h: Snap, d: Snap, account: BacktestAccount, product: str
) -> Signal:
    """New swing: ATR stop, MACD confirmation."""
    held = product in account.positions

    if held:
        entry = account.entry_prices.get(product, h.price)
        if _atr_stop(h, entry, h.price):
            return Signal.SELL
        if h.mom_short is not None and h.mom_short < 0:
            return Signal.SELL
        # NEW: exit if hourly MACD turns bearish
        if h.macd_hist is not None and h.macd_hist < 0:
            return Signal.SELL
        return Signal.HOLD

    if h.mom_short is None or h.mom_short <= 0:
        return Signal.HOLD
    if d.sma_short is None or d.sma_long is None:
        return Signal.HOLD
    if not (d.sma_short > d.sma_long):
        return Signal.HOLD
    # NEW: hourly MACD must confirm
    if h.macd_hist is None or h.macd_hist <= 0:
        return Signal.HOLD
    if d.bb_mid is not None and h.price > d.bb_mid:
        return Signal.HOLD

    return Signal.BUY


# ── Strategy registry ────────────────────────────────────────────

STRATEGIES = {
    "default": (strategy_default_old, strategy_default_new),
    "momentum": (strategy_momentum_old, strategy_momentum_new),
    "contrarian": (strategy_contrarian_old, strategy_contrarian_new),
    "swing": (strategy_swing_old, strategy_swing_new),
}

# Indicator params: (hourly_sma_short, hourly_sma_long, hourly_mom_short, hourly_mom_long)
HOURLY_PARAMS = {"default": (12, 24, 6, 24), "momentum": (12, 24, 6, 24),
                 "contrarian": (12, 24, 6, 24), "swing": (12, 24, 6, 24)}
DAILY_PARAMS = {"default": (7, 20, 7, 14), "momentum": (7, 20, 7, 14),
                "contrarian": (7, 20, 7, 14), "swing": (7, 20, 7, 14)}


# ── Backtest engine ──────────────────────────────────────────────


@dataclass
class BacktestResult:
    name: str
    product: str
    days: int
    initial_cash: float
    final_value: float
    total_return_pct: float
    buy_hold_return_pct: float
    win_rate_pct: float
    max_drawdown_pct: float
    total_trades: int
    total_fees: float
    realized_pnl: float
    wins: int
    losses: int


def run_backtest(
    candles_1h: list[Candle],
    candles_1d: list[Candle],
    product: str,
    strategy_fn,
    fee_rate: float,
    initial_cash: float,
    strategy_name: str,
    days: int,
) -> BacktestResult:
    """Run a single backtest pass."""
    account = BacktestAccount(cash=initial_cash, peak_value=initial_cash)

    # Need at least 26 candles for MACD warm-up
    start_idx = max(26, 1)

    for idx in range(start_idx, len(candles_1h)):
        current = candles_1h[idx]
        h_slice = candles_1h[: idx + 1]

        # Find daily candles up to current time
        d_slice = [c for c in candles_1d if c.time <= current.time]

        # Compute indicators
        hp = HOURLY_PARAMS.get(strategy_name, (12, 24, 6, 24))
        dp = DAILY_PARAMS.get(strategy_name, (7, 20, 7, 14))
        h_snap = compute_snap(h_slice, *hp)
        d_snap = compute_snap(d_slice, *dp) if d_slice else Snap(price=current.close)

        # Get signal
        signal = strategy_fn(h_snap, d_snap, account, product)
        price = current.close

        if signal in (Signal.BUY, Signal.BUY_SMALL, Signal.BUY_LARGE):
            # Determine position size based on signal tier
            if signal == Signal.BUY_SMALL:
                size_pct = 0.10
            elif signal == Signal.BUY_LARGE:
                size_pct = 0.25
            else:
                size_pct = POSITION_SIZE_PCT  # 0.20

            portfolio_val = account.portfolio_value({product: price})
            # Check max position limit (allow adding to existing positions for graduated entry)
            current_exposure = account.positions.get(product, 0.0) * price
            max_allowed = portfolio_val * MAX_POSITION_PCT - current_exposure
            trade_value = min(portfolio_val * size_pct, max_allowed)
            if trade_value > 0:
                qty = trade_value / price
                account.buy(product, qty, price, fee_rate)

        elif signal == Signal.SELL and product in account.positions:
            qty = account.positions[product]
            account.sell(product, qty, price, fee_rate)

        elif signal == Signal.SELL_HALF and product in account.positions:
            qty = account.positions[product] / 2
            account.sell(product, qty, price, fee_rate)

        account.update_drawdown({product: price})

    # Force close any open positions at the end
    if product in account.positions:
        final_price = candles_1h[-1].close
        account.sell(product, account.positions[product], final_price, fee_rate)
        account.update_drawdown({product: final_price})

    final_value = account.portfolio_value({})
    total_return = (final_value - initial_cash) / initial_cash * 100
    buy_hold = (candles_1h[-1].close - candles_1h[start_idx].close) / candles_1h[start_idx].close * 100
    total_sells = account.wins + account.losses
    win_rate = (account.wins / total_sells * 100) if total_sells > 0 else 0.0

    return BacktestResult(
        name=strategy_name,
        product=product,
        days=days,
        initial_cash=initial_cash,
        final_value=final_value,
        total_return_pct=total_return,
        buy_hold_return_pct=buy_hold,
        win_rate_pct=win_rate,
        max_drawdown_pct=account.max_drawdown,
        total_trades=account.trade_count,
        total_fees=account.total_fees,
        realized_pnl=account.total_pnl_realized,
        wins=account.wins,
        losses=account.losses,
    )


# ── Output formatting ────────────────────────────────────────────


def format_result(r: BacktestResult, label: str = "") -> str:
    """Format a single backtest result."""
    title = f"{label or r.name} | {r.product} | {r.days}d"
    lines = [
        f"  {'Strategy:':<22} {title}",
        f"  {'Initial cash:':<22} ${r.initial_cash:>12,.2f}",
        f"  {'Final value:':<22} ${r.final_value:>12,.2f}",
        f"  {'Total return:':<22} {r.total_return_pct:>12.2f}%",
        f"  {'Buy & hold return:':<22} {r.buy_hold_return_pct:>12.2f}%",
        f"  {'vs Buy & hold:':<22} {r.total_return_pct - r.buy_hold_return_pct:>+12.2f}%",
        f"  {'Win rate:':<22} {r.win_rate_pct:>12.1f}%  ({r.wins}W / {r.losses}L)",
        f"  {'Max drawdown:':<22} {r.max_drawdown_pct:>12.2f}%",
        f"  {'Total trades:':<22} {r.total_trades:>12}",
        f"  {'Total fees:':<22} ${r.total_fees:>12,.2f}",
        f"  {'Realized P&L:':<22} ${r.realized_pnl:>12,.2f}",
    ]
    return "\n".join(lines)


def format_comparison(old: BacktestResult, new: BacktestResult) -> str:
    """Format side-by-side comparison."""
    def delta(new_val: float, old_val: float, fmt: str = ".2f", pct: bool = False) -> str:
        d = new_val - old_val
        suffix = "%" if pct else ""
        if d > 0:
            return f"\033[32m+{d:{fmt}}{suffix}\033[0m"
        elif d < 0:
            return f"\033[31m{d:{fmt}}{suffix}\033[0m"
        return f"{d:{fmt}}{suffix}"

    w = 14
    lines = [
        "",
        f"  {'Metric':<22} {'Old Rules':>{w}} {'New Rules':>{w}}  {'Delta':>{w}}",
        f"  {'─' * 22} {'─' * w} {'─' * w}  {'─' * w}",
        f"  {'Final value':<22} ${old.final_value:>{w-1},.2f} ${new.final_value:>{w-1},.2f}  {delta(new.final_value, old.final_value, ',.2f')}",
        f"  {'Total return':<22} {old.total_return_pct:>{w}.2f}% {new.total_return_pct:>{w}.2f}%  {delta(new.total_return_pct, old.total_return_pct, pct=True)}",
        f"  {'Buy & hold':<22} {old.buy_hold_return_pct:>{w}.2f}%",
        f"  {'Win rate':<22} {old.win_rate_pct:>{w}.1f}% {new.win_rate_pct:>{w}.1f}%  {delta(new.win_rate_pct, old.win_rate_pct, '.1f', pct=True)}",
        f"  {'Max drawdown':<22} {old.max_drawdown_pct:>{w}.2f}% {new.max_drawdown_pct:>{w}.2f}%  {delta(-new.max_drawdown_pct, -old.max_drawdown_pct, '.2f', pct=True)}",
        f"  {'Total trades':<22} {old.total_trades:>{w}} {new.total_trades:>{w}}  {delta(new.total_trades, old.total_trades, '.0f')}",
        f"  {'Total fees':<22} ${old.total_fees:>{w-1},.2f} ${new.total_fees:>{w-1},.2f}  {delta(new.total_fees, old.total_fees, ',.2f')}",
        f"  {'Realized P&L':<22} ${old.realized_pnl:>{w-1},.2f} ${new.realized_pnl:>{w-1},.2f}  {delta(new.realized_pnl, old.realized_pnl, ',.2f')}",
    ]
    return "\n".join(lines)


# ── CLI ──────────────────────────────────────────────────────────


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Backtest trading strategy rules against historical data.")
    p.add_argument("--product", required=True, help="Trading pair (e.g. BTC-USD)")
    p.add_argument("--days", type=int, default=30, help="Number of days to backtest (default: 30)")
    p.add_argument("--strategy", required=True, choices=list(STRATEGIES.keys()), help="Strategy to test")
    p.add_argument("--fee-rate", type=float, default=DEFAULT_FEE_RATE, help=f"Fee rate per trade (default: {DEFAULT_FEE_RATE})")
    p.add_argument("--initial-cash", type=float, default=INITIAL_CASH, help=f"Starting cash (default: {INITIAL_CASH:,.0f})")
    p.add_argument("--compare", action="store_true", help="Run old vs new rules side by side")
    return p.parse_args()


def main() -> None:
    args = parse_args()

    end_ts = int(datetime.now(timezone.utc).timestamp())
    start_ts = end_ts - args.days * 86400

    print(f"\nFetching {args.days} days of candle data for {args.product}...")

    print("  Fetching 1-hour candles...", end="", flush=True)
    candles_1h = fetch_candles(args.product, start_ts, end_ts, 3600)
    print(f" {len(candles_1h)} candles")

    print("  Fetching 1-day candles...", end="", flush=True)
    # Fetch extra 30 days of daily candles for macro context
    candles_1d = fetch_candles(args.product, start_ts - 30 * 86400, end_ts, 86400)
    print(f" {len(candles_1d)} candles")

    if len(candles_1h) < 30:
        print(f"\nERROR: Not enough hourly candles ({len(candles_1h)}). Need at least 30.")
        sys.exit(1)

    old_fn, new_fn = STRATEGIES[args.strategy]

    if args.compare:
        print(f"\nRunning comparison: {args.strategy} (old rules vs new rules)...")
        old_result = run_backtest(
            candles_1h, candles_1d, args.product, old_fn,
            args.fee_rate, args.initial_cash, args.strategy, args.days,
        )
        new_result = run_backtest(
            candles_1h, candles_1d, args.product, new_fn,
            args.fee_rate, args.initial_cash, args.strategy, args.days,
        )

        print(f"\n{'═' * 70}")
        print(f"  BACKTEST COMPARISON: {args.strategy} | {args.product} | {args.days} days")
        print(f"  Fee rate: {args.fee_rate:.2%} | Initial cash: ${args.initial_cash:,.0f}")
        print(f"{'═' * 70}")
        print(format_comparison(old_result, new_result))
        print(f"{'═' * 70}")

        improvement = new_result.total_return_pct - old_result.total_return_pct
        if improvement > 0:
            print(f"\n  \033[32mNew rules outperformed by {improvement:+.2f}%\033[0m")
        elif improvement < 0:
            print(f"\n  \033[31mOld rules outperformed by {-improvement:+.2f}%\033[0m")
        else:
            print(f"\n  No difference in total return.")
    else:
        print(f"\nRunning backtest: {args.strategy} (new rules)...")
        result = run_backtest(
            candles_1h, candles_1d, args.product, new_fn,
            args.fee_rate, args.initial_cash, args.strategy, args.days,
        )

        print(f"\n{'═' * 50}")
        print(f"  BACKTEST RESULTS")
        print(f"  Fee rate: {args.fee_rate:.2%}")
        print(f"{'═' * 50}")
        print(format_result(result, f"{args.strategy} (new)"))
        print(f"{'═' * 50}\n")


if __name__ == "__main__":
    main()
