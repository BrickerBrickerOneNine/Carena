"""
Trading Tool — an @agent_tool that executes buy/sell trades against
an in-memory portfolio store and rerenders a Rich Live dashboard
after every trade.

Prices are sourced from a Kafka topic (market_data.prices) published
by the Coinbase connector, which keeps a live price book.

The account store is keyed by agent_id so multiple agent runtimes
can each maintain independent portfolios.  The agent_id is resolved
at runtime via ToolContext injection (ctx.agent_name).

Usage:
    uv run python tools_and_dashboard.py --bootstrap-servers <broker-url>
"""

from __future__ import annotations

import json
import logging
import os
import time
import typing
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

import plotext as plt
import sympy
from rich.ansi import AnsiDecoder
from rich.columns import Columns
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from calfkit.models.tool_context import ToolContext
from calfkit.nodes.base_tool_node import agent_tool
from coinbase_consumer import PriceBook

logger = logging.getLogger(__name__)

# ── Configuration ────────────────────────────────────────────────

INITIAL_CASH = 1_000.0
TRADE_FEE_RATE = float(os.getenv("TRADE_FEE_RATE", "0.05"))  # 5% default (simulated mode)
TAX_RATE = float(os.getenv("TAX_RATE", "0.30"))  # 30% combined federal + state short-term cap gains

# Coinbase Advanced Trade fee schedule
COINBASE_TAKER_FEE = 0.012   # 1.2% — market orders (auto-detected at startup)
COINBASE_MAKER_FEE = 0.004   # 0.4% — limit orders (auto-detected at startup)

MAX_BALANCE_HISTORY = 300  # ~25 min at 5s intervals

# ── Trading mode ─────────────────────────────────────────────────
TRADING_MODE: str = os.getenv("TRADING_MODE", "simulated")  # "simulated" or "live"

# ── Trade guardrails ─────────────────────────────────────────────
MIN_HOLD_SECONDS = 120  # 2 min minimum hold (unless stop-loss)
MAX_TRADES_PER_HOUR = 10  # rolling 1-hour window
STOP_LOSS_OVERRIDE_PCT = 1.5  # loss % that overrides min hold time

AGENT_COLORS: dict[str, str] = {
    "momentum": "cyan",
    "brainrot-daytrader": "magenta",
    "scalper": "yellow",
}
_FALLBACK_COLORS = ["green", "red", "blue", "orange", "white"]


# ── Data model ───────────────────────────────────────────────────


@dataclass
class TradeResult:
    success: bool
    message: str


@dataclass
class TradeLogEntry:
    timestamp: str
    agent_id: str
    action: str
    product_id: str
    quantity: float
    price: float
    fee: float
    latency: float | None


@dataclass
class LimitOrder:
    order_id: str
    agent_id: str
    product_id: str
    action: str  # "buy" or "sell"
    quantity: float
    limit_price: float
    created_at: float  # Unix timestamp

    _next_id: typing.ClassVar[int] = 0

    @classmethod
    def next_id(cls) -> str:
        cls._next_id += 1
        return f"LO-{cls._next_id}"

    def to_dict(self) -> dict:
        return {
            "order_id": self.order_id,
            "agent_id": self.agent_id,
            "product_id": self.product_id,
            "action": self.action,
            "quantity": self.quantity,
            "limit_price": self.limit_price,
            "created_at": self.created_at,
        }

    @classmethod
    def from_dict(cls, data: dict) -> LimitOrder:
        return cls(
            order_id=data["order_id"],
            agent_id=data["agent_id"],
            product_id=data["product_id"],
            action=data["action"],
            quantity=data["quantity"],
            limit_price=data["limit_price"],
            created_at=data["created_at"],
        )


@dataclass
class AgentAccount:
    cash: float = INITIAL_CASH
    initial_cash: float = INITIAL_CASH  # tracks each agent's actual starting cash for P&L
    positions: dict[str, float] = field(default_factory=dict)
    cost_basis: dict[str, float] = field(default_factory=dict)
    # Weighted-average entry timestamp (Unix epoch) per position
    avg_entry_ts: dict[str, float] = field(default_factory=dict)
    trade_count: int = 0
    total_fees: float = 0.0
    # Performance tracking
    wins: int = 0
    losses: int = 0
    total_pnl_realized: float = 0.0
    peak_value: float = INITIAL_CASH
    max_drawdown: float = 0.0
    consecutive_losses: int = 0
    # Trade guardrail state
    last_buy_ts: dict[str, float] = field(default_factory=dict)  # product -> timestamp of last buy
    trade_timestamps: list[float] = field(default_factory=list)  # rolling trade times for rate limit

    def portfolio_value(self, price_book: PriceBook) -> float:
        """Total value: cash + mark-to-market of all positions using mid-price."""
        positions_value = 0.0
        for pid, qty in self.positions.items():
            entry = price_book.get(pid)
            if entry is not None:
                mid = (float(entry["best_bid"]) + float(entry["best_ask"])) / 2
                positions_value += qty * mid
        return self.cash + positions_value

    def avg_cost_per_unit(self, product_id: str) -> float:
        """Average cost per unit for a position."""
        qty = self.positions.get(product_id, 0)
        if qty == 0:
            return 0.0
        return self.cost_basis.get(product_id, 0.0) / qty

    def to_dict(self) -> dict:
        return {
            "cash": self.cash,
            "initial_cash": self.initial_cash,
            "positions": dict(self.positions),
            "cost_basis": dict(self.cost_basis),
            "avg_entry_ts": dict(self.avg_entry_ts),
            "trade_count": self.trade_count,
            "total_fees": self.total_fees,
            "wins": self.wins,
            "losses": self.losses,
            "total_pnl_realized": self.total_pnl_realized,
            "peak_value": self.peak_value,
            "max_drawdown": self.max_drawdown,
            "consecutive_losses": self.consecutive_losses,
            "last_buy_ts": dict(self.last_buy_ts),
            "trade_timestamps": list(self.trade_timestamps),
        }

    @classmethod
    def from_dict(cls, data: dict) -> AgentAccount:
        acct = cls()
        acct.cash = data["cash"]
        # Fall back to cash value for old checkpoints that don't have initial_cash
        acct.initial_cash = data.get("initial_cash", data["cash"])
        acct.positions = data.get("positions", {})
        acct.cost_basis = data.get("cost_basis", {})
        acct.avg_entry_ts = data.get("avg_entry_ts", {})
        acct.trade_count = data.get("trade_count", 0)
        acct.total_fees = data.get("total_fees", 0.0)
        acct.wins = data.get("wins", 0)
        acct.losses = data.get("losses", 0)
        acct.total_pnl_realized = data.get("total_pnl_realized", 0.0)
        acct.peak_value = data.get("peak_value", INITIAL_CASH)
        acct.max_drawdown = data.get("max_drawdown", 0.0)
        acct.consecutive_losses = data.get("consecutive_losses", 0)
        acct.last_buy_ts = data.get("last_buy_ts", {})
        acct.trade_timestamps = data.get("trade_timestamps", [])
        return acct


# ── Trade recorder protocol ──────────────────────────────────────


class TradeRecorder(typing.Protocol):
    def record_trade(
        self,
        *,
        agent_id: str,
        action: str,
        product_id: str,
        quantity: float,
        price: float,
        fee: float,
        cash_after: float,
        latency: float | None,
        trade_pnl: float | None = None,
    ) -> None: ...


# ── Account store ────────────────────────────────────────────────


class AccountStore:
    """In-memory trading account store, keyed by agent_id."""

    def __init__(self, price_book: PriceBook) -> None:
        self._accounts: dict[str, AgentAccount] = {}
        self._trade_log: list[TradeLogEntry] = []
        self._price_book = price_book
        self._data_recorder: TradeRecorder | None = None
        self._pending_orders: list[LimitOrder] = []
        self._coinbase_trader: object | None = None  # CoinbaseTrader when live
        self._checkpoint_path: Path | None = None

    def attach_recorder(self, recorder: TradeRecorder) -> None:
        self._data_recorder = recorder

    def attach_coinbase_trader(self, trader: object) -> None:
        """Attach a CoinbaseTrader for live trading mode."""
        self._coinbase_trader = trader
        logger.info("Live Coinbase trading enabled")

    # ── State persistence ──────────────────────────────────────────

    def set_checkpoint_path(self, path: str | Path) -> None:
        self._checkpoint_path = Path(path)

    def save_checkpoint(self) -> None:
        """Atomically write current state to JSON checkpoint file."""
        if self._checkpoint_path is None:
            return
        state = {
            "accounts": {aid: acct.to_dict() for aid, acct in self._accounts.items()},
            "pending_orders": [o.to_dict() for o in self._pending_orders],
            "next_order_id": LimitOrder._next_id,
            "saved_at": datetime.now().isoformat(),
        }
        tmp = self._checkpoint_path.with_suffix(".tmp")
        tmp.parent.mkdir(parents=True, exist_ok=True)
        tmp.write_text(json.dumps(state, indent=2))
        os.replace(str(tmp), str(self._checkpoint_path))

    @classmethod
    def load_checkpoint(cls, path: str | Path) -> dict | None:
        """Read a checkpoint file. Returns parsed dict or None if missing/corrupt."""
        p = Path(path)
        if not p.exists():
            return None
        try:
            return json.loads(p.read_text())
        except (json.JSONDecodeError, OSError) as e:
            logger.warning("Failed to load checkpoint %s: %s", p, e)
            return None

    def restore_from_checkpoint(self, state: dict) -> None:
        """Rebuild accounts and pending orders from checkpoint data."""
        self._accounts = {
            aid: AgentAccount.from_dict(data)
            for aid, data in state.get("accounts", {}).items()
        }
        self._pending_orders = [
            LimitOrder.from_dict(o) for o in state.get("pending_orders", [])
        ]
        LimitOrder._next_id = state.get("next_order_id", 0)
        logger.info(
            "Restored %d account(s) and %d pending order(s) from checkpoint",
            len(self._accounts), len(self._pending_orders),
        )

    def get_or_create(self, agent_id: str) -> AgentAccount:
        if agent_id not in self._accounts:
            self._accounts[agent_id] = AgentAccount(
                cash=INITIAL_CASH,
                initial_cash=INITIAL_CASH,
                peak_value=INITIAL_CASH,
            )
        return self._accounts[agent_id]

    @property
    def accounts(self) -> dict[str, AgentAccount]:
        return self._accounts

    @property
    def price_book(self) -> PriceBook:
        return self._price_book

    @property
    def trade_log(self) -> list[TradeLogEntry]:
        return self._trade_log

    # ── Management methods (used by web dashboard) ─────────────

    def reset_agent(self, agent_id: str) -> TradeResult:
        """Reset an agent account to initial state (starting cash, no positions)."""
        if agent_id not in self._accounts:
            return TradeResult(False, f"Agent '{agent_id}' not found.")
        # Cancel any pending orders
        self._pending_orders = [o for o in self._pending_orders if o.agent_id != agent_id]
        self._accounts[agent_id] = AgentAccount(
            cash=INITIAL_CASH,
            initial_cash=INITIAL_CASH,
            peak_value=INITIAL_CASH,
        )
        self.save_checkpoint()
        return TradeResult(True, f"Agent '{agent_id}' reset to ${INITIAL_CASH:,.2f} with no positions.")

    def adjust_cash(self, agent_id: str, amount: float) -> TradeResult:
        """Add or remove cash from an agent account."""
        if agent_id not in self._accounts:
            return TradeResult(False, f"Agent '{agent_id}' not found.")
        account = self._accounts[agent_id]
        new_cash = account.cash + amount
        if new_cash < 0:
            return TradeResult(False, f"Cannot set cash below zero (current: ${account.cash:,.2f}, adjustment: ${amount:,.2f}).")
        account.cash = new_cash
        self.save_checkpoint()
        sign = "+" if amount >= 0 else ""
        return TradeResult(True, f"Agent '{agent_id}' cash adjusted by {sign}${amount:,.2f}. New balance: ${new_cash:,.2f}.")

    def liquidate_agent(self, agent_id: str) -> TradeResult:
        """Sell all positions for an agent at current market prices."""
        if agent_id not in self._accounts:
            return TradeResult(False, f"Agent '{agent_id}' not found.")
        account = self._accounts[agent_id]
        if not account.positions:
            return TradeResult(True, f"Agent '{agent_id}' has no open positions.")
        results = []
        for pid, qty in list(account.positions.items()):
            result = self.execute_trade(agent_id, pid, qty, "sell")
            results.append(f"{pid}: {result.message}")
        return TradeResult(True, f"Liquidated all positions for '{agent_id}'. " + " | ".join(results))

    def sync_from_coinbase(self, agent_id: str, num_agents: int = 1) -> str:
        """Sync an agent's account to match real Coinbase balances.

        Queries Coinbase for actual USD + crypto holdings and overwrites
        the agent's cash and positions.  In multi-agent mode, USD is split
        evenly but crypto positions are only assigned to the first agent
        (since we can't know which agent owns which position).
        """
        if self._coinbase_trader is None:
            return "No Coinbase trader attached — cannot sync."

        # Get total balances (available + held) for crypto positions
        balances = self._coinbase_trader.get_balances(include_holds=True)
        if not balances:
            return "Failed to fetch Coinbase balances."
        # Get available-only USD for cash (held USD is locked in open orders)
        avail_balances = self._coinbase_trader.get_balances(include_holds=False)

        account = self.get_or_create(agent_id)

        # USD cash — use available only (held USD is in open orders)
        usd = avail_balances.pop("USD", 0.0)
        per_agent_usd = round(usd / max(1, num_agents), 2)
        account.cash = per_agent_usd
        account.initial_cash = per_agent_usd
        account.peak_value = per_agent_usd

        # Crypto positions — map to Coinbase product IDs (e.g. ETH -> ETH-USD)
        # Skip fiat and stablecoins (already counted as cash or dust)
        # Query spot prices from Coinbase so initial_cash includes crypto value
        account.positions.clear()
        account.cost_basis.clear()
        synced_positions = []
        crypto_value = 0.0
        skip_currencies = {"USD", "USDC", "USDT", "DAI", "GUSD", "PAX"}
        for currency, qty in balances.items():
            if qty <= 0 or currency in skip_currencies:
                continue
            product_id = f"{currency}-USD"
            account.positions[product_id] = qty
            # Query current spot price from Coinbase for accurate initial value
            spot = self._coinbase_trader.get_spot_price(product_id)
            if spot is not None:
                position_value = qty * spot
                crypto_value += position_value
                # Set cost basis to total cost (qty * price), not per-unit
                account.cost_basis[product_id] = position_value
                synced_positions.append(f"{qty:.6f} {currency} @ ${spot:,.2f} = ${position_value:,.2f}")
            else:
                account.cost_basis[product_id] = 0.0
                synced_positions.append(f"{qty:.6f} {currency} (price unavailable)")

        # Reset P&L counters since we're starting fresh from real state
        account.total_pnl_realized = 0.0
        account.total_fees = 0.0
        account.wins = 0
        account.losses = 0
        account.trade_count = 0
        account.consecutive_losses = 0
        account.max_drawdown = 0.0

        # Set initial_cash to total portfolio value (USD + crypto) so P&L starts at 0
        total_value = per_agent_usd + crypto_value
        account.initial_cash = total_value
        account.peak_value = total_value

        self.save_checkpoint()

        parts = [f"Synced: ${per_agent_usd:,.2f} USD + ${crypto_value:,.2f} crypto = ${total_value:,.2f} total"]
        if synced_positions:
            parts.append("Positions: " + ", ".join(synced_positions))
        else:
            parts.append("No crypto positions")
        return " | ".join(parts)

    def resync_from_coinbase(self, agent_id: str, num_agents: int = 1) -> str:
        """Re-sync an agent's cash and positions from Coinbase without resetting P&L.

        Unlike sync_from_coinbase() (which resets all counters for initial setup),
        this method only updates cash and position quantities to match reality,
        preserving trade history, P&L, fees, and other accounting state.
        """
        if self._coinbase_trader is None:
            return "No Coinbase trader attached — cannot resync."

        balances = self._coinbase_trader.get_balances(include_holds=True)
        if not balances:
            return "Failed to fetch Coinbase balances."
        avail_balances = self._coinbase_trader.get_balances(include_holds=False)

        account = self._accounts.get(agent_id)
        if account is None:
            return f"Agent '{agent_id}' not found — skipping resync."

        # ── Cash ──────────────────────────────────────────────────
        usd = avail_balances.pop("USD", 0.0)
        new_cash = round(usd / max(1, num_agents), 2)
        cash_delta = new_cash - account.cash
        account.cash = new_cash

        # ── Positions ─────────────────────────────────────────────
        skip_currencies = {"USD", "USDC", "USDT", "DAI", "GUSD", "PAX"}
        position_deltas: list[str] = []

        # Build set of real positions from Coinbase
        real_positions: dict[str, float] = {}
        for currency, qty in balances.items():
            if qty > 0 and currency not in skip_currencies:
                real_positions[f"{currency}-USD"] = qty

        # Update existing and add new positions
        for product_id, real_qty in real_positions.items():
            old_qty = account.positions.get(product_id, 0.0)
            if abs(real_qty - old_qty) > 1e-8:
                delta = real_qty - old_qty
                account.positions[product_id] = real_qty
                # Adjust cost basis proportionally
                if old_qty > 0 and real_qty > 0:
                    old_basis = account.cost_basis.get(product_id, 0.0)
                    account.cost_basis[product_id] = old_basis * (real_qty / old_qty)
                elif old_qty == 0:
                    # New position we didn't track — use spot price as cost basis
                    spot = self._coinbase_trader.get_spot_price(product_id)
                    account.cost_basis[product_id] = real_qty * (spot or 0.0)
                position_deltas.append(f"{product_id}: {old_qty:.6f} → {real_qty:.6f} ({delta:+.6f})")

        # Remove positions that no longer exist on Coinbase
        for product_id in list(account.positions.keys()):
            if product_id not in real_positions:
                old_qty = account.positions.pop(product_id)
                account.cost_basis.pop(product_id, None)
                account.avg_entry_ts.pop(product_id, None)
                account.last_buy_ts.pop(product_id, None)
                position_deltas.append(f"{product_id}: {old_qty:.6f} → 0 (removed)")

        self.save_checkpoint()

        if abs(cash_delta) < 0.01 and not position_deltas:
            return "In sync — no changes."

        parts = []
        if abs(cash_delta) >= 0.01:
            parts.append(f"Cash: ${account.cash - cash_delta:,.2f} → ${account.cash:,.2f} ({cash_delta:+,.2f})")
        if position_deltas:
            parts.append("Positions: " + ", ".join(position_deltas))
        return "Resynced: " + " | ".join(parts)

    def cancel_all_orders(self, agent_id: str) -> TradeResult:
        """Cancel all pending limit orders for an agent."""
        before = len(self._pending_orders)
        self._pending_orders = [o for o in self._pending_orders if o.agent_id != agent_id]
        cancelled = before - len(self._pending_orders)
        if cancelled == 0:
            return TradeResult(True, f"Agent '{agent_id}' has no pending orders.")
        self.save_checkpoint()
        return TradeResult(True, f"Cancelled {cancelled} pending order(s) for '{agent_id}'.")

    def execute_trade(
        self,
        agent_id: str,
        product_id: str,
        quantity: float,
        action: str,
        latency: float | None = None,
    ) -> TradeResult:
        product_id = product_id.upper().strip()
        action = action.lower().strip()

        if action not in ("buy", "sell"):
            return TradeResult(False, f"Invalid action '{action}'. Must be 'buy' or 'sell'.")

        entry = self._price_book.get(product_id)
        if entry is None:
            available = ", ".join(sorted(self._price_book.snapshot().keys()))
            return TradeResult(
                False,
                f"No live price for '{product_id}'. "
                f"Available: {available or 'none (waiting for price data)'}",
            )

        if quantity <= 0:
            return TradeResult(False, "Quantity must be positive.")

        rounded = round(quantity, 6)
        if abs(quantity - rounded) > 1e-9:
            return TradeResult(
                False, "Quantity must have at most 6 decimal places (e.g., 0.5, 0.001, 0.000001)."
            )
        quantity = rounded

        account = self.get_or_create(agent_id)

        # ── Trade frequency guardrail ─────────────────────────────
        now_ts = time.time()
        # Prune old timestamps (older than 1 hour)
        account.trade_timestamps = [
            ts for ts in account.trade_timestamps if now_ts - ts < 3600
        ]
        if len(account.trade_timestamps) >= MAX_TRADES_PER_HOUR:
            return TradeResult(
                False,
                f"Rate limit: {MAX_TRADES_PER_HOUR} trades/hour reached. "
                f"Wait before trading again to avoid fee bleed.",
            )

        if action == "buy":
            price = float(entry["best_ask"])
            cost = price * quantity
            # In live mode, use Coinbase fee for cash check (real fee comes from fills)
            if TRADING_MODE == "live":
                fee_rate = COINBASE_TAKER_FEE
            else:
                fee_rate = TRADE_FEE_RATE
            fee = cost * fee_rate
            total_cost = cost + fee
            if total_cost > account.cash:
                return TradeResult(
                    False,
                    f"Insufficient cash. Need ${total_cost:,.2f} (incl ${fee:,.2f} fee) "
                    f"but only have ${account.cash:,.2f}.",
                )
            # Live trading: place real order on Coinbase, then use actual fill data
            if TRADING_MODE == "live" and self._coinbase_trader is not None:
                from coinbase_trader import CoinbaseTrader
                trader: CoinbaseTrader = self._coinbase_trader  # type: ignore[assignment]
                result = trader.market_buy(product_id, cost)
                if not result.success:
                    return TradeResult(False, f"[LIVE] {result.message}")
                # Override with real fill data from Coinbase
                if result.filled_price is not None:
                    price = result.filled_price
                    quantity = result.filled_qty or quantity
                    fee = result.filled_fees or 0.0
                    cost = result.filled_quote_size or (price * quantity)
                    total_cost = cost + fee
            account.cash -= total_cost
            account.total_fees += fee
            existing_qty = account.positions.get(product_id, 0)
            existing_ts = account.avg_entry_ts.get(product_id, now_ts)
            account.avg_entry_ts[product_id] = (existing_qty * existing_ts + quantity * now_ts) / (
                existing_qty + quantity
            )
            account.positions[product_id] = existing_qty + quantity
            account.cost_basis[product_id] = account.cost_basis.get(product_id, 0.0) + cost
            account.trade_count += 1
            account.last_buy_ts[product_id] = now_ts
            account.trade_timestamps.append(now_ts)
            self._record_trade(agent_id, action, product_id, quantity, price, fee, latency)
            return TradeResult(
                True,
                f"Bought {quantity} {product_id} @ ${price:,.2f} for ${cost:,.2f} "
                f"(fee: ${fee:,.2f}). Cash remaining: ${account.cash:,.2f}.",
            )

        # sell
        price = float(entry["best_bid"])
        held = account.positions.get(product_id, 0)
        if quantity > held:
            return TradeResult(
                False,
                f"Insufficient holdings. Want to sell {quantity} {product_id} "
                f"but only hold {held}.",
            )

        # ── Minimum hold time guardrail ───────────────────────────
        last_buy = account.last_buy_ts.get(product_id)
        if last_buy is not None:
            hold_seconds = now_ts - last_buy
            if hold_seconds < MIN_HOLD_SECONDS:
                # Allow override only for stop-loss (position down > STOP_LOSS_OVERRIDE_PCT)
                avg_cost = account.avg_cost_per_unit(product_id)
                loss_pct = ((avg_cost - price) / avg_cost * 100) if avg_cost > 0 else 0
                if loss_pct < STOP_LOSS_OVERRIDE_PCT:
                    remaining = int(MIN_HOLD_SECONDS - hold_seconds)
                    return TradeResult(
                        False,
                        f"Minimum hold time not met. Wait {remaining}s more "
                        f"(or position must be down >{STOP_LOSS_OVERRIDE_PCT}% for emergency exit).",
                    )

        # Live trading: place real order on Coinbase, then use actual fill data
        if TRADING_MODE == "live" and self._coinbase_trader is not None:
            from coinbase_trader import CoinbaseTrader
            trader: CoinbaseTrader = self._coinbase_trader  # type: ignore[assignment]
            result = trader.market_sell(product_id, quantity)
            if not result.success:
                return TradeResult(False, f"[LIVE] {result.message}")
            # Override with real fill data from Coinbase
            if result.filled_price is not None:
                price = result.filled_price
                quantity = result.filled_qty or quantity
                fee = result.filled_fees or 0.0
            else:
                fee = 0.0  # live mode — don't fabricate fees

        if TRADING_MODE != "live":
            fee = price * quantity * TRADE_FEE_RATE

        gross_proceeds = price * quantity
        net_proceeds = gross_proceeds - fee
        # Track realized P&L for this sell
        avg_cost = account.avg_cost_per_unit(product_id)
        trade_pnl = (price - avg_cost) * quantity - fee
        account.total_pnl_realized += trade_pnl
        if trade_pnl >= 0:
            account.wins += 1
            account.consecutive_losses = 0
        else:
            account.losses += 1
            account.consecutive_losses += 1

        account.cash += net_proceeds
        account.total_fees += fee
        # Reduce cost basis proportionally (average cost method)
        account.cost_basis[product_id] = account.cost_basis.get(product_id, 0.0) - (
            avg_cost * quantity
        )
        new_qty = round(held - quantity, 8)
        if new_qty <= 1e-9:
            account.positions.pop(product_id, None)
            account.cost_basis.pop(product_id, None)
            account.avg_entry_ts.pop(product_id, None)
            account.last_buy_ts.pop(product_id, None)
        else:
            account.positions[product_id] = new_qty
        account.trade_count += 1
        account.trade_timestamps.append(now_ts)

        # Update peak value and drawdown
        current_value = account.portfolio_value(self._price_book)
        if current_value > account.peak_value:
            account.peak_value = current_value
        drawdown = (account.peak_value - current_value) / account.peak_value * 100
        if drawdown > account.max_drawdown:
            account.max_drawdown = drawdown

        self._record_trade(agent_id, action, product_id, quantity, price, fee, latency, trade_pnl=trade_pnl)
        pnl_sign = "+" if trade_pnl >= 0 else ""
        return TradeResult(
            True,
            f"Sold {quantity} {product_id} @ ${price:,.2f} for ${gross_proceeds:,.2f} "
            f"(fee: ${fee:,.2f}, net: ${net_proceeds:,.2f}, P&L: {pnl_sign}${trade_pnl:,.2f}). "
            f"Cash remaining: ${account.cash:,.2f}.",
        )

    def _record_trade(
        self,
        agent_id: str,
        action: str,
        product_id: str,
        quantity: float,
        price: float,
        fee: float,
        latency: float | None = None,
        trade_pnl: float | None = None,
    ) -> None:
        ts = datetime.now().strftime("%H:%M:%S")
        self._trade_log.append(TradeLogEntry(ts, agent_id, action, product_id, quantity, price, fee, latency))

        if self._data_recorder is not None:
            account = self._accounts.get(agent_id)
            self._data_recorder.record_trade(
                agent_id=agent_id,
                action=action,
                product_id=product_id,
                quantity=quantity,
                price=price,
                fee=fee,
                cash_after=account.cash if account else 0.0,
                latency=latency,
                trade_pnl=trade_pnl,
            )

        self.save_checkpoint()

    # ── Limit orders ──────────────────────────────────────────────

    def place_limit_order(
        self, agent_id: str, product_id: str, action: str, quantity: float, limit_price: float
    ) -> TradeResult:
        product_id = product_id.upper().strip()
        action = action.lower().strip()
        if action not in ("buy", "sell"):
            return TradeResult(False, f"Invalid action '{action}'. Must be 'buy' or 'sell'.")
        if quantity <= 0:
            return TradeResult(False, "Quantity must be positive.")
        if limit_price <= 0:
            return TradeResult(False, "Limit price must be positive.")

        account = self.get_or_create(agent_id)

        # Validate: buy orders need enough cash at limit price
        if action == "buy":
            cost = limit_price * quantity
            fee = cost * TRADE_FEE_RATE
            if cost + fee > account.cash:
                return TradeResult(
                    False,
                    f"Insufficient cash for limit buy. Need ${cost + fee:,.2f} but have ${account.cash:,.2f}.",
                )
        else:
            held = account.positions.get(product_id, 0)
            if quantity > held:
                return TradeResult(
                    False,
                    f"Insufficient holdings for limit sell. Hold {held} {product_id}.",
                )

        # Live trading: place real limit order on Coinbase
        coinbase_order_id = None
        if TRADING_MODE == "live" and self._coinbase_trader is not None:
            from coinbase_trader import CoinbaseTrader
            trader: CoinbaseTrader = self._coinbase_trader  # type: ignore[assignment]
            if action == "buy":
                result = trader.limit_buy(product_id, quantity, limit_price)
            else:
                result = trader.limit_sell(product_id, quantity, limit_price)
            if not result.success:
                return TradeResult(False, f"[LIVE] {result.message}")
            coinbase_order_id = result.order_id

        order = LimitOrder(
            order_id=coinbase_order_id or LimitOrder.next_id(),
            agent_id=agent_id,
            product_id=product_id,
            action=action,
            quantity=quantity,
            limit_price=limit_price,
            created_at=time.time(),
        )
        self._pending_orders.append(order)
        self.save_checkpoint()
        return TradeResult(
            True,
            f"Limit {action} order placed: {quantity} {product_id} @ ${limit_price:,.2f} "
            f"(order ID: {order.order_id}). Will fill when price reaches target.",
        )

    def cancel_order(self, agent_id: str, order_id: str) -> TradeResult:
        for i, order in enumerate(self._pending_orders):
            if order.order_id == order_id and order.agent_id == agent_id:
                self._pending_orders.pop(i)
                self.save_checkpoint()
                return TradeResult(True, f"Order {order_id} cancelled.")
        return TradeResult(False, f"Order '{order_id}' not found or not yours.")

    def get_pending_orders(self, agent_id: str) -> list[LimitOrder]:
        return [o for o in self._pending_orders if o.agent_id == agent_id]

    def check_and_fill_orders(self) -> list[str]:
        """Check all pending limit orders against current prices. Returns list of fill messages."""
        # In live mode, Coinbase handles limit order fills on the exchange.
        # Don't also fire local market orders — that would double-execute.
        if TRADING_MODE == "live":
            return []
        filled: list[str] = []
        remaining: list[LimitOrder] = []
        for order in self._pending_orders:
            entry = self._price_book.get(order.product_id)
            if entry is None:
                remaining.append(order)
                continue

            should_fill = False
            if order.action == "buy":
                # Buy limit fills when ask <= limit price
                ask = float(entry["best_ask"])
                if ask <= order.limit_price:
                    should_fill = True
            else:
                # Sell limit fills when bid >= limit price
                bid = float(entry["best_bid"])
                if bid >= order.limit_price:
                    should_fill = True

            if should_fill:
                result = self.execute_trade(
                    order.agent_id, order.product_id, order.quantity, order.action
                )
                msg = f"[Limit order {order.order_id} filled] {result.message}"
                filled.append(msg)
                logger.info(msg)
            else:
                remaining.append(order)

        self._pending_orders = remaining
        return filled


# ── Rich Live view ───────────────────────────────────────────────


class PlotextChart:
    """Rich-compatible renderable that draws a plotext line chart."""

    def __init__(
        self,
        balance_history: dict[str, deque[tuple[str, float]]],
        chart_height: int = 12,
    ) -> None:
        self._balance_history = balance_history
        self._chart_height = chart_height

    def __rich_console__(
        self, console: object, options: object
    ) -> typing.Generator[Text, None, None]:
        width = getattr(options, "max_width", 80)

        plt.clf()
        plt.plotsize(width, self._chart_height)
        plt.theme("dark")
        plt.title("Portfolio Value Over Time")
        plt.ylabel("USD")

        has_data = any(len(d) > 0 for d in self._balance_history.values())

        if not has_data:
            plt.plot([0, 1], [INITIAL_CASH, INITIAL_CASH], label="waiting...", color="gray")
        else:
            # Right-align all series so the latest snapshot is always at
            # the right edge, regardless of when each agent started.
            max_len = max(len(h) for h in self._balance_history.values())

            fallback_idx = 0
            for agent_id, history in self._balance_history.items():
                if not history:
                    continue
                timestamps, values = zip(*history)
                n = len(values)
                offset = max_len - n
                x_indices = list(range(offset, offset + n))
                color = AGENT_COLORS.get(agent_id)
                if color is None:
                    color = _FALLBACK_COLORS[fallback_idx % len(_FALLBACK_COLORS)]
                    fallback_idx += 1
                plt.plot(x_indices, list(values), label=agent_id, color=color, marker="braille")

            # Build evenly-spaced time tick labels from the longest series
            longest = max(self._balance_history.values(), key=len)
            n = len(longest)
            num_ticks = min(7, n)
            if num_ticks > 1:
                step = (n - 1) / (num_ticks - 1)
                positions = [int(round(i * step)) for i in range(num_ticks)]
            else:
                positions = [0]
            labels = [longest[p][0] for p in positions]
            plt.xticks(positions, labels)

        canvas = plt.build()
        decoder = AnsiDecoder()
        yield from decoder.decode(canvas)


class PortfolioView:
    """Builds and rerenders a Rich Live dashboard from AccountStore state."""

    def __init__(self, store: AccountStore) -> None:
        self._store = store
        self._live: Live | None = None
        self._balance_history: dict[str, deque[tuple[str, float]]] = {}

    def attach_live(self, live: Live) -> None:
        self._live = live

    def rerender(self) -> None:
        if self._live is not None:
            self._capture_balance_snapshot()
            self._live.update(self._build_layout(), refresh=True)

    def _capture_balance_snapshot(self) -> None:
        price_book = self._store.price_book
        ts = datetime.now().strftime("%H:%M:%S")
        for agent_id, account in self._store.accounts.items():
            if agent_id not in self._balance_history:
                self._balance_history[agent_id] = deque(maxlen=MAX_BALANCE_HISTORY)
            value = account.portfolio_value(price_book)
            self._balance_history[agent_id].append((ts, value))

    def _build_layout(self) -> Layout:
        layout = Layout()
        layout.split_column(
            Layout(name="header", size=3),
            Layout(name="summary_header", size=1),
            Layout(name="summary", size=7),
            Layout(name="body", ratio=2),
            Layout(name="chart", size=15),
        )
        layout["header"].update(self._build_header())
        layout["summary_header"].update(
            Text.from_markup("[bold]Agent Account Summaries[/]", justify="center")
        )
        layout["summary"].update(self._build_summary_cards())
        layout["body"].split_row(
            Layout(name="positions", ratio=3),
            Layout(name="log", ratio=2),
        )
        layout["positions"].update(self._build_positions_table())
        layout["log"].update(self._build_trade_log())
        layout["chart"].update(self._build_chart())
        return layout

    def _build_chart(self) -> Panel:
        chart = PlotextChart(self._balance_history, chart_height=12)
        return Panel(chart, border_style="blue")

    def _build_header(self) -> Panel:
        now = datetime.now().strftime("%H:%M:%S")
        return Panel(
            Text.from_markup(
                "[bold cyan]Portfolio Dashboard[/]  [bold red]●[/] "
                f"[bold green]LIVE[/]  [dim]|  {now}[/]"
            ),
            style="cyan",
            height=3,
        )

    def _build_summary_cards(self) -> Columns:
        accounts = self._store.accounts
        price_book = self._store.price_book

        cards = []
        sorted_accounts = sorted(
            accounts.items(),
            key=lambda item: item[1].portfolio_value(price_book),
            reverse=True,
        )
        for rank, (agent_id, account) in enumerate(sorted_accounts, start=1):
            value = account.portfolio_value(price_book)
            estimated_tax = max(0.0, account.total_pnl_realized) * TAX_RATE
            pnl_color = "green" if account.total_pnl_realized >= 0 else "red"
            pnl_sign = "+" if account.total_pnl_realized >= 0 else ""
            card = Panel(
                Text.from_markup(
                    f"[magenta]Total Value:[/] ${value:,.2f}\n"
                    f"[yellow]Positions:[/] {len(account.positions)}  "
                    f"[cyan]Trades:[/] {account.trade_count}  "
                    f"[red]Fees:[/] ${account.total_fees:,.2f}\n"
                    f"[{pnl_color}]Realized P&L:[/] {pnl_sign}${account.total_pnl_realized:,.2f}  "
                    f"[red]Est Tax:[/] -${estimated_tax:,.2f}"
                ),
                title=f"[bold]#{rank} {agent_id}[/]",
                border_style="cyan",
            )
            cards.append(card)

        if not cards:
            cards.append(Panel("[dim]No accounts yet[/]", border_style="dim"))

        return Columns(cards, expand=True, equal=True)

    def _build_positions_table(self) -> Panel:
        table = Table(expand=True, show_lines=False)
        table.add_column("Agent", style="bold cyan", ratio=2)
        table.add_column("Trades", justify="right", ratio=1)
        table.add_column("Cash", justify="right", ratio=2)
        table.add_column("Fees Paid", justify="right", ratio=2)
        table.add_column("Ticker", ratio=2)
        table.add_column("Qty", justify="right", ratio=1)
        table.add_column("Cost Basis", justify="right", ratio=2)
        table.add_column("Mkt Value", justify="right", ratio=2)
        table.add_column("P&L", justify="right", ratio=2)
        table.add_column("Total Value", justify="right", ratio=2)

        accounts = self._store.accounts
        price_book = self._store.price_book
        if not accounts:
            table.add_row("[dim]No accounts yet[/]", "", "", "", "", "", "", "", "", "")
        else:
            first = True
            for agent_id, account in accounts.items():
                if not first:
                    table.add_section()
                first = False
                total_value = account.portfolio_value(price_book)
                total_pnl = total_value - account.initial_cash
                # Agent header row with cash and fees
                table.add_row(
                    agent_id,
                    str(account.trade_count),
                    f"[green]${account.cash:,.2f}[/]",
                    f"[red]${account.total_fees:,.2f}[/]",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                )
                # Individual ticker rows
                if not account.positions:
                    table.add_row(
                        "",
                        "",
                        "",
                        "",
                        "[dim]—[/]",
                        "[dim]—[/]",
                        "[dim]—[/]",
                        "[dim]—[/]",
                        "[dim]—[/]",
                        "",
                    )
                else:
                    for pid, qty in sorted(account.positions.items()):
                        entry = price_book.get(pid)
                        price = ((float(entry["best_bid"]) + float(entry["best_ask"])) / 2) if entry else 0.0
                        mkt_val = price * qty
                        cost_basis = account.cost_basis.get(pid, 0.0)
                        pnl = mkt_val - cost_basis
                        pnl_color = "green" if pnl >= 0 else "red"
                        pnl_sign = "+" if pnl >= 0 else ""
                        table.add_row(
                            "",
                            "",
                            "",
                            "",
                            pid,
                            f"{qty:g}",
                            f"${cost_basis:,.2f}",
                            f"${mkt_val:,.2f}",
                            f"[{pnl_color}]{pnl_sign}${pnl:,.2f}[/]",
                            "",
                        )
                # Total value row
                total_pnl_color = "green" if total_pnl >= 0 else "red"
                total_pnl_sign = "+" if total_pnl >= 0 else ""
                table.add_row(
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "[bold]Total[/]",
                    f"[bold {total_pnl_color}]{total_pnl_sign}${total_pnl:,.2f}[/]",
                    f"[bold]${total_value:,.2f}[/]",
                )

        return Panel(table, title="[bold]Agent Portfolios[/]", border_style="green")

    def _build_trade_log(self) -> Panel:
        table = Table(expand=True, show_lines=False, show_header=True, box=None)
        table.add_column("Time", style="dim", ratio=1)
        table.add_column("Action", ratio=1)
        table.add_column("Qty", justify="right", ratio=1)
        table.add_column("Ticker", ratio=2)
        table.add_column("Unit Price", justify="right", ratio=2)
        table.add_column("Fee", justify="right", ratio=1)
        table.add_column("Agent", style="dim", ratio=2)
        table.add_column("Latency", justify="right", style="dim", ratio=1)

        log = self._store.trade_log
        if not log:
            table.add_row("[dim italic]No trades yet...[/]", "", "", "", "", "", "", "")
        else:
            for entry in reversed(log):
                action_style = "bold green" if entry.action == "buy" else "bold red"
                latency_str = f"{entry.latency:.1f}s" if entry.latency is not None else ""
                table.add_row(
                    entry.timestamp,
                    f"[{action_style}]{entry.action.upper()}[/]",
                    f"{entry.quantity:g}",
                    entry.product_id,
                    f"${entry.price:,.2f}",
                    f"[red]${entry.fee:,.2f}[/]",
                    entry.agent_id,
                    latency_str,
                )

        return Panel(table, title="[bold]Trade Log (most recent)[/]", border_style="yellow")


# ── Module-level singletons ──────────────────────────────────────

price_book = PriceBook()
store = AccountStore(price_book)
view = PortfolioView(store)


# ── Shared tool logic ────────────────────────────────────────────


def _execute_trade(
    agent_id: str, product_id: str, quantity: float, action: str, latency: float | None = None
) -> str:
    result = store.execute_trade(agent_id, product_id, quantity, action, latency=latency)
    view.rerender()
    return result.message


def _format_hold_time(entry_ts: float | None) -> str:
    """Format elapsed time since entry as a human-readable string."""
    if entry_ts is None:
        return "N/A"
    seconds = datetime.now().timestamp() - entry_ts
    if seconds < 60:
        return f"{int(seconds)}s"
    minutes = seconds / 60
    if minutes < 60:
        return f"{int(minutes)}m"
    hours = minutes / 60
    if hours < 24:
        return f"{hours:.1f}h"
    return f"{hours / 24:.1f}d"


def _get_portfolio(agent_id: str) -> str:
    account = store.get_or_create(agent_id)
    pb = store.price_book

    lines = [f"Cash: ${account.cash:,.2f}"]
    lines.append(f"Total fees paid: ${account.total_fees:,.2f}")

    if not account.positions:
        lines.append("Positions: none")
    else:
        lines.append(
            "| Ticker | Qty | Avg Cost | Total Cost "
            "| Current Price | Mkt Value | P&L | Avg Time Held |"
        )
        lines.append("| --- | --- | --- | --- | --- | --- | --- | --- |")
        for pid in sorted(account.positions):
            qty = account.positions[pid]
            avg_cost = account.avg_cost_per_unit(pid)
            total_cost = account.cost_basis.get(pid, 0.0)
            hold_str = _format_hold_time(account.avg_entry_ts.get(pid))

            entry = pb.get(pid)
            if entry is not None:
                current_price = (float(entry["best_bid"]) + float(entry["best_ask"])) / 2
                mkt_value = current_price * qty
                pnl = mkt_value - total_cost
                pnl_sign = "+" if pnl >= 0 else ""
                lines.append(
                    f"| {pid} | {qty:g} | ${avg_cost:,.2f} | ${total_cost:,.2f} "
                    f"| ${current_price:,.2f} | ${mkt_value:,.2f} "
                    f"| {pnl_sign}${pnl:,.2f} | {hold_str} |"
                )
            else:
                lines.append(
                    f"| {pid} | {qty:g} | ${avg_cost:,.2f} | ${total_cost:,.2f} "
                    f"| N/A | N/A | N/A | {hold_str} |"
                )

    portfolio_val = account.portfolio_value(pb)
    lines.append(f"\nTotal portfolio value: ${portfolio_val:,.2f}")
    lines.append(f"Fee rate: {TRADE_FEE_RATE:.4%} per trade")

    # Performance stats
    total_sells = account.wins + account.losses
    lines.append("\n**Performance Stats:**")
    if total_sells > 0:
        win_rate = account.wins / total_sells * 100
        avg_pnl = account.total_pnl_realized / total_sells
        lines.append(f"- Win rate: {win_rate:.0f}% ({account.wins}W / {account.losses}L)")
        lines.append(f"- Avg P&L per trade: ${avg_pnl:,.2f}")
        lines.append(f"- Total realized P&L: ${account.total_pnl_realized:,.2f}")
        # Tax estimate on realized gains
        estimated_tax = max(0.0, account.total_pnl_realized) * TAX_RATE
        after_tax_pnl = account.total_pnl_realized - estimated_tax
        lines.append(f"- Estimated tax ({TAX_RATE:.0%}): -${estimated_tax:,.2f}")
        lines.append(f"- After-tax realized P&L: ${after_tax_pnl:,.2f}")
    else:
        lines.append("- No completed round-trips yet")
    lines.append(f"- Max drawdown: {account.max_drawdown:.2f}%")
    lines.append(f"- Consecutive losses: {account.consecutive_losses}")

    # Guardrail status
    now_ts = time.time()
    recent_trades = sum(1 for ts in account.trade_timestamps if now_ts - ts < 3600)
    lines.append(f"\n**Guardrails:** {recent_trades}/{MAX_TRADES_PER_HOUR} trades used this hour")

    # Pending limit orders
    pending = store.get_pending_orders(agent_id)
    if pending:
        lines.append("\n**Pending Limit Orders:**")
        lines.append("| Order ID | Action | Product | Qty | Limit Price | Age |")
        lines.append("| --- | --- | --- | --- | --- | --- |")
        for o in pending:
            age = _format_hold_time(o.created_at)
            lines.append(
                f"| {o.order_id} | {o.action} | {o.product_id} | {o.quantity:g} "
                f"| ${o.limit_price:,.2f} | {age} |"
            )

    return "\n".join(lines)


# ── Shared agent tools (ToolContext injection) ───────────────────


@agent_tool
def execute_trade(ctx: ToolContext, product_id: str, quantity: float, action: str) -> str:
    """Execute a buy or sell trade (fill-or-cancel). The order fills immediately at the current
    market price if possible, or returns an error if it cannot be filled — it never waits or queues.
    Buys execute at the best ask price, sells at the best bid.
    Fractional trading is allowed, up to 6 decimal places (e.g., 0.5, 0.001, 0.000001).

    Args:
        product_id: Trading pair (e.g., BTC-USD, ETH-USD, SOL-USD)
        quantity: Number of units to trade (positive, up to 6 decimal places)
        action: 'buy' or 'sell'

    Returns:
        Trade confirmation with execution price and remaining cash, or an error message
    """
    latency: float | None = None
    if isinstance(ctx.deps, dict) and "invoked_at" in ctx.deps:
        latency = time.time() - ctx.deps["invoked_at"]
    return _execute_trade(ctx.agent_name, product_id, quantity, action, latency=latency)


@agent_tool
def get_portfolio(ctx: ToolContext) -> str:
    """View your portfolio: available cash, open positions, and total value.

    Returns:
        A table of positions with quantity, average cost basis, current market
        price, unrealized P&L, and average time held — plus cash and total value
    """
    return _get_portfolio(ctx.agent_name)


@agent_tool
def calculator(ctx: ToolContext, expression: str) -> str:
    """Evaluate a math expression. Use for financial calculations you can't do in your head,
    such as position sizing, P&L, percentage changes, or risk/reward ratios.

    Respects standard order of operations (PEMDAS).
    Supported operators: +, -, *, /, ** (power), % (modulo), parentheses for grouping.
    Functions: abs(), sqrt(), log(), floor(), ceil(), min(), max().

    Args:
        expression: A math expression (e.g., '100000 * 0.02', '64200 / 3',
            '(50000 - 32100) / 32100 * 100', 'max(10, 20)')

    Returns:
        The numeric result
    """

    try:
        result = sympy.sympify(expression)
        return str(result.evalf() if not result.is_number else result)
    except (sympy.SympifyError, TypeError) as e:
        return f"Invalid expression: {e}"


@agent_tool
def place_limit_order(
    ctx: ToolContext, product_id: str, quantity: float, action: str, limit_price: float
) -> str:
    """Place a limit order that fills automatically when the price reaches your target.
    Unlike execute_trade (which fills immediately at market price), limit orders wait
    for a better price. Buy limits fill when the ask drops to your price; sell limits
    fill when the bid rises to your price.

    Args:
        product_id: Trading pair (e.g., BTC-USD, ETH-USD, SOL-USD)
        quantity: Number of units to trade (positive, up to 6 decimal places)
        action: 'buy' or 'sell'
        limit_price: The price at which the order should fill

    Returns:
        Order confirmation with order ID, or an error message
    """
    result = store.place_limit_order(ctx.agent_name, product_id, action, quantity, limit_price)
    view.rerender()
    return result.message


@agent_tool
def cancel_limit_order(ctx: ToolContext, order_id: str) -> str:
    """Cancel a pending limit order by its order ID.

    Args:
        order_id: The order ID returned when the limit order was placed (e.g., 'LO-1')

    Returns:
        Confirmation or error message
    """
    result = store.cancel_order(ctx.agent_name, order_id)
    view.rerender()
    return result.message
