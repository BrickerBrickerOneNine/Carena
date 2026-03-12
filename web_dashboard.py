"""
Web Dashboard — a FastAPI + WebSocket server that provides a live
browser-based view of the Crypto Daytrading Arena.

Runs inside the same process as tools_and_dashboard.py to share
the in-memory AccountStore and PriceBook singletons.

Also captures LLM agent activity (tool calls, reasoning, results)
via push_activity() called from tools_and_dashboard.py's Kafka subscriber.

Usage (standalone, for testing):
    uv run python web_dashboard.py --port 8080

Normally started automatically by tools_and_dashboard.py via:
    await start_web_dashboard(store, port=8080)
"""

import asyncio
import logging
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone

import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse

logger = logging.getLogger(__name__)

MAX_ACTIVITY = 200
MAX_BALANCE_HISTORY = 28800  # ~24h at 3s broadcast interval


# ── Activity log (populated by Kafka subscriber in tools_and_dashboard) ──


@dataclass
class ActivityEntry:
    timestamp: str
    agent_name: str
    kind: str  # "TOOL_CALL", "RESPONSE", "TOOL_RESULT"
    details: str


class ActivityLog:
    """Thread-safe-ish activity log for agent LLM events."""

    def __init__(self, maxlen: int = MAX_ACTIVITY) -> None:
        self._entries: deque[ActivityEntry] = deque(maxlen=maxlen)
        self._seen: set[tuple[str, int]] = set()

    def push(
        self,
        agent_name: str,
        kind: str,
        details: str,
        trace_id: str | None = None,
        history_len: int = 0,
    ) -> None:
        if trace_id:
            key = (trace_id, history_len)
            if key in self._seen:
                return
            self._seen.add(key)
            # Prevent unbounded growth of seen set
            if len(self._seen) > 5000:
                self._seen.clear()
        ts = datetime.now().strftime("%H:%M:%S")
        self._entries.append(ActivityEntry(ts, agent_name, kind, details))

    def serialize(self, limit: int = 100) -> list[dict]:
        entries = list(self._entries)[-limit:]
        return [
            {
                "timestamp": e.timestamp,
                "agent_name": e.agent_name,
                "kind": e.kind,
                "details": e.details,
            }
            for e in reversed(entries)
        ]


# Module-level instance — tools_and_dashboard.py pushes events here
activity_log = ActivityLog()


def push_activity(
    agent_name: str,
    kind: str,
    details: str,
    trace_id: str | None = None,
    history_len: int = 0,
) -> None:
    """Public API for pushing agent activity events into the web dashboard."""
    activity_log.push(agent_name, kind, details, trace_id, history_len)


# ── System health tracker ─────────────────────────────────────


class SystemHealth:
    """Tracks LLM API health based on agent_router.output message patterns.

    Every message on agent_router.output is recorded as ``"received"``.
    If the message contains an actual LLM response it is also recorded
    as ``"llm_response"``, and if that response includes tool calls,
    ``"tool_call"`` is recorded too.  When the LLM API is failing
    (quota exceeded, key invalid, model down), we see ``received``
    events but zero ``llm_response`` events — the response rate drops
    to 0% and the status transitions to ERROR.
    """

    WINDOW_SECONDS = 300  # 5-minute rolling window

    def __init__(self) -> None:
        self._events: deque[tuple[float, str]] = deque(maxlen=2000)
        self._first_event_at: float | None = None

    def record(self, category: str) -> None:
        now = time.time()
        if self._first_event_at is None:
            self._first_event_at = now
        self._events.append((now, category))

    def _window_counts(self) -> dict[str, int]:
        cutoff = time.time() - self.WINDOW_SECONDS
        counts: dict[str, int] = {"received": 0, "llm_response": 0, "tool_call": 0}
        for ts, cat in self._events:
            if ts >= cutoff:
                counts[cat] = counts.get(cat, 0) + 1
        return counts

    def status(self) -> str:
        now = time.time()
        counts = self._window_counts()
        received = counts["received"]
        responses = counts["llm_response"]

        if self._first_event_at is None:
            return "HEALTHY"

        time_since_first = now - self._first_event_at

        # Silence detection — messages stopped arriving
        if self._events:
            silence = now - self._events[-1][0]
            if silence > 300 and time_since_first > 300:
                return "ERROR"
            if silence > 120 and time_since_first > 120:
                return "WARNING"

        if received < 3:
            return "HEALTHY"

        response_rate = responses / received if received > 0 else 0
        if response_rate >= 0.25:
            return "HEALTHY"
        if response_rate > 0:
            return "WARNING"
        if received >= 5:
            return "ERROR"
        return "WARNING"

    def serialize(self) -> dict:
        now = time.time()
        counts = self._window_counts()
        received = counts["received"]
        responses = counts["llm_response"]
        tool_calls = counts["tool_call"]
        last_response_at: float | None = None
        last_event_at: float | None = None
        for ts, cat in reversed(self._events):
            if last_event_at is None:
                last_event_at = ts
            if cat == "llm_response" and last_response_at is None:
                last_response_at = ts
            if last_event_at is not None and last_response_at is not None:
                break
        return {
            "status": self.status(),
            "window_seconds": self.WINDOW_SECONDS,
            "messages_received": received,
            "llm_responses": responses,
            "tool_calls": tool_calls,
            "response_rate": round(responses / received, 3) if received > 0 else 0,
            "last_response_ago": round(now - last_response_at, 1) if last_response_at else None,
            "last_event_ago": round(now - last_event_at, 1) if last_event_at else None,
            "total_events": len(self._events),
        }


system_health = SystemHealth()


def update_health(category: str) -> None:
    """Public API for recording health events from tools_and_dashboard.py."""
    system_health.record(category)


# ── Connection manager ─────────────────────────────────────────


class ConnectionManager:
    def __init__(self) -> None:
        self._connections: list[WebSocket] = []

    async def connect(self, ws: WebSocket) -> None:
        await ws.accept()
        self._connections.append(ws)

    def disconnect(self, ws: WebSocket) -> None:
        if ws in self._connections:
            self._connections.remove(ws)

    async def broadcast(self, data: dict) -> None:
        dead: list[WebSocket] = []
        for ws in self._connections:
            try:
                await ws.send_json(data)
            except Exception:
                dead.append(ws)
        for ws in dead:
            if ws in self._connections:
                self._connections.remove(ws)


# ── Balance history tracker ────────────────────────────────────


class BalanceHistory:
    """Samples portfolio values each broadcast cycle for charting.

    Stores (unix_timestamp, value) tuples so the JS frontend can
    filter by arbitrary time ranges (1h, 6h, 1d, all).
    """

    def __init__(self, maxlen: int = MAX_BALANCE_HISTORY) -> None:
        self._series: dict[str, deque[tuple[float, float]]] = {}
        self._maxlen = maxlen

    def sample(self, store) -> None:
        now = time.time()
        pb = store.price_book
        for agent_id, account in store.accounts.items():
            if agent_id not in self._series:
                self._series[agent_id] = deque(maxlen=self._maxlen)
            val = account.portfolio_value(pb)
            self._series[agent_id].append((now, val))

    def serialize(self) -> dict[str, list[list]]:
        """Return {agent_id: [[unix_ts, value], ...]}."""
        return {
            aid: [[ts, v] for ts, v in series]
            for aid, series in self._series.items()
        }


# ── Serialization ──────────────────────────────────────────────


def _serialize_prices(price_book) -> dict:
    result = {}
    for pid, entry in price_book.snapshot().items():
        bid = float(entry["best_bid"])
        ask = float(entry["best_ask"])
        mid = (bid + ask) / 2
        spread = ask - bid
        spread_pct = (spread / mid * 100) if mid > 0 else 0
        result[pid] = {
            "price": float(entry["price"]),
            "bid": bid,
            "ask": ask,
            "spread": spread,
            "spread_pct": spread_pct,
            "volume_24h": float(entry["volume_24h"]),
            "time": entry.get("time", ""),
        }
    return result


def _serialize_agents(store, initial_cash: float, tax_rate: float) -> list[dict]:
    price_book = store.price_book
    agents = []
    for agent_id, account in store.accounts.items():
        total_value = account.portfolio_value(price_book)
        total_pnl = total_value - initial_cash
        return_pct = (total_pnl / initial_cash * 100) if initial_cash > 0 else 0
        estimated_tax = max(0.0, account.total_pnl_realized) * tax_rate
        after_tax_pnl = account.total_pnl_realized - estimated_tax
        total_trades = account.wins + account.losses
        win_rate = (account.wins / total_trades * 100) if total_trades > 0 else 0
        positions = []
        for pid, qty in sorted(account.positions.items()):
            entry = price_book.get(pid)
            if entry:
                mid = (float(entry["best_bid"]) + float(entry["best_ask"])) / 2
                mkt_val = mid * qty
                cost = account.cost_basis.get(pid, 0.0)
                pnl = mkt_val - cost
                pnl_pct = (pnl / cost * 100) if cost > 0 else 0
                entry_ts = account.avg_entry_ts.get(pid)
                hold_secs = (time.time() - entry_ts) if entry_ts else 0
                positions.append({
                    "product_id": pid,
                    "quantity": qty,
                    "cost_basis": cost,
                    "avg_cost": account.avg_cost_per_unit(pid),
                    "market_price": mid,
                    "market_value": mkt_val,
                    "unrealized_pnl": pnl,
                    "unrealized_pnl_pct": pnl_pct,
                    "hold_seconds": hold_secs,
                })
        # Pending limit orders for this agent
        orders = [
            {
                "order_id": o.order_id,
                "product_id": o.product_id,
                "action": o.action,
                "quantity": o.quantity,
                "limit_price": o.limit_price,
                "age_seconds": time.time() - o.created_at,
            }
            for o in store._pending_orders
            if o.agent_id == agent_id
        ]
        agents.append({
            "agent_id": agent_id,
            "cash": account.cash,
            "total_value": total_value,
            "total_pnl": total_pnl,
            "return_pct": return_pct,
            "trade_count": account.trade_count,
            "total_fees": account.total_fees,
            "wins": account.wins,
            "losses": account.losses,
            "win_rate": win_rate,
            "realized_pnl": account.total_pnl_realized,
            "estimated_tax": estimated_tax,
            "after_tax_pnl": after_tax_pnl,
            "max_drawdown": account.max_drawdown,
            "consecutive_losses": account.consecutive_losses,
            "positions": positions,
            "orders": orders,
        })
    agents.sort(key=lambda a: a["total_value"], reverse=True)
    return agents


def _serialize_trades(store, limit: int = 100) -> list[dict]:
    entries = store.trade_log[-limit:]
    return [
        {
            "timestamp": e.timestamp,
            "agent_id": e.agent_id,
            "action": e.action,
            "product_id": e.product_id,
            "quantity": e.quantity,
            "price": e.price,
            "total": e.price * e.quantity,
            "fee": e.fee,
            "latency": e.latency,
        }
        for e in reversed(entries)
    ]


def _serialize_full_state(
    store,
    initial_cash: float,
    tax_rate: float,
    balance_history: BalanceHistory,
    fee_rate_getter=None,
    tax_rate_getter=None,
) -> dict:
    import trading_tools
    return {
        "prices": _serialize_prices(store.price_book),
        "agents": _serialize_agents(store, initial_cash, tax_rate),
        "trades": _serialize_trades(store),
        "activity": activity_log.serialize(limit=100),
        "balance_history": balance_history.serialize(),
        "initial_cash": initial_cash,
        "settings": {
            "fee_rate": trading_tools.TRADE_FEE_RATE,
            "tax_rate": trading_tools.TAX_RATE,
        },
        "system_health": system_health.serialize(),
        "timestamp": datetime.now(timezone.utc).strftime("%H:%M:%S UTC"),
    }


# ── HTML template ─────────────────────────────────────────────

HTML_TEMPLATE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Crypto Daytrading Arena</title>
<style>
*, *::before, *::after { margin: 0; padding: 0; box-sizing: border-box; }
:root {
  --bg: #0b0f14; --surface: #141a22; --surface2: #1a2230;
  --border: #253040; --border-light: #2d3d50;
  --text: #e8edf4; --text-secondary: #9aa8b8; --text-dim: #6b7a8a;
  --green: #00d68f; --green-dim: rgba(0,214,143,0.12);
  --red: #ff5c5c; --red-dim: rgba(255,92,92,0.12);
  --blue: #5b9cf6; --blue-dim: rgba(91,156,246,0.12);
  --yellow: #ffc107; --yellow-dim: rgba(255,193,7,0.12);
  --cyan: #4dd9d0; --purple: #b18cfe;
  --radius: 10px;
}
html { font-size: 15px; }
body {
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
  background: var(--bg); color: var(--text);
  padding: 20px 24px; line-height: 1.55; min-height: 100vh;
}

/* Header */
.header {
  display: flex; justify-content: space-between; align-items: center;
  padding: 16px 24px; background: var(--surface); border: 1px solid var(--border);
  border-radius: var(--radius); margin-bottom: 20px;
}
.header h1 { font-size: 1.3rem; font-weight: 700; color: var(--cyan); letter-spacing: -0.3px; }
.header-right { display: flex; align-items: center; gap: 16px; }
.live-badge {
  display: inline-flex; align-items: center; gap: 7px;
  padding: 5px 14px; border-radius: 20px; font-size: 0.75rem;
  background: var(--green-dim); color: var(--green); font-weight: 700;
  letter-spacing: 0.5px;
}
.live-dot {
  width: 9px; height: 9px; border-radius: 50%;
  background: var(--green); animation: pulse 2s ease-in-out infinite;
}
@keyframes pulse { 0%,100%{opacity:1;transform:scale(1)} 50%{opacity:0.3;transform:scale(0.85)} }
.disconnected .live-badge { background: var(--red-dim); color: var(--red); }
.disconnected .live-dot { background: var(--red); animation: none; }
.timestamp { color: var(--text-dim); font-size: 0.8rem; font-weight: 500; }

/* Price bar */
.price-bar {
  display: flex; gap: 10px; flex-wrap: wrap;
  margin-bottom: 20px;
}
.price-card {
  flex: 1; min-width: 140px;
  background: var(--surface); border: 1px solid var(--border);
  border-radius: var(--radius); padding: 14px 16px;
}
.price-symbol { font-size: 0.75rem; font-weight: 700; color: var(--text-dim); text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 4px; }
.price-value { font-size: 1.25rem; font-weight: 700; color: var(--text); }
.price-meta { font-size: 0.7rem; color: var(--text-dim); margin-top: 4px; }
.price-meta span { margin-right: 10px; }

/* Tabs */
.tabs {
  display: flex; gap: 4px; margin-bottom: 16px;
  border-bottom: 1px solid var(--border); padding-bottom: 0;
}
.tab {
  padding: 10px 20px; cursor: pointer;
  font-size: 0.85rem; font-weight: 600; color: var(--text-dim);
  border: none; background: none; border-bottom: 2px solid transparent;
  transition: all 0.15s;
}
.tab:hover { color: var(--text-secondary); }
.tab.active { color: var(--cyan); border-bottom-color: var(--cyan); }
.tab-count {
  font-size: 0.7rem; background: var(--border); border-radius: 8px;
  padding: 1px 7px; margin-left: 6px; font-weight: 500;
}
.tab-panel { display: none; }
.tab-panel.active { display: block; }

/* Leaderboard table */
.board { width: 100%; border-collapse: separate; border-spacing: 0; }
.board th {
  text-align: left; padding: 10px 14px; font-size: 0.75rem;
  color: var(--text-dim); font-weight: 600; text-transform: uppercase;
  letter-spacing: 0.4px; border-bottom: 1px solid var(--border);
  position: sticky; top: 0; background: var(--bg); z-index: 1;
}
.board td {
  padding: 12px 14px; font-size: 0.88rem;
  border-bottom: 1px solid rgba(37,48,64,0.5); vertical-align: top;
}
.board tr:hover td { background: rgba(77,217,208,0.03); }
.board .rank-cell { font-weight: 800; color: var(--text-dim); width: 40px; text-align: center; }
.board .agent-cell { font-weight: 700; }
.board .value-cell { font-weight: 600; font-variant-numeric: tabular-nums; }
.board .num { font-variant-numeric: tabular-nums; }
.pos { color: var(--green); }
.neg { color: var(--red); }
.dim { color: var(--text-dim); }

/* Agent detail (expandable row) */
.agent-detail {
  background: var(--surface); border-radius: var(--radius);
  padding: 16px; margin: 8px 0;
}
.detail-grid { display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 20px; }
.detail-section h3 {
  font-size: 0.75rem; font-weight: 700; color: var(--text-dim);
  text-transform: uppercase; letter-spacing: 0.4px; margin-bottom: 10px;
  padding-bottom: 6px; border-bottom: 1px solid var(--border);
}
.detail-row {
  display: flex; justify-content: space-between; padding: 4px 0;
  font-size: 0.82rem;
}
.detail-label { color: var(--text-secondary); }
.detail-value { font-weight: 600; font-variant-numeric: tabular-nums; }

/* Activity feed */
.activity-feed {
  background: var(--surface); border: 1px solid var(--border);
  border-radius: var(--radius); max-height: 600px; overflow-y: auto;
}
.activity-entry {
  padding: 10px 16px; border-bottom: 1px solid rgba(37,48,64,0.4);
  font-size: 0.82rem;
}
.activity-entry:hover { background: rgba(77,217,208,0.02); }
.activity-header { display: flex; align-items: center; gap: 10px; margin-bottom: 3px; }
.activity-time { color: var(--text-dim); font-size: 0.75rem; font-variant-numeric: tabular-nums; min-width: 60px; }
.activity-agent { font-weight: 700; color: var(--cyan); }
.activity-kind {
  font-size: 0.65rem; font-weight: 700; padding: 2px 8px;
  border-radius: 4px; text-transform: uppercase; letter-spacing: 0.5px;
}
.kind-TOOL_CALL { background: var(--yellow-dim); color: var(--yellow); }
.kind-RESPONSE { background: var(--green-dim); color: var(--green); }
.kind-TOOL_RESULT { background: var(--blue-dim); color: var(--blue); }
.activity-details {
  color: var(--text-secondary); font-size: 0.8rem; line-height: 1.5;
  padding-left: 70px; white-space: pre-wrap; word-break: break-word;
  font-family: 'SF Mono', 'Cascadia Code', Consolas, monospace;
}

/* Trade log */
.trade-table { width: 100%; border-collapse: separate; border-spacing: 0; }
.trade-table th {
  text-align: left; padding: 10px 12px; font-size: 0.72rem;
  color: var(--text-dim); font-weight: 600; text-transform: uppercase;
  letter-spacing: 0.3px; border-bottom: 1px solid var(--border);
  position: sticky; top: 0; background: var(--surface); z-index: 1;
}
.trade-table td { padding: 9px 12px; font-size: 0.82rem; border-bottom: 1px solid rgba(37,48,64,0.4); font-variant-numeric: tabular-nums; }
.trade-table tr:hover td { background: rgba(77,217,208,0.03); }
.trade-scroll {
  background: var(--surface); border: 1px solid var(--border);
  border-radius: var(--radius); max-height: 500px; overflow-y: auto;
}
.action-buy { color: var(--green); font-weight: 700; }
.action-sell { color: var(--red); font-weight: 700; }

/* Chart area */
.chart-area {
  background: var(--surface); border: 1px solid var(--border);
  border-radius: var(--radius); padding: 20px; min-height: 380px;
  position: relative;
}
.chart-controls {
  display: flex; justify-content: space-between; align-items: center;
  margin-bottom: 14px;
}
.chart-range-btns { display: flex; gap: 4px; }
.range-btn {
  padding: 5px 14px; border: 1px solid var(--border); border-radius: 6px;
  background: none; color: var(--text-secondary); font-size: 0.78rem;
  font-weight: 600; cursor: pointer; transition: all 0.15s;
  font-family: inherit;
}
.range-btn:hover { border-color: var(--text-dim); color: var(--text); }
.range-btn.active { background: var(--cyan); color: var(--bg); border-color: var(--cyan); }
.chart-stats { display: flex; gap: 16px; font-size: 0.78rem; color: var(--text-secondary); }
.chart-stats .stat-hi { color: var(--green); font-weight: 600; }
.chart-stats .stat-lo { color: var(--red); font-weight: 600; }
.chart-canvas { width: 100%; height: 340px; }

/* Management panel */
.mgmt-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }
@media (max-width: 900px) { .mgmt-grid { grid-template-columns: 1fr; } }
.mgmt-section {
  background: var(--surface); border: 1px solid var(--border);
  border-radius: var(--radius); padding: 18px;
}
.mgmt-section h3 {
  font-size: 0.82rem; font-weight: 700; color: var(--cyan);
  text-transform: uppercase; letter-spacing: 0.4px;
  margin-bottom: 14px; padding-bottom: 8px; border-bottom: 1px solid var(--border);
}
.mgmt-row { display: flex; gap: 10px; align-items: center; margin-bottom: 10px; flex-wrap: wrap; }
.mgmt-row label { font-size: 0.82rem; color: var(--text-secondary); min-width: 80px; }
.mgmt-select, .mgmt-input {
  padding: 7px 12px; border: 1px solid var(--border); border-radius: 6px;
  background: var(--bg); color: var(--text); font-size: 0.82rem;
  font-family: inherit; outline: none; transition: border-color 0.15s;
}
.mgmt-select:focus, .mgmt-input:focus { border-color: var(--cyan); }
.mgmt-select { min-width: 160px; }
.mgmt-input { width: 120px; }
.mgmt-input.wide { width: 200px; }
.mgmt-btn {
  padding: 7px 16px; border: none; border-radius: 6px;
  font-size: 0.8rem; font-weight: 600; cursor: pointer;
  font-family: inherit; transition: all 0.15s;
}
.mgmt-btn:active { transform: scale(0.97); }
.btn-primary { background: var(--cyan); color: var(--bg); }
.btn-primary:hover { background: #3bc4bc; }
.btn-danger { background: var(--red); color: #fff; }
.btn-danger:hover { background: #e04848; }
.btn-warning { background: var(--yellow); color: var(--bg); }
.btn-warning:hover { background: #e6af06; }
.btn-secondary { background: var(--border); color: var(--text); }
.btn-secondary:hover { background: var(--border-light); }
.mgmt-toast {
  position: fixed; bottom: 24px; right: 24px; z-index: 1000;
  padding: 12px 20px; border-radius: 8px; font-size: 0.85rem;
  font-weight: 600; animation: slideIn 0.3s ease-out;
  max-width: 500px; word-break: break-word;
}
.toast-success { background: var(--green-dim); color: var(--green); border: 1px solid rgba(0,214,143,0.3); }
.toast-error { background: var(--red-dim); color: var(--red); border: 1px solid rgba(255,92,92,0.3); }
@keyframes slideIn { from { transform: translateY(20px); opacity: 0; } to { transform: translateY(0); opacity: 1; } }
.mgmt-log {
  max-height: 200px; overflow-y: auto; margin-top: 12px;
  background: var(--bg); border-radius: 6px; padding: 10px;
  font-size: 0.78rem; font-family: 'SF Mono', Consolas, monospace;
}
.mgmt-log-entry { padding: 3px 0; border-bottom: 1px solid rgba(37,48,64,0.3); color: var(--text-secondary); }
.mgmt-log-entry .log-time { color: var(--text-dim); margin-right: 8px; }
.mgmt-log-entry.log-ok .log-msg { color: var(--green); }
.mgmt-log-entry.log-err .log-msg { color: var(--red); }

/* Utility */
.no-data { text-align: center; padding: 40px 20px; color: var(--text-dim); font-size: 0.9rem; }
.scroll-wrap { max-height: 600px; overflow-y: auto; }

/* Health indicator */
.health-badge {
  display: inline-flex; align-items: center; gap: 7px;
  padding: 5px 14px; border-radius: 20px; font-size: 0.75rem;
  font-weight: 700; letter-spacing: 0.5px; margin-right: 4px;
}
.health-HEALTHY { background: var(--green-dim); color: var(--green); }
.health-HEALTHY .health-dot { background: var(--green); }
.health-WARNING { background: var(--yellow-dim); color: var(--yellow); }
.health-WARNING .health-dot { background: var(--yellow); animation: pulse 1s ease-in-out infinite; }
.health-ERROR { background: var(--red-dim); color: var(--red); }
.health-ERROR .health-dot { background: var(--red); animation: pulse 0.5s ease-in-out infinite; }
.health-dot { width: 9px; height: 9px; border-radius: 50%; }

/* Health warning banner */
.health-banner {
  display: none; padding: 12px 20px; border-radius: var(--radius);
  margin-bottom: 16px; font-size: 0.85rem; font-weight: 600;
  align-items: center; gap: 10px;
}
.health-banner.show { display: flex; }
.health-banner.banner-WARNING {
  background: var(--yellow-dim); color: var(--yellow);
  border: 1px solid rgba(255,193,7,0.3);
}
.health-banner.banner-ERROR {
  background: var(--red-dim); color: var(--red);
  border: 1px solid rgba(255,92,92,0.3);
}
.health-banner .banner-detail { font-weight: 400; font-size: 0.8rem; margin-left: auto; opacity: 0.8; }

/* Health metrics in management tab */
.health-metrics { display: grid; grid-template-columns: repeat(auto-fit, minmax(140px, 1fr)); gap: 10px; }
.health-metric {
  background: var(--bg); border-radius: 8px; padding: 12px; text-align: center;
}
.health-metric .metric-value { font-size: 1.3rem; font-weight: 700; font-variant-numeric: tabular-nums; }
.health-metric .metric-label { font-size: 0.72rem; color: var(--text-dim); text-transform: uppercase; letter-spacing: 0.3px; margin-top: 4px; }
</style>
</head>
<body>

<div class="header" id="header">
  <h1>Crypto Daytrading Arena</h1>
  <div class="header-right">
    <span class="health-badge health-HEALTHY" id="health-badge"><span class="health-dot"></span><span id="health-text">LLM OK</span></span>
    <span class="live-badge"><span class="live-dot"></span><span id="status-text">CONNECTING</span></span>
    <span class="timestamp" id="timestamp"></span>
  </div>
</div>

<div class="price-bar" id="price-bar"></div>

<div class="health-banner" id="health-banner">
  <span>&#9888;</span>
  <span id="health-banner-msg">LLM API may be unresponsive</span>
  <span class="banner-detail" id="health-banner-detail"></span>
</div>

<div class="tabs" id="tabs">
  <button class="tab active" data-tab="leaderboard">Leaderboard</button>
  <button class="tab" data-tab="activity">Agent Activity <span class="tab-count" id="activity-count">0</span></button>
  <button class="tab" data-tab="trades">Trade Log <span class="tab-count" id="trade-count">0</span></button>
  <button class="tab" data-tab="chart">Portfolio Chart</button>
  <button class="tab" data-tab="manage">Management</button>
</div>

<div class="tab-panel active" id="panel-leaderboard">
  <div class="scroll-wrap">
    <table class="board" id="board">
      <thead>
        <tr>
          <th>#</th><th>Agent</th><th>Total Value</th><th>P&L</th><th>Return</th>
          <th>Cash</th><th>Win Rate</th><th>Trades</th><th>Fees</th>
          <th>Drawdown</th><th>Positions</th>
        </tr>
      </thead>
      <tbody id="board-body">
        <tr><td colspan="11" class="no-data">Waiting for agents...</td></tr>
      </tbody>
    </table>
  </div>
  <div id="agent-details"></div>
</div>

<div class="tab-panel" id="panel-activity">
  <div class="activity-feed" id="activity-feed">
    <div class="no-data">Waiting for agent activity...</div>
  </div>
</div>

<div class="tab-panel" id="panel-trades">
  <div class="trade-scroll">
    <table class="trade-table">
      <thead>
        <tr>
          <th>Time</th><th>Agent</th><th>Action</th><th>Product</th>
          <th>Quantity</th><th>Price</th><th>Total</th><th>Fee</th><th>Latency</th>
        </tr>
      </thead>
      <tbody id="trade-body">
        <tr><td colspan="9" class="no-data">No trades yet</td></tr>
      </tbody>
    </table>
  </div>
</div>

<div class="tab-panel" id="panel-chart">
  <div class="chart-area">
    <div class="chart-controls">
      <div class="chart-range-btns">
        <button class="range-btn" data-range="300" title="Last 5 minutes">5m</button>
        <button class="range-btn" data-range="900" title="Last 15 minutes">15m</button>
        <button class="range-btn" data-range="1800" title="Last 30 minutes">30m</button>
        <button class="range-btn active" data-range="3600" title="Last 1 hour">1h</button>
        <button class="range-btn" data-range="21600" title="Last 6 hours">6h</button>
        <button class="range-btn" data-range="86400" title="Last 24 hours">1d</button>
        <button class="range-btn" data-range="0" title="All available data">All</button>
      </div>
      <div class="chart-stats" id="chart-stats"></div>
    </div>
    <canvas class="chart-canvas" id="chart-canvas"></canvas>
  </div>
</div>

<div class="tab-panel" id="panel-manage">
  <div class="mgmt-grid">
    <div class="mgmt-section" style="grid-column: 1 / -1;">
      <h3>LLM API Health</h3>
      <div id="health-status-line" style="margin-bottom:12px; font-size:0.9rem;"></div>
      <div class="health-metrics" id="health-metrics"></div>
    </div>

    <div class="mgmt-section">
      <h3>Agent Account Controls</h3>
      <div class="mgmt-row">
        <label>Agent</label>
        <select class="mgmt-select" id="mgmt-agent"></select>
      </div>
      <div class="mgmt-row">
        <button class="mgmt-btn btn-danger" onclick="mgmtAction('reset')">Reset Account</button>
        <button class="mgmt-btn btn-warning" onclick="mgmtAction('liquidate')">Liquidate All</button>
        <button class="mgmt-btn btn-secondary" onclick="mgmtAction('cancel_orders')">Cancel Orders</button>
      </div>
      <div class="mgmt-row" style="margin-top:12px">
        <label>Adjust Cash</label>
        <input class="mgmt-input" type="number" id="mgmt-cash-amount" placeholder="Amount" step="any">
        <button class="mgmt-btn btn-primary" onclick="mgmtAction('adjust_cash')">Apply</button>
      </div>
    </div>

    <div class="mgmt-section">
      <h3>Manual Trade</h3>
      <div class="mgmt-row">
        <label>Agent</label>
        <select class="mgmt-select" id="trade-agent"></select>
      </div>
      <div class="mgmt-row">
        <label>Product</label>
        <select class="mgmt-select" id="trade-product"></select>
      </div>
      <div class="mgmt-row">
        <label>Action</label>
        <select class="mgmt-select" id="trade-action">
          <option value="buy">BUY</option>
          <option value="sell">SELL</option>
        </select>
      </div>
      <div class="mgmt-row">
        <label>Quantity</label>
        <input class="mgmt-input" type="number" id="trade-qty" placeholder="0.0" step="any" min="0">
      </div>
      <div class="mgmt-row">
        <button class="mgmt-btn btn-primary" onclick="mgmtTrade()">Execute Trade</button>
      </div>
    </div>

    <div class="mgmt-section">
      <h3>Arena Settings</h3>
      <div class="mgmt-row">
        <label>Fee Rate</label>
        <input class="mgmt-input" type="number" id="mgmt-fee-rate" step="0.0001" min="0">
        <span class="dim" id="mgmt-fee-pct"></span>
        <button class="mgmt-btn btn-primary" onclick="mgmtSettings('fee_rate')">Update</button>
      </div>
      <div class="mgmt-row">
        <label>Tax Rate</label>
        <input class="mgmt-input" type="number" id="mgmt-tax-rate" step="0.01" min="0" max="1">
        <span class="dim" id="mgmt-tax-pct"></span>
        <button class="mgmt-btn btn-primary" onclick="mgmtSettings('tax_rate')">Update</button>
      </div>
      <div class="mgmt-row" style="margin-top:12px">
        <button class="mgmt-btn btn-secondary" onclick="mgmtAction('save_checkpoint')">Save Checkpoint</button>
        <button class="mgmt-btn btn-danger" onclick="if(confirm('Reset ALL agents?')) mgmtAction('reset_all')">Reset All Agents</button>
      </div>
    </div>

    <div class="mgmt-section">
      <h3>Action Log</h3>
      <div class="mgmt-log" id="mgmt-log">
        <div class="mgmt-log-entry"><span class="log-time">--:--:--</span><span class="log-msg dim">Management panel ready</span></div>
      </div>
    </div>
  </div>
</div>

<script>
const $ = id => document.getElementById(id);
let currentTab = 'leaderboard';
let selectedAgent = null;
let latestState = null;

// Tabs
document.querySelectorAll('.tab').forEach(btn => {
  btn.addEventListener('click', () => {
    document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
    document.querySelectorAll('.tab-panel').forEach(p => p.classList.remove('active'));
    btn.classList.add('active');
    const tab = btn.dataset.tab;
    $('panel-' + tab).classList.add('active');
    currentTab = tab;
    if (tab === 'chart' && latestState) drawChart(latestState.balance_history, latestState.initial_cash);
  });
});

function fmt(n, dec=2) {
  if (n == null) return '--';
  return n.toLocaleString('en-US', {minimumFractionDigits: dec, maximumFractionDigits: dec});
}
function fmtPrice(n) {
  if (!n || n === 0) return '$0';
  const dec = Math.max(2, 4 - Math.floor(Math.log10(Math.abs(n))) - 1);
  return '$' + n.toLocaleString('en-US', {minimumFractionDigits: dec, maximumFractionDigits: dec});
}
function fmtPnl(v) {
  const cls = v >= 0 ? 'pos' : 'neg';
  const sign = v >= 0 ? '+' : '';
  return `<span class="${cls}">${sign}$${fmt(Math.abs(v))}</span>`;
}
function fmtPct(v) {
  const cls = v >= 0 ? 'pos' : 'neg';
  const sign = v >= 0 ? '+' : '';
  return `<span class="${cls}">${sign}${fmt(v)}%</span>`;
}
function fmtDuration(secs) {
  if (!secs || secs <= 0) return '--';
  if (secs < 60) return Math.round(secs) + 's';
  if (secs < 3600) return Math.round(secs/60) + 'm';
  return (secs/3600).toFixed(1) + 'h';
}

// Prices
function renderPrices(prices) {
  const bar = $('price-bar');
  if (!prices || Object.keys(prices).length === 0) {
    bar.innerHTML = '<div class="no-data" style="width:100%">Waiting for price data...</div>';
    return;
  }
  bar.innerHTML = Object.entries(prices)
    .sort((a,b) => b[1].volume_24h - a[1].volume_24h)
    .map(([pid, p]) => `
      <div class="price-card">
        <div class="price-symbol">${pid.replace('-USD','')}</div>
        <div class="price-value">${fmtPrice(p.price)}</div>
        <div class="price-meta">
          <span>Spread: ${fmtPrice(p.spread)} (${p.spread_pct.toFixed(3)}%)</span>
          <span>Vol: ${p.volume_24h >= 1e6 ? (p.volume_24h/1e6).toFixed(1)+'M' : fmt(p.volume_24h,0)}</span>
        </div>
      </div>
    `).join('');
}

// Leaderboard
function renderLeaderboard(agents, initialCash) {
  const tbody = $('board-body');
  if (!agents || agents.length === 0) {
    tbody.innerHTML = '<tr><td colspan="11" class="no-data">Waiting for agents...</td></tr>';
    return;
  }
  tbody.innerHTML = agents.map((a, i) => {
    const wr = (a.wins + a.losses) > 0 ? a.win_rate.toFixed(0) + '%' : '--';
    const posCount = a.positions.length + (a.orders.length > 0 ? ` +${a.orders.length} ord` : '');
    const cls = selectedAgent === a.agent_id ? 'style="background:var(--surface2)"' : '';
    return `
      <tr ${cls} onclick="toggleAgent('${a.agent_id}')" style="cursor:pointer">
        <td class="rank-cell">${i+1}</td>
        <td class="agent-cell">${a.agent_id}</td>
        <td class="value-cell">$${fmt(a.total_value)}</td>
        <td class="value-cell">${fmtPnl(a.total_pnl)}</td>
        <td class="num">${fmtPct(a.return_pct)}</td>
        <td class="num">$${fmt(a.cash)}</td>
        <td class="num">${wr} <span class="dim">(${a.wins}W/${a.losses}L)</span></td>
        <td class="num">${a.trade_count}</td>
        <td class="num dim">$${fmt(a.total_fees)}</td>
        <td class="num neg">${fmt(a.max_drawdown)}%</td>
        <td class="num">${posCount}</td>
      </tr>`;
  }).join('');
  renderAgentDetail(agents);
}

function toggleAgent(id) {
  selectedAgent = selectedAgent === id ? null : id;
  if (latestState) renderLeaderboard(latestState.agents, latestState.initial_cash);
}

function renderAgentDetail(agents) {
  const container = $('agent-details');
  if (!selectedAgent) { container.innerHTML = ''; return; }
  const a = agents.find(x => x.agent_id === selectedAgent);
  if (!a) { container.innerHTML = ''; return; }

  let posRows = a.positions.map(p => `
    <div class="detail-row">
      <span class="detail-label">${p.product_id.replace('-USD','')}</span>
      <span class="detail-value">
        ${fmt(p.quantity,4)} @ ${fmtPrice(p.avg_cost)}
        &rarr; ${fmtPrice(p.market_price)}
        ${fmtPnl(p.unrealized_pnl)} (${fmtPct(p.unrealized_pnl_pct)})
        <span class="dim">${fmtDuration(p.hold_seconds)} held</span>
      </span>
    </div>`).join('');
  if (!posRows) posRows = '<div class="detail-row dim">No open positions</div>';

  let ordRows = a.orders.map(o => `
    <div class="detail-row">
      <span class="detail-label">${o.order_id} ${o.action.toUpperCase()} ${o.product_id.replace('-USD','')}</span>
      <span class="detail-value">${fmt(o.quantity,4)} @ ${fmtPrice(o.limit_price)} <span class="dim">${fmtDuration(o.age_seconds)} ago</span></span>
    </div>`).join('');
  if (!ordRows) ordRows = '<div class="detail-row dim">No pending orders</div>';

  container.innerHTML = `
    <div class="agent-detail">
      <div class="detail-grid">
        <div class="detail-section">
          <h3>Performance</h3>
          <div class="detail-row"><span class="detail-label">Realized P&L</span><span class="detail-value">${fmtPnl(a.realized_pnl)}</span></div>
          <div class="detail-row"><span class="detail-label">Unrealized P&L</span><span class="detail-value">${fmtPnl(a.total_pnl - a.realized_pnl)}</span></div>
          <div class="detail-row"><span class="detail-label">Est. Tax Owed</span><span class="detail-value neg">$${fmt(a.estimated_tax)}</span></div>
          <div class="detail-row"><span class="detail-label">After-Tax P&L</span><span class="detail-value">${fmtPnl(a.after_tax_pnl)}</span></div>
          <div class="detail-row"><span class="detail-label">Total Fees Paid</span><span class="detail-value dim">$${fmt(a.total_fees)}</span></div>
          <div class="detail-row"><span class="detail-label">Max Drawdown</span><span class="detail-value neg">${fmt(a.max_drawdown)}%</span></div>
          <div class="detail-row"><span class="detail-label">Consec. Losses</span><span class="detail-value">${a.consecutive_losses}</span></div>
        </div>
        <div class="detail-section">
          <h3>Open Positions</h3>
          ${posRows}
        </div>
        <div class="detail-section">
          <h3>Pending Orders</h3>
          ${ordRows}
        </div>
      </div>
    </div>`;
}

// Activity feed
function renderActivity(activity) {
  $('activity-count').textContent = activity ? activity.length : 0;
  const feed = $('activity-feed');
  if (!activity || activity.length === 0) {
    feed.innerHTML = '<div class="no-data">Waiting for agent activity...<br><span style="font-size:0.8rem;margin-top:8px;display:inline-block">Agent reasoning, tool calls, and results will appear here as agents make decisions.</span></div>';
    return;
  }
  feed.innerHTML = activity.map(e => `
    <div class="activity-entry">
      <div class="activity-header">
        <span class="activity-time">${e.timestamp}</span>
        <span class="activity-agent">${e.agent_name}</span>
        <span class="activity-kind kind-${e.kind}">${e.kind.replace('_',' ')}</span>
      </div>
      <div class="activity-details">${escapeHtml(e.details)}</div>
    </div>
  `).join('');
}

function escapeHtml(s) {
  const d = document.createElement('div');
  d.textContent = s;
  return d.innerHTML;
}

// Trade log
function renderTrades(trades) {
  $('trade-count').textContent = trades ? trades.length : 0;
  const tbody = $('trade-body');
  if (!trades || trades.length === 0) {
    tbody.innerHTML = '<tr><td colspan="9" class="no-data">No trades yet</td></tr>';
    return;
  }
  tbody.innerHTML = trades.map(t => `
    <tr>
      <td>${t.timestamp}</td>
      <td style="font-weight:600">${t.agent_id}</td>
      <td class="action-${t.action}">${t.action.toUpperCase()}</td>
      <td>${t.product_id}</td>
      <td class="num">${fmt(t.quantity, 4)}</td>
      <td class="num">${fmtPrice(t.price)}</td>
      <td class="num">$${fmt(t.total)}</td>
      <td class="num dim">$${fmt(t.fee)}</td>
      <td class="num dim">${t.latency != null ? t.latency.toFixed(1)+'s' : '--'}</td>
    </tr>
  `).join('');
}

// Chart
const COLORS = ['#4dd9d0','#5b9cf6','#00d68f','#ff5c5c','#ffc107','#b18cfe','#f97316','#ec4899','#8b5cf6','#06b6d4','#84cc16','#f43f5e'];
let chartRange = 3600; // default 1h in seconds, 0 = all

// Range button handlers
document.querySelectorAll('.range-btn').forEach(btn => {
  btn.addEventListener('click', () => {
    document.querySelectorAll('.range-btn').forEach(b => b.classList.remove('active'));
    btn.classList.add('active');
    chartRange = parseInt(btn.dataset.range);
    if (latestState) drawChart(latestState.balance_history, latestState.initial_cash);
  });
});

function fmtTime(unixTs) {
  const d = new Date(unixTs * 1000);
  return d.toLocaleTimeString('en-US', {hour:'2-digit', minute:'2-digit', second:'2-digit', hour12:false});
}

function fmtTimeShort(unixTs) {
  const d = new Date(unixTs * 1000);
  if (chartRange > 21600) {
    return d.toLocaleTimeString('en-US', {hour:'2-digit', minute:'2-digit', hour12:false});
  }
  return d.toLocaleTimeString('en-US', {hour:'2-digit', minute:'2-digit', second:'2-digit', hour12:false});
}

function drawChart(history, initialCash) {
  const canvas = $('chart-canvas');
  if (!canvas) return;
  const ctx = canvas.getContext('2d');
  const dpr = window.devicePixelRatio || 1;
  const rect = canvas.parentElement.getBoundingClientRect();
  const CH = 340;
  canvas.width = rect.width * dpr;
  canvas.height = CH * dpr;
  canvas.style.width = rect.width + 'px';
  canvas.style.height = CH + 'px';
  ctx.scale(dpr, dpr);
  const W = rect.width, H = CH;
  ctx.clearRect(0, 0, W, H);

  if (!history || Object.keys(history).length === 0) {
    ctx.fillStyle = '#6b7a8a'; ctx.font = '15px sans-serif'; ctx.textAlign = 'center';
    ctx.fillText('Waiting for balance history...', W/2, H/2);
    $('chart-stats').innerHTML = '';
    return;
  }

  // Filter data by time range
  const nowTs = Date.now() / 1000;
  const cutoff = chartRange > 0 ? nowTs - chartRange : 0;
  const filtered = {};
  let hasData = false;
  for (const [aid, series] of Object.entries(history)) {
    const f = series.filter(([ts]) => ts >= cutoff);
    if (f.length > 0) { filtered[aid] = f; hasData = true; }
  }
  if (!hasData) {
    ctx.fillStyle = '#6b7a8a'; ctx.font = '14px sans-serif'; ctx.textAlign = 'center';
    ctx.fillText('No data in selected range', W/2, H/2);
    $('chart-stats').innerHTML = '';
    return;
  }

  const agents = Object.entries(filtered);
  const pad = {t:24, r:90, b:50, l:75};
  const cW = W - pad.l - pad.r, cH = H - pad.t - pad.b;

  // Find global time range and value range
  let tMin = Infinity, tMax = -Infinity;
  let allMin = Infinity, allMax = -Infinity;
  agents.forEach(([,s]) => s.forEach(([t,v]) => {
    tMin = Math.min(tMin, t); tMax = Math.max(tMax, t);
    allMin = Math.min(allMin, v); allMax = Math.max(allMax, v);
  }));

  // Include initial cash in range if close
  allMin = Math.min(allMin, initialCash);
  allMax = Math.max(allMax, initialCash);
  const yRange = allMax - allMin || 1;
  allMin -= yRange * 0.06; allMax += yRange * 0.06;
  const tRange = tMax - tMin || 1;

  function xPos(t) { return pad.l + cW * (t - tMin) / tRange; }
  function yPos(v) { return pad.t + cH * (1 - (v - allMin) / (allMax - allMin)); }

  // Background fill
  ctx.fillStyle = '#0e1419';
  ctx.fillRect(pad.l, pad.t, cW, cH);

  // Horizontal grid lines + Y labels
  ctx.strokeStyle = '#1e2a36'; ctx.lineWidth = 0.5;
  const yTicks = 5;
  for (let i = 0; i <= yTicks; i++) {
    const y = pad.t + (cH * i / yTicks);
    ctx.beginPath(); ctx.moveTo(pad.l, y); ctx.lineTo(pad.l + cW, y); ctx.stroke();
    const val = allMax - (allMax - allMin) * i / yTicks;
    ctx.fillStyle = '#6b7a8a'; ctx.font = '12px -apple-system, sans-serif'; ctx.textAlign = 'right';
    ctx.fillText('$' + val.toLocaleString('en-US', {maximumFractionDigits: 0}), pad.l - 10, y + 4);
  }

  // Vertical grid lines + time labels
  const numTimeTicks = Math.min(8, Math.max(3, Math.floor(cW / 100)));
  ctx.strokeStyle = '#1e2a36'; ctx.lineWidth = 0.5;
  for (let i = 0; i <= numTimeTicks; i++) {
    const t = tMin + tRange * i / numTimeTicks;
    const x = xPos(t);
    ctx.beginPath(); ctx.moveTo(x, pad.t); ctx.lineTo(x, pad.t + cH); ctx.stroke();
    ctx.fillStyle = '#6b7a8a'; ctx.font = '11px -apple-system, sans-serif'; ctx.textAlign = 'center';
    ctx.fillText(fmtTimeShort(t), x, H - pad.b + 18);
  }

  // Initial cash reference line
  const iy = yPos(initialCash);
  if (iy > pad.t && iy < pad.t + cH) {
    ctx.setLineDash([6,4]); ctx.strokeStyle = 'rgba(107,122,138,0.35)'; ctx.lineWidth = 1;
    ctx.beginPath(); ctx.moveTo(pad.l, iy); ctx.lineTo(pad.l + cW, iy); ctx.stroke();
    ctx.setLineDash([]);
    ctx.fillStyle = '#6b7a8a'; ctx.font = '11px -apple-system, sans-serif'; ctx.textAlign = 'left';
    ctx.fillText('$' + initialCash.toLocaleString('en-US', {maximumFractionDigits:0}) + ' start', pad.l + cW + 6, iy + 4);
  }

  // Draw lines + area fill + end labels
  agents.forEach(([name, series], ci) => {
    if (series.length < 2) return;
    const color = COLORS[ci % COLORS.length];

    // Area fill (subtle gradient)
    ctx.beginPath();
    ctx.moveTo(xPos(series[0][0]), yPos(series[0][1]));
    series.forEach(([t,v]) => ctx.lineTo(xPos(t), yPos(v)));
    ctx.lineTo(xPos(series[series.length-1][0]), pad.t + cH);
    ctx.lineTo(xPos(series[0][0]), pad.t + cH);
    ctx.closePath();
    const grad = ctx.createLinearGradient(0, pad.t, 0, pad.t + cH);
    grad.addColorStop(0, color + '18');
    grad.addColorStop(1, color + '02');
    ctx.fillStyle = grad;
    ctx.fill();

    // Line
    ctx.strokeStyle = color; ctx.lineWidth = 2; ctx.lineJoin = 'round'; ctx.lineCap = 'round';
    ctx.beginPath();
    series.forEach(([t,v], i) => {
      const x = xPos(t), y = yPos(v);
      i === 0 ? ctx.moveTo(x, y) : ctx.lineTo(x, y);
    });
    ctx.stroke();

    // End dot
    const lastPt = series[series.length - 1];
    const ex = xPos(lastPt[0]), ey = yPos(lastPt[1]);
    ctx.beginPath(); ctx.arc(ex, ey, 3.5, 0, Math.PI * 2); ctx.fillStyle = color; ctx.fill();
    ctx.beginPath(); ctx.arc(ex, ey, 6, 0, Math.PI * 2);
    ctx.fillStyle = color + '30'; ctx.fill();

    // End label (right side)
    const lastVal = lastPt[1];
    const pnl = lastVal - initialCash;
    const pnlStr = (pnl >= 0 ? '+' : '') + '$' + Math.abs(pnl).toFixed(0);
    ctx.fillStyle = color; ctx.font = 'bold 11px -apple-system, sans-serif'; ctx.textAlign = 'left';
    ctx.fillText(name, pad.l + cW + 8, ey - 6);
    ctx.font = '11px -apple-system, sans-serif';
    ctx.fillText('$' + lastVal.toFixed(0) + ' (' + pnlStr + ')', pad.l + cW + 8, ey + 8);
  });

  // Legend bar at bottom
  ctx.textAlign = 'left';
  let lx = pad.l;
  const ly = H - 8;
  agents.forEach(([name], ci) => {
    const color = COLORS[ci % COLORS.length];
    ctx.fillStyle = color;
    ctx.beginPath(); ctx.arc(lx + 5, ly - 4, 4, 0, Math.PI*2); ctx.fill();
    ctx.fillStyle = '#9aa8b8'; ctx.font = '12px -apple-system, sans-serif';
    ctx.fillText(name, lx + 14, ly);
    lx += ctx.measureText(name).width + 30;
  });

  // Stats bar
  let globalHi = -Infinity, globalLo = Infinity, globalStart = 0, globalEnd = 0, agentCount = agents.length;
  agents.forEach(([,s]) => {
    s.forEach(([,v]) => { globalHi = Math.max(globalHi, v); globalLo = Math.min(globalLo, v); });
    if (s.length > 0) { globalStart += s[0][1]; globalEnd += s[s.length-1][1]; }
  });
  const avgStart = agentCount > 0 ? globalStart / agentCount : 0;
  const avgEnd = agentCount > 0 ? globalEnd / agentCount : 0;
  const avgChange = avgEnd - avgStart;
  const rangeLbl = chartRange === 0 ? 'All' : chartRange < 3600 ? (chartRange/60)+'m' : chartRange < 86400 ? (chartRange/3600)+'h' : (chartRange/86400)+'d';
  const dataPoints = agents.reduce((sum, [,s]) => sum + s.length, 0);
  $('chart-stats').innerHTML = `
    <span>High: <span class="stat-hi">$${fmt(globalHi)}</span></span>
    <span>Low: <span class="stat-lo">$${fmt(globalLo)}</span></span>
    <span>Avg Change: ${fmtPnl(avgChange)}</span>
    <span class="dim">${dataPoints} pts / ${rangeLbl}</span>
  `;
}

// ── Management panel ──

let mgmtInitialized = false;

function updateMgmtDropdowns(state) {
  if (!state) return;
  const agents = state.agents || [];
  const prices = state.prices || {};

  // Agent selectors
  ['mgmt-agent', 'trade-agent'].forEach(id => {
    const sel = $(id);
    const prev = sel.value;
    const opts = agents.map(a => `<option value="${a.agent_id}">${a.agent_id}</option>`).join('');
    if (sel.innerHTML !== opts) { sel.innerHTML = opts; if (prev) sel.value = prev; }
  });

  // Product selector
  const prodSel = $('trade-product');
  const prevProd = prodSel.value;
  const prodOpts = Object.keys(prices).sort().map(p => `<option value="${p}">${p}</option>`).join('');
  if (prodSel.innerHTML !== prodOpts) { prodSel.innerHTML = prodOpts; if (prevProd) prodSel.value = prevProd; }

  // Settings (only populate once)
  if (!mgmtInitialized && state.settings) {
    $('mgmt-fee-rate').value = state.settings.fee_rate;
    $('mgmt-fee-pct').textContent = (state.settings.fee_rate * 100).toFixed(2) + '%';
    $('mgmt-tax-rate').value = state.settings.tax_rate;
    $('mgmt-tax-pct').textContent = (state.settings.tax_rate * 100).toFixed(0) + '%';
    mgmtInitialized = true;
  }
}

$('mgmt-fee-rate').addEventListener('input', function() {
  $('mgmt-fee-pct').textContent = (parseFloat(this.value || 0) * 100).toFixed(2) + '%';
});
$('mgmt-tax-rate').addEventListener('input', function() {
  $('mgmt-tax-pct').textContent = (parseFloat(this.value || 0) * 100).toFixed(0) + '%';
});

function showToast(msg, ok) {
  const el = document.createElement('div');
  el.className = 'mgmt-toast ' + (ok ? 'toast-success' : 'toast-error');
  el.textContent = msg;
  document.body.appendChild(el);
  setTimeout(() => el.remove(), 4000);
}

function mgmtLog(msg, ok) {
  const log = $('mgmt-log');
  const ts = new Date().toLocaleTimeString('en-US', {hour12:false});
  const cls = ok === true ? 'log-ok' : ok === false ? 'log-err' : '';
  log.innerHTML = `<div class="mgmt-log-entry ${cls}"><span class="log-time">${ts}</span><span class="log-msg">${escapeHtml(msg)}</span></div>` + log.innerHTML;
}

async function mgmtAction(action) {
  const agentId = $('mgmt-agent').value;
  const body = { action };
  if (action === 'adjust_cash') {
    const amount = parseFloat($('mgmt-cash-amount').value);
    if (isNaN(amount) || amount === 0) { showToast('Enter a non-zero amount', false); return; }
    body.agent_id = agentId;
    body.amount = amount;
  } else if (action === 'save_checkpoint' || action === 'reset_all') {
    // No agent needed
  } else {
    if (!agentId) { showToast('Select an agent first', false); return; }
    body.agent_id = agentId;
  }

  try {
    const r = await fetch('/api/manage', { method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify(body) });
    const data = await r.json();
    showToast(data.message, data.success);
    mgmtLog(`${action}${body.agent_id ? ' ['+body.agent_id+']' : ''}: ${data.message}`, data.success);
  } catch(err) {
    showToast('Request failed: ' + err.message, false);
    mgmtLog('Error: ' + err.message, false);
  }
}

async function mgmtTrade() {
  const agent = $('trade-agent').value;
  const product = $('trade-product').value;
  const action = $('trade-action').value;
  const qty = parseFloat($('trade-qty').value);
  if (!agent) { showToast('Select an agent', false); return; }
  if (!product) { showToast('Select a product', false); return; }
  if (isNaN(qty) || qty <= 0) { showToast('Enter a valid quantity', false); return; }

  try {
    const r = await fetch('/api/trade', {
      method: 'POST',
      headers: {'Content-Type':'application/json'},
      body: JSON.stringify({ agent_id: agent, product_id: product, action, quantity: qty })
    });
    const data = await r.json();
    showToast(data.message, data.success);
    mgmtLog(`${action.toUpperCase()} ${qty} ${product} [${agent}]: ${data.message}`, data.success);
    if (data.success) $('trade-qty').value = '';
  } catch(err) {
    showToast('Trade failed: ' + err.message, false);
    mgmtLog('Trade error: ' + err.message, false);
  }
}

async function mgmtSettings(field) {
  const value = parseFloat($(field === 'fee_rate' ? 'mgmt-fee-rate' : 'mgmt-tax-rate').value);
  if (isNaN(value) || value < 0) { showToast('Enter a valid value', false); return; }
  try {
    const r = await fetch('/api/settings', {
      method: 'POST',
      headers: {'Content-Type':'application/json'},
      body: JSON.stringify({ [field]: value })
    });
    const data = await r.json();
    showToast(data.message, data.success);
    mgmtLog(`Settings ${field}=${value}: ${data.message}`, data.success);
  } catch(err) {
    showToast('Settings update failed: ' + err.message, false);
  }
}

// Health status
function renderHealth(health) {
  if (!health) return;
  const badge = $('health-badge');
  const banner = $('health-banner');
  const st = health.status;

  badge.className = 'health-badge health-' + st;
  const labels = { HEALTHY: 'LLM OK', WARNING: 'LLM SLOW', ERROR: 'LLM DOWN' };
  $('health-text').textContent = labels[st] || st;

  if (st === 'HEALTHY') {
    banner.classList.remove('show', 'banner-WARNING', 'banner-ERROR');
  } else {
    banner.className = 'health-banner show banner-' + st;
    const msgs = {
      WARNING: 'LLM API may be degraded \u2014 agents are receiving data but few responses detected',
      ERROR: 'LLM API appears DOWN \u2014 agents receive market data but get NO responses. Check API key, quota/billing, and model availability.'
    };
    $('health-banner-msg').textContent = msgs[st] || 'Health issue detected';
    const detail = health.last_response_ago != null
      ? 'Last response: ' + fmtDuration(health.last_response_ago) + ' ago'
      : 'No LLM responses recorded';
    $('health-banner-detail').textContent = detail + ' | ' + health.messages_received + ' msgs in ' + (health.window_seconds/60) + 'min window';
  }

  const statusLine = $('health-status-line');
  if (statusLine) {
    const color = st === 'HEALTHY' ? 'var(--green)' : st === 'WARNING' ? 'var(--yellow)' : 'var(--red)';
    statusLine.innerHTML = 'Status: <span style="color:' + color + ';font-weight:700">' + st + '</span>' +
      ' <span class="dim">(rolling ' + (health.window_seconds/60) + ' min window)</span>';
  }
  const metrics = $('health-metrics');
  if (metrics) {
    const pct = (health.response_rate * 100).toFixed(1);
    const lastResp = health.last_response_ago != null ? fmtDuration(health.last_response_ago) + ' ago' : 'Never';
    const lastEvt = health.last_event_ago != null ? fmtDuration(health.last_event_ago) + ' ago' : 'Never';
    metrics.innerHTML =
      '<div class="health-metric"><div class="metric-value">' + health.messages_received + '</div><div class="metric-label">Messages Received</div></div>' +
      '<div class="health-metric"><div class="metric-value">' + health.llm_responses + '</div><div class="metric-label">LLM Responses</div></div>' +
      '<div class="health-metric"><div class="metric-value">' + health.tool_calls + '</div><div class="metric-label">Tool Calls</div></div>' +
      '<div class="health-metric"><div class="metric-value">' + pct + '%</div><div class="metric-label">Response Rate</div></div>' +
      '<div class="health-metric"><div class="metric-value">' + lastResp + '</div><div class="metric-label">Last LLM Response</div></div>' +
      '<div class="health-metric"><div class="metric-value">' + lastEvt + '</div><div class="metric-label">Last Event</div></div>' +
      '<div class="health-metric"><div class="metric-value">' + health.total_events + '</div><div class="metric-label">Total Events</div></div>';
  }
}

// Main render
function render(state) {
  latestState = state;
  renderHealth(state.system_health);
  $('timestamp').textContent = state.timestamp || '';
  renderPrices(state.prices);
  renderLeaderboard(state.agents, state.initial_cash);
  renderActivity(state.activity);
  renderTrades(state.trades);
  if (currentTab === 'chart') drawChart(state.balance_history, state.initial_cash);
  updateMgmtDropdowns(state);
}

// WebSocket
let ws, reconnectDelay = 1000;
function connect() {
  const proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
  ws = new WebSocket(proto + '//' + location.host + '/ws');
  ws.onopen = () => {
    $('status-text').textContent = 'LIVE';
    $('header').classList.remove('disconnected');
    reconnectDelay = 1000;
  };
  ws.onmessage = e => { try { render(JSON.parse(e.data)); } catch(err) { console.error(err); } };
  ws.onclose = () => {
    $('status-text').textContent = 'DISCONNECTED';
    $('header').classList.add('disconnected');
    setTimeout(connect, reconnectDelay);
    reconnectDelay = Math.min(reconnectDelay * 1.5, 10000);
  };
  ws.onerror = () => ws.close();
}
connect();
window.addEventListener('resize', () => { if (currentTab === 'chart' && latestState) drawChart(latestState.balance_history, latestState.initial_cash); });
</script>
</body>
</html>"""


# ── FastAPI app factory ───────────────────────────────────────


def create_app(store, initial_cash: float, tax_rate: float) -> FastAPI:
    app = FastAPI(title="Crypto Daytrading Arena")
    manager = ConnectionManager()
    balance_history = BalanceHistory()

    @app.get("/", response_class=HTMLResponse)
    async def index():
        return HTML_TEMPLATE

    @app.get("/api/state")
    async def get_state():
        return _serialize_full_state(store, initial_cash, tax_rate, balance_history)

    @app.websocket("/ws")
    async def websocket_endpoint(ws: WebSocket):
        await manager.connect(ws)
        try:
            await ws.send_json(
                _serialize_full_state(store, initial_cash, tax_rate, balance_history)
            )
            while True:
                await ws.receive_text()
        except WebSocketDisconnect:
            manager.disconnect(ws)
        except Exception:
            manager.disconnect(ws)

    async def _broadcast_loop():
        while True:
            await asyncio.sleep(3)
            balance_history.sample(store)
            if manager._connections:
                state = _serialize_full_state(
                    store, initial_cash, tax_rate, balance_history
                )
                await manager.broadcast(state)

    @app.on_event("startup")
    async def startup():
        asyncio.create_task(_broadcast_loop())

    # ── Management API endpoints ──────────────────────────────

    from starlette.requests import Request

    @app.post("/api/manage")
    async def manage(request: Request):
        import trading_tools
        body = await request.json()
        action = body.get("action", "")
        agent_id = body.get("agent_id")
        amount = body.get("amount")

        if action == "reset" and agent_id:
            result = store.reset_agent(agent_id)
            return {"success": result.success, "message": result.message}

        elif action == "liquidate" and agent_id:
            result = store.liquidate_agent(agent_id)
            return {"success": result.success, "message": result.message}

        elif action == "cancel_orders" and agent_id:
            result = store.cancel_all_orders(agent_id)
            return {"success": result.success, "message": result.message}

        elif action == "adjust_cash" and agent_id and amount is not None:
            result = store.adjust_cash(agent_id, float(amount))
            return {"success": result.success, "message": result.message}

        elif action == "save_checkpoint":
            store.save_checkpoint()
            return {"success": True, "message": "Checkpoint saved."}

        elif action == "reset_all":
            agent_ids = list(store.accounts.keys())
            for aid in agent_ids:
                store.reset_agent(aid)
            return {"success": True, "message": f"Reset {len(agent_ids)} agent(s)."}

        return {"success": False, "message": f"Unknown action: {action}"}

    @app.post("/api/trade")
    async def manual_trade(request: Request):
        body = await request.json()
        result = store.execute_trade(
            agent_id=body["agent_id"],
            product_id=body["product_id"],
            quantity=float(body["quantity"]),
            action=body["action"],
        )
        return {"success": result.success, "message": result.message}

    @app.post("/api/settings")
    async def update_settings(request: Request):
        import trading_tools
        body = await request.json()
        msgs = []
        if "fee_rate" in body and body["fee_rate"] is not None:
            trading_tools.TRADE_FEE_RATE = float(body["fee_rate"])
            msgs.append(f"Fee rate set to {trading_tools.TRADE_FEE_RATE:.4%}")
        if "tax_rate" in body and body["tax_rate"] is not None:
            trading_tools.TAX_RATE = float(body["tax_rate"])
            msgs.append(f"Tax rate set to {trading_tools.TAX_RATE:.0%}")
        return {"success": True, "message": ". ".join(msgs) + "."}

    return app


# ── Launcher ──────────────────────────────────────────────────


async def start_web_dashboard(
    store,
    port: int = 8080,
    host: str = "0.0.0.0",
    initial_cash: float = 1_000.0,
    tax_rate: float = 0.30,
) -> asyncio.Task:
    """Start the web dashboard as a background asyncio task."""
    app = create_app(store, initial_cash, tax_rate)
    config = uvicorn.Config(app, host=host, port=port, log_level="warning")
    server = uvicorn.Server(config)
    task = asyncio.create_task(server.serve())
    logger.info("Web dashboard started at http://%s:%d", host, port)
    return task


if __name__ == "__main__":
    import argparse
    from coinbase_consumer import PriceBook
    from trading_tools import AccountStore

    parser = argparse.ArgumentParser(description="Standalone web dashboard (for testing)")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--host", type=str, default="0.0.0.0")
    args = parser.parse_args()

    pb = PriceBook()
    st = AccountStore(pb)
    app = create_app(st, initial_cash=1_000.0, tax_rate=0.30)
    uvicorn.run(app, host=args.host, port=args.port)
