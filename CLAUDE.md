# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Multi-agent crypto trading arena where AI agents compete by trading live Coinbase market data. Built on [Calfkit](https://github.com/calf-ai/calfkit-sdk) for agent orchestration and Kafka event streaming. Each component runs as an independent process communicating via a Kafka broker.

## Commands

```bash
# Install dependencies
uv sync

# Run any component
uv run python <component>.py --bootstrap-servers localhost:9092

# Full arena launch (macOS, requires arena.env with API keys)
./launch_arena.sh
./launch_arena.sh --teardown

# Start local Kafka broker (Docker required)
cd ~/calfkit-broker && make dev-up
```

### Component launch order (each in its own terminal):
1. Kafka broker (`make dev-up` in calfkit-broker)
2. `coinbase_connector.py --bootstrap-servers <url> [--interval 60]`
3. `tools_and_dashboard.py --bootstrap-servers <url> [--snapshot-interval 600] [--data-dir ./data] [--fee-rate 0.05]`
4. `deploy_chat_node.py --name <node-name> --model-id <model> --bootstrap-servers <url> --api-key <key> [--base-url <url>] [--reasoning-effort <level>]`
5. `deploy_router_node.py --name <agent-name> --chat-node-name <node-name> --strategy <default|momentum|brainrot|scalper> --bootstrap-servers <url>`
6. `response_viewer.py --bootstrap-servers <url>` (optional)

## Architecture

```
Coinbase WebSocket → coinbase_kafka_connector.py → Kafka → deploy_router_node.py (agents)
                                                      ↕              ↕
                                                tools_and_dashboard.py ← → deploy_chat_node.py (LLM)
```

**Key components and their roles:**

- **`coinbase_kafka_connector.py`** — Streams Coinbase ticker + candle data to Kafka. Buffers latest tick per product, publishes on interval. `coinbase_consumer.py` provides `PriceBook` (live prices) and `CandleBook` (multi-timeframe OHLCV) used by other components.
- **`deploy_chat_node.py`** — Stateless LLM inference server. Supports any OpenAI-compatible provider. Multiple agents can share one ChatNode.
- **`deploy_router_node.py`** — One per agent. Each has a named strategy (system prompt in `STRATEGIES` dict) and targets a specific ChatNode. Subscribes to market data with its own consumer group for independent fan-out.
- **`trading_tools.py`** — Core trading engine. `AccountStore` manages per-agent portfolios (cash, positions, cost basis). Provides `execute_trade()`, `get_portfolio()`, `calculator()` tool functions. Also contains `PortfolioView` (Rich Live dashboard) and `PlotextChart` for ASCII portfolio charts.
- **`tools_and_dashboard.py`** — Registers trading tools with Calfkit's `NodesService`, runs the price subscriber, dashboard, and optional CSV data recorder.
- **`data_recorder.py`** — CSV logger for trades and periodic portfolio snapshots. Files written to `data/` directory.
- **`response_viewer.py`** — Live Rich dashboard showing agent reasoning, tool calls, and results.

**Design patterns:**
- Agent identity resolved at runtime via `ToolContext` — single tool deployment serves all agents
- Agent accounts auto-created on first trade (no pre-registration)
- All I/O is async (asyncio, aiokafka, httpx, websockets)
- State is in-memory only (no database persistence)
- Pydantic models for all data structures (`TickerMessage`, `TradeRow`, `SnapshotRow`, etc.)

## Configuration Defaults

| Location | Constant | Default | Purpose |
|----------|----------|---------|---------|
| `trading_tools.py` | `INITIAL_CASH` | `100_000.0` | Starting cash per agent |
| `trading_tools.py` | `TRADE_FEE_RATE` | `0.05` (5%) | Per-trade fee rate |
| `coinbase_consumer.py` | `DEFAULT_PRODUCTS` | BTC-USD, FARTCOIN-USD, SOL-USD | Tracked products |
| `coinbase_kafka_connector.py` | `DEFAULT_MIN_INTERVAL` | `60` | Seconds between market data pushes |

## Environment Variables

- `KAFKA_BOOTSTRAP_SERVERS` — Broker address (default: `localhost:9092`)
- `OPENAI_API_KEY` / `ANTHROPIC_API_KEY` — LLM provider keys (set in `arena.env`)
- `TRADE_FEE_RATE` — Override default fee rate

## Dependencies

Python 3.10+ required. Managed with `uv` (pyproject.toml). Key framework: `calfkit` for agent orchestration and Kafka streaming, `rich` for terminal UI, `plotext` for ASCII charts, `pydantic` for data validation.
