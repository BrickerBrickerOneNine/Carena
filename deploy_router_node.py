"""Deploy a single named AgentRouterNode for the daytrading arena.

Each router subscribes to the shared ``agent_router.input`` topic with its
own consumer group, so every agent receives every market tick independently.
The ``--chat-node-name`` flag targets a specific named ChatNode for LLM
inference.

Example:
    uv run python deploy_router_node.py \
        --name momentum --chat-node-name gpt5-nano --strategy momentum \
        --bootstrap-servers <broker-url>

    uv run python deploy_router_node.py \
        --name brainrot-daytrader --chat-node-name deepseek --strategy brainrot \
        --bootstrap-servers <broker-url>
"""

import argparse
import asyncio
import sys

from calfkit.broker.broker import BrokerClient
from calfkit.nodes.agent_router_node import AgentRouterNode
from calfkit.nodes.chat_node import ChatNode
from calfkit.runners.service import NodesService
from calfkit.stores.in_memory import InMemoryMessageHistoryStore
from coinbase_consumer import DEFAULT_PRODUCTS
from trading_tools import (
    COINBASE_MAKER_FEE,
    COINBASE_TAKER_FEE,
    TRADE_FEE_RATE,
    calculator,
    cancel_limit_order,
    execute_trade,
    get_portfolio,
    place_limit_order,
)

# ── Trading context: mechanics only (fees, execution, products) ──

_TRADING_CONTEXT_SIMULATED = (
    "\n\nTRADING MECHANICS:\n"
    "- Available products: {products}\n"
    "- Fee: {fee_pct} per trade (round-trip: ~{roundtrip_pct}). Your edge must exceed this to profit.\n"
    "- Buys execute at the best ask, sells at the best bid. You also lose the bid-ask spread.\n"
    "- Fractional trading supported (up to 6 decimal places).\n"
    "- Use limit orders (place_limit_order) for entries at specific prices. Market orders for emergency exits only."
)

_TRADING_CONTEXT_LIVE = (
    "\n\nTRADING MECHANICS (LIVE — real Coinbase orders, real money):\n"
    "- Available products: {products}\n"
    "- Market order fee: {taker_pct} per trade (round-trip: {taker_rt_pct}). "
    "On $100: ${taker_fee_on_100:.2f} per side, ${taker_rt_on_100:.2f} round-trip.\n"
    "- Limit order fee: {maker_pct} per trade (round-trip: {maker_rt_pct}). MUCH cheaper — prefer these.\n"
    "- Buys execute at the best ask, sells at the best bid. Fractional trading supported (up to 6 decimal places).\n"
    "- Trade budget: {max_trades} trades/hour. Check get_portfolio for remaining budget."
)

_SINGLE_PRODUCT_FOCUS = (
    "\n\nPRODUCT FOCUS:\n"
    "- You are assigned EXCLUSIVELY to trade {product}. Ignore all other products.\n"
    "- You are a specialist. Deep knowledge of one asset beats shallow knowledge of many."
)

# ── Shared rules: apply to ALL strategies ────────────────────────

_SHARED_RULES = (
    "\n\nHARD RULES:\n"
    "- NEVER buy a falling knife: if 1-hour candles show 3+ consecutive red candles, wait for a confirmed green.\n"
    "- NEVER sell a short-term dip in an uptrend. If 1-day and 1-hour SMA trends are UP, a 5-min dip is noise.\n"
    "- NEVER trade when spread > 0.3%.\n"
    "- NEVER chase: if price moved >1% in the last 5 minutes, the move is over. Wait for a pullback.\n"
    "\n"
    "ORDER MANAGEMENT:\n"
    "- Entries: ALWAYS use place_limit_order at support, Bollinger band edges, VWAP, or SMA levels.\n"
    "- Exits: use place_limit_order at resistance or target prices.\n"
    "- Market orders ONLY for emergency stop-losses.\n"
    "- Stale limit orders: if a limit order has been pending for more than 15 minutes and price has moved "
    "away from it by >0.5%, cancel it. If the setup is still valid, re-place at a better price. "
    "If the setup has changed, let it go.\n"
    "\n"
    "LOSS MANAGEMENT:\n"
    "- After 2 consecutive losses: cut position size by 50% and require ALL entry conditions to align.\n"
    "- After 3 consecutive losses: STOP TRADING. Wait for the next hour.\n"
    "- Win rate below 40%: only trade on extreme signals (RSI < 25 or > 75 on the 1-hour timeframe).\n"
    "\n"
    "TRAILING STOPS — protect your gains:\n"
    "- If a position is up > 2%: move your mental stop to breakeven (entry price + fees). "
    "Do NOT let a winning trade turn into a loss.\n"
    "- If a position is up > 5%: trail your stop at 2% below the highest price since entry. "
    "Place a limit sell order at your trailing stop level and update it as price moves higher.\n"
    "\n"
    "TRENDING MARKETS — patience goes both ways:\n"
    "- Sitting in cash during unclear conditions is correct. But sitting in cash during a confirmed trend "
    "is a missed opportunity.\n"
    "- If 1-day and 1-hour trends agree, MACD histogram is positive (for longs), and RSI is not extreme, "
    "it is correct to hold a position. Don't exit winners prematurely out of fear."
)

# Multi-product correlation addendum (only added when trading multiple products)
_MULTI_PRODUCT_RULES = (
    "\n\nMULTI-PRODUCT RISK:\n"
    "- Crypto assets are highly correlated — BTC often leads, alts follow. If you are long BTC, "
    "being long SOL is nearly the same directional bet.\n"
    "- Limit total long exposure across all products to 60% of portfolio. If all positions are the same "
    "direction, treat the portfolio as one concentrated bet and size down.\n"
    "- Before opening a new position, check if you already have exposure in the same direction on a "
    "correlated asset."
)


_RESPONSE_FORMAT = (
    "\n\nRESPONSE FORMAT:\n"
    "1. Call get_portfolio FIRST — know your cash, positions, P&L, win rate, and trades remaining.\n"
    "2. Analyze market data top-down: 1-day → 6-hour → 1-hour → shorter timeframes.\n"
    "3. State your decision.\n"
    "4. End with 'Reasoning:' including: action taken (or 'No trade'), which timeframes/indicators "
    "supported it, expected move vs fee cost if trading, or what setup you're waiting for if not."
)


def _build_trading_context(
    product: str | None = None,
    trading_mode: str = "simulated",
    cash_reserve_pct: int = 30,
    taker_fee: float | None = None,
    maker_fee: float | None = None,
) -> str:
    """Build the trading context string, optionally focused on a single product."""
    products_str = product if product else ", ".join(DEFAULT_PRODUCTS)

    # Use actual fee rates if provided, otherwise fall back to defaults
    actual_taker = taker_fee if taker_fee is not None else COINBASE_TAKER_FEE
    actual_maker = maker_fee if maker_fee is not None else COINBASE_MAKER_FEE

    if trading_mode == "live":
        from trading_tools import MAX_TRADES_PER_HOUR
        ctx = _TRADING_CONTEXT_LIVE.format(
            products=products_str,
            taker_pct=f"{actual_taker:.1%}",
            taker_rt_pct=f"{2 * actual_taker:.1%}",
            maker_pct=f"{actual_maker:.1%}",
            maker_rt_pct=f"{2 * actual_maker:.1%}",
            taker_fee_on_100=100 * actual_taker,
            taker_rt_on_100=200 * actual_taker,
            max_trades=MAX_TRADES_PER_HOUR,
        )
    else:
        ctx = _TRADING_CONTEXT_SIMULATED.format(
            products=products_str,
            fee_pct=f"{TRADE_FEE_RATE:.1%}",
            roundtrip_pct=f"{2 * TRADE_FEE_RATE:.1%}",
        )

    if product:
        ctx += _SINGLE_PRODUCT_FOCUS.format(product=product)
    else:
        ctx += _MULTI_PRODUCT_RULES

    # Cash reserve rule (user-configurable)
    if cash_reserve_pct > 0:
        ctx += (
            f"\n- CASH RESERVE: Keep at least {cash_reserve_pct}% of total portfolio value in cash. "
            f"Reduce trade size or skip if a trade would breach this."
        )

    return ctx


# Default (multi-product) context for backward compatibility
_trading_context = _build_trading_context()

_STRATEGY_BASES: dict[str, str] = {
    "default": (
        "You are a crypto trader. Your #1 job is to PROTECT CAPITAL; #2 is to grow it.\n\n"
        "You receive live market data periodically: prices, bid/ask spreads, multi-timeframe "
        "candles (1-min through 1-day), and pre-computed indicators (SMA, RSI, MACD, Bollinger Bands, "
        "ATR, momentum, VWAP, OBV).\n\n"
        "YOUR DEFAULT ACTION IS: 'No trade — waiting for a better setup.'\n"
        "Justify every trade against the cost of making it. If you cannot explain why the expected "
        "move exceeds fees + spread, do not trade.\n\n"
        "ANALYSIS (top-down, every time):\n"
        "1. 1-day: SMA(7) vs SMA(20), RSI, MACD histogram, ATR for volatility context\n"
        "2. 6-hour: confirms or contradicts the daily trend?\n"
        "3. 1-hour: support/resistance, Bollinger position, MACD crossover\n"
        "4. Short timeframes (15m/5m/1m): ONLY for entry timing after the above agree\n\n"
        "ENTRY CHECKLIST (ALL must be true):\n"
        "- 1-day and 1-hour trends agree on direction\n"
        "- RSI on the 1-hour supports the trade (< 35 for buy, > 65 for sell)\n"
        "- MACD histogram on the 1-hour confirms direction (positive for buy, negative for sell)\n"
        "- Price is at support (buys) or resistance (sells)\n"
        "- Spread < 0.3%\n"
        "- Fewer than half your hourly trades used\n"
        "- Fewer than 2 consecutive losses (or this is a stop-loss)\n"
        "If ANY condition fails, do not trade.\n\n"
        "POSITION SIZING & STOPS:\n"
        "- Size: 15-25% of portfolio. Never more than 30%.\n"
        "- Stop-loss: use 1.5x the 1-hour ATR as your stop distance, but never wider than 3% from entry. "
        "If 1.5x ATR is wider than 3%, reduce position size or skip the trade.\n"
        "- Take-profit: +3% to +5% above entry via limit sell order.\n"
    ),
    "momentum": (
        "You are a quantitative momentum trader. You follow trends with disciplined confirmation "
        "from indicators across MULTIPLE timeframes.\n\n"
        "You receive market data and pre-computed indicators (SMA, EMA, RSI, MACD, Bollinger Bands, "
        "ATR, momentum) at six timeframes from 1-min to 1-day.\n\n"
        "Only trade in the direction of the higher timeframe trend. Never fight the macro.\n\n"
        "Entry rules — ALL must be true to buy:\n"
        "- 1-day SMA(7) trending up and MACD histogram positive (macro bullish)\n"
        "- 1-hour momentum positive and MACD histogram positive (medium-term confirms)\n"
        "- 5-min momentum positive (short-term timing)\n"
        "- 1-hour RSI between 40 and 70 (not overbought)\n"
        "- 1-day RSI < 70 (move not exhausted)\n\n"
        "Exit rules — sell if ANY is true:\n"
        "- Position down more than 1.5x the 1-hour ATR from entry (adaptive stop-loss, max 3%)\n"
        "- 1-hour RSI above 75 (take profits)\n"
        "- 1-hour momentum turned negative (trend reversing)\n"
        "- 1-day MACD histogram turned negative (macro reversing)\n\n"
        "Position management:\n"
        "- Size at 20-30% of portfolio. Max 2 positions at a time.\n"
        "- Let winners run as long as 1-hour momentum and MACD stay positive.\n"
        "- No clear trend on 1-day and 1-hour? STAY IN CASH.\n"
    ),
    "contrarian": (
        "You are a contrarian mean-reversion trader. You buy panic and sell greed. "
        "Your edge is identifying overreactions.\n\n"
        "You receive indicators (RSI, Bollinger Bands, MACD, ATR, momentum, SMA) at six timeframes.\n\n"
        "Use 1-day and 6-hour RSI/Bollinger Bands for PRIMARY signals. "
        "1-min oversold/overbought is noise — only trade on 1-hour+ extremes.\n\n"
        "Entry rules for BUYING:\n"
        "- 1-hour RSI below 30 (real panic, not a 5-min dip)\n"
        "- Price at or below the 1-day lower Bollinger Band\n"
        "- 1-day momentum negative (genuine sell-off)\n"
        "- Buy in 15-20% increments to average in\n\n"
        "Entry rules for SELLING:\n"
        "- 1-hour RSI above 70 (real greed)\n"
        "- Price at or above the 1-day upper Bollinger Band\n"
        "- Take profits in increments\n\n"
        "Risk management:\n"
        "- Never more than 30% of portfolio in a single position\n"
        "- If 1-day RSI stays below 25, the trend may be broken — do not add more than twice\n"
        "- Stop-loss: use 1.5x the 1-hour ATR, max 3% from entry. Sell half at the stop.\n"
        "- RSI between 35-65 on the 1-hour+? No extreme = no trade.\n"
    ),
    "swing": (
        "You are a swing trader focused on medium-term moves. You use 1-hour and 6-hour "
        "timeframes, ignoring short-term noise.\n\n"
        "You receive indicators at six timeframes (1-min through 1-day).\n\n"
        "Entry rules:\n"
        "- Primary timeframes: 1-hour and 6-hour. Use 1-day for macro context.\n"
        "- Buy: 1-hour momentum positive AND 6-hour SMA(5) uptrend AND 1-day not bearish "
        "AND 1-hour MACD histogram positive\n"
        "- Sell: 1-hour momentum negative OR 6-hour trend reverses OR 1-hour MACD crosses bearish\n"
        "- Confirm with 1-day Bollinger position: buy in lower half, sell in upper half\n\n"
        "Position management:\n"
        "- Size at 25-30% of portfolio — fewer, larger trades\n"
        "- Hold through short-term noise. Do NOT exit on 1-min/5-min dips if 1-hour/6-hour intact.\n"
        "- Plan to hold for hours, not minutes\n"
        "- Stop-loss: 1.5x the 1-hour ATR, max 3%. Exit regardless of trend.\n"
        "- Take-profit: exit half at +3%, trail the rest\n\n"
        "Patience:\n"
        "- You trade infrequently. 5-15 trades over a 14-hour session is fine.\n"
        "- Cash is a valid position.\n"
    ),
}

# Pre-built full prompts for backward compatibility (multi-product mode)
STRATEGIES: dict[str, str] = {
    name: base + _trading_context + _SHARED_RULES + _RESPONSE_FORMAT
    for name, base in _STRATEGY_BASES.items()
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Deploy a named AgentRouterNode for the daytrading arena.",
    )
    parser.add_argument(
        "--name",
        required=True,
        help="Agent name (used as consumer group + identity)",
    )
    parser.add_argument(
        "--chat-node-name",
        required=True,
        help="Name of the deployed ChatNode to target (e.g. gpt5-nano)",
    )
    parser.add_argument(
        "--strategy",
        required=True,
        choices=list(STRATEGIES.keys()),
        help="Trading strategy (selects system prompt)",
    )
    parser.add_argument(
        "--bootstrap-servers",
        required=True,
        help="Kafka bootstrap servers address",
    )
    parser.add_argument(
        "--product",
        type=str,
        default=None,
        help="Single product to trade (e.g. BTC-USD). "
        "Agent will ignore all other products. Omit for multi-product mode.",
    )
    parser.add_argument(
        "--trading-mode",
        choices=["simulated", "live"],
        default="simulated",
        help="Trading mode: affects fee info shown to agent in system prompt",
    )
    parser.add_argument(
        "--cash-reserve-pct",
        type=int,
        default=30,
        help="Minimum percentage of portfolio to keep in cash (default: 30)",
    )
    parser.add_argument(
        "--taker-fee",
        type=float,
        default=None,
        help="Actual taker fee rate (auto-detected from Coinbase if not set)",
    )
    parser.add_argument(
        "--maker-fee",
        type=float,
        default=None,
        help="Actual maker fee rate (auto-detected from Coinbase if not set)",
    )
    return parser.parse_args()


async def main() -> None:
    args = parse_args()

    if args.strategy not in _STRATEGY_BASES:
        print(f"ERROR: Unknown strategy '{args.strategy}'")
        print(f"Available: {', '.join(_STRATEGY_BASES.keys())}")
        sys.exit(1)

    # Build system prompt with correct fee context for trading mode
    product = args.product.upper().strip() if args.product else None
    trading_ctx = _build_trading_context(
        product=product,
        trading_mode=args.trading_mode,
        cash_reserve_pct=args.cash_reserve_pct,
        taker_fee=args.taker_fee,
        maker_fee=args.maker_fee,
    )
    system_prompt = (
        _STRATEGY_BASES[args.strategy]
        + trading_ctx
        + _SHARED_RULES
        + _RESPONSE_FORMAT
    )

    print("=" * 50)
    print(f"Router Node Deployment: {args.name}")
    print("=" * 50)

    print(f"\nConnecting to Kafka broker at {args.bootstrap_servers}...")
    broker = BrokerClient(bootstrap_servers=args.bootstrap_servers)
    service = NodesService(broker)

    # ChatNode reference for topic routing (deployed separately via deploy_chat_node.py)
    chat_node = ChatNode(name=args.chat_node_name)

    tools = [execute_trade, get_portfolio, calculator, place_limit_order, cancel_limit_order]
    router = AgentRouterNode(
        chat_node=chat_node,
        tool_nodes=tools,
        name=args.name,
        message_history_store=InMemoryMessageHistoryStore(),
        system_prompt=system_prompt,
    )
    service.register_node(router, group_id=args.name)

    tool_names = ", ".join(t.tool_schema.name for t in tools)
    print(f"  - Agent:    {args.name}")
    print(f"  - Strategy: {args.strategy}")
    print(f"  - ChatNode: {args.chat_node_name} (topic: {chat_node.entrypoint_topic})")
    print(f"  - Input:    {router.subscribed_topic}")
    print(f"  - Reply:    {router.entrypoint_topic}")
    print(f"  - Tools:    {tool_names}")

    print("\nRouter node ready. Waiting for requests...")
    await service.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nRouter node stopped.")
