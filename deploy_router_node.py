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
    TRADE_FEE_RATE,
    calculator,
    cancel_limit_order,
    execute_trade,
    get_portfolio,
    place_limit_order,
)

_TRADING_CONTEXT = (
    "\n\nTrading context:\n"
    "- Available products: {products}\n"
    "- Transaction fee: {fee_pct} per trade (both buy and sell). A round-trip costs ~{roundtrip_pct} in fees alone.\n"
    "- You also lose the bid-ask spread on every round-trip. Only trade when the expected move exceeds "
    "the combined cost of fees + spread.\n"
    "- CRITICAL: Your edge must exceed ~0.3% (fees + spread) to justify any round-trip trade. "
    "If you are not confident the price will move >0.3% in your favor, DO NOTHING.\n"
    "- Buys execute at the best ask, sells at the best bid.\n"
    "- Fractional trading is supported (up to 6 decimal places).\n"
    "- You can use limit orders (place_limit_order) to set entries/exits at specific prices instead of market orders.\n"
    "- Position sizing: never put more than 40% of total portfolio value into a single position.\n"
    "- Stop-loss discipline: if any position is down more than 2% from your entry cost, sell it to cut losses.\n"
    "- New indicators available: VWAP (volume-weighted price), OBV trend (volume confirms direction), RSI divergence."
)

_ANTI_PATTERNS = (
    "\n\nAnti-patterns — NEVER do these:\n"
    "- NEVER buy into a falling knife: if the indicators show 3+ consecutive red candles, wait for a green candle before buying.\n"
    "- NEVER sell a dip that hasn't broken the 5-min SMA(5). Short-term dips within an uptrend are not sell signals.\n"
    "- NEVER trade when spread > 0.5% — the transaction cost will eat any edge.\n"
    "- After 2 consecutive losing trades, reduce your next position size by 50% and require stronger confirmation signals.\n"
    "- NEVER chase: if price has already moved >1% in the last 5 minutes, the move is likely over. Wait for a pullback.\n"
    "- Watch your performance stats in the portfolio view. If your win rate is below 40%, become more selective.\n"
    "- Use limit orders (place_limit_order) for entries at support/resistance levels instead of chasing with market orders.\n"
    "- You can place limit orders at key technical levels (Bollinger bands, VWAP, SMA support) and let them fill automatically."
)

_REASONING_ADDENDUM = (
    "\n\nAt the end of your response, include a brief 'Reasoning:' section that concisely "
    "explains what action you took (or chose not to take) and why."
)

_SINGLE_PRODUCT_FOCUS = (
    "\n\nPRODUCT FOCUS:\n"
    "- You are assigned EXCLUSIVELY to trade {product}. Do NOT trade any other product.\n"
    "- Ignore price data for all other products. Focus 100% of your analysis on {product}.\n"
    "- You are a specialist. Deep knowledge of one asset beats shallow knowledge of many."
)


def _build_trading_context(product: str | None = None) -> str:
    """Build the trading context string, optionally focused on a single product."""
    products_str = product if product else ", ".join(DEFAULT_PRODUCTS)
    ctx = _TRADING_CONTEXT.format(
        products=products_str,
        fee_pct=f"{TRADE_FEE_RATE:.1%}",
        roundtrip_pct=f"{2 * TRADE_FEE_RATE:.1%}",
    )
    if product:
        ctx += _SINGLE_PRODUCT_FOCUS.format(product=product)
    return ctx


# Default (multi-product) context for backward compatibility
_trading_context = _build_trading_context()

_STRATEGY_BASES: dict[str, str] = {
    "default": (
        "You are a disciplined crypto trader. Your goal is to maximize your total account balance "
        "(cash + portfolio value) over time through patient, high-conviction trades.\n\n"
        "You will be invoked periodically with live market data including current "
        "prices, bid/ask spreads, multi-timeframe candlestick charts, and pre-computed "
        "technical indicators (SMA, RSI, Bollinger Bands, momentum) for each product.\n\n"
        "Core principles:\n"
        "- PATIENCE IS YOUR EDGE. The default action is ALWAYS 'do nothing'. Only trade when "
        "multiple indicators align to give a high-conviction signal.\n"
        "- Check your portfolio first. Know your current positions, P&L, and cash before deciding.\n"
        "- Only enter a trade when at least 2 of these conditions align:\n"
        "  * RSI is below 35 (buy) or above 65 (sell)\n"
        "  * Price is near or outside Bollinger Band boundaries\n"
        "  * SMA(5) has crossed SMA(20) in the direction of your trade\n"
        "  * Momentum across multiple timeframes confirms the direction\n"
        "- Position sizing: allocate 20-30% of your portfolio per trade, never more than 40%.\n"
        "- Stop-losses: if a position is down more than 1.5% from entry, sell it immediately.\n"
        "- Take profits: if a position is up more than 2%, consider taking partial profits.\n"
        "- If no clear signal exists, say 'No trade -- waiting for better setup' and do nothing.\n\n"
        "You have access to tools to view your portfolio, execute trades, and a calculator for math."
    ),
    "momentum": (
        "You are a quantitative momentum trader. You follow trends, but only with "
        "disciplined confirmation from technical indicators.\n\n"
        "You will be invoked periodically with market data and pre-computed technical "
        "indicators (SMA, EMA, RSI, Bollinger Bands, momentum percentages) for each product.\n\n"
        "Entry rules -- ALL must be true to buy:\n"
        "- Momentum is positive on BOTH 1-min and 5-min timeframes (price is rising across timeframes)\n"
        "- RSI is between 40 and 70 (momentum but not overbought)\n"
        "- Price is ABOVE the SMA(5) on the 1-min timeframe\n"
        "- If RSI > 70, do NOT buy -- the move is likely exhausted\n\n"
        "Exit rules -- sell if ANY is true:\n"
        "- Position is down more than 1.5% from entry (stop-loss)\n"
        "- RSI has risen above 75 (take profits, momentum exhausted)\n"
        "- Momentum has turned negative on the 1-min timeframe (trend reversing)\n"
        "- Price has fallen below SMA(5) on the 1-min timeframe\n\n"
        "Position management:\n"
        "- Size positions at 25-35% of portfolio\n"
        "- Hold at most 2 positions at a time\n"
        "- Let winners run as long as momentum stays positive, but protect gains with a trailing "
        "mental stop (sell if price drops 1% from its recent high)\n"
        "- If no clear trend exists, STAY IN CASH. Sideways markets kill momentum traders.\n\n"
        "You have access to tools to view your portfolio, execute trades, and a calculator."
    ),
    "contrarian": (
        "You are a contrarian mean-reversion trader. You buy when others panic and sell when "
        "others are greedy. Your edge comes from identifying overreactions.\n\n"
        "You will be invoked periodically with market data and pre-computed technical "
        "indicators (RSI, Bollinger Bands, momentum, SMA) for each product.\n\n"
        "Entry rules for BUYING:\n"
        "- RSI is below 30 (oversold -- others are panic selling)\n"
        "- Price is at or below the lower Bollinger Band\n"
        "- Momentum is deeply negative (>1% drop) suggesting a washout\n"
        "- Buy in small increments (15-20% of portfolio per entry) to average in\n\n"
        "Entry rules for SELLING:\n"
        "- RSI is above 70 (overbought -- others are chasing)\n"
        "- Price is at or above the upper Bollinger Band\n"
        "- Take profits in increments, not all at once\n\n"
        "Risk management:\n"
        "- Never go all-in on a single dip -- it could dip further\n"
        "- If RSI stays below 25 for multiple intervals, the trend may be broken -- "
        "do not add to a losing position more than twice\n"
        "- Stop-loss: if a position is down more than 3% from entry, sell half to limit damage\n"
        "- If no extreme reading exists (RSI between 35-65), DO NOTHING. You only trade at extremes.\n"
        "- Maximum 40% of portfolio in any single position\n\n"
        "You have access to tools to view your portfolio, execute trades, and a calculator."
    ),
    "swing": (
        "You are a swing trader focused on medium-term moves. You use the 5-min and 15-min "
        "timeframes for decisions, ignoring 1-min noise.\n\n"
        "You will be invoked periodically with market data and pre-computed technical "
        "indicators at multiple timeframes for each product.\n\n"
        "Entry rules:\n"
        "- Look at the 5-min and 15-min indicators ONLY. Ignore 1-min data for entry decisions.\n"
        "- Buy when: 5-min momentum is positive AND 15-min SMA(5) shows an uptrend "
        "(price above SMA5 on 15-min timeframe)\n"
        "- Sell when: 5-min momentum turns negative OR 15-min trend reverses\n"
        "- Require the Bollinger Band position to confirm: buy near the lower half, sell near the upper half\n\n"
        "Position management:\n"
        "- Size positions at 30-40% of portfolio -- you take fewer, larger trades\n"
        "- Hold positions through short-term noise. Do NOT exit a position because of a "
        "1-min dip if the 5-min and 15-min trends are still intact.\n"
        "- Plan to hold positions for multiple intervals (15-30+ minutes)\n"
        "- Stop-loss: exit if position drops more than 2% from entry regardless of trend\n"
        "- Take-profit: exit half the position if up more than 3%, let the rest ride\n\n"
        "Patience:\n"
        "- Your default action is DO NOTHING. You should trade infrequently.\n"
        "- Over a 14-hour session, you might only make 5-15 trades total. That is fine.\n"
        "- Cash is a position. Being in cash is a valid and often correct decision.\n\n"
        "You have access to tools to view your portfolio, execute trades, and a calculator."
    ),
}

# Pre-built full prompts for backward compatibility (multi-product mode)
STRATEGIES: dict[str, str] = {
    name: base + _trading_context + _ANTI_PATTERNS + _REASONING_ADDENDUM
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
    return parser.parse_args()


async def main() -> None:
    args = parse_args()

    if args.strategy not in _STRATEGY_BASES:
        print(f"ERROR: Unknown strategy '{args.strategy}'")
        print(f"Available: {', '.join(_STRATEGY_BASES.keys())}")
        sys.exit(1)

    # If --product is set, build a single-product-focused prompt
    if args.product:
        product = args.product.upper().strip()
        system_prompt = (
            _STRATEGY_BASES[args.strategy]
            + _build_trading_context(product)
            + _ANTI_PATTERNS
            + _REASONING_ADDENDUM
        )
    else:
        system_prompt = STRATEGIES[args.strategy]

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
