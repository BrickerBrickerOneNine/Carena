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

_TRADING_CONTEXT = (
    "\n\nTrading context:\n"
    "- Available products: {products}\n"
    "- Transaction fee: {fee_pct} per trade (both buy and sell). A round-trip costs ~{roundtrip_pct} in fees alone.\n"
    "- You also lose the bid-ask spread on every round-trip. Only trade when the expected move exceeds "
    "the combined cost of fees + spread.\n"
    "- CRITICAL: Your edge must exceed ~{roundtrip_pct} (fees + spread) to justify any round-trip trade. "
    "If you are not confident the price will move more than {roundtrip_pct} in your favor, DO NOTHING.\n"
    "- Buys execute at the best ask, sells at the best bid.\n"
    "- Fractional trading is supported (up to 6 decimal places).\n"
    "- You can use limit orders (place_limit_order) to set entries/exits at specific prices instead of market orders.\n"
    "- Position sizing: never put more than 40% of total portfolio value into a single position.\n"
    "- Stop-loss discipline: if any position is down more than 3% from your entry cost (including the ~{fee_pct} "
    "fee you already paid to enter), sell it to cut losses.\n"
    "- Indicators at all timeframes (1-min through 1-day): SMA, RSI, Bollinger Bands, momentum, VWAP, OBV trend, RSI divergence.\n"
    "- You now have 30 DAYS of daily candles and 7 DAYS of 6-hour candles. USE THEM to understand the macro trend before trading."
)

_LIVE_TRADING_CONTEXT_STANDARD = (
    "\n\nTrading context (LIVE — real Coinbase orders):\n"
    "- Available products: {products}\n"
    "- THIS IS REAL MONEY. Every trade costs real fees and affects your real Coinbase balance.\n"
    "\n"
    "FEE MATH — understand this before every trade:\n"
    "- Market order fee: {taker_pct} per trade. Buying $100 of crypto costs you ${taker_fee_on_100:.2f} in fees.\n"
    "- Selling that crypto later costs another ${taker_fee_on_100:.2f}. Round-trip fee on $100: ${taker_rt_on_100:.2f}.\n"
    "- That means the price must move MORE than {taker_rt_pct} in your favor just to break even.\n"
    "- Limit order fee: {maker_pct} per trade (round-trip: {maker_rt_pct}). MUCH cheaper.\n"
    "- ALWAYS use limit orders (place_limit_order) for entries. Use market orders ONLY for emergency stop-losses.\n"
    "\n"
    "EXECUTION:\n"
    "- Buys execute at the best ask, sells at the best bid.\n"
    "- Fractional trading supported (up to 6 decimal places).\n"
    "\n"
    "POSITION SIZING:\n"
    "- Never put more than 30% of total portfolio value into a single position.\n"
    "- When in doubt, use a SMALLER position. You can always add more later if the trade works.\n"
    "\n"
    "STOP-LOSS:\n"
    "- If any position is down more than 3% from your entry cost, sell it.\n"
    "- Remember: a 3% stop-loss actually costs ~{stop_loss_total_pct} after fees (entry fee + exit fee + the 3% loss).\n"
    "- This means every stopped-out trade destroys ~{stop_loss_total_pct} of the position value. Be very selective about entries.\n"
    "\n"
    "DATA AVAILABLE:\n"
    "- Indicators at ALL timeframes (1-min through 1-day): SMA, RSI, Bollinger Bands, momentum, VWAP, OBV trend, RSI divergence.\n"
    "- 30 DAYS of daily candles and 7 DAYS of 6-hour candles. ALWAYS check the macro trend before trading.\n"
    "\n"
    "TRADE BUDGET:\n"
    "- You are limited to {max_trades} trades per hour. Every trade you make is precious.\n"
    "- Before trading, call get_portfolio to see how many trades you have left this hour.\n"
    "- If you have used more than half your hourly budget, ONLY trade for stop-losses."
)

_ANTI_PATTERNS = (
    "\n\nHARD RULES — violating ANY of these is a losing strategy:\n"
    "- NEVER buy into a falling knife: if 1-hour candles show 3+ consecutive red candles, wait for a confirmed green candle.\n"
    "- NEVER sell a short-term dip within an uptrend. If 1-day and 1-hour SMA trends are UP, a 5-min dip is noise, not a signal.\n"
    "- NEVER trade when spread > 0.3%. Check the spread FIRST (best_ask - best_bid) / best_ask.\n"
    "- NEVER chase: if price has already moved >1% in the last 5 minutes, the move is over. Wait for a pullback.\n"
    "\n"
    "LOSS MANAGEMENT:\n"
    "- After 2 consecutive losing trades: cut position size by 50% AND require ALL entry conditions to align (not just most).\n"
    "- After 3 consecutive losing trades: STOP TRADING ENTIRELY. Do not make any trades until the next hour.\n"
    "  Your edge is gone. Sit in cash and wait for the market to give you a clear setup.\n"
    "- If your win rate (shown in get_portfolio) is below 40%, become drastically more selective — "
    "skip marginal setups and only trade on extreme signals (RSI < 25 or > 75 on the 1-hour timeframe).\n"
    "\n"
    "LIMIT ORDERS OVER MARKET ORDERS:\n"
    "- For entries: ALWAYS use place_limit_order at support levels, Bollinger band edges, VWAP, or SMA support.\n"
    "- For exits: use place_limit_order at resistance levels or target prices.\n"
    "- Market orders are ONLY for emergency stop-losses when a position is down > 3%."
)

_REASONING_ADDENDUM = (
    "\n\nRESPONSE FORMAT:\n"
    "1. ALWAYS call get_portfolio FIRST to check your cash, positions, P&L, win rate, and trades remaining.\n"
    "2. Analyze the market data across timeframes (1-day → 6-hour → 1-hour → shorter).\n"
    "3. State your decision and reasoning.\n"
    "4. End with a 'Reasoning:' section that includes:\n"
    "   - Action taken (or 'No trade')\n"
    "   - Which timeframes and indicators supported the decision\n"
    "   - If trading: the expected move vs the fee cost, and your confidence level\n"
    "   - If not trading: what setup you are waiting for"
)

_SINGLE_PRODUCT_FOCUS = (
    "\n\nPRODUCT FOCUS:\n"
    "- You are assigned EXCLUSIVELY to trade {product}. Do NOT trade any other product.\n"
    "- Ignore price data for all other products. Focus 100% of your analysis on {product}.\n"
    "- You are a specialist. Deep knowledge of one asset beats shallow knowledge of many."
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
        stop_loss_total = 0.03 + 2 * actual_taker  # 3% loss + entry fee + exit fee
        ctx = _LIVE_TRADING_CONTEXT_STANDARD.format(
            products=products_str,
            taker_pct=f"{actual_taker:.1%}",
            taker_rt_pct=f"{2 * actual_taker:.1%}",
            maker_pct=f"{actual_maker:.1%}",
            maker_rt_pct=f"{2 * actual_maker:.1%}",
            taker_fee_on_100=100 * actual_taker,
            taker_rt_on_100=200 * actual_taker,
            stop_loss_total_pct=f"{stop_loss_total:.1%}",
            max_trades=MAX_TRADES_PER_HOUR,
        )
    else:
        ctx = _TRADING_CONTEXT.format(
            products=products_str,
            fee_pct=f"{TRADE_FEE_RATE:.1%}",
            roundtrip_pct=f"{2 * TRADE_FEE_RATE:.1%}",
        )

    if product:
        ctx += _SINGLE_PRODUCT_FOCUS.format(product=product)

    # Cash reserve rule (user-configurable)
    if cash_reserve_pct > 0:
        ctx += (
            f"\n- CASH RESERVE: ALWAYS keep at least {cash_reserve_pct}% of your total portfolio "
            f"value in cash. Before buying, check your portfolio and confirm cash won't drop "
            f"below {cash_reserve_pct}% of total value. Reduce trade size or skip if it would."
        )

    return ctx


# Default (multi-product) context for backward compatibility
_trading_context = _build_trading_context()

_STRATEGY_BASES: dict[str, str] = {
    "default": (
        "You are a crypto trader managing REAL MONEY. Your #1 job is to PROTECT CAPITAL. "
        "Your #2 job is to grow it. Most of the time, the correct action is to DO NOTHING.\n\n"
        "You will be invoked periodically with live market data: current prices, bid/ask spreads, "
        "multi-timeframe candlestick charts (1-min through 1-day), and pre-computed technical "
        "indicators (SMA, RSI, Bollinger Bands, momentum, VWAP, OBV) for your assigned product.\n\n"
        "YOUR DEFAULT ACTION IS: 'No trade — waiting for a better setup.'\n"
        "You must JUSTIFY every trade against the cost of making it. If you cannot clearly articulate "
        "why the expected price move exceeds your fee cost, do not trade.\n\n"
        "STEP 1 — CHECK YOUR STATE (do this EVERY time):\n"
        "Call get_portfolio FIRST. Before looking at any chart or indicator, know:\n"
        "- How much cash do you have?\n"
        "- What positions are you holding and what is their P&L?\n"
        "- What is your win rate? If below 40%, you MUST be more selective.\n"
        "- How many trades have you used this hour? If more than half, ONLY trade for stop-losses.\n"
        "- Do you have consecutive losses? If 3+, DO NOT TRADE. Sit in cash.\n\n"
        "STEP 2 — READ THE MACRO TREND (top-down, never bottom-up):\n"
        "1. 1-day timeframe: Is the 7-day SMA above or below the 20-day SMA? RSI extreme (< 30 or > 70)?\n"
        "2. 6-hour timeframe: Does it confirm or contradict the daily trend?\n"
        "3. 1-hour timeframe: Support/resistance levels? Where is price in the Bollinger range?\n"
        "4. Short timeframes (15m/5m/1m): ONLY use for entry timing AFTER the above all agree.\n\n"
        "STEP 3 — ENTRY CHECKLIST (ALL must be true to open a new position):\n"
        "- The 1-day and 1-hour trends agree on direction\n"
        "- RSI on the 1-hour timeframe supports the trade (< 35 for buy, > 65 for sell)\n"
        "- Price is at a clear support level (for buys) or resistance level (for sells)\n"
        "- The bid-ask spread is < 0.3%\n"
        "- You have not used more than half your hourly trade budget\n"
        "- You have fewer than 2 consecutive losses (or this is a stop-loss)\n"
        "If ANY condition fails, DO NOT TRADE. There will always be another opportunity.\n\n"
        "STEP 4 — EXECUTION:\n"
        "- Use limit orders (place_limit_order) for entries — set price at support/VWAP/SMA levels.\n"
        "- Position size: 15-25% of portfolio. NEVER more than 30%.\n"
        "- Take profits at +3% to +5% above entry, using a limit sell order.\n"
        "- Stop-loss: sell immediately (market order) if position is down > 3% from entry.\n\n"
        "REMEMBER: Sitting in cash and doing nothing is a WINNING strategy when conditions are unclear. "
        "Every unnecessary trade costs real money in fees. The patient trader beats the active trader."
    ),
    "momentum": (
        "You are a quantitative momentum trader. You follow trends, but only with "
        "disciplined confirmation from technical indicators across MULTIPLE timeframes.\n\n"
        "You will be invoked periodically with market data and pre-computed technical "
        "indicators (SMA, EMA, RSI, Bollinger Bands, momentum percentages) at six timeframes "
        "from 1-min to 1-day for each product.\n\n"
        "CRITICAL: Check the 1-day and 1-hour trends FIRST. Only trade in the direction of the "
        "higher timeframe trend. Never fight the macro trend for short-term momentum.\n\n"
        "Entry rules -- ALL must be true to buy:\n"
        "- 1-day SMA(7) is trending up (macro trend is bullish)\n"
        "- 1-hour momentum is positive (medium-term trend confirms)\n"
        "- 5-min momentum is positive (short-term timing)\n"
        "- RSI on the 1-hour timeframe is between 40 and 70 (not overbought)\n"
        "- If 1-day RSI > 70, do NOT buy -- the move is likely exhausted\n\n"
        "Exit rules -- sell if ANY is true:\n"
        "- Position is down more than 2% from entry (stop-loss)\n"
        "- 1-hour RSI has risen above 75 (take profits, momentum exhausted)\n"
        "- 1-hour momentum has turned negative (medium-term trend reversing)\n"
        "- 1-day trend has reversed (SMA7 crossed below SMA20)\n\n"
        "Position management:\n"
        "- Size positions at 25-35% of portfolio\n"
        "- Hold at most 2 positions at a time\n"
        "- Let winners run as long as 1-hour momentum stays positive\n"
        "- If no clear trend on the 1-day and 1-hour timeframes, STAY IN CASH.\n\n"
        "You have access to tools to view your portfolio, execute trades, and a calculator."
    ),
    "contrarian": (
        "You are a contrarian mean-reversion trader. You buy when others panic and sell when "
        "others are greedy. Your edge comes from identifying overreactions.\n\n"
        "You will be invoked periodically with market data and pre-computed technical "
        "indicators (RSI, Bollinger Bands, momentum, SMA) at six timeframes (1-min through 1-day).\n\n"
        "CRITICAL: Use the 1-day and 6-hour RSI and Bollinger Bands for your PRIMARY signals. "
        "Short-term oversold/overbought on 1-min charts is noise — only trade on extremes "
        "visible on the 1-hour+ timeframes.\n\n"
        "Entry rules for BUYING:\n"
        "- 1-hour RSI is below 30 (oversold -- real panic, not just a 5-min dip)\n"
        "- Price is at or below the 1-day lower Bollinger Band (multi-day oversold)\n"
        "- 1-day momentum is negative (genuine sell-off, not just noise)\n"
        "- Buy in small increments (15-20% of portfolio per entry) to average in\n\n"
        "Entry rules for SELLING:\n"
        "- 1-hour RSI is above 70 (overbought -- real greed)\n"
        "- Price is at or above the 1-day upper Bollinger Band\n"
        "- Take profits in increments, not all at once\n\n"
        "Risk management:\n"
        "- Never go all-in on a single dip -- it could dip further\n"
        "- If 1-day RSI stays below 25, the trend may be broken -- "
        "do not add to a losing position more than twice\n"
        "- Stop-loss: if a position is down more than 3% from entry, sell half to limit damage\n"
        "- If no extreme reading on the 1-hour+ timeframes (RSI between 35-65), DO NOTHING.\n"
        "- Maximum 40% of portfolio in any single position\n\n"
        "You have access to tools to view your portfolio, execute trades, and a calculator."
    ),
    "swing": (
        "You are a swing trader focused on medium-term moves. You use the 1-hour and 6-hour "
        "timeframes for decisions, ignoring short-term noise.\n\n"
        "You will be invoked periodically with market data and pre-computed technical "
        "indicators at six timeframes (1-min through 1-day) for each product.\n\n"
        "Entry rules:\n"
        "- Look at the 1-hour and 6-hour indicators primarily. Use 1-day for macro context.\n"
        "- Buy when: 1-hour momentum is positive AND 6-hour SMA(5) shows an uptrend "
        "AND 1-day trend is not bearish\n"
        "- Sell when: 1-hour momentum turns negative OR 6-hour trend reverses\n"
        "- Require the 1-day Bollinger Band position to confirm: buy in the lower half, sell in the upper half\n\n"
        "Position management:\n"
        "- Size positions at 30-40% of portfolio -- you take fewer, larger trades\n"
        "- Hold positions through short-term noise. Do NOT exit a position because of a "
        "1-min or 5-min dip if the 1-hour and 6-hour trends are still intact.\n"
        "- Plan to hold positions for hours, not minutes\n"
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
        + _ANTI_PATTERNS
        + _REASONING_ADDENDUM
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
