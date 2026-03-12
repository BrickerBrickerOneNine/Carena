import argparse
import asyncio
import logging

from dotenv import load_dotenv
from rich.live import Live

from calfkit.broker.broker import BrokerClient
from calfkit.runners.service import NodesService
from coinbase_kafka_connector import (
    PRICE_TOPIC,
    TickerMessage,
)
import trading_tools
from trading_tools import (
    calculator,
    cancel_limit_order,
    execute_trade,
    get_portfolio,
    place_limit_order,
    price_book,
    store,
    view,
)

# Tools & Price Feed — Deploys trading tool workers and subscribes
# to the Kafka price topic published by the connector.
#
# The price subscriber hydrates the shared price book that the trading
# tools read from when executing trades.
#
# Usage:
#     uv run python examples/daytrading_agents_arena/tools_and_dashboard.py
#
# Prerequisites:
#     - Kafka broker running at localhost:9092

load_dotenv()


def parse_args():
    parser = argparse.ArgumentParser(
        description="Deploy trading tools, price feed, and dashboard.",
    )
    parser.add_argument(
        "--bootstrap-servers",
        required=True,
        help="Kafka bootstrap servers address",
    )
    parser.add_argument(
        "--snapshot-interval",
        type=float,
        default=600.0,
        help="Seconds between portfolio snapshots (0 disables recording)",
    )
    parser.add_argument(
        "--data-dir",
        type=str,
        default="./data",
        help="Output directory for CSV data files",
    )
    parser.add_argument(
        "--fee-rate",
        type=float,
        default=None,
        help="Transaction fee rate as a decimal (e.g. 0.001 = 0.1%%). "
        "Overrides TRADE_FEE_RATE env var. Default: 0.001",
    )
    parser.add_argument(
        "--trading-mode",
        choices=["simulated", "live"],
        default="simulated",
        help="Trading mode: 'simulated' (default) or 'live' (real Coinbase orders)",
    )
    parser.add_argument(
        "--coinbase-api-key",
        type=str,
        default=None,
        help="Coinbase CDP API key name (required for live mode)",
    )
    parser.add_argument(
        "--coinbase-api-secret",
        type=str,
        default=None,
        help="Coinbase CDP API secret / EC private key (required for live mode)",
    )
    parser.add_argument(
        "--state-file",
        type=str,
        default=None,
        help="Path for state checkpoint file (default: <data-dir>/arena_state.json)",
    )
    parser.add_argument(
        "--no-restore",
        action="store_true",
        help="Skip restoring state from checkpoint file",
    )
    parser.add_argument(
        "--tax-rate",
        type=float,
        default=None,
        help="Estimated tax rate on short-term gains as decimal (e.g. 0.30 = 30%%). Default: 0.30",
    )
    parser.add_argument(
        "--num-agents",
        type=int,
        default=1,
        help="Number of agents (used to split Coinbase balance in live mode)",
    )
    parser.add_argument(
        "--web-port",
        type=int,
        default=0,
        help="Port for web dashboard (0 to disable, default: 0)",
    )
    return parser.parse_args()


async def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
        datefmt="%H:%M:%S",
    )

    args = parse_args()

    # ── Fee rate override ──────────────────────────────────────────
    if args.fee_rate is not None:
        trading_tools.TRADE_FEE_RATE = args.fee_rate

    # ── Tax rate override ─────────────────────────────────────────
    if args.tax_rate is not None:
        trading_tools.TAX_RATE = args.tax_rate

    # ── State checkpoint ──────────────────────────────────────────
    from pathlib import Path as _Path
    from trading_tools import AccountStore

    state_file = _Path(args.state_file) if args.state_file else _Path(args.data_dir) / "arena_state.json"
    store.set_checkpoint_path(state_file)

    if not args.no_restore:
        checkpoint = AccountStore.load_checkpoint(state_file)
        if checkpoint is not None:
            n_accounts = len(checkpoint.get("accounts", {}))
            n_orders = len(checkpoint.get("pending_orders", []))
            saved_at = checkpoint.get("saved_at", "unknown")
            print(f"\nFound saved state: {n_accounts} account(s), {n_orders} pending order(s) (saved {saved_at})")
            try:
                answer = input("Restore previous session? [Y/n] ").strip().lower()
            except EOFError:
                answer = "y"
            if answer in ("", "y", "yes"):
                store.restore_from_checkpoint(checkpoint)
                print("State restored successfully.")
            else:
                print("Starting fresh session.")

    # ── Trading mode ──────────────────────────────────────────────
    trading_tools.TRADING_MODE = args.trading_mode
    if args.trading_mode == "live":
        # Try CLI args first, then fall back to arena_config.json
        cb_key = args.coinbase_api_key
        cb_secret = args.coinbase_api_secret
        if not cb_key or not cb_secret:
            config_path = _Path(__file__).resolve().parent / "arena_config.json"
            print(f"  Reading credentials from {config_path}...")
            if config_path.exists():
                import json as _json
                _cfg = _json.loads(config_path.read_text())
                cb_key = cb_key or _cfg.get("coinbase", {}).get("api_key", "")
                cb_secret = cb_secret or _cfg.get("coinbase", {}).get("api_secret", "")
                if cb_key:
                    print(f"  API key found: {cb_key[:30]}...")
                else:
                    print("  WARNING: No api_key found in arena_config.json")
            else:
                print(f"  WARNING: {config_path} not found")
        if not cb_key or not cb_secret:
            print("ERROR: Coinbase credentials not found (checked CLI args and arena_config.json)")
            return
        from coinbase_trader import CoinbaseTrader
        trader = CoinbaseTrader(cb_key, cb_secret)
        store.attach_coinbase_trader(trader)

        # Fetch real Coinbase USD balance and split across agents
        print("\nFetching Coinbase account balance...")
        try:
            balances = trader.get_balances()
            print(f"  All balances: {balances}")
            # Check for USD or USDC
            usd_balance = balances.get("USD", 0.0) + balances.get("USDC", 0.0)
            if usd_balance > 0:
                num_agents = max(1, args.num_agents)
                per_agent = round(usd_balance / num_agents, 2)
                trading_tools.INITIAL_CASH = per_agent
                # Update dataclass defaults so new AgentAccounts use the real balance
                trading_tools.AgentAccount.__dataclass_fields__["cash"].default = per_agent
                trading_tools.AgentAccount.__dataclass_fields__["peak_value"].default = per_agent
                print(f"  Coinbase balance: ${usd_balance:,.2f}")
                print(f"  Per-agent starting cash: ${per_agent:,.2f} ({num_agents} agents)")
            else:
                print("  WARNING: USD/USDC balance is $0.00")
                print(f"  Using default starting cash: ${trading_tools.INITIAL_CASH:,.2f}")
        except Exception as e:
            print(f"  ERROR fetching balance: {e}")
            print(f"  Using default starting cash: ${trading_tools.INITIAL_CASH:,.2f}")

    mode_label = "LIVE (real Coinbase orders)" if args.trading_mode == "live" else "SIMULATED"
    print("=" * 50)
    print("Tools & Price Feed Deployment")
    print(f"  Trading mode: {mode_label}")
    print(f"  Starting cash per agent: ${trading_tools.INITIAL_CASH:,.2f}")
    print(f"  Fee rate: {trading_tools.TRADE_FEE_RATE:.4%} per trade")
    print(f"  Tax rate: {trading_tools.TAX_RATE:.0%} (short-term capital gains estimate)")
    print(f"  State file: {state_file}")
    print("=" * 50)

    print(f"\nConnecting to Kafka broker at {args.bootstrap_servers}...")
    broker = BrokerClient(bootstrap_servers=args.bootstrap_servers)
    service = NodesService(broker)

    # ── Tool nodes ───────────────────────────────────────────────
    print("\nRegistering trading tool nodes...")
    for tool in (execute_trade, get_portfolio, calculator, place_limit_order, cancel_limit_order):
        service.register_node(tool)
        print(f"  - {tool.tool_schema.name} (topic: {tool.subscribed_topic})")

    # ── Price subscriber ─────────────────────────────────────────
    @broker.subscriber(PRICE_TOPIC, group_id="tools-dashboard")
    async def handle_price_update(ticker: TickerMessage) -> None:
        price_book.update(ticker.model_dump())
        # Check and fill any pending limit orders on each price update
        store.check_and_fill_orders()
        view.rerender()

    # ── Data recorder (optional) ────────────────────────────────
    recorder = None
    if args.snapshot_interval > 0:
        from data_recorder import DataRecorder

        recorder = DataRecorder(data_dir=args.data_dir)
        store.attach_recorder(recorder)
        print(f"\nCSV recording enabled → {args.data_dir}/ (snapshots every {args.snapshot_interval}s)")

    # ── Web dashboard (optional) ────────────────────────────────
    if args.web_port:
        from web_dashboard import start_web_dashboard, push_activity, update_health
        await start_web_dashboard(
            store,
            port=args.web_port,
            initial_cash=trading_tools.INITIAL_CASH,
            tax_rate=trading_tools.TAX_RATE,
        )
        print(f"\nWeb dashboard: http://localhost:{args.web_port}")

        # Subscribe to agent activity for the web dashboard
        import json as _json
        from calfkit._vendor.pydantic_ai.messages import (
            ModelRequest,
            ModelResponse,
            TextPart,
            ToolCallPart,
            ToolReturnPart,
        )
        from calfkit.models.event_envelope import EventEnvelope

        def _format_tool_call(part: ToolCallPart) -> str:
            try:
                args_d = part.args_as_dict()
            except Exception:
                args_d = {}
            if args_d:
                params = ", ".join(
                    f"{k}={_json.dumps(v)[:80]}" for k, v in args_d.items()
                )
                return f"{part.tool_name}({params})"
            return f"{part.tool_name}()"

        @broker.subscriber("agent_router.output", group_id="web-dashboard-activity")
        async def handle_agent_activity(envelope: EventEnvelope) -> None:
            # Always record that a message was received (for health tracking)
            update_health("received")

            last_msg = envelope.latest_message_in_history
            if last_msg is None:
                return
            agent_name = envelope.agent_name or "unknown"
            trace_id = envelope.trace_id
            history_len = len(envelope.message_history)

            if isinstance(last_msg, ModelResponse):
                update_health("llm_response")
                tool_calls = [p for p in last_msg.parts if isinstance(p, ToolCallPart)]
                text_parts = [p.content for p in last_msg.parts if isinstance(p, TextPart)]
                if tool_calls:
                    update_health("tool_call")
                    lines = [_format_tool_call(tc) for tc in tool_calls]
                    if text_parts:
                        lines.append(f'"{" ".join(text_parts)}"')
                    push_activity(agent_name, "TOOL_CALL", "\n".join(lines), trace_id, history_len)
                elif text_parts:
                    push_activity(agent_name, "RESPONSE", " ".join(text_parts), trace_id, history_len)

            elif isinstance(last_msg, ModelRequest):
                tool_returns = [p for p in last_msg.parts if isinstance(p, ToolReturnPart)]
                if tool_returns:
                    lines = [
                        f"{tr.tool_name} -> {tr.model_response_str()[:200]}"
                        for tr in tool_returns
                    ]
                    push_activity(agent_name, "TOOL_RESULT", "\n".join(lines), trace_id, history_len)

    print("\nStarting portfolio dashboard (prices via Kafka)...")

    try:
        with Live(view._build_layout(), auto_refresh=False, screen=True) as live:
            view.attach_live(live)
            if recorder is not None:
                recorder.start_snapshot_loop(store, interval=args.snapshot_interval)
            await service.run()
    finally:
        if recorder is not None:
            await recorder.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nTools and price feed stopped.")
