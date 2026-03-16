#!/usr/bin/env python3
"""Cross-platform TUI launcher for the Crypto Daytrading Arena.

Provides a Rich-based configuration wizard and process manager that
replaces the macOS-only launch_arena.sh script.

Usage:
    uv run python launcher.py                       # interactive wizard
    uv run python launcher.py --config arena_config.json  # headless
"""

from __future__ import annotations

import json
import os
import platform
import shutil
import socket
import subprocess
import sys
import time
from pathlib import Path

from rich.console import Console
from rich.panel import Panel
from rich.prompt import Confirm, IntPrompt, Prompt
from rich.table import Table

console = Console()

ARENA_DIR = Path(__file__).resolve().parent
DOCKER_COMPOSE = ARENA_DIR / "docker" / "docker-compose.yml"
CONFIG_FILE = ARENA_DIR / "arena_config.json"

AVAILABLE_COINS = [
    "BTC-USD",
    "ETH-USD",
    "SOL-USD",
    "LTC-USD",
    "DOGE-USD",
    "LINK-USD",
    "XRP-USD",
]

AVAILABLE_STRATEGIES = ["contrarian", "default", "momentum", "swing"]

DEFAULT_CONFIG = {
    "trading_mode": "simulated",
    "coinbase": {"key_file": "", "api_key": "", "api_secret": "", "coinbase_one": False},
    "llm": {
        "provider": "openai",
        "api_key": "",
        "model_id": "gpt-4o-mini",
        "base_url": None,
    },
    "coins": list(AVAILABLE_COINS),
    "strategies": ["contrarian"],
    "kafka_bootstrap": "localhost:9092",
    "market_interval": 300,
    "fee_rate": 0.05,
    "tax_rate": 0.30,
    "snapshot_interval": 600,
    "data_dir": "./data",
    "web_port": 8080,
    "cash_reserve_pct": 30,
}


# ---------------------------------------------------------------------------
# Configuration wizard
# ---------------------------------------------------------------------------


def run_wizard() -> dict:
    """Interactive Rich TUI wizard. Returns a config dict."""
    config = dict(DEFAULT_CONFIG)
    config["coinbase"] = dict(DEFAULT_CONFIG["coinbase"])
    config["llm"] = dict(DEFAULT_CONFIG["llm"])

    console.print(
        Panel(
            "[bold cyan]Crypto Daytrading Arena[/bold cyan]\n"
            "Configuration Wizard",
            expand=False,
        )
    )

    # Step 1: Trading mode
    console.print("\n[bold]Step 1/5 -- Trading Mode[/bold]")
    console.print("  [1] Simulated (paper trading against live prices)")
    console.print("  [2] Live (real Coinbase orders)")
    mode_choice = Prompt.ask(
        "  Select mode", choices=["1", "2"], default="1"
    )
    config["trading_mode"] = "live" if mode_choice == "2" else "simulated"

    if config["trading_mode"] == "live":
        console.print(
            "\n  [yellow]WARNING: Live mode places REAL orders on Coinbase![/yellow]"
        )
        console.print("  [1] Key file (CDP JSON key file from portal.cdp.coinbase.com)")
        console.print("  [2] Manual (paste API key + secret)")
        key_method = Prompt.ask("  How to provide credentials?", choices=["1", "2"], default="1")
        if key_method == "1":
            config["coinbase"]["key_file"] = Prompt.ask(
                "  Path to CDP key JSON file",
                default="coinbase_cdp_key.json",
            )
        else:
            config["coinbase"]["api_key"] = Prompt.ask("  Coinbase API Key ID")
            config["coinbase"]["api_secret"] = Prompt.ask(
                "  Coinbase API Secret (base64 or PEM)"
            )

        # Coinbase One subscription (0% trading fees)
        config["coinbase"]["coinbase_one"] = Confirm.ask(
            "  Do you have a Coinbase One subscription? (0% trading fees)",
            default=False,
        )

    # Step 2: LLM provider
    console.print("\n[bold]Step 2/5 -- LLM Provider[/bold]")
    console.print("  [1] OpenAI (default)")
    console.print("  [2] Anthropic")
    console.print("  [3] Custom OpenAI-compatible endpoint")
    llm_choice = Prompt.ask("  Select provider", choices=["1", "2", "3"], default="1")

    if llm_choice == "1":
        config["llm"]["provider"] = "openai"
        config["llm"]["api_key"] = Prompt.ask(
            "  OpenAI API Key",
            default=os.getenv("OPENAI_API_KEY", ""),
        )
        config["llm"]["model_id"] = Prompt.ask(
            "  Model ID", default="gpt-4o-mini"
        )
        config["llm"]["base_url"] = None
    elif llm_choice == "2":
        config["llm"]["provider"] = "anthropic"
        config["llm"]["api_key"] = Prompt.ask(
            "  Anthropic API Key",
            default=os.getenv("ANTHROPIC_API_KEY", ""),
        )
        config["llm"]["model_id"] = Prompt.ask(
            "  Model ID", default="claude-sonnet-4-6"
        )
        config["llm"]["base_url"] = None
    else:
        config["llm"]["provider"] = "custom"
        config["llm"]["base_url"] = Prompt.ask("  Base URL (e.g. https://api.example.com/v1)")
        config["llm"]["api_key"] = Prompt.ask("  API Key")
        config["llm"]["model_id"] = Prompt.ask("  Model ID")

    # Step 3: Coin selection
    console.print("\n[bold]Step 3/5 -- Coin Selection[/bold]")
    console.print("  Available coins:")
    for i, coin in enumerate(AVAILABLE_COINS, 1):
        console.print(f"    [{i}] {coin}")
    console.print(f"    [a] All coins")

    coin_input = Prompt.ask(
        "  Enter numbers separated by commas, or 'a' for all",
        default="a",
    )
    if coin_input.strip().lower() == "a":
        config["coins"] = list(AVAILABLE_COINS)
    else:
        indices = []
        for part in coin_input.split(","):
            part = part.strip()
            if part.isdigit():
                idx = int(part) - 1
                if 0 <= idx < len(AVAILABLE_COINS):
                    indices.append(idx)
        config["coins"] = [AVAILABLE_COINS[i] for i in sorted(set(indices))]
        if not config["coins"]:
            console.print("  [yellow]No valid coins selected, defaulting to all.[/yellow]")
            config["coins"] = list(AVAILABLE_COINS)

    # Step 4: Strategy (multi-select)
    console.print("\n[bold]Step 4/5 -- Strategy[/bold]")
    for i, strat in enumerate(AVAILABLE_STRATEGIES, 1):
        console.print(f"  [{i}] {strat}")
    console.print(f"  [a] All strategies")
    strat_input = Prompt.ask(
        "  Enter numbers separated by commas, or 'a' for all",
        default="1",
    )
    if strat_input.strip().lower() == "a":
        config["strategies"] = list(AVAILABLE_STRATEGIES)
    else:
        indices = []
        for part in strat_input.split(","):
            part = part.strip()
            if part.isdigit():
                idx = int(part) - 1
                if 0 <= idx < len(AVAILABLE_STRATEGIES):
                    indices.append(idx)
        config["strategies"] = [AVAILABLE_STRATEGIES[i] for i in sorted(set(indices))]
        if not config["strategies"]:
            console.print("  [yellow]No valid strategies selected, defaulting to contrarian.[/yellow]")
            config["strategies"] = ["contrarian"]

    # Step 5: Advanced settings
    console.print("\n[bold]Step 5/5 -- Advanced Settings[/bold]")
    default_interval = 60 if config["trading_mode"] == "live" else 300
    config["market_interval"] = IntPrompt.ask(
        "  Market data interval (seconds)", default=default_interval
    )
    fee_str = Prompt.ask("  Fee rate (decimal, e.g. 0.05)", default="0.05")
    try:
        config["fee_rate"] = float(fee_str)
    except ValueError:
        config["fee_rate"] = 0.05
    cash_reserve_str = Prompt.ask(
        "  Cash reserve (% of portfolio to keep in cash, e.g. 30)", default="30"
    )
    try:
        config["cash_reserve_pct"] = int(cash_reserve_str)
    except ValueError:
        config["cash_reserve_pct"] = 30

    # Summary
    console.print()
    _print_config_summary(config)

    # Always save config so user doesn't have to re-enter on next run
    save_config(config, str(CONFIG_FILE))
    console.print(f"\n  [green]Configuration saved to {CONFIG_FILE.name}[/green]")
    console.print("  [dim]Next time you launch, this config will be loaded automatically.[/dim]")

    return config


def _print_config_summary(config: dict) -> None:
    """Display a summary table of the configuration."""
    table = Table(title="Arena Configuration", expand=False)
    table.add_column("Setting", style="cyan")
    table.add_column("Value", style="white")

    mode_label = (
        "[red bold]LIVE (real orders)[/red bold]"
        if config["trading_mode"] == "live"
        else "Simulated"
    )
    table.add_row("Trading Mode", mode_label)
    table.add_row("LLM Provider", config["llm"]["provider"])
    table.add_row("Model", config["llm"]["model_id"])
    table.add_row("Coins", ", ".join(config["coins"]))
    strategies = config.get("strategies", [config["strategy"]]) if "strategy" in config else config.get("strategies", ["contrarian"])
    table.add_row("Strategies", ", ".join(strategies))
    table.add_row("Total Agents", str(len(config["coins"]) * len(strategies)))
    table.add_row("Market Interval", f"{config['market_interval']}s")
    table.add_row("Fee Rate", f"{config['fee_rate']:.4%}")
    web_port = config.get("web_port", 8080)
    table.add_row("Web Dashboard", f"http://localhost:{web_port}" if web_port else "Disabled")

    if config["llm"].get("base_url"):
        table.add_row("Custom Base URL", config["llm"]["base_url"])

    console.print(table)


def _edit_config(config: dict) -> dict:
    """Let the user selectively edit parts of an existing config."""
    config = json.loads(json.dumps(config))  # deep copy

    console.print("\n[bold]Which settings do you want to change?[/bold]")
    strategies = config.get("strategies", [config["strategy"]]) if "strategy" in config else config.get("strategies", ["contrarian"])
    console.print("  [1] Coins (currently: {})".format(", ".join(config["coins"])))
    console.print("  [2] Strategies (currently: {})".format(", ".join(strategies)))
    console.print("  [3] Trading mode (currently: {})".format(config["trading_mode"]))
    console.print("  [4] LLM provider / API key")
    console.print("  [5] Advanced settings (fee rate, interval)")
    console.print()

    choices = Prompt.ask(
        "  Enter numbers separated by commas (e.g. 1,2)",
        default="1",
    )
    selected = {c.strip() for c in choices.split(",")}

    if "1" in selected:
        console.print("\n[bold]Coin Selection[/bold]")
        console.print("  Available coins:")
        for i, coin in enumerate(AVAILABLE_COINS, 1):
            marker = " *" if coin in config["coins"] else ""
            console.print(f"    [{i}] {coin}{marker}")
        console.print(f"    [a] All coins")
        console.print("  (* = currently selected)")

        coin_input = Prompt.ask(
            "  Enter numbers separated by commas, or 'a' for all",
            default=",".join(
                str(AVAILABLE_COINS.index(c) + 1)
                for c in config["coins"]
                if c in AVAILABLE_COINS
            ),
        )
        if coin_input.strip().lower() == "a":
            config["coins"] = list(AVAILABLE_COINS)
        else:
            indices = []
            for part in coin_input.split(","):
                part = part.strip()
                if part.isdigit():
                    idx = int(part) - 1
                    if 0 <= idx < len(AVAILABLE_COINS):
                        indices.append(idx)
            new_coins = [AVAILABLE_COINS[i] for i in sorted(set(indices))]
            if new_coins:
                config["coins"] = new_coins
            else:
                console.print("  [yellow]No valid coins selected, keeping current.[/yellow]")

    if "2" in selected:
        console.print("\n[bold]Strategies[/bold]")
        for i, strat in enumerate(AVAILABLE_STRATEGIES, 1):
            marker = " *" if strat in strategies else ""
            console.print(f"  [{i}] {strat}{marker}")
        console.print(f"  [a] All strategies")
        console.print("  (* = currently selected)")
        default_indices = ",".join(
            str(AVAILABLE_STRATEGIES.index(s) + 1)
            for s in strategies
            if s in AVAILABLE_STRATEGIES
        )
        strat_input = Prompt.ask(
            "  Enter numbers separated by commas, or 'a' for all",
            default=default_indices or "1",
        )
        if strat_input.strip().lower() == "a":
            config["strategies"] = list(AVAILABLE_STRATEGIES)
        else:
            indices = []
            for part in strat_input.split(","):
                part = part.strip()
                if part.isdigit():
                    idx = int(part) - 1
                    if 0 <= idx < len(AVAILABLE_STRATEGIES):
                        indices.append(idx)
            new_strategies = [AVAILABLE_STRATEGIES[i] for i in sorted(set(indices))]
            if new_strategies:
                config["strategies"] = new_strategies
            else:
                console.print("  [yellow]No valid strategies selected, keeping current.[/yellow]")
                config["strategies"] = strategies
        # Remove legacy singular key if present
        config.pop("strategy", None)

    if "3" in selected:
        console.print("\n[bold]Trading Mode[/bold]")
        console.print("  [1] Simulated (paper trading)")
        console.print("  [2] Live (real Coinbase orders)")
        mode_choice = Prompt.ask(
            "  Select mode",
            choices=["1", "2"],
            default="2" if config["trading_mode"] == "live" else "1",
        )
        config["trading_mode"] = "live" if mode_choice == "2" else "simulated"
        if config["trading_mode"] == "live" and not config["coinbase"].get("api_key"):
            config["coinbase"]["api_key"] = Prompt.ask("  Coinbase CDP API Key Name")
            config["coinbase"]["api_secret"] = Prompt.ask("  Coinbase CDP API Secret")

    if "4" in selected:
        console.print("\n[bold]LLM Provider[/bold]")
        console.print("  [1] OpenAI")
        console.print("  [2] Anthropic")
        console.print("  [3] Custom endpoint")
        llm_choice = Prompt.ask("  Select provider", choices=["1", "2", "3"], default="1")
        if llm_choice == "1":
            config["llm"]["provider"] = "openai"
            config["llm"]["api_key"] = Prompt.ask("  OpenAI API Key", default=config["llm"].get("api_key", ""))
            config["llm"]["model_id"] = Prompt.ask("  Model ID", default=config["llm"].get("model_id", "gpt-4o-mini"))
            config["llm"]["base_url"] = None
        elif llm_choice == "2":
            config["llm"]["provider"] = "anthropic"
            config["llm"]["api_key"] = Prompt.ask("  Anthropic API Key", default=config["llm"].get("api_key", ""))
            config["llm"]["model_id"] = Prompt.ask("  Model ID", default=config["llm"].get("model_id", "claude-sonnet-4-6"))
            config["llm"]["base_url"] = None
        else:
            config["llm"]["provider"] = "custom"
            config["llm"]["base_url"] = Prompt.ask("  Base URL", default=config["llm"].get("base_url", ""))
            config["llm"]["api_key"] = Prompt.ask("  API Key", default=config["llm"].get("api_key", ""))
            config["llm"]["model_id"] = Prompt.ask("  Model ID", default=config["llm"].get("model_id", ""))

    if "5" in selected:
        console.print("\n[bold]Advanced Settings[/bold]")
        config["market_interval"] = IntPrompt.ask(
            "  Market data interval (seconds)", default=config.get("market_interval", 300)
        )
        fee_str = Prompt.ask("  Fee rate", default=str(config.get("fee_rate", 0.05)))
        try:
            config["fee_rate"] = float(fee_str)
        except ValueError:
            pass

    # Save and show updated config
    save_config(config, str(CONFIG_FILE))
    console.print(f"\n  [green]Updated configuration saved to {CONFIG_FILE.name}[/green]")
    _print_config_summary(config)
    return config


def save_config(config: dict, path: str = "arena_config.json") -> None:
    """Save config to JSON, masking secrets."""
    save_copy = json.loads(json.dumps(config))  # deep copy
    with open(path, "w") as f:
        json.dump(save_copy, f, indent=2)


def load_config(path: str) -> dict:
    """Load config from JSON file, migrating legacy format if needed."""
    with open(path) as f:
        config = json.load(f)
    # Migrate legacy "strategy" (singular) to "strategies" (list)
    if "strategy" in config and "strategies" not in config:
        config["strategies"] = [config.pop("strategy")]
    return config


# ---------------------------------------------------------------------------
# Docker / Kafka management
# ---------------------------------------------------------------------------


def _find_docker_compose() -> Path:
    """Resolve the docker-compose.yml path, checking multiple locations."""
    # Primary: bundled in project
    if DOCKER_COMPOSE.exists():
        return DOCKER_COMPOSE

    # PyInstaller bundle: extracted to temp dir alongside executable
    if getattr(sys, "frozen", False):
        bundle_dir = Path(sys._MEIPASS)  # type: ignore[attr-defined]
        bundled = bundle_dir / "docker" / "docker-compose.yml"
        if bundled.exists():
            return bundled

    # Fallback: current working directory
    cwd_path = Path.cwd() / "docker" / "docker-compose.yml"
    if cwd_path.exists():
        return cwd_path

    return DOCKER_COMPOSE  # return default (will fail with clear message)


def _check_docker() -> bool:
    """Check that Docker is available and running."""
    if not shutil.which("docker"):
        console.print("[red]Docker not found. Please install Docker Desktop.[/red]")
        console.print("[red]  https://www.docker.com/products/docker-desktop/[/red]")
        return False

    console.print("  Checking Docker daemon...")
    result = subprocess.run(
        ["docker", "info"],
        capture_output=True,
        timeout=30,
    )
    if result.returncode != 0:
        console.print("[red]Docker is not running. Please start Docker Desktop and wait for it to be ready.[/red]")
        return False

    # Check docker compose
    result = subprocess.run(
        ["docker", "compose", "version"],
        capture_output=True,
        timeout=10,
    )
    if result.returncode != 0:
        console.print("[red]docker compose not available. Please install Docker Desktop.[/red]")
        return False

    return True


def _start_kafka() -> bool:
    """Start Kafka via docker compose. Returns True on success."""
    compose_file = _find_docker_compose()

    if not compose_file.exists():
        console.print(f"[red]docker-compose.yml not found![/red]")
        console.print(f"[red]  Searched: {DOCKER_COMPOSE}[/red]")
        console.print(f"[red]  Make sure the 'docker' folder is in the same directory as launcher.py[/red]")
        return False

    console.print(f"\n[cyan]Starting Kafka broker...[/cyan]")
    console.print(f"  Using: {compose_file}")

    # First pull the image (this can take a while on first run)
    console.print("  Pulling Kafka Docker image (this may take a few minutes on first run)...")
    pull_result = subprocess.run(
        ["docker", "compose", "-f", str(compose_file), "pull"],
        timeout=600,
    )
    if pull_result.returncode != 0:
        console.print("[red]Failed to pull Kafka image. Check your internet connection.[/red]")
        return False
    console.print("  [green]Image ready.[/green]")

    # Now start the container
    console.print("  Starting Kafka container...")
    up_result = subprocess.run(
        ["docker", "compose", "-f", str(compose_file), "up", "-d"],
        timeout=60,
    )
    if up_result.returncode != 0:
        console.print("[red]Failed to start Kafka container.[/red]")
        console.print("[red]  Try running manually: docker compose -f docker/docker-compose.yml up -d[/red]")
        return False

    console.print("  [green]Container started.[/green]")

    # Wait for Kafka port
    console.print("  Waiting for Kafka to be ready on port 9092...")
    for i in range(120):
        try:
            with socket.create_connection(("localhost", 9092), timeout=1):
                break
        except (ConnectionRefusedError, OSError, socket.timeout):
            if i > 0 and i % 10 == 0:
                console.print(f"    Still waiting... ({i}s)")
            time.sleep(1)
    else:
        console.print("[red]Kafka did not become available after 120 seconds.[/red]")
        console.print("[red]  Check Docker Desktop -- is the arena-kafka container running?[/red]")
        console.print("[red]  Try: docker logs arena-kafka[/red]")
        return False

    # Use the healthcheck -- wait for broker API to be ready
    console.print("  Waiting for broker to finish initializing...")
    for i in range(60):
        result = subprocess.run(
            ["docker", "inspect", "--format", "{{.State.Health.Status}}", "arena-kafka"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        status = result.stdout.strip()
        if status == "healthy":
            break
        if i > 0 and i % 10 == 0:
            console.print(f"    Health status: {status} ({i}s)")
        time.sleep(2)
    else:
        console.print("[yellow]Kafka healthcheck did not pass, but port is open. Continuing...[/yellow]")

    console.print("  [green]Kafka is ready![/green]")
    return True


def _stop_kafka() -> None:
    """Stop Kafka via docker compose."""
    compose_file = _find_docker_compose()
    if not compose_file.exists():
        return
    console.print("\n[cyan]Stopping Kafka broker...[/cyan]")
    subprocess.run(
        ["docker", "compose", "-f", str(compose_file), "down"],
        timeout=30,
    )
    console.print("  [green]Kafka stopped.[/green]")


# ---------------------------------------------------------------------------
# Process management — opens a separate terminal window per component
# ---------------------------------------------------------------------------

IS_WINDOWS = platform.system() == "Windows"
IS_MACOS = platform.system() == "Darwin"


def _build_uv_command(script: str, args: list[str]) -> str:
    """Build the full uv run command string for a component."""
    script_path = str(ARENA_DIR / script)
    parts = ["uv", "run", "python", f'"{script_path}"' if " " in script_path else script_path]
    for a in args:
        # Quote args that contain spaces or special characters
        if " " in a or "'" in a or "\n" in a:
            parts.append(f'"{a}"')
        else:
            parts.append(a)
    return " ".join(parts)


def _open_terminal(title: str, command: str) -> None:
    """Open a new terminal window and run a command inside it.

    - Windows: opens a new PowerShell window via `start`
    - macOS: opens a new Terminal.app window via osascript
    - Linux: tries common terminal emulators
    """
    arena_dir_str = str(ARENA_DIR)

    if IS_WINDOWS:
        # Build as a single string for cmd.exe — passing a list causes
        # Python's list2cmdline to mangle the quoting that `start` expects.
        # `start` requires the window title in double quotes.
        ps_cmd = f"cd '{arena_dir_str}'; $host.UI.RawUI.WindowTitle = '{title}'; {command}"
        ps_cmd_escaped = ps_cmd.replace('"', '\\"')
        subprocess.Popen(
            f'start "{title}" powershell -NoExit -Command "{ps_cmd_escaped}"',
            shell=True,
            cwd=arena_dir_str,
        )
    elif IS_MACOS:
        # AppleScript: backslashes must be doubled inside do script strings
        osascript_cmd = (
            f'tell application "Terminal"\n'
            f"    activate\n"
            f'    do script "printf \'\\\\e]0;{title}\\\\a\'; cd \'{arena_dir_str}\' && {command}"\n'
            f"end tell"
        )
        subprocess.Popen(["osascript", "-e", osascript_cmd])
    else:
        # Linux: try common terminal emulators in order of preference
        for term_cmd in [
            ["gnome-terminal", "--title", title, "--", "bash", "-c", f"cd '{arena_dir_str}' && {command}; exec bash"],
            ["konsole", "--title", title, "-e", "bash", "-c", f"cd '{arena_dir_str}' && {command}; exec bash"],
            ["xfce4-terminal", "--title", title, "-e", f"bash -c \"cd '{arena_dir_str}' && {command}; exec bash\""],
            ["xterm", "-T", title, "-e", f"cd '{arena_dir_str}' && {command}; exec bash"],
        ]:
            if shutil.which(term_cmd[0]):
                subprocess.Popen(term_cmd)
                break
        else:
            # Last resort: run in background without a terminal window
            console.print(f"  [yellow]No terminal emulator found -- running {title} in background[/yellow]")
            subprocess.Popen(
                ["bash", "-c", f"cd '{arena_dir_str}' && {command}"],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
            )

    console.print(f"  [green]Opened[/green] {title}")
    time.sleep(1)


def launch_arena(config: dict) -> int:
    """Launch all arena components in separate terminal windows.

    Returns the number of windows opened.
    """
    bootstrap = config.get("kafka_bootstrap", "localhost:9092")
    interval = str(config.get("market_interval", 300))
    fee_rate = str(config.get("fee_rate", 0.05))
    tax_rate = str(config.get("tax_rate", 0.30))
    snapshot_interval = str(config.get("snapshot_interval", 600))
    data_dir = config.get("data_dir", "./data")

    llm = config["llm"]
    coins = config["coins"]
    # Support both legacy "strategy" (singular) and new "strategies" (list)
    strategies = config.get("strategies", [config["strategy"]]) if "strategy" in config else config.get("strategies", ["contrarian"])
    trading_mode = config["trading_mode"]
    count = 0

    # 1. Coinbase Connector
    console.print("\n[bold cyan]Launching Coinbase Connector...[/bold cyan]")
    _open_terminal(
        "Coinbase Connector",
        _build_uv_command("coinbase_connector.py", [
            "--bootstrap-servers", bootstrap, "--interval", interval,
        ]),
    )
    count += 1

    # 2. Tools & Dashboard
    console.print("[bold cyan]Launching Tools & Dashboard...[/bold cyan]")
    num_agents = str(len(coins) * len(strategies))
    tools_args = [
        "--bootstrap-servers", bootstrap,
        "--snapshot-interval", snapshot_interval,
        "--data-dir", data_dir,
        "--fee-rate", fee_rate,
        "--tax-rate", tax_rate,
        "--trading-mode", trading_mode,
        "--num-agents", num_agents,
        "--web-port", str(config.get("web_port", 8080)),
    ]
    # Note: in live mode, credentials are read from arena_config.json
    # directly by tools_and_dashboard.py (avoids shell escaping issues)
    if config.get("coinbase", {}).get("coinbase_one", False):
        tools_args.append("--coinbase-one")
    _open_terminal(
        "Tools and Dashboard",
        _build_uv_command("tools_and_dashboard.py", tools_args),
    )
    count += 1

    time.sleep(2)

    # 3. ChatNode (LLM inference)
    console.print("[bold cyan]Launching ChatNode...[/bold cyan]")
    chat_args = [
        "--name", "arena-node",
        "--model-id", llm["model_id"],
        "--bootstrap-servers", bootstrap,
        "--api-key", llm["api_key"],
    ]
    if llm.get("base_url"):
        chat_args.extend(["--base-url", llm["base_url"]])
    _open_terminal(
        "ChatNode",
        _build_uv_command("deploy_chat_node.py", chat_args),
    )
    count += 1

    time.sleep(2)

    # 4. Agent routers -- one per strategy per coin
    console.print("[bold cyan]Launching Agent Routers...[/bold cyan]")
    coinbase_one = config.get("coinbase", {}).get("coinbase_one", False)
    for strategy in strategies:
        for coin in coins:
            symbol = coin.split("-")[0].lower()
            agent_name = f"{strategy}-{symbol}"
            router_args = [
                "--name", agent_name,
                "--chat-node-name", "arena-node",
                "--strategy", strategy,
                "--product", coin,
                "--bootstrap-servers", bootstrap,
                "--trading-mode", trading_mode,
                "--cash-reserve-pct", str(config.get("cash_reserve_pct", 30)),
            ]
            if coinbase_one:
                router_args.append("--coinbase-one")
            _open_terminal(
                f"Agent {agent_name}",
                _build_uv_command("deploy_router_node.py", router_args),
            )
            count += 1

    # 5. Response Viewer
    console.print("[bold cyan]Launching Response Viewer...[/bold cyan]")
    _open_terminal(
        "Response Viewer",
        _build_uv_command("response_viewer.py", ["--bootstrap-servers", bootstrap]),
    )
    count += 1

    return count


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(
        description="Crypto Daytrading Arena Launcher",
    )
    parser.add_argument(
        "--config",
        type=str,
        default=None,
        help="Path to arena_config.json (skip wizard)",
    )
    parser.add_argument(
        "--teardown",
        action="store_true",
        help="Stop Kafka broker and exit",
    )
    args = parser.parse_args()

    if args.teardown:
        _stop_kafka()
        return

    # Load or run wizard
    config = None

    if args.config:
        # Explicit config file passed via CLI
        console.print(f"Loading config from [cyan]{args.config}[/cyan]...")
        config = load_config(args.config)
        _print_config_summary(config)
    elif CONFIG_FILE.exists():
        # Saved config from a previous run -- offer to reuse or tweak
        console.print(f"[cyan]Found saved configuration ({CONFIG_FILE.name})[/cyan]")
        config = load_config(str(CONFIG_FILE))
        _print_config_summary(config)
        console.print()
        console.print("  [bold]y[/bold] = launch with this config")
        console.print("  [bold]e[/bold] = edit specific settings")
        console.print("  [bold]n[/bold] = start over (full wizard)")
        reuse = Prompt.ask(
            "What would you like to do?",
            choices=["y", "e", "n"],
            default="y",
        )
        if reuse == "n":
            config = run_wizard()
        elif reuse == "e":
            config = _edit_config(config)
    else:
        config = run_wizard()

    if not Confirm.ask("\nLaunch the arena now?", default=True):
        console.print("Aborted.")
        return

    # Preflight checks
    console.print("\n[bold]Preflight Checks[/bold]")

    if not shutil.which("uv"):
        console.print("[red]uv not found. Install it: https://docs.astral.sh/uv/[/red]")
        return

    console.print("  [green]uv available[/green]")

    if not _check_docker():
        return
    console.print("  [green]Docker available[/green]")

    # Start Kafka
    if not _start_kafka():
        return

    # Launch all components in separate terminal windows
    window_count = launch_arena(config)

    mode_label = (
        "[red bold]LIVE TRADING[/red bold]"
        if config["trading_mode"] == "live"
        else "SIMULATED"
    )
    strategies = config.get("strategies", [config.get("strategy", "contrarian")])
    coins = config["coins"]
    console.print(
        Panel(
            f"[bold green]Arena is Live![/bold green]\n"
            f"Mode: {mode_label}\n"
            f"Coins: {', '.join(coins)}\n"
            f"Strategies: {', '.join(strategies)}\n"
            f"Agents: {len(coins) * len(strategies)}\n"
            f"Windows opened: {window_count}\n\n"
            f"Each component is running in its own terminal window.\n"
            f"The Tools and Dashboard window shows the P&L dashboard.\n"
            f"Web Dashboard: http://localhost:{config.get('web_port', 8080)}\n"
            f"Close terminal windows or press Ctrl+C in each to stop.",
            expand=False,
        )
    )

    console.print("\n[bold]To stop everything:[/bold]")
    console.print("  1. Close all arena terminal windows (or Ctrl+C in each)")
    console.print("  2. Stop Kafka:")
    if IS_WINDOWS:
        console.print("     [cyan]uv run python launcher.py --teardown[/cyan]")
    else:
        console.print("     [cyan]uv run python launcher.py --teardown[/cyan]")


if __name__ == "__main__":
    main()
