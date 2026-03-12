# Crypto Daytrading Arena - Windows Setup Guide

## Prerequisites

You need these installed on your Windows machine:

### 1. Python 3.10+
Download from https://www.python.org/downloads/
- **IMPORTANT:** Check "Add Python to PATH" during installation

### 2. uv (Python package manager)
Open PowerShell and run:
```powershell
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
```
Then close and reopen PowerShell.

### 3. Docker Desktop for Windows
Download from https://www.docker.com/products/docker-desktop/
- Install and start Docker Desktop
- Make sure WSL 2 backend is enabled (Docker will prompt you)

---

## Quick Start

### Step 1: Extract the zip
Extract `crypto-daytrading-arena.zip` to a folder, e.g. `C:\arena`

### Step 2: Open PowerShell
```powershell
cd C:\arena
```

### Step 3: Install dependencies
```powershell
uv sync
```

### Step 4: Launch the arena (interactive wizard)
```powershell
uv run python launcher.py
```

The wizard will walk you through:
1. **Trading mode** - Simulated (paper trading) or Live (real Coinbase orders)
2. **LLM provider** - Enter your OpenAI or Anthropic API key
3. **Coin selection** - Pick which cryptocurrencies to trade
4. **Strategy** - contrarian, momentum, swing, etc.
5. **Advanced settings** - Fee rate, market interval

The launcher automatically starts Kafka (via Docker) and all components.

### Step 5: Stop the arena
Press `Ctrl+C` in the launcher window, or run:
```powershell
uv run python launcher.py --teardown
```

---

## Headless Mode (skip wizard)

After running the wizard once, your config is saved to `arena_config.json`.
Re-launch without the wizard:
```powershell
uv run python launcher.py --config arena_config.json
```

---

## Live Trading Setup (Real Money)

1. Go to https://portal.cdp.coinbase.com/access/api
2. Create a new API key with **trade** permissions
3. Save the API Key Name and API Secret
4. When launching, select "Live" mode and enter your credentials

**WARNING:** Live mode places REAL orders on Coinbase using real money.

---

## Building a Standalone .exe (Optional)

If you want a standalone executable that doesn't require Python/uv:

```powershell
uv pip install pyinstaller
build_windows.bat
```

The executable will be at `dist\arena.exe`. You still need Docker running for Kafka.

---

## Manual Launch (without launcher.py)

If you prefer to run each component in its own PowerShell window:

**Terminal 1 - Start Kafka:**
```powershell
docker compose -f docker\docker-compose.yml up -d
```

**Terminal 2 - Coinbase Connector:**
```powershell
uv run python coinbase_connector.py --bootstrap-servers localhost:9092 --interval 300
```

**Terminal 3 - Tools & Dashboard:**
```powershell
uv run python tools_and_dashboard.py --bootstrap-servers localhost:9092
```

**Terminal 4 - Chat Node (LLM):**
```powershell
$env:OPENAI_API_KEY="sk-your-key-here"
uv run python deploy_chat_node.py --name arena-node --model-id gpt-4o-mini --bootstrap-servers localhost:9092 --api-key $env:OPENAI_API_KEY
```

**Terminal 5+ - Agent Routers (one per coin):**
```powershell
uv run python deploy_router_node.py --name contrarian-btc --chat-node-name arena-node --strategy contrarian --product BTC-USD --bootstrap-servers localhost:9092
uv run python deploy_router_node.py --name contrarian-eth --chat-node-name arena-node --strategy contrarian --product ETH-USD --bootstrap-servers localhost:9092
```

**Stop Kafka when done:**
```powershell
docker compose -f docker\docker-compose.yml down
```

---

## Troubleshooting

| Problem | Solution |
|---------|----------|
| `uv` not found | Reopen PowerShell after installing uv |
| Docker not running | Open Docker Desktop and wait for it to start |
| Port 9092 in use | Run `docker compose -f docker\docker-compose.yml down` first |
| `uv sync` fails | Make sure Python 3.10+ is installed and on PATH |
| Connection refused | Wait 30s after Kafka starts, it needs time to initialize |
