#!/usr/bin/env bash
# =============================================================================
# launch_pi.sh — Start the Crypto Daytrading Arena on Raspberry Pi
#
# Uses tmux tabs instead of Terminal.app. Run from the project directory.
#
# Usage:
#   ./launch_pi.sh              # launch everything
#   ./launch_pi.sh --teardown   # stop everything
#
# Requires: arena.env file with API keys, Docker running, tmux installed
# =============================================================================

set -euo pipefail

ARENA_DIR="$(cd "$(dirname "$0")" && pwd)"
BS="localhost:9092"
INTERVAL=300

# ---------------------------------------------------------------------------
# Handle --teardown
# ---------------------------------------------------------------------------
if [[ "${1:-}" == "--teardown" ]]; then
    echo "Stopping arena..."
    tmux kill-session -t arena 2>/dev/null || true
    (cd "$ARENA_DIR" && docker compose -f docker/docker-compose.yml down 2>/dev/null) || true
    echo "Done."
    exit 0
fi

# ---------------------------------------------------------------------------
# Preflight
# ---------------------------------------------------------------------------
if [[ ! -f "$ARENA_DIR/arena.env" ]]; then
    echo "ERROR: Missing arena.env — copy arena.env.example and fill in your keys"
    exit 1
fi

source "$ARENA_DIR/arena.env"

if ! command -v tmux &>/dev/null; then
    echo "Installing tmux..."
    sudo apt install -y tmux
fi

if ! docker info &>/dev/null; then
    echo "ERROR: Docker is not running"
    exit 1
fi

# ---------------------------------------------------------------------------
# Start Kafka
# ---------------------------------------------------------------------------
echo "Starting Kafka broker..."
(cd "$ARENA_DIR" && docker compose -f docker/docker-compose.yml up -d)

echo "Waiting for Kafka at $BS..."
while ! nc -z localhost 9092 2>/dev/null; do sleep 1; done
sleep 5
echo "Kafka ready."

# ---------------------------------------------------------------------------
# Launch tmux session with all components
# ---------------------------------------------------------------------------
tmux kill-session -t arena 2>/dev/null || true
tmux new-session -d -s arena -n connector

# Coinbase connector
tmux send-keys -t arena:connector \
    "cd $ARENA_DIR && uv run python coinbase_connector.py --bootstrap-servers $BS --interval $INTERVAL" Enter

# Tools & Dashboard
tmux new-window -t arena -n dashboard
tmux send-keys -t arena:dashboard \
    "cd $ARENA_DIR && uv run python tools_and_dashboard.py --bootstrap-servers $BS" Enter

sleep 3

# ChatNode
tmux new-window -t arena -n chatnode
tmux send-keys -t arena:chatnode \
    "cd $ARENA_DIR && uv run python deploy_chat_node.py --name mini-node --model-id gpt-5-mini-2025-07-18 --bootstrap-servers $BS --api-key $OPENAI_API_KEY" Enter

sleep 3

# Agents (one per coin)
for coin in BTC ETH SOL LTC DOGE LINK XRP; do
    lower=$(echo "$coin" | tr '[:upper:]' '[:lower:]')
    tmux new-window -t arena -n "$lower"
    tmux send-keys -t arena:"$lower" \
        "cd $ARENA_DIR && uv run python deploy_router_node.py --name contrarian-$lower --chat-node-name mini-node --strategy contrarian --product ${coin}-USD --bootstrap-servers $BS" Enter
done

# Response viewer
tmux new-window -t arena -n viewer
tmux send-keys -t arena:viewer \
    "cd $ARENA_DIR && uv run python response_viewer.py --bootstrap-servers $BS" Enter

# ---------------------------------------------------------------------------
# Done
# ---------------------------------------------------------------------------
echo ""
echo "========================================="
echo "  Arena launched in tmux session 'arena'"
echo "========================================="
echo ""
echo "  Attach:       tmux attach -t arena"
echo "  Next tab:     Ctrl+B then n"
echo "  Prev tab:     Ctrl+B then p"
echo "  Jump to tab:  Ctrl+B then 0-9"
echo "  Detach:       Ctrl+B then d"
echo "  Stop:         ./launch_pi.sh --teardown"
echo ""
