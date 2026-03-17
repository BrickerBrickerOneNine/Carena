#!/usr/bin/env bash
# =============================================================================
# launch_arena.sh — Start the entire Crypto Daytrading Arena
#
# Opens a single Terminal.app window with a tab for each component.
#
# Usage:
#   ./launch_arena.sh              # launch everything
#   ./launch_arena.sh --teardown   # stop broker + close windows
#
# Requires: arena.env file with API keys (see arena.env.example)
# =============================================================================

set -euo pipefail

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BROKER_DIR="$HOME/calfkit-broker"
ARENA_DIR="$SCRIPT_DIR"
ENV_FILE="$ARENA_DIR/arena.env"
BOOTSTRAP="localhost:9092"
MARKET_INTERVAL=300  # seconds between market data pushes

# ---------------------------------------------------------------------------
# Colors
# ---------------------------------------------------------------------------
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

log()   { echo -e "${GREEN}[ARENA]${NC} $1"; }
warn()  { echo -e "${YELLOW}[ARENA]${NC} $1"; }
error() { echo -e "${RED}[ARENA]${NC} $1"; }
header(){ echo -e "\n${BOLD}${BLUE}═══════════════════════════════════════════════════${NC}"; echo -e "${BOLD}${BLUE}  $1${NC}"; echo -e "${BOLD}${BLUE}═══════════════════════════════════════════════════${NC}"; }

# ---------------------------------------------------------------------------
# Handle --teardown
# ---------------------------------------------------------------------------
if [[ "${1:-}" == "--teardown" ]]; then
    header "Tearing Down Arena"
    log "Stopping Kafka broker..."
    (cd "$BROKER_DIR" && make dev-down 2>/dev/null) || true
    log "Broker stopped. Close any remaining Terminal windows manually."
    exit 0
fi

# ---------------------------------------------------------------------------
# Preflight checks
# ---------------------------------------------------------------------------
header "Preflight Checks"

# Check arena.env exists
if [[ ! -f "$ENV_FILE" ]]; then
    error "Missing $ENV_FILE"
    error "Create it from the template:"
    error "  cp arena.env.example arena.env"
    error "Then fill in your API keys."
    exit 1
fi

# Load environment variables
set -a
source "$ENV_FILE"
set +a

# Validate API keys
if [[ -z "${OPENAI_API_KEY:-}" || "$OPENAI_API_KEY" == sk-your-* ]]; then
    error "OPENAI_API_KEY is not set or still has the placeholder value in $ENV_FILE"
    exit 1
fi

if [[ -z "${ANTHROPIC_API_KEY:-}" || "$ANTHROPIC_API_KEY" == sk-ant-your-* ]]; then
    error "ANTHROPIC_API_KEY is not set or still has the placeholder value in $ENV_FILE"
    exit 1
fi
log "API keys loaded ✓"

# Check Docker — auto-launch if not running
if ! docker info &>/dev/null; then
    warn "Docker is not running. Launching Docker Desktop..."
    open -a Docker
    docker_wait=0
    while ! docker info &>/dev/null; do
        docker_wait=$((docker_wait + 2))
        if [[ $docker_wait -ge 60 ]]; then
            error "Docker did not start after 60s. Open it manually and retry."
            exit 1
        fi
        sleep 2
    done
    log "Docker Desktop started ✓"
else
    log "Docker running ✓"
fi

# Check docker compose
if ! docker compose version &>/dev/null; then
    error "docker compose not found. Make sure Docker Desktop is installed (not just the CLI)."
    exit 1
fi
log "Docker Compose available ✓"

# Check broker repo
if [[ ! -d "$BROKER_DIR" ]]; then
    error "Broker repo not found at $BROKER_DIR"
    exit 1
fi
log "Broker repo found ✓"

# Check uv
if ! command -v uv &>/dev/null; then
    error "uv not found. Install it: brew install uv"
    exit 1
fi
log "uv available ✓"

# ---------------------------------------------------------------------------
# Helper: build a single AppleScript to open all tabs at once
# ---------------------------------------------------------------------------
_TABSCRIPT=""

# Escape a string for inclusion in an AppleScript double-quoted string
_esc() {
    local s="$1"
    s="${s//\\/\\\\}"   # \ → \\
    s="${s//\"/\\\"}"   # " → \"
    printf '%s' "$s"
}

# Open the first tab (creates a new Terminal window)
open_first_tab() {
    local title="$1"
    local cmd="$2"
    log "Opening window: ${CYAN}${title}${NC}"
    local esc
    esc=$(_esc "printf '\e]0;${title}\a'; ${cmd}")
    _TABSCRIPT=$(mktemp /tmp/arena_launch.XXXXXX)
    printf 'tell application "Terminal"\n    activate\n    do script "%s"\nend tell\n' "$esc" > "$_TABSCRIPT"
}

# Queue a new tab to be opened in the existing window
queue_tab() {
    local title="$1"
    local cmd="$2"
    log "  Queued tab: ${CYAN}${title}${NC}"
    local esc
    esc=$(_esc "printf '\e]0;${title}\a'; ${cmd}")
    printf 'tell application "System Events"\n' >> "$_TABSCRIPT"
    printf '    tell process "Terminal"\n' >> "$_TABSCRIPT"
    printf '        keystroke "t" using {command down}\n' >> "$_TABSCRIPT"
    printf '    end tell\n' >> "$_TABSCRIPT"
    printf 'end tell\n' >> "$_TABSCRIPT"
    printf 'delay 0.5\n' >> "$_TABSCRIPT"
    printf 'tell application "Terminal"\n' >> "$_TABSCRIPT"
    printf '    do script "%s" in selected tab of front window\n' "$esc" >> "$_TABSCRIPT"
    printf 'end tell\n' >> "$_TABSCRIPT"
    printf 'delay 0.3\n' >> "$_TABSCRIPT"
}

# Add a delay between groups (embedded in the AppleScript)
queue_delay() {
    printf 'delay %s\n' "$1" >> "$_TABSCRIPT"
}

# Execute the built-up AppleScript and clean up
flush_tabs() {
    log "Launching all tabs..."
    if ! osascript "$_TABSCRIPT" 2>/dev/null; then
        warn "Tab creation may have failed."
        warn "Grant Terminal accessibility permissions:"
        warn "  System Settings → Privacy & Security → Accessibility → Terminal"
    fi
    rm -f "$_TABSCRIPT"
}

# ===========================================================================
# LAUNCH SEQUENCE
# ===========================================================================

header "Step 1/6 — Kafka Broker"

# Kafka gets the first tab (creates a new Terminal window)
open_first_tab "Kafka Broker" \
    "cd $BROKER_DIR && make dev-up"
osascript "$_TABSCRIPT" 2>/dev/null
# Don't rm — we'll keep building on this file for remaining tabs

log "Waiting for Kafka broker at $BOOTSTRAP..."
kafka_wait=0
while ! nc -z localhost 9092 2>/dev/null; do
    kafka_wait=$((kafka_wait + 1))
    if [[ $kafka_wait -ge 90 ]]; then
        error "Kafka did not become available after 90s"
        exit 1
    fi
    sleep 1
done
log "Kafka port is open — waiting for broker to finish initializing..."
sleep 5
log "Kafka is ready ✓"

# ---------------------------------------------------------------------------
header "Steps 2-6 — Remaining Components"

# Reset the script file for the remaining tabs
: > "$_TABSCRIPT"

# Step 2: Coinbase Market Data
queue_tab "Coinbase Connector" \
    "cd $ARENA_DIR && uv run python coinbase_connector.py --bootstrap-servers $BOOTSTRAP --interval $MARKET_INTERVAL"

# Step 3: Tools & Dashboard
queue_tab "Tools and Dashboard" \
    "cd $ARENA_DIR && uv run python tools_and_dashboard.py --bootstrap-servers $BOOTSTRAP"

queue_delay 3

# Step 4: ChatNode
queue_tab "ChatNode GPT-5 Mini" \
    "cd $ARENA_DIR && uv run python deploy_chat_node.py --name mini-node --model-id gpt-5-mini-2025-07-18 --bootstrap-servers $BOOTSTRAP --api-key $OPENAI_API_KEY"

queue_delay 3

# Step 5: Agent Routers (1 agent per coin, contrarian strategy)
queue_tab "Agent contrarian-BTC" \
    "cd $ARENA_DIR && uv run python deploy_router_node.py --name contrarian-btc --chat-node-name mini-node --strategy contrarian --product BTC-USD --bootstrap-servers $BOOTSTRAP"

queue_tab "Agent contrarian-ETH" \
    "cd $ARENA_DIR && uv run python deploy_router_node.py --name contrarian-eth --chat-node-name mini-node --strategy contrarian --product ETH-USD --bootstrap-servers $BOOTSTRAP"

queue_tab "Agent contrarian-SOL" \
    "cd $ARENA_DIR && uv run python deploy_router_node.py --name contrarian-sol --chat-node-name mini-node --strategy contrarian --product SOL-USD --bootstrap-servers $BOOTSTRAP"

queue_tab "Agent contrarian-LTC" \
    "cd $ARENA_DIR && uv run python deploy_router_node.py --name contrarian-ltc --chat-node-name mini-node --strategy contrarian --product LTC-USD --bootstrap-servers $BOOTSTRAP"

queue_tab "Agent contrarian-DOGE" \
    "cd $ARENA_DIR && uv run python deploy_router_node.py --name contrarian-doge --chat-node-name mini-node --strategy contrarian --product DOGE-USD --bootstrap-servers $BOOTSTRAP"

queue_tab "Agent contrarian-LINK" \
    "cd $ARENA_DIR && uv run python deploy_router_node.py --name contrarian-link --chat-node-name mini-node --strategy contrarian --product LINK-USD --bootstrap-servers $BOOTSTRAP"

queue_tab "Agent contrarian-XRP" \
    "cd $ARENA_DIR && uv run python deploy_router_node.py --name contrarian-xrp --chat-node-name mini-node --strategy contrarian --product XRP-USD --bootstrap-servers $BOOTSTRAP"

# Step 6: Response Viewer
queue_tab "Response Viewer" \
    "cd $ARENA_DIR && uv run python response_viewer.py --bootstrap-servers $BOOTSTRAP"

# Execute all queued tabs in a single AppleScript call
flush_tabs

# ===========================================================================
# Summary
# ===========================================================================

header "Arena is Live!"

echo ""
echo -e "  ${BOLD}12 tabs opened in one Terminal window:${NC}"
echo -e "    ${GREEN}●${NC} Kafka Broker"
echo -e "    ${GREEN}●${NC} Coinbase Connector (${MARKET_INTERVAL}s interval)"
echo -e "    ${GREEN}●${NC} Tools & Dashboard"
echo -e "    ${GREEN}●${NC} ChatNode: mini-node (gpt-5-mini)"
echo -e "    ${GREEN}●${NC} Agent: contrarian-btc  (BTC-USD)"
echo -e "    ${GREEN}●${NC} Agent: contrarian-eth  (ETH-USD)"
echo -e "    ${GREEN}●${NC} Agent: contrarian-sol  (SOL-USD)"
echo -e "    ${GREEN}●${NC} Agent: contrarian-ltc  (LTC-USD)"
echo -e "    ${GREEN}●${NC} Agent: contrarian-doge (DOGE-USD)"
echo -e "    ${GREEN}●${NC} Agent: contrarian-link (LINK-USD)"
echo -e "    ${GREEN}●${NC} Agent: contrarian-xrp  (XRP-USD)"
echo -e "    ${GREEN}●${NC} Response Viewer"
echo ""
echo -e "  ${BOLD}To stop:${NC}"
echo -e "    1. Run: ${YELLOW}./launch_arena.sh --teardown${NC} (stops Kafka)"
echo -e "    2. Close the Terminal window (⌘W) or Ctrl+C in each tab"
echo ""
