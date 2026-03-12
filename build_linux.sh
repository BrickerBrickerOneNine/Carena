#!/usr/bin/env bash
# Build the Crypto Daytrading Arena executable for Linux/macOS.
# Requires: pip install pyinstaller
# Output: dist/arena

set -euo pipefail

echo "Building Crypto Daytrading Arena..."

pip install pyinstaller 2>/dev/null || true
pyinstaller launcher.spec --noconfirm

echo ""
echo "Build successful! Executable at: dist/arena"
echo ""
echo "Usage:"
echo "  ./dist/arena                              # interactive wizard"
echo "  ./dist/arena --config arena_config.json   # headless launch"
echo "  ./dist/arena --teardown                   # stop Kafka"
