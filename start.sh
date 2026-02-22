#!/bin/bash
# ── Generations Study — Aggregated Dashboard Launcher ────────────────────────
# Starts a local HTTP server and opens the aggregated data dashboard.
# Usage:  ./start.sh          (default port 8080)
#         ./start.sh 9090     (custom port)

PORT=${1:-8080}
DIR="$(cd "$(dirname "$0")" && pwd)"
URL="http://localhost:${PORT}/"

echo "Generations Study — Aggregated Data Dashboard"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Serving:  ${DIR}"
echo "  URL:      ${URL}"
echo "  Stop:     Ctrl+C"
echo ""

# Kill any existing server on this port
lsof -ti tcp:${PORT} | xargs kill -9 2>/dev/null

# Open browser after a short delay (let the server start first)
(sleep 0.8 && open "${URL}") &

# Start server (Python 3)
cd "${DIR}" && python3 -m http.server ${PORT}
