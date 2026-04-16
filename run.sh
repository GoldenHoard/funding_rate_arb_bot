#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# Funding Rate Arb — Cron Runner
# ─────────────────────────────────────────────────────────────────────────────
# Called by crontab every 8 hours. Runs closer first, then opener.
#
# Usage:
#   ./run.sh              — run both closer + opener (normal cron usage)
#   ./run.sh opener       — run opener only
#   ./run.sh closer       — run closer only
# ─────────────────────────────────────────────────────────────────────────────

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

# Load environment variables from .env
if [[ -f .env ]]; then
    set -a
    source .env
    set +a
else
    echo "ERROR: .env file not found in $SCRIPT_DIR" >&2
    exit 1
fi

LOG_DIR="$SCRIPT_DIR/logs"
mkdir -p "$LOG_DIR"

TIMESTAMP=$(date -u +"%Y%m%d_%H%M%S")

run_closer() {
    echo "[$TIMESTAMP] Running closer..." | tee -a "$LOG_DIR/closer.log"
    python3 "$SCRIPT_DIR/close_lambda.py" >> "$LOG_DIR/closer.log" 2>&1
    echo "[$TIMESTAMP] Closer finished with exit code $?" | tee -a "$LOG_DIR/closer.log"
}

run_opener() {
    echo "[$TIMESTAMP] Running opener..." | tee -a "$LOG_DIR/opener.log"
    python3 "$SCRIPT_DIR/lambda_function.py" >> "$LOG_DIR/opener.log" 2>&1
    echo "[$TIMESTAMP] Opener finished with exit code $?" | tee -a "$LOG_DIR/opener.log"
}

MODE="${1:-both}"

case "$MODE" in
    closer)
        run_closer
        ;;
    opener)
        run_opener
        ;;
    both|*)
        run_closer
        sleep 5
        run_opener
        ;;
esac
