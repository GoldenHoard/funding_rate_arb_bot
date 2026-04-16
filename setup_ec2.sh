#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# EC2 Instance Setup Script
# ─────────────────────────────────────────────────────────────────────────────
# Run this ONCE after SSH-ing into a fresh Amazon Linux 2023 / AL2 instance.
#
# Prerequisites:
#   1. Launch a t4g.nano (ARM) or t3.micro (x86) in ap-northeast-1
#   2. Attach an Elastic IP
#   3. Security Group: allow SSH (port 22) from your IP only
#   4. Attach IAM Role with AmazonDynamoDBFullAccess_v2
#   5. SSH in: ssh -i your-key.pem ec2-user@<elastic-ip>
#   6. Run: bash setup_ec2.sh
#
# After setup:
#   1. Copy .env.example to .env and fill in your keys
#   2. Add the Elastic IP to your Binance API whitelist
#   3. Test: ./run.sh
#   4. Cron will auto-run every 8 hours
# ─────────────────────────────────────────────────────────────────────────────

set -euo pipefail

log()  { echo "▶ $*"; }
ok()   { echo "✓ $*"; }

APP_DIR="$HOME/funding_rate_arb_bot"

# ─────────────────────────────────────────────────────────────────────────────
# Step 1: System packages
# ─────────────────────────────────────────────────────────────────────────────

log "Updating system packages..."
sudo dnf update -y -q

log "Installing Python 3.12 and git..."
sudo dnf install -y -q python3.12 python3.12-pip git

ok "System packages installed."

# ─────────────────────────────────────────────────────────────────────────────
# Step 2: Clone or update the bot code
# ─────────────────────────────────────────────────────────────────────────────

if [[ -d "$APP_DIR" ]]; then
    log "App directory exists — pulling latest..."
    cd "$APP_DIR"
    git pull || true
else
    log "Creating app directory..."
    mkdir -p "$APP_DIR"
fi

cd "$APP_DIR"
ok "Working directory: $APP_DIR"

# ─────────────────────────────────────────────────────────────────────────────
# Step 3: Install Python dependencies
# ─────────────────────────────────────────────────────────────────────────────

log "Installing Python dependencies..."
python3.12 -m pip install --upgrade pip -q
python3.12 -m pip install ccxt aiohttp boto3 -q

ok "Python dependencies installed."

# ─────────────────────────────────────────────────────────────────────────────
# Step 4: AWS CLI config (for DynamoDB access via instance role)
# ─────────────────────────────────────────────────────────────────────────────

log "Configuring AWS region..."
mkdir -p ~/.aws
cat > ~/.aws/config << 'AWSEOF'
[default]
region = ap-northeast-1
output = json
AWSEOF

ok "AWS region set to ap-northeast-1."

# ─────────────────────────────────────────────────────────────────────────────
# Step 5: Make scripts executable
# ─────────────────────────────────────────────────────────────────────────────

chmod +x run.sh 2>/dev/null || true

# ─────────────────────────────────────────────────────────────────────────────
# Step 6: Set up crontab — every 8 hours aligned to funding windows
# ─────────────────────────────────────────────────────────────────────────────

log "Setting up crontab..."

# Funding windows: 00:00, 08:00, 16:00 UTC
# We run 10 minutes before each window
CRON_LINE="50 7,15,23 * * * cd $APP_DIR && ./run.sh >> $APP_DIR/logs/cron.log 2>&1"

# Install crontab (preserve existing entries)
(crontab -l 2>/dev/null | grep -v "funding_rate_arb_bot" || true; echo "$CRON_LINE") | crontab -

ok "Crontab installed: runs at 07:50, 15:50, 23:50 UTC"

# ─────────────────────────────────────────────────────────────────────────────
# Step 7: Create .env from example if it doesn't exist
# ─────────────────────────────────────────────────────────────────────────────

if [[ ! -f .env ]]; then
    if [[ -f .env.example ]]; then
        cp .env.example .env
        log "Created .env from .env.example — EDIT IT NOW with your real keys:"
        log "  nano $APP_DIR/.env"
    else
        log "WARNING: No .env.example found. Create .env manually."
    fi
else
    ok ".env already exists."
fi

# ─────────────────────────────────────────────────────────────────────────────
# Done
# ─────────────────────────────────────────────────────────────────────────────

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  EC2 Setup Complete!"
echo ""
echo "  Next steps:"
echo "    1. Edit .env with your real API keys:"
echo "       nano $APP_DIR/.env"
echo ""
echo "    2. Add this instance's Elastic IP to Binance API whitelist"
echo ""
echo "    3. Test manually:"
echo "       cd $APP_DIR && ./run.sh"
echo ""
echo "    4. Check logs:"
echo "       tail -f $APP_DIR/logs/opener.log"
echo "       tail -f $APP_DIR/logs/closer.log"
echo ""
echo "  Cron schedule (UTC):"
echo "    07:50 — Closer + Opener (before 08:00 funding)"
echo "    15:50 — Closer + Opener (before 16:00 funding)"
echo "    23:50 — Closer + Opener (before 00:00 funding)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
