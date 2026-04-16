#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# Funding Rate Arb Bot — AWS Lambda Deployment Script
# ─────────────────────────────────────────────────────────────────────────────
#
# Usage:
#   chmod +x deploy.sh
#   ./deploy.sh [--region ap-east-1] [--profile myprofile] [--dry-run]
#
# What this script does:
#   1. Installs Python dependencies into ./package/ (Lambda-compatible linux build)
#   2. Zips package/ + both Lambda .py files into opener.zip and closer.zip
#   3. Creates or updates two Lambda functions:
#        funding-rate-arb-opener  ← lambda_function.py
#        funding-rate-arb-closer  ← close_lambda.py
#   4. Attaches an EventBridge Scheduler rule to each function
#   5. Sets all required environment variables on both functions
#
# Prerequisites:
#   - AWS CLI v2 installed and configured (aws configure)
#   - Python 3.10+ with pip
#   - An existing IAM role with AWSLambdaBasicExecutionRole
#     Set LAMBDA_ROLE_ARN below or export it as an env var before running
#
# ─────────────────────────────────────────────────────────────────────────────

set -euo pipefail

# ─────────────────────────────────────────────────────────────────────────────
# Configuration — edit these or export as env vars before running
# ─────────────────────────────────────────────────────────────────────────────

AWS_REGION="${AWS_REGION:-ap-east-1}"
AWS_PROFILE="${AWS_PROFILE:-default}"
LAMBDA_ROLE_ARN="${LAMBDA_ROLE_ARN:-arn:aws:iam::YOUR_ACCOUNT_ID:role/YOUR_LAMBDA_ROLE}"
PYTHON_RUNTIME="python3.12"
LAMBDA_TIMEOUT=300        # 5 minutes — plenty for async market ops
LAMBDA_MEMORY=256         # MB — async ccxt is lightweight

OPENER_FUNCTION="funding-rate-arb-opener"
CLOSER_FUNCTION="funding-rate-arb-closer"

# EventBridge cron expressions (UTC)
# Closer runs 10 min before funding, opener runs 5 min before funding
# Funding windows: 08:00, 16:00, 00:00 UTC
CLOSER_CRON="cron(45 7,15,23 * * ? *)"   # 07:45, 15:45, 23:45
OPENER_CRON="cron(55 7,15,23 * * ? *)"   # 07:55, 15:55, 23:55

# ─────────────────────────────────────────────────────────────────────────────
# Parse flags
# ─────────────────────────────────────────────────────────────────────────────

DRY_RUN=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --region)    AWS_REGION="$2";  shift 2 ;;
    --profile)   AWS_PROFILE="$2"; shift 2 ;;
    --dry-run)   DRY_RUN=true;     shift   ;;
    *)           echo "Unknown flag: $1"; exit 1 ;;
  esac
done

AWS_CLI="aws --region $AWS_REGION --profile $AWS_PROFILE"

if $DRY_RUN; then
  echo "⚠  DRY RUN MODE — no AWS changes will be made"
  AWS_CLI="echo [DRY-RUN] aws --region $AWS_REGION --profile $AWS_PROFILE"
fi

# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

log()  { echo "▶ $*"; }
ok()   { echo "✓ $*"; }
err()  { echo "✗ $*" >&2; exit 1; }

require_env() {
  local var=$1
  if [[ -z "${!var:-}" ]]; then
    err "Required environment variable $var is not set. Export it before running."
  fi
}

# ─────────────────────────────────────────────────────────────────────────────
# Step 0: Validate required secrets are exported
# ─────────────────────────────────────────────────────────────────────────────

log "Validating required environment variables..."
require_env BINANCE_API_KEY
require_env BINANCE_API_SECRET
require_env TELEGRAM_BOT_TOKEN
require_env TELEGRAM_CHAT_ID

[[ "$LAMBDA_ROLE_ARN" == *"YOUR_ACCOUNT_ID"* ]] && \
  err "LAMBDA_ROLE_ARN is still the placeholder. Set it before deploying."

ok "Environment variables validated."

# ─────────────────────────────────────────────────────────────────────────────
# Step 1: Install dependencies into ./package/ using manylinux wheels
#         This ensures binary libs (e.g. aiohttp's _helpers.so) work on Lambda
# ─────────────────────────────────────────────────────────────────────────────

log "Installing dependencies for Lambda (linux/x86_64)..."
rm -rf package/
mkdir -p package/

pip install \
  --platform manylinux2014_x86_64 \
  --implementation cp \
  --python-version 3.12 \
  --only-binary=:all: \
  --target package/ \
  -r requirements.txt \
  --quiet

ok "Dependencies installed into ./package/"

# ─────────────────────────────────────────────────────────────────────────────
# Step 2: Build deployment zips
#         Each Lambda gets its own zip: shared deps + its specific handler file
# ─────────────────────────────────────────────────────────────────────────────

log "Building opener.zip..."
rm -f opener.zip
(cd package && zip -r9 ../opener.zip . -x "*.pyc" -x "*/__pycache__/*" -q)
zip -g opener.zip lambda_function.py db.py -q
ok "opener.zip built ($(du -sh opener.zip | cut -f1))"

log "Building closer.zip..."
rm -f closer.zip
(cd package && zip -r9 ../closer.zip . -x "*.pyc" -x "*/__pycache__/*" -q)
zip -g closer.zip close_lambda.py db.py -q
ok "closer.zip built ($(du -sh closer.zip | cut -f1))"

# ─────────────────────────────────────────────────────────────────────────────
# Step 3: Environment variables JSON for Lambda config
# ─────────────────────────────────────────────────────────────────────────────

# Shared env vars for both functions
ENV_VARS=$(cat <<EOF
{
  "Variables": {
    "BINANCE_API_KEY":         "$BINANCE_API_KEY",
    "BINANCE_API_SECRET":      "$BINANCE_API_SECRET",
    "TELEGRAM_BOT_TOKEN":      "$TELEGRAM_BOT_TOKEN",
    "TELEGRAM_CHAT_ID":        "$TELEGRAM_CHAT_ID",
    "CAPITAL_PER_LEG_USD":     "${CAPITAL_PER_LEG_USD:-1000}",
    "MIN_FUNDING_RATE_TO_HOLD":"${MIN_FUNDING_RATE_TO_HOLD:-0.0001}",
    "DYNAMO_TABLE_NAME":       "${DYNAMO_TABLE_NAME:-funding-rate-arb-positions}"
  }
}
EOF
)

# ─────────────────────────────────────────────────────────────────────────────
# Step 3.5: Create DynamoDB table (if it doesn't exist)
# ─────────────────────────────────────────────────────────────────────────────

DYNAMO_TABLE="${DYNAMO_TABLE_NAME:-funding-rate-arb-positions}"

log "Checking DynamoDB table '$DYNAMO_TABLE'..."

if $AWS_CLI dynamodb describe-table --table-name "$DYNAMO_TABLE" > /dev/null 2>&1; then
  ok "DynamoDB table '$DYNAMO_TABLE' already exists."
else
  log "Creating DynamoDB table '$DYNAMO_TABLE'..."
  $AWS_CLI dynamodb create-table \
    --table-name "$DYNAMO_TABLE" \
    --attribute-definitions AttributeName=symbol,AttributeType=S \
    --key-schema AttributeName=symbol,KeyType=HASH \
    --billing-mode PAY_PER_REQUEST \
    --output text > /dev/null

  log "Waiting for table to become ACTIVE..."
  $AWS_CLI dynamodb wait table-exists --table-name "$DYNAMO_TABLE"
  ok "DynamoDB table '$DYNAMO_TABLE' created."
fi

# ─────────────────────────────────────────────────────────────────────────────
# Step 4: Create or update Lambda functions
# ─────────────────────────────────────────────────────────────────────────────

deploy_lambda() {
  local name=$1
  local zip=$2
  local handler=$3
  local description=$4

  log "Deploying $name..."

  # Check if function exists
  if $AWS_CLI lambda get-function --function-name "$name" > /dev/null 2>&1; then
    log "  Function exists — updating code..."
    $AWS_CLI lambda update-function-code \
      --function-name "$name" \
      --zip-file "fileb://$zip" \
      --output text \
      --query 'FunctionArn' > /dev/null

    log "  Waiting for code update to complete..."
    $AWS_CLI lambda wait function-updated --function-name "$name"

    log "  Updating configuration..."
    $AWS_CLI lambda update-function-configuration \
      --function-name "$name" \
      --handler "$handler" \
      --runtime "$PYTHON_RUNTIME" \
      --timeout "$LAMBDA_TIMEOUT" \
      --memory-size "$LAMBDA_MEMORY" \
      --environment "$ENV_VARS" \
      --output text \
      --query 'FunctionArn' > /dev/null

  else
    log "  Function does not exist — creating..."
    $AWS_CLI lambda create-function \
      --function-name "$name" \
      --runtime "$PYTHON_RUNTIME" \
      --role "$LAMBDA_ROLE_ARN" \
      --handler "$handler" \
      --zip-file "fileb://$zip" \
      --timeout "$LAMBDA_TIMEOUT" \
      --memory-size "$LAMBDA_MEMORY" \
      --description "$description" \
      --environment "$ENV_VARS" \
      --output text \
      --query 'FunctionArn' > /dev/null
  fi

  ok "$name deployed."
}

deploy_lambda \
  "$OPENER_FUNCTION" \
  "opener.zip" \
  "lambda_function.lambda_handler" \
  "Funding rate arb opener — scans and opens delta-neutral positions"

deploy_lambda \
  "$CLOSER_FUNCTION" \
  "closer.zip" \
  "close_lambda.lambda_handler" \
  "Funding rate arb closer — monitors and closes delta-neutral positions"

# ─────────────────────────────────────────────────────────────────────────────
# Step 5: Attach EventBridge Scheduler rules
# ─────────────────────────────────────────────────────────────────────────────

attach_schedule() {
  local function_name=$1
  local rule_name=$2
  local cron_expr=$3

  log "Attaching EventBridge rule '$rule_name' ($cron_expr) to $function_name..."

  # Create or update the rule
  RULE_ARN=$($AWS_CLI events put-rule \
    --name "$rule_name" \
    --schedule-expression "$cron_expr" \
    --state ENABLED \
    --query 'RuleArn' \
    --output text)

  # Get Lambda ARN
  LAMBDA_ARN=$($AWS_CLI lambda get-function \
    --function-name "$function_name" \
    --query 'Configuration.FunctionArn' \
    --output text)

  # Grant EventBridge permission to invoke the Lambda (idempotent)
  $AWS_CLI lambda add-permission \
    --function-name "$function_name" \
    --statement-id "eventbridge-${rule_name}" \
    --action "lambda:InvokeFunction" \
    --principal "events.amazonaws.com" \
    --source-arn "$RULE_ARN" \
    --output text > /dev/null 2>&1 || true  # Ignore if permission already exists

  # Attach Lambda as target
  $AWS_CLI events put-targets \
    --rule "$rule_name" \
    --targets "Id=1,Arn=$LAMBDA_ARN" \
    --output text > /dev/null

  ok "Schedule attached: $cron_expr → $function_name"
}

attach_schedule "$CLOSER_FUNCTION" "arb-closer-schedule" "$CLOSER_CRON"
attach_schedule "$OPENER_FUNCTION" "arb-opener-schedule" "$OPENER_CRON"

# ─────────────────────────────────────────────────────────────────────────────
# Done
# ─────────────────────────────────────────────────────────────────────────────

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Deployment complete!"
echo ""
echo "  Opener:  $OPENER_FUNCTION"
echo "    Cron:  $OPENER_CRON  (07:55, 15:55, 23:55 UTC)"
echo ""
echo "  Closer:  $CLOSER_FUNCTION"
echo "    Cron:  $CLOSER_CRON  (07:45, 15:45, 23:45 UTC)"
echo ""
echo "  ⚠  TEST ON BINANCE TESTNET BEFORE GOING LIVE."
echo "     Set BINANCE_API_KEY/SECRET to your testnet credentials"
echo "     and add 'testnet: True' to the ccxt options in both files."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
