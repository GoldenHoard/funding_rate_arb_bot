# Delta-Neutral Funding Rate Arbitrage Bot

Automated trading system that captures funding rate yield on Binance USDⓈ-M perpetual futures through delta-neutral positions.

## Strategy

Perpetual futures contracts pay/receive a **funding rate** every 8 hours. When funding is positive, shorts receive payment from longs. This bot exploits that by holding a **market-neutral** position:

```
Long Spot  +  Short Perpetual  =  Zero directional exposure, collect funding
```

### Execution Flow

1. **Scan** — Fetch funding rates for all Binance USDⓈ-M perpetuals
2. **Filter** — Remove coins below the minimum funding rate threshold or with < $50M 24h volume (spot & futures)
3. **Rank** — Sort by funding rate descending, select top 3 candidates
4. **Pre-trade checks** — Fetch orderbook BBO, calculate expected slippage, abort if spread > 0.2%
5. **Execute** — Concurrently fire Market BUY (Spot) + Market SELL (Futures) via `asyncio.gather` to minimise legging risk
6. **Monitor** — Every 8 hours, evaluate exit triggers and unwind positions when carry deteriorates
7. **Alert** — Send execution summaries to Telegram (entry prices, slippage, projected APY)

## Architecture

```
┌──────────────┐    cron (every 8h)    ┌──────────────┐
│  close_       │◄──────────────────────│   EC2 / λ    │
│  lambda.py    │   evaluate & close    │   crontab    │
└──────┬───────┘                       └──────┬───────┘
       │                                      │
       │         ┌──────────────┐             │
       └────────►│   Binance    │◄────────────┘
                 │  Spot + USDM │    open positions
       ┌────────►│   via ccxt   │◄────────────┐
       │         └──────────────┘             │
┌──────┴───────┐                       ┌──────┴───────┐
│  lambda_      │   scan & open        │   DynamoDB   │
│  function.py  │──────────────────────►│  positions   │
└──────────────┘   track positions     └──────────────┘
```

| Component | Purpose |
|---|---|
| `lambda_function.py` | **Opener** — scans for opportunities and opens delta-neutral positions |
| `close_lambda.py` | **Closer** — monitors open positions and unwinds on exit triggers |
| `db.py` | DynamoDB helper — position tracking, duplicate prevention, PnL recording |
| `run.sh` | Cron entrypoint — loads `.env`, runs closer then opener, logs output |
| `deploy.sh` | AWS Lambda deployment script (alternative to EC2) |
| `setup_ec2.sh` | EC2 instance provisioning (Python, pip, cron, AWS CLI config) |

## Risk Management

| Protection | Description |
|---|---|
| **Slippage guard** | Aborts if expected slippage > 0.2% based on orderbook depth |
| **Volume filter** | Requires $50M+ 24h volume on both spot and futures legs |
| **Concurrent execution** | `asyncio.gather` fires both legs simultaneously to minimise legging risk |
| **Leg failure handling** | If one leg fills and the other fails, raises `LegExecutionError` with immediate Telegram alert |
| **Minimum hold period** | Configurable `MIN_HOLD_HOURS` prevents fee-churning on short-lived positions |
| **Minimum funding threshold** | Separate open (`MIN_FUNDING_RATE_TO_OPEN`) and hold (`MIN_FUNDING_RATE_TO_HOLD`) thresholds |
| **Symbol blacklist** | `BLACKLIST_SYMBOLS` excludes assets (e.g. BNB used for fee discounts) |
| **Precision formatting** | Strict compliance with Binance `stepSize` / `tickSize` via ccxt |
| **reduceOnly** | Futures close orders use `reduceOnly=True` to prevent accidental position flips |
| **Dry-run mode** | Full pipeline execution without placing real orders |
| **Testnet support** | Route to Binance testnet endpoints for integration testing |

## Exit Triggers

The closer evaluates positions in priority order:

| Priority | Trigger | Bypasses Hold Time? |
|---|---|---|
| 1 | `FORCE_CLOSE` — manual override via env var | Yes |
| 2 | Funding rate turned **negative** (paying, not receiving) | Yes |
| 3 | **Predicted** next funding rate is negative (pre-emptive) | No |
| 4 | Rate **decayed** below `MIN_FUNDING_RATE_TO_HOLD` | No |

## Setup

### Prerequisites

- Python 3.10+
- Binance account with Spot + USDⓈ-M Futures enabled (subaccount recommended)
- AWS account (EC2 + DynamoDB)
- Telegram bot for alerts

### EC2 Deployment

```bash
# 1. Launch EC2 (Amazon Linux 2023, t4g.nano, attach IAM role with DynamoDB access)
# 2. Assign Elastic IP and whitelist on Binance

# 3. Upload code
scp -i your-key.pem -r ./* ec2-user@YOUR_IP:~/funding_rate_arb_bot/

# 4. SSH in and run setup
ssh -i your-key.pem ec2-user@YOUR_IP
cd ~/funding_rate_arb_bot
bash setup_ec2.sh

# 5. Configure environment
cp .env.example .env
nano .env  # fill in your API keys

# 6. Test (dry-run)
DRY_RUN=true ./run.sh

# 7. Crontab is auto-configured by setup_ec2.sh:
#    45 7,15,23 * * *  closer runs
#    55 7,15,23 * * *  opener runs
```

### Configuration

All parameters are controlled via environment variables (see `.env.example`):

```bash
CAPITAL_PER_LEG_USD=1000        # USD per side per position ($2k total per coin)
MIN_FUNDING_RATE_TO_OPEN=0.00013  # ~14.2% APY minimum to open
MIN_FUNDING_RATE_TO_HOLD=0.00004  # ~4.4% APY minimum to hold
MIN_HOLD_HOURS=24                 # Minimum hold before non-emergency exit
BLACKLIST_SYMBOLS=BNB             # Comma-separated, never trade these
DRY_RUN=false                     # true = simulate without placing orders
USE_TESTNET=false                 # true = route to Binance testnet
```

## Tech Stack

- **Python 3.10+** with `asyncio` for concurrent exchange operations
- **ccxt** — unified crypto exchange API (async support)
- **AWS DynamoDB** — serverless position tracking
- **AWS EC2** — hosting (or Lambda for serverless deployment)
- **Telegram Bot API** — real-time trade alerts

## Disclaimer

This software is for educational purposes. Cryptocurrency trading carries substantial risk. Use at your own risk. Always test on testnet before deploying live capital.
