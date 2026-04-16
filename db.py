"""
DynamoDB Position Store
━━━━━━━━━━━━━━━━━━━━━━━
Shared helper for both opener and closer Lambdas.

Table schema:
  PK: symbol (S)     — e.g. "ETH"
  Attributes:
    status           — "OPEN" | "CLOSED"
    open_time        — ISO 8601 timestamp
    close_time       — ISO 8601 timestamp (set on close)
    spot_entry       — Decimal
    futures_entry    — Decimal
    spot_exit        — Decimal (set on close)
    futures_exit     — Decimal (set on close)
    quantity         — Decimal
    funding_rate     — Decimal (8h rate at time of open)
    projected_apy    — Decimal
    basis_pnl_usd    — Decimal (set on close)
    spot_order_id    — String
    futures_order_id — String
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from decimal import Decimal
from typing import Optional

import boto3
from boto3.dynamodb.conditions import Attr

logger = logging.getLogger(__name__)

TABLE_NAME: str = os.environ.get("DYNAMO_TABLE_NAME", "funding-rate-arb-positions")

# Lazy-init: created on first call so module import never fails even without AWS creds
_table = None


def _get_table():
    global _table
    if _table is None:
        region = os.environ.get("AWS_REGION", "ap-northeast-1")
        dynamodb = boto3.resource("dynamodb", region_name=region)
        _table = dynamodb.Table(TABLE_NAME)
    return _table


# ─────────────────────────────────────────────────────────────────────────────
# Read Operations
# ─────────────────────────────────────────────────────────────────────────────


def is_position_open(symbol: str) -> bool:
    """Check if there is an active OPEN position for this symbol."""
    try:
        resp = _get_table().get_item(Key={"symbol": symbol})
        item = resp.get("Item")
        return item is not None and item.get("status") == "OPEN"
    except Exception as exc:
        logger.error(f"DynamoDB read failed for {symbol}: {exc}")
        # Fail-safe: if we can't read, assume position exists to avoid duplicates
        return True


def get_open_positions() -> list[dict]:
    """Return all records with status=OPEN."""
    try:
        resp = _get_table().scan(
            FilterExpression=Attr("status").eq("OPEN"),
        )
        return resp.get("Items", [])
    except Exception as exc:
        logger.error(f"DynamoDB scan failed: {exc}")
        return []


def get_position(symbol: str) -> Optional[dict]:
    """Fetch a single position record by symbol."""
    try:
        resp = _get_table().get_item(Key={"symbol": symbol})
        return resp.get("Item")
    except Exception as exc:
        logger.error(f"DynamoDB get_item failed for {symbol}: {exc}")
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Write Operations
# ─────────────────────────────────────────────────────────────────────────────


def record_open(
    symbol: str,
    spot_entry: float,
    futures_entry: float,
    quantity: float,
    funding_rate: float,
    projected_apy: float,
    spot_order_id: str = "",
    futures_order_id: str = "",
) -> None:
    """Write a new OPEN position record. Overwrites any previous CLOSED record for the same symbol."""
    try:
        _get_table().put_item(Item={
            "symbol": symbol,
            "status": "OPEN",
            "open_time": datetime.now(timezone.utc).isoformat(),
            "spot_entry": Decimal(str(spot_entry)),
            "futures_entry": Decimal(str(futures_entry)),
            "quantity": Decimal(str(quantity)),
            "funding_rate": Decimal(str(funding_rate)),
            "projected_apy": Decimal(str(projected_apy)),
            "spot_order_id": spot_order_id,
            "futures_order_id": futures_order_id,
        })
        logger.info(f"DB: Recorded OPEN position for {symbol}")
    except Exception as exc:
        # DB failure must not crash the trade — position is already filled on-exchange
        logger.error(f"DynamoDB write failed for {symbol}: {exc}")


def record_close(
    symbol: str,
    spot_exit: float,
    futures_exit: float,
    basis_pnl_usd: float,
    spot_order_id: str = "",
    futures_order_id: str = "",
) -> None:
    """Update an existing position record to CLOSED with exit details."""
    try:
        _get_table().update_item(
            Key={"symbol": symbol},
            UpdateExpression=(
                "SET #s = :status, "
                "close_time = :ct, "
                "spot_exit = :se, "
                "futures_exit = :fe, "
                "basis_pnl_usd = :pnl, "
                "close_spot_order_id = :soid, "
                "close_futures_order_id = :foid"
            ),
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={
                ":status": "CLOSED",
                ":ct": datetime.now(timezone.utc).isoformat(),
                ":se": Decimal(str(spot_exit)),
                ":fe": Decimal(str(futures_exit)),
                ":pnl": Decimal(str(basis_pnl_usd)),
                ":soid": spot_order_id,
                ":foid": futures_order_id,
            },
        )
        logger.info(f"DB: Recorded CLOSE for {symbol}, BasisPnL=${basis_pnl_usd:+.4f}")
    except Exception as exc:
        logger.error(f"DynamoDB update failed for {symbol}: {exc}")
