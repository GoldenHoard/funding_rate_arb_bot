"""
Delta-Neutral Funding Rate Arbitrage — Position Closer
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
AWS Lambda Function | Companion to lambda_function.py (the opener)

Responsibility:
  This Lambda scans for ALL active arbitrage positions opened by the opener bot
  and closes any that no longer justify staying open, defined by:

  Exit Triggers (ANY one is sufficient to close):
    1. Funding Rate Decay  — current 8h rate has fallen below MIN_FUNDING_RATE_TO_HOLD
    2. Funding Rate Flip   — rate has turned NEGATIVE (we would be PAYING funding)
    3. Manual Override     — FORCE_CLOSE_SYMBOLS env var contains the symbol

  Close Execution:
    - Concurrent: Market SELL Spot + Market BUY (cover short) Futures
    - Same legging-risk protection as the opener (asyncio.gather + LegExecutionError)
    - Realised PnL = Funding collected (tracked externally) + basis P&L
    - Telegram alert with net PnL estimate

  Position Discovery:
    Rather than maintaining a separate database, this bot reads live state
    directly from Binance:
      - Futures: fetch_positions() → filter for short positions with size > 0
      - Spot:    fetch_balance()   → find non-zero USDT-paired base assets

  Deployment:
    - Runtime  : Python 3.10+ (Lambda)
    - Trigger  : AWS EventBridge Cron — every 8h, BEFORE the opener runs
                 Recommended: cron(45 7,15,23 * * ? *)  (10 min before funding)
    - Env Vars : Same as opener PLUS:
                   MIN_FUNDING_RATE_TO_HOLD  (default 0.0001 = 0.01% per 8h ≈ 11% APY)
                   FORCE_CLOSE_SYMBOLS       (optional CSV, e.g. "BTC,ETH")
                   MIN_SPOT_NOTIONAL_USD     (skip dust balances, default $10)
"""

from __future__ import annotations

import asyncio
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

import aiohttp
import ccxt.async_support as ccxt

import db

# ─────────────────────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────────────────────

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# ─────────────────────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────────────────────

# Minimum 8h funding rate to justify holding the position.
# Below this threshold the carry no longer compensates for operational risk.
# Default: 0.0001 = 0.01% per 8h = ~10.95% APY
MIN_FUNDING_RATE_TO_HOLD: float = float(
    os.environ.get("MIN_FUNDING_RATE_TO_HOLD", "0.0001")
)

# Dust filter: ignore spot balances whose USDT value is below this
MIN_SPOT_NOTIONAL_USD: float = float(
    os.environ.get("MIN_SPOT_NOTIONAL_USD", "10.0")
)

# Optional manual override: comma-separated base assets to force-close regardless
# of funding rate. e.g. "BTC,ETH,SOL"
_force_close_raw: str = os.environ.get("FORCE_CLOSE_SYMBOLS", "")
FORCE_CLOSE_SYMBOLS: set[str] = {
    s.strip().upper() for s in _force_close_raw.split(",") if s.strip()
}

# Comma-separated base assets to ignore entirely. e.g. "BNB,BUSD"
# BNB is blacklisted by default to protect fee-payment balances.
_blacklist_raw: str = os.environ.get("BLACKLIST_SYMBOLS", "BNB")
BLACKLIST_SYMBOLS: set[str] = {
    s.strip().upper() for s in _blacklist_raw.split(",") if s.strip()
}

# Funding periods per year (8h interval)
FUNDING_PERIODS_PER_YEAR: int = 3 * 365

# Minimum hold time in hours before a position can be closed due to rate decay.
# Protects against being churned by fees on short-lived positions.
# Does NOT override emergency exits (negative rate or force-close).
# Default: 24 hours (3 funding periods). Set to 0 to disable.
MIN_HOLD_HOURS: float = float(os.environ.get("MIN_HOLD_HOURS", "24"))

# ─────────────────────────────────────────────────────────────────────────────
# Safety Modes
# ─────────────────────────────────────────────────────────────────────────────

# DRY_RUN=true → Discover positions and evaluate exit triggers, but SKIP
#                actual close orders. Use this to verify the logic is correct
#                before letting it touch real positions.
DRY_RUN: bool = os.environ.get("DRY_RUN", "false").lower() == "true"

# USE_TESTNET=true → Route ccxt to Binance Testnet endpoints.
USE_TESTNET: bool = os.environ.get("USE_TESTNET", "false").lower() == "true"

# ─────────────────────────────────────────────────────────────────────────────
# Secrets
# ─────────────────────────────────────────────────────────────────────────────

BINANCE_API_KEY: str = os.environ["BINANCE_API_KEY"]
BINANCE_API_SECRET: str = os.environ["BINANCE_API_SECRET"]
TELEGRAM_BOT_TOKEN: str = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID: str = os.environ.get("TELEGRAM_CHAT_ID", "")

# ─────────────────────────────────────────────────────────────────────────────
# Data Classes
# ─────────────────────────────────────────────────────────────────────────────


@dataclass
class OpenPosition:
    """Represents one active arb position (one spot leg + one futures short leg)."""
    symbol: str              # Base asset, e.g. "ETH"
    spot_symbol: str         # e.g. "ETH/USDT"
    futures_symbol: str      # e.g. "ETH/USDT:USDT"
    spot_qty: float          # Spot holdings in base asset
    futures_qty: float       # Futures short size (positive number)
    current_funding_rate: float
    predicted_funding_rate: float = 0.0  # Next period's estimated rate
    exit_reason: str = ""    # Populated when a close trigger is met


@dataclass
class CloseResult:
    """Execution record for a completed (or failed) close."""
    symbol: str
    spot_exit: float
    futures_exit: float
    quantity: float
    basis_pnl_usd: float     # Futures exit - spot exit (positive = favourable)
    spot_order_id: str = ""
    futures_order_id: str = ""
    error: Optional[str] = None


# ─────────────────────────────────────────────────────────────────────────────
# Custom Exceptions
# ─────────────────────────────────────────────────────────────────────────────


class LegCloseError(Exception):
    """
    Raised when one close leg executes but the other fails.
    Leaves a naked directional position — requires IMMEDIATE manual action.
    """


# ─────────────────────────────────────────────────────────────────────────────
# Exchange Factory (identical config to opener for consistency)
# ─────────────────────────────────────────────────────────────────────────────


def _make_spot_exchange() -> ccxt.binance:
    config: dict = {
        "apiKey": BINANCE_API_KEY,
        "secret": BINANCE_API_SECRET,
        "enableRateLimit": True,
        "options": {
            "defaultType": "spot",
            "adjustForTimeDifference": True,
            "fetchCurrencies": False,
        },
    }
    if USE_TESTNET:
        config["options"]["sandboxMode"] = True
        logger.info("SPOT exchange → TESTNET mode")
    return ccxt.binance(config)


def _make_futures_exchange() -> ccxt.binance:
    config: dict = {
        "apiKey": BINANCE_API_KEY,
        "secret": BINANCE_API_SECRET,
        "enableRateLimit": True,
        "options": {
            "defaultType": "future",
            "adjustForTimeDifference": True,
            "fetchCurrencies": False,
        },
    }
    if USE_TESTNET:
        config["options"]["sandboxMode"] = True
        logger.info("FUTURES exchange → TESTNET mode")
    return ccxt.binance(config)


# ─────────────────────────────────────────────────────────────────────────────
# Phase 1 — Position Discovery
# ─────────────────────────────────────────────────────────────────────────────


async def discover_open_positions(
    spot_ex: ccxt.binance,
    futures_ex: ccxt.binance,
) -> list[OpenPosition]:
    """
    Identify active arb positions by reading live account state from Binance.

    Strategy:
      - Futures side: fetch_positions() → any SHORT with notional > $0
      - Spot side:    fetch_balance()   → any non-USDT asset with value > MIN_SPOT_NOTIONAL_USD
      - Match the two sides by base asset symbol
      - Enrich with current funding rate

    This approach requires no external database. The source of truth is the
    exchange itself, making it resilient to Lambda cold-starts and state loss.
    """
    logger.info("Discovering open positions...")

    (
        _,                  # load_markets for precision formatting
        futures_positions,
        spot_balance,
        funding_rates,
        spot_tickers,
    ) = await asyncio.gather(
        futures_ex.load_markets(),
        futures_ex.fetch_positions(),           # All futures positions
        spot_ex.fetch_balance(),                # Full spot wallet
        futures_ex.fetch_funding_rates(),       # Current + predicted funding rates
        spot_ex.fetch_tickers(),                # For spot USD price conversion
    )

    # fetch_funding_rates() returns both fundingRate (current) and
    # fundingRateTimestamp; for predicted rates we need per-symbol calls.
    # We'll fetch predicted rates later only for matched positions to
    # minimise API calls.

    # ── Build futures short map: base_asset → (futures_symbol, short_qty) ──
    futures_shorts: dict[str, tuple[str, float]] = {}
    for pos in futures_positions:
        # ccxt position side: "short" in one-way mode shows as negative notional
        # or explicitly as side="short" in hedge mode
        side = pos.get("side", "")
        contracts = float(pos.get("contracts") or 0)
        symbol = pos.get("symbol", "")  # e.g. "ETH/USDT:USDT"

        # In one-way mode, a short has negative contracts or side=="short"
        if contracts == 0:
            continue

        is_short = (side == "short") or (contracts < 0)
        if not is_short:
            continue

        parts = symbol.split("/")
        if len(parts) != 2:
            continue
        base = parts[0]
        futures_shorts[base] = (symbol, abs(contracts))
        logger.debug(f"Futures short found: {base} qty={abs(contracts):.6f}")

    if not futures_shorts:
        logger.info("No open futures short positions found.")
        return []

    # ── Build spot holdings map: base_asset → qty ─────────────────────────
    spot_holdings: dict[str, float] = {}
    free_balances: dict = spot_balance.get("free", {})

    for asset, qty in free_balances.items():
        qty = float(qty or 0)
        if qty <= 0 or asset == "USDT":
            continue

        spot_symbol = f"{asset}/USDT"
        ticker = spot_tickers.get(spot_symbol)
        if not ticker:
            continue

        last_price = float(ticker.get("last") or ticker.get("close") or 0)
        notional = qty * last_price

        if notional < MIN_SPOT_NOTIONAL_USD:
            logger.debug(f"Skipping dust balance: {asset} qty={qty:.6f} notional=${notional:.2f}")
            continue

        spot_holdings[asset] = qty
        logger.debug(f"Spot holding found: {asset} qty={qty:.6f} notional=${notional:.2f}")

    # ── Match spot + futures legs, enrich with funding rate ───────────────
    matched: list[tuple[str, str, str, float, float, float]] = []
    for base, (futures_symbol, futures_qty) in futures_shorts.items():
        if base in BLACKLIST_SYMBOLS:
            logger.debug(f"Skipping blacklisted symbol: {base}")
            continue

        spot_symbol = f"{base}/USDT"

        if base not in spot_holdings:
            logger.warning(
                f"Futures short found for {base} but NO matching spot holding. "
                f"This may indicate a pre-existing position or manual trade — skipping."
            )
            continue

        spot_qty = spot_holdings[base]
        rate_info = funding_rates.get(futures_symbol, {})
        funding_rate = float(rate_info.get("fundingRate") or 0.0)
        matched.append((base, spot_symbol, futures_symbol, spot_qty, futures_qty, funding_rate))

    if not matched:
        return []

    # ── Fetch predicted funding rates for matched positions only ────────
    # fetch_funding_rate() (singular) returns both current and predicted
    # rates for a specific symbol. We batch these calls concurrently.
    async def _get_predicted(sym: str) -> float:
        try:
            info = await futures_ex.fetch_funding_rate(sym)
            return float(info.get("fundingRate") or 0.0)
        except Exception as exc:
            logger.warning(f"Failed to fetch predicted rate for {sym}: {exc}")
            return 0.0

    predicted_rates = await asyncio.gather(
        *[_get_predicted(m[2]) for m in matched]
    )

    positions: list[OpenPosition] = []
    for i, (base, spot_symbol, futures_symbol, spot_qty, futures_qty, funding_rate) in enumerate(matched):
        predicted = predicted_rates[i]
        positions.append(OpenPosition(
            symbol=base,
            spot_symbol=spot_symbol,
            futures_symbol=futures_symbol,
            spot_qty=spot_qty,
            futures_qty=futures_qty,
            current_funding_rate=funding_rate,
            predicted_funding_rate=predicted,
        ))
        logger.info(
            f"ACTIVE POSITION: {base} | SpotQty={spot_qty:.6f} FutQty={futures_qty:.6f} "
            f"| CurrentRate={funding_rate * 100:.4f}% | PredictedRate={predicted * 100:.4f}%"
        )

    return positions


# ─────────────────────────────────────────────────────────────────────────────
# Phase 2 — Exit Decision Engine
# ─────────────────────────────────────────────────────────────────────────────


def _get_hold_hours(symbol: str) -> float | None:
    """Look up the DynamoDB record for *symbol* and return how many hours
    the position has been open.  Returns ``None`` if no record is found
    (e.g. position was opened before DB tracking was enabled)."""
    try:
        rec = db.get_position(symbol)
        if rec and "open_time" in rec:
            open_dt = datetime.fromisoformat(rec["open_time"])
            delta = datetime.now(timezone.utc) - open_dt
            return delta.total_seconds() / 3600.0
    except Exception as exc:
        logger.warning(f"Could not fetch hold duration for {symbol}: {exc}")
    return None


def evaluate_exit_triggers(positions: list[OpenPosition]) -> list[OpenPosition]:
    """
    Apply exit rules to each position. Returns only positions that should be closed.

    Exit Rules (in priority order):
      1. FORCE_CLOSE       — symbol is in the manual override list      (bypasses hold time)
      2. NEGATIVE          — current funding rate is negative            (bypasses hold time)
      3. PREDICTED_NEGATIVE — next period's predicted rate is negative   (respects hold time)
      4. DECAY             — rate below MIN_FUNDING_RATE_TO_HOLD         (respects hold time)

    Minimum Hold Time:
      Rules 3 and 4 are blocked while the position is younger than
      MIN_HOLD_HOURS.  This prevents fee-churning on short-lived positions.
      Rules 1 and 2 are emergency exits and always execute immediately.
    """
    to_close: list[OpenPosition] = []

    for pos in positions:
        # --- Emergency exits (bypass hold time) ---
        if pos.symbol in FORCE_CLOSE_SYMBOLS:
            pos.exit_reason = f"Manual force-close override"
            to_close.append(pos)
            logger.info(f"EXIT [{pos.symbol}]: {pos.exit_reason}")
            continue

        if pos.current_funding_rate < 0:
            pos.exit_reason = (
                f"Funding rate NEGATIVE ({pos.current_funding_rate * 100:.4f}%) "
                f"— paying funding, immediate exit"
            )
            to_close.append(pos)
            logger.info(f"EXIT [{pos.symbol}]: {pos.exit_reason}")
            continue

        # --- Hold-time gated exits ---
        hold_hours = _get_hold_hours(pos.symbol)
        if MIN_HOLD_HOURS > 0 and hold_hours is not None and hold_hours < MIN_HOLD_HOURS:
            remaining = MIN_HOLD_HOURS - hold_hours
            apy = pos.current_funding_rate * FUNDING_PERIODS_PER_YEAR * 100
            logger.info(
                f"HOLD [{pos.symbol}]: held {hold_hours:.1f}h / min {MIN_HOLD_HOURS:.0f}h "
                f"({remaining:.1f}h remaining) | rate={pos.current_funding_rate*100:.4f}% "
                f"({apy:.1f}% APY) — minimum hold period not met, skipping non-emergency exit"
            )
            continue

        if pos.predicted_funding_rate < 0:
            pos.exit_reason = (
                f"Predicted next rate NEGATIVE ({pos.predicted_funding_rate * 100:.4f}%) "
                f"— pre-emptive exit before we start paying"
            )
            if hold_hours is not None:
                pos.exit_reason += f" [held {hold_hours:.1f}h]"
            to_close.append(pos)
            logger.info(f"EXIT [{pos.symbol}]: {pos.exit_reason}")

        elif pos.current_funding_rate < MIN_FUNDING_RATE_TO_HOLD:
            apy = pos.current_funding_rate * FUNDING_PERIODS_PER_YEAR * 100
            threshold_apy = MIN_FUNDING_RATE_TO_HOLD * FUNDING_PERIODS_PER_YEAR * 100
            pos.exit_reason = (
                f"Funding rate decayed to {pos.current_funding_rate * 100:.4f}% "
                f"({apy:.1f}% APY) — below {threshold_apy:.1f}% APY threshold"
            )
            if hold_hours is not None:
                pos.exit_reason += f" [held {hold_hours:.1f}h]"
            to_close.append(pos)
            logger.info(f"EXIT [{pos.symbol}]: {pos.exit_reason}")

        else:
            apy = pos.current_funding_rate * FUNDING_PERIODS_PER_YEAR * 100
            pred_apy = pos.predicted_funding_rate * FUNDING_PERIODS_PER_YEAR * 100
            held_str = f" | held={hold_hours:.1f}h" if hold_hours is not None else ""
            logger.info(
                f"HOLD [{pos.symbol}]: rate={pos.current_funding_rate*100:.4f}% ({apy:.1f}% APY) "
                f"| predicted={pos.predicted_funding_rate*100:.4f}% ({pred_apy:.1f}% APY) — above threshold{held_str}"
            )

    return to_close


# ─────────────────────────────────────────────────────────────────────────────
# Phase 3 — Precision Formatting
# ─────────────────────────────────────────────────────────────────────────────


def fmt_amount(exchange: ccxt.binance, symbol: str, amount: float) -> float:
    """Round to exchange stepSize. Required before every order submission."""
    return float(exchange.amount_to_precision(symbol, amount))


# ─────────────────────────────────────────────────────────────────────────────
# Phase 4 — Concurrent Close Execution
# ─────────────────────────────────────────────────────────────────────────────


async def execute_close(
    spot_ex: ccxt.binance,
    futures_ex: ccxt.binance,
    position: OpenPosition,
) -> CloseResult:
    """
    Close the delta-neutral position by concurrently executing:
      - Spot:    Market SELL (liquidate spot holdings)
      - Futures: Market BUY  (cover the short, reduceOnly=True for safety)

    reduceOnly=True on the futures leg prevents accidentally opening a long
    if there is any quantity mismatch due to partial fills or dust.
    """
    # Apply precision formatting to both quantities
    spot_qty_fmt = fmt_amount(spot_ex, position.spot_symbol, position.spot_qty)
    futures_qty_fmt = fmt_amount(futures_ex, position.futures_symbol, position.futures_qty)

    # Use the smaller of the two as the close quantity to avoid over-closing
    # (minor discrepancies can arise from partial fills during the open)
    close_qty = min(spot_qty_fmt, futures_qty_fmt)
    close_qty_spot = fmt_amount(spot_ex, position.spot_symbol, close_qty)
    close_qty_futures = fmt_amount(futures_ex, position.futures_symbol, close_qty)

    mode_tag = "[DRY-RUN] " if DRY_RUN else ""
    logger.info(
        f"{mode_tag}CLOSING {position.symbol}: SpotSell={close_qty_spot} "
        f"FuturesBuy={close_qty_futures} | Reason: {position.exit_reason}"
    )

    if DRY_RUN:
        # Simulate exit using current best-bid/ask mid from the position's funding rate.
        # In reality there's no clean "current price" here without a ticker call;
        # we log the intent clearly and return a placeholder result.
        logger.info(
            f"[DRY-RUN] Would close {position.symbol}: "
            f"Qty={close_qty_spot} | Skipping actual orders."
        )
        return CloseResult(
            symbol=position.symbol,
            spot_exit=0.0,          # No real fill price available in dry-run
            futures_exit=0.0,
            quantity=close_qty,
            basis_pnl_usd=0.0,
            spot_order_id="DRY_RUN_SPOT",
            futures_order_id="DRY_RUN_FUTURES",
        )

    spot_coro = spot_ex.create_order(
        symbol=position.spot_symbol,
        type="market",
        side="sell",
        amount=close_qty_spot,
    )

    futures_coro = futures_ex.create_order(
        symbol=position.futures_symbol,
        type="market",
        side="buy",            # Buy to cover the short
        amount=close_qty_futures,
        params={
            "reduceOnly": True,    # Safety: only close, never flip to long
            "positionSide": "BOTH",
        },
    )

    # Concurrent dispatch — minimise legging risk on close
    raw_results: list = await asyncio.gather(
        spot_coro,
        futures_coro,
        return_exceptions=True,
    )

    spot_result = raw_results[0]
    futures_result = raw_results[1]

    spot_ok = not isinstance(spot_result, Exception)
    futures_ok = not isinstance(futures_result, Exception)

    if spot_ok and futures_ok:
        spot_exit = float(
            spot_result.get("average") or spot_result.get("price") or 0
        )
        futures_exit = float(
            futures_result.get("average") or futures_result.get("price") or 0
        )

        # Basis PnL: if futures exit > spot exit, our short gained on the basis
        basis_pnl = (futures_exit - spot_exit) * close_qty

        logger.info(
            f"CLOSED {position.symbol}: SpotExit={spot_exit:.6f} "
            f"FuturesExit={futures_exit:.6f} BasisPnL=${basis_pnl:+.4f}"
        )

        return CloseResult(
            symbol=position.symbol,
            spot_exit=spot_exit,
            futures_exit=futures_exit,
            quantity=close_qty,
            basis_pnl_usd=basis_pnl,
            spot_order_id=str(spot_result.get("id", "")),
            futures_order_id=str(futures_result.get("id", "")),
        )

    elif spot_ok and not futures_ok:
        # Spot sold but futures still short — now NAKED SHORT on futures
        raise LegCloseError(
            f"[{position.symbol}] Spot SELL filled (ID={spot_result.get('id','?')}) "
            f"but Futures BUY (cover) FAILED: {futures_result}. "
            f"NAKED FUTURES SHORT — buy to cover manually NOW."
        )

    elif futures_ok and not spot_ok:
        # Futures covered but still holding spot — now NAKED LONG on spot
        raise LegCloseError(
            f"[{position.symbol}] Futures BUY (cover) filled (ID={futures_result.get('id','?')}) "
            f"but Spot SELL FAILED: {spot_result}. "
            f"NAKED SPOT LONG — sell spot manually NOW."
        )

    else:
        raise RuntimeError(
            f"Both close legs failed for {position.symbol}. "
            f"Position remains open. SpotErr: {spot_result} | FuturesErr: {futures_result}"
        )


# ─────────────────────────────────────────────────────────────────────────────
# Telegram Alerting
# ─────────────────────────────────────────────────────────────────────────────


async def _post_telegram(text: str) -> None:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.warning("Telegram credentials not configured — skipping alert.")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML"}
    try:
        timeout = aiohttp.ClientTimeout(total=10)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(url, json=payload) as resp:
                if resp.status != 200:
                    logger.error(f"Telegram HTTP {resp.status}: {(await resp.text())[:200]}")
    except Exception as exc:
        logger.error(f"Telegram alert failed: {exc}")


def _fmt_hold_summary(held: list[OpenPosition]) -> str:
    if not held:
        return ""
    lines = ["<b>📊 POSITIONS HELD</b>"]
    for p in held:
        apy = p.current_funding_rate * FUNDING_PERIODS_PER_YEAR * 100
        lines.append(f"✅ <b>{p.symbol}</b>: {p.current_funding_rate*100:.4f}% ({apy:.1f}% APY)")
    return "\n".join(lines)


def _fmt_close_alert(r: CloseResult) -> str:
    pnl_emoji = "🟢" if r.basis_pnl_usd >= 0 else "🔴"
    return (
        "<b>🔒 POSITION CLOSED</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"<b>Asset:</b>           {r.symbol}/USDT\n"
        f"<b>Spot Exit:</b>       ${r.spot_exit:.6f}\n"
        f"<b>Futures Exit:</b>    ${r.futures_exit:.6f}\n"
        f"<b>Quantity:</b>        {r.quantity:.6f} {r.symbol}\n"
        f"<b>Basis PnL:</b>       {pnl_emoji} ${r.basis_pnl_usd:+.4f}\n"
        f"<b>Spot Order ID:</b>   <code>{r.spot_order_id}</code>\n"
        f"<b>Futures Order ID:</b> <code>{r.futures_order_id}</code>"
    )


def _fmt_close_error(symbol: str, reason: str) -> str:
    return (
        "<b>❌ CLOSE FAILED</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        f"<b>Asset:</b>  {symbol}\n"
        f"<b>Reason:</b> {reason[:400]}"
    )


def _fmt_session_summary(
    closed: list[CloseResult],
    held: list[OpenPosition],
    skipped: list[tuple[str, str]],
) -> str:
    total_basis_pnl = sum(r.basis_pnl_usd for r in closed if not r.error)
    pnl_emoji = "🟢" if total_basis_pnl >= 0 else "🔴"

    lines = ["<b>📋 CLOSER SESSION SUMMARY</b>", "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"]

    for r in closed:
        if r.error:
            lines.append(f"❌ <b>{r.symbol}</b>: {r.error[:100]}")
        else:
            lines.append(
                f"🔒 <b>{r.symbol}</b>: closed | Basis PnL ${r.basis_pnl_usd:+.4f}"
            )

    for p in held:
        apy = p.current_funding_rate * FUNDING_PERIODS_PER_YEAR * 100
        lines.append(f"✅ <b>{p.symbol}</b>: holding ({apy:.1f}% APY)")

    for sym, reason in skipped:
        lines.append(f"⚠️ <b>{sym}</b>: {reason}")

    if closed:
        lines.append(f"\n{pnl_emoji} <b>Total Basis PnL: ${total_basis_pnl:+.4f}</b>")

    if not closed and not held and not skipped:
        lines.append("No open positions found.")

    return "\n".join(lines)


def _fmt_critical_alert(detail: str) -> str:
    return (
        "🚨 <b>CRITICAL — CLOSE LEG IMBALANCE</b> 🚨\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{detail[:800]}\n\n"
        "<b>ACTION REQUIRED: Manually close the open leg to restore delta-neutrality.</b>"
    )


# ─────────────────────────────────────────────────────────────────────────────
# Main Orchestration
# ─────────────────────────────────────────────────────────────────────────────


async def run_closer() -> None:
    """
    Full close-cycle execution.

    Phases:
      1 — Discover all active arb positions from live exchange state
      2 — Evaluate exit triggers for each position
      3 — Execute concurrent close for positions that triggered exit
      4 — Send per-close and session summary Telegram alerts
    """
    spot_ex = _make_spot_exchange()
    futures_ex = _make_futures_exchange()

    closed: list[CloseResult] = []
    held: list[OpenPosition] = []
    skipped: list[tuple[str, str]] = []

    try:
        # Phase 1: Discover
        all_positions = await discover_open_positions(spot_ex, futures_ex)

        if not all_positions:
            logger.info("No active arb positions found — nothing to close.")
            await _post_telegram(
                "<b>Funding Arb Closer</b>\nNo open positions found this cycle."
            )
            return

        # Phase 2: Evaluate exit triggers
        to_close = evaluate_exit_triggers(all_positions)
        held = [p for p in all_positions if p not in to_close]

        if not to_close:
            logger.info(f"All {len(held)} positions pass hold criteria — no closes needed.")
            summary = _fmt_session_summary([], held, [])
            await _post_telegram(summary)
            return

        logger.info(f"Closing {len(to_close)} position(s), holding {len(held)}.")

        # Phase 3: Execute closes
        for position in to_close:
            logger.info(f"━━ Closing {position.symbol}: {position.exit_reason} ━━")
            try:
                result = await execute_close(spot_ex, futures_ex, position)
                closed.append(result)

                # Update DynamoDB record to CLOSED with exit details
                if not result.error:
                    db.record_close(
                        symbol=result.symbol,
                        spot_exit=result.spot_exit,
                        futures_exit=result.futures_exit,
                        basis_pnl_usd=result.basis_pnl_usd,
                        spot_order_id=result.spot_order_id,
                        futures_order_id=result.futures_order_id,
                    )

                await _post_telegram(_fmt_close_alert(result))

            except LegCloseError as exc:
                # One leg closed, one didn't — CRITICAL, halt immediately
                logger.critical(str(exc))
                await _post_telegram(_fmt_critical_alert(str(exc)))
                raise  # Re-raise so Lambda marks invocation as failed

            except Exception as exc:
                reason = str(exc)
                logger.error(f"Close failed for {position.symbol}: {reason}")
                closed.append(CloseResult(
                    symbol=position.symbol,
                    spot_exit=0.0,
                    futures_exit=0.0,
                    quantity=0.0,
                    basis_pnl_usd=0.0,
                    error=reason,
                ))
                skipped.append((position.symbol, reason))
                await _post_telegram(_fmt_close_error(position.symbol, reason))

        # Phase 4: Session summary
        await _post_telegram(_fmt_session_summary(closed, held, skipped))

    finally:
        await asyncio.gather(
            spot_ex.close(),
            futures_ex.close(),
            return_exceptions=True,
        )
        logger.info("Exchange connections closed.")


# ─────────────────────────────────────────────────────────────────────────────
# AWS Lambda Entry Point
# ─────────────────────────────────────────────────────────────────────────────


def lambda_handler(event: dict, context) -> dict:
    """
    AWS Lambda handler for the closer bot.

    Recommended EventBridge cron (runs 10 min BEFORE each funding window,
    and BEFORE the opener Lambda so stale positions are closed first):
      cron(45 7,15,23 * * ? *)   → fires at 07:45, 15:45, 23:45 UTC

    Can also be invoked manually to force-close specific symbols:
      Payload: {"force_close": "BTC,ETH"}
    """
    logger.info(
        f"Closer Lambda invoked. RequestID={getattr(context, 'aws_request_id', 'local')} "
        f"Event={event}"
    )
    if DRY_RUN:
        logger.warning("=" * 60)
        logger.warning("  DRY-RUN MODE ACTIVE — no real orders will be placed")
        logger.warning("=" * 60)
    if USE_TESTNET:
        logger.warning("  TESTNET MODE ACTIVE — using Binance paper trading endpoints")

    # Support manual invocation payload for emergency force-close
    if "force_close" in event:
        overrides = {s.strip().upper() for s in event["force_close"].split(",") if s.strip()}
        FORCE_CLOSE_SYMBOLS.update(overrides)
        logger.info(f"Force-close override applied: {overrides}")

    try:
        asyncio.run(run_closer())
        logger.info("Closer cycle completed successfully.")
        suffix = " [DRY-RUN]" if DRY_RUN else ""
        return {"statusCode": 200, "body": f"Closer cycle complete.{suffix}"}

    except LegCloseError as exc:
        logger.critical(f"LEG CLOSE IMBALANCE: {exc}")
        return {"statusCode": 500, "body": f"CRITICAL LEG CLOSE IMBALANCE: {exc}"}

    except Exception as exc:
        logger.error(f"Unhandled exception in closer: {exc}", exc_info=True)
        return {"statusCode": 500, "body": f"Closer failed: {exc}"}


# ─────────────────────────────────────────────────────────────────────────────
# Standalone execution (EC2 / local cron)
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    event = {}
    # Support: python3 close_lambda.py --force BTC,ETH
    if "--force" in sys.argv:
        idx = sys.argv.index("--force")
        if idx + 1 < len(sys.argv):
            event["force_close"] = sys.argv[idx + 1]
    lambda_handler(event, type("Ctx", (), {"aws_request_id": "local"})())
