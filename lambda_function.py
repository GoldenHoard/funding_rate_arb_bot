"""
Delta-Neutral Funding Rate Arbitrage Bot
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
AWS Lambda Function | Binance USDⓈ-M Perpetuals + Spot

Strategy Logic:
  1. Scan ALL Binance USDM perpetuals for positive funding rates
  2. Filter out coins below MIN_VOLUME_USD on either leg + optional MIN_LEG_VOLUME_RATIO
  3. Rank by funding rate descending; select TOP_N candidates (default 3)
  4. Pre-trade slippage/spread check: abort individual coin if > 0.2%
  5. Concurrently fire: Market BUY Spot + Market SELL (Short) Futures @ 1x leverage
  6. On any leg imbalance (one fills, one fails): raise CRITICAL and halt execution
  7. Broadcast per-trade and session summary to Telegram

Deployment:
  - Runtime  : Python 3.10+ (Lambda)
  - Trigger  : AWS EventBridge Cron — recommended every 8h aligned to funding settle
  - Env Vars : BINANCE_API_KEY, BINANCE_API_SECRET, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
               CAPITAL_PER_LEG_USD (optional, default 1000)
  - Layer    : ccxt >= 4.2.0, aiohttp >= 3.9.0 (see requirements.txt)

Risk Notes:
  - This script OPENS positions only. A companion "close" Lambda (or manual process)
    is required to unwind when funding turns negative.
  - Position sizing is per-leg USD; total capital deployed = CAPITAL_PER_LEG_USD * TOP_N * 2
    (one spot leg + one futures leg per symbol).
  - Always test on Binance Testnet before deploying live capital.
"""

from __future__ import annotations

import asyncio
import logging
import os
from dataclasses import dataclass, field
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
# Configuration — all tuneable values in one place
# ─────────────────────────────────────────────────────────────────────────────

# USD notional per leg (spot buy = $1k, futures short = $1k → $2k total per symbol)
CAPITAL_PER_LEG_USD: float = float(os.environ.get("CAPITAL_PER_LEG_USD", "1000.0"))

# Number of top-ranked candidates to trade
TOP_N: int = int(os.environ.get("TOP_N", "3"))

# 24h quote volume floor for BOTH spot and futures legs (USDT quoted).
# Env MIN_VOLUME_USD — default loosened from $50M to $25M for more opportunities.
MIN_VOLUME_USD: float = float(os.environ.get("MIN_VOLUME_USD", "25000000"))

# Minimum min(spot,futures) / max(spot,futures) 24h volume ratio.
# Rejects one-sided hype (thin spot + hot perp) — a common cause of bad fills despite high APY.
# Set to 0 to disable this filter entirely.
MIN_LEG_VOLUME_RATIO: float = float(os.environ.get("MIN_LEG_VOLUME_RATIO", "0.2"))

# Maximum tolerated combined slippage (worst of VWAP slip vs cross-spread) before aborting.
MAX_SLIPPAGE_PCT: float = float(os.environ.get("MAX_SLIPPAGE_PCT", "0.002"))

# Order book depth levels for VWAP simulation (more levels = stabler estimate on wide books).
ORDER_BOOK_LIMIT: int = int(os.environ.get("ORDER_BOOK_LIMIT", "40"))

# Minimum 8h funding rate required to OPEN a new position.
# Default: 0.00008 ≈ ~8.8% gross APY (before fees/slippage). Tune MIN_FUNDING_RATE_TO_OPEN in .env.
MIN_FUNDING_RATE_TO_OPEN: float = float(
    os.environ.get("MIN_FUNDING_RATE_TO_OPEN", "0.00008")
)

# Binance pays/charges funding every 8 hours → 3 × 365 periods per year
FUNDING_PERIODS_PER_YEAR: int = 3 * 365

# Comma-separated base assets to never trade. e.g. "BNB,BUSD"
# BNB is blacklisted by default to protect fee-payment balances.
_blacklist_raw: str = os.environ.get("BLACKLIST_SYMBOLS", "BNB")
BLACKLIST_SYMBOLS: set[str] = {
    s.strip().upper() for s in _blacklist_raw.split(",") if s.strip()
}

# ─────────────────────────────────────────────────────────────────────────────
# Safety Modes
# ─────────────────────────────────────────────────────────────────────────────

# DRY_RUN=true  → Run every check (funding scan, volume filter, slippage) but
#                 SKIP actual order placement. Simulates fills using BBO mid-price.
#                 Safe to run against the live API with real credentials.
DRY_RUN: bool = os.environ.get("DRY_RUN", "false").lower() == "true"

# USE_TESTNET=true → Point ccxt at Binance Testnet endpoints.
#                    Requires separate testnet API keys from testnet.binancefuture.com
#                    Note: Testnet spot liquidity is thin; slippage checks may abort.
USE_TESTNET: bool = os.environ.get("USE_TESTNET", "false").lower() == "true"

# ─────────────────────────────────────────────────────────────────────────────
# Secrets — pulled from Lambda environment variables (set via SSM/Secrets Mgr)
# ─────────────────────────────────────────────────────────────────────────────

BINANCE_API_KEY: str = os.environ["BINANCE_API_KEY"]
BINANCE_API_SECRET: str = os.environ["BINANCE_API_SECRET"]
TELEGRAM_BOT_TOKEN: str = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID: str = os.environ.get("TELEGRAM_CHAT_ID", "")

# ─────────────────────────────────────────────────────────────────────────────
# Data Classes
# ─────────────────────────────────────────────────────────────────────────────


@dataclass
class TradeCandidate:
    """A coin that has passed funding-rate and volume screening."""
    symbol: str          # Base asset ticker, e.g. "ETH"
    funding_rate: float  # Current 8h funding rate (decimal)
    spot_symbol: str     # ccxt unified spot symbol,    e.g. "ETH/USDT"
    futures_symbol: str  # ccxt unified futures symbol, e.g. "ETH/USDT:USDT"
    spot_volume_usd: float = 0.0
    futures_volume_usd: float = 0.0


@dataclass
class TradeResult:
    """Execution record for a completed (or failed) trade."""
    symbol: str
    spot_entry: float
    futures_entry: float
    quantity: float
    funding_rate: float
    slippage_pct: float
    projected_apy: float
    spot_order_id: str = ""
    futures_order_id: str = ""
    error: Optional[str] = None  # Set only if this result represents a failure


# ─────────────────────────────────────────────────────────────────────────────
# Custom Exceptions
# ─────────────────────────────────────────────────────────────────────────────


class LegExecutionError(Exception):
    """
    Raised when one leg of the delta-neutral trade filled but the other failed.
    This leaves naked directional exposure and requires IMMEDIATE manual action.
    """


# ─────────────────────────────────────────────────────────────────────────────
# Exchange Factory
# ─────────────────────────────────────────────────────────────────────────────


def _make_spot_exchange() -> ccxt.binance:
    """Binance Spot exchange instance with async support."""
    config: dict = {
        "apiKey": BINANCE_API_KEY,
        "secret": BINANCE_API_SECRET,
        "enableRateLimit": True,
        "options": {
            "defaultType": "spot",
            "adjustForTimeDifference": True,
            # Prevent ccxt from calling sapi/v1/capital/config/getall during
            # load_markets(). That endpoint requires withdrawal permissions which
            # are unnecessary and often disabled on trading-only API keys.
            "fetchCurrencies": False,
        },
    }
    if USE_TESTNET:
        # Binance does not offer a public spot testnet; using the same testnet
        # base URL that hosts both spot and futures paper trading accounts.
        config["options"]["sandboxMode"] = True
        logger.info("SPOT exchange → TESTNET mode")
    return ccxt.binance(config)


def _make_futures_exchange() -> ccxt.binance:
    """Binance USDⓈ-M Perpetuals exchange instance with async support."""
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
# Phase 1 — Candidate Discovery: Funding Rate Scan + Volume Filter
# ─────────────────────────────────────────────────────────────────────────────


async def fetch_top_candidates(
    spot_ex: ccxt.binance,
    futures_ex: ccxt.binance,
) -> list[TradeCandidate]:
    """
    Identify the top-N altcoins by funding rate that pass the volume filter.

    Efficiency note: We call fetch_tickers() once per exchange (a single bulk
    REST call) rather than n individual ticker fetches. This keeps the discovery
    phase fast and well within Binance rate limits.
    """
    logger.info("Loading markets and fetching bulk data...")

    # Parallel: load markets (needed for precision formatting later) +
    #           fetch all funding rates + fetch all 24h tickers in one shot each
    (
        _spot_markets,
        _futures_markets,
        all_funding_rates,
        spot_tickers,
        futures_tickers,
    ) = await asyncio.gather(
        spot_ex.load_markets(),
        futures_ex.load_markets(),
        futures_ex.fetch_funding_rates(),   # {futures_symbol: {fundingRate: float, ...}}
        spot_ex.fetch_tickers(),            # {spot_symbol:    {quoteVolume: float, ...}}
        futures_ex.fetch_tickers(),         # {futures_symbol: {quoteVolume: float, ...}}
    )

    candidates: list[TradeCandidate] = []

    for futures_symbol, rate_info in all_funding_rates.items():
        funding_rate: Optional[float] = rate_info.get("fundingRate")

        # Only trade when longs are paying shorts (positive rate = income for us)
        if funding_rate is None or funding_rate <= 0:
            continue

        # Enforce minimum APY floor — below this the carry doesn't cover fees
        if funding_rate < MIN_FUNDING_RATE_TO_OPEN:
            continue

        # Derive the spot symbol from the futures symbol.
        # ccxt USDM format: "BTC/USDT:USDT" → base = "BTC", spot = "BTC/USDT"
        parts = futures_symbol.split("/")
        if len(parts) != 2:
            continue
        base = parts[0]

        if base in BLACKLIST_SYMBOLS:
            continue

        spot_symbol = f"{base}/USDT"

        # Skip if this coin has no corresponding spot market on Binance
        if spot_symbol not in spot_tickers or futures_symbol not in futures_tickers:
            continue

        # ── Volume Filter ──────────────────────────────────────────────────
        # quoteVolume is the 24h trading volume denominated in USDT
        spot_vol: float = spot_tickers[spot_symbol].get("quoteVolume") or 0.0
        futures_vol: float = futures_tickers[futures_symbol].get("quoteVolume") or 0.0

        if spot_vol < MIN_VOLUME_USD:
            logger.debug(
                f"SKIP {base}: Spot vol ${spot_vol/1e6:.1f}M < ${MIN_VOLUME_USD/1e6:.0f}M floor"
            )
            continue

        if futures_vol < MIN_VOLUME_USD:
            logger.debug(
                f"SKIP {base}: Futures vol ${futures_vol/1e6:.1f}M < ${MIN_VOLUME_USD/1e6:.0f}M floor"
            )
            continue

        if MIN_LEG_VOLUME_RATIO > 0:
            vmax = max(spot_vol, futures_vol)
            if vmax > 0 and min(spot_vol, futures_vol) / vmax < MIN_LEG_VOLUME_RATIO:
                logger.debug(
                    f"SKIP {base}: leg volume imbalance "
                    f"(spot ${spot_vol/1e6:.1f}M vs fut ${futures_vol/1e6:.1f}M "
                    f"< ratio {MIN_LEG_VOLUME_RATIO:.0%})"
                )
                continue

        candidates.append(TradeCandidate(
            symbol=base,
            funding_rate=funding_rate,
            spot_symbol=spot_symbol,
            futures_symbol=futures_symbol,
            spot_volume_usd=spot_vol,
            futures_volume_usd=futures_vol,
        ))

    if not candidates:
        min_apy = MIN_FUNDING_RATE_TO_OPEN * FUNDING_PERIODS_PER_YEAR * 100
        logger.warning(
            f"No candidates survived filters. "
            f"Min APY floor: {min_apy:.1f}% ({MIN_FUNDING_RATE_TO_OPEN*100:.4f}% per 8h). "
            f"Market funding rates are currently too low to trade."
        )
        return []

    # Sort descending by funding rate; take the top N
    candidates.sort(key=lambda c: c.funding_rate, reverse=True)
    top = candidates[:TOP_N]

    for c in top:
        logger.info(
            f"CANDIDATE: {c.symbol:<8} | FundingRate: {c.funding_rate * 100:+.4f}%"
            f" | SpotVol: ${c.spot_volume_usd / 1e6:.0f}M"
            f" | FutVol: ${c.futures_volume_usd / 1e6:.0f}M"
        )

    return top


# ─────────────────────────────────────────────────────────────────────────────
# Phase 2 — Pre-Trade Slippage & Spread Check
# ─────────────────────────────────────────────────────────────────────────────


async def check_slippage_and_bbo(
    spot_ex: ccxt.binance,
    futures_ex: ccxt.binance,
    candidate: TradeCandidate,
) -> tuple[bool, float, float, float]:
    """
    Fetch the Level-2 orderbook for both legs and simulate the VWAP fill price
    for a CAPITAL_PER_LEG_USD market order.

    Returns:
        ok            — True if combined slippage is within MAX_SLIPPAGE_PCT
        spot_vwap     — Simulated spot ask-side fill price
        futures_vwap  — Simulated futures bid-side fill price
        total_slip    — Combined slippage fraction (worst of spread vs VWAP deviation)

    Fetches 20 levels; for a $1k order on a $50M+ daily volume coin this
    captures more than enough liquidity. Adjust `limit` for larger positions.
    """
    try:
        spot_ob, futures_ob = await asyncio.gather(
            spot_ex.fetch_order_book(candidate.spot_symbol, limit=ORDER_BOOK_LIMIT),
            futures_ex.fetch_order_book(candidate.futures_symbol, limit=ORDER_BOOK_LIMIT),
        )
    except Exception as exc:
        raise RuntimeError(
            f"Orderbook fetch failed for {candidate.symbol}: {exc}"
        ) from exc

    def _simulate_vwap(levels: list[list[float]], notional_usd: float) -> tuple[float, float]:
        """
        Walk the orderbook levels to compute VWAP for a given USD notional.

        Args:
            levels       : [[price, qty], ...] — asks (ascending) or bids (descending)
            notional_usd : total USD value to fill

        Returns:
            (vwap, best_price) — best_price is the innermost (most favourable) level
        """
        remaining_usd = notional_usd
        filled_base_qty = 0.0
        filled_usd = 0.0
        best_price = levels[0][0] if levels else 0.0

        for price, size in levels:
            if remaining_usd <= 0:
                break
            level_usd = price * size
            take_usd = min(level_usd, remaining_usd)
            filled_base_qty += take_usd / price
            filled_usd += take_usd
            remaining_usd -= take_usd

        vwap = (filled_usd / filled_base_qty) if filled_base_qty > 0 else best_price
        return vwap, best_price

    spot_asks: list = spot_ob.get("asks", [])
    futures_bids: list = futures_ob.get("bids", [])

    if not spot_asks or not futures_bids:
        raise RuntimeError(f"Empty orderbook returned for {candidate.symbol}")

    spot_vwap, spot_best_ask = _simulate_vwap(spot_asks, CAPITAL_PER_LEG_USD)
    futures_vwap, futures_best_bid = _simulate_vwap(futures_bids, CAPITAL_PER_LEG_USD)

    # ── Slippage Components ────────────────────────────────────────────────
    # Spot slippage: how much worse is our VWAP vs the innermost ask?
    spot_slip = (spot_vwap - spot_best_ask) / spot_best_ask if spot_best_ask else 0.0
    # Futures slippage: how much worse is our VWAP vs the innermost bid?
    futures_slip = (futures_best_bid - futures_vwap) / futures_best_bid if futures_best_bid else 0.0
    total_slip = spot_slip + futures_slip

    # ── Bid-Ask Spread (sanity cross-check) ───────────────────────────────
    spot_mid = (spot_asks[0][0] + spot_ob["bids"][0][0]) / 2 if spot_ob.get("bids") else spot_asks[0][0]
    futures_mid = (futures_ob["asks"][0][0] + futures_bids[0][0]) / 2 if futures_ob.get("asks") else futures_bids[0][0]
    # Cross-venue spread: in a perfectly flat basis market this would be ≈0;
    # we flag it if it eats too much of the expected funding income.
    cross_spread = abs(spot_mid - futures_mid) / spot_mid if spot_mid else 0.0

    worst = max(total_slip, cross_spread)
    ok = worst <= MAX_SLIPPAGE_PCT

    logger.info(
        f"SLIPPAGE CHECK {candidate.symbol}: "
        f"SpotVWAP={spot_vwap:.6f} FutVWAP={futures_vwap:.6f} | "
        f"SpotSlip={spot_slip * 100:.3f}% FutSlip={futures_slip * 100:.3f}% "
        f"CrossSpread={cross_spread * 100:.3f}% → {'PASS' if ok else 'ABORT'}"
    )

    return ok, spot_vwap, futures_vwap, worst


# ─────────────────────────────────────────────────────────────────────────────
# Phase 3 — Precision Formatting (stepSize / tickSize compliance)
# ─────────────────────────────────────────────────────────────────────────────


def fmt_amount(exchange: ccxt.binance, symbol: str, amount: float) -> float:
    """
    Round an order quantity to the exchange's stepSize.
    Binance rejects orders whose amount has more decimal places than stepSize allows.
    ccxt reads this from the market's `precision.amount` field loaded via load_markets().
    """
    return float(exchange.amount_to_precision(symbol, amount))


def fmt_price(exchange: ccxt.binance, symbol: str, price: float) -> float:
    """
    Round a price to the exchange's tickSize.
    Not strictly needed for pure market orders, but included for limit-order extensions.
    """
    return float(exchange.price_to_precision(symbol, price))


# ─────────────────────────────────────────────────────────────────────────────
# Phase 4 — Futures Margin Setup
# ─────────────────────────────────────────────────────────────────────────────


async def configure_futures_margin(
    futures_ex: ccxt.binance,
    futures_symbol: str,
) -> None:
    """
    Enforce isolated margin + 1x leverage before placing any order.

    Why isolated over cross? At 1x leverage our spot position fully hedges the
    futures short, but isolated margin caps the maximum loss to the posted
    margin if something goes wrong, preventing contagion to other positions.
    """
    if DRY_RUN:
        logger.info(f"[DRY-RUN] Skipping margin/leverage config for {futures_symbol}")
        return
    try:
        # set_margin_mode raises if already in this mode on some accounts — swallow it
        await futures_ex.set_margin_mode("isolated", futures_symbol)
        logger.info(f"Margin mode → ISOLATED for {futures_symbol}")
    except ccxt.MarginModeAlreadySet:
        logger.debug(f"Margin already isolated for {futures_symbol}")
    except Exception as exc:
        logger.warning(f"set_margin_mode warning ({futures_symbol}): {exc}")

    try:
        await futures_ex.set_leverage(1, futures_symbol)
        logger.info(f"Leverage → 1x for {futures_symbol}")
    except Exception as exc:
        # Binance may raise if leverage is already 1x; non-fatal
        logger.warning(f"set_leverage warning ({futures_symbol}): {exc}")


# ─────────────────────────────────────────────────────────────────────────────
# Phase 5 — Concurrent Order Execution (Legging Risk Minimisation)
# ─────────────────────────────────────────────────────────────────────────────


async def execute_delta_neutral_trade(
    spot_ex: ccxt.binance,
    futures_ex: ccxt.binance,
    candidate: TradeCandidate,
    spot_ref_price: float,    # From VWAP simulation — used for qty calculation only
    futures_ref_price: float,
    slippage_pct: float,
) -> TradeResult:
    """
    Fire both legs concurrently via asyncio.gather to minimise the time window
    between spot and futures fills (legging risk).

    Order of operations:
      1. Compute raw quantity from capital and reference price
      2. Format to exchange precision rules (stepSize)
      3. Dispatch spot BUY + futures SELL simultaneously
      4. Inspect results: both ok → record; one ok → LegExecutionError; both fail → RuntimeError
    """
    raw_qty = CAPITAL_PER_LEG_USD / spot_ref_price

    # Apply precision formatting BEFORE sending — Binance will reject otherwise
    spot_qty = fmt_amount(spot_ex, candidate.spot_symbol, raw_qty)
    futures_qty = fmt_amount(futures_ex, candidate.futures_symbol, raw_qty)

    mode_tag = "[DRY-RUN] " if DRY_RUN else ""
    logger.info(
        f"{mode_tag}PLACING ORDERS: {candidate.symbol} | "
        f"SpotQty={spot_qty} FuturesQty={futures_qty} "
        f"Notional≈${spot_ref_price * spot_qty:,.2f}"
    )

    if DRY_RUN:
        # Simulate fills using the VWAP reference prices already calculated
        # by the slippage check. No real orders are placed.
        logger.info(
            f"[DRY-RUN] Simulated fill: SpotBuy@{spot_ref_price:.6f} "
            f"FuturesSell@{futures_ref_price:.6f}"
        )
        projected_apy = candidate.funding_rate * FUNDING_PERIODS_PER_YEAR
        return TradeResult(
            symbol=candidate.symbol,
            spot_entry=spot_ref_price,
            futures_entry=futures_ref_price,
            quantity=spot_qty,
            funding_rate=candidate.funding_rate,
            slippage_pct=slippage_pct,
            projected_apy=projected_apy,
            spot_order_id="DRY_RUN_SPOT",
            futures_order_id="DRY_RUN_FUTURES",
        )

    # Build coroutines (do not await yet — we'll launch them together below)
    spot_coro = spot_ex.create_order(
        symbol=candidate.spot_symbol,
        type="market",
        side="buy",
        amount=spot_qty,
        # No extra params needed for a plain spot market buy
    )

    futures_coro = futures_ex.create_order(
        symbol=candidate.futures_symbol,
        type="market",
        side="sell",       # "sell" opens a SHORT in one-way position mode
        amount=futures_qty,
        params={
            # positionSide="BOTH" is correct for Binance one-way mode (default).
            # If your account is in hedge mode, change to "SHORT".
            "positionSide": "BOTH",
        },
    )

    # ── Concurrent Dispatch ────────────────────────────────────────────────
    # return_exceptions=True lets us inspect each result individually rather
    # than having gather() cancel the other leg on the first exception.
    raw_results: list = await asyncio.gather(
        spot_coro,
        futures_coro,
        return_exceptions=True,
    )

    spot_result = raw_results[0]
    futures_result = raw_results[1]

    spot_ok = not isinstance(spot_result, Exception)
    futures_ok = not isinstance(futures_result, Exception)

    # ── Result Triage ──────────────────────────────────────────────────────

    if spot_ok and futures_ok:
        # Happy path — extract actual VWAP fill prices from exchange response
        spot_fill = float(
            spot_result.get("average") or spot_result.get("price") or spot_ref_price
        )
        futures_fill = float(
            futures_result.get("average") or futures_result.get("price") or futures_ref_price
        )
        projected_apy = candidate.funding_rate * FUNDING_PERIODS_PER_YEAR

        logger.info(
            f"FILLED {candidate.symbol}: SpotFill={spot_fill:.6f} "
            f"FuturesFill={futures_fill:.6f} APY≈{projected_apy * 100:.2f}%"
        )
        return TradeResult(
            symbol=candidate.symbol,
            spot_entry=spot_fill,
            futures_entry=futures_fill,
            quantity=spot_qty,
            funding_rate=candidate.funding_rate,
            slippage_pct=slippage_pct,
            projected_apy=projected_apy,
            spot_order_id=str(spot_result.get("id", "")),
            futures_order_id=str(futures_result.get("id", "")),
        )

    elif spot_ok and not futures_ok:
        # ⚠ CRITICAL: Spot filled → we are now LONG {symbol} with no hedge.
        # Do NOT silently continue. Raise immediately for human intervention.
        raise LegExecutionError(
            f"[{candidate.symbol}] SPOT BUY filled (ID={spot_result.get('id','?')}) "
            f"but FUTURES SHORT FAILED: {futures_result}. "
            f"UNHEDGED LONG EXPOSURE — close the spot position manually NOW."
        )

    elif futures_ok and not spot_ok:
        # ⚠ CRITICAL: Futures filled → we are now SHORT {symbol} with no hedge.
        raise LegExecutionError(
            f"[{candidate.symbol}] FUTURES SHORT filled (ID={futures_result.get('id','?')}) "
            f"but SPOT BUY FAILED: {spot_result}. "
            f"UNHEDGED SHORT EXPOSURE — close the futures position manually NOW."
        )

    else:
        # Both legs failed — no exposure created, safe to move on to next candidate
        raise RuntimeError(
            f"Both legs failed for {candidate.symbol}. "
            f"SpotErr: {spot_result} | FuturesErr: {futures_result}"
        )


# ─────────────────────────────────────────────────────────────────────────────
# Phase 6 — Telegram Alerting
# ─────────────────────────────────────────────────────────────────────────────


async def _post_telegram(text: str) -> None:
    """Low-level Telegram Bot API call. Fire-and-forget with a 10s timeout."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.warning("Telegram credentials not set — skipping alert.")
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML"}

    try:
        timeout = aiohttp.ClientTimeout(total=10)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(url, json=payload) as resp:
                if resp.status != 200:
                    body = await resp.text()
                    logger.error(f"Telegram HTTP {resp.status}: {body[:200]}")
                else:
                    logger.info("Telegram alert delivered.")
    except Exception as exc:
        # Alerting failure must never crash the trade loop
        logger.error(f"Telegram alert failed: {exc}")


def _fmt_trade_alert(r: TradeResult) -> str:
    """Rich HTML message for a successful trade execution."""
    mode = "<b>[DRY-RUN] </b>" if DRY_RUN else ""
    return (
        f"{mode}<b>✅ FUNDING ARB — POSITION OPENED</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"<b>Asset:</b>           {r.symbol}/USDT\n"
        f"<b>Spot Entry:</b>      ${r.spot_entry:.6f}\n"
        f"<b>Futures Entry:</b>   ${r.futures_entry:.6f}\n"
        f"<b>Quantity:</b>        {r.quantity:.6f} {r.symbol}\n"
        f"<b>Notional:</b>        ${r.spot_entry * r.quantity:,.2f}\n"
        f"<b>Funding Rate (8h):</b> {r.funding_rate * 100:.4f}%\n"
        f"<b>Projected APY:</b>   {r.projected_apy * 100:.2f}%\n"
        f"<b>Slippage:</b>        {r.slippage_pct * 100:.3f}%\n"
        f"<b>Spot Order ID:</b>   <code>{r.spot_order_id}</code>\n"
        f"<b>Futures Order ID:</b> <code>{r.futures_order_id}</code>"
    )


def _fmt_abort_alert(symbol: str, reason: str) -> str:
    """Alert for a trade that was screened out before order placement."""
    return (
        "<b>⏭ TRADE ABORTED</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        f"<b>Asset:</b>  {symbol}\n"
        f"<b>Reason:</b> {reason[:400]}"
    )


def _fmt_critical_alert(detail: str) -> str:
    """High-urgency alert for leg imbalance — requires immediate manual action."""
    return (
        "🚨 <b>CRITICAL — LEG IMBALANCE DETECTED</b> 🚨\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{detail[:800]}\n\n"
        "<b>ACTION REQUIRED: Close the filled leg immediately to restore delta-neutrality.</b>"
    )


def _fmt_session_summary(
    results: list[TradeResult],
    skipped: list[tuple[str, str]],
) -> str:
    """End-of-cycle digest showing all outcomes."""
    lines = ["<b>📊 FUNDING ARB SESSION SUMMARY</b>", "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"]
    for r in results:
        if r.error:
            lines.append(f"❌ <b>{r.symbol}</b>: {r.error[:120]}")
        else:
            lines.append(
                f"✅ <b>{r.symbol}</b>: APY {r.projected_apy * 100:.1f}%"
                f" | Slip {r.slippage_pct * 100:.3f}%"
                f" | Rate {r.funding_rate * 100:.4f}%"
            )
    for sym, reason in skipped:
        lines.append(f"⏭ <b>{sym}</b>: {reason}")
    if not results and not skipped:
        lines.append("No candidates found this cycle.")
    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# Main Orchestration
# ─────────────────────────────────────────────────────────────────────────────


async def run_strategy() -> None:
    """
    Full end-to-end strategy execution.

    Phases:
      1 — Candidate discovery (bulk fetch, volume filter, ranking)
      2 — Per-candidate margin setup
      3 — Slippage / spread check
      4 — Concurrent order execution
      5 — Session summary alert
    """
    spot_ex = _make_spot_exchange()
    futures_ex = _make_futures_exchange()

    results: list[TradeResult] = []
    skipped: list[tuple[str, str]] = []  # (symbol, reason)

    try:
        # ── Phase 1: Discover candidates ──────────────────────────────────
        candidates = await fetch_top_candidates(spot_ex, futures_ex)

        if not candidates:
            msg = "No viable funding-rate candidates found this cycle."
            logger.warning(msg)
            await _post_telegram(f"<b>Funding Arb Bot</b>\n{msg}")
            return

        # ── Per-candidate execution loop ───────────────────────────────────
        for candidate in candidates:
            logger.info(f"━━ Processing {candidate.symbol} ━━")

            # Phase 1.5: Duplicate check — skip if we already have an open position
            if db.is_position_open(candidate.symbol):
                reason = "Position already OPEN in DB — skipping to avoid duplicate"
                logger.info(f"SKIP {candidate.symbol}: {reason}")
                skipped.append((candidate.symbol, reason))
                continue

            # Phase 2: Enforce isolated margin + 1x leverage
            await configure_futures_margin(futures_ex, candidate.futures_symbol)

            # Phase 3: Slippage check — gate before any order is placed
            try:
                ok, spot_price, futures_price, slippage = await check_slippage_and_bbo(
                    spot_ex, futures_ex, candidate
                )
            except Exception as exc:
                reason = f"Orderbook error: {exc}"
                logger.error(f"Aborting {candidate.symbol}: {reason}")
                skipped.append((candidate.symbol, reason))
                await _post_telegram(_fmt_abort_alert(candidate.symbol, reason))
                continue  # Try next candidate

            if not ok:
                reason = (
                    f"Slippage {slippage * 100:.3f}% exceeds "
                    f"{MAX_SLIPPAGE_PCT * 100:.1f}% limit"
                )
                logger.warning(f"Aborting {candidate.symbol}: {reason}")
                skipped.append((candidate.symbol, reason))
                await _post_telegram(_fmt_abort_alert(candidate.symbol, reason))
                continue

            # Phase 4: Fire both legs concurrently
            try:
                result = await execute_delta_neutral_trade(
                    spot_ex, futures_ex,
                    candidate,
                    spot_price, futures_price,
                    slippage,
                )
                results.append(result)

                # Record the open position in DynamoDB
                if not result.error:
                    db.record_open(
                        symbol=result.symbol,
                        spot_entry=result.spot_entry,
                        futures_entry=result.futures_entry,
                        quantity=result.quantity,
                        funding_rate=result.funding_rate,
                        projected_apy=result.projected_apy,
                        spot_order_id=result.spot_order_id,
                        futures_order_id=result.futures_order_id,
                    )

                await _post_telegram(_fmt_trade_alert(result))

            except LegExecutionError as exc:
                # ⚠ One leg filled, one didn't — HALT and alert immediately.
                # Re-raise after alerting so Lambda marks the invocation failed
                # and CloudWatch/EventBridge can trigger an incident alarm.
                logger.critical(str(exc))
                await _post_telegram(_fmt_critical_alert(str(exc)))
                raise  # Propagate to lambda_handler

            except Exception as exc:
                # Both legs failed, or other non-critical error — log and continue
                reason = str(exc)
                logger.error(f"Execution error for {candidate.symbol}: {reason}")
                results.append(TradeResult(
                    symbol=candidate.symbol,
                    spot_entry=0.0,
                    futures_entry=0.0,
                    quantity=0.0,
                    funding_rate=candidate.funding_rate,
                    slippage_pct=0.0,
                    projected_apy=0.0,
                    error=reason,
                ))
                await _post_telegram(_fmt_abort_alert(candidate.symbol, reason))

        # ── Phase 5: Session summary ───────────────────────────────────────
        await _post_telegram(_fmt_session_summary(results, skipped))

    finally:
        # Always release aiohttp connections and ccxt resources
        await asyncio.gather(
            spot_ex.close(),
            futures_ex.close(),
            return_exceptions=True,  # Swallow close() errors; don't mask real exceptions
        )
        logger.info("Exchange connections closed.")


# ─────────────────────────────────────────────────────────────────────────────
# AWS Lambda Entry Point
# ─────────────────────────────────────────────────────────────────────────────


def lambda_handler(event: dict, context) -> dict:
    """
    AWS Lambda handler — triggered by an EventBridge Scheduler rule.

    Recommended cron expression (runs 5 minutes before each 8h funding window):
      cron(55 7,15,23 * * ? *)   → fires at 07:55, 15:55, 23:55 UTC

    Returns a standard API-Gateway-compatible response dict so the function
    can also be invoked manually via the Lambda console or AWS CLI without errors.
    """
    logger.info(
        f"Lambda invoked. RequestID={getattr(context, 'aws_request_id', 'local')} "
        f"Event={event}"
    )
    if DRY_RUN:
        logger.warning("=" * 60)
        logger.warning("  DRY-RUN MODE ACTIVE — no real orders will be placed")
        logger.warning("=" * 60)
    if USE_TESTNET:
        logger.warning("  TESTNET MODE ACTIVE — using Binance paper trading endpoints")

    try:
        # asyncio.run() creates a fresh event loop each invocation, which is
        # correct for Lambda (execution environments may be reused but we
        # should never share loop state across invocations).
        asyncio.run(run_strategy())
        logger.info("Strategy cycle completed successfully.")
        suffix = " [DRY-RUN]" if DRY_RUN else ""
        return {"statusCode": 200, "body": f"Funding rate arb cycle complete.{suffix}"}

    except LegExecutionError as exc:
        # Leg imbalance — return 500 so EventBridge/CloudWatch alarms fire
        logger.critical(f"LEG IMBALANCE: {exc}")
        return {"statusCode": 500, "body": f"CRITICAL LEG IMBALANCE: {exc}"}

    except Exception as exc:
        logger.error(f"Unhandled exception in strategy: {exc}", exc_info=True)
        return {"statusCode": 500, "body": f"Strategy failed: {exc}"}


# ─────────────────────────────────────────────────────────────────────────────
# Standalone execution (EC2 / local cron)
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    lambda_handler({}, type("Ctx", (), {"aws_request_id": "local"})())
