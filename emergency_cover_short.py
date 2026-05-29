#!/usr/bin/env python3
"""Emergency: market-buy to cover a naked futures SHORT. Usage: python3 emergency_cover_short.py NEAR"""
from __future__ import annotations

import os
import sys

import ccxt


def main() -> None:
    base = (sys.argv[1] if len(sys.argv) > 1 else "").strip().upper()
    if not base:
        print("Usage: python3 emergency_cover_short.py SYMBOL  (e.g. NEAR)")
        sys.exit(1)

    ex = ccxt.binance(
        {
            "apiKey": os.environ["BINANCE_API_KEY"],
            "secret": os.environ["BINANCE_API_SECRET"],
            "options": {"defaultType": "future", "fetchCurrencies": False},
        }
    )
    ex.load_markets()
    sym = f"{base}/USDT:USDT"

    qty = 0.0
    for p in ex.fetch_positions([sym]):
        contracts = float(p.get("contracts") or 0)
        side = p.get("side", "")
        if contracts == 0:
            continue
        if side == "short" or contracts < 0:
            qty = abs(contracts)
            break

    if qty <= 0:
        print(f"No futures short found for {base}.")
        return

    qty = float(ex.amount_to_precision(sym, qty))
    print(f"Covering {base} short qty={qty}...")
    o = ex.create_order(
        sym, "market", "buy", qty, params={"reduceOnly": True, "positionSide": "BOTH"}
    )
    print(f"Done. Order ID: {o.get('id')}")


if __name__ == "__main__":
    main()
