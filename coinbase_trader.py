"""Real Coinbase Advanced Trade API integration.

Wraps the ``coinbase-advanced-py`` SDK to place real buy/sell orders
on the Coinbase exchange.  Used when the arena is running in **live**
trading mode.

The SDK is synchronous, so all methods that hit the network are
designed to be called via ``asyncio.to_thread()`` from the async arena.
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass

from coinbase.rest import RESTClient

logger = logging.getLogger(__name__)


@dataclass
class OrderResult:
    success: bool
    order_id: str | None
    message: str
    filled_price: float | None = None
    filled_qty: float | None = None


class CoinbaseTrader:
    """Thin wrapper around the Coinbase Advanced Trade REST API."""

    def __init__(self, api_key: str, api_secret: str) -> None:
        self._client = RESTClient(api_key=api_key, api_secret=api_secret)
        logger.info("CoinbaseTrader initialized (live trading enabled)")

    # ── Market orders ─────────────────────────────────────────────

    def market_buy(self, product_id: str, quote_size: float) -> OrderResult:
        """Place a market buy order spending *quote_size* USD."""
        try:
            resp = self._client.market_order_buy(
                client_order_id=str(uuid.uuid4()),
                product_id=product_id,
                quote_size=str(round(quote_size, 2)),
            )
            return self._parse_order_response(resp, "buy", product_id)
        except Exception as e:
            logger.exception("Market buy failed for %s", product_id)
            return OrderResult(False, None, f"Coinbase market buy failed: {e}")

    def market_sell(self, product_id: str, base_size: float) -> OrderResult:
        """Place a market sell order selling *base_size* units of crypto."""
        try:
            resp = self._client.market_order_sell(
                client_order_id=str(uuid.uuid4()),
                product_id=product_id,
                base_size=str(base_size),
            )
            return self._parse_order_response(resp, "sell", product_id)
        except Exception as e:
            logger.exception("Market sell failed for %s", product_id)
            return OrderResult(False, None, f"Coinbase market sell failed: {e}")

    # ── Limit orders ──────────────────────────────────────────────

    def limit_buy(
        self, product_id: str, base_size: float, limit_price: float
    ) -> OrderResult:
        """Place a GTC limit buy order."""
        try:
            resp = self._client.limit_order_gtc_buy(
                client_order_id=str(uuid.uuid4()),
                product_id=product_id,
                base_size=str(base_size),
                limit_price=str(round(limit_price, 2)),
            )
            return self._parse_order_response(resp, "limit_buy", product_id)
        except Exception as e:
            logger.exception("Limit buy failed for %s", product_id)
            return OrderResult(False, None, f"Coinbase limit buy failed: {e}")

    def limit_sell(
        self, product_id: str, base_size: float, limit_price: float
    ) -> OrderResult:
        """Place a GTC limit sell order."""
        try:
            resp = self._client.limit_order_gtc_sell(
                client_order_id=str(uuid.uuid4()),
                product_id=product_id,
                base_size=str(base_size),
                limit_price=str(round(limit_price, 2)),
            )
            return self._parse_order_response(resp, "limit_sell", product_id)
        except Exception as e:
            logger.exception("Limit sell failed for %s", product_id)
            return OrderResult(False, None, f"Coinbase limit sell failed: {e}")

    # ── Order management ──────────────────────────────────────────

    def cancel_order(self, order_id: str) -> bool:
        """Cancel a pending order by its Coinbase order ID."""
        try:
            resp = self._client.cancel_orders(order_ids=[order_id])
            results = getattr(resp, "results", [])
            if results and getattr(results[0], "success", False):
                logger.info("Cancelled order %s", order_id)
                return True
            logger.warning("Cancel order %s returned: %s", order_id, resp)
            return False
        except Exception as e:
            logger.exception("Cancel order failed for %s", order_id)
            return False

    # ── Account info ──────────────────────────────────────────────

    def get_balances(self) -> dict[str, float]:
        """Fetch available balances from Coinbase. Returns {currency: amount}."""
        try:
            resp = self._client.get_accounts()
            balances: dict[str, float] = {}
            # Handle both object-style and dict-style responses
            accounts = getattr(resp, "accounts", None)
            if accounts is None and isinstance(resp, dict):
                accounts = resp.get("accounts", [])
            if accounts is None:
                logger.warning("Unexpected get_accounts response: %s", type(resp))
                return {}
            for account in accounts:
                if isinstance(account, dict):
                    currency = account.get("currency", "")
                    avail = account.get("available_balance", {})
                    available = float(avail.get("value", "0") if isinstance(avail, dict) else getattr(avail, "value", "0"))
                else:
                    currency = getattr(account, "currency", "")
                    available = float(getattr(account.available_balance, "value", "0"))
                if available > 0:
                    balances[currency] = balances.get(currency, 0.0) + available
            logger.info("Coinbase balances: %s", balances)
            return balances
        except Exception as e:
            logger.exception("Failed to fetch Coinbase balances: %s", e)
            return {}

    # ── Helpers ───────────────────────────────────────────────────

    def _parse_order_response(
        self, resp: object, action: str, product_id: str
    ) -> OrderResult:
        """Parse SDK order response into our OrderResult."""
        success_resp = getattr(resp, "success_response", None)
        error_resp = getattr(resp, "error_response", None)

        if success_resp:
            order_id = getattr(success_resp, "order_id", None)
            logger.info(
                "Coinbase %s order placed: %s %s (order_id=%s)",
                action, product_id, action, order_id,
            )
            return OrderResult(
                success=True,
                order_id=order_id,
                message=f"Coinbase {action} order placed for {product_id} (order_id: {order_id})",
            )

        if error_resp:
            error_msg = getattr(error_resp, "message", str(error_resp))
            preview_failure = getattr(error_resp, "preview_failure_reason", "")
            full_msg = f"{error_msg} ({preview_failure})" if preview_failure else error_msg
            logger.warning("Coinbase %s order failed: %s", action, full_msg)
            return OrderResult(False, None, f"Coinbase {action} failed: {full_msg}")

        # Fallback for unexpected response shape
        logger.warning("Unexpected Coinbase response: %s", resp)
        return OrderResult(
            success=bool(getattr(resp, "success", False)),
            order_id=getattr(resp, "order_id", None),
            message=str(resp),
        )
