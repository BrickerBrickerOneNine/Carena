"""
Coinbase Exchange WebSocket consumer that maintains an up-to-date price book.

Subscribes to ticker_batch (~5s updates) for a set of products and keeps
a local dictionary of the latest bid/ask/price for each.

Also provides a REST-polling alternative (``poll_rest``) that fetches
1-minute OHLCV candles + current prices from the Coinbase REST API.

Usage:
    uv run python coinbase_consumer.py
"""

import asyncio
import io
import json
import logging
import math
import signal
import sys
from dataclasses import dataclass
from datetime import datetime, timezone

import httpx
import websockets

logger = logging.getLogger(__name__)

COINBASE_WS_URL = "wss://ws-feed.exchange.coinbase.com"
COINBASE_REST_BASE = "https://api.exchange.coinbase.com"

DEFAULT_PRODUCTS = [
    "BTC-USD",
    "ETH-USD",
    "SOL-USD",
    "LTC-USD",
    "DOGE-USD",
    "LINK-USD",
    "XRP-USD",
]


def _fmt_price(value: float) -> str:
    """Format a price with adaptive precision — works for $60k BTC and $0.07 SOL."""
    if value == 0:
        return "0"
    # Use enough decimal places so at least 4 significant figures show
    decimals = max(2, 4 - int(math.floor(math.log10(abs(value)))) - 1)
    return f"{value:.{decimals}f}"


class PriceBook:
    """Maintains the latest price snapshot for each subscribed product."""

    def __init__(self):
        self._book: dict[str, dict] = {}

    def update(self, data: dict) -> None:
        self._book[data["product_id"]] = {
            "price": data["price"],
            "best_bid": data["best_bid"],
            "best_bid_size": data["best_bid_size"],
            "best_ask": data["best_ask"],
            "best_ask_size": data["best_ask_size"],
            "side": data["side"],
            "last_size": data["last_size"],
            "volume_24h": data["volume_24h"],
            "time": data["time"],
        }

    def get(self, product_id: str) -> dict | None:
        return self._book.get(product_id)

    def snapshot(self) -> dict[str, dict]:
        return dict(self._book)

    def display(self) -> None:
        if not self._book:
            return

        now = datetime.now(timezone.utc).strftime("%H:%M:%S")
        print(f"\n{'=' * 78}")
        print(f"  Price Book @ {now} UTC")
        print(f"{'=' * 78}")
        print(
            f"  {'Product':<14} {'Price':>12} {'Bid':>12} {'Ask':>12}"
            f" {'Spread':>10} {'Vol 24h':>14}"
        )
        print(f"  {'-' * 74}")

        for product_id in DEFAULT_PRODUCTS:
            entry = self._book.get(product_id)
            if entry is None:
                print(f"  {product_id:<14} {'--':>12}")
                continue

            bid = float(entry["best_bid"])
            ask = float(entry["best_ask"])
            spread = ask - bid

            print(
                f"  {product_id:<14}"
                f" {entry['price']:>12}"
                f" {entry['best_bid']:>12}"
                f" {entry['best_ask']:>12}"
                f" {spread:>10.6f}"
                f" {float(entry['volume_24h']):>14,.2f}"
            )

        print(f"{'=' * 78}")


@dataclass
class Candle:
    time: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float


@dataclass
class Timeframe:
    """Defines a candle granularity and the time window it covers."""

    granularity: int  # seconds (Coinbase API param: 60, 300, 900)
    start_minutes_ago: int  # beginning of window (farther from now)
    end_minutes_ago: int  # end of window (closer to now)
    label: str  # human-readable label for the agent prompt


TIMEFRAMES = [
    Timeframe(86400, 43200, 0, "1-day candles (last 30 days)"),
    Timeframe(21600, 10080, 0, "6-hour candles (last 7 days)"),
    Timeframe(3600, 2880, 0, "1-hour candles (last 2 days)"),
    Timeframe(900, 360, 0, "15-min candles (last 6 hours)"),
    Timeframe(300, 120, 0, "5-min candles (last 2 hours)"),
    Timeframe(60, 60, 0, "1-min candles (last 60 minutes)"),
]


class CandleBook:
    """Maintains multi-timeframe OHLCV candles
    per product from the Coinbase REST API."""

    def __init__(self) -> None:
        # Keyed by (product_id, granularity_seconds)
        self._candles: dict[tuple[str, int], list[Candle]] = {}

    def update_from_api(self, product_id: str, granularity: int, raw_candles: list[list]) -> None:
        """Parse Coinbase REST candle response and replace stored candles.

        Coinbase returns arrays of
        ``[timestamp, low, high, open, close, volume]``
        in *descending* time order.
        """
        candles = [
            Candle(
                time=datetime.fromtimestamp(row[0], tz=timezone.utc),
                open=float(row[3]),
                high=float(row[2]),
                low=float(row[1]),
                close=float(row[4]),
                volume=float(row[5]),
            )
            for row in raw_candles
        ]
        candles.sort(key=lambda c: c.time)
        self._candles[(product_id, granularity)] = candles

    def format_prompt(
        self,
        product_ids: list[str],
        ticker_data: dict[str, dict] | None = None,
    ) -> str:
        """Build a structured, multi-timeframe price history + indicators for the agent prompt."""
        from indicators import compute_indicators_summary

        buf = io.StringIO()
        for tf in TIMEFRAMES:
            buf.write(f"### {tf.label}\n")
            buf.write("product,time,open,high,low,close,volume\n")
            for pid in product_ids:
                for c in self._candles.get((pid, tf.granularity), []):
                    buf.write(
                        f"{pid},{c.time.strftime('%Y-%m-%dT%H:%M:%SZ')},"
                        f"{_fmt_price(c.open)},{_fmt_price(c.high)},{_fmt_price(c.low)},"
                        f"{_fmt_price(c.close)},{c.volume:.2f}\n"
                    )
            buf.write("\n")

        # Technical indicators summary
        buf.write("### Technical Indicators Summary\n")
        buf.write("Pre-computed indicators from the candle data above. Use these to inform your decisions.\n\n")
        for pid in product_ids:
            candles_by_tf = {
                tf.granularity: self._candles.get((pid, tf.granularity), [])
                for tf in TIMEFRAMES
            }
            bid = 0.0
            ask = 0.0
            if ticker_data and pid in ticker_data:
                bid = float(ticker_data[pid].get("best_bid", 0))
                ask = float(ticker_data[pid].get("best_ask", 0))
            buf.write(compute_indicators_summary(candles_by_tf, bid, ask, pid))
            buf.write("\n\n")

        return buf.getvalue()

    def get_candles(self, product_id: str, granularity: int) -> list[Candle]:
        """Return stored candles for a product/granularity pair (empty list if none)."""
        return self._candles.get((product_id, granularity), [])

    def has_data(self) -> bool:
        return any(bool(v) for v in self._candles.values())


async def consume(price_book: PriceBook) -> None:
    while True:
        try:
            async with websockets.connect(COINBASE_WS_URL) as ws:
                await ws.send(
                    json.dumps(
                        {
                            "type": "subscribe",
                            "product_ids": DEFAULT_PRODUCTS,
                            "channels": ["ticker_batch"],
                        }
                    )
                )

                print(f"Connected. Subscribed to {len(DEFAULT_PRODUCTS)} products.")
                print("Waiting for data (updates every ~5s)...\n")

                async for raw in ws:
                    data = json.loads(raw)

                    if data.get("type") == "ticker":
                        price_book.update(data)
                        price_book.display()

        except websockets.ConnectionClosed:
            print("\nConnection lost. Reconnecting in 3s...")
            await asyncio.sleep(3)
        except Exception as e:
            print(f"\nError: {e}. Reconnecting in 5s...")
            await asyncio.sleep(5)


async def _fetch_product_candles(
    client: httpx.AsyncClient,
    product_id: str,
    candle_book: CandleBook,
    now: int,
    semaphore: asyncio.Semaphore,
) -> None:
    """Fetch all timeframe candles for a single product with rate limiting."""
    for tf in TIMEFRAMES:
        start = now - tf.start_minutes_ago * 60
        end = now - tf.end_minutes_ago * 60
        async with semaphore:
            for attempt in range(3):
                resp = await client.get(
                    f"/products/{product_id}/candles",
                    params={
                        "granularity": tf.granularity,
                        "start": start,
                        "end": end,
                    },
                )
                if resp.status_code == 429:
                    wait = 1.0 * (attempt + 1)
                    logger.warning("Rate limited on %s, retrying in %.0fs...", product_id, wait)
                    await asyncio.sleep(wait)
                    continue
                resp.raise_for_status()
                candle_book.update_from_api(product_id, tf.granularity, resp.json())
                break
            await asyncio.sleep(0.15)  # pace requests to stay under rate limit


async def poll_rest(
    products: list[str],
    price_book: PriceBook | None,
    candle_book: CandleBook,
    interval: float = 60.0,
) -> None:
    """Poll Coinbase REST API for multi-timeframe candles and (optionally) current prices.

    Runs indefinitely, fetching candles at each configured timeframe
    every ``interval`` seconds.  When *price_book* is provided, also
    fetches the latest ticker for each product; pass ``None`` to skip
    ticker fetches (e.g. when live prices come from a WebSocket).
    """
    async with httpx.AsyncClient(base_url=COINBASE_REST_BASE, timeout=15.0) as client:
        # Limit concurrent requests to avoid Coinbase 429 rate limits
        semaphore = asyncio.Semaphore(3)
        while True:
            now = int(datetime.now(timezone.utc).timestamp())

            # Fetch candles for all products with rate limiting
            candle_tasks = [
                _fetch_product_candles(client, pid, candle_book, now, semaphore)
                for pid in products
            ]
            results = await asyncio.gather(*candle_tasks, return_exceptions=True)
            for pid, result in zip(products, results):
                if isinstance(result, Exception):
                    logger.exception("REST candle poll failed for %s", pid, exc_info=result)

            # Optionally fetch current tickers
            if price_book is not None:
                for product_id in products:
                    try:
                        await asyncio.sleep(0.15)
                        resp = await client.get(f"/products/{product_id}/ticker")
                        resp.raise_for_status()
                        ticker = resp.json()
                        price_book.update(
                            {
                                "product_id": product_id,
                                "price": ticker.get("price", "0"),
                                "best_bid": ticker.get("bid", "0"),
                                "best_bid_size": ticker.get("bid_size", "0"),
                                "best_ask": ticker.get("ask", "0"),
                                "best_ask_size": ticker.get("ask_size", "0"),
                                "side": ticker.get("side", ""),
                                "last_size": ticker.get("size", "0"),
                                "volume_24h": ticker.get("volume", "0"),
                                "time": ticker.get("time", ""),
                            }
                        )
                    except Exception:
                        logger.exception("REST ticker poll failed for %s", product_id)

            await asyncio.sleep(interval)


def main() -> None:
    price_book = PriceBook()

    loop = asyncio.new_event_loop()

    def shutdown(sig, frame):
        print("\nShutting down...")
        loop.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    loop.run_until_complete(consume(price_book))


if __name__ == "__main__":
    main()
