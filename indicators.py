"""Pure-Python technical indicators for the daytrading arena.

Computes SMA, EMA, RSI, MACD, Bollinger Bands, and momentum from
``Candle`` objects provided by :mod:`coinbase_consumer`.  No external
dependencies beyond the standard library.
"""

from __future__ import annotations

import math

from coinbase_consumer import Candle, _fmt_price


# ── Indicator functions ──────────────────────────────────────────


def sma(candles: list[Candle], period: int) -> float | None:
    """Simple Moving Average of close prices over the last *period* candles."""
    if len(candles) < period:
        return None
    return sum(c.close for c in candles[-period:]) / period


def ema(candles: list[Candle], period: int) -> float | None:
    """Exponential Moving Average of close prices."""
    if len(candles) < period:
        return None
    k = 2.0 / (period + 1)
    result = sum(c.close for c in candles[:period]) / period
    for c in candles[period:]:
        result = c.close * k + result * (1 - k)
    return result


def rsi(candles: list[Candle], period: int = 14) -> float | None:
    """Relative Strength Index (Wilder's smoothing). Returns 0-100."""
    if len(candles) < period + 1:
        return None
    changes = [candles[i].close - candles[i - 1].close for i in range(1, len(candles))]
    gains = [max(0.0, c) for c in changes]
    losses = [max(0.0, -c) for c in changes]
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    for i in range(period, len(changes)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


def macd(
    candles: list[Candle],
    fast: int = 12,
    slow: int = 26,
    signal_period: int = 9,
) -> tuple[float, float, float] | None:
    """MACD line, signal line, and histogram.

    Returns ``(macd_line, signal_line, histogram)`` or ``None`` if
    fewer than *slow* candles are available.
    """
    if len(candles) < slow:
        return None
    fast_ema = ema(candles, fast)
    slow_ema = ema(candles, slow)
    if fast_ema is None or slow_ema is None:
        return None
    macd_line = fast_ema - slow_ema

    # Build MACD series for the signal-line EMA
    macd_series: list[float] = []
    for end in range(slow, len(candles) + 1):
        subset = candles[:end]
        f = ema(subset, fast)
        s = ema(subset, slow)
        if f is not None and s is not None:
            macd_series.append(f - s)

    if len(macd_series) < signal_period:
        return (macd_line, macd_line, 0.0)

    k = 2.0 / (signal_period + 1)
    sig = sum(macd_series[:signal_period]) / signal_period
    for val in macd_series[signal_period:]:
        sig = val * k + sig * (1 - k)
    histogram = macd_line - sig
    return (macd_line, sig, histogram)


def bollinger_bands(
    candles: list[Candle],
    period: int = 20,
    num_std: float = 2.0,
) -> tuple[float, float, float] | None:
    """Bollinger Bands: ``(upper, middle, lower)``."""
    if len(candles) < period:
        return None
    closes = [c.close for c in candles[-period:]]
    middle = sum(closes) / period
    variance = sum((x - middle) ** 2 for x in closes) / period
    std_dev = math.sqrt(variance)
    return (middle + num_std * std_dev, middle, middle - num_std * std_dev)


def momentum_pct(candles: list[Candle], period: int) -> float | None:
    """Percentage change in close price over the last *period* candles."""
    if len(candles) < period + 1:
        return None
    old = candles[-(period + 1)].close
    if old == 0:
        return None
    return ((candles[-1].close - old) / old) * 100.0


def atr(candles: list[Candle], period: int = 14) -> float | None:
    """Average True Range over the last *period* candles. Returns the ATR in price units."""
    if len(candles) < period + 1:
        return None
    true_ranges = []
    for i in range(1, len(candles)):
        high_low = candles[i].high - candles[i].low
        high_prev_close = abs(candles[i].high - candles[i - 1].close)
        low_prev_close = abs(candles[i].low - candles[i - 1].close)
        true_ranges.append(max(high_low, high_prev_close, low_prev_close))
    # Wilder's smoothing
    atr_val = sum(true_ranges[:period]) / period
    for tr in true_ranges[period:]:
        atr_val = (atr_val * (period - 1) + tr) / period
    return atr_val


def spread_pct(bid: float, ask: float) -> float:
    """Spread as percentage of mid-price."""
    mid = (bid + ask) / 2
    if mid == 0:
        return 0.0
    return ((ask - bid) / mid) * 100.0


def vwap(candles: list[Candle]) -> float | None:
    """Volume-Weighted Average Price."""
    if not candles:
        return None
    total_vp = sum((c.high + c.low + c.close) / 3 * c.volume for c in candles)
    total_vol = sum(c.volume for c in candles)
    if total_vol == 0:
        return None
    return total_vp / total_vol


def obv(candles: list[Candle]) -> float | None:
    """On-Balance Volume (cumulative). Returns the final OBV value."""
    if len(candles) < 2:
        return None
    result = 0.0
    for i in range(1, len(candles)):
        if candles[i].close > candles[i - 1].close:
            result += candles[i].volume
        elif candles[i].close < candles[i - 1].close:
            result -= candles[i].volume
    return result


def obv_trend(candles: list[Candle], lookback: int = 5) -> str | None:
    """OBV trend direction over last *lookback* candles. Returns 'RISING', 'FALLING', or 'FLAT'."""
    if len(candles) < lookback + 2:
        return None
    recent_obv = obv(candles[-lookback:])
    older_obv = obv(candles[-(lookback * 2) : -lookback]) if len(candles) >= lookback * 2 + 2 else None
    if recent_obv is None or older_obv is None:
        return None
    diff = recent_obv - older_obv
    if abs(diff) < 1:  # near zero
        return "FLAT"
    return "RISING" if diff > 0 else "FALLING"


def rsi_divergence(candles: list[Candle], period: int = 14, lookback: int = 10) -> str | None:
    """Detect RSI/price divergence over recent *lookback* candles.

    Returns 'BULLISH_DIVERGENCE', 'BEARISH_DIVERGENCE', or None.
    Bullish: price makes lower low but RSI makes higher low.
    Bearish: price makes higher high but RSI makes lower high.
    """
    if len(candles) < period + lookback + 1:
        return None
    # Compute RSI for each of the last `lookback` candles
    rsi_vals = []
    for i in range(lookback):
        end = len(candles) - lookback + i + 1
        r = rsi(candles[:end], period)
        if r is None:
            return None
        rsi_vals.append(r)

    prices = [candles[-(lookback - i)].close for i in range(lookback)]

    mid = lookback // 2
    # Check for bullish divergence: price lower low, RSI higher low
    price_low_first = min(prices[:mid])
    price_low_second = min(prices[mid:])
    rsi_low_first = min(rsi_vals[:mid])
    rsi_low_second = min(rsi_vals[mid:])
    if price_low_second < price_low_first and rsi_low_second > rsi_low_first:
        return "BULLISH_DIVERGENCE"

    # Check for bearish divergence: price higher high, RSI lower high
    price_high_first = max(prices[:mid])
    price_high_second = max(prices[mid:])
    rsi_high_first = max(rsi_vals[:mid])
    rsi_high_second = max(rsi_vals[mid:])
    if price_high_second > price_high_first and rsi_high_second < rsi_high_first:
        return "BEARISH_DIVERGENCE"

    return None


def consecutive_red_candles(candles: list[Candle]) -> int:
    """Count consecutive red (close < open) candles from the most recent."""
    count = 0
    for c in reversed(candles):
        if c.close < c.open:
            count += 1
        else:
            break
    return count


# ── Summary formatter ────────────────────────────────────────────


def compute_indicators_summary(
    candles_by_tf: dict[int, list[Candle]],
    bid: float,
    ask: float,
    product_id: str,
) -> str:
    """Compute and format a technical-indicators summary for one product.

    Parameters
    ----------
    candles_by_tf:
        ``{granularity_seconds: [Candle, ...]}`` sorted by time.
    bid / ask:
        Current best bid/ask (for spread calculation).
    product_id:
        E.g. ``"BTC-USD"``.
    """
    lines: list[str] = [f"#### {product_id}"]

    # Spread
    sp = spread_pct(bid, ask)
    lines.append(f"Spread: {sp:.4f}% of mid-price")

    c1 = candles_by_tf.get(60, [])
    c5 = candles_by_tf.get(300, [])
    c15 = candles_by_tf.get(900, [])
    c1h = candles_by_tf.get(3600, [])
    c6h = candles_by_tf.get(21600, [])
    c1d = candles_by_tf.get(86400, [])

    # ── 1-min indicators (short-term, ~60 candles) ───────────────
    if c1:
        lines.append("1-min timeframe:")
        _append_common_indicators(lines, c1, sma_periods=(5, 20), rsi_period=14, mom_periods=(5, 10))
        bb = bollinger_bands(c1, 20)
        if bb is not None:
            upper, mid_bb, lower = bb
            price = c1[-1].close
            if price > upper:
                bb_pos = "ABOVE upper band (overbought)"
            elif price < lower:
                bb_pos = "BELOW lower band (oversold)"
            else:
                pct_b = (price - lower) / (upper - lower) * 100 if upper != lower else 50
                bb_pos = f"within bands ({pct_b:.0f}% from lower)"
            lines.append(
                f"  Bollinger(20,2): upper={_fmt_price(upper)} mid={_fmt_price(mid_bb)} "
                f"lower={_fmt_price(lower)} | {bb_pos}"
            )
        # Volume indicators
        vwap_val = vwap(c1)
        if vwap_val is not None:
            trend = "ABOVE" if c1[-1].close > vwap_val else "BELOW"
            lines.append(f"  VWAP: {_fmt_price(vwap_val)} (price {trend})")
        obv_dir = obv_trend(c1, 5)
        if obv_dir is not None:
            lines.append(f"  OBV trend: {obv_dir}")
        red_count = consecutive_red_candles(c1)
        if red_count >= 3:
            lines.append(f"  WARNING: {red_count} consecutive red candles (falling knife)")
        # Divergence
        div = rsi_divergence(c1, 14, 10)
        if div is not None:
            lines.append(f"  RSI Divergence: {div}")

    # ── 5-min indicators (medium-term, ~24 candles) ──────────────
    if c5:
        lines.append("5-min timeframe:")
        _append_common_indicators(lines, c5, sma_periods=(5,), rsi_period=14, mom_periods=(5,))
        ema12 = ema(c5, 12)
        if ema12 is not None:
            trend = "ABOVE" if c5[-1].close > ema12 else "BELOW"
            lines.append(f"  EMA(12): {_fmt_price(ema12)} (price {trend})")
        vwap_val = vwap(c5)
        if vwap_val is not None:
            trend = "ABOVE" if c5[-1].close > vwap_val else "BELOW"
            lines.append(f"  VWAP: {_fmt_price(vwap_val)} (price {trend})")
        obv_dir = obv_trend(c5, 5)
        if obv_dir is not None:
            lines.append(f"  OBV trend: {obv_dir}")
        div = rsi_divergence(c5, 14, 10)
        if div is not None:
            lines.append(f"  RSI Divergence: {div}")

    # ── 15-min indicators (longer-term, ~24 candles) ─────────────
    if c15:
        lines.append("15-min timeframe:")
        sma5_val = sma(c15, 5)
        if sma5_val is not None:
            trend = "ABOVE" if c15[-1].close > sma5_val else "BELOW"
            lines.append(f"  SMA(5): {_fmt_price(sma5_val)} (price {trend})")
        mom3 = momentum_pct(c15, 3)
        if mom3 is not None:
            lines.append(f"  Momentum(3): {mom3:+.3f}%")

    # ── 1-hour indicators (2 days, ~48 candles) ───────────────
    if c1h:
        lines.append("1-hour timeframe (2 days):")
        _append_common_indicators(lines, c1h, sma_periods=(12, 24), rsi_period=14, mom_periods=(6, 24))
        ema12 = ema(c1h, 12)
        if ema12 is not None:
            trend = "ABOVE" if c1h[-1].close > ema12 else "BELOW"
            lines.append(f"  EMA(12): {_fmt_price(ema12)} (price {trend})")
        vwap_val = vwap(c1h)
        if vwap_val is not None:
            trend = "ABOVE" if c1h[-1].close > vwap_val else "BELOW"
            lines.append(f"  VWAP: {_fmt_price(vwap_val)} (price {trend})")
        obv_dir = obv_trend(c1h, 5)
        if obv_dir is not None:
            lines.append(f"  OBV trend: {obv_dir}")
        div = rsi_divergence(c1h, 14, 10)
        if div is not None:
            lines.append(f"  RSI Divergence: {div}")
        macd_val = macd(c1h)
        if macd_val is not None:
            ml, sl, hist = macd_val
            signal = "BULLISH" if hist > 0 else "BEARISH"
            lines.append(f"  MACD: line={ml:.2f} signal={sl:.2f} hist={hist:.2f} ({signal})")
        atr_val = atr(c1h, 14)
        if atr_val is not None:
            atr_pct = atr_val / c1h[-1].close * 100 if c1h[-1].close > 0 else 0
            lines.append(f"  ATR(14): {_fmt_price(atr_val)} ({atr_pct:.2f}% of price)")

    # ── 6-hour indicators (7 days, ~28 candles) ──────────────
    if c6h:
        lines.append("6-hour timeframe (7 days):")
        _append_common_indicators(lines, c6h, sma_periods=(5, 14), rsi_period=14, mom_periods=(4, 7))
        vwap_val = vwap(c6h)
        if vwap_val is not None:
            trend = "ABOVE" if c6h[-1].close > vwap_val else "BELOW"
            lines.append(f"  VWAP: {_fmt_price(vwap_val)} (price {trend})")
        atr_val = atr(c6h, 14)
        if atr_val is not None:
            atr_pct = atr_val / c6h[-1].close * 100 if c6h[-1].close > 0 else 0
            lines.append(f"  ATR(14): {_fmt_price(atr_val)} ({atr_pct:.2f}% of price)")

    # ── 1-day indicators (30 days, ~30 candles) ──────────────
    if c1d:
        lines.append("1-day timeframe (30 days):")
        _append_common_indicators(lines, c1d, sma_periods=(7, 20), rsi_period=14, mom_periods=(7, 14))
        bb = bollinger_bands(c1d, 20)
        if bb is not None:
            upper, mid_bb, lower = bb
            price = c1d[-1].close
            if price > upper:
                bb_pos = "ABOVE upper band (overbought)"
            elif price < lower:
                bb_pos = "BELOW lower band (oversold)"
            else:
                pct_b = (price - lower) / (upper - lower) * 100 if upper != lower else 50
                bb_pos = f"within bands ({pct_b:.0f}% from lower)"
            lines.append(
                f"  Bollinger(20,2): upper={_fmt_price(upper)} mid={_fmt_price(mid_bb)} "
                f"lower={_fmt_price(lower)} | {bb_pos}"
            )
        macd_val = macd(c1d)
        if macd_val is not None:
            ml, sl, hist = macd_val
            signal = "BULLISH" if hist > 0 else "BEARISH"
            lines.append(f"  MACD: line={ml:.2f} signal={sl:.2f} hist={hist:.2f} ({signal})")
        atr_val = atr(c1d, 14)
        if atr_val is not None:
            atr_pct = atr_val / c1d[-1].close * 100 if c1d[-1].close > 0 else 0
            lines.append(f"  ATR(14): {_fmt_price(atr_val)} ({atr_pct:.2f}% of price)")

    return "\n".join(lines)


def _append_common_indicators(
    lines: list[str],
    candles: list[Candle],
    *,
    sma_periods: tuple[int, ...],
    rsi_period: int,
    mom_periods: tuple[int, ...],
) -> None:
    """Append SMA, RSI, and momentum lines for a single timeframe."""
    sma_vals: dict[int, float] = {}
    for p in sma_periods:
        val = sma(candles, p)
        if val is not None:
            sma_vals[p] = val
            trend = "ABOVE" if candles[-1].close > val else "BELOW"
            lines.append(f"  SMA({p}): {_fmt_price(val)} (price {trend})")

    # SMA cross signal (if we have two SMA periods)
    if len(sma_periods) >= 2 and sma_periods[0] in sma_vals and sma_periods[1] in sma_vals:
        short_sma, long_sma = sma_vals[sma_periods[0]], sma_vals[sma_periods[1]]
        if short_sma > long_sma:
            lines.append(f"  SMA cross: BULLISH (SMA{sma_periods[0]} > SMA{sma_periods[1]})")
        else:
            lines.append(f"  SMA cross: BEARISH (SMA{sma_periods[0]} < SMA{sma_periods[1]})")

    rsi_val = rsi(candles, rsi_period)
    if rsi_val is not None:
        label = "OVERBOUGHT" if rsi_val > 70 else ("OVERSOLD" if rsi_val < 30 else "NEUTRAL")
        lines.append(f"  RSI({rsi_period}): {rsi_val:.1f} ({label})")

    for p in mom_periods:
        mom = momentum_pct(candles, p)
        if mom is not None:
            lines.append(f"  Momentum({p}): {mom:+.3f}%")
