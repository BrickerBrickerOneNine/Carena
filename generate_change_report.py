"""Generate a PDF change report for the Crypto Daytrading Arena improvements."""

from fpdf import FPDF


class Report(FPDF):
    ACCENT = (41, 128, 185)  # steel blue
    DARK = (30, 30, 30)
    LIGHT_BG = (245, 247, 250)
    WHITE = (255, 255, 255)
    GREEN = (39, 174, 96)
    RED = (231, 76, 60)

    def header(self):
        if self.page_no() > 1:
            self.set_font("Helvetica", "I", 8)
            self.set_text_color(*self.ACCENT)
            self.cell(0, 8, "Crypto Daytrading Arena  |  Profitability Improvements Change Report", align="R")
            self.ln(12)

    def footer(self):
        self.set_y(-15)
        self.set_font("Helvetica", "I", 8)
        self.set_text_color(150, 150, 150)
        self.cell(0, 10, f"Page {self.page_no()}/{{nb}}", align="C")

    def section_title(self, title):
        self.set_font("Helvetica", "B", 14)
        self.set_text_color(*self.ACCENT)
        self.cell(0, 10, title, new_x="LMARGIN", new_y="NEXT")
        self.set_draw_color(*self.ACCENT)
        self.set_line_width(0.6)
        self.line(self.l_margin, self.get_y(), self.w - self.r_margin, self.get_y())
        self.ln(4)

    def subsection(self, title):
        self.set_font("Helvetica", "B", 11)
        self.set_text_color(*self.DARK)
        self.cell(0, 8, title, new_x="LMARGIN", new_y="NEXT")
        self.ln(1)

    def body_text(self, text):
        self.set_font("Helvetica", "", 10)
        self.set_text_color(50, 50, 50)
        self.multi_cell(0, 5.5, text)
        self.ln(2)

    def bullet(self, text, indent=10):
        x = self.get_x()
        self.set_font("Helvetica", "", 10)
        self.set_text_color(50, 50, 50)
        self.cell(indent)
        self.set_font("Helvetica", "B", 10)
        self.cell(5, 5.5, "-")
        self.set_font("Helvetica", "", 10)
        self.multi_cell(0, 5.5, f"  {text}")
        self.ln(1)

    def file_badge(self, filename):
        self.set_font("Courier", "", 9)
        self.set_fill_color(230, 236, 244)
        self.set_text_color(*self.ACCENT)
        w = self.get_string_width(filename) + 8
        self.cell(w, 6, filename, fill=True, new_x="END")
        self.cell(3)

    def stat_box(self, label, value, color):
        x, y = self.get_x(), self.get_y()
        box_w = 55
        self.set_fill_color(*color)
        self.rect(x, y, box_w, 22, style="F")
        self.set_xy(x + 3, y + 3)
        self.set_font("Helvetica", "", 8)
        self.set_text_color(*self.WHITE)
        self.cell(box_w - 6, 5, label)
        self.set_xy(x + 3, y + 10)
        self.set_font("Helvetica", "B", 14)
        self.cell(box_w - 6, 9, value)
        self.set_xy(x + box_w + 5, y)

    def table_row(self, cols, widths, bold=False, fill=False):
        style = "B" if bold else ""
        self.set_font("Helvetica", style, 9)
        if fill:
            self.set_fill_color(*self.LIGHT_BG)
        self.set_text_color(*self.DARK)
        h = 7
        for i, (col, w) in enumerate(zip(cols, widths)):
            self.cell(w, h, col, border=0, fill=fill)
        self.ln(h)


def build_report():
    pdf = Report()
    pdf.alias_nb_pages()
    pdf.set_auto_page_break(auto=True, margin=20)
    pdf.add_page()

    # ── Title page ──────────────────────────────────────────────
    pdf.ln(25)
    pdf.set_font("Helvetica", "B", 28)
    pdf.set_text_color(*Report.ACCENT)
    pdf.cell(0, 14, "Crypto Daytrading Arena", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.set_font("Helvetica", "", 16)
    pdf.set_text_color(100, 100, 100)
    pdf.cell(0, 10, "Profitability Improvements  -  Change Report", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(6)
    pdf.set_draw_color(*Report.ACCENT)
    pdf.set_line_width(1)
    mid = pdf.w / 2
    pdf.line(mid - 40, pdf.get_y(), mid + 40, pdf.get_y())
    pdf.ln(10)

    pdf.set_font("Helvetica", "", 11)
    pdf.set_text_color(80, 80, 80)
    pdf.cell(0, 7, "March 2026", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(20)

    # Stats row
    x_start = 22
    pdf.set_xy(x_start, pdf.get_y())
    pdf.stat_box("Files Modified", "6", Report.ACCENT)
    pdf.stat_box("New Indicators", "5", Report.GREEN)
    pdf.stat_box("New Tools", "2", (142, 68, 173))
    pdf.ln(30)

    # Summary
    pdf.set_font("Helvetica", "", 11)
    pdf.set_text_color(60, 60, 60)
    pdf.multi_cell(0, 6,
        "This report summarizes seven high-impact improvements to the multi-agent crypto "
        "daytrading arena. The changes address the biggest profitability leaks: overtrading "
        "(fee bleed), limited technical analysis, insufficient historical data, and agents' "
        "lack of self-awareness about their own performance. Together, these changes give "
        "agents better data, smarter guardrails, and more order types to execute on."
    )

    # ── Page 2+: Changes ────────────────────────────────────────
    pdf.add_page()

    # 1. New Indicators
    pdf.section_title("1.  New Technical Indicators")
    pdf.file_badge("indicators.py")
    pdf.ln(8)
    pdf.body_text(
        "Added five new indicator functions and wired them into the summary that agents "
        "receive with every market data update. Volume-based indicators are the single best "
        "confirmation signal for crypto moves - they help agents distinguish real breakouts "
        "from fakeouts."
    )
    pdf.subsection("VWAP (Volume-Weighted Average Price)")
    pdf.body_text(
        "Calculates the average price weighted by volume across all candles in the window. "
        "When price is above VWAP, buying pressure dominates; below VWAP indicates selling "
        "pressure. Computed for both 1-min and 5-min timeframes."
    )
    pdf.subsection("OBV & OBV Trend (On-Balance Volume)")
    pdf.body_text(
        "Cumulative volume indicator: adds volume on up-candles, subtracts on down-candles. "
        "The trend function compares recent vs. older OBV to determine if volume is RISING, "
        "FALLING, or FLAT - confirming or contradicting price direction."
    )
    pdf.subsection("RSI Divergence Detection")
    pdf.body_text(
        "Detects bullish divergence (price makes lower low while RSI makes higher low) and "
        "bearish divergence (price makes higher high while RSI makes lower high). One of the "
        "most reliable reversal signals in crypto trading."
    )
    pdf.subsection("Consecutive Red Candle Counter")
    pdf.body_text(
        "Counts consecutive red (close < open) candles from the most recent. Triggers a "
        "\"falling knife\" warning at 3+ candles, directly feeding into the anti-pattern rules."
    )

    # 2. Expanded Candle Windows
    pdf.add_page()
    pdf.section_title("2.  Expanded Candle Data Windows")
    pdf.file_badge("coinbase_consumer.py")
    pdf.ln(8)
    pdf.body_text(
        "The previous candle windows were too narrow for reliable indicator computation. "
        "RSI(14) on 20 candles is barely statistically meaningful. The expanded windows give "
        "indicators much more data to work with, producing more reliable signals."
    )

    widths = [50, 50, 50, 30]
    pdf.table_row(["Timeframe", "Before", "After", "Gain"], widths, bold=True, fill=True)
    pdf.table_row(["1-minute", "20 candles (20 min)", "60 candles (1 hour)", "3x"], widths)
    pdf.table_row(["5-minute", "14 candles (with gap)", "24 candles (2 hours)", "1.7x"], widths, fill=True)
    pdf.table_row(["15-minute", "6 candles (with gap)", "24 candles (6 hours)", "4x"], widths)
    pdf.ln(4)
    pdf.body_text(
        "Gaps between timeframes have also been eliminated. Previously, the 5-min window "
        "started at 90 minutes ago and ended at 20 minutes ago, leaving a blind spot. "
        "All windows now extend to the present (end_minutes_ago = 0)."
    )

    # 3. Trade Guardrails
    pdf.section_title("3.  Trade Frequency Guardrails")
    pdf.file_badge("trading_tools.py")
    pdf.ln(8)
    pdf.body_text(
        "Hard-coded guardrails in the trading engine prevent agents from overtrading. At 0.2% "
        "round-trip cost, an agent making 50 trades/day bleeds ~10% in fees alone. These "
        "guardrails are enforced at the engine level - agents cannot override them."
    )
    pdf.subsection("Maximum 10 trades per hour")
    pdf.body_text(
        "A rolling 1-hour window tracks trade timestamps per agent. Trades beyond the limit "
        "are rejected with a clear error message. This prevents fee bleed from hyperactive agents."
    )
    pdf.subsection("2-minute minimum hold time")
    pdf.body_text(
        "Sell orders are rejected if the position was bought less than 120 seconds ago. "
        "The only exception is a stop-loss override: if the position is down more than 1.5% "
        "from entry, the sell is allowed regardless of hold time."
    )

    # 4. Performance Tracking
    pdf.add_page()
    pdf.section_title("4.  Performance Tracking in Portfolio")
    pdf.file_badge("trading_tools.py")
    pdf.ln(8)
    pdf.body_text(
        "Agents previously had no awareness of their own track record. Now the portfolio view "
        "includes comprehensive performance statistics, enabling agents to self-regulate their "
        "risk based on actual results."
    )
    pdf.subsection("New metrics tracked per agent:")
    pdf.bullet("Win rate - percentage of profitable round-trip trades (wins / total sells)")
    pdf.bullet("Average P&L per trade - mean realized profit/loss across all closed positions")
    pdf.bullet("Total realized P&L - cumulative net profit from all completed trades")
    pdf.bullet("Max drawdown - largest peak-to-trough decline as a percentage")
    pdf.bullet("Consecutive losses - current streak of losing trades (triggers risk reduction)")
    pdf.bullet("Trades used this hour - shows proximity to the 10/hour rate limit")
    pdf.ln(2)
    pdf.body_text(
        "The anti-pattern rules reference these stats directly: \"If your win rate is below 40%, "
        "become more selective\" and \"After 2 consecutive losses, reduce position size by 50%.\""
    )

    # 5. Limit Orders
    pdf.section_title("5.  Limit Order Support")
    pdf.file_badge("trading_tools.py")
    pdf.cell(3)
    pdf.file_badge("tools_and_dashboard.py")
    pdf.cell(3)
    pdf.file_badge("deploy_router_node.py")
    pdf.ln(8)
    pdf.body_text(
        "Previously, agents could only execute market orders - always buying at the ask and "
        "selling at the bid, guaranteeing spread loss on every trade. Limit orders let agents "
        "set entries at support levels and exits at resistance levels."
    )
    pdf.subsection("New tools available to agents:")
    pdf.bullet("place_limit_order(product_id, quantity, action, limit_price) - queues an order "
               "that fills automatically when price reaches the target")
    pdf.bullet("cancel_limit_order(order_id) - removes a pending order from the queue")
    pdf.ln(2)
    pdf.body_text(
        "Limit orders are checked against live prices on every price update via the Kafka "
        "subscriber. Buy limits fill when the ask drops to the limit price; sell limits fill "
        "when the bid rises to the limit price. Pending orders are shown in the portfolio view."
    )

    # 6. Anti-Pattern Rules
    pdf.add_page()
    pdf.section_title("6.  Anti-Pattern Rules in Strategy Prompts")
    pdf.file_badge("deploy_router_node.py")
    pdf.ln(8)
    pdf.body_text(
        "A shared anti-pattern block is now appended to all four strategy system prompts "
        "(default, momentum, contrarian, swing). These rules tell agents what NOT to do - "
        "which is often more valuable than entry signals."
    )
    pdf.subsection("Rules enforced via prompt:")
    pdf.bullet("No buying falling knives - wait for a green candle after 3+ consecutive red candles")
    pdf.bullet("No selling dips within uptrends - don't sell if 5-min SMA(5) hasn't been broken")
    pdf.bullet("No trading when spread > 0.5% - transaction costs would eat any edge")
    pdf.bullet("Reduce position size by 50% after 2 consecutive losses")
    pdf.bullet("No chasing moves > 1% in 5 minutes - wait for pullback")
    pdf.bullet("Self-regulate based on win rate - become more selective below 40%")
    pdf.bullet("Prefer limit orders over market orders for entries at key technical levels")

    # 7. Event-Driven Invocation
    pdf.ln(4)
    pdf.section_title("7.  Event-Driven Agent Invocation")
    pdf.file_badge("coinbase_kafka_connector.py")
    pdf.ln(8)
    pdf.body_text(
        "Agents previously received data only on a fixed interval (typically 60 seconds). "
        "Fast moves - breakouts and crashes - could be completely missed between intervals."
    )
    pdf.body_text(
        "Now, when any product's price moves more than 0.5% since the last published update, "
        "the connector immediately invokes agents with fresh data. This ensures agents can "
        "react to significant moves in near real-time while still avoiding noise from small "
        "price fluctuations."
    )

    # Files summary
    pdf.add_page()
    pdf.section_title("Files Modified")
    pdf.ln(2)
    files = [
        ("indicators.py", "New VWAP, OBV, RSI divergence, red candle counter; wired into summary"),
        ("coinbase_consumer.py", "Expanded TIMEFRAMES: 1-min (60 candles), 5-min (24), 15-min (24)"),
        ("trading_tools.py", "Guardrails, performance tracking, limit order engine + tools"),
        ("deploy_router_node.py", "Anti-pattern rules, limit order mentions, new indicator references"),
        ("tools_and_dashboard.py", "Registered place_limit_order & cancel_limit_order; order fill on price update"),
        ("coinbase_kafka_connector.py", "Event-driven invocation on 0.5% price moves"),
    ]
    widths2 = [60, 120]
    pdf.table_row(["File", "Changes"], widths2, bold=True, fill=True)
    for i, (f, desc) in enumerate(files):
        pdf.table_row([f, desc], widths2, fill=(i % 2 == 1))

    pdf.ln(10)
    pdf.section_title("Impact Summary")
    pdf.ln(2)
    pdf.body_text(
        "These seven changes work together as a system. Better indicators give agents more "
        "signal. Expanded data windows make those signals statistically reliable. Guardrails "
        "prevent the most common profitability killers (overtrading, panic selling). Performance "
        "tracking creates a feedback loop for self-regulation. Limit orders reduce spread costs. "
        "Anti-pattern rules encode hard-won trading wisdom. And event-driven invocation ensures "
        "agents don't miss the moves that matter most."
    )

    return pdf


if __name__ == "__main__":
    pdf = build_report()
    path = "crypto_daytrading_arena_change_report.pdf"
    pdf.output(path)
    print(f"Report saved to {path}")
