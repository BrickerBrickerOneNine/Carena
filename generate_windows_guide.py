"""Generate a single-page PDF setup guide for Windows users."""

from fpdf import FPDF


class Guide(FPDF):
    ACCENT = (41, 128, 185)
    DARK = (30, 30, 30)
    LIGHT_BG = (245, 247, 250)
    WHITE = (255, 255, 255)
    GREEN = (39, 174, 96)
    ORANGE = (230, 126, 34)
    RED = (231, 76, 60)

    def header(self):
        pass

    def footer(self):
        self.set_y(-10)
        self.set_font("Helvetica", "I", 7)
        self.set_text_color(160, 160, 160)
        self.cell(0, 5, "Crypto Daytrading Arena  |  Windows Setup Guide  |  March 2026", align="C")

    def section(self, number, title):
        self.set_font("Helvetica", "B", 10)
        self.set_fill_color(*self.ACCENT)
        self.set_text_color(*self.WHITE)
        self.cell(16, 6, f"  STEP {number}", fill=True)
        self.cell(2)
        self.set_text_color(*self.DARK)
        self.cell(0, 6, title, new_x="LMARGIN", new_y="NEXT")
        self.ln(1.5)

    def body(self, text):
        self.set_font("Helvetica", "", 8.5)
        self.set_text_color(60, 60, 60)
        self.multi_cell(0, 4.2, text)
        self.ln(1)

    def code(self, text):
        self.set_font("Courier", "", 8)
        self.set_fill_color(240, 240, 240)
        self.set_text_color(40, 40, 40)
        self.cell(4)
        self.multi_cell(0, 4.5, text, fill=True)
        self.ln(1.5)

    def bullet(self, text, indent=6):
        self.set_font("Helvetica", "", 8.5)
        self.set_text_color(60, 60, 60)
        self.cell(indent)
        self.cell(4, 4.2, "-")
        self.multi_cell(0, 4.2, text)
        self.ln(0.5)

    def note_box(self, text, color=None):
        if color is None:
            color = self.ORANGE
        x, y = self.get_x(), self.get_y()
        w = self.w - self.l_margin - self.r_margin
        self.set_fill_color(*color)
        self.rect(x, y, 2.5, 12, style="F")
        self.set_fill_color(255, 248, 240)
        self.rect(x + 2.5, y, w - 2.5, 12, style="F")
        self.set_xy(x + 6, y + 1.5)
        self.set_font("Helvetica", "B", 8)
        self.set_text_color(*color)
        self.multi_cell(w - 10, 4.2, text)
        self.set_xy(x, y + 14)


def build_guide():
    pdf = Guide("P", "mm", "A4")
    pdf.set_auto_page_break(auto=False)
    pdf.add_page()
    pw = pdf.w - pdf.l_margin - pdf.r_margin

    # ── Title banner ──────────────────────────────────────────
    pdf.set_fill_color(*Guide.ACCENT)
    pdf.rect(0, 0, pdf.w, 32, style="F")
    pdf.set_xy(pdf.l_margin, 6)
    pdf.set_font("Helvetica", "B", 20)
    pdf.set_text_color(*Guide.WHITE)
    pdf.cell(0, 10, "Crypto Daytrading Arena", new_x="LMARGIN", new_y="NEXT")
    pdf.set_font("Helvetica", "", 12)
    pdf.set_text_color(200, 220, 240)
    pdf.cell(0, 7, "Windows Setup Guide", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(10)

    # ── Prerequisites ─────────────────────────────────────────
    pdf.set_font("Helvetica", "B", 11)
    pdf.set_text_color(*Guide.DARK)
    pdf.cell(0, 6, "Prerequisites", new_x="LMARGIN", new_y="NEXT")
    pdf.set_draw_color(*Guide.ACCENT)
    pdf.set_line_width(0.4)
    pdf.line(pdf.l_margin, pdf.get_y(), pdf.l_margin + 40, pdf.get_y())
    pdf.ln(3)

    pdf.body(
        "Install these three tools before starting. All are free."
    )

    prereqs = [
        ("Python 3.10+", "https://python.org/downloads  --  Check \"Add Python to PATH\" during install"),
        ("uv (package manager)", "Open PowerShell, run:  powershell -ExecutionPolicy ByPass -c \"irm https://astral.sh/uv/install.ps1 | iex\""),
        ("Docker Desktop", "https://docker.com/products/docker-desktop  --  Enable WSL 2 backend when prompted"),
    ]
    for name, detail in prereqs:
        pdf.set_font("Helvetica", "B", 8.5)
        pdf.set_text_color(*Guide.DARK)
        pdf.cell(6)
        pdf.cell(40, 4.5, name)
        pdf.set_font("Helvetica", "", 8)
        pdf.set_text_color(100, 100, 100)
        pdf.cell(0, 4.5, detail, new_x="LMARGIN", new_y="NEXT")
        pdf.ln(0.5)

    pdf.ln(3)

    # ── Step 1: Extract ───────────────────────────────────────
    pdf.section("1", "Extract & Install")
    pdf.body("Extract crypto-daytrading-arena.zip to a folder (e.g. C:\\arena), open PowerShell, and install dependencies:")
    pdf.code("cd C:\\arena\nuv sync")

    # ── Step 2: Launch ────────────────────────────────────────
    pdf.section("2", "Launch the Arena")
    pdf.body("Run the interactive launcher. It starts Kafka automatically and walks you through configuration:")
    pdf.code("uv run python launcher.py")
    pdf.body("The wizard will ask you to configure:")
    pdf.bullet("Trading mode -- Simulated (paper trading) or Live (real Coinbase orders)")
    pdf.bullet("LLM provider -- Enter your OpenAI, Anthropic, or custom API key and model")
    pdf.bullet("Coins -- Select which cryptocurrencies to trade (BTC, ETH, SOL, LTC, DOGE, LINK, XRP)")
    pdf.bullet("Strategy -- contrarian (recommended), momentum, swing, or default")
    pdf.ln(0.5)
    pdf.body("Your configuration is saved to arena_config.json. Re-launch without the wizard:")
    pdf.code("uv run python launcher.py --config arena_config.json")

    # ── Step 3: Live Trading ──────────────────────────────────
    pdf.section("3", "Live Trading (Real Money)")
    pdf.body(
        "To trade with real money, select \"Live\" mode in Step 1 of the wizard and enter your Coinbase CDP credentials."
    )
    pdf.bullet("Go to https://portal.cdp.coinbase.com/access/api and create an API key with trade permissions")
    pdf.bullet("You will receive an API Key Name (organizations/...) and an API Secret (EC private key)")
    pdf.bullet("Enter these when prompted by the launcher wizard")
    pdf.ln(1)
    pdf.note_box("WARNING: Live mode places REAL orders on Coinbase with real money. Start with a small balance.", Guide.RED)
    pdf.ln(1)

    # ── Step 4: Stop ──────────────────────────────────────────
    pdf.section("4", "Stop the Arena")
    pdf.body("Press Ctrl+C in the launcher window. To fully stop Kafka:")
    pdf.code("uv run python launcher.py --teardown")

    # ── Optional: Build .exe ──────────────────────────────────
    pdf.ln(1)
    pdf.set_font("Helvetica", "B", 11)
    pdf.set_text_color(*Guide.DARK)
    pdf.cell(0, 6, "Optional: Build Standalone .exe", new_x="LMARGIN", new_y="NEXT")
    pdf.set_draw_color(*Guide.ACCENT)
    pdf.line(pdf.l_margin, pdf.get_y(), pdf.l_margin + 55, pdf.get_y())
    pdf.ln(3)

    pdf.body("Create a standalone executable that doesn't require Python or uv to be installed:")
    pdf.code("build_windows.bat")
    pdf.body("Output: dist\\arena.exe -- Run it the same way as the Python launcher. Docker is still required for Kafka.")

    # ── Troubleshooting ───────────────────────────────────────
    pdf.ln(1)
    pdf.set_font("Helvetica", "B", 11)
    pdf.set_text_color(*Guide.DARK)
    pdf.cell(0, 6, "Troubleshooting", new_x="LMARGIN", new_y="NEXT")
    pdf.set_draw_color(*Guide.ACCENT)
    pdf.line(pdf.l_margin, pdf.get_y(), pdf.l_margin + 35, pdf.get_y())
    pdf.ln(3)

    issues = [
        ("\"uv\" not recognized", "Close and reopen PowerShell after installing uv"),
        ("Docker not running", "Open Docker Desktop and wait for it to fully start"),
        ("Port 9092 in use", "Run: uv run python launcher.py --teardown"),
        ("Connection refused", "Wait 30 seconds after Kafka starts -- it needs time to initialize"),
    ]
    col1, col2 = 55, pw - 55
    pdf.set_font("Helvetica", "B", 8)
    pdf.set_fill_color(*Guide.LIGHT_BG)
    pdf.set_text_color(*Guide.DARK)
    pdf.cell(col1, 5, "  Problem", fill=True)
    pdf.cell(col2, 5, "  Solution", fill=True, new_x="LMARGIN", new_y="NEXT")
    for problem, solution in issues:
        pdf.set_font("Courier", "", 7.5)
        pdf.set_text_color(80, 80, 80)
        pdf.cell(col1, 4.5, f"  {problem}")
        pdf.set_font("Helvetica", "", 8)
        pdf.cell(col2, 4.5, f"  {solution}", new_x="LMARGIN", new_y="NEXT")

    return pdf


if __name__ == "__main__":
    pdf = build_guide()
    path = "windows_setup_guide.pdf"
    pdf.output(path)
    print(f"Guide saved to {path}")
