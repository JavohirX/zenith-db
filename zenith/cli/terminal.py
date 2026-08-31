"""
ZenithDB Terminal UI & Formatting Engine
Zero-dependency replacement for Rich, Colorama, Tabulate, and Tqdm.
Cross-platform and safe across Windows cp1252 / UTF-8 terminals.
"""

import sys
import time
from typing import Any, List, Optional, Sequence

# Attempt to configure stdout to UTF-8 on Windows
if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass


class Colors:
    """ANSI color escape sequences."""
    RESET = "\033[0m"
    BOLD = "\033[1m"
    DIM = "\033[2m"
    ITALIC = "\033[3m"
    UNDERLINE = "\033[4m"

    BLACK = "\033[30m"
    RED = "\033[31m"
    GREEN = "\033[32m"
    YELLOW = "\033[33m"
    BLUE = "\033[34m"
    MAGENTA = "\033[35m"
    CYAN = "\033[36m"
    WHITE = "\033[37m"

    BRIGHT_BLACK = "\033[90m"
    BRIGHT_RED = "\033[91m"
    BRIGHT_GREEN = "\033[92m"
    BRIGHT_YELLOW = "\033[93m"
    BRIGHT_BLUE = "\033[94m"
    BRIGHT_MAGENTA = "\033[95m"
    BRIGHT_CYAN = "\033[96m"
    BRIGHT_WHITE = "\033[97m"

    @classmethod
    def colorize(cls, text: str, color_code: str) -> str:
        return f"{color_code}{text}{cls.RESET}"


def format_table(
    headers: List[str],
    rows: List[List[Any]],
    style: str = "ascii",  # Default ascii for universal compatibility
) -> str:
    """
    Renders a formatted table with automatic column sizing.
    """
    if not headers and not rows:
        return ""

    col_count = max(len(headers), max((len(r) for r in rows), default=0))
    padded_headers = headers + [""] * (col_count - len(headers))
    padded_rows = [
        [str(cell) for cell in r] + [""] * (col_count - len(r)) for r in rows
    ]

    widths = [len(h) for h in padded_headers]
    for r in padded_rows:
        for i, cell in enumerate(r):
            # Strip ANSI codes for length calculation
            clean_cell = cell
            for c in (
                Colors.RESET, Colors.BOLD, Colors.DIM, Colors.GREEN, Colors.RED,
                Colors.YELLOW, Colors.CYAN, Colors.BRIGHT_CYAN, Colors.BRIGHT_GREEN,
                Colors.BRIGHT_RED, Colors.BRIGHT_YELLOW,
            ):
                clean_cell = clean_cell.replace(c, "")
            widths[i] = max(widths[i], len(clean_cell))

    # Border characters
    tl, tr, bl, br = "+", "+", "+", "+"
    h_line, v_line = "-", "|"
    tm, bm, ml, mr, mm = "+", "+", "+", "+", "+"

    lines = []

    # Top border
    top = tl + tm.join(h_line * (w + 2) for w in widths) + tr
    lines.append(top)

    # Header
    if headers:
        hdr_cells = [
            f" {h.ljust(widths[i])} " for i, h in enumerate(padded_headers)
        ]
        hdr_line = v_line + v_line.join(hdr_cells) + v_line
        lines.append(hdr_line)

        # Header separator
        mid = ml + mm.join(h_line * (w + 2) for w in widths) + mr
        lines.append(mid)

    # Rows
    for r in padded_rows:
        row_cells = []
        for i, cell in enumerate(r):
            clean_cell = cell
            for c in (
                Colors.RESET, Colors.BOLD, Colors.DIM, Colors.GREEN, Colors.RED,
                Colors.YELLOW, Colors.CYAN, Colors.BRIGHT_CYAN, Colors.BRIGHT_GREEN,
                Colors.BRIGHT_RED, Colors.BRIGHT_YELLOW,
            ):
                clean_cell = clean_cell.replace(c, "")
            pad_len = widths[i] - len(clean_cell)
            row_cells.append(f" {cell}{' ' * pad_len} ")
        lines.append(v_line + v_line.join(row_cells) + v_line)

    # Bottom border
    bot = bl + bm.join(h_line * (w + 2) for w in widths) + br
    lines.append(bot)

    return "\n".join(lines)


class ProgressBar:
    """Zero-dependency dynamic terminal progress bar."""

    def __init__(
        self,
        total: int,
        description: str = "Progress",
        bar_length: int = 25,
    ) -> None:
        self.total = max(1, total)
        self.description = description
        self.bar_length = bar_length
        self.current = 0
        self.start_time = time.time()

    def update(self, count: int = 1) -> None:
        self.current += count
        self.render()

    def render(self) -> None:
        fraction = min(1.0, self.current / self.total)
        filled_len = int(self.bar_length * fraction)
        bar = "=" * filled_len + "-" * (self.bar_length - filled_len)

        elapsed = time.time() - self.start_time
        rate = self.current / elapsed if elapsed > 0 else 0
        percent = fraction * 100

        output = (
            f"\r{Colors.BRIGHT_CYAN}{self.description}{Colors.RESET} "
            f"[{Colors.BRIGHT_GREEN}{bar}{Colors.RESET}] "
            f"{percent:5.1f}% ({self.current}/{self.total}) "
            f"{rate:,.0f} ops/s"
        )
        try:
            sys.stdout.write(output)
            sys.stdout.flush()
        except Exception:
            pass

        if self.current >= self.total:
            try:
                sys.stdout.write("\n")
                sys.stdout.flush()
            except Exception:
                pass


def print_banner() -> None:
    banner = f"""{Colors.BRIGHT_CYAN}
  =============================================================
   ZENITH-DB : Zero-Dependency Multi-Model Storage Engine
  =============================================================
{Colors.RESET}  {Colors.BOLD}LSM-Tree KV · BM25 Search · Vector Index · RESP & REST{Colors.RESET}
  {Colors.GREEN}100% Python Standard Library | Zero Dependencies{Colors.RESET}
    """
    try:
        print(banner)
    except Exception:
        print("=== ZENITH-DB : Zero-Dependency Multi-Model Storage Engine ===")
