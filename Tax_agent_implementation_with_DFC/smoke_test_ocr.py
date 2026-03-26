"""
OCR Smoke Test — standalone, no MCP server needed.

Steps this script runs:
  1. Generate three synthetic receipt images using Pillow
  2. Run _parse_and_store_receipt() on each via an in-memory DuckDB
  3. Print results and assert key fields are correct
"""

import sys
import os
import re
import duckdb
from pathlib import Path

# ---------------------------------------------------------------------------
# 0.  Make sure we can import helpers from the server file without
#     starting the MCP server (FastMCP() runs at module level, so we
#     monkey-patch it before import).
# ---------------------------------------------------------------------------
from unittest.mock import MagicMock, patch

# Patch FastMCP so the module-level `mcp = FastMCP("TaxAgent")` is a no-op
# and the DB init runs against a temp file we control.
RECEIPTS_DIR = Path(__file__).parent / "test_receipts"
RECEIPTS_DIR.mkdir(exist_ok=True)

TEST_DB = str(Path(__file__).parent / "_smoke_test.duckdb")
if os.path.exists(TEST_DB):
    os.remove(TEST_DB)

# Patch DB_PATH before import
import importlib
import mcp_server_phase_18  # noqa: E402 — loaded after patches below

# We'll use a fresh in-memory connection rather than the module's CON
TEST_CON = duckdb.connect(TEST_DB)
TEST_CON.execute("""
    CREATE TABLE IF NOT EXISTS receipts (
        id       INTEGER PRIMARY KEY,
        vendor   VARCHAR,
        amount   DOUBLE,
        category VARCHAR,
        date     DATE
    )
""")

# Pull helpers directly from the server module
_parse_and_store_receipt = mcp_server_phase_18._parse_and_store_receipt
_normalise_text          = mcp_server_phase_18._normalise_text
_normalise_category      = mcp_server_phase_18._normalise_category
_parse_amount            = mcp_server_phase_18._parse_amount
_parse_date              = mcp_server_phase_18._parse_date


# ---------------------------------------------------------------------------
# 1.  Generate synthetic receipt images with Pillow
# ---------------------------------------------------------------------------

def make_receipt_image(
    path: str,
    vendor: str,
    items: list[tuple[str, str]],   # [(description, price_str)]
    total: str,
    date_str: str,
    address: str = "123 Main St, Springfield",
) -> None:
    """
    Draw a clean, high-resolution receipt image optimised for EasyOCR.

    Design choices to minimise OCR errors:
    - 800 px wide, large fonts (28 / 22 px)
    - TOTAL label and amount on the SAME left-to-right line, well inside margins
    - Generous line spacing so OCR doesn't merge adjacent rows
    - Arial preferred (cleaner letterforms); falls back to Courier then default
    """
    try:
        from PIL import Image, ImageDraw, ImageFont
    except ImportError:
        sys.exit("Pillow not found. Run: pip install pillow")

    W   = 800
    PAD = 40          # left/right margin
    LG  = 28          # font size — vendor / total
    SM  = 22          # font size — body text
    LINE_SM = SM + 14
    LINE_LG = LG + 14

    # Font resolution: prefer Arial (clean sans-serif), then Courier, then default
    def _font(size):
        for name in ("arial.ttf", "Arial.ttf", "DejaVuSans.ttf", "cour.ttf"):
            try:
                return ImageFont.truetype(name, size)
            except OSError:
                continue
        return ImageFont.load_default()

    font_lg = _font(LG)
    font_sm = _font(SM)

    rows = 6 + len(items) + 3   # header + items + total block + footer
    H    = rows * LINE_SM + 80
    img  = Image.new("RGB", (W, H), "white")
    draw = ImageDraw.Draw(img)

    def text_centered(y, s, font):
        bx = draw.textbbox((0, 0), s, font=font)
        w  = bx[2] - bx[0]
        draw.text(((W - w) // 2, y), s, fill="black", font=font)
        return y + (bx[3] - bx[1]) + 14

    def text_row(y, left, right, font):
        """Draw left-aligned label and right-aligned value on the same row."""
        draw.text((PAD, y), left, fill="black", font=font)
        bx = draw.textbbox((0, 0), right, font=font)
        draw.text((W - PAD - (bx[2] - bx[0]), y), right, fill="black", font=font)
        return y + (bx[3] - bx[1]) + 14

    def divider(y):
        draw.line([(PAD, y + 4), (W - PAD, y + 4)], fill="black", width=2)
        return y + 16

    y = 20
    y = text_centered(y, vendor, font_lg)
    y = text_centered(y, address, font_sm)
    y = text_centered(y, f"Date: {date_str}", font_sm)
    y = divider(y)

    for desc, price in items:
        y = text_row(y, desc, price, font_sm)

    y = divider(y)

    # TOTAL — render as one centered string so OCR reads it as a single token.
    # Right-aligning the amount at the margin causes '$' to be mis-read as '5'.
    y = text_centered(y, f"TOTAL: {total}", font_lg)

    y += 10
    y = divider(y)
    y = text_centered(y, "Thank you for your business!", font_sm)

    img.save(path, dpi=(200, 200))
    print(f"[gen] saved {path}")


RECEIPTS = [
    {
        "file":    "meal_receipt.png",
        "vendor":  "THE FAKE RESTAURANT",
        "address": "99 Broadway, New York NY",
        "date":    "03/15/2026",
        "items":   [("Client Lunch x2", "$62.00"), ("Tax (8%)", "$4.96"), ("Tip", "$7.29")],
        "total":   "$74.25",
        "expect":  {"category": "meal", "amount": 74.25},
    },
    {
        "file":    "transport_receipt.png",
        "vendor":  "Uber Technologies",
        "address": "1455 Market St, San Francisco CA",
        "date":    "March 20, 2026",
        "items":   [("Trip fare", "$14.00"), ("Service fee", "$2.50"), ("Tax", "$2.00")],
        "total":   "$18.50",
        "expect":  {"category": "transport", "amount": 18.50},
    },
    {
        "file":    "supplies_receipt.png",
        "vendor":  "Office Depot",
        "address": "500 Office Park Blvd, Chicago IL",
        "date":    "2026-03-22",
        "items":   [("Printer paper (ream)", "$12.99"), ("Ink cartridge", "$24.99"),
                    ("Pens (box)", "$5.02")],
        "total":   "$43.00",
        "expect":  {"category": "supplies", "amount": 43.00},
    },
]


# ---------------------------------------------------------------------------
# 2.  Run OCR and assert results
# ---------------------------------------------------------------------------

def run_smoke_test():
    print("\n" + "=" * 60)
    print("  OCR SMOKE TEST")
    print("=" * 60)

    # Generate images
    for r in RECEIPTS:
        make_receipt_image(
            path=str(RECEIPTS_DIR / r["file"]),
            vendor=r["vendor"],
            items=r["items"],
            total=r["total"],
            date_str=r["date"],
            address=r["address"],
        )

    print()

    passed = 0
    failed = 0

    for r in RECEIPTS:
        img_path = str(RECEIPTS_DIR / r["file"])
        print(f"--- {r['file']} ---")

        try:
            result = _parse_and_store_receipt(img_path, TEST_CON)
        except Exception as exc:
            print(f"  ERROR: {exc}")
            failed += 1
            continue

        ok = True

        # --- category check ---
        exp_cat = r["expect"]["category"]
        got_cat = result["category"]
        cat_ok  = got_cat == exp_cat
        mark    = "✓" if cat_ok else "✗"
        print(f"  {mark} category : expected={exp_cat!r:12s}  got={got_cat!r}")
        if not cat_ok:
            ok = False

        # --- amount check (within $1 tolerance for OCR variance) ---
        exp_amt = r["expect"]["amount"]
        got_amt = result["amount"]
        amt_ok  = abs(got_amt - exp_amt) <= 1.00
        mark    = "✓" if amt_ok else "✗"
        print(f"  {mark} amount   : expected={exp_amt:<10.2f}  got={got_amt:.2f}")
        if not amt_ok:
            ok = False

        # --- vendor normalisation check ---
        vendor = result["vendor"]
        is_title = vendor == vendor.title() or vendor.isupper()
        mark = "✓" if vendor else "✗"
        print(f"  {mark} vendor   : {vendor!r}")

        # --- date present ---
        dt = result["date"]
        date_ok = bool(re.fullmatch(r"\d{4}-\d{2}-\d{2}", dt))
        mark = "✓" if date_ok else "✗"
        print(f"  {mark} date     : {dt!r}")
        if not date_ok:
            ok = False

        # --- receipt_id in DB ---
        rid = result["receipt_id"]
        row = TEST_CON.execute(
            "SELECT id FROM receipts WHERE id = ?", [rid]
        ).fetchone()
        db_ok = row is not None
        mark = "✓" if db_ok else "✗"
        print(f"  {mark} DB row   : receipt_id={rid}")

        print(f"  OCR lines seen: {len(result['ocr_lines'])}")
        if not ok:
            print("  [debug] raw OCR lines:")
            for ln in result["ocr_lines"]:
                print(f"    | {ln}")
        print()

        if ok and db_ok:
            passed += 1
        else:
            failed += 1

    print("=" * 60)
    print(f"  RESULTS: {passed} passed, {failed} failed")
    print("=" * 60)

    if failed:
        sys.exit(1)


if __name__ == "__main__":
    run_smoke_test()