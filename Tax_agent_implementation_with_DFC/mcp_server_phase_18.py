"""
Phase 6 — Tax Agent MCP Server (Vanilla)

Completely standard MCP server.
No DFC logic, no execute_sql, no tool_inputs, no chains.
Just get_receipt and set_expense as normal tools.

The client handles everything:
  - SQL intent parsing
  - DFC policy enforcement (SQLRewriter)
  - Per-tool input/output tables (local DuckDB)
  - JSON arg extraction and tool dispatch

OCR integration (EasyOCR):
  - get_receipt accepts an optional image_path parameter
  - If receipt_id is provided it takes priority; image_path is ignored
  - If only image_path is provided, _parse_and_store_receipt() runs OCR,
    extracts and normalizes fields, inserts a new receipts row, and returns
    the new receipt_id for the standard DB lookup path
"""

from mcp.server.fastmcp import FastMCP
import os
import re
import unicodedata
import duckdb
from datetime import date, datetime
from typing import Optional

DB_PATH = os.path.join(os.path.dirname(__file__), "expenses.duckdb")

# ---------------------------------------------------------------------------
# OCR helper — lazy import so the server starts even without easyocr/torch
# ---------------------------------------------------------------------------

_ocr_reader = None  # initialised on first use


def _get_ocr_reader():
    global _ocr_reader
    if _ocr_reader is None:
        try:
            import easyocr
            _ocr_reader = easyocr.Reader(["en"], gpu=False, verbose=False)
        except ImportError:
            raise RuntimeError(
                "easyocr is not installed. Run: pip install easyocr"
            )
    return _ocr_reader


# ---------------------------------------------------------------------------
# Normalisation utilities
# ---------------------------------------------------------------------------

# Canonical category keywords — order matters: first match wins
_CATEGORY_KEYWORDS: list[tuple[str, list[str]]] = [
    ("meal", [
        "restaurant", "cafe", "coffee", "lunch", "dinner", "breakfast",
        "food", "diner", "bistro", "grill", "bar", "pub", "pizza", "burger",
        "sushi", "thai", "indian", "mexican", "chinese", "italian", "bakery",
        "eatery", "kitchen", "cantina", "steakhouse", "seafood", "noodle",
        "sandwich", "boba", "smoothie", "juice",
    ]),
    ("transport", [
        "uber", "lyft", "taxi", "cab", "transit", "mta", "metro", "bus",
        "train", "flight", "airline", "parking", "toll", "gas", "fuel",
        "rideshare", "amtrak", "greyhound", "zipcar", "hertz", "avis",
        "enterprise", "shell", "chevron", "bp", "exxon",
    ]),
    ("supplies", [
        "office", "staples", "amazon", "best buy", "depot", "supply",
        "supplies", "equipment", "hardware", "costco", "walmart", "target",
        "home depot", "ikea", "fedex", "ups", "print", "paper", "ink",
    ]),
]

_DATE_PATTERNS = [
    (r"\b(\d{1,2})[/\-](\d{1,2})[/\-](\d{4})\b",  "mdy_slash"),  # MM/DD/YYYY
    (r"\b(\d{4})[/\-](\d{1,2})[/\-](\d{1,2})\b",  "iso"),        # YYYY-MM-DD
    (
        r"\b(Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|"
        r"Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|"
        r"Dec(?:ember)?)\s+(\d{1,2}),?\s+(\d{4})\b",
        "month_name",
    ),
]

_MONTH_ABBR = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}

_TOTAL_PATTERNS = [
    r"total\s*[:\-]?\s*\$?\s*([\d,]+\.\d{2})",
    r"amount\s+due\s*[:\-]?\s*\$?\s*([\d,]+\.\d{2})",
    r"balance\s+due\s*[:\-]?\s*\$?\s*([\d,]+\.\d{2})",
    r"grand\s+total\s*[:\-]?\s*\$?\s*([\d,]+\.\d{2})",
    r"subtotal\s*[:\-]?\s*\$?\s*([\d,]+\.\d{2})",
    r"\$\s*([\d,]+\.\d{2})",  # fallback: any dollar amount
]


def _normalise_text(s: str) -> str:
    """Strip, collapse whitespace, and remove non-printable characters."""
    s = unicodedata.normalize("NFKC", s)
    s = re.sub(r"[^\x20-\x7E]", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def _normalise_vendor(s: str) -> str:
    return _normalise_text(s).title()


def _normalise_category(raw: str, vendor: str = "") -> str:
    """Map free-form text to one of: meal / transport / supplies / other."""
    haystack = (raw + " " + vendor).lower()
    for canonical, keywords in _CATEGORY_KEYWORDS:
        if any(kw in haystack for kw in keywords):
            return canonical
    return "other"


def _normalise_ocr_line(s: str) -> str:
    """
    Fix the most common single-character OCR mis-reads found on receipts:
      S/s/$ at the start of a number → $    (e.g. S74.25 → $74.25)
      5 before a small amount         → $    (e.g. 518.50 when true total is $18.50)
      _ or - between digits           → .    (e.g. 18_50  → 18.50)

    The '5' rule: OCR frequently reads '$' as '5' when the dollar sign is
    rendered at a margin edge.  A leading '5' followed by digits and a decimal
    point where the remaining portion is < 500 is treated as a mis-read '$'.
    """
    # S/s → $ prefix
    s = re.sub(r"(?<![A-Za-z])[Ss](\d)", r"$\1", s)
    # 5 → $ when it looks like a mis-read dollar sign: 5XX.XX where XX.XX < 500
    def _fix_5(m):
        inner = m.group(1)
        try:
            if float(inner) < 500:
                return f"${inner}"
        except ValueError:
            pass
        return m.group(0)
    s = re.sub(r"(?<!\d)5([\d,]{1,4}\.\d{2})\b", _fix_5, s)
    # merge split decimals: 18 _ 50 → 18.50
    s = re.sub(r"(\d)[_\-\s](\d{2})\b", r"\1.\2", s)
    return s


def _extract_number(s: str) -> Optional[float]:
    """Pull the first X.XX or X,XXX.XX number from a string."""
    m = re.search(r"\$?\s*([\d,]{1,7}\.\d{2})", s)
    if m:
        try:
            return round(float(m.group(1).replace(",", "")), 2)
        except ValueError:
            pass
    return None


# Labels OCR commonly produces for "TOTAL" — L is often read as I or 1
_TOTAL_LABELS = re.compile(
    r"(grand\s*tota[li1]|tota[li1]|amount\s+due|balance\s+due|subtota[li1])",
    re.IGNORECASE,
)


def _parse_amount(lines: list[str]) -> Optional[float]:
    """
    Extract the receipt total from OCR lines.

    Strategy (in priority order):
    1. Find a line containing a TOTAL-like label (tolerates TOTAI, TOTA1).
       Search that line plus the next two lines for a dollar amount —
       this handles OCR splitting the label and number into separate blocks.
    2. Search each line individually for a labelled total pattern.
    3. Last resort: return the largest dollar amount in the document that
       is NOT on a line containing typical line-item words (Tax, Tip, Fee).
    """
    norm = [_normalise_ocr_line(ln) for ln in lines]

    # Pass 1 — proximity search around the TOTAL label
    for i, line in enumerate(norm):
        if _TOTAL_LABELS.search(line):
            window = " ".join(norm[i: i + 3])  # label line + up to 2 more
            amt = _extract_number(window)
            if amt is not None and amt > 0:
                return amt

    # Pass 2 — per-line labelled patterns (joined within a single OCR line)
    for line in norm:
        for pattern in _TOTAL_PATTERNS[:-1]:
            m = re.search(pattern, line, re.IGNORECASE)
            if m:
                try:
                    return round(float(m.group(1).replace(",", "")), 2)
                except ValueError:
                    continue

    # Pass 3 — largest amount, excluding obvious line-item lines
    _ITEM_WORDS = re.compile(r"\b(tax|tip|fee|item|qty|price|each|sub)\b", re.IGNORECASE)
    candidates = []
    for line in norm:
        if _ITEM_WORDS.search(line):
            continue
        amt = _extract_number(line)
        if amt is not None:
            candidates.append(amt)
    if candidates:
        return max(candidates)

    return None


def _parse_date(lines: list[str]) -> Optional[str]:
    """Return an ISO date string (YYYY-MM-DD) or None."""
    joined = " ".join(lines)
    for pattern, fmt in _DATE_PATTERNS:
        m = re.search(pattern, joined, re.IGNORECASE)
        if not m:
            continue
        try:
            if fmt == "mdy_slash":
                mo, dy, yr = int(m.group(1)), int(m.group(2)), int(m.group(3))
                return date(yr, mo, dy).isoformat()
            elif fmt == "iso":
                yr, mo, dy = int(m.group(1)), int(m.group(2)), int(m.group(3))
                return date(yr, mo, dy).isoformat()
            elif fmt == "month_name":
                month_str = m.group(1)[:3].lower()
                mo = _MONTH_ABBR.get(month_str)
                if mo:
                    dy, yr = int(m.group(2)), int(m.group(3))
                    return date(yr, mo, dy).isoformat()
        except (ValueError, KeyError):
            continue
    return None


def _pick_vendor(results: list[tuple]) -> str:
    """
    Select the vendor name from OCR results.

    Strategy:
      1. Find all text blocks in the top 25% of the image that are not purely
         numeric/symbolic, sorted left-to-right by their centre x.
      2. Concatenate them in reading order to reconstruct multi-word names
         (e.g. "THE" + "FAKE" + "RESTAURANT" → "THE FAKE RESTAURANT").
      3. If the top region yields nothing, fall back to the first qualifying
         block anywhere in the image.
    """
    if not results:
        return "Unknown Vendor"

    # EasyOCR result: (bbox, text, confidence)
    # bbox is [[x1,y1],[x2,y2],[x3,y3],[x4,y4]]
    all_y = [pt[1] for bbox, _, _ in results for pt in bbox]
    img_height = max(all_y) if all_y else 1

    def _centre_y(bbox):
        return sum(pt[1] for pt in bbox) / 4

    def _centre_x(bbox):
        return sum(pt[0] for pt in bbox) / 4

    def _is_text(s):
        return (
            len(s.strip()) >= 2
            and not re.fullmatch(r"[\d\s\$\.\,\:\-\/\(\)]+", s.strip())
        )

    # Collect qualifying blocks in top 25%, sorted by vertical band then x
    top_blocks = sorted(
        [
            (bbox, text)
            for bbox, text, _ in results
            if _centre_y(bbox) < img_height * 0.25 and _is_text(text)
        ],
        key=lambda b: (_centre_y(b[0]), _centre_x(b[0])),
    )

    if top_blocks:
        # Group into lines (blocks within 10px vertically are on the same line)
        lines_grouped: list[list[str]] = []
        current_line: list[tuple] = []
        prev_y = None
        for bbox, text in top_blocks:
            cy = _centre_y(bbox)
            if prev_y is None or abs(cy - prev_y) <= 10:
                current_line.append(text)
            else:
                if current_line:
                    lines_grouped.append(current_line)
                current_line = [text]
            prev_y = cy
        if current_line:
            lines_grouped.append(current_line)

        # First line with at least one token is the vendor
        for line_tokens in lines_grouped:
            candidate = " ".join(line_tokens).strip()
            if candidate:
                return _normalise_vendor(candidate)

    # fallback: first qualifying block anywhere
    for _, text, _ in results:
        if _is_text(text):
            return _normalise_vendor(text)

    return "Unknown Vendor"


# ---------------------------------------------------------------------------
# Core OCR parse-and-store
# ---------------------------------------------------------------------------

def _parse_and_store_receipt(image_path: str, con: duckdb.DuckDBPyConnection) -> dict:
    """
    Run EasyOCR on image_path, extract and normalise fields, insert a new
    receipts row, and return a dict with all parsed fields + new receipt_id.

    Raises RuntimeError if easyocr is not installed.
    Raises FileNotFoundError if image_path does not exist.
    """
    if not os.path.isfile(image_path):
        raise FileNotFoundError(f"Image not found: {image_path}")

    reader = _get_ocr_reader()
    results = reader.readtext(image_path)  # [(bbox, text, confidence)]

    print(f"[ocr] {len(results)} text blocks detected in {image_path}")

    # Collect raw text lines (filter very low confidence)
    lines = [
        _normalise_text(text)
        for _, text, conf in results
        if conf >= 0.20 and _normalise_text(text)
    ]

    vendor   = _pick_vendor(results)
    amount   = _parse_amount(lines)
    date_str = _parse_date(lines)
    category = _normalise_category("", vendor)

    # Defaults for fields we couldn't parse
    if amount is None:
        amount = 0.0
    if date_str is None:
        date_str = date.today().isoformat()

    # Next available ID
    max_id = con.execute("SELECT COALESCE(MAX(id), 0) FROM receipts").fetchone()[0]
    new_id = max_id + 1

    con.execute(
        "INSERT INTO receipts (id, vendor, amount, category, date) VALUES (?, ?, ?, ?, ?)",
        [new_id, vendor, amount, category, date_str],
    )

    print(
        f"[ocr] stored receipt id={new_id} vendor='{vendor}' "
        f"amount={amount} category='{category}' date={date_str}"
    )

    return {
        "receipt_id": new_id,
        "vendor":     vendor,
        "amount":     amount,
        "category":   category,
        "date":       date_str,
        "ocr_lines":  lines,
    }


def init_db():
    con = duckdb.connect(DB_PATH)

    con.execute("CREATE SEQUENCE IF NOT EXISTS expenses_id_seq START 1")

    con.execute("""
        CREATE TABLE IF NOT EXISTS expenses (
            id           INTEGER PRIMARY KEY DEFAULT nextval('expenses_id_seq'),
            date         DATE NOT NULL DEFAULT current_date,
            amount       DOUBLE NOT NULL,
            category     VARCHAR NOT NULL,
            business_use DOUBLE DEFAULT 0.0,
            note         VARCHAR DEFAULT '',
            valid        BOOLEAN DEFAULT TRUE
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS receipts (
            id       INTEGER PRIMARY KEY,
            vendor   VARCHAR,
            amount   DOUBLE,
            category VARCHAR,
            date     DATE
        )
    """)

    if con.execute("SELECT COUNT(*) FROM receipts").fetchone()[0] == 0:
        con.execute("""
            INSERT INTO receipts VALUES
                (1, 'Restaurant ABC',  50.0,  'meal',      current_date),
                (2, 'Uber',            25.0,  'transport', current_date),
                (3, 'Office Supplies', 100.0, 'supplies',  current_date),
                (4, 'Coffee Shop',     15.0,  'meal',      current_date)
        """)

    con.close()


init_db()

mcp = FastMCP("TaxAgent")
CON = duckdb.connect(DB_PATH)


@mcp.tool()
def get_receipt(receipt_id: Optional[int] = None, image_path: Optional[str] = None) -> dict:
    """
    Fetch a receipt by ID, or parse one from a receipt image via OCR.

    Priority: receipt_id takes precedence — if both are supplied, image_path
    is ignored and the existing DB row is returned as-is.

    Args:
        receipt_id: ID of an existing receipt in the database.
        image_path: Absolute path to a receipt image (JPEG/PNG/etc.).
                    Used only when receipt_id is not provided.

    Returns vendor, amount, category, date.
    OCR responses also include ocr_source=True and ocr_lines for transparency.
    """
    if receipt_id is None and image_path is None:
        return {"status": "error", "error": "Provide receipt_id or image_path"}

    ocr_meta = {}

    if receipt_id is None:
        # --- OCR path ---
        try:
            parsed = _parse_and_store_receipt(image_path, CON)
        except FileNotFoundError as exc:
            return {"status": "error", "error": str(exc)}
        except RuntimeError as exc:
            return {"status": "error", "error": str(exc)}

        receipt_id = parsed["receipt_id"]
        ocr_meta   = {"ocr_source": True, "ocr_lines": parsed["ocr_lines"]}

    # --- Standard DB lookup (used by both paths) ---
    row = CON.execute(
        "SELECT id, vendor, amount, category, date FROM receipts WHERE id = ?",
        [receipt_id]
    ).fetchone()

    if not row:
        return {"status": "error", "error": f"Receipt {receipt_id} not found"}

    print(f"[get_receipt] receipt_id={receipt_id}, category={row[3]}")

    return {
        "status":     "ok",
        "receipt_id": row[0],
        "vendor":     row[1],
        "amount":     row[2],
        "category":   row[3],
        "date":       str(row[4]),
        **ocr_meta,
    }


@mcp.tool()
def set_expense(
    receipt_id:   int,
    amount:       float,
    category:     str,
    business_use: float,
) -> dict:
    """
    Record a business expense.
    Returns status and the new expense id.
    """
    CON.execute("""
        INSERT INTO expenses (date, amount, category, business_use, note)
        VALUES (current_date, ?, ?, ?, ?)
    """, [amount, category, business_use, f"receipt_id={receipt_id}"])

    expense_id = CON.execute(
        "SELECT MAX(id) FROM expenses"
    ).fetchone()[0]

    print(
        f"[set_expense] receipt={receipt_id}, category={category}, "
        f"business_use={business_use} → expense_id={expense_id}"
    )

    return {
        "status":     "ok",
        "expense_id": expense_id,
        "message":    f"Expense recorded — category={category}, amount={amount}, business_use={business_use}",
    }


@mcp.tool()
def list_expenses(limit: int = 100) -> dict:
    """
    List recorded expenses in reverse chronological order.
    """
    rows = CON.execute(
        """
        SELECT id, date, amount, category, business_use, note, valid
        FROM expenses
        ORDER BY date DESC, id DESC
        LIMIT ?
        """,
        [min(max(int(limit), 1), 1000)]
    ).fetchall()

    result = [
        {
            "id": r[0],
            "date": str(r[1]),
            "amount": r[2],
            "category": r[3],
            "business_use": r[4],
            "note": r[5],
            "valid": r[6],
        }
        for r in rows
    ]

    print(f"[list_expenses] returned {len(result)} rows")

    return {
        "status": "ok",
        "count":  len(result),
        "rows":   result,
    }


if __name__ == "__main__":
    mcp.run(transport="sse")