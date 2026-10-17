"""Public-product data integration for the Regulation Z v2 benchmark."""

from __future__ import annotations

import csv
from dataclasses import asdict, dataclass
from datetime import date, datetime
import hashlib
from pathlib import Path
from typing import Any

import pandas as pd

TCCP_2025_H2_URL = "https://files.consumerfinance.gov/f/documents/cfpb_tccp-data_2025-12-31.xlsx"

CATALOG_FIELDNAMES = [
    "source_record_id",
    "source_row",
    "product_code",
    "product_name",
    "purchase_apr",
    "rate_variable",
    "index_name",
    "report_date",
    "source_url",
]


@dataclass(frozen=True)
class ProductTerm:
    """One pseudonymized public product term suitable for benchmark seeding."""

    source_record_id: str
    source_row: int
    product_code: str
    product_name: str
    purchase_apr: float
    rate_variable: bool
    index_name: str | None
    report_date: date
    source_url: str


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    normalized = str(value).strip().lower()
    if normalized in {"true", "yes", "1", "v"}:
        return True
    if normalized in {"false", "no", "0", "f", ""}:
        return False
    raise ValueError(f"Unsupported boolean value: {value!r}")


def _as_date(value: Any) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value).strip()[:10])


def _validate_product_term(term: ProductTerm) -> None:
    if not term.source_record_id:
        raise ValueError("source_record_id must not be empty")
    if not term.product_code:
        raise ValueError("product_code must not be empty")
    if not 0.0 < term.purchase_apr <= 100.0:
        raise ValueError(
            f"purchase_apr must be expressed in percentage points: {term.purchase_apr}"
        )


def load_product_terms(path: str | Path) -> list[ProductTerm]:
    """Load a normalized, pseudonymized product catalog."""
    catalog_path = Path(path)
    if catalog_path.suffix.lower() in {".xlsx", ".xlsm"}:
        return extract_tccp_product_terms(catalog_path)

    with catalog_path.open(encoding="utf-8", newline="") as source_file:
        rows = list(csv.DictReader(source_file))
    terms = [
        ProductTerm(
            source_record_id=str(row["source_record_id"]),
            source_row=int(row["source_row"]),
            product_code=str(row["product_code"]),
            product_name=str(row["product_name"]),
            purchase_apr=float(row["purchase_apr"]),
            rate_variable=_as_bool(row["rate_variable"]),
            index_name=str(row["index_name"]).strip() or None,
            report_date=_as_date(row["report_date"]),
            source_url=str(row["source_url"]),
        )
        for row in rows
    ]
    if not terms:
        raise ValueError(f"No product terms found in {catalog_path}")
    for term in terms:
        _validate_product_term(term)
    return terms


def _find_header_row(raw: pd.DataFrame) -> int:
    for row_number, row in raw.iterrows():
        values = {str(value).strip() for value in row if pd.notna(value)}
        if {"Institution Name", "Product Name", "Report Date"}.issubset(values):
            return int(row_number)
    raise ValueError("Could not find the TCCP header row")


def extract_tccp_product_terms(
    path: str | Path,
    *,
    source_url: str = TCCP_2025_H2_URL,
    limit: int | None = None,
    sample_count: int | None = None,
) -> list[ProductTerm]:
    """Extract simple, unambiguous purchase APRs from a CFPB TCCP workbook."""
    workbook_path = Path(path)
    raw = pd.read_excel(workbook_path, sheet_name="TCCP Survey", header=None)
    header_row = _find_header_row(raw)
    frame = pd.read_excel(workbook_path, sheet_name="TCCP Survey", header=header_row)
    required_columns = {
        "Institution Name",
        "Product Name",
        "Report Date",
        "Purchase APR Offered?",
        "Purchase APR Vary by Balance",
        "Purchase APR Vary by Credit Tier",
        "Purchase APR min",
        "Purchase APR median",
        "Purchase APR max",
        "Index",
        "Variable Rate Index",
    }
    missing = sorted(required_columns - set(frame.columns))
    if missing:
        raise ValueError(f"TCCP workbook is missing columns: {', '.join(missing)}")

    minimum = pd.to_numeric(frame["Purchase APR min"], errors="coerce")
    median = pd.to_numeric(frame["Purchase APR median"], errors="coerce")
    maximum = pd.to_numeric(frame["Purchase APR max"], errors="coerce")
    eligible = frame.loc[
        (frame["Purchase APR Offered?"] == "Yes")
        & (frame["Purchase APR Vary by Balance"] == "No")
        & (frame["Purchase APR Vary by Credit Tier"] == "No")
        & minimum.between(0.0001, 0.5)
        & median.between(0.0001, 0.5)
        & maximum.between(0.0001, 0.5)
        & ((minimum - median).abs() <= 0.000000001)
        & ((median - maximum).abs() <= 0.000000001)
    ]
    if limit is not None and sample_count is not None:
        raise ValueError("limit and sample_count cannot be used together")
    if limit is not None:
        if limit < 1:
            raise ValueError("limit must be at least 1")
        eligible = eligible.head(limit)
    if sample_count is not None:
        if sample_count < 1:
            raise ValueError("sample_count must be at least 1")
        if sample_count > len(eligible):
            raise ValueError(
                f"sample_count {sample_count} exceeds {len(eligible)} eligible products"
            )
        if sample_count == 1:
            positions = [0]
        else:
            positions = [
                int(index * (len(eligible) - 1) / (sample_count - 1))
                for index in range(sample_count)
            ]
        eligible = eligible.iloc[positions]

    terms: list[ProductTerm] = []
    for output_index, (source_index, row) in enumerate(eligible.iterrows(), start=1):
        report_date = _as_date(row["Report Date"])
        source_key = "|".join(
            [
                str(row["Institution Name"]).strip(),
                str(row["Product Name"]).strip(),
                report_date.isoformat(),
            ]
        )
        source_record_id = f"tccp-{hashlib.sha256(source_key.encode()).hexdigest()[:16]}"
        index_name = None
        if pd.notna(row["Variable Rate Index"]):
            index_name = str(row["Variable Rate Index"]).strip() or None
        term = ProductTerm(
            source_record_id=source_record_id,
            source_row=int(source_index) + header_row + 2,
            product_code=f"RZP{output_index:03d}",
            product_name=f"Public Card Product {output_index:03d}",
            purchase_apr=round(float(row["Purchase APR median"]) * 100.0, 3),
            rate_variable=str(row["Index"]).strip().upper() == "V",
            index_name=index_name,
            report_date=report_date,
            source_url=source_url,
        )
        _validate_product_term(term)
        terms.append(term)
    if not terms:
        raise ValueError("No simple product-wide purchase APRs were found in the workbook")
    return terms


def write_product_terms(path: str | Path, terms: list[ProductTerm]) -> None:
    """Write the normalized public-product catalog used by the benchmark."""
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="") as output_file:
        writer = csv.DictWriter(
            output_file,
            fieldnames=CATALOG_FIELDNAMES,
            lineterminator="\n",
        )
        writer.writeheader()
        for term in terms:
            row = asdict(term)
            row["rate_variable"] = str(term.rate_variable).lower()
            row["index_name"] = term.index_name or ""
            row["purchase_apr"] = f"{term.purchase_apr:.3f}"
            row["report_date"] = term.report_date.isoformat()
            writer.writerow(row)
