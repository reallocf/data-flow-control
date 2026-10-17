#!/usr/bin/env python3
"""Download and normalize public CFPB product terms for the v2 benchmark."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys
import tempfile
from urllib.request import urlretrieve

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / "src"))

from vldb_experiments.strategies.regulation_z_v2_data import (  # noqa: E402
    TCCP_2025_H2_URL,
    extract_tccp_product_terms,
    write_product_terms,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Create a pseudonymized catalog from a CFPB TCCP workbook."
    )
    parser.add_argument("--input", type=Path, help="Existing TCCP workbook; otherwise download it.")
    parser.add_argument("--source-url", default=TCCP_2025_H2_URL)
    parser.add_argument(
        "--output",
        type=Path,
        default=project_root / "data/regulation_z_v2/tccp_product_terms.csv",
    )
    parser.add_argument("--limit", type=int)
    parser.add_argument(
        "--sample-count",
        type=int,
        help="Select this many evenly spaced eligible products from the workbook.",
    )
    args = parser.parse_args()

    if args.input:
        terms = extract_tccp_product_terms(
            args.input,
            source_url=args.source_url,
            limit=args.limit,
            sample_count=args.sample_count,
        )
    else:
        with tempfile.TemporaryDirectory(prefix="regulation-z-v2-") as temp_dir:
            workbook_path = Path(temp_dir) / "tccp.xlsx"
            urlretrieve(args.source_url, workbook_path)
            terms = extract_tccp_product_terms(
                workbook_path,
                source_url=args.source_url,
                limit=args.limit,
                sample_count=args.sample_count,
            )
    write_product_terms(args.output, terms)
    print(f"Wrote {len(terms)} pseudonymized product terms to {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
