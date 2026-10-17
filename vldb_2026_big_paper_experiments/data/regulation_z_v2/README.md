# Regulation Z v2 public product terms

`tccp_2025_h2_product_terms.csv` is a small, pinned, pseudonymized extract from the CFPB's
Terms of Credit Card Plans survey for July 1 through December 31, 2025.

Source workbook:
https://files.consumerfinance.gov/f/documents/cfpb_tccp-data_2025-12-31.xlsx

The extract contains only products with a positive, unambiguous product-wide purchase APR:

- purchase APR is offered;
- APR does not vary by balance;
- APR does not vary by credit tier;
- minimum, median, and maximum purchase APR are equal; and
- the source value is between 0.0001 and 0.5 before conversion.

APRs are converted from the source workbook's fractional representation to percentage points.
Issuer and product names are replaced with benchmark identifiers before the data are stored here.
`source_record_id`, `source_row`, `report_date`, and `source_url` preserve reproducible provenance.

The public APR is used only to seed an internal `apr_config` row. Accounts, transactions, daily
balances, statements, and publication attempts are synthetic. The benchmark never interprets a
survey row as evidence that a particular consumer statement is correct.

Regenerate a catalog with:

```bash
.venv/bin/python scripts/prepare_regulation_z_v2_data.py \
  --sample-count 24 \
  --output data/regulation_z_v2/tccp_2025_h2_product_terms.csv
```
