"""
Shared utilities for the SBO Tax Agent app.
"""

# Define expected schemas
SCHEMAS = {
    'tax_return': {
        'columns': ['tax_year', 'business_name', 'business_desc'],
        'description': 'One row per (person, tax year) return'
    },
    'bank_txn': {
        'columns': ['txn_id', 'amount', 'category', 'description'],
        'description': 'Raw transactions (bank + credit card). Positive=inflow, negative=outflow.'
    },
    'form_1099_k': {
        'columns': ['form_name', 'amount'],
        'description': '1099-Ks (raw). One row per reported amount.'
    }
}

def validate_csv_schema(df, table_name):
    """Validate that the uploaded CSV matches the expected schema."""
    expected_cols = set(SCHEMAS[table_name]['columns'])
    actual_cols = set(df.columns)
    
    if expected_cols != actual_cols:
        missing = expected_cols - actual_cols
        extra = actual_cols - expected_cols
        errors = []
        if missing:
            errors.append(f"Missing columns: {', '.join(missing)}")
        if extra:
            errors.append(f"Unexpected columns: {', '.join(extra)}")
        return False, '; '.join(errors)
    
    return True, None

