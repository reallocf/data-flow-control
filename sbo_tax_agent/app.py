"""
Main Streamlit app for SBO Tax Agent.

This is the entry point for the multi-page Streamlit app.
Pages are automatically loaded from the pages/ directory.
"""

import sys
import argparse
import streamlit as st
import db

# Parse command-line arguments
# Streamlit passes custom arguments after --, e.g., streamlit run app.py -- --tax-return path/to/file.csv
def parse_args():
    """Parse command-line arguments for data file paths."""
    parser = argparse.ArgumentParser(
        description='SBO Tax Agent with optional data file loading'
    )
    parser.add_argument(
        '--bank-txn',
        dest='bank_txn',
        type=str,
        help='Path to bank_txn CSV file'
    )
    parser.add_argument(
        '--tax-return',
        dest='tax_return',
        type=str,
        help='Path to tax_return CSV file'
    )
    parser.add_argument(
        '--form-1099-k',
        dest='form_1099_k',
        type=str,
        help='Path to form_1099_k CSV file'
    )
    parser.add_argument(
        '--policies',
        dest='policies',
        type=str,
        help='Path to policies CSV file'
    )
    
    # Find arguments after -- separator (Streamlit convention)
    if '--' in sys.argv:
        idx = sys.argv.index('--')
        args_to_parse = sys.argv[idx + 1:]
    else:
        # If no -- separator, try parsing all args (argparse will ignore unknown ones)
        args_to_parse = sys.argv[1:]
    
    # Use parse_known_args to ignore any arguments we don't recognize
    # (in case Streamlit or other tools add their own)
    # Wrap in try-except to handle any parsing errors gracefully
    try:
        args, _ = parser.parse_known_args(args_to_parse)
    except SystemExit:
        # argparse calls sys.exit() on error, catch it and return defaults
        args = argparse.Namespace(tax_return=None, form_1099_k=None, bank_txn=None, policies=None)
    
    return args

# Parse arguments
args = parse_args()

# Page configuration
st.set_page_config(
    page_title="SBO Tax Agent",
    layout="wide"
)

# Initialize DuckDB connection (this will create tables if they don't exist)
# Pass file paths if provided via command-line arguments
try:
    db.get_db_connection(
        tax_return_path=args.tax_return,
        form_1099_k_path=args.form_1099_k,
        bank_txn_path=args.bank_txn,
        policies_path=args.policies
    )
except Exception as e:
    st.error(f"Failed to initialize database: {str(e)}")
    st.info("Make sure you've built the DuckDB library by running 'make' in the extended_duckdb directory.")
    st.stop()


# Main page content
st.title("SBO Tax Agent")
st.markdown("Small business owner tax agent using data flow control policies")

st.markdown("""
Welcome to the SBO Tax Agent! Use the sidebar to navigate between pages:

- **Upload Data**: Upload CSV files for tax returns, bank transactions, and 1099-K forms
- **Create Policies**: Define data flow control policies
- **Propose Taxes**: Generate tax proposals based on uploaded data
""")

