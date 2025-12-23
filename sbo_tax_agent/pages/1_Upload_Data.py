"""
Upload Data page for SBO Tax Agent.

Allows uploading CSV files for tax_return, bank_txn, and form_1099_k.
"""

import streamlit as st
import pandas as pd
from utils import SCHEMAS, validate_csv_schema
import db

st.set_page_config(
    page_title="Upload Data - SBO Tax Agent",
    layout="wide"
)

# Initialize session state for storing uploaded data
if 'tax_return' not in st.session_state:
    st.session_state.tax_return = None
if 'bank_txn' not in st.session_state:
    st.session_state.bank_txn = None
if 'form_1099_k' not in st.session_state:
    st.session_state.form_1099_k = None

st.header("Upload Data")
st.markdown("Upload CSV files for tax return data, bank transactions, and 1099-K forms.")

# Create three columns for the three upload sections
col1, col2, col3 = st.columns(3)

# Tax Return Upload
with col1:
    st.subheader("Tax Return")
    st.caption(SCHEMAS['tax_return']['description'])
    tax_return_file = st.file_uploader(
        "Upload tax_return.csv",
        type=['csv'],
        key='tax_return_uploader',
        help="Expected columns: return_id, tax_year, full_name, ssn, address, business_name, business_desc"
    )
    
    if tax_return_file is not None:
        try:
            df = pd.read_csv(tax_return_file)
            
            # Validate row count: must be at most 1 row (2 lines total: header + 1 data row)
            if len(df) > 1:
                st.error(f"❌ Tax return dataset must contain at most 1 row. Found {len(df)} rows.")
            else:
                is_valid, error_msg = validate_csv_schema(df, 'tax_return')
                
                if is_valid:
                    try:
                        rewriter = db.get_db_connection()
                        db.load_dataframe_to_table(rewriter, df, 'tax_return')
                        st.session_state.tax_return = df
                        st.success(f"✅ Uploaded {len(df)} rows to database")
                        st.info(f"Columns: {', '.join(df.columns)}")
                    except Exception as e:
                        st.error(f"❌ Failed to load data into database: {str(e)}")
                else:
                    st.error(f"❌ Schema validation failed: {error_msg}")
        except Exception as e:
            st.error(f"❌ Error reading CSV: {str(e)}")
    else:
        if st.session_state.tax_return is not None:
            st.info(f"📄 Current data: {len(st.session_state.tax_return)} rows")
        else:
            st.info("No file uploaded")

# Bank Transaction Upload
with col2:
    st.subheader("Bank Transactions")
    st.caption(SCHEMAS['bank_txn']['description'])
    bank_txn_file = st.file_uploader(
        "Upload bank_txn.csv",
        type=['csv'],
        key='bank_txn_uploader',
        help="Expected columns: return_id, txn_id, txn_date, amount, description, account_name, source_file"
    )
    
    if bank_txn_file is not None:
        try:
            df = pd.read_csv(bank_txn_file)
            is_valid, error_msg = validate_csv_schema(df, 'bank_txn')
            
            if is_valid:
                try:
                    rewriter = db.get_db_connection()
                    db.load_dataframe_to_table(rewriter, df, 'bank_txn')
                    st.session_state.bank_txn = df
                    st.success(f"✅ Uploaded {len(df)} rows to database")
                    st.info(f"Columns: {', '.join(df.columns)}")
                except Exception as e:
                    st.error(f"❌ Failed to load data into database: {str(e)}")
            else:
                st.error(f"❌ Schema validation failed: {error_msg}")
        except Exception as e:
            st.error(f"❌ Error reading CSV: {str(e)}")
    else:
        if st.session_state.bank_txn is not None:
            st.info(f"📄 Current data: {len(st.session_state.bank_txn)} rows")
        else:
            st.info("No file uploaded")

# Form 1099-K Upload
with col3:
    st.subheader("Form 1099-K")
    st.caption(SCHEMAS['form_1099_k']['description'])
    form_1099_k_file = st.file_uploader(
        "Upload form_1099_k.csv",
        type=['csv'],
        key='form_1099_k_uploader',
        help="Expected columns: return_id, form_id, payer_name, payer_tin, amount_type, amount, source_file"
    )
    
    if form_1099_k_file is not None:
        try:
            df = pd.read_csv(form_1099_k_file)
            is_valid, error_msg = validate_csv_schema(df, 'form_1099_k')
            
            if is_valid:
                try:
                    rewriter = db.get_db_connection()
                    db.load_dataframe_to_table(rewriter, df, 'form_1099_k')
                    st.session_state.form_1099_k = df
                    st.success(f"✅ Uploaded {len(df)} rows to database")
                    st.info(f"Columns: {', '.join(df.columns)}")
                except Exception as e:
                    st.error(f"❌ Failed to load data into database: {str(e)}")
            else:
                st.error(f"❌ Schema validation failed: {error_msg}")
        except Exception as e:
            st.error(f"❌ Error reading CSV: {str(e)}")
    else:
        if st.session_state.form_1099_k is not None:
            st.info(f"📄 Current data: {len(st.session_state.form_1099_k)} rows")
        else:
            st.info("No file uploaded")

# Display database statistics
st.divider()
st.subheader("Database Statistics")

try:
    rewriter = db.get_db_connection()
    col1, col2, col3 = st.columns(3)
    
    with col1:
        count = db.get_table_row_count(rewriter, 'tax_return')
        st.metric("Tax Returns", count)
    
    with col2:
        count = db.get_table_row_count(rewriter, 'bank_txn')
        st.metric("Bank Transactions", count)
    
    with col3:
        count = db.get_table_row_count(rewriter, 'form_1099_k')
        st.metric("Form 1099-K", count)
except Exception as e:
    st.error(f"Failed to get database statistics: {str(e)}")

# Display uploaded data
st.divider()
st.subheader("Uploaded Data Preview")

# Create tabs for each table
preview_tabs = st.tabs(["Tax Return", "Bank Transactions", "Form 1099-K"])

with preview_tabs[0]:
    if st.session_state.tax_return is not None:
        st.dataframe(st.session_state.tax_return, use_container_width=True)
        st.caption(f"Total rows: {len(st.session_state.tax_return)}")
    else:
        st.info("No tax return data uploaded yet.")

with preview_tabs[1]:
    if st.session_state.bank_txn is not None:
        st.dataframe(st.session_state.bank_txn, use_container_width=True)
        st.caption(f"Total rows: {len(st.session_state.bank_txn)}")
    else:
        st.info("No bank transaction data uploaded yet.")

with preview_tabs[2]:
    if st.session_state.form_1099_k is not None:
        st.dataframe(st.session_state.form_1099_k, use_container_width=True)
        st.caption(f"Total rows: {len(st.session_state.form_1099_k)}")
    else:
        st.info("No form 1099-K data uploaded yet.")

