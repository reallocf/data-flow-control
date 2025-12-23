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

# Get database connection (once at page load)
try:
    rewriter = db.get_db_connection()
except Exception as e:
    st.error(f"Failed to connect to database: {str(e)}")
    st.stop()

st.header("Upload Data")
st.markdown("Upload CSV files for tax return data, bank transactions, and 1099-K forms.")

# Create three columns for the three upload sections
col1, col2, col3 = st.columns(3)

# Tax Return Upload
with col1:
    st.subheader("Tax Return")
    st.caption(SCHEMAS['tax_return']['description'])
    tax_return_file = st.file_uploader(
        "Select tax_return.csv",
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
                    st.info(f"✅ File validated. Ready to upload {len(df)} row(s)")
                    st.caption(f"Columns: {', '.join(df.columns)}")
                else:
                    st.error(f"❌ Schema validation failed: {error_msg}")
        except Exception as e:
            st.error(f"❌ Error reading CSV: {str(e)}")
    
    # Upload button
    upload_tax_return = st.button("Upload Tax Return", key="upload_tax_return", type="primary", disabled=(tax_return_file is None))
    
    if upload_tax_return and tax_return_file is not None:
        try:
            tax_return_file.seek(0)  # Reset file pointer
            df = pd.read_csv(tax_return_file)
            
            # Validate row count: must be at most 1 row
            if len(df) > 1:
                st.error(f"❌ Tax return dataset must contain at most 1 row. Found {len(df)} rows.")
            else:
                is_valid, error_msg = validate_csv_schema(df, 'tax_return')
                
                if is_valid:
                    try:
                        db.load_dataframe_to_table(rewriter, df, 'tax_return')
                        st.success(f"✅ Uploaded {len(df)} rows to database")
                        st.rerun()
                    except Exception as e:
                        st.error(f"❌ Failed to load data into database: {str(e)}")
                else:
                    st.error(f"❌ Schema validation failed: {error_msg}")
        except Exception as e:
            st.error(f"❌ Error reading CSV: {str(e)}")
    
    # Show current data status
    if tax_return_file is None:
        try:
            count = db.get_table_row_count(rewriter, 'tax_return')
            if count > 0:
                st.info(f"📄 Current data: {count} row(s)")
            else:
                st.info("No file uploaded")
        except Exception as e:
            st.info("No file uploaded")

# Bank Transaction Upload
with col2:
    st.subheader("Bank Transactions")
    st.caption(SCHEMAS['bank_txn']['description'])
    bank_txn_file = st.file_uploader(
        "Select bank_txn.csv",
        type=['csv'],
        key='bank_txn_uploader',
        help="Expected columns: return_id, txn_id, txn_date, amount, description, account_name, source_file"
    )
    
    if bank_txn_file is not None:
        try:
            df = pd.read_csv(bank_txn_file)
            is_valid, error_msg = validate_csv_schema(df, 'bank_txn')
            
            if is_valid:
                st.info(f"✅ File validated. Ready to upload {len(df)} row(s)")
                st.caption(f"Columns: {', '.join(df.columns)}")
            else:
                st.error(f"❌ Schema validation failed: {error_msg}")
        except Exception as e:
            st.error(f"❌ Error reading CSV: {str(e)}")
    
    # Upload button
    upload_bank_txn = st.button("Upload Bank Transactions", key="upload_bank_txn", type="primary", disabled=(bank_txn_file is None))
    
    if upload_bank_txn and bank_txn_file is not None:
        try:
            bank_txn_file.seek(0)  # Reset file pointer
            df = pd.read_csv(bank_txn_file)
            is_valid, error_msg = validate_csv_schema(df, 'bank_txn')
            
            if is_valid:
                try:
                    db.load_dataframe_to_table(rewriter, df, 'bank_txn')
                    st.success(f"✅ Uploaded {len(df)} rows to database")
                    st.rerun()
                except Exception as e:
                    st.error(f"❌ Failed to load data into database: {str(e)}")
            else:
                st.error(f"❌ Schema validation failed: {error_msg}")
        except Exception as e:
            st.error(f"❌ Error reading CSV: {str(e)}")
    
    # Show current data status
    if bank_txn_file is None:
        try:
            count = db.get_table_row_count(rewriter, 'bank_txn')
            if count > 0:
                st.info(f"📄 Current data: {count} row(s)")
            else:
                st.info("No file uploaded")
        except Exception as e:
            st.info("No file uploaded")

# Form 1099-K Upload
with col3:
    st.subheader("Form 1099-K")
    st.caption(SCHEMAS['form_1099_k']['description'])
    form_1099_k_file = st.file_uploader(
        "Select form_1099_k.csv",
        type=['csv'],
        key='form_1099_k_uploader',
        help="Expected columns: return_id, form_id, payer_name, payer_tin, amount_type, amount, source_file"
    )
    
    if form_1099_k_file is not None:
        try:
            df = pd.read_csv(form_1099_k_file)
            is_valid, error_msg = validate_csv_schema(df, 'form_1099_k')
            
            if is_valid:
                st.info(f"✅ File validated. Ready to upload {len(df)} row(s)")
                st.caption(f"Columns: {', '.join(df.columns)}")
            else:
                st.error(f"❌ Schema validation failed: {error_msg}")
        except Exception as e:
            st.error(f"❌ Error reading CSV: {str(e)}")
    
    # Upload button
    upload_form_1099_k = st.button("Upload Form 1099-K", key="upload_form_1099_k", type="primary", disabled=(form_1099_k_file is None))
    
    if upload_form_1099_k and form_1099_k_file is not None:
        try:
            form_1099_k_file.seek(0)  # Reset file pointer
            df = pd.read_csv(form_1099_k_file)
            is_valid, error_msg = validate_csv_schema(df, 'form_1099_k')
            
            if is_valid:
                try:
                    db.load_dataframe_to_table(rewriter, df, 'form_1099_k')
                    st.success(f"✅ Uploaded {len(df)} rows to database")
                    st.rerun()
                except Exception as e:
                    st.error(f"❌ Failed to load data into database: {str(e)}")
            else:
                st.error(f"❌ Schema validation failed: {error_msg}")
        except Exception as e:
            st.error(f"❌ Error reading CSV: {str(e)}")
    
    # Show current data status
    if form_1099_k_file is None:
        try:
            count = db.get_table_row_count(rewriter, 'form_1099_k')
            if count > 0:
                st.info(f"📄 Current data: {count} row(s)")
            else:
                st.info("No file uploaded")
        except Exception as e:
            st.info("No file uploaded")

# Display database statistics
st.divider()
st.subheader("Database Statistics")

try:
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
    try:
        result = rewriter.conn.execute("SELECT * FROM tax_return").df()
        if len(result) > 0:
            st.dataframe(result, use_container_width=True)
            st.caption(f"Total rows: {len(result)}")
        else:
            st.info("No tax return data uploaded yet.")
    except Exception as e:
        st.info("No tax return data uploaded yet.")

with preview_tabs[1]:
    try:
        result = rewriter.conn.execute("SELECT * FROM bank_txn").df()
        if len(result) > 0:
            st.dataframe(result, use_container_width=True)
            st.caption(f"Total rows: {len(result)}")
        else:
            st.info("No bank transaction data uploaded yet.")
    except Exception as e:
        st.info("No bank transaction data uploaded yet.")

with preview_tabs[2]:
    try:
        result = rewriter.conn.execute("SELECT * FROM form_1099_k").df()
        if len(result) > 0:
            st.dataframe(result, use_container_width=True)
            st.caption(f"Total rows: {len(result)}")
        else:
            st.info("No form 1099-K data uploaded yet.")
    except Exception as e:
        st.info("No form 1099-K data uploaded yet.")

