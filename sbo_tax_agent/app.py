"""
Main Streamlit app for SBO Tax Agent.

This is the entry point for the multi-page Streamlit app.
Pages are automatically loaded from the pages/ directory.
"""

import streamlit as st
import db

# Page configuration
st.set_page_config(
    page_title="SBO Tax Agent",
    layout="wide"
)

# Initialize DuckDB connection (this will create tables if they don't exist)
try:
    db.get_db_connection()
except Exception as e:
    st.error(f"Failed to initialize database: {str(e)}")
    st.info("Make sure you've built the DuckDB library by running 'make' in the resolution_ui directory.")

# Initialize session state for storing uploaded data
if 'tax_return' not in st.session_state:
    st.session_state.tax_return = None
if 'bank_txn' not in st.session_state:
    st.session_state.bank_txn = None
if 'form_1099_k' not in st.session_state:
    st.session_state.form_1099_k = None

# Main page content
st.title("SBO Tax Agent")
st.markdown("Small business owner tax agent using data flow control policies")

st.markdown("""
Welcome to the SBO Tax Agent! Use the sidebar to navigate between pages:

- **Upload Data**: Upload CSV files for tax returns, bank transactions, and 1099-K forms
- **Create Policies**: Define data flow control policies
- **Propose Taxes**: Generate tax proposals based on uploaded data
""")

