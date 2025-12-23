"""
Database module for SBO Tax Agent.

Handles DuckDB connection initialization and table creation.
Uses the local DuckDB build from resolution_ui.
Wraps the connection with SQLRewriter for policy enforcement.
"""

import duckdb
import use_local_duckdb  # Must be imported before duckdb to configure environment
from sql_rewriter import SQLRewriter

# Initialize local DuckDB environment
# Fail loudly if the library doesn't exist
use_local_duckdb.setup_local_duckdb()


def get_db_connection():
    """Get or create the DuckDB connection for the app.
    
    Uses an in-memory database that persists across the Streamlit session.
    The connection is wrapped with SQLRewriter for policy enforcement.
    The SQLRewriter instance is stored in session state.
    
    Returns:
        SQLRewriter: The SQLRewriter instance wrapping the DuckDB connection
    """
    import streamlit as st
    
    if 'db_rewriter' not in st.session_state:
        # Create in-memory database connection
        conn = duckdb.connect()
        initialize_tables(conn)
        # Wrap with SQLRewriter
        st.session_state.db_rewriter = SQLRewriter(conn=conn)
    
    return st.session_state.db_rewriter


def initialize_tables(conn):
    """Initialize all database tables.
    
    Creates the following tables:
    1. tax_return - One row per (person, tax year) return
    2. bank_txn - Raw transactions (bank + credit card)
    3. form_1099_k - 1099-Ks (raw), one row per reported amount
    4. schedule_c_review - Transaction-level Schedule C staging for human review
    
    Args:
        conn: DuckDB connection
    """
    # 1) Tax return table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS tax_return (
            return_id        UBIGINT,
            tax_year         INTEGER,
            full_name        VARCHAR,
            ssn              VARCHAR,
            address          VARCHAR,
            business_name    VARCHAR,
            business_desc    VARCHAR
        )
    """)
    
    # 2) Bank transactions table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS bank_txn (
            return_id        UBIGINT,
            txn_id           UBIGINT,
            txn_date         DATE,
            amount           DOUBLE,
            description      VARCHAR,
            account_name     VARCHAR,
            source_file      VARCHAR
        )
    """)
    
    # 3) Form 1099-K table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS form_1099_k (
            return_id        UBIGINT,
            form_id          UBIGINT,
            payer_name       VARCHAR,
            payer_tin        VARCHAR,
            amount_type      VARCHAR,
            amount           DOUBLE,
            source_file      VARCHAR
        )
    """)
    
    # 4) Schedule C review table (output)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS schedule_c_review (
            return_id        UBIGINT,
            review_id        UBIGINT,
            txn_id           UBIGINT,
            txn_date         DATE,
            original_amount  DOUBLE,
            kind             VARCHAR,
            schedule_c_line  VARCHAR,
            subcategory      VARCHAR,
            business_use_pct DOUBLE,
            deductible_amount DOUBLE,
            note             VARCHAR
        )
    """)


def load_dataframe_to_table(rewriter, df, table_name):
    """Load a pandas DataFrame into a DuckDB table, replacing existing data.
    
    Args:
        rewriter: SQLRewriter instance
        df: pandas DataFrame to load
        table_name: Name of the table to load into
    """
    # Register the DataFrame as a temporary view
    temp_view_name = f"temp_{table_name}"
    rewriter.conn.register(temp_view_name, df)
    
    try:
        # Drop existing data (direct execution, no policy transformation needed)
        rewriter.conn.execute(f"DELETE FROM {table_name}")
        
        # Insert data from the registered DataFrame (direct execution)
        rewriter.conn.execute(f"INSERT INTO {table_name} SELECT * FROM {temp_view_name}")
    finally:
        # Always unregister the temporary view
        try:
            rewriter.conn.unregister(temp_view_name)
        except:
            pass  # Ignore if unregister fails


def get_table_row_count(rewriter, table_name):
    """Get the number of rows in a table.
    
    Args:
        rewriter: SQLRewriter instance
        table_name: Name of the table
        
    Returns:
        int: Number of rows in the table
    """
    result = rewriter.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
    return result[0] if result else 0

