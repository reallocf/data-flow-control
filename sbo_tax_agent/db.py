"""
Database module for SBO Tax Agent.

Handles DuckDB connection initialization and table creation.
Uses the local DuckDB build from extended_duckdb.
Wraps the connection with SQLRewriter for policy enforcement.

Uses a global shared DuckDB instance that persists across all Streamlit sessions.
"""

import os
import threading
from pathlib import Path
import duckdb
import pandas as pd
import use_local_duckdb  # Must be imported before duckdb to configure environment
from sql_rewriter import SQLRewriter, DFCPolicy, AggregateDFCPolicy
from utils import SCHEMAS, validate_csv_schema
from agent import create_bedrock_client, BEDROCK_MODEL_ID

# Initialize local DuckDB environment
# Fail loudly if the library doesn't exist
use_local_duckdb.setup_local_duckdb()

# Get extension path for loading external extension
_SCRIPT_DIR = Path(__file__).parent
_PROJECT_ROOT = _SCRIPT_DIR.parent
_EXT_PATH = _PROJECT_ROOT / "extended_duckdb" / "build" / "release" / "repository" / "v1.4.1" / "osx_arm64" / "external.duckdb_extension"
if not _EXT_PATH.exists():
    alt_paths = [
        _PROJECT_ROOT / "extended_duckdb" / "build" / "release" / "extension" / "external" / "external.duckdb_extension",
        _PROJECT_ROOT / "extended_duckdb" / "build" / "release" / "external.duckdb_extension",
    ]
    for alt_path in alt_paths:
        if alt_path.exists():
            _EXT_PATH = alt_path
            break

# Global shared database connection (initialized once per server)
_db_rewriter = None
_db_lock = threading.Lock()


def get_db_connection(tax_return_path=None, form_1099_k_path=None, bank_txn_path=None, policies_path=None):
    """Get or create the global shared DuckDB connection for the app.
    
    Uses a single in-memory database instance that persists across all Streamlit sessions.
    The connection is wrapped with SQLRewriter for policy enforcement.
    Thread-safe initialization ensures only one connection is created.
    
    Args:
        tax_return_path: Optional path to tax_return CSV file to load on initialization
        form_1099_k_path: Optional path to form_1099_k CSV file to load on initialization
        bank_txn_path: Optional path to bank_txn CSV file to load on initialization
        policies_path: Optional path to policies CSV file to load on initialization
    
    Returns:
        SQLRewriter: The SQLRewriter instance wrapping the DuckDB connection
    """
    global _db_rewriter
    
    with _db_lock:
        if _db_rewriter is None:
            # Create in-memory database connection (once per server)
            conn = duckdb.connect(
                database=":memory:",
                config={"allow_unsigned_extensions": "true"},
            )
            
            # Load the external extension (required for async rewrite and external operator)
            if _EXT_PATH.exists():
                conn.execute(f"LOAD '{_EXT_PATH}'")
                print(f"[DB] Loaded external extension from {_EXT_PATH}")
            else:
                print(f"[WARNING] External extension not found at {_EXT_PATH}. Async rewrite will not work.")
            
            initialize_tables(conn)
            
            # Create Bedrock client for LLM resolution policies
            try:
                bedrock_client = create_bedrock_client()
            except Exception as e:
                # If Bedrock client creation fails, continue without it
                # LLM resolution policies won't work, but other functionality will
                print(f"[WARNING] Failed to create Bedrock client: {e}. LLM resolution policies will not be available.")
                bedrock_client = None
            
            # Wrap with SQLRewriter, passing Bedrock client and model ID
            _db_rewriter = SQLRewriter(
                conn=conn,
                bedrock_client=bedrock_client,
                bedrock_model_id=BEDROCK_MODEL_ID
            )
            
            # Load data from file paths if provided
            if tax_return_path or form_1099_k_path or bank_txn_path:
                load_data_from_files(_db_rewriter, tax_return_path, form_1099_k_path, bank_txn_path)
            
            # Load policies if provided
            if policies_path:
                load_policies_from_file(_db_rewriter, policies_path)
        else:
            # Connection exists - load data from file paths if provided and table is empty
            if tax_return_path:
                count = get_table_row_count(_db_rewriter, 'tax_return')
                if count == 0:
                    load_data_from_files(_db_rewriter, tax_return_path, None, None)
            
            if form_1099_k_path:
                count = get_table_row_count(_db_rewriter, 'form_1099_k')
                if count == 0:
                    load_data_from_files(_db_rewriter, None, form_1099_k_path, None)
            
            if bank_txn_path:
                count = get_table_row_count(_db_rewriter, 'bank_txn')
                if count == 0:
                    load_data_from_files(_db_rewriter, None, None, bank_txn_path)
            
            # Load policies if provided (only on first initialization, policies persist)
            if policies_path:
                # Only load if no policies are registered yet
                existing_policies = _db_rewriter.get_dfc_policies()
                if len(existing_policies) == 0:
                    load_policies_from_file(_db_rewriter, policies_path)
    
    return _db_rewriter


def initialize_tables(conn):
    """Initialize all database tables.
    
    Creates the following tables:
    1. tax_return - One row per (person, tax year) return
    2. bank_txn - Raw transactions (bank + credit card)
    3. form_1099_k - 1099-Ks (raw), one row per reported amount
    4. irs_form - Transaction-level Schedule C staging for human review
    
    Args:
        conn: DuckDB connection
    """
    # 1) Tax return table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS tax_return (
            tax_year         INTEGER,
            business_name    VARCHAR,
            business_desc    VARCHAR
        )
    """)
    
    # 2) Bank transactions table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS bank_txn (
            txn_id           UBIGINT,
            amount           DOUBLE,
            category         VARCHAR,
            description      VARCHAR
        )
    """)
    
    # 3) Form 1099-K table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS form_1099_k (
            form_name        VARCHAR,
            amount           DOUBLE
        )
    """)
    
    # 4) Schedule C review table (output)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS irs_form (
            txn_id           UBIGINT,
            amount           DOUBLE,
            kind             VARCHAR,
            business_use_pct       DOUBLE,
            valid            BOOLEAN,
            _policy_4394bd62_tmp1  DOUBLE
        )
    """)
    
    # 5) Agent interaction logs table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS agent_logs (
            txn_id           UBIGINT,
            log_line         VARCHAR,
            log_order        INTEGER
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


def load_data_from_files(rewriter, tax_return_path=None, form_1099_k_path=None, bank_txn_path=None):
    """Load CSV files into database tables with schema validation.
    
    Validates schemas match expected table schemas and fails hard on any errors.
    
    Args:
        rewriter: SQLRewriter instance
        tax_return_path: Optional path to tax_return CSV file
        form_1099_k_path: Optional path to form_1099_k CSV file
        bank_txn_path: Optional path to bank_txn CSV file
    
    Returns:
        dict: Dictionary with keys 'tax_return', 'form_1099_k', 'bank_txn' containing loaded DataFrames
    
    Raises:
        FileNotFoundError: If a provided file path doesn't exist
        ValueError: If schema validation fails
        Exception: For any other loading errors
    """
    loaded_dfs = {}
    
    if tax_return_path:
        if not os.path.exists(tax_return_path):
            raise FileNotFoundError(f"Tax return file not found: {tax_return_path}")
        
        try:
            df = pd.read_csv(tax_return_path)
            is_valid, error_msg = validate_csv_schema(df, 'tax_return')
            if not is_valid:
                raise ValueError(f"Schema validation failed for tax_return file {tax_return_path}: {error_msg}")
            
            # Validate row count: must be at most 1 row
            if len(df) > 1:
                raise ValueError(f"Tax return dataset must contain at most 1 row. Found {len(df)} rows in {tax_return_path}")
            
            load_dataframe_to_table(rewriter, df, 'tax_return')
            loaded_dfs['tax_return'] = df
        except Exception as e:
            raise Exception(f"Failed to load tax_return from {tax_return_path}: {str(e)}") from e
    
    if form_1099_k_path:
        if not os.path.exists(form_1099_k_path):
            raise FileNotFoundError(f"Form 1099-K file not found: {form_1099_k_path}")
        
        try:
            df = pd.read_csv(form_1099_k_path)
            is_valid, error_msg = validate_csv_schema(df, 'form_1099_k')
            if not is_valid:
                raise ValueError(f"Schema validation failed for form_1099_k file {form_1099_k_path}: {error_msg}")
            
            load_dataframe_to_table(rewriter, df, 'form_1099_k')
            loaded_dfs['form_1099_k'] = df
        except Exception as e:
            raise Exception(f"Failed to load form_1099_k from {form_1099_k_path}: {str(e)}") from e
    
    if bank_txn_path:
        if not os.path.exists(bank_txn_path):
            raise FileNotFoundError(f"Bank transaction file not found: {bank_txn_path}")
        
        try:
            df = pd.read_csv(bank_txn_path)
            is_valid, error_msg = validate_csv_schema(df, 'bank_txn')
            if not is_valid:
                raise ValueError(f"Schema validation failed for bank_txn file {bank_txn_path}: {error_msg}")
            
            load_dataframe_to_table(rewriter, df, 'bank_txn')
            loaded_dfs['bank_txn'] = df
        except Exception as e:
            raise Exception(f"Failed to load bank_txn from {bank_txn_path}: {str(e)}") from e
    
    return loaded_dfs


def load_policies_from_file(rewriter, policies_path):
    """Load policies from a CSV file and register them.
    
    The CSV file should have a 'policy' column with policy statements
    in the format: SOURCE <source> SINK <sink> CONSTRAINT <constraint> ON FAIL <on_fail> [DESCRIPTION <description>]
    Optionally, the CSV can have a separate 'description' column that will be used if DESCRIPTION
    is not present in the policy string.
    
    Args:
        rewriter: SQLRewriter instance
        policies_path: Path to policies CSV file
    
    Raises:
        FileNotFoundError: If the file doesn't exist
        ValueError: If the CSV format is invalid or policies cannot be parsed
        Exception: For any other loading or registration errors
    """
    if not os.path.exists(policies_path):
        raise FileNotFoundError(f"Policies file not found: {policies_path}")
    
    try:
        df = pd.read_csv(policies_path)
        
        # Validate CSV has 'policy' column
        if 'policy' not in df.columns:
            raise ValueError(f"CSV file {policies_path} must have a 'policy' column. Found columns: {', '.join(df.columns)}")
        
        # Load and register each policy
        for idx, row in df.iterrows():
            policy_text = str(row['policy']).strip()
            
            if not policy_text or policy_text.lower() == 'nan':
                raise ValueError(f"Empty policy text at row {idx + 2} (1-indexed, including header) in {policies_path}")
            
            try:
                # Determine if this is an aggregate policy by checking for AGGREGATE keyword
                normalized = policy_text.strip().upper()
                is_aggregate = normalized.startswith('AGGREGATE') or ' AGGREGATE ' in normalized
                
                # Parse and create the policy from string
                if is_aggregate:
                    policy = AggregateDFCPolicy.from_policy_str(policy_text)
                else:
                    policy = DFCPolicy.from_policy_str(policy_text)
                
                # If description column exists and policy doesn't have a description, use CSV description
                if 'description' in df.columns and not policy.description:
                    csv_description = str(row['description']).strip()
                    if csv_description and csv_description.lower() != 'nan':
                        policy.description = csv_description
                
                # Register the policy (this will validate against database)
                rewriter.register_policy(policy)
                
            except ValueError as e:
                raise ValueError(f"Failed to parse policy at row {idx + 2} in {policies_path}: {str(e)}") from e
            except Exception as e:
                raise Exception(f"Failed to register policy at row {idx + 2} in {policies_path}: {str(e)}") from e
                
    except pd.errors.EmptyDataError:
        raise ValueError(f"Policies file {policies_path} is empty")
    except Exception as e:
        if isinstance(e, (FileNotFoundError, ValueError)):
            raise
        raise Exception(f"Failed to load policies from {policies_path}: {str(e)}") from e


def save_agent_logs(rewriter, txn_id, logs):
    """Save agent interaction logs for a transaction to the database.
    
    Args:
        rewriter: SQLRewriter instance
        txn_id: Transaction ID (will be converted to int)
        logs: List of log strings
    """
    # Normalize txn_id to int for consistency
    try:
        txn_id_int = int(txn_id)
    except (ValueError, TypeError):
        # If txn_id can't be converted to int, use a hash or default value
        # For now, we'll try to extract numeric part or use 0
        txn_id_int = 0
    
    # Delete existing logs for this transaction
    rewriter.conn.execute(
        "DELETE FROM agent_logs WHERE txn_id = ?",
        [txn_id_int]
    )
    
    # Insert new logs
    for order, log_line in enumerate(logs):
        rewriter.conn.execute(
            "INSERT INTO agent_logs (txn_id, log_line, log_order) VALUES (?, ?, ?)",
            [txn_id_int, log_line, order]
        )


def load_agent_logs(rewriter):
    """Load all agent interaction logs from the database.
    
    Args:
        rewriter: SQLRewriter instance
        
    Returns:
        dict: Dictionary mapping txn_id (as int) to list of log strings
    """
    result = rewriter.conn.execute(
        "SELECT txn_id, log_line, log_order FROM agent_logs ORDER BY txn_id, log_order"
    ).fetchall()
    
    logs_dict = {}
    for txn_id, log_line, log_order in result:
        # Ensure txn_id is int for consistency
        txn_id_int = int(txn_id) if txn_id is not None else 0
        if txn_id_int not in logs_dict:
            logs_dict[txn_id_int] = []
        logs_dict[txn_id_int].append(log_line)
    
    return logs_dict

