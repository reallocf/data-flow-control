"""
Main Streamlit app for SBO Tax Agent.

This is the entry point for the multi-page Streamlit app.
Pages are automatically loaded from the pages/ directory.

Command-Line Arguments:
    --tax-return <path>: Path to tax_return CSV file to load on startup
    --bank-txn <path>: Path to bank_txn CSV file to load on startup
    --form-1099-k <path>: Path to form_1099_k CSV file to load on startup
    --policies <path>: Path to policies CSV file to load on startup
    --record <directory>: Record all LLM requests and responses to files
    --replay <session_dir>: Replay a previous session using recorded responses
    --delay <ms>: Delay in milliseconds before returning replayed responses (only applies with --replay)

Examples:
    # Run with data files
    streamlit run app.py -- --tax-return data/tax_return.csv --bank-txn data/bank_txn.csv
    
    # Record LLM interactions
    streamlit run app.py -- --record session_records
    
    # Replay a previous session
    streamlit run app.py -- --replay session_records/session_20260117_100205
    
    # Replay with delay to simulate network latency (useful for demos)
    streamlit run app.py -- --replay session_records/session_20260117_100205 --delay 500
    
    # Record while replaying (useful for testing)
    streamlit run app.py -- --record new_sessions --replay session_records/session_20260117_100205
"""

import sys
import argparse
import streamlit as st
import db

# Parse command-line arguments
# Streamlit passes custom arguments after --, e.g., streamlit run app.py -- --tax-return path/to/file.csv
def parse_args():
    """Parse command-line arguments for data file paths and LLM recording/replay.
    
    Returns:
        argparse.Namespace: Parsed arguments with the following attributes:
            - tax_return: Path to tax_return CSV file (optional)
            - form_1099_k: Path to form_1099_k CSV file (optional)
            - bank_txn: Path to bank_txn CSV file (optional)
            - policies: Path to policies CSV file (optional)
            - record: Directory to record LLM responses (optional)
            - replay: Session directory to replay (optional)
            - delay: Delay in milliseconds for replayed responses (optional, default: 0)
    """
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
    parser.add_argument(
        '--record',
        dest='record',
        type=str,
        help='Directory to record LLM responses to files'
    )
    parser.add_argument(
        '--replay',
        dest='replay',
        type=str,
        help='Session recording directory to replay (returns recorded responses instead of calling LLM)'
    )
    parser.add_argument(
        '--delay',
        dest='delay',
        type=int,
        default=0,
        help='Delay in milliseconds before returning replayed LLM responses (only applies when --replay is used)'
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
        args = argparse.Namespace(tax_return=None, form_1099_k=None, bank_txn=None, policies=None, record=None, replay=None, delay=0)
    
    return args

# Parse arguments
args = parse_args()

# Initialize recording if --record flag is provided
if args.record:
    from recording import LLMRecorder
    recorder = LLMRecorder(base_dir=args.record)
    st.session_state.llm_recorder = recorder
else:
    st.session_state.llm_recorder = None

# Initialize replay if --replay flag is provided
if args.replay:
    from replay import ReplayManager
    replay_manager = ReplayManager(session_dir=args.replay, delay_ms=args.delay)
    st.session_state.replay_manager = replay_manager
else:
    st.session_state.replay_manager = None

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

