"""
Propose Taxes page for SBO Tax Agent.

Generates tax proposals based on uploaded data and policies.
Uses an agentic loop with AWS Bedrock to analyze transactions.
"""

import pandas as pd
import streamlit as st

import agent
import db

st.set_page_config(
    page_title="Propose Taxes - SBO Tax Agent",
    layout="wide"
)

st.header("Propose Taxes")

# Get database connection
try:
    rewriter = db.get_db_connection()
    # Set recorder if available
    recorder = st.session_state.get("llm_recorder")
    if recorder:
        rewriter.set_recorder(recorder)
    # Set replay manager if available
    replay_manager = st.session_state.get("replay_manager")
    if replay_manager:
        rewriter.set_replay_manager(replay_manager)
except Exception as e:
    st.error(f"Failed to connect to database: {e!s}")
    st.stop()

# Reset button
if st.button("Reset Page", type="secondary"):
    try:
        # Truncate irs_form table
        rewriter.conn.execute("DELETE FROM irs_form")

        # Truncate agent_logs table
        rewriter.conn.execute("DELETE FROM agent_logs")

        # Reset stream file path to avoid including same stream entries across different runs
        rewriter.reset_stream_file_path()

        # Clear session state
        if "agent_processing" in st.session_state:
            st.session_state.agent_processing = False
        if "agent_progress" in st.session_state:
            st.session_state.agent_progress = []
        if "transactions_to_process" in st.session_state:
            st.session_state.transactions_to_process = []
        if "current_txn_index" in st.session_state:
            st.session_state.current_txn_index = 0
        if "tax_return_info" in st.session_state:
            st.session_state.tax_return_info = None
        if "aggregate_policy_violations" in st.session_state:
            st.session_state.aggregate_policy_violations = {}

        st.success("Page reset successfully!")
        st.rerun()
    except Exception as e:
        st.error(f"Error resetting page: {e!s}")

# Check if there are any existing agent logs
try:
    existing_logs = db.load_agent_logs(rewriter)
    has_existing_logs = len(existing_logs) > 0
except Exception:
    has_existing_logs = False

# Initialize session state for agent processing
if "agent_processing" not in st.session_state:
    st.session_state.agent_processing = False
if "agent_progress" not in st.session_state:
    st.session_state.agent_progress = []
if "transactions_to_process" not in st.session_state:
    st.session_state.transactions_to_process = []
if "current_txn_index" not in st.session_state:
    st.session_state.current_txn_index = 0
if "tax_return_info" not in st.session_state:
    st.session_state.tax_return_info = None

# Get tax return (assuming single return)
try:
    tax_return_query = "SELECT * FROM tax_return LIMIT 1"
    tax_return_result = rewriter.execute(tax_return_query)
    tax_return_rows = tax_return_result.fetchall()

    if not tax_return_rows:
        st.warning("No tax return data found. Please upload tax return data on the Upload Data page.")
        st.stop()

    tax_return_columns = [desc[0] for desc in tax_return_result.description]
    tax_return_info = dict(zip(tax_return_columns, tax_return_rows[0]))

    st.info(f"Processing tax return for: {tax_return_info.get('business_name', 'N/A')} (Tax Year: {tax_return_info.get('tax_year', 'N/A')})")

except Exception as e:
    st.error(f"Error loading tax return: {e!s}")
    st.stop()

# Create two columns for tables
col1, col2 = st.columns(2)

# Left column: Bank transactions
with col1:
    st.subheader("Bank Transactions")
    try:
        bank_txn_query = """
            SELECT * FROM bank_txn 
            ORDER BY txn_id
        """
        bank_txn_result = rewriter.execute(bank_txn_query)
        bank_txn_rows = bank_txn_result.fetchall()
        bank_txn_columns = [desc[0] for desc in bank_txn_result.description]

        if bank_txn_rows:
            bank_txn_df = pd.DataFrame(bank_txn_rows, columns=bank_txn_columns)
            st.dataframe(bank_txn_df, width="stretch", hide_index=True)
            st.caption(f"Total transactions: {len(bank_txn_df)}")
        else:
            st.info("No bank transactions found for this tax return.")
    except Exception as e:
        st.error(f"Error loading bank transactions: {e!s}")

# Right column: Tax Form
with col2:
    st.subheader("Tax Form")
    try:
        irs_form_query = """
            SELECT * FROM irs_form 
            ORDER BY txn_id
        """
        irs_form_result = rewriter.execute(irs_form_query)
        irs_form_rows = irs_form_result.fetchall()
        irs_form_columns = [desc[0] for desc in irs_form_result.description]

        if irs_form_rows:
            irs_form_df = pd.DataFrame(irs_form_rows, columns=irs_form_columns)

            # Format numeric columns to show minimal decimals
            def format_number(x):
                if pd.notna(x) and isinstance(x, (int, float)):
                    return f"{x:g}"
                return x

            format_dict = {}
            numeric_columns = ["amount", "business_use_pct"]
            for col in numeric_columns:
                if col in irs_form_df.columns:
                    format_dict[col] = format_number

            # Remove 'valid' column from display but keep it for styling
            has_valid_column = "valid" in irs_form_df.columns

            # Remove policy temp columns from display (they're internal tracking columns)
            policy_temp_columns = [col for col in irs_form_df.columns if col.startswith("_policy_") and "_tmp" in col]

            # Check for aggregate policy violations
            aggregate_violations = st.session_state.get("aggregate_policy_violations", {})
            has_aggregate_violations = any(
                msg is not None for msg in aggregate_violations.values()
            )

            # Drop columns that shouldn't be displayed
            columns_to_drop = []
            if has_valid_column:
                columns_to_drop.append("valid")
            if policy_temp_columns:
                columns_to_drop.extend(policy_temp_columns)

            if columns_to_drop:
                # Store invalid mask before dropping columns
                invalid_rows = irs_form_df["valid"] == False if has_valid_column else pd.Series([False] * len(irs_form_df), index=irs_form_df.index)
                # Drop the columns from display
                display_df = irs_form_df.drop(columns=columns_to_drop)

                # Update format_dict to only include columns still in display_df
                display_format_dict = {k: v for k, v in format_dict.items() if k in display_df.columns}

                # Apply styling: highlight invalid rows in red background
                # and add subtle background tint for aggregate violations
                def highlight_invalid(row):
                    styles = []
                    for col in display_df.columns:
                        style = ""
                        if has_valid_column and invalid_rows.loc[row.name]:
                            # Strong red background for invalid rows (takes priority)
                            style = "background-color: #ffcccc"
                        elif has_aggregate_violations:
                            # Very subtle red tint for all other cells when aggregate violations exist
                            # This provides a subtle table-wide indicator
                            style = "background-color: #fff5f5"
                        styles.append(style)
                    return styles

                styled_df = display_df.style.apply(highlight_invalid, axis=1)

                if display_format_dict:
                    styled_df = styled_df.format(display_format_dict)

                st.dataframe(styled_df, width="stretch", hide_index=True)
            else:
                # No valid column or policy temp columns to drop, but still check for policy temp columns
                display_df = irs_form_df
                if policy_temp_columns:
                    display_df = irs_form_df.drop(columns=policy_temp_columns)

                display_format_dict = {k: v for k, v in format_dict.items() if k in display_df.columns}
                if display_format_dict:
                    styled_df = display_df.style.format(display_format_dict)
                    st.dataframe(styled_df, width="stretch", hide_index=True)
                else:
                    st.dataframe(display_df, width="stretch", hide_index=True)

            st.caption(f"Total entries: {len(irs_form_df)}")
        else:
            st.info("No tax form entries yet. Click 'Propose Taxes' to start processing.")
    except Exception as e:
        st.error(f"Error loading tax form: {e!s}")

# Disable button if processing or if logs already exist
button_disabled = st.session_state.agent_processing or has_existing_logs

if st.button("Propose Taxes", type="primary", disabled=button_disabled):
    try:
        # Get tax return info
        tax_return_query = "SELECT * FROM tax_return LIMIT 1"
        tax_return_result = rewriter.execute(tax_return_query)
        tax_return_rows = tax_return_result.fetchall()

        if not tax_return_rows:
            st.error("No tax return found")
        else:
            tax_return_columns = [desc[0] for desc in tax_return_result.description]
            tax_return_info = dict(zip(tax_return_columns, tax_return_rows[0]))

            # Get all transactions
            transactions_query = """
                SELECT * FROM bank_txn 
                ORDER BY txn_id
            """
            transactions_result = rewriter.execute(transactions_query)
            transactions_rows = transactions_result.fetchall()
            transaction_columns = [desc[0] for desc in transactions_result.description]

            transactions = []
            for row in transactions_rows:
                transactions.append(dict(zip(transaction_columns, row)))

            # Initialize processing state
            st.session_state.agent_processing = True
            st.session_state.agent_progress = []
            st.session_state.tax_return_info = tax_return_info
            st.session_state.transactions_to_process = transactions
            st.session_state.current_txn_index = 0
            st.rerun()
    except Exception as e:
        st.error(f"Error initializing processing: {e!s}")

# Agent Interaction Logs Section
# Always show logs section (even during processing) so tabs appear as transactions complete
st.divider()
st.subheader("Agent Interaction Logs")

# Load logs from database
try:
    agent_logs = db.load_agent_logs(rewriter)

    if agent_logs:
        # Create tabs for each transaction
        txn_ids = sorted(agent_logs.keys())  # Sort for consistent ordering

        if txn_ids:
            # Create tab labels
            tab_labels = [f"Txn {txn_id}" for txn_id in txn_ids]
            tabs = st.tabs(tab_labels)

            for idx, (tab, txn_id) in enumerate(zip(tabs, txn_ids)):
                with tab:
                    logs = agent_logs[txn_id]

                    # Find transaction details for display
                    transaction_info = None
                    for progress in st.session_state.agent_progress:
                        # Normalize both txn_ids for comparison
                        progress_txn_id = progress.get("transaction", {}).get("txn_id")
                        try:
                            progress_txn_id_int = int(progress_txn_id) if progress_txn_id is not None else None
                        except (ValueError, TypeError):
                            progress_txn_id_int = None

                        if progress_txn_id_int == txn_id:
                            transaction_info = progress.get("transaction", {})
                            break

                    # If not in progress, try to get from database
                    if not transaction_info:
                        try:
                            txn_query = f"SELECT * FROM bank_txn WHERE txn_id = {txn_id} LIMIT 1"
                            txn_result = rewriter.execute(txn_query)
                            txn_rows = txn_result.fetchall()
                            if txn_rows:
                                txn_columns = [desc[0] for desc in txn_result.description]
                                transaction_info = dict(zip(txn_columns, txn_rows[0]))
                        except Exception:
                            pass

                    if transaction_info:
                        st.caption(f"Transaction ID: {txn_id} | Amount: ${transaction_info.get('amount', 'N/A')} | Description: {transaction_info.get('description', 'N/A')}")

                    # Display logs in a fixed-height, scrollable container
                    log_text = "\n".join(logs)
                    st.text_area(
                        "Full Agent Logs",
                        value=log_text,
                        height=400,
                        disabled=True,
                        key=f"logs_{txn_id}_{idx}"
                    )
        else:
            if st.session_state.agent_processing:
                st.info("Processing transactions... Logs will appear here as each transaction completes.")
            else:
                st.info("No agent logs yet. Click 'Propose Taxes' to start processing.")
    else:
        if st.session_state.agent_processing:
            st.info("Processing transactions... Logs will appear here as each transaction completes.")
        else:
            st.info("No agent logs yet. Click 'Propose Taxes' to start processing.")
except Exception as e:
    # Show error if logs can't be loaded
    st.warning(f"Could not load agent logs: {e!s}")


# Process agentic loop if processing
if st.session_state.agent_processing and st.session_state.transactions_to_process:
    try:
        # Check if we have more transactions to process
        if st.session_state.current_txn_index < len(st.session_state.transactions_to_process):
            # Get current transaction
            transaction = st.session_state.transactions_to_process[st.session_state.current_txn_index]
            total_transactions = len(st.session_state.transactions_to_process)
            current_index = st.session_state.current_txn_index + 1

            # Process this transaction
            try:
                bedrock_client = agent.create_bedrock_client()
                recorder = st.session_state.get("llm_recorder")
                replay_manager = st.session_state.get("replay_manager")
                entry_created, message, logs = agent.process_transaction_with_agent(
                    bedrock_client,
                    rewriter,
                    transaction,
                    st.session_state.tax_return_info,
                    recorder=recorder,
                    replay_manager=replay_manager
                )

                # Save logs to database for persistence
                txn_id = transaction.get("txn_id", current_index)
                # Normalize txn_id to int for consistency with database
                try:
                    txn_id_int = int(txn_id)
                except (ValueError, TypeError):
                    txn_id_int = current_index

                try:
                    db.save_agent_logs(rewriter, txn_id_int, logs)
                except Exception as e:
                    st.warning(f"Could not save logs to database: {e!s}")

                # Record progress
                progress_update = {
                    "transaction_index": current_index,
                    "total_transactions": total_transactions,
                    "transaction": transaction,
                    "success": True,
                    "message": message,
                    "entry_created": entry_created
                }
                st.session_state.agent_progress.append(progress_update)

                # Move to next transaction
                st.session_state.current_txn_index += 1

                # Continue processing if not done
                if st.session_state.current_txn_index < len(st.session_state.transactions_to_process):
                    st.rerun()
                else:
                    # All transactions processed - finalize aggregate policies
                    try:
                        violations = rewriter.finalize_aggregate_policies("irs_form")
                        st.session_state.aggregate_policy_violations = violations
                    except Exception as e:
                        st.warning(f"Error finalizing aggregate policies: {e!s}")
                        st.session_state.aggregate_policy_violations = {}

                    st.session_state.agent_processing = False
                    st.rerun()

            except Exception as e:
                # Error processing this transaction
                txn_id = transaction.get("txn_id", current_index)
                # Normalize txn_id to int for consistency with database
                try:
                    txn_id_int = int(txn_id)
                except (ValueError, TypeError):
                    txn_id_int = current_index

                error_logs = [f"[ERROR] {e!s}"]

                # Save error logs to database for persistence
                try:
                    db.save_agent_logs(rewriter, txn_id_int, error_logs)
                except Exception as e2:
                    st.warning(f"Could not save error logs to database: {e2!s}")

                progress_update = {
                    "transaction_index": current_index,
                    "total_transactions": total_transactions,
                    "transaction": transaction,
                    "success": False,
                    "message": f"Error: {e!s}",
                    "entry_created": False,
                    "error": str(e)
                }
                st.session_state.agent_progress.append(progress_update)
                st.session_state.agent_processing = False
        else:
            # All transactions processed - finalize aggregate policies
            try:
                violations = rewriter.finalize_aggregate_policies("irs_form")
                st.session_state.aggregate_policy_violations = violations
            except Exception as e:
                st.warning(f"Error finalizing aggregate policies: {e!s}")
                st.session_state.aggregate_policy_violations = {}

            st.session_state.agent_processing = False

    except Exception as e:
        st.error(f"Error during agentic processing: {e!s}")
        st.session_state.agent_processing = False
