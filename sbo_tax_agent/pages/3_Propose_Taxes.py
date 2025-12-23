"""
Propose Taxes page for SBO Tax Agent.

Generates tax proposals based on uploaded data and policies.
Uses an agentic loop with AWS Bedrock to analyze transactions.
"""

import streamlit as st
import pandas as pd
import db
import agent


st.set_page_config(
    page_title="Propose Taxes - SBO Tax Agent",
    layout="wide"
)

st.header("Propose Taxes")

# Get database connection
try:
    rewriter = db.get_db_connection()
except Exception as e:
    st.error(f"Failed to connect to database: {str(e)}")
    st.stop()

# Initialize session state for agent processing
if 'agent_processing' not in st.session_state:
    st.session_state.agent_processing = False
if 'agent_progress' not in st.session_state:
    st.session_state.agent_progress = []
if 'current_return_id' not in st.session_state:
    st.session_state.current_return_id = None
if 'transactions_to_process' not in st.session_state:
    st.session_state.transactions_to_process = []
if 'current_txn_index' not in st.session_state:
    st.session_state.current_txn_index = 0
if 'tax_return_info' not in st.session_state:
    st.session_state.tax_return_info = None

# Get first tax return
try:
    tax_return_query = "SELECT * FROM tax_return ORDER BY return_id LIMIT 1"
    tax_return_result = rewriter.execute(tax_return_query)
    tax_return_rows = tax_return_result.fetchall()
    
    if not tax_return_rows:
        st.warning("No tax return data found. Please upload tax return data on the Upload Data page.")
        st.stop()
    
    tax_return_columns = [desc[0] for desc in tax_return_result.description]
    tax_return_info = dict(zip(tax_return_columns, tax_return_rows[0]))
    return_id = tax_return_info['return_id']
    st.session_state.current_return_id = return_id
    
    st.info(f"Processing tax return for: {tax_return_info.get('business_name', 'N/A')} (Tax Year: {tax_return_info.get('tax_year', 'N/A')})")
    
except Exception as e:
    st.error(f"Error loading tax return: {str(e)}")
    st.stop()

# Create two columns for tables
col1, col2 = st.columns(2)

# Left column: Bank transactions
with col1:
    st.subheader("Bank Transactions")
    try:
        bank_txn_query = f"""
            SELECT * FROM bank_txn 
            WHERE return_id = {return_id}
            ORDER BY txn_date, txn_id
        """
        bank_txn_result = rewriter.execute(bank_txn_query)
        bank_txn_rows = bank_txn_result.fetchall()
        bank_txn_columns = [desc[0] for desc in bank_txn_result.description]
        
        if bank_txn_rows:
            bank_txn_df = pd.DataFrame(bank_txn_rows, columns=bank_txn_columns)
            st.dataframe(bank_txn_df, use_container_width=True, hide_index=True)
            st.caption(f"Total transactions: {len(bank_txn_df)}")
        else:
            st.info("No bank transactions found for this tax return.")
    except Exception as e:
        st.error(f"Error loading bank transactions: {str(e)}")

# Right column: Schedule C Review
with col2:
    st.subheader("Schedule C Review")
    try:
        schedule_c_query = f"""
            SELECT * FROM schedule_c_review 
            WHERE return_id = {return_id}
            ORDER BY txn_date, review_id
        """
        schedule_c_result = rewriter.execute(schedule_c_query)
        schedule_c_rows = schedule_c_result.fetchall()
        schedule_c_columns = [desc[0] for desc in schedule_c_result.description]
        
        if schedule_c_rows:
            schedule_c_df = pd.DataFrame(schedule_c_rows, columns=schedule_c_columns)
            st.dataframe(schedule_c_df, use_container_width=True, hide_index=True)
            st.caption(f"Total entries: {len(schedule_c_df)}")
        else:
            st.info("No schedule C review entries yet. Click 'Propose Taxes' to start processing.")
    except Exception as e:
        st.error(f"Error loading schedule C review: {str(e)}")

# Progress section
if st.session_state.agent_processing:
    st.divider()
    st.subheader("Processing Status")
    
    if st.session_state.agent_progress:
        latest = st.session_state.agent_progress[-1]
        
        if 'error' in latest:
            st.error(f"Error: {latest['error']}")
            st.session_state.agent_processing = False
        else:
            progress_pct = (latest['transaction_index'] / latest['total_transactions']) * 100
            st.progress(progress_pct / 100)
            st.write(f"Processing transaction {latest['transaction_index']} of {latest['total_transactions']}")
            st.write(f"Status: {latest['message']}")
            
            if latest['entry_created']:
                st.success(f"✓ Created schedule_c_review entry for transaction {latest['transaction']['txn_id']}")
            else:
                st.info(f"○ Skipped transaction {latest['transaction']['txn_id']} (not a business expense)")

# Propose Taxes button
st.divider()

if st.button("Propose Taxes", type="primary", disabled=st.session_state.agent_processing):
    if return_id is None:
        st.error("No tax return selected")
    else:
        try:
            # Get tax return info
            tax_return_query = f"SELECT * FROM tax_return WHERE return_id = {return_id}"
            tax_return_result = rewriter.execute(tax_return_query)
            tax_return_rows = tax_return_result.fetchall()
            
            if not tax_return_rows:
                st.error(f"No tax return found with return_id {return_id}")
            else:
                tax_return_columns = [desc[0] for desc in tax_return_result.description]
                tax_return_info = dict(zip(tax_return_columns, tax_return_rows[0]))
                
                # Get all transactions
                transactions_query = f"""
                    SELECT * FROM bank_txn 
                    WHERE return_id = {return_id}
                    ORDER BY txn_date, txn_id
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
                st.session_state.current_return_id = return_id
                st.session_state.tax_return_info = tax_return_info
                st.session_state.transactions_to_process = transactions
                st.session_state.current_txn_index = 0
                st.rerun()
        except Exception as e:
            st.error(f"Error initializing processing: {str(e)}")

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
                entry_created, message = agent.process_transaction_with_agent(
                    bedrock_client,
                    rewriter,
                    transaction,
                    st.session_state.tax_return_info
                )
                
                # Record progress
                progress_update = {
                    'transaction_index': current_index,
                    'total_transactions': total_transactions,
                    'transaction': transaction,
                    'success': True,
                    'message': message,
                    'entry_created': entry_created
                }
                st.session_state.agent_progress.append(progress_update)
                
                # Move to next transaction
                st.session_state.current_txn_index += 1
                
                # Continue processing if not done
                if st.session_state.current_txn_index < len(st.session_state.transactions_to_process):
                    st.rerun()
                else:
                    st.session_state.agent_processing = False
                    
            except Exception as e:
                # Error processing this transaction
                progress_update = {
                    'transaction_index': current_index,
                    'total_transactions': total_transactions,
                    'transaction': transaction,
                    'success': False,
                    'message': f"Error: {str(e)}",
                    'entry_created': False,
                    'error': str(e)
                }
                st.session_state.agent_progress.append(progress_update)
                st.session_state.agent_processing = False
        else:
            # All transactions processed
            st.session_state.agent_processing = False
                        
    except Exception as e:
        st.error(f"Error during agentic processing: {str(e)}")
        st.session_state.agent_processing = False
