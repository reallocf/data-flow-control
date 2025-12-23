"""
Create Policies page for SBO Tax Agent.

Allows defining data flow control policies.
"""

import streamlit as st
from sql_rewriter import DFCPolicy
import db
import pandas as pd

st.set_page_config(
    page_title="Create Policies - SBO Tax Agent",
    layout="wide"
)

st.header("Create Policies")

# Get database connection
try:
    rewriter = db.get_db_connection()
except Exception as e:
    st.error(f"Failed to connect to database: {str(e)}")
    st.stop()


# Display database schema
st.subheader("Database Schema")

try:
    # Query all tables and their columns from information_schema
    schema_query = """
        SELECT 
            table_name,
            column_name,
            data_type,
            ordinal_position
        FROM information_schema.columns
        WHERE table_schema = 'main'
        ORDER BY table_name, ordinal_position
    """
    
    result = rewriter.conn.execute(schema_query).fetchall()
    
    if result:
        # Group columns by table
        schema_data = {}
        for table_name, column_name, data_type, ordinal_position in result:
            if table_name not in schema_data:
                schema_data[table_name] = []
            schema_data[table_name].append(f"{column_name} ({data_type})")
        
        # Create dataframe with table names and their columns
        df_data = []
        for table_name in sorted(schema_data.keys()):
            columns_str = ", ".join(schema_data[table_name])
            df_data.append({
                "Table": table_name,
                "Columns": columns_str
            })
        
        df = pd.DataFrame(df_data)
        st.dataframe(df, use_container_width=True, hide_index=True)
    else:
        st.info("No tables found in the database.")
except Exception as e:
    st.warning(f"Could not load database schema: {str(e)}")

# Policy creation form
st.subheader("Create New Policy")

st.markdown("**Example policy:**")
st.code("SOURCE bank_txn SINK schedule_c_review CONSTRAINT sum(bank_txn.amount) > 0 ON FAIL REMOVE", language=None)

policy_text = st.text_area(
    "Policy Definition",
    placeholder="SOURCE bank_txn SINK schedule_c_review CONSTRAINT sum(bank_txn.amount) > 0 ON FAIL REMOVE",
    help="Enter policy in the format: SOURCE <source> SINK <sink> CONSTRAINT <constraint> ON FAIL <on_fail>\nFields can be separated by any whitespace (spaces, tabs, newlines).",
    height=150
)

if st.button("Create Policy"):
    if not policy_text.strip():
        st.error("Please enter policy text")
    else:
        try:
            # Parse and create the policy from string
            policy = DFCPolicy.from_policy_str(policy_text)
            
            # Register the policy (this will validate against database)
            rewriter.register_policy(policy)
            
            st.success("Policy created and registered successfully!")
            st.rerun()
            
        except ValueError as e:
            st.error(f"Parsing error: {str(e)}")
        except Exception as e:
            st.error(f"Unexpected error: {str(e)}")

# Display registered policies
st.subheader("Registered Policies")

# Get the list of registered policies
policies = rewriter.get_dfc_policies()

if not policies:
    st.info("No policies registered yet. Create a policy using the form above.")
else:
    for idx, policy in enumerate(policies, 1):
        with st.expander(f"Policy {idx}", expanded=False):
            # Build policy string on one line, only including SOURCE/SINK if they exist
            parts = []
            if policy.source:
                parts.append(f"SOURCE {policy.source}")
            if policy.sink:
                parts.append(f"SINK {policy.sink}")
            parts.append(f"CONSTRAINT {policy.constraint}")
            parts.append(f"ON FAIL {policy.on_fail.value}")
            st.text(" ".join(parts))

