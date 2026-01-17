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


# Display registered policies
st.subheader("Registered Policies")

# Initialize session state for editing
if 'editing_policy_index' not in st.session_state:
    st.session_state.editing_policy_index = None
if 'editing_policy_text' not in st.session_state:
    st.session_state.editing_policy_text = ""

# Get the list of registered policies
policies = rewriter.get_dfc_policies()

def build_policy_text(policy, include_description=False):
    """Build policy string from policy object.
    
    Args:
        policy: The DFCPolicy object
        include_description: If True, include DESCRIPTION in the output (for editing)
    """
    parts = []
    if policy.source:
        parts.append(f"SOURCE {policy.source}")
    if policy.sink:
        parts.append(f"SINK {policy.sink}")
    parts.append(f"CONSTRAINT {policy.constraint}")
    parts.append(f"ON FAIL {policy.on_fail.value}")
    if include_description and policy.description:
        parts.append(f"DESCRIPTION {policy.description}")
    return ' '.join(parts)

if not policies:
    st.info("No policies registered yet. Create a policy using the form below.")
else:
    for idx, policy in enumerate(policies):
        # Build policy string for display (without description, shown separately)
        policy_text = build_policy_text(policy, include_description=False)
        
        # Check if this policy is being edited
        if st.session_state.editing_policy_index == idx:
            # Show editable text area
            edited_text = st.text_area(
                f"Editing Policy {idx + 1}",
                value=st.session_state.editing_policy_text,
                key=f"edit_text_{idx}",
                height=100
            )
            
            col1, col2 = st.columns([1, 10])
            with col1:
                if st.button("✓", key=f"confirm_{idx}"):
                    try:
                        # Parse the new policy
                        new_policy = DFCPolicy.from_policy_str(edited_text)
                        
                        # Get all current policies before making changes
                        all_policies = rewriter.get_dfc_policies()
                        
                        # To maintain position: delete all policies from this position onwards,
                        # then re-register them with the new policy at the correct position
                        
                        # Get policies that come after the one we're editing
                        policies_after = all_policies[idx + 1:] if idx + 1 < len(all_policies) else []
                        
                        # Delete policies from the end backwards to avoid index shifting issues
                        # First delete all policies that come after (in reverse order)
                        for p in reversed(policies_after):
                            rewriter.delete_policy(
                                source=p.source,
                                sink=p.sink,
                                constraint=p.constraint,
                                on_fail=p.on_fail,
                                description=p.description
                            )
                        
                        # Delete the old policy
                        deleted = rewriter.delete_policy(
                            source=policy.source,
                            sink=policy.sink,
                            constraint=policy.constraint,
                            on_fail=policy.on_fail,
                            description=policy.description
                        )
                        
                        if not deleted:
                            st.error("Failed to delete old policy")
                        else:
                            # Register the new policy (this puts it at the end, which is position idx now)
                            rewriter.register_policy(new_policy)
                            
                            # Re-register the policies that came after (in original order)
                            for p in policies_after:
                                rewriter.register_policy(p)
                            
                            st.session_state.editing_policy_index = None
                            st.session_state.editing_policy_text = ""
                            st.success("Policy updated successfully!")
                            st.rerun()
                            
                    except ValueError as e:
                        st.error(f"Parsing error: {str(e)}")
                    except Exception as e:
                        st.error(f"Error updating policy: {str(e)}")
            
            with col2:
                if st.button("✗", key=f"cancel_{idx}"):
                    st.session_state.editing_policy_index = None
                    st.session_state.editing_policy_text = ""
                    st.rerun()
        else:
            # Display policy with edit button
            col1, col2 = st.columns([1, 20])
            with col1:
                if st.button("✏️", key=f"edit_{idx}"):
                    st.session_state.editing_policy_index = idx
                    st.session_state.editing_policy_text = build_policy_text(policy, include_description=True)
                    st.rerun()
            
            with col2:
                # Display policy with description if available
                if policy.description:
                    st.markdown(f"- **{policy.description}**")
                    st.markdown(f"  `{policy_text}`")
                else:
                    st.markdown(f"- {policy_text}")

# Policy creation form
st.subheader("Create New Policy")

st.markdown("**Example policy:**")
st.code("SOURCE bank_txn SINK irs_form CONSTRAINT sum(bank_txn.amount) > 0 ON FAIL REMOVE", language=None)

policy_text = st.text_area(
    "Policy Definition",
    placeholder="SOURCE bank_txn SINK irs_form CONSTRAINT sum(bank_txn.amount) > 0 ON FAIL REMOVE",
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
            AND table_name != 'agent_logs'
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
        st.dataframe(df, width='stretch', hide_index=True)
    else:
        st.info("No tables found in the database.")
except Exception as e:
    st.warning(f"Could not load database schema: {str(e)}")

