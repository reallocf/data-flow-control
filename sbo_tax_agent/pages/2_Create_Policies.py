"""
Create Policies page for SBO Tax Agent.

Allows defining data flow control policies.
"""

import streamlit as st
from sql_rewriter import DFCPolicy, AggregateDFCPolicy
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

# Get the list of registered policies (both regular and aggregate)
regular_policies = rewriter.get_dfc_policies()
aggregate_policies = rewriter.get_aggregate_policies()

# Combine all policies into a single list with type information
all_policies = []
for policy in regular_policies:
    all_policies.append(('regular', policy))
for policy in aggregate_policies:
    all_policies.append(('aggregate', policy))

def build_policy_text(policy, include_description=False):
    """Build policy string from policy object.
    
    Args:
        policy: The DFCPolicy or AggregateDFCPolicy object
        include_description: If True, include DESCRIPTION in the output (for editing)
    """
    parts = []
    # Add AGGREGATE keyword for aggregate policies
    if isinstance(policy, AggregateDFCPolicy):
        parts.append("AGGREGATE")
    if policy.source:
        parts.append(f"SOURCE {policy.source}")
    if policy.sink:
        parts.append(f"SINK {policy.sink}")
    parts.append(f"CONSTRAINT {policy.constraint}")
    parts.append(f"ON FAIL {policy.on_fail.value}")
    if include_description and policy.description:
        parts.append(f"DESCRIPTION {policy.description}")
    return ' '.join(parts)

if not all_policies:
    st.info("No policies registered yet. Create a policy using the form below.")
else:
    # Display all policies together
    for idx, (policy_type, policy) in enumerate(all_policies):
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
                        # Parse the new policy - determine type from the text
                        edited_text_normalized = edited_text.strip().upper()
                        is_aggregate = edited_text_normalized.startswith('AGGREGATE') or ' AGGREGATE ' in edited_text_normalized
                        
                        if is_aggregate:
                            new_policy = AggregateDFCPolicy.from_policy_str(edited_text)
                        else:
                            new_policy = DFCPolicy.from_policy_str(edited_text)
                        
                        # Get all current policies before making changes
                        all_regular = rewriter.get_dfc_policies()
                        all_aggregate = rewriter.get_aggregate_policies()
                        
                        # Build list of all policies with their types
                        all_current = []
                        for p in all_regular:
                            all_current.append(('regular', p))
                        for p in all_aggregate:
                            all_current.append(('aggregate', p))
                        
                        # Get policies that come after the one we're editing
                        policies_after = all_current[idx + 1:] if idx + 1 < len(all_current) else []
                        
                        # Delete policies from the end backwards to avoid index shifting issues
                        # First delete all policies that come after (in reverse order)
                        for p_type, p in reversed(policies_after):
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
                            for p_type, p in policies_after:
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

# Policy type selector
policy_type = st.radio(
    "Policy Type",
    ["Regular", "Aggregate"],
    help="Regular policies evaluate constraints over a single data flow. Aggregate policies evaluate constraints over all data flows."
)

if policy_type == "Regular":
    st.markdown("**Example regular policy:**")
    st.code("SOURCE bank_txn SINK irs_form CONSTRAINT min(bank_txn.amount) > -1000 ON FAIL REMOVE", language=None)
    placeholder = "SOURCE bank_txn SINK irs_form CONSTRAINT min(bank_txn.amount) > -1000 ON FAIL REMOVE"
    help_text = "Enter policy in the format: SOURCE <source> SINK <sink> CONSTRAINT <constraint> ON FAIL <on_fail>\nFields can be separated by any whitespace (spaces, tabs, newlines)."
else:
    st.markdown("**Example aggregate policy:**")
    st.code("AGGREGATE SOURCE bank_txn SINK irs_form CONSTRAINT min(min(bank_txn.amount)) > 1000 ON FAIL INVALIDATE", language=None)
    placeholder = "AGGREGATE SOURCE bank_txn SINK irs_form CONSTRAINT min(min(bank_txn.amount)) > 1000 ON FAIL INVALIDATE"
    help_text = "Enter aggregate policy in the format: AGGREGATE SOURCE <source> SINK <sink> CONSTRAINT <constraint> ON FAIL <on_fail>\nAggregate policies currently only support INVALIDATE resolution.\nFields can be separated by any whitespace (spaces, tabs, newlines)."

policy_text = st.text_area(
    "Policy Definition",
    placeholder=placeholder,
    help=help_text,
    height=150
)

if st.button("Create Policy"):
    if not policy_text.strip():
        st.error("Please enter policy text")
    else:
        try:
            # Parse and create the policy from string
            if policy_type == "Aggregate":
                policy = AggregateDFCPolicy.from_policy_str(policy_text)
            else:
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

