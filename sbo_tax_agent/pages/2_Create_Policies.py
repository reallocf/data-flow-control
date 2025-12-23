"""
Create Policies page for SBO Tax Agent.

Allows defining data flow control policies.
"""

import streamlit as st
from sql_rewriter import DFCPolicy, Resolution
import db
import re
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


def parse_policy_text(text: str) -> tuple[str | None, str | None, str, Resolution]:
    """Parse policy text in the format:
    
    SOURCE <source> SINK <sink> CONSTRAINT <constraint> ON FAIL <on_fail>
    
    Fields can be separated by any whitespace (spaces, tabs, newlines).
    The constraint value can contain spaces.
    
    Args:
        text: The policy text to parse
        
    Returns:
        Tuple of (source, sink, constraint, on_fail)
        
    Raises:
        ValueError: If the text cannot be parsed
    """
    if not text or not text.strip():
        raise ValueError("Policy text is empty")
    
    # Normalize whitespace: replace all whitespace sequences with single spaces
    normalized = re.sub(r'\s+', ' ', text.strip())
    
    source = None
    sink = None
    constraint = None
    on_fail = None
    
    # Find positions of all keywords (case-insensitive)
    # Handle "ON FAIL" as a special case since it's two words
    keyword_positions = []
    
    # Find single-word keywords
    for keyword in ['SOURCE', 'SINK', 'CONSTRAINT']:
        pattern = r'\b' + re.escape(keyword) + r'\b'
        for match in re.finditer(pattern, normalized, re.IGNORECASE):
            keyword_positions.append((match.start(), keyword.upper()))
    
    # Find "ON FAIL" (two words)
    for match in re.finditer(r'\bON\s+FAIL\b', normalized, re.IGNORECASE):
        keyword_positions.append((match.start(), 'ON FAIL'))
    
    # Sort by position
    keyword_positions.sort()
    
    # Extract values between keywords
    for i, (pos, keyword) in enumerate(keyword_positions):
        # Find the start of the value (after the keyword and whitespace)
        if keyword == 'ON FAIL':
            value_start = pos + 7  # "ON FAIL" is 7 characters
        else:
            value_start = pos + len(keyword)
        # Skip whitespace after keyword
        while value_start < len(normalized) and normalized[value_start] == ' ':
            value_start += 1
        
        # Find the end of the value (start of next keyword or end of string)
        if i + 1 < len(keyword_positions):
            value_end = keyword_positions[i + 1][0]
            # Back up to remove trailing whitespace
            while value_end > value_start and normalized[value_end - 1] == ' ':
                value_end -= 1
        else:
            value_end = len(normalized)
        
        value = normalized[value_start:value_end].strip()
        
        if keyword == 'SOURCE':
            if value and value.upper() != 'NONE':
                source = value
            else:
                source = None
        elif keyword == 'SINK':
            if value and value.upper() != 'NONE':
                sink = value
            else:
                sink = None
        elif keyword == 'CONSTRAINT':
            constraint = value
        elif keyword == 'ON FAIL':
            try:
                on_fail = Resolution(value.upper())
            except ValueError:
                raise ValueError(
                    f"Invalid ON FAIL value '{value}'. Must be 'REMOVE' or 'KILL'"
                )
    
    # Validate required fields
    if constraint is None:
        raise ValueError("CONSTRAINT is required but not found in policy text")
    
    if on_fail is None:
        raise ValueError("ON FAIL is required but not found in policy text")
    
    if source is None and sink is None:
        raise ValueError("Either SOURCE or SINK must be provided")
    
    return source, sink, constraint, on_fail


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
st.code("SOURCE bank_txn SINK schedule_c_review CONSTRAINT bank_txn.amount > 0 ON FAIL REMOVE", language=None)

policy_text = st.text_area(
    "Policy Definition",
    placeholder="SOURCE bank_txn SINK schedule_c_review CONSTRAINT bank_txn.amount > 0 ON FAIL REMOVE",
    help="Enter policy in the format: SOURCE <source> SINK <sink> CONSTRAINT <constraint> ON FAIL <on_fail>\nFields can be separated by any whitespace (spaces, tabs, newlines).",
    height=150
)

if st.button("Create Policy"):
    if not policy_text.strip():
        st.error("Please enter policy text")
    else:
        try:
            # Parse the policy text
            source, sink, constraint, on_fail = parse_policy_text(policy_text)
            
            # Create the policy (this will validate syntax)
            policy = DFCPolicy(
                constraint=constraint,
                on_fail=on_fail,
                source=source,
                sink=sink,
            )
            
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

