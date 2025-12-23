"""
Agent module for SBO Tax Agent.

Handles agentic loop using AWS Bedrock to analyze bank transactions
and generate Schedule C review entries.
"""

import json
import os
from typing import Dict, Any, Iterator, Tuple, Optional
import boto3
from botocore.exceptions import ClientError, BotoCoreError


BEDROCK_MODEL_ID = "anthropic.claude-haiku-4-5-20251001-v1:0"


def format_value_for_prompt(value):
    """Format a value for inclusion in a prompt string.
    
    Handles dates, None values, and other types.
    
    Args:
        value: Value to format
        
    Returns:
        str: Formatted value
    """
    if value is None:
        return 'N/A'
    elif hasattr(value, 'strftime'):  # Date/datetime object
        return value.strftime('%Y-%m-%d')
    else:
        return str(value)


def create_bedrock_client():
    """Initialize boto3 Bedrock Runtime client.
    
    Supports authentication via:
    - AWS_BEARER_TOKEN_BEDROCK environment variable (bearer token)
    - Standard AWS credentials (access key/secret key, IAM role, etc.)
    
    Returns:
        boto3.client: Bedrock Runtime client
        
    Raises:
        Exception: If client creation fails
    """
    try:
        region = os.environ.get("AWS_REGION", "us-east-1")
        
        # Check if bearer token is provided
        bearer_token = os.environ.get("AWS_BEARER_TOKEN_BEDROCK")
        
        # boto3 will automatically use AWS_BEARER_TOKEN_BEDROCK if set
        # No special configuration needed - boto3 checks this env var automatically
        client = boto3.client(
            service_name="bedrock-runtime",
            region_name=region
        )
        return client
    except Exception as e:
        raise Exception(f"Failed to create Bedrock client: {str(e)}")


def create_db_tool(rewriter):
    """Create a function tool that allows the agent to execute SQL queries.
    
    The tool accepts SQL queries and executes them through the SQLRewriter
    to respect DFC policies. Returns results in a structured format.
    
    Args:
        rewriter: SQLRewriter instance
        
    Returns:
        function: Database tool function
    """
    def db_tool(sql_query: str) -> str:
        """Execute a SQL query and return results.
        
        Args:
            sql_query: SQL query to execute (SELECT, INSERT, UPDATE, DELETE)
            
        Returns:
            str: JSON string with query results or error message
        """
        try:
            # Execute query through rewriter (respects DFC policies)
            result = rewriter.execute(sql_query)
            
            # For SELECT queries, fetch and format results
            if sql_query.strip().upper().startswith("SELECT"):
                rows = result.fetchall()
                columns = [desc[0] for desc in result.description] if result.description else []
                
                # Convert to list of dicts for JSON serialization
                data = []
                for row in rows:
                    row_dict = {}
                    for i, col in enumerate(columns):
                        # Convert non-serializable types
                        value = row[i]
                        if value is None:
                            row_dict[col] = None
                        elif isinstance(value, (int, float, str, bool)):
                            row_dict[col] = value
                        else:
                            row_dict[col] = str(value)
                    data.append(row_dict)
                
                return json.dumps({
                    "success": True,
                    "row_count": len(data),
                    "columns": columns,
                    "data": data
                }, indent=2)
            else:
                # For INSERT, UPDATE, DELETE - return success message
                return json.dumps({
                    "success": True,
                    "message": "Query executed successfully"
                }, indent=2)
                
        except Exception as e:
            return json.dumps({
                "success": False,
                "error": str(e)
            }, indent=2)
    
    return db_tool


def build_agent_prompt(transaction: Dict[str, Any], tax_return_info: Dict[str, Any]) -> str:
    """Build the fixed prompt for the agent.
    
    Args:
        transaction: Dictionary with transaction details
        tax_return_info: Dictionary with tax return information
        
    Returns:
        str: Formatted prompt string
    """
    # Format values for prompt
    txn_date_str = format_value_for_prompt(transaction.get('txn_date'))
    amount = transaction.get('amount', 0)
    return_id = tax_return_info.get('return_id')
    txn_id = transaction.get('txn_id')
    
    prompt = f"""You are a tax agent analyzing bank transactions to identify business expenses for Schedule C.

TAX RETURN CONTEXT:
- Business Name: {format_value_for_prompt(tax_return_info.get('business_name'))}
- Business Description: {format_value_for_prompt(tax_return_info.get('business_desc'))}
- Tax Year: {format_value_for_prompt(tax_return_info.get('tax_year'))}

CURRENT TRANSACTION:
- Transaction ID: {format_value_for_prompt(txn_id)}
- Date: {txn_date_str}
- Amount: {format_value_for_prompt(amount)}
- Description: {format_value_for_prompt(transaction.get('description'))}
- Account: {format_value_for_prompt(transaction.get('account_name'))}

SCHEDULE_C_REVIEW TABLE SCHEMA:
- return_id (UBIGINT): The tax return ID
- review_id (UBIGINT): Unique ID for this review entry (you can use txn_id or generate a unique number)
- txn_id (UBIGINT): The transaction ID from bank_txn
- txn_date (DATE): Transaction date
- original_amount (DOUBLE): Original transaction amount (use absolute value for expenses)
- kind (VARCHAR): Type of expense (e.g., "Expense", "Income")
- schedule_c_line (VARCHAR): Schedule C line number (e.g., "27a", "27b", "8", etc.)
- subcategory (VARCHAR): More specific category (e.g., "Office Supplies", "Travel", "Meals")
- business_use_pct (DOUBLE): Percentage of business use (0.0 to 100.0)
- deductible_amount (DOUBLE): Calculated as original_amount * (business_use_pct / 100.0)
- note (VARCHAR): Optional note explaining the classification

INSTRUCTIONS:
1. Analyze the transaction to determine if it's a business expense or income related to the business.
2. If the transaction is a business expense (business_use_pct > 0):
   - Use the database tool to INSERT a row into schedule_c_review
   - Set business_use_pct to a value between 0 and 100 based on how much is business-related
   - Calculate deductible_amount = ABS(original_amount) * (business_use_pct / 100.0)
   - Classify the expense with appropriate kind, schedule_c_line, and subcategory
   - Include a helpful note explaining your reasoning
3. If the transaction is NOT a business expense (business_use_pct = 0), do not insert anything.
4. Only insert one row per transaction.

You have access to a database tool. Use it to:
- Query existing data if needed: SELECT * FROM table_name WHERE conditions
- Insert new schedule_c_review entries: INSERT INTO schedule_c_review (columns...) VALUES (values...)

Example INSERT statement:
INSERT INTO schedule_c_review (return_id, review_id, txn_id, txn_date, original_amount, kind, schedule_c_line, subcategory, business_use_pct, deductible_amount, note)
VALUES ({return_id}, {txn_id}, {txn_id}, '{txn_date_str}', {abs(amount)}, 'Expense', '27a', 'Office Supplies', 100.0, {abs(amount)}, 'Office supplies for business operations')

Analyze this transaction and take appropriate action."""
    
    return prompt


def process_transaction_with_agent(
    bedrock_client,
    rewriter,
    transaction: Dict[str, Any],
    tax_return_info: Dict[str, Any]
) -> Tuple[bool, str]:
    """Process a single transaction with the agent.
    
    Args:
        bedrock_client: Boto3 Bedrock Runtime client
        rewriter: SQLRewriter instance
        transaction: Transaction dictionary
        tax_return_info: Tax return information dictionary
        
    Returns:
        Tuple[bool, str]: (success, message) - success indicates if entry was created
    """
    try:
        # Build prompt
        prompt = build_agent_prompt(transaction, tax_return_info)
        
        # Create database tool
        db_tool_func = create_db_tool(rewriter)
        
        # Prepare messages for Claude
        messages = [
            {
                "role": "user",
                "content": prompt
            }
        ]
        
        # Create tool definition for database access
        tools = [
            {
                "name": "execute_sql",
                "description": "Execute a SQL query on the database. Supports SELECT, INSERT, UPDATE, DELETE operations. Returns results as JSON.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "sql_query": {
                            "type": "string",
                            "description": "The SQL query to execute"
                        }
                    },
                    "required": ["sql_query"]
                }
            }
        ]
        
        # Call Bedrock
        request_body = {
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": 4096,
            "messages": messages,
            "tools": tools,
            "tool_choice": {
                "type": "auto"
            }
        }
        
        response = bedrock_client.invoke_model(
            modelId=BEDROCK_MODEL_ID,
            body=json.dumps(request_body)
        )
        
        response_body = json.loads(response['body'].read())
        
        # Process response - handle tool use with conversation loop
        entry_created = False
        max_iterations = 5  # Prevent infinite loops
        iteration = 0
        
        while iteration < max_iterations:
            iteration += 1
            
            if 'content' not in response_body:
                break
            
            # Check for tool use
            tool_uses = []
            for content_block in response_body['content']:
                if content_block['type'] == 'tool_use':
                    tool_uses.append(content_block)
            
            if not tool_uses:
                # No more tool uses, agent is done
                break
            
            # Process each tool use
            tool_results = []
            for tool_use in tool_uses:
                tool_name = tool_use['name']
                tool_input = tool_use['input']
                
                if tool_name == 'execute_sql':
                    sql_query = tool_input.get('sql_query', '')
                    # Execute the tool
                    tool_result = db_tool_func(sql_query)
                    tool_results.append({
                        "type": "tool_result",
                        "tool_use_id": tool_use['id'],
                        "content": tool_result
                    })
                    
                    # Check if it was an INSERT into schedule_c_review
                    if 'INSERT INTO schedule_c_review' in sql_query.upper():
                        entry_created = True
            
            # Add assistant's tool use to messages
            messages.append({
                "role": "assistant",
                "content": [tool_use for tool_use in tool_uses]
            })
            
            # Add tool results to messages
            messages.append({
                "role": "user",
                "content": tool_results
            })
            
            # Get next response from Bedrock
            request_body = {
                "anthropic_version": "bedrock-2023-05-31",
                "max_tokens": 4096,
                "messages": messages,
                "tools": tools,
                "tool_choice": {
                    "type": "auto"
                }
            }
            
            response = bedrock_client.invoke_model(
                modelId=BEDROCK_MODEL_ID,
                body=json.dumps(request_body)
            )
            
            response_body = json.loads(response['body'].read())
        
        if entry_created:
            return True, "Created schedule_c_review entry"
        else:
            return False, "Transaction analyzed, no business expense identified"
            
    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', 'Unknown')
        error_msg = e.response.get('Error', {}).get('Message', str(e))
        return False, f"Bedrock API error ({error_code}): {error_msg}"
    except BotoCoreError as e:
        return False, f"AWS error: {str(e)}"
    except Exception as e:
        return False, f"Error processing transaction: {str(e)}"


def run_agentic_loop(rewriter, return_id: int) -> Iterator[Dict[str, Any]]:
    """Run the agentic loop to process all transactions for a tax return.
    
    Args:
        rewriter: SQLRewriter instance
        return_id: Tax return ID to process
        
    Yields:
        Dict with progress information: {
            'transaction_index': int,
            'total_transactions': int,
            'transaction': dict,
            'success': bool,
            'message': str,
            'entry_created': bool
        }
    """
    try:
        # Create Bedrock client
        bedrock_client = create_bedrock_client()
        
        # Get tax return info
        tax_return_query = f"SELECT * FROM tax_return WHERE return_id = {return_id}"
        tax_return_result = rewriter.execute(tax_return_query)
        tax_return_rows = tax_return_result.fetchall()
        
        if not tax_return_rows:
            yield {
                'error': f'No tax return found with return_id {return_id}',
                'success': False
            }
            return
        
        tax_return_columns = [desc[0] for desc in tax_return_result.description]
        tax_return_info = dict(zip(tax_return_columns, tax_return_rows[0]))
        
        # Get all transactions for this return_id
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
        
        total_transactions = len(transactions)
        
        if total_transactions == 0:
            yield {
                'error': f'No transactions found for return_id {return_id}',
                'success': False
            }
            return
        
        # Process each transaction
        for idx, transaction in enumerate(transactions, 1):
            entry_created, message = process_transaction_with_agent(
                bedrock_client,
                rewriter,
                transaction,
                tax_return_info
            )
            
            yield {
                'transaction_index': idx,
                'total_transactions': total_transactions,
                'transaction': transaction,
                'success': True,
                'message': message,
                'entry_created': entry_created
            }
            
    except Exception as e:
        yield {
            'error': f'Error in agentic loop: {str(e)}',
            'success': False
        }

