"""
Agent module for SBO Tax Agent.

Handles agentic loop using AWS Bedrock to analyze bank transactions
and generate IRS Form review entries.
"""

from collections.abc import Iterator
import json
import os
from typing import Any

import boto3
from botocore.exceptions import BotoCoreError, ClientError
import sqlglot
from sqlglot import exp

BEDROCK_MODEL_ID = "us.anthropic.claude-haiku-4-5-20251001-v1:0"


def format_value_for_prompt(value):
    """Format a value for inclusion in a prompt string.

    Handles dates, None values, and other types.

    Args:
        value: Value to format

    Returns:
        str: Formatted value
    """
    if value is None:
        return "N/A"
    if hasattr(value, "strftime"):  # Date/datetime object
        return value.strftime("%Y-%m-%d")
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
        region = os.environ.get("AWS_REGION", "us-east-2")

        # boto3 will automatically use AWS_BEARER_TOKEN_BEDROCK if set
        # No special configuration needed - boto3 checks this env var automatically
        return boto3.client(
            service_name="bedrock-runtime",
            region_name=region
        )
    except Exception as e:
        raise Exception(f"Failed to create Bedrock client: {e!s}") from e


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
            # Check for disallowed statements
            try:
                parsed = sqlglot.parse_one(sql_query, read="duckdb")

                # Check if trying to create a table
                if isinstance(parsed, exp.Create):
                    # Verify it's a CREATE TABLE by checking the SQL output
                    create_sql = parsed.sql(dialect="duckdb").upper()
                    if "CREATE TABLE" in create_sql or create_sql.startswith("CREATE TABLE"):
                        return json.dumps({
                            "success": False,
                            "error": "Creating new tables is not allowed. You can only query existing tables and insert into existing tables."
                        }, indent=2)

                # Check if INSERT statement without SELECT
                if isinstance(parsed, exp.Insert):
                    # Check if INSERT has a SELECT (not just VALUES)
                    select_expr = parsed.find(exp.Select)
                    if not select_expr:
                        return json.dumps({
                            "success": False,
                            "error": "INSERT statements must include a SELECT statement. Use INSERT INTO table (columns...) SELECT ... FROM ... WHERE ... format."
                        }, indent=2)
            except Exception as e:
                # If parsing fails, continue with execution (let rewriter handle it)
                print(f"Parsing failed: {e}")

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


def build_agent_prompt(transaction: dict[str, Any], tax_return_info: dict[str, Any]) -> str:
    """Build the fixed prompt for the agent.

    Args:
        transaction: Dictionary with transaction details
        tax_return_info: Dictionary with tax return information

    Returns:
        str: Formatted prompt string
    """
    # Format values for prompt
    amount = transaction.get("amount", 0)
    txn_id = transaction.get("txn_id")

    return f"""You are a tax agent analyzing bank transactions to identify business expenses and income.

TAX RETURN CONTEXT:
- Business Name: {format_value_for_prompt(tax_return_info.get('business_name'))}
- Business Description: {format_value_for_prompt(tax_return_info.get('business_desc'))}
- Tax Year: {format_value_for_prompt(tax_return_info.get('tax_year'))}

CURRENT TRANSACTION:
- Transaction ID: {format_value_for_prompt(txn_id)}
- Amount: {format_value_for_prompt(amount)}
- Description: {format_value_for_prompt(transaction.get('description'))}

BANK_TXN TABLE SCHEMA:
- txn_id (UBIGINT): Unique transaction identifier
- amount (DOUBLE): Transaction amount
- category (VARCHAR): Transaction category
- description (VARCHAR): Transaction description text

IRS_FORM TABLE SCHEMA:
- txn_id (UBIGINT): The transaction ID from bank_txn
- amount (DOUBLE): Transaction amount (use absolute value)
- kind (VARCHAR): Type of transaction (e.g., "Expense", "Income")
- business_use_pct (DOUBLE): Percentage of the transaction that is deductible (0.0 to 100.0)

INSTRUCTIONS:
1. Analyze the transaction to determine if it's business-related:
   - POSITIVE amounts (payments received) are typically INCOME
   - NEGATIVE amounts (payments made) are typically EXPENSES
2. If the transaction is business-related:
   - Use the database tool to INSERT a row into irs_form data FROM bank_txn
   - Set kind = Income|Expense
   - Set amount = ABS(transaction amount)
   - Set business_use_pct to a value between 0 and 100 based on how much is business-related
   - Refer directly to the bank_txn columns in the SELECT statement where appropriate
3. If the transaction is NOT business-related, insert a business_use_pct of 0.
4. Only insert one row per transaction.
5. Consider meals as 100% business use unless told otherwise.

You have access to a database tool. Use it to:
- Query existing data if needed: SELECT * FROM table_name WHERE conditions
- Insert new irs_form entries: INSERT INTO irs_form (irs_form columns...) SELECT (bank_txn columns...) FROM bank_txn WHERE conditions

Analyze this transaction and take appropriate action."""


def process_transaction_with_agent(
    bedrock_client,
    rewriter,
    transaction: dict[str, Any],
    tax_return_info: dict[str, Any],
    recorder=None,
    replay_manager=None
) -> tuple[bool, str, list]:
    """Process a single transaction with the agent.

    Uses AWS Bedrock to analyze a bank transaction and determine if it's a business
    expense or income. The agent can query the database and insert entries into the
    irs_form table. Supports recording and replaying LLM interactions.

    Args:
        bedrock_client: Boto3 Bedrock Runtime client (used if replay_manager is None or
                       no recorded response is found)
        rewriter: SQLRewriter instance for database access
        transaction: Transaction dictionary with transaction details
        tax_return_info: Tax return information dictionary
        recorder: Optional LLMRecorder instance for recording LLM requests/responses
        replay_manager: Optional ReplayManager instance for replaying recorded responses
                       instead of calling the LLM

    Returns:
        Tuple[bool, str, list]:
            - success: True if an irs_form entry was created, False otherwise
            - message: Human-readable message describing the result
            - logs: List of log strings from the agent interaction

    Note:
        If replay_manager is provided and enabled, the function will attempt to use
        recorded responses instead of calling Bedrock. If no recorded response is found,
        it falls back to calling the actual LLM. All interactions are logged, and if
        recorder is provided, new interactions are also recorded.
    """
    logs = []

    # Get transaction ID for recording
    transaction_id = transaction.get("txn_id")

    def log(msg: str):
        """Add a log message and also print it."""
        logs.append(msg)
        # print(msg)  # Uncomment this to print the logs to the console

    try:
        # Build prompt
        prompt = build_agent_prompt(transaction, tax_return_info)

        log("=" * 80)
        log("AGENTIC LOOP: Starting transaction processing")
        log("=" * 80)
        log("\n[INITIAL USER MESSAGE]")
        log("Role: user")
        log(f"Content:\n{prompt}\n")

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
                "input_schema": {
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

        log("[REQUEST] Sending to Bedrock (iteration 1)")
        log(json.dumps(request_body, indent=2))
        log("")

        # Check if we should replay instead of calling LLM
        if replay_manager and replay_manager.is_enabled():
            response_body = replay_manager.get_agent_loop_response(
                transaction_id=transaction_id,
                iteration=1,
                request_body=request_body
            )
            if response_body is None:
                # Fall through to actual LLM call
                response = bedrock_client.invoke_model(
                    modelId=BEDROCK_MODEL_ID,
                    body=json.dumps(request_body)
                )
                response_body = json.loads(response["body"].read())
                log("[RESPONSE] Received from Bedrock (iteration 1)")
                log(json.dumps(response_body, indent=2))
                log("")
            else:
                log("[RESPONSE] Received from replay (iteration 1)")
                log(json.dumps(response_body, indent=2))
                log("")
        else:
            # Record request if recorder is available
            if recorder and recorder.is_enabled():
                recorder.record_agent_loop_request(
                    transaction_id=transaction_id,
                    iteration=1,
                    request_body=request_body
                )

            response = bedrock_client.invoke_model(
                modelId=BEDROCK_MODEL_ID,
                body=json.dumps(request_body)
            )

            response_body = json.loads(response["body"].read())

            log("[RESPONSE] Received from Bedrock (iteration 1)")
            log(json.dumps(response_body, indent=2))
            log("")

        # Record response if recorder is available (for both replay and live calls)
        if recorder and recorder.is_enabled():
            recorder.record_agent_loop_response(
                transaction_id=transaction_id,
                iteration=1,
                response_body=response_body
            )

        # Process response - handle tool use with conversation loop
        entry_created = False
        max_iterations = 5  # Prevent infinite loops
        iteration = 0

        while iteration < max_iterations:
            iteration += 1

            if "content" not in response_body:
                log(f"[ITERATION {iteration}] No content in response, breaking")
                break

            # Check for tool use
            tool_uses = []
            text_content = []
            for content_block in response_body["content"]:
                if content_block["type"] == "tool_use":
                    tool_uses.append(content_block)
                elif content_block["type"] == "text":
                    text_content.append(content_block.get("text", ""))

            # Print any text content from assistant
            if text_content:
                log(f"[ITERATION {iteration}] Assistant text response:")
                for text in text_content:
                    log(text)
                log("")

            if not tool_uses:
                # No more tool uses, agent is done
                log(f"[ITERATION {iteration}] No tool uses detected, conversation complete")
                break

            log(f"[ITERATION {iteration}] Detected {len(tool_uses)} tool use(s):")
            for tool_use in tool_uses:
                log(f"  - Tool: {tool_use['name']}")
                log(f"    ID: {tool_use['id']}")
                log(f"    Input: {json.dumps(tool_use['input'], indent=4)}")
            log("")

            # Process each tool use
            tool_results = []
            for tool_use in tool_uses:
                tool_name = tool_use["name"]
                tool_input = tool_use["input"]

                if tool_name == "execute_sql":
                    sql_query = tool_input.get("sql_query", "")
                    log("[TOOL EXECUTION] Executing SQL query:")
                    log(f"  {sql_query}")

                    # Execute the tool
                    tool_result = db_tool_func(sql_query)
                    log("[TOOL RESULT]")
                    log(f"  {tool_result}")
                    log("")

                    tool_results.append({
                        "type": "tool_result",
                        "tool_use_id": tool_use["id"],
                        "content": tool_result
                    })

                    # Check if it was an INSERT into irs_form
                    if "INSERT INTO irs_form" in sql_query.upper():
                        entry_created = True

            # Add assistant's tool use to messages
            assistant_message = {
                "role": "assistant",
                "content": list(tool_uses)
            }
            messages.append(assistant_message)
            log("[MESSAGE STREAM] Added assistant message with tool uses")
            log(json.dumps(assistant_message, indent=2))
            log("")

            # Add tool results to messages
            user_message = {
                "role": "user",
                "content": tool_results
            }
            messages.append(user_message)
            log("[MESSAGE STREAM] Added user message with tool results")
            log(json.dumps(user_message, indent=2))
            log("")

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

            log(f"[REQUEST] Sending to Bedrock (iteration {iteration + 1})")
            log(json.dumps(request_body, indent=2))
            log("")

            # Check if we should replay instead of calling LLM
            if replay_manager and replay_manager.is_enabled():
                response_body = replay_manager.get_agent_loop_response(
                    transaction_id=transaction_id,
                    iteration=iteration + 1,
                    request_body=request_body
                )
                if response_body is None:
                    # Fall through to actual LLM call
                    response = bedrock_client.invoke_model(
                        modelId=BEDROCK_MODEL_ID,
                        body=json.dumps(request_body)
                    )
                    response_body = json.loads(response["body"].read())
                    log(f"[RESPONSE] Received from Bedrock (iteration {iteration + 1})")
                    log(json.dumps(response_body, indent=2))
                    log("")
                else:
                    log(f"[RESPONSE] Received from replay (iteration {iteration + 1})")
                    log(json.dumps(response_body, indent=2))
                    log("")
            else:
                # Record request if recorder is available
                if recorder and recorder.is_enabled():
                    recorder.record_agent_loop_request(
                        transaction_id=transaction_id,
                        iteration=iteration + 1,
                        request_body=request_body
                    )

                response = bedrock_client.invoke_model(
                    modelId=BEDROCK_MODEL_ID,
                    body=json.dumps(request_body)
                )

                response_body = json.loads(response["body"].read())

                log(f"[RESPONSE] Received from Bedrock (iteration {iteration + 1})")
                log(json.dumps(response_body, indent=2))
                log("")

            # Record response if recorder is available (for both replay and live calls)
            if recorder and recorder.is_enabled():
                recorder.record_agent_loop_response(
                    transaction_id=transaction_id,
                    iteration=iteration + 1,
                    response_body=response_body
                )

        log("=" * 80)
        log("AGENTIC LOOP: Completed")
        log(f"Entry created: {entry_created}")
        log("=" * 80)
        log("")

        if entry_created:
            return True, "Created irs_form entry", logs
        return False, "Transaction analyzed, no business expense identified", logs

    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code", "Unknown")
        error_msg = e.response.get("Error", {}).get("Message", str(e))
        error_msg_full = f"Bedrock API error ({error_code}): {error_msg}"
        log(f"[ERROR] {error_msg_full}")
        return False, error_msg_full, logs
    except BotoCoreError as e:
        error_msg = f"AWS error: {e!s}"
        log(f"[ERROR] {error_msg}")
        return False, error_msg, logs
    except Exception as e:
        error_msg = f"Error processing transaction: {e!s}"
        log(f"[ERROR] {error_msg}")
        return False, error_msg, logs


def run_agentic_loop(rewriter) -> Iterator[dict[str, Any]]:
    """Run the agentic loop to process all transactions for a tax return.

    Args:
        rewriter: SQLRewriter instance

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

        # Get tax return info (assuming single return)
        tax_return_query = "SELECT * FROM tax_return LIMIT 1"
        tax_return_result = rewriter.execute(tax_return_query)
        tax_return_rows = tax_return_result.fetchall()

        if not tax_return_rows:
            yield {
                "error": "No tax return found",
                "success": False
            }
            return

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

        total_transactions = len(transactions)

        if total_transactions == 0:
            yield {
                "error": "No transactions found",
                "success": False
            }
            return

        # Process each transaction
        for idx, transaction in enumerate(transactions, 1):
            entry_created, message, logs = process_transaction_with_agent(
                bedrock_client,
                rewriter,
                transaction,
                tax_return_info
            )

            yield {
                "transaction_index": idx,
                "total_transactions": total_transactions,
                "transaction": transaction,
                "success": True,
                "message": message,
                "entry_created": entry_created,
                "logs": logs
            }

    except Exception as e:
        yield {
            "error": f"Error in agentic loop: {e!s}",
            "success": False
        }
