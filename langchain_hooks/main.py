#!/usr/bin/env python3

"""
Main Entry Point

Includes:
1. AWS environment validation
2. Direct Bedrock boto3 connectivity test (Qwen)
3. LangChain ChatBedrock test
4. Full Agent execution (with hooks)
"""

import json
import os
import sys
import traceback

import boto3
from botocore.exceptions import BotoCoreError, ClientError
from langchain_classic.agents import initialize_agent, AgentType

# LangChain imports
from langchain_core.messages import HumanMessage
from langchain_classic.agents import AgentExecutor

# Your local modules
from duckdb_tool import query_duckdb
from hooks import SQLToolCallback
from test_beadrock import get_llm


# ============================================================
# CONFIG
# ============================================================

BEDROCK_MODEL_ID = "arn:aws:bedrock:us-east-1:920736616554:application-inference-profile/nix3w87nd0mc"


# ============================================================
# ENVIRONMENT + BASIC CONNECTIVITY
# ============================================================


# ============================================================
# LangChain Test
# ============================================================

def test_langchain_llm():
    print("Testing LangChain ChatBedrock (Qwen)...")

    try:
        llm = get_llm()

        response = llm.invoke(
            [
                HumanMessage(content="Say 'LangChain OK' and nothing else."),
            ]
        )

        print("[OK] LangChain test successful")
        print("Response:", response.content.strip(), "\n")
        return True

    except Exception:
        print("[FAIL] LangChain test failed")
        print(traceback.format_exc())
        return False


# ============================================================
# AGENT EXECUTION
# ============================================================

def run_agent():
    """Run the SQL agent with ChatBedrockConverse, hooks, and DuckDB tool."""
    print("Running SQL Agent with ChatBedrockConverse...\n")

    # Get LLM
    llm = get_llm()
    print("[OK] LLM initialized (ChatBedrockConverse)\n")

    # Define tools
    tools = [query_duckdb]
    print(f"[OK] Tools available: {[tool.name for tool in tools]}\n")

    # Create system prompt
    system_prompt = """You are a SQL expert assistant. Your task is to help users query a DuckDB database.

Guidelines:
1. Always use the query_duckdb tool to execute SQL queries.
2. First, inspect the database schema if you don't know the tables:
   - Run: SHOW TABLES;
   - Then: DESCRIBE <table_name>;
3. Only execute SELECT queries - no INSERT, UPDATE, DELETE, CREATE, ALTER, or DROP.
4. Explain your findings clearly to the user.
5. If something goes wrong, ask the user for clarification."""

    # Create callbacks
    callback_handler = SQLToolCallback()

    # Use older initialize_agent API for better non-streaming support
    agent = initialize_agent(
        tools=tools,
        llm=llm,
        agent=AgentType.ZERO_SHOT_REACT_DESCRIPTION,
        verbose=True,
        callbacks=[callback_handler],
        max_iterations=5,
        agent_kwargs={
            "prefix": system_prompt,
        }
    )
    print("[OK] Agent created using initialize_agent\n")

    # Run the agent
    user_input = "show all data from transactions?"
    
    print("=" * 60)
    print(f"USER: {user_input}")
    print("=" * 60)
    print("(Tool calls will appear as [TOOL] START / [TOOL] END below if the model uses query_duckdb)\n")

    try:
        # Pass callbacks at run time so they propagate to tool execution (on_tool_start/on_tool_end)
        result = agent.invoke(
            {"input": user_input},
            config={"callbacks": [callback_handler]},
        )
        output = result.get("output", "") if isinstance(result, dict) else str(result)
    except Exception as e:
        output = f"Error: {str(e)}"

    print("\n" + "=" * 60)
    print(f"ASSISTANT: {output}")
    print("=" * 60)


# ============================================================
# Main Entry
# ============================================================

if __name__ == "__main__":
    run_agent()