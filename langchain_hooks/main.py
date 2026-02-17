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
from langchain_aws import ChatBedrock
from langchain_core.messages import HumanMessage
from langchain_classic.agents import AgentExecutor
from langchain_classic.agents import create_tool_calling_agent
from langchain_core.prompts import PromptTemplate

# Your local modules
from duckdb_tool import query_duckdb
from hooks import SQLToolCallback
from test_beadrock import get_llm, main


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

        print("✓ LangChain test successful")
        print("Response:", response.content.strip(), "\n")
        return True

    except Exception:
        print("❌ LangChain test failed")
        print(traceback.format_exc())
        return False


# ============================================================
# AGENT EXECUTION
# ============================================================

def run_agent():
    print("Running full agent...\n")

    llm = get_llm()
    tools = [query_duckdb]

    callback_handler = SQLToolCallback()

    agent = initialize_agent(
        tools=tools,
        llm=llm,
        agent=AgentType.ZERO_SHOT_REACT_DESCRIPTION,
        verbose=True,
        callbacks=[callback_handler],
        agent_kwargs={"callbacks": [callback_handler],"stop_sequences": None}   # 🔥 Attach here
    )

    response = agent.invoke(
        {"input": "delete id from transactions the table has id, year, revenue, expense"},
        config={"callbacks": [callback_handler]}  # 🔥 Ensures full propagation
    )

    print("\nFinal Agent Output:")
    print(response["output"])

run_agent()