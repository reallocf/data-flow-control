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
from langchain_core.prompts import ChatPromptTemplate
from langchain.agents import create_agent
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

    template = '''You have access to the following tools:
{tools}

Use the following format:

Question: the input question you must answer
Thought: you should always think about what to do
Action: the action to take, should be one of [{tool_names}]
Action Input: the input to the action, MUST be valid JSON format
Observation: the result of the action
... (this Thought/Action/Action Input/Observation can repeat N times)
Thought: I now know the final answer
Final Answer: the final answer to the original input question

Rules:
- Use the tool to query DuckDB whenever you need real data.
- Never guess table names. If unsure, first inspect schema using information_schema 
  (e.g., select table_name from information_schema.tables).
- Write operations are forbidden. Do not attempt DELETE, UPDATE, or INSERT.

'''
    prompt = ChatPromptTemplate.from_messages(template)
    agent = create_agent(
        llm=llm,
        tools=tools,
        prompt=prompt,
    )

    executor = AgentExecutor(
        agent=agent,
        tools=tools,
        verbose=True,
        callbacks=[callback_handler]
    )

    response = agent_executor.ainvoke(
        {"input": "select revenue from transactions"},
        config={"callbacks": [callback_handler]}
    )

    print("\nFinal Agent Output:")

    print(response)

run_agent()

'''First iteration does tool call for schema info 
second tool call for making query and then final response with query results.'''

'''Agentic loop
1.Special tool agent can select and return results
2. How is the callback triggered in the agentic loop?
3. '''