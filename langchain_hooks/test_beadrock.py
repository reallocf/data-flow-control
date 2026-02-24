#!/usr/bin/env python3
"""
Comprehensive AWS Bedrock Connectivity Test

This script verifies:
1. AWS credentials
2. Bedrock runtime access
3. Direct boto3 model invocation
4. LangChain ChatBedrock integration
"""

import json
import os
import sys
import traceback

import boto3
from botocore.exceptions import BotoCoreError, ClientError

# LangChain
from langchain_aws import ChatBedrock
from langchain_core.messages import HumanMessage

# ✅ Correct model ID (change if needed)
BEDROCK_MODEL_ID = "arn:aws:bedrock:us-east-1:920736616554:application-inference-profile/nix3w87nd0mc"


# ============================================================
# Environment Check
# ============================================================

def check_environment():
    print("Checking environment configuration...\n")

    has_credentials = False
    has_bearer_token = False

    if os.environ.get("AWS_BEARER_TOKEN_BEDROCK"):
        has_bearer_token = True
        print("  ✓ AWS_BEARER_TOKEN_BEDROCK is set")

    if os.environ.get("AWS_ACCESS_KEY_ID") and os.environ.get("AWS_SECRET_ACCESS_KEY"):
        has_credentials = True
        print("  ✓ AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY found")
    elif os.path.exists(os.path.expanduser("~/.aws/credentials")):
        has_credentials = True
        print("  ✓ ~/.aws/credentials file found")

    region = os.environ.get("AWS_REGION", "us-east-1")
    print(f"  ✓ Region: {region}\n")

    if not (has_credentials or has_bearer_token):
        print("  ✗ No authentication method detected.\n")
        return False
    return True


# ============================================================
# Bedrock Client Test
# ============================================================

def create_bedrock_client():
    print("Creating Bedrock runtime client...")

    try:
        region = os.environ.get("AWS_REGION", "us-east-1")

        client = boto3.client(
            service_name="bedrock-runtime",
            region_name=region
        )

        print("  ✓ Client created successfully\n")
        return client

    except Exception as e:
        print(f"  ✗ Failed to create client: {e}\n")
        return None


# ============================================================
# Direct Boto3 Invocation
# ============================================================

def test_boto3_invocation(client):
    print("Testing direct boto3 invocation...")
    print(f"  Model: {BEDROCK_MODEL_ID}")

    request_body = {
    "prompt": "<s>[INST] Say 'Hello Bedrock test successful!' and nothing else. [/INST]",
    "max_gen_len": 50,
    "temperature": 0.0,
    "top_p": 0.9
    }

    try:
        response = client.invoke_model(
            modelId=BEDROCK_MODEL_ID,
            body=json.dumps(request_body),
            contentType="application/json",
            accept="application/json"
        )

        response_body = json.loads(response["body"].read())

        text = ""
        text = response_body.get("generation", "")
        print("  ✓ Boto3 invocation successful")
        print(f"  Response: {text.strip()}\n")

        return True

    except ClientError as e:
        print("  ✗ AWS ClientError")
        print(json.dumps(e.response, indent=2, default=str))
        print()
        return False

    except Exception as e:
        print("  ✗ Unexpected error")
        print(traceback.format_exc())
        return False


# ============================================================
# LangChain get_llm()
# ============================================================
class ToolBindingLLM:
    def __init__(self, llm):
        self._llm = llm
        self._bound_tools = None

    def bind_tools(self, tools):
        # store tools (agent may inspect) and return an LLM-like object
        self._bound_tools = tools
        return self

    def invoke(self, *args, **kwargs):
        return self._llm.invoke(*args, **kwargs)

    def __getattr__(self, name):
        # delegate other attributes/methods to the underlying LLM
        return getattr(self._llm, name)
    
def get_llm():
    """
    Proper LangChain Bedrock LLM wrapper
    """
    region = os.environ.get("AWS_REGION", "us-east-1")

    return ChatBedrock(
        model_id=BEDROCK_MODEL_ID,
        base_model_id='us.anthropic.claude-haiku-4-5-20251001-v1:0',
        region_name=region,
        provider="anthropic",
        model_kwargs={
            "temperature": 0,
            "max_tokens": 500,
        }
    )
    # return ToolBindingLLM(basellm)


# ============================================================
# LangChain Invocation Test
# ============================================================

def test_langchain_llm():
    print("Testing LangChain ChatBedrock wrapper...")

    try:
        llm = get_llm()

        response = llm.invoke
        ("Say 'LangChain test successful!' and nothing else.")


        print("  ✓ LangChain invocation successful")
        print(f"  Response: {response}")

        return True

    except Exception as e:
        print("  ✗ LangChain invocation failed")
        print(traceback.format_exc())
        return False


# ============================================================
# Main
# ============================================================

def main():
    print("=" * 60)
    print("AWS Bedrock + LangChain Connectivity Test")
    print("=" * 60)
    print()

    if not check_environment():
        print("Environment not configured correctly.")
        sys.exit(1)

    client = create_bedrock_client()
    if not client:
        sys.exit(1)

    boto_success = test_boto3_invocation(client)
    lc_success = test_langchain_llm()

    print("=" * 60)

    if boto_success and lc_success:
        print("✓ All tests passed successfully!")
        sys.exit(0)
    else:
        print("✗ One or more tests failed.")
        sys.exit(1)


if __name__ == "__main__":
    main()
