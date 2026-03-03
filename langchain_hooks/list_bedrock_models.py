#!/usr/bin/env python3
"""
List all available Bedrock models and their capabilities
"""

import json
import os
import boto3
from botocore.exceptions import ClientError

def list_bedrock_models():
    """List all available Bedrock models"""
    region = os.environ.get("AWS_REGION", "us-east-1")
    
    try:
        client = boto3.client("bedrock", region_name=region)
        
        print("=" * 80)
        print("Available Bedrock Models")
        print("=" * 80)
        print()
        
        response = client.list_foundation_models()
        
        models = response.get("modelSummaries", [])
        
        if not models:
            print("No models found.")
            return
        
        # Group by provider
        by_provider = {}
        for model in models:
            provider = model.get("providerName", "Unknown")
            if provider not in by_provider:
                by_provider[provider] = []
            by_provider[provider].append(model)
        
        for provider in sorted(by_provider.keys()):
            print(f"\n{'=' * 80}")
            print(f"Provider: {provider}")
            print(f"{'=' * 80}")
            
            for model in by_provider[provider]:
                model_id = model.get("modelId", "N/A")
                model_name = model.get("modelName", "N/A")
                input_tokens = model.get("inputTokenCount", "N/A")
                output_tokens = model.get("outputTokenCount", "N/A")
                supported_features = model.get("supportedCustomizations", [])
                
                print(f"\n📌 Model: {model_name}")
                print(f"   ID: {model_id}")
                print(f"   Input Tokens: {input_tokens:,}" if isinstance(input_tokens, int) else f"   Input Tokens: {input_tokens}")
                print(f"   Output Tokens: {output_tokens:,}" if isinstance(output_tokens, int) else f"   Output Tokens: {output_tokens}")
                print(f"   Supported Features: {', '.join(supported_features) if supported_features else 'None'}")
        
        print("\n" + "=" * 80)
        print("Model IDs Summary:")
        print("=" * 80)
        for model in models:
            provider = model.get("providerName", "Unknown")
            model_id = model.get("modelId", "N/A")
            print(f"  {provider:15} -> {model_id}")
        
    except ClientError as e:
        print(f"❌ AWS Error: {e}")
    except Exception as e:
        print(f"❌ Error: {e}")


def test_model_with_tools(model_id: str):
    """Test if a specific model supports tool use"""
    region = os.environ.get("AWS_REGION", "us-east-1")
    
    print(f"\n{'=' * 80}")
    print(f"Testing Tool Use Support for: {model_id}")
    print(f"{'=' * 80}\n")
    
    try:
        from langchain_aws import ChatBedrockConverse
        from langchain_core.tools import tool
        
        @tool
        def test_tool(query: str) -> str:
            """A test tool"""
            return f"Test response for: {query}"
        
        # Try with tool use
        llm = ChatBedrockConverse(
            model_id=model_id,
            region_name=region,
            provider="anthropic",
        )
        
        # Bind tools
        llm_with_tools = llm.bind_tools([test_tool])
        
        print(f"✓ Model {model_id} successfully initialized with tools")
        print(f"  This model appears to support tool use!")
        
    except Exception as e:
        print(f"✗ Model {model_id} issue: {e}")


if __name__ == "__main__":
    list_bedrock_models()
    
    # Test some common models
    print("\n\n" + "=" * 80)
    print("Testing Popular Models for Tool Use")
    print("=" * 80)
    
    popular_models = [
        "us.anthropic.claude-opus-4-1-20250805",  # Claude 3.5 Opus
        "us.anthropic.claude-sonnet-4-20250514",  # Claude 3.5 Sonnet
        "us.anthropic.claude-haiku-4-5-20251001-v1:0",  # Claude 3.5 Haiku
    ]
    
    for model_id in popular_models:
        test_model_with_tools(model_id)
