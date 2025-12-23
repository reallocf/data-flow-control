#!/usr/bin/env python3
"""
Test script to verify AWS Bedrock connectivity and Claude Haiku 4.5 access.

This script makes a simple API call to verify that:
1. AWS credentials are configured correctly
2. Bedrock access is enabled
3. Claude Haiku 4.5 model is accessible
"""

import json
import os
import sys
import boto3
from botocore.exceptions import ClientError, BotoCoreError


BEDROCK_MODEL_ID = "anthropic.claude-haiku-4-5-20251001-v1:0"


def test_bedrock_connection():
    """Test basic Bedrock client creation."""
    print("Testing Bedrock client creation...")
    try:
        region = os.environ.get("AWS_REGION", "us-east-1")
        bearer_token = os.environ.get("AWS_BEARER_TOKEN_BEDROCK")
        
        if bearer_token:
            print(f"  ✓ Found AWS_BEARER_TOKEN_BEDROCK (bearer token authentication)")
        else:
            print(f"  ✓ Using standard AWS credentials")
        
        print(f"  ✓ Region: {region}")
        
        client = boto3.client(
            service_name="bedrock-runtime",
            region_name=region
        )
        print("  ✓ Bedrock client created successfully\n")
        return client
    except Exception as e:
        print(f"  ✗ Failed to create Bedrock client: {str(e)}\n")
        return None


def test_model_invocation(client):
    """Test a simple model invocation."""
    print("Testing model invocation...")
    print(f"  Model: {BEDROCK_MODEL_ID}")
    
    # Simple test prompt
    test_prompt = "Say 'Hello, this is a test!' and nothing else."
    
    try:
        # Prepare the request
        request_body = {
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": 100,
            "messages": [
                {
                    "role": "user",
                    "content": test_prompt
                }
            ]
        }
        
        print(f"  Sending test prompt: '{test_prompt}'")
        
        # Invoke the model
        response = client.invoke_model(
            modelId=BEDROCK_MODEL_ID,
            body=json.dumps(request_body)
        )
        
        # Parse the response
        response_body = json.loads(response['body'].read())
        
        # Extract the text content
        if 'content' in response_body:
            text_content = ""
            for content_block in response_body['content']:
                if content_block['type'] == 'text':
                    text_content += content_block['text']
            
            print(f"  ✓ Model response received")
            print(f"  Response: {text_content.strip()}\n")
            return True
        else:
            print(f"  ✗ Unexpected response format: {response_body}\n")
            return False
            
    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', 'Unknown')
        error_msg = e.response.get('Error', {}).get('Message', str(e))
        
        print(f"  ✗ AWS API Error Details:")
        print(f"     Error Code: {error_code}")
        print(f"     Error Message: {error_msg}")
        print(f"     Full Response: {json.dumps(e.response, indent=2, default=str)}")
        print()
        
        if error_code == 'AccessDeniedException':
            print(f"  Possible issues:")
            print(f"     - Model access not granted in Bedrock console")
            print(f"     - IAM permissions missing")
            print(f"     - Wrong region (model may not be available in {os.environ.get('AWS_REGION', 'us-east-1')})")
            print(f"     - Bearer token invalid or expired (if using AWS_BEARER_TOKEN_BEDROCK)")
        elif error_code == 'ValidationException':
            print(f"  Check if model ID is correct: {BEDROCK_MODEL_ID}")
        print()
        return False
        
    except BotoCoreError as e:
        print(f"  ✗ AWS SDK error: {str(e)}")
        print(f"  Full exception: {repr(e)}\n")
        return False
        
    except Exception as e:
        print(f"  ✗ Unexpected error: {str(e)}")
        print(f"  Full exception: {repr(e)}")
        import traceback
        print(f"  Traceback:\n{''.join(traceback.format_exception(type(e), e, e.__traceback__))}\n")
        return False


def check_environment():
    """Check environment configuration."""
    print("Checking environment configuration...")
    
    has_credentials = False
    has_bearer_token = False
    has_region = False
    
    # Check for bearer token
    if os.environ.get("AWS_BEARER_TOKEN_BEDROCK"):
        has_bearer_token = True
        print("  ✓ AWS_BEARER_TOKEN_BEDROCK is set")
    
    # Check for standard credentials
    if os.environ.get("AWS_ACCESS_KEY_ID") and os.environ.get("AWS_SECRET_ACCESS_KEY"):
        has_credentials = True
        print("  ✓ AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY are set")
    elif os.path.exists(os.path.expanduser("~/.aws/credentials")):
        has_credentials = True
        print("  ✓ AWS credentials file found (~/.aws/credentials)")
    
    # Check for region
    region = os.environ.get("AWS_REGION")
    if region:
        has_region = True
        print(f"  ✓ AWS_REGION is set: {region}")
    else:
        print("  ⚠ AWS_REGION not set, will use default: us-east-1")
    
    print()
    
    if not (has_bearer_token or has_credentials):
        print("  ✗ No authentication method found!")
        print("     Set either AWS_BEARER_TOKEN_BEDROCK or AWS credentials\n")
        return False
    
    return True


def main():
    """Run all tests."""
    print("=" * 60)
    print("AWS Bedrock Connection Test")
    print("=" * 60)
    print()
    
    # Check environment
    if not check_environment():
        print("Environment check failed. Please configure AWS credentials.")
        sys.exit(1)
    
    # Test client creation
    client = test_bedrock_connection()
    if not client:
        print("Failed to create Bedrock client.")
        sys.exit(1)
    
    # Test model invocation
    success = test_model_invocation(client)
    
    # Summary
    print("=" * 60)
    if success:
        print("✓ All tests passed! Bedrock is configured correctly.")
        sys.exit(0)
    else:
        print("✗ Test failed. Please check the error messages above.")
        sys.exit(1)


if __name__ == "__main__":
    main()

