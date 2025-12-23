# SBO Tax Agent

A small business owner tax agent that uses `sql_rewriter` and `extended_duckdb` for data flow control and query processing.

## Installation

This project uses `uv` for package management. To install dependencies:

```bash
uv sync
```

To install with development dependencies (including pytest):

```bash
uv sync --extra dev
```

## Using Local DuckDB Build

If you want to use a locally built DuckDB from the `extended_duckdb` submodule (which includes custom extensions), you have several options:

### Option 1: Use the wrapper script (Recommended)

Use the provided wrapper script that automatically configures the environment:

```bash
./uv_with_local_duckdb.sh sync
./uv_with_local_duckdb.sh run python your_script.py
```

### Option 2: Source the setup script

Before running uv commands, source the setup script:

```bash
source setup_local_duckdb.sh
uv sync
uv run python your_script.py
```

### Option 3: Import the Python helper

In your Python code, import the helper module before importing duckdb:

```python
import use_local_duckdb  # Must be imported before duckdb
import duckdb
from sql_rewriter import SQLRewriter

# Now SQLRewriter will use the local DuckDB build
rewriter = SQLRewriter()
```

**Note**: Make sure you've built the DuckDB library first by running `make` in the `extended_duckdb` directory.

## Accessing sql_rewriter

The `sql_rewriter` package is included as a local editable dependency. You can import it directly:

```python
from sql_rewriter import SQLRewriter, DFCPolicy, Resolution
```

## Accessing extended_duckdb

The `extended_duckdb` directory contains the DuckDB extension build. To use it:

1. Build the extension: `cd extended_duckdb && make`
2. Use the helper scripts or Python module to configure the environment (see above)
3. The local DuckDB build will be automatically used when you import `duckdb`

## Running the Streamlit App

The app provides a web interface for uploading data, creating policies, and proposing taxes.

To run the app:

```bash
uv run streamlit run app.py
```

Or with the local DuckDB wrapper:

```bash
./uv_with_local_duckdb.sh run streamlit run app.py
```

The app will open in your default web browser at `http://localhost:8501`.

### App Features

The app has three main tabs:

1. **Upload Data**: Upload CSV files for:
   - `tax_return`: Tax return information (one row per person/tax year)
   - `bank_txn`: Bank and credit card transactions
   - `form_1099_k`: 1099-K form data

2. **Create Policies**: Define data flow control policies using a text-based format:
   - View database schema to see available tables and columns
   - Enter policies in the format: `SOURCE <source> SINK <sink> CONSTRAINT <constraint> ON FAIL <on_fail>`
   - Fields can be separated by any whitespace (spaces, tabs, newlines)
   - View all registered policies in a list

3. **Propose Taxes**: Generate tax proposals using an AI agent that analyzes bank transactions and identifies business expenses

## AWS Bedrock Setup

The Propose Taxes page uses AWS Bedrock to access Claude 3 Haiku for analyzing transactions. You need to set up AWS credentials and enable Bedrock access.

### 1. Enable Claude Models in AWS Bedrock

1. Go to the [AWS Bedrock Console](https://console.aws.amazon.com/bedrock/)
2. Navigate to **Model access** in the left sidebar
3. Click **Request model access**
4. Select **Claude Haiku 4.5** (or the model you want to use)
5. Submit the request (approval is usually instant for Claude models)
6. Wait for the model to show as "Access granted"

**Note**: The default model used is `anthropic.claude-haiku-4-5-20251001-v1:0`. You can change this in `agent.py` if needed.

### 2. Set Up AWS Credentials

You have three options for providing AWS credentials:

#### Option A: AWS Credentials File (Recommended for Local Development)

Create or edit `~/.aws/credentials`:

```ini
[default]
aws_access_key_id = YOUR_ACCESS_KEY_ID
aws_secret_access_key = YOUR_SECRET_ACCESS_KEY
region = us-east-1
```

#### Option B: Environment Variables

You can use either standard AWS credentials or a Bedrock bearer token:

**Standard AWS Credentials:**
```bash
export AWS_ACCESS_KEY_ID=your_access_key_id
export AWS_SECRET_ACCESS_KEY=your_secret_access_key
export AWS_REGION=us-east-1
```

**Bedrock Bearer Token (Alternative):**
```bash
export AWS_BEARER_TOKEN_BEDROCK=your_bedrock_api_key
export AWS_REGION=us-east-2
```

**Note**: If `AWS_BEARER_TOKEN_BEDROCK` is set, it will be used for authentication. Otherwise, boto3 will fall back to standard AWS credentials (access key/secret key, IAM role, etc.).

#### Option C: IAM Role (For EC2/ECS)

If running on AWS infrastructure, attach an IAM role with Bedrock permissions.

#### Option D: Bedrock API Key (Bearer Token)

You can use a Bedrock API key by setting the `AWS_BEARER_TOKEN_BEDROCK` environment variable. This is useful for simplified authentication without managing IAM credentials. Get your API key from the [AWS Bedrock Console](https://console.aws.amazon.com/bedrock/).

### 3. IAM Permissions Required

Your AWS user/role needs the following permissions:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "bedrock:InvokeModel",
        "bedrock:InvokeModelWithResponseStream"
      ],
      "Resource": "arn:aws:bedrock:*::foundation-model/anthropic.claude-haiku-4-5-20251001-v1:0"
    }
  ]
}
```

Or use the AWS managed policy `AmazonBedrockFullAccess` (less secure, but simpler for testing).

**Important**: Check for explicit deny policies. If you see an error like "with an explicit deny in an identity-based policy", it means there's an IAM policy that explicitly denies Bedrock access. Explicit deny policies override allow policies, so you'll need to:
1. Check IAM policies attached to your user or groups
2. Remove or modify any policies with `"Effect": "Deny"` for Bedrock actions
3. Contact your AWS administrator if you don't have permission to modify policies

### 4. Verify Setup

You can test your Bedrock setup using the provided test script:

```bash
uv run python test_aws_bedrock.py
```

Or with the local DuckDB wrapper:

```bash
./uv_with_local_duckdb.sh run python test_aws_bedrock.py
```

The test script will:
- Check your AWS credentials configuration
- Verify Bedrock client creation
- Make a test API call to Claude Haiku 4.5
- Display detailed error messages if something fails

This helps identify issues like missing model access, IAM permission problems, or explicit deny policies before running the full application.

### 5. Region Configuration

The default region is `us-east-1`. To use a different region:

1. Set the `AWS_REGION` environment variable, or
2. Modify the region in `agent.py` in the `create_bedrock_client()` function

**Note**: Not all Claude models are available in all regions. Check the [AWS Bedrock documentation](https://docs.aws.amazon.com/bedrock/latest/userguide/model-access.html) for regional availability.

## Development

Run tests (if you have any):

```bash
uv run pytest
```

