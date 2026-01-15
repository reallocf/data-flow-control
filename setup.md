# Setup Guide

This repository relies on a custom DuckDB build, a SQL rewriter, and an AI-powered tax agent. Follow the steps below carefully.

---

## 1. Clone the Repository

    git clone --recurse-submodules https://github.com/reallocf/data-flow-control
    cd data-flow-control

---

## 2. Build `extended_duckdb`

You must independently install and build DuckDB and the extension CI tools.

    cd extended_duckdb
    git clone https://github.com/duckdb/duckdb
    git clone https://github.com/duckdb/extension-ci-tools

### Build DuckDB + Extensions

    make

This step may take a long time.

### If You Encounter Build Errors

Pin the repositories to known-good commits.

    cd duckdb
    git fetch --all
    git checkout b390a7c

    cd ../extension-ci-tools
    git fetch --all
    git checkout c098325

Then rerun:

    cd ..
    make

---

## 3. Set Up `sql_rewriter`

    cd sql_rewriter
    uv sync --extra dev

Test the installation using the local DuckDB build:

    ./uv_with_local_duckdb.sh sync
    ./uv_with_local_duckdb.sh run pytest

---

## 4. `sbo_tax_agent`

See the full documentation below.

---

# SBO Tax Agent

A small business owner tax agent that uses `sql_rewriter` and `extended_duckdb` for data flow control and query processing.

---

## Installation

This project uses `uv` for dependency management.

    uv sync

For development dependencies:

    uv sync --extra dev

---

## Using a Local DuckDB Build

If you want to use the locally built DuckDB from `extended_duckdb` (recommended), you have several options.

### Option 1: Wrapper Script (Recommended)

    ./uv_with_local_duckdb.sh sync
    ./uv_with_local_duckdb.sh run python your_script.py

---

### Option 2: Source the Setup Script

    source setup_local_duckdb.sh
    uv sync
    uv run python your_script.py

---

### Option 3: Python Helper Import

Import the helper before importing DuckDB.

    import use_local_duckdb
    import duckdb
    from sql_rewriter import SQLRewriter

    rewriter = SQLRewriter()

Note: Make sure you have run `make` inside `extended_duckdb` first.

---

## Accessing `sql_rewriter`

The package is included as a local editable dependency.

    from sql_rewriter import SQLRewriter, DFCPolicy, Resolution

---

## Accessing `extended_duckdb`

1. Build DuckDB:

       cd extended_duckdb && make

2. Use one of the local DuckDB setup methods above  
3. Importing `duckdb` will automatically use the local build

---

## Running the Streamlit App

    uv run streamlit run app.py

Or with the local DuckDB wrapper:

    ./uv_with_local_duckdb.sh run streamlit run app.py

The app will open at:

    http://localhost:8501

---

## App Overview

### Upload Data

Upload CSVs for:
- `tax_return` – one row per person per tax year
- `bank_txn` – bank and credit card transactions
- `form_1099_k` – 1099-K data

### Create Policies

Define data flow control policies using:

    SOURCE <source> SINK <sink> CONSTRAINT <constraint> ON FAIL <on_fail>

Whitespace is flexible. All registered policies are viewable in the UI.

### Propose Taxes

Uses an AI agent to analyze transactions and identify business expenses.

---

## AWS Bedrock Setup

The Propose Taxes page uses Claude Haiku 4.5 via AWS Bedrock.

---

### Enable Claude in Bedrock

1. Open the AWS Bedrock Console  
2. Go to Model access  
3. Request access for Claude Haiku 4.5  
4. Wait until access is granted  

Default model:

    anthropic.claude-haiku-4-5-20251001-v1:0

You can change this in `agent.py`.

---

## AWS Credentials

### Option A: Credentials File (Recommended)

Create or edit `~/.aws/credentials`:

    [default]
    aws_access_key_id = YOUR_ACCESS_KEY_ID
    aws_secret_access_key = YOUR_SECRET_ACCESS_KEY
    region = us-east-1

---

### Option B: Environment Variables

Standard credentials:

    export AWS_ACCESS_KEY_ID=your_access_key_id
    export AWS_SECRET_ACCESS_KEY=your_secret_access_key
    export AWS_REGION=us-east-1

Or Bedrock bearer token:

    export AWS_BEARER_TOKEN_BEDROCK=your_bedrock_api_key
    export AWS_REGION=us-east-2

If `AWS_BEARER_TOKEN_BEDROCK` is set, it will be used automatically.

---

### Option C: IAM Role

Attach an IAM role with Bedrock permissions (for EC2/ECS).

---

## Required IAM Permissions

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

You may alternatively use AmazonBedrockFullAccess for testing.

Important: Explicit deny policies override allow policies.

---

## Verify Bedrock Setup

    uv run python test_aws_bedrock.py

Or:

    ./uv_with_local_duckdb.sh run python test_aws_bedrock.py

---

## Region Configuration

Default region is `us-east-1`.

To change it:
- Set `AWS_REGION`, or
- Modify `create_bedrock_client()` in `agent.py`

---

## Development

Run tests:

    uv run pytest

## What Charlie is concretely doing (after setup)

Set up AWS access
```export AWS_BEARER_TOKEN_BEDROCK=...```

Enter project dir
```cd sbo_tax_agent```

Start app with default data
```./uv_with_local_duckdb.sh run streamlit run app.py -- --tax-return ../data/simple_tax_return.csv --form-1099-k ../data/simple_form_1099_k.csv --bank-txn ../data/simple_bank_txn.csv --policies ../data/simple_policies.csv```

Start app with different default data for Eric's LLM-based stuff (this will break until I've fully integrated Eric's work)
```./uv_with_local_duckdb.sh run streamlit run app.py -- --tax-return ../data/simple_tax_return.csv --form-1099-k ../data/simple_form_1099_k.csv --bank-txn ../data/simple_bank_txn.csv --policies ../data/simple_policies2.csv```
