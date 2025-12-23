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

3. **Propose Taxes**: Generate tax proposals based on uploaded data (TODO)

## Development

Run tests (if you have any):

```bash
uv run pytest
```

