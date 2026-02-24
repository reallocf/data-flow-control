# AWS SQL Server Setup

This project supports running the multi-database TPC-H comparison against SQL Server hosted on AWS.
We assume a SQL Server instance running on AWS (typically an EC2 VM with SQL Server installed).

## AWS-side setup

1. **Provision SQL Server on AWS**
   - Launch a Windows EC2 instance with SQL Server (License Included) from the AWS Marketplace or your
     preferred SQL Server AMI.
   - Ensure the instance has enough CPU/RAM/disk for the TPC-H scale factor you plan to load.

2. **Networking**
   - Security group inbound rules should allow:
     - TCP `1433` from your client IP (SQL Server)
     - TCP `3389` from your client IP (RDP for administration)
   - Verify Windows Firewall allows inbound TCP `1433`.

3. **SQL Server configuration**
   - Enable TCP/IP in SQL Server Configuration Manager if it is not already enabled.
   - Ensure SQL Server is configured to accept remote connections.

4. **Create a database and login**
   - Use SSMS (via RDP) to create a database (e.g., `tpch`) and a SQL login with `db_owner`.
   - These credentials will be used by the experiment scripts and tests.

## Local setup

1. **ODBC Driver**
   - Install Microsoft ODBC Driver 18 for SQL Server on your local machine.
   - Make sure the driver name matches the default (`ODBC Driver 18 for SQL Server`) or set
     `SQLSERVER_DRIVER`.

2. **Python dependencies**
   - `pyodbc` is required (included in the project dependencies). Run `uv sync` (or your preferred
     install method) in `vldb_2026_big_paper_experiments/`.

3. **Environment variables**
   - Configure the connection information as environment variables (only the fields below are required):

```
SQLSERVER_PASSWORD=<password>
```

Notes:
- Defaults (override only if needed):
  - SQLSERVER_HOST=data-flow-control-sql-server.cu3qs4uisn3k.us-east-1.rds.amazonaws.com
  - SQLSERVER_PORT=1433
  - SQLSERVER_USER=tpch
  - SQLSERVER_DATABASE=tpch
- The SQL Server integration hard-codes:
  - driver: `ODBC Driver 18 for SQL Server`
  - encrypt: `yes`
  - trust server certificate: `yes`
  - login timeout: `10`
  - query timeout: `0`
- ODBC Driver 18 defaults to encrypted connections; the hard-coded settings assume a self-signed
  cert. If you need different settings, update `sqlserver.py`.

## Loading TPC-H data

The SQL Server client loads TPC-H tables directly over the connection using batched inserts. The
first run will take time depending on scale factor and instance size. Subsequent runs re-use the
existing tables.

## Verification

Run the SQL Server smoke test (requires env vars to be set):

```
cd vldb_2026_big_paper_experiments
uv run pytest tests/test_multi_db.py::test_sqlserver_smoke
```

## Running the multi-db experiment

```
cd vldb_2026_big_paper_experiments
python scripts/run_tpch_multi_db.py --sf 1 --suffix _sqlserver --engine sqlserver
```
