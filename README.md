# data-flow-control
Research prototypes for data flow control.

Papers
------
1. [Please Don't Kill My Vibe: Empowering Agents with Data Flow Control](https://arxiv.org/abs/2512.05374)

Projects
--------

## sql_rewriter

A SQL rewriter that intercepts queries, transforms them according to data flow control rules, and executes them against a DuckDB database. See [`sql_rewriter/README.md`](sql_rewriter/README.md) for more details.

## sbo_tax_agent

A small business owner tax agent that demonstrates data flow control using `sql_rewriter`. See [`sbo_tax_agent/README.md`](sbo_tax_agent/README.md) for more details.

## extended_duckdb

A custom DuckDB build with extensions. See [`extended_duckdb/README.md`](extended_duckdb/README.md) for more details.

## experiment_harness

A reusable framework for running experiments using the Strategy design pattern. Provides configurable execution parameters, warm-up runs, setup/teardown steps, and CSV result export. See [`experiment_harness/README.md`](experiment_harness/README.md) for more details.

Developer Workflow
------------------

Run linting and tests from each project directory.

Linting:
- `sql_rewriter`: `python3 -m ruff check .`
- `experiment_harness`: `python3 -m ruff check .`
- `sbo_tax_agent`: `python3 -m ruff check .`
- `vldb_2026_big_paper_experiments`: `.venv/bin/python -m ruff check src/ tests/`

Tests:
- `sql_rewriter`: `uv run pytest`
- `experiment_harness`: `uv run --group dev python -m pytest`
- `sbo_tax_agent`: no tests currently
- `vldb_2026_big_paper_experiments`: `source setup_local_smokedduck.sh && .venv/bin/python -m pytest`
