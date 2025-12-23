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
