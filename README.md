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


# TODO list
#
# When violations happen, we see it
# Should be obvious that we cannot do this otherwise
# Conseca and others are not data focused
# Describe a simple policy -- how would we do it for the agent? How can we push it all into the system?
#
# Next steps:
# - Make sure agent doesn't INSERT directly so policies are all exercised
# - Generate 2 policies (1099K and Meals / Entertainment or something simple)
# - Thread through HUMAN resolution to frontend
# - Tweak data to make sure there's at least one policy violation
#
# How much work would it take for others to do what we're doing -- then we can judge the amount of work
# Ignoring performance
#
# Create a task list -- before the new year
#
