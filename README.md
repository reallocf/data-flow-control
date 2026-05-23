# data-flow-control
Research prototypes for data flow control.

Papers
------
1. [Please Don't Kill My Vibe: Empowering Agents with Data Flow Control](https://arxiv.org/abs/2512.05374)

Projects
--------

## shared_sql_utils

Shared SQL utilities used across projects (balanced constraint composition, etc.). See [`shared_sql_utils/README.md`](shared_sql_utils/README.md) for details.

## sql_rewriter

A SQL rewriter that intercepts queries, transforms them according to data flow control rules, and executes them against a DuckDB database. See [`sql_rewriter/README.md`](sql_rewriter/README.md) for more details.

## sbo_tax_agent

A small business owner tax agent that demonstrates data flow control using `sql_rewriter`. See [`sbo_tax_agent/README.md`](sbo_tax_agent/README.md) for more details.

## passant

Rust-backed Data Flow Control rewrite engine with Python bindings. The Python package is published to PyPI as [`data-flow-control`](https://pypi.org/project/data-flow-control/). See [`passant/README.md`](passant/README.md) for more details.

## extended_duckdb

A custom DuckDB build with extensions. See [`extended_duckdb/README.md`](extended_duckdb/README.md) for more details.

## experiment_harness

A reusable framework for running experiments using the Strategy design pattern. Provides configurable execution parameters, warm-up runs, setup/teardown steps, and CSV result export. See [`experiment_harness/README.md`](experiment_harness/README.md) for more details.

Developer Workflow
------------------

Run linting and tests from each project directory.

Linting:
- `shared_sql_utils`: `python3 -m ruff check .`
- `sql_rewriter`: `python3 -m ruff check .`
- `experiment_harness`: `python3 -m ruff check .`
- `sbo_tax_agent`: `python3 -m ruff check .`
- `vldb_2026_big_paper_experiments`: `.venv/bin/python -m ruff check src/ tests/`

Tests:
- `shared_sql_utils`: `python3 -m pytest`
- `sql_rewriter`: `uv run pytest`
- `experiment_harness`: `uv run --group dev python -m pytest`
- `sbo_tax_agent`: no tests currently
- `vldb_2026_big_paper_experiments`: `source setup_local_smokedduck.sh && .venv/bin/python -m pytest`

PyPI Deployment
---------------

The Python bindings in [`passant/`](passant/) are published to PyPI as [`data-flow-control`](https://pypi.org/project/data-flow-control/). Publishing is manual only via the [Publish to PyPI](.github/workflows/pypi-publish.yml) GitHub Actions workflow.

### Prerequisites

PyPI trusted publishing must be configured for this repository. The pending publisher should reference:

- **Repository:** `reallocf/data-flow-control`
- **Workflow:** `pypi-publish.yml`
- **Environment name:** `(Any)`

No `PYPI_API_TOKEN` or other repository secrets are required. The workflow authenticates to PyPI using OIDC.

### Publish a release

1. Bump the package version in [`passant/pyproject.toml`](passant/pyproject.toml) and the workspace version in [`passant/Cargo.toml`](passant/Cargo.toml) so they match.
2. Merge the version bump to the branch you want to publish from (typically `main`).
3. In GitHub, open **Actions → Publish to PyPI → Run workflow**.
4. Select the branch to publish from and click **Run workflow**.
5. Wait for the workflow to build wheels for Linux, macOS, and Windows, then upload them to PyPI.
6. Verify the new version on [pypi.org/project/data-flow-control](https://pypi.org/project/data-flow-control/).

Users can install the published package with:

```bash
pip install data-flow-control
```
