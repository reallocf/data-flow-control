"""Replay the catalog's scenario suite with SQL/Python differential assertions.

The original tests retain their expected outcomes and transaction assertions.
The wrapper compares every derived field before the existing policy is applied.
"""

from datetime import date
import json
import os
import sqlite3

import pytest

from tests import test_regulation_z_53_full_policy_catalog as scenarios
from vldb_experiments.strategies import regulation_z_53_runtime as runtime
from vldb_experiments.strategies.regulation_z_expanded import allocation_facts_sql
from vldb_experiments.strategies.regulation_z_expanded_gprom import sqlite_sql
from vldb_experiments.strategies.regulation_z_expanded_validation import VALID_ALLOCATION_SQL
from vldb_experiments.strategies.regulation_z_gprom import export_gprom_workload, run_gprom_workload
from vldb_experiments.strategies.regulation_z_payment_allocation_benchmark import (
    generate_payment_allocation_workload,
)

conn = scenarios.conn
# Preserve the scenario suite's parametrization, rather than maintaining a second
# copy that can drift as the operational policy model changes.
for _name in dir(scenarios):
    if _name.startswith("test_"):
        globals()[_name] = getattr(scenarios, _name)


def _sqlite_copy(conn):
    target = sqlite3.connect(":memory:")
    for (name,) in conn.execute("SHOW TABLES").fetchall():
        if name == "regz_context":
            continue
        columns = conn.execute(f'DESCRIBE "{name}"').fetchall()
        target.execute(
            f'CREATE TABLE "{name}" ('
            + ",".join(
                f'"{col[0]}" ' + ("TEXT" if col[1] in ("VARCHAR", "DATE") else "NUMERIC")
                for col in columns
            )
            + ")"
        )
        values = conn.execute(f'SELECT * FROM "{name}"').fetchall()
        target.executemany(
            f'INSERT INTO "{name}" VALUES (' + ",".join("?" for _ in columns) + ")",
            [
                tuple(
                    v.isoformat()
                    if isinstance(v, date)
                    else float(v)
                    if type(v).__name__ == "Decimal"
                    else v
                    for v in row
                )
                for row in values
            ],
        )
    return target


@pytest.fixture(autouse=True)
def compare_live_derivations(monkeypatch):
    original = runtime.derive_allocation_context

    def checked(conn, batch_id):
        try:
            expected = original(conn, batch_id)
        except runtime.EvidenceError:
            valid = {row[0] for row in conn.execute(VALID_ALLOCATION_SQL).fetchall()}
            assert batch_id not in valid
            with _sqlite_copy(conn) as sqlite:
                assert batch_id not in {
                    r[0] for r in sqlite.execute(sqlite_sql(VALID_ALLOCATION_SQL))
                }
            raise
        cursor = conn.execute(allocation_facts_sql())
        rows = [dict(zip([c[0] for c in cursor.description], row)) for row in cursor.fetchall()]
        actual = next(row for row in rows if row["batch_id"] == batch_id)
        assert actual == expected
        assert batch_id in {r[0] for r in conn.execute(VALID_ALLOCATION_SQL).fetchall()}
        with _sqlite_copy(conn) as sqlite:
            cursor = sqlite.execute(sqlite_sql(allocation_facts_sql()))
            rows = [dict(zip([c[0] for c in cursor.description], row)) for row in cursor.fetchall()]
            actual = next(row for row in rows if row["batch_id"] == batch_id)
            normalized = {
                k: v.isoformat() if isinstance(v, date) else v for k, v in expected.items()
            }
            assert actual == normalized
            assert batch_id in {r[0] for r in sqlite.execute(sqlite_sql(VALID_ALLOCATION_SQL))}
        return expected

    monkeypatch.setattr(runtime, "derive_allocation_context", checked)


@pytest.mark.parametrize("no_ties", [False, True])
def test_expanded_live_gprom(tmp_path, no_ties):
    if os.environ.get("GPROM_TEST_DOCKER") != "1":
        pytest.skip("Set GPROM_TEST_DOCKER=1 for actual GProM")
    workload = generate_payment_allocation_workload(num_accounts=4, num_updates=4, no_ties=no_ties)
    export_gprom_workload(tmp_path, workload, expanded=True)
    with sqlite3.connect(tmp_path / "data.sqlite") as conn:
        tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        assert tables.isdisjoint(
            {"regz_context", "balance_components", "allocation_snapshot_context"}
        )
    result = run_gprom_workload(tmp_path)
    assert result["source_unchanged"]
    assert sorted(c["decision"] for c in result["cases"]) == ["ALLOW", "ALLOW", "REJECT", "REJECT"]
    with sqlite3.connect(tmp_path / "data.sqlite") as conn:
        case = result["cases"][0]["allocation_id"]
        params = json.loads((tmp_path / f"{case}.parameters.json").read_text())
        query = (tmp_path / f"{case}.sql").read_text()
        conn.execute(
            "DELETE FROM regz_balances WHERE account_id=? AND balance_id=?",
            (params["account_id"], params["batch_id"]),
        )
        assert {r[2] for r in conn.execute(query, params)} == {"INPUT_ERROR"}


@pytest.mark.parametrize(
    ("credit", "reverse", "notice_date", "recipient", "content", "expected"),
    [
        (20000, 0, "2026-01-15", "consumer", "reviewed", (True, True)),
        (19000, 0, "2026-01-15", "consumer", "reviewed", (False, True)),
        (20000, -1000, "2026-01-15", "consumer", "reviewed", (False, True)),
        (20000, 0, "2026-02-02", "consumer", "reviewed", (True, False)),
        (20000, 0, "2026-01-15", "other", "reviewed", (True, False)),
        (20000, 0, "2026-01-15", "consumer", "  ", (True, False)),
    ],
)
def test_expanded_correction_queries(
    conn, tmp_path, credit, reverse, notice_date, recipient, content, expected
):
    from vldb_experiments.strategies.regulation_z_expanded_gprom import export_standalone_sql

    conn.execute(
        "INSERT INTO regz_error_obligations VALUES ('E','2026-01-01','2026-02-01','consumer')"
    )
    conn.execute("INSERT INTO regz_error_items VALUES ('E','principal',20000)")
    conn.execute(
        "INSERT INTO regz_credits VALUES ('C','E','principal',?,'2026-01-10'), ('R','E','principal',?,'2026-01-11')",
        [credit, reverse],
    )
    conn.execute(
        "INSERT INTO regz_dispatches VALUES ('N','E',?,'correction',?,?)",
        [recipient, notice_date, content],
    )
    conn.execute("INSERT INTO regz_correction_completions VALUES ('E','2026-02-03')")
    export_standalone_sql(tmp_path)
    with _sqlite_copy(conn) as sqlite:
        actual = tuple(
            bool(sqlite.execute((tmp_path / f"P{n}.sql").read_text()).fetchall()) for n in (19, 20)
        )
        assert actual == expected
        sqlite.execute("DELETE FROM regz_error_items")
        assert tuple(
            bool(sqlite.execute((tmp_path / f"P{n}.sql").read_text()).fetchall()) for n in (19, 20)
        ) == (False, False)


def test_standalone_allocation_queries_match_catalog(tmp_path):
    import duckdb

    from vldb_experiments.strategies.regulation_z_53_full_policy_catalog import (
        FULL_REGULATION_Z_53_POLICY_SPECS,
    )
    from vldb_experiments.strategies.regulation_z_payment_allocation_benchmark import (
        reset_payment_allocation_database,
    )

    workload = generate_payment_allocation_workload(num_accounts=4, num_updates=4, no_ties=True)
    export_gprom_workload(tmp_path, workload, expanded=True)
    with duckdb.connect() as reference, sqlite3.connect(tmp_path / "data.sqlite") as expanded:
        reset_payment_allocation_database(reference, workload)
        for spec in FULL_REGULATION_Z_53_POLICY_SPECS:
            if spec.constraint is None or spec.number in (19, 20):
                continue
            expected = sorted(
                reference.execute(
                    "SELECT batch_id FROM regz_context WHERE " + spec.constraint
                ).fetchall()
            )
            actual = sorted(
                expanded.execute((tmp_path / f"{spec.proposition_id}.sql").read_text()).fetchall()
            )
            assert actual == expected, spec.proposition_id
