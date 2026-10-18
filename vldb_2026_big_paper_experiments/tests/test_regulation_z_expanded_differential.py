"""Replay catalog scenarios and compare each inline decision with its reference."""

import pytest

from tests import test_regulation_z_53_full_policy_catalog as scenarios
from vldb_experiments.strategies import regulation_z_53_runtime as runtime
from vldb_experiments.strategies.regulation_z_53_full_policy_catalog import (
    FULL_REGULATION_Z_53_POLICY_SPECS,
)
from vldb_experiments.strategies.regulation_z_expanded import allocation_facts_sql, decision_sql

conn = scenarios.conn
for _name in dir(scenarios):
    if _name.startswith("test_"):
        globals()[_name] = getattr(scenarios, _name)


@pytest.fixture(autouse=True)
def compare_every_policy(monkeypatch):
    original = runtime.derive_allocation_context

    def checked(conn, batch_id):
        expected = original(conn, batch_id)
        cursor = conn.execute(allocation_facts_sql())
        names = [c[0] for c in cursor.description]
        actual = next(
            dict(zip(names, row, strict=True)) for row in cursor.fetchall() if row[0] == batch_id
        )
        assert actual == expected
        columns = list(expected)
        facts = "SELECT " + ", ".join("? AS " + name for name in columns)
        for spec in FULL_REGULATION_Z_53_POLICY_SPECS:
            if spec.constraint is None:
                continue
            reference = conn.execute(
                "SELECT " + spec.constraint + " FROM (" + facts + ") regz_context",
                [expected[c] for c in columns],
            ).fetchone()[0]
            allowed = {r[0] for r in conn.execute(decision_sql(spec.number)).fetchall()}
            assert (batch_id in allowed) == reference, spec.proposition_id
        return expected

    monkeypatch.setattr(runtime, "derive_allocation_context", checked)
