"""Check export completeness and preservation of the original UPDATE selection."""

from dataclasses import replace
import json

import duckdb
import pytest
import sqlglot

from vldb_experiments.strategies.regulation_z_payment_allocation_benchmark import (
    generate_payment_allocation_workload,
    reset_payment_allocation_database,
)
from vldb_experiments.strategies.regulation_z_provsql import (
    build_provsql_query,
    export_provsql_workload,
)


@pytest.mark.parametrize("no_ties", [False, True])
def test_generated_selection_preserves_update(no_ties):
    cases = generate_payment_allocation_workload(num_accounts=4, num_updates=4, no_ties=no_ties)
    for case in cases:
        script = build_provsql_query(case)
        start = script.index("CREATE TEMP TABLE frozen_choice ON COMMIT DROP AS\n") + len(
            "CREATE TEMP TABLE frozen_choice ON COMMIT DROP AS\n"
        )
        candidate = sqlglot.parse_one(
            script[start : script.index("\nDO $$", start)], read="postgres"
        )
        update = sqlglot.parse_one(case.sql, read="duckdb")
        select = candidate
        assert select.args["where"].sql(dialect="postgres") == update.args["where"].sql(
            dialect="postgres"
        )
        assert select.expressions[2].this.sql(dialect="postgres") == update.expressions[
            0
        ].expression.sql(dialect="postgres")
        sources = "SELECT * FROM " + select.args["joins"][0].this.sql(dialect="postgres")
        sources += " " + " ".join(join.sql(dialect="postgres") for join in select.args["joins"][1:])
        expected_sources = "SELECT * FROM " + update.args["from_"].this.sql(dialect="postgres")
        assert (
            sqlglot.parse_one(sources, read="postgres").sql()
            == sqlglot.parse_one(expected_sources, read="postgres").sql()
        )


def test_export_preserves_all_generated_tables_and_refuses_overwrite(tmp_path):
    cases = generate_payment_allocation_workload(num_accounts=4, num_updates=4)
    export_provsql_workload(tmp_path, cases)
    manifest = json.loads((tmp_path / "manifest.json").read_text())
    with duckdb.connect() as conn:
        reset_payment_allocation_database(conn, cases)
        expected = {
            name: conn.execute(f'SELECT count(*) FROM "{name}"').fetchone()[0]
            for (name,) in conn.execute("SHOW TABLES").fetchall()
        }
    assert manifest["tables"] == expected
    assert expected["regz_context"] == 8
    original = (tmp_path / "data.sql").read_bytes()
    with pytest.raises(FileExistsError):
        export_provsql_workload(tmp_path, cases)
    assert (tmp_path / "data.sql").read_bytes() == original


def test_rejects_unrelated_update():
    case = generate_payment_allocation_workload(num_accounts=1, num_updates=1)[0]
    with pytest.raises(
        ValueError, match="Expected the generator's single-assignment allocation UPDATE"
    ):
        build_provsql_query(replace(case, sql="UPDATE other SET value = 1"))
