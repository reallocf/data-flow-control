"""Compare unchanged policies over a live view with both previous approaches."""

import json

from data_flow_control.adapters.duckdb import DuckDBAdapter
from data_flow_control.connection import Connection
import duckdb

from vldb_experiments.strategies.regulation_z_expanded import allocation_facts_sql, expanded_update
from vldb_experiments.strategies.regulation_z_expanded_benchmark import ExpandedRegulationZStrategy
from vldb_experiments.strategies.regulation_z_payment_allocation_benchmark import (
    build_regulation_z_53_policies,
    reset_payment_allocation_database,
)


class ViewCatalogAdapter(DuckDBAdapter):
    """Include views in the catalog sent to Rust, as the existing v2 runner does."""

    def _list_catalog_tables(self):
        return [row[0] for row in self.execute("SHOW TABLES").fetchall()]


def prepare_view_database(conn, workload):
    reset_payment_allocation_database(conn, workload, materialize_context=False)
    conn.execute("DROP TABLE regz_context")
    # The v3 workload only submits allocations, not correction completions.
    conn.execute("CREATE VIEW regz_context AS " + allocation_facts_sql())


class ViewRegulationZStrategy(ExpandedRegulationZStrategy):
    def setup(self, context):
        super().setup(context)
        conn = duckdb.connect()
        prepare_view_database(conn, self.workload)
        self.connections["view"] = conn
        self.rewriters["view"] = Connection(ViewCatalogAdapter(conn))
        for policy in build_regulation_z_53_policies():
            self.rewriters["view"].register_policy(policy)
        columns = [
            r[0]
            for r in self.connections["materialized"].execute("DESCRIBE regz_context").fetchall()
        ]
        query = "SELECT " + ", ".join(columns) + " FROM regz_context ORDER BY batch_id"
        assert (
            conn.execute(query).fetchall()
            == self.connections["materialized"].execute(query).fetchall()
        )
        (self.output / "view.sql").write_text(
            "CREATE VIEW regz_context AS\n" + allocation_facts_sql() + ";\n"
        )

    def execute(self, context):
        result = super().execute(context)
        assert result.custom_metrics["view_correct"] == len(self.workload)
        return result

    def teardown(self, context):
        # Profile separately, after timing, so profiling cannot distort comparisons.
        for name, conn in self.connections.items():
            case = self.workload[0]
            query = expanded_update(case.sql) if name == "expanded" else case.sql
            rewritten = self.rewriters[name].transform_query(query)
            path = (self.output / f"{name}.profile.json").resolve()
            conn.execute("PRAGMA enable_profiling='json'")
            conn.execute("PRAGMA profiling_mode='detailed'")
            conn.execute("PRAGMA profiling_output='" + str(path) + "'")
            conn.execute(rewritten).fetchall()
            conn.execute("PRAGMA disable_profiling")
            profile = json.loads(path.read_text())
            path.write_text(json.dumps(profile, separators=(",", ":")) + "\n")
        super().teardown(context)
