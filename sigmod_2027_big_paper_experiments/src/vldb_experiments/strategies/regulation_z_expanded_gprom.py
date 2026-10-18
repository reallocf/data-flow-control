"""Raw-evidence SQLite policies composed with real GProM reenactment SQL."""

from pathlib import Path
import re

import sqlglot
from sqlglot import exp

from vldb_experiments.strategies.regulation_z_expanded import (
    allocation_facts_sql,
    correction_facts_sql,
    decision_sql,
)
from vldb_experiments.strategies.regulation_z_expanded_validation import (
    VALID_ALLOCATION_SQL,
    VALID_CORRECTION_SQL,
)


def sqlite_sql(sql: str) -> str:
    """Translate explicitly where generic transpilation loses date/min semantics.

    These scalar min/max inputs are nonnull under the evidence contract. Avoid
    sqlglot's SQLite LEAST lowering, which can silently discard an operand.
    """
    sql = sql.replace(
        "CROSS JOIN (VALUES (false), (true)) part(disputed)",
        "CROSS JOIN (SELECT false AS disputed UNION ALL SELECT true AS disputed) part",
    )
    sql = sql.replace(
        "b.cycle_end + CAST(p.offset_days AS INTEGER)",
        "DATE(b.cycle_end, PRINTF('%+d days', p.offset_days))",
    ).replace("c.end_date + 1", "DATE(c.end_date, '+1 day')")
    sql = sql.replace("GREATEST(", "REGZ_MAX(").replace("LEAST(", "REGZ_MIN(")
    tree = sqlglot.parse_one(sql, read="duckdb")
    for cte in tree.find_all(exp.CTE):
        cte.set("materialized", True)
    return (
        tree.sql(dialect="sqlite", pretty=True)
        .replace("REGZ_MAX(", "MAX(")
        .replace("REGZ_MIN(", "MIN(")
    )


def expanded_gprom_template() -> str:
    template = Path(__file__).with_name("regulation_z_gprom.sql").read_text()
    facts = sqlite_sql(allocation_facts_sql())
    validity = sqlite_sql(VALID_ALLOCATION_SQL)
    # A local CTE supplies freshly computed facts; no summary table is read.
    template = template.replace("regz_context", "computed_facts")
    template = template.replace(
        "WITH\nproposed AS (",
        f"WITH\ncomputed_facts AS MATERIALIZED (\n{facts}\n),\nvalid_evidence AS (\n{validity}\n),\nproposed AS (",
        1,
    )
    template = template.replace(
        "(SELECT COUNT(*) FROM selected_state) = 1",
        "(SELECT COUNT(*) FROM selected_state) = 1\n"
        "      AND EXISTS (SELECT 1 FROM valid_evidence v WHERE v.batch_id=:batch_id)",
        1,
    )
    template = re.sub(r"(?m)^(\w+) AS \($", r"\1 AS MATERIALIZED (", template)
    return template.replace(
        "-- Trusted computed_facts summaries cover the exact complete allocation proposal.",
        "-- All compliance facts are computed below from raw evidence in this statement.",
    )


def export_standalone_sql(output: Path) -> None:
    """Each file includes its computations and input validation, with no views."""
    output.mkdir(parents=True, exist_ok=True)
    for number in range(1, 41):
        try:
            query = decision_sql(number)
        except ValueError:
            continue  # Supporting meanings are represented in the full report.
        if number in (19, 20):
            # Corrections have a separate checkpoint and evidence workflow.
            query = correction_facts_sql()
            predicate = "missing_credit_cents=0" if number == 19 else "notice_count>0"
            query = f"SELECT * FROM ({query}) correction WHERE {predicate} AND batch_id IN ({VALID_CORRECTION_SQL})"
        else:
            query = (
                f"SELECT p.batch_id FROM ({query}) p WHERE p.batch_id IN ({VALID_ALLOCATION_SQL})"
            )
        (output / f"P{number:02}.sql").write_text(sqlite_sql(query) + ";\n")
    (output / "allocation-facts.sql").write_text(sqlite_sql(allocation_facts_sql()) + ";\n")
    (output / "policy.template.sql").write_text(expanded_gprom_template())
