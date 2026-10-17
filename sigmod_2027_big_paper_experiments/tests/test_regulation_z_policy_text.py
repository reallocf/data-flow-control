from dataclasses import replace
import json

import duckdb
import pytest
from sqlglot import exp, parse_one

from vldb_experiments.strategies.regulation_z_53_full_policy_catalog import (
    FULL_REGULATION_Z_53_POLICY_SPECS,
)
from vldb_experiments.strategies.regulation_z_payment_allocation_benchmark import (
    POLICY_TEXT,
    build_payment_allocation_prompt,
    generate_payment_allocation_workload,
    reset_payment_allocation_database,
)
from vldb_experiments.strategies.regulation_z_policy_text import policy_decision_text


def test_exact_dispute_and_snapshot_decision_conditions() -> None:
    specs = {s.number: s for s in FULL_REGULATION_Z_53_POLICY_SPECS}
    assert policy_decision_text(specs[17]) == (
        "When (`kind` equals 'allocation'), require "
        "`protected_reduction` is less than or equal to `unavoidable_protected_reduction`. "
        "Otherwise satisfied."
    )
    assert policy_decision_text(specs[13]) == (
        "When (`kind` equals 'allocation'), require all of: "
        "`snapshot_date` is greater than or equal to `cycle_end`; "
        "`snapshot_date` is less than or equal to `credited_date`. Otherwise satisfied."
    )
    with pytest.raises(ValueError, match="No plain-language rendering"):
        policy_decision_text(replace(specs[17], constraint="regz_context.kind IS NULL"))


@pytest.mark.parametrize("no_ties", [False, True])
def test_prompt_exposes_identical_context_facts_and_all_decision_conditions(no_ties) -> None:
    case = generate_payment_allocation_workload(
        num_accounts=1,
        num_updates=1,
        no_ties=no_ties,
    )[0]
    prompt = build_payment_allocation_prompt(case)
    evidence = json.loads(
        prompt.split("Database evidence:\n")[1].split("\n\nShould this completed")[0]
    )
    with duckdb.connect(":memory:") as conn:
        reset_payment_allocation_database(conn, [case])
        cursor = conn.execute("SELECT * FROM regz_context ORDER BY ALL")
        names = [c[0] for c in cursor.description]
        actual = [dict(zip(names, row)) for row in cursor.fetchall()]
    assert evidence["regz_context"] == json.loads(json.dumps(actual, default=str))
    lines = [line for line in POLICY_TEXT.splitlines() if line.startswith("P") and ". " in line]
    assert lines == [
        f"{s.proposition_id}. {s.name}: {s.explanation}"
        + (f" Assumption/limitation: {s.limitation}" if s.limitation else "")
        + f" Decision condition: {policy_decision_text(s)}"
        for s in FULL_REGULATION_Z_53_POLICY_SPECS
    ]
    for spec in FULL_REGULATION_Z_53_POLICY_SPECS:
        if spec.constraint:
            columns = {c.name for c in parse_one(spec.constraint).find_all(exp.Column)}
            for context in evidence["regz_context"]:
                assert columns <= context.keys()
                assert all(context[column] is not None for column in columns)
