"""Render the catalog's decision conditions without a second hand-maintained rule set."""

from sqlglot import exp, parse_one

from vldb_experiments.strategies.regulation_z_53_full_policy_catalog import (
    RegulationZ53PolicySpec,
)


def _flatten(node: exp.Expression, kind: type[exp.Expression]) -> list[exp.Expression]:
    node = node.unnest()
    if isinstance(node, kind):
        return _flatten(node.this, kind) + _flatten(node.expression, kind)
    return [node]


def _negated(node: exp.Expression) -> exp.Expression:
    node = node.unnest()
    inverse = {
        exp.EQ: exp.NEQ,
        exp.NEQ: exp.EQ,
        exp.GT: exp.LTE,
        exp.GTE: exp.LT,
        exp.LT: exp.GTE,
        exp.LTE: exp.GT,
    }
    if type(node) in inverse:
        return inverse[type(node)](this=node.this.copy(), expression=node.expression.copy())
    return exp.Not(this=node.copy())


def _condition_text(node: exp.Expression) -> str:
    if isinstance(node, exp.Paren):
        return _condition_text(node.this)
    if isinstance(node, exp.And):
        return "all of: " + "; ".join(_condition_text(part) for part in _flatten(node, exp.And))
    if isinstance(node, exp.Or):
        return "at least one of: " + "; ".join(
            _condition_text(part) for part in _flatten(node, exp.Or)
        )
    comparisons = {
        exp.EQ: "equals",
        exp.NEQ: "does not equal",
        exp.GT: "is greater than",
        exp.GTE: "is greater than or equal to",
        exp.LT: "is less than",
        exp.LTE: "is less than or equal to",
    }
    for kind, wording in comparisons.items():
        if isinstance(node, kind):
            return f"{_condition_text(node.this)} {wording} {_condition_text(node.expression)}"
    if isinstance(node, exp.Not):
        if isinstance(node.this, exp.Column):
            return f"{_condition_text(node.this)} is false"
        return f"it is not the case that ({_condition_text(node.this)})"
    if isinstance(node, exp.Column):
        if node.table != "regz_context":
            raise ValueError(f"Unexpected policy source: {node.sql()}")
        return f"`{node.name}`"
    if isinstance(node, exp.Literal):
        return repr(node.this) if node.is_string else str(node.this)
    raise ValueError(f"No plain-language rendering for {type(node).__name__}: {node.sql()}")


def policy_decision_text(spec: RegulationZ53PolicySpec) -> str:
    if spec.constraint is None:
        return "Supporting scope, definition, or permission; no separate rejection condition."
    alternatives = _flatten(parse_one(spec.constraint, read="duckdb"), exp.Or)
    if len(alternatives) == 1:
        return "Require " + _condition_text(alternatives[0]) + "."
    # A OR B OR C is equivalent to requiring C when both NOT A and NOT B hold.
    # Context predicate inputs are non-null, as guaranteed by the derivation.
    applicability = " and ".join(
        f"({_condition_text(_negated(part))})" for part in alternatives[:-1]
    )
    return (
        f"When {applicability}, require {_condition_text(alternatives[-1])}. Otherwise satisfied."
    )


EVIDENCE_SEMANTICS = """Interpretation of the supplied evidence:
Evaluate the regz_context row whose batch_id equals the transaction selected by
the SQL. Its matching regz_proposal_lines describe the complete allocation of
minimum plus excess. Assess the completed allocation, not whether every other
candidate proposal is compliant. The payment_allocations.transaction_id identifies
the payment transaction, not the balance-producing transaction selected by the SQL.

A lot is an individually identified portion of the outstanding balance. A slice
is its disputed or undisputed portion; capacity is the amount available to receive
payment. The original tables describe the candidate-selection query. The regz_*
tables provide complete account and proposal evidence, including additional minimum
and promotional lots. Matching identifiers refer to the same underlying objects;
do not add amounts across these overlapping representations. Rows sharing a
balance_id at different snapshot dates describe one balance at different times.
Use the relevant snapshot for each calculation, never the sum across snapshots.

The regz_context rows are authoritative, deterministically derived summaries of
the supplied evidence for their respective proposals. You may use these summaries
directly; they are facts, not final acceptance decisions. Field names in each
decision condition refer to this selected context row. Boolean fields used alone
must be true. 'At least one of' is inclusive OR; 'all of' requires every listed
condition. Inputs used in the decision conditions are non-null. A conditional
requirement is satisfied when its applicability condition is false.
Return true if and only if every applicable decision condition is satisfied for
the completed allocation; otherwise return false. The decision conditions define
acceptance for this benchmark. Descriptions explain their purpose and do not
introduce additional checks. Use the supplied derived facts directly; independent
reconstruction of each regulatory calculation is not required.
Supporting entries impose no additional rejection condition. Do not add obligations
beyond the listed rules and their stated benchmark assumptions.

Amounts in regz_* are exact integer cents; amounts in the original statement,
transaction, balance, and payment-allocation tables are dollars. APR values are
percentage points (25 means 25 percent). Comparisons in the decision conditions
are exact, with no implicit monetary or APR tolerance. Dates are calendar dates;
greater-than-or-equal and less-than-or-equal include their endpoints. Null program,
request, or replacement identifiers mean no such association. A null window_start
means this lot has no qualifying program active on the payment's credited date.
A non-null window_start is the calculated priority-window start and may be in the
future. A program can be active before this window begins. Window priority applies
only from window_start through expiration_date, inclusive, for an active qualifying
program; a non-null window_start alone does not establish window eligibility.

For dispute protection, use each lot's payment-date amount and protected amount.
Its undisputed capacity is amount_cents minus protected_cents. Consume undisputed
capacity before protected capacity within the lot. protected_reduction is the sum
over lots of max(0, minimum_cents + excess_cents - undisputed capacity).
unavoidable_protected_reduction is max(0, the entire payment's total_cents minus
the sum of undisputed capacity over all outstanding lots). other_capacity_remaining
is the sum of max(0, undisputed capacity - minimum_cents - excess_cents).
Thus dispute protection includes the minimum and all available undisputed lots,
not just the cash-advance candidates; it takes precedence over APR, program, and
request preferences. Partial protection is supported. P17 permits equality with
the unavoidable amount; P18 adds its residual condition only as stated below.

Rate-priority counts consider excess allocation after minimum allocation consumes
capacity. Within each disputed/undisputed class, active deferred-window capacity
precedes ordinary capacity. Ordinary historical lots precede later lots; within
each such group higher effective APR precedes lower APR. A violation is a pair
where a lower-priority slice receives a positive excess allocation while a
higher-priority slice retains capacity. Equal-priority slices have no required
order. highest_violations counts these APR violations from the highest ordinary
rate group; remaining_violations counts those from other ordinary rate groups;
ordinary_violations counts both. deferred_violations counts deferred-window
priority violations, snapshot_violations counts historical-before-later violations,
and early_expiry_violations counts deferred violations in the early-expiry partial
cycle. P17 and P18 separately enforce priority across dispute classes.

Program classification requires interest_accrues and waived_on_payoff, excludes
grace_period, and excludes temporary_zero unless retroactive_interest is true.
An active qualifying program includes both its start_date and expiration_date.
Its effective allocation APR is zero; other historical lots use their selected
snapshot APR and later lots use their payment-date APR. Mismatch fields count
disagreements between proposed values and these derived values. The supplied
window and snapshot facts use the conventions and limitations listed below.
Request fields are evaluated only in their stated request branches; correction
fields only for kind='correction'. False request facts and empty correction tables
do not themselves invalidate a default allocation.
"""
