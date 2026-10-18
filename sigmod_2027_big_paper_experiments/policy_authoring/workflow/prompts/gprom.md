TARGET INTERFACE: GProM reenactment + complete SQLite backend decision query

GProM translates reenactment requests into SQL describing hypothetical post-update
state. It does not supply regulatory predicates or a check-before-write workflow.
The installed frontend does not accept this application's UPDATE FROM form. The
backend is SQLite; it supports CTEs, joins, CASE, correlated subqueries, ordinary
comparisons, and scalar MIN. Boolean values are 0/1 and dates are ISO strings.
The driver invokes GProM's SQL executor (-Pexecutor sql) against a read-only SQLite
fixture to obtain actual generated SQL; do not fabricate a replacement reenactment.

Author the complete executable package in exactly these three files. These are
engine input/output boundaries, not a prescribed decomposition of the regulations:

candidate.sql
  One complete read-only SQLite SELECT that expresses the original UPDATE's
  selection and proposed assignment. Input parameter: :allocation_id. It returns
  allocation_id, account_id, statement_id, payment_id, batch_id,
  proposed_cash_dollars for the originally selected contributor. The runner freezes
  this row and refuses more than one. Zero rows means no applicable write (NO_MATCH).
  The runner does not generate this query or repair its semantics. Policy rejection
  must not be used to choose a different contributor in this query.

reenact.sql
  One complete GProM request implementing that selected update. The driver replaces
  {{allocation_id_literal}} with the frozen target as an escaped SQL string literal,
  and {{proposed_cash_literal}} with the frozen finite numeric amount. These are the
  only placeholders permitted here. For unrelated syntax illustration:
  REENACT WITH PROVENANCE (
    UPDATE inventory SET quantity = 4 WHERE item_id = 'demo';
  );
  The resulting backend SELECT exposes payment_allocations' original columns and
  engine-managed provenance columns. It describes the hypothetical updated table,
  not just the changed row. Do not rely on generated provenance-column names.

check.sql
  One complete read-only SQLite statement returning exactly one decision row with
  columns allocation_id, batch_id, proposed_cash_dollars, decision. decision is
  ALLOW or REJECT for the frozen candidate. All applicable regulatory conditions
  and integration with the proposed state must be in this statement. CTEs and
  subqueries are allowed; no particular structure or per-proposition output is
  required. Do not emit a list of independent SELECTs for a caller to combine.

  The one engine substitution {{reenacted_select}} is replaced by GProM's actual
  generated SELECT, with its trailing semicolon removed. Embed it as a query
  expression wherever your statement needs the hypothetical state. Named parameters
  :allocation_id, :account_id, :statement_id, :payment_id, :batch_id,
  :proposed_cash_dollars come ONLY from the frozen candidate row. Their escaping,
  not their semantic validation or policy evaluation, is provided by the driver.
  The decision must refer to the corresponding hypothetical target row and selected
  context; missing or inconsistent state must not yield ALLOW. Use the input schema
  to relate them. A query that ignores the generated state is incomplete.

This package must leave the SQLite fixture unchanged. The benchmark checks selection,
proposed sink state, and decision together; an ALLOW flag for the wrong contributor
is incorrect. GProM here provides read-only preflight, not a production concurrency
protocol or physical write. The comparison must report that distinction from the
native DFC and PostgreSQL checked-write paths.
