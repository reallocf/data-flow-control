IMPLEMENTATION TARGET: PostgreSQL policy checks in a ProvSQL workflow
ProvSQL is a PostgreSQL extension that attaches provenance tokens to supported
relations and query results. Provenance identifies contributing evidence; it does
not automatically enforce Regulation Z conditions.
The supplied adapter freezes the original selected contributor inside a transaction,
checks all policy conditions, and permits the write only if every check passes.
It enables provenance on regz_context using:
CREATE EXTENSION provsql CASCADE;
SELECT provsql.add_provenance('regz_context');
It evaluates policy checks with SET LOCAL provsql.active = off, then explains the
selected context with provenance enabled, using provenance() and
sr_formula(provenance()). Only context provenance is claimed, not provenance of
the original ORDER BY/LIMIT or physical UPDATE. These steps are supplied infrastructure.

For each policy, output exactly one read-only PostgreSQL SELECT returning one row
per regz_context row, with columns batch_id and passed (Boolean).
Use (<condition>) IS TRUE AS passed. An inapplicable condition must pass.
Do not filter away failures, use LIMIT, write tables, configure the extension, or
include psql commands. You may use CTEs and subqueries, but only regz_context is
available to the policy. Ignore the extension-managed provenance column.
Fields used in conditions are non-null. Boolean fields are native Boolean values;
dates are DATE. Use AND, OR, NOT, standard comparisons, and single-quoted strings.

Unrelated syntax example (not a Regulation Z policy):
SELECT f.batch_id,
       (f.mode <> 'air' OR f.weight_kg <= 20) IS TRUE AS passed
FROM shipment_facts AS f;

Evaluation executes policy checks in PostgreSQL with provenance disabled, matching
this workflow. It tests policy authoring, not provenance-circuit construction.
