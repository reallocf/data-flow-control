TARGET INTERFACE: PostgreSQL with ProvSQL — complete checked transaction

ProvSQL adds provenance to PostgreSQL relations/results. It does not supply the
regulatory predicates, select the correct candidate, or automatically gate writes.
Author one complete executable PostgreSQL script named transaction.sql. It must
implement the application's checked UPDATE, including selection, conditions,
transaction handling, and permitted mutation. Do not return individual policy
SELECTs or rely on a supplied frozen-choice/checked-write adapter.

The driver substitutes {{allocation_id_literal}} with the requested target as an
escaped SQL string literal. No other placeholders or generated SQL are supplied.
The driver uses psql with ON_ERROR_STOP=1 against a disposable database, and observes
the target and non-target table state. SQL statements, DO blocks, temporary tables,
CTEs, and transactions are supported. You choose the implementation structure.
A rejected proposal can explicitly roll back/raise an exception, or complete without
changing the target; either must leave all persistent application tables unchanged.
A successful proposal must preserve the original UPDATE's selected contributor,
computed assignment, and every field it does not assign. A missing original
candidate must produce no write. Database/execution errors are recorded separately
from justified policy rejection; do not use unconditional exceptions as enforcement.

Database prerequisites are installed before your script: the ProvSQL extension
exists and regz_context has been provenance-enabled with
  SELECT provsql.add_provenance('regz_context');
payment_allocations is an ordinary table, not provenance-enabled. All declared
schema/evidence columns remain available. Use schema public for application tables;
ProvSQL functions reside in schema provsql. The extension-managed provenance column
must not be treated as an application value or written by your script.

Interface facts for the installed workflow:
- SET LOCAL provsql.active = off disables provenance rewriting for Boolean checks
  and other unsupported query shapes. SET LOCAL requires a transaction.
- SET LOCAL provsql.active = on enables supported SELECT provenance queries.
- SET LOCAL provsql.update_provenance = off disables experimental DML provenance.
- provenance() returns a query result's provenance token, and
  sr_formula(provenance()) renders its semiring formula, with provsql on search_path.
- An unrelated provenance query is:
  SELECT s.shipment_id, provenance(), sr_formula(provenance()) FROM shipments s;
  This assumes shipments was provenance-enabled and provenance is active.

In addition to enforcement, return a provenance SELECT result identifying the
selected context's batch_id, provenance token, and formula, using these extension
functions. Tie it to the same contributor used for the decision. This explains the
trusted context row; it is not provenance of why ORDER BY/LIMIT chose that row or
of the physical UPDATE. No provenance-circuit construction is required by hand.
Do not substitute a string literal or synthetic token for extension output.

PostgreSQL uses native BOOLEAN, DATE, exact numeric amounts, standard comparisons,
and LEAST. Implement the complete transaction yourself using the common specification.
