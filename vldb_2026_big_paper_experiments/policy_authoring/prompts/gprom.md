IMPLEMENTATION TARGET: GProM with a SQLite policy wrapper
GProM rewrites provenance requests and reenactments into backend SQL. A reenactment
computes a hypothetical post-update state without applying the update. GProM does
not infer Regulation Z requirements or install an enforcement gate automatically.
The installed frontend cannot directly accept this application's UPDATE FROM shape.
The supplied adapter freezes the original selected contributor and assigned value,
reenacts a specialized update, and evaluates policy SQL around the generated SELECT.
For example, its GProM request has the form:
REENACT WITH PROVENANCE (
  UPDATE payment_allocations SET cash_advance_allocation = 100
  WHERE allocation_id = 'an-example-id';
);
The actual target/value are adapter inputs, not constants to put in your answer.

You author the backend policy queries, not REENACT syntax or that adapter. Its
regz_context relation contains authoritative context rows for the proposals being
checked. The adapter picks the result for the frozen contributor's batch_id.
For each policy, output exactly one read-only SQLite SELECT returning one row per
regz_context row, with columns batch_id and passed (integer 0 or 1).
Use CASE WHEN (<condition>) THEN 1 ELSE 0 END AS passed. An inapplicable condition
must pass. Do not filter away failures, use LIMIT, write tables, or invoke extensions.
You may use CTEs and subqueries, but only regz_context is available to the policy.
All fields are non-null. SQLite Boolean fields are integer 0/1; ISO YYYY-MM-DD
strings preserve chronological date ordering. Quoted strings use single quotes.
AND, OR, NOT and ordinary comparisons are supported.

Unrelated syntax example (not a Regulation Z policy):
SELECT f.batch_id,
       CASE WHEN f.mode <> 'air' OR f.weight_kg <= 20 THEN 1 ELSE 0 END AS passed
FROM shipment_facts AS f;

Evaluation uses SQLite directly, the same backend boundary as the supplied GProM
wrapper. It does not measure GProM reenactment generation or lineage quality.
