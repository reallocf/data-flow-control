TARGET INTERFACE: DFC / Passant native policies

DFC registers declarative constraints on source-to-sink data flows and rewrites the
application SQL using those constraints. You author native policies; the runtime
loads them and submits the original parameterized UPDATE through the DFC connection.
You do not author a rewritten UPDATE or a parallel handwritten SQL policy gate.
The loader supplies no additional regulatory predicates or frozen-choice adapter.

Return one or more files named policies/<name>.pgn. Choose the number, grouping,
and names yourself. Each file contains one complete native PGN policy:
SOURCE regz_context
SINK payment_allocations AS allocation
CONSTRAINT
    (<Boolean DuckDB SQL expression>)
ON FAIL REMOVE

Every referenced evidence column must be qualified with regz_context. For this
source, the native policy parser and runtime support the scalar evidence conditions
shown; do not add aggregation to these already derived scalar summaries. A true
condition permits a contribution and false removes it. Multiple applicable policies
are conjunctive. All policies apply only to flows with this source and sink.
A removed contribution must not cause substitution of another selected contributor.
Whether the actual rewrite satisfies that requirement is checked during evaluation.
The same exact proposal/evidence preparation contract applies as for SQL targets.

The native API is Policy.from_pgn(text), followed by connection.register_policy(policy)
for each parsed policy, and connection.execute(original_update). The harness owns
connection creation, loading files, binding the target identifier, and observing
before/after sink state. It does not generate predicates from your policy names.
Use REMOVE for this benchmark so an excluded allocation is a no-op, not a forced
runtime error. Unknown syntax or unsupported policy behavior counts as an error.

Unrelated syntax illustration:
SOURCE shipment_facts
SINK shipments AS shipment
CONSTRAINT
    (shipment_facts.mode <> 'air' OR shipment_facts.weight_kg <= 20)
ON FAIL REMOVE
