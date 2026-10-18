IMPLEMENTATION TARGET: DFC / Passant PGN
DFC attaches declarative conditions to source-to-sink data flows. The engine applies
registered conditions to the contributing source rows during query rewriting.
The harness parses every code string with data_flow_control.Policy.from_pgn().

For each policy, output a complete PGN definition with this syntax:
SOURCE regz_context
SINK payment_allocations AS allocation
CONSTRAINT
    (<Boolean SQL expression>)
ON FAIL REMOVE

Qualify every evidence column with regz_context. Use DuckDB Boolean/comparison
syntax: AND, OR, NOT, =, <>, <, <=, >, >=, quoted string literals, and parentheses.
These are scalar conditions over an authoritative context row; do not introduce
aggregation or derive the summaries again. Parenthesize the complete condition.
The source name, sink name, sink alias, and resolution must match the declaration
above. Multiple registered policies are conjunctive. REMOVE filters violating
contributions; the supplied adapter owns whole-proposal atomicity.

Unrelated syntax example (not a Regulation Z policy):
SOURCE shipment_facts
SINK shipments AS shipment
CONSTRAINT
    (shipment_facts.mode <> 'air' OR shipment_facts.weight_kg <= 20)
ON FAIL REMOVE

Evaluation in this study parses the native PGN and evaluates its scalar condition
on context records. It tests policy semantics and declared bindings, not the
correctness or performance of the DFC rewrite engine.
