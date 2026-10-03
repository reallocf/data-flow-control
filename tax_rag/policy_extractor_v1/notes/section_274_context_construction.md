# Section 274 bounded statutory context construction

## Goal

Automatic DFC policy construction requires statutory context beyond a single
provision. Definitions, exceptions, conditions, and other rules can appear in
referenced provisions.

The current task is therefore to construct a bounded statutory dependency
context before DFC policy construction.

## Dependency representation

The procedure treats statutory provisions as a dependency graph.

For a starting provision:

1. statutory references are resolved;
2. references are mapped to fine-grained structural units;
3. repeated targets and units contained within broader admitted units are
   removed;
4. resolved dependencies are expanded breadth-first;
5. each complete candidate layer is measured against an explicit context
   capacity;
6. a complete layer enters the context only when the cumulative statutory
   material remains within capacity.

This provides a deterministic baseline before later experiments with semantic
prioritization or alternative ordering.

## Section 274 case

For Section 274, the direct federal-tax-code branch maps to 19 structural units
across 15 sections.

The direct cross-code branch contains three provisions and contributes 9,179
tokens.

Under the current structural mapping:

- Layer 1 combined statutory context: 24,298 tokens
- Layer 2 combined statutory context: 171,340 tokens
- Layer 3 combined statutory context: 627,165 tokens
- Layer 4 complete candidate: 1,197,630 tokens
- working statutory-context capacity: 992,498 tokens

Accordingly, Layer 3 remains within the working capacity, while the complete
Layer 4 candidate exceeds it.

## Interpretation

The Layer 3 / Layer 4 boundary is a conditional baseline rather than complete
legal closure.

Later dependency layers still contain structurally ambiguous or unresolved
references. Those cases remain explicit instead of being treated as resolved.

The current result therefore establishes a deterministic context-construction
baseline. It does not yet establish that the resulting context improves DFC
policy quality.

## Next research question

The downstream evaluation should compare alternative context-construction
strategies and measure whether dependency-aware statutory context improves the
completeness and correctness of DFC policies.