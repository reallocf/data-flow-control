# REJ16-style reference layer
## Purpose
The project does not require a full reproduction of REJ16. It uses the central REJ16 idea: detect a legal cross-reference expression, interpret it with legal-text structure, and link it to the target provision.
## Interpretation
The hierarchy provides context for resolution. It does not require a reference to remain inside the current hierarchy branch.
A reference elsewhere in the same legal text can still be internal.
## Current section 274 implementation
The current layer handles explicit section references, local structural references, self-references, coordinated references, complex expressions, chapter and subchapter references, and references to named external Acts.
## Reviewed result
The section 274 gold set contains 123 occurrences.
The current output has:
- 123 predictions
- 123 exact surface, span, and target matches
- 0 wrong matches
- 0 missing occurrences
- 0 extra occurrences
This result is in-sample because section 274 was used while adapting the rules.
## Research use
The resolved references are intended to form a cross-reference graph. Referenced provisions can then be added to the legal context supplied to policy generation.
The structured layer is the primary path. A model can remain useful for unusual or ambiguous cases that the structured rules cannot resolve.
