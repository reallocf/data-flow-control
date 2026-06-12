# Reference resolution plan for section 274

The old detected_refs field was difficult to interpret because it only listed section-like numbers without surface text or legal context.

The audit now makes the numbers explainable. Each reference has source span, surface text, reference class, parsed target, resolution status, and a short note.

Current result:
- 211 total scanned references
- 89 references in the main section 274 text
- 122 references from editorial notes, amendments, statutory notes, or other non-main text
- 39 main-text references resolved to Title 26 sections
- 49 main-text local structural references still needing hierarchy-aware resolution
- 1 main-text external reference: section 16(a) of the Securities Exchange Act
- 9 main-text section 212 references marked as needed for the seminar policy

This alleviates the immediate confusion about the number list. The remaining research problem is not what the numbers mean, but how to resolve local references such as paragraph (1), subparagraph (A), and subsection (d) against the internal structure of section 274.

Next implementation step: replace the broad detected_refs output with two separate stages:
1. explicit section reference detection
2. local structural reference resolution
