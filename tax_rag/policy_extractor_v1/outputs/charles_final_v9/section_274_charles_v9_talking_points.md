# Section 274 — Charles presentation talking points

## Slide 1 — Research question

The problem is not simply retrieving Section 274 itself.  
The policy extractor also needs statutory provisions reached through cross-references.

The question I studied is:

**Which dependencies should enter model context when recursive retrieval can eventually exceed the model budget?**

The baseline rule I test is deterministic: admit complete dependency layers breadth-first until the next complete layer no longer fits.


## Slide 2 — Entry context

Section 274's direct dependency layer maps to:

- 19 structural units,
- across 15 Title-26 sections,
- totaling 15,119 tokens.

There are also three direct cross-code provisions totaling 9,179 tokens.

So the complete direct layer is 24,298 statutory tokens before later recursive expansion.


## Slide 3 — Structural validation

Before relying on recursive retrieval, I checked whether the plain-text hierarchy mapping was structurally credible.

For Section 267, the parser recovered all 88 expected structural identifiers with no extras or missing identifiers.

For Section 274, it recovered all 142 expected identifiers, again with zero structural extras or omissions.

This validates the hierarchy representation used to map cross-references into statutory units.


## Slide 4 — Capacity boundary

The important result here is the complete-layer budget boundary.

The frozen layer-admission calculation uses local `o200k_base` token accounting.

The model context-window assumption is 1,047,576 tokens. I reserve 32,768 tokens for model output and 16,384 tokens as a safety margin:

**1,047,576 - 32,768 - 16,384 = 998,424**

That gives a 998,424-token working input limit.

The fixed extraction instructions plus the Section 274 target require 5,926 local prompt tokens:

**998,424 - 5,926 = 992,498**

So the remaining statutory-context capacity used by the retrieval algorithm is 992,498 tokens.

The complete-layer trace is:

- Layer 1: 24,298 combined statutory tokens.
- Layer 2: 171,340.
- Layer 3: 627,165 — still within capacity.
- Layer 4: 1,197,630 — beyond capacity.

Therefore the bounded baseline admits through Layer 3 and stops before Layer 4.

The API later reported a constant 128-token input-count offset above the canonical local prompt count in every experimental condition. I keep that observation separate from the frozen local layer-admission accounting; the offset does not change the Layer-3 / Layer-4 boundary.

One caveat: Layer 3 is a **budget-complete admitted structural frontier**, not a claim that every ambiguous, missing, or deeper external reference has been semantically resolved.
## Slide 5 — Retrieval algorithm

The baseline procedure is:

1. Start from the target provision.
2. Map direct references to structural statutory units.
3. Deduplicate repeated or contained units.
4. Include the direct cross-code branch.
5. Expand later dependencies breadth-first.
6. Measure each complete candidate layer before adding it.
7. Admit the layer only if cumulative statutory context fits.
8. Stop before the first complete layer that exceeds capacity.

The design deliberately avoids partial admission of a dependency layer.


## Slide 6 — Experiment design

I then tested whether this extra legal context improves DFC policy construction.

The controlled variable is statutory context.

The three conditions are:

- target-only: 5,926 prompt tokens,
- direct dependencies: 30,260,
- bounded recursive context: 671,210.

The same Section 274 target, extraction instructions, model, and temperature were used in all conditions.

The model was gpt-4.1-mini at temperature zero.

Each condition was generated three times.

The policy judge was disabled during generation so judge behavior would not confound the context comparison.


## Slide 7 — Three-replicate result

The bounded condition had the highest statutory-family discovery recall:

- target-only: 28.57%,
- direct: 23.81%,
- bounded: 35.71%.

But that wider discovery did not translate into better executable policies.

Usable-family recall was:

- target-only: 23.81%,
- direct: 23.81%,
- bounded: 21.43%.

Exact candidate precision was:

- target-only: 6.25%,
- direct: 5.88%,
- bounded: 0%.

The stability result is especially informative.

Bounded reproduced the same seven unique normalized constraints in all three generations, with exact-string Jaccard 1.000.

So the bounded errors are not simply run-to-run sampling noise; under this frozen prompt they are systematic.

The case-study conclusion is therefore:

**Recursive retrieval improves legal-rule discovery breadth, but faithful translation of the retrieved statute into executable DFC constraints remains the bottleneck.**

I would not generalize this beyond the Section 274 case study without testing additional statutes and models.


## One-sentence result

For Section 274, bounded recursive statutory retrieval increased rule-family discovery, but did not improve executable-correct DFC generation, and the bounded encoding errors were perfectly stable across three runs.


## If asked why not continue Layer 4

Layer 4 is not omitted because of an arbitrary depth setting.

It is omitted because the complete candidate frontier requires 1,197,630 statutory tokens, exceeding the 992,498-token statutory-context capacity.

The baseline therefore stops before Layer 4 rather than partially selecting from it.


## If asked about unresolved references

The experiment does not assume every reference is perfectly resolved.

Unresolved structural, missing-section, and deeper cross-code cases remain explicit diagnostics.

The claim is about the complete **admitted resolvable structural frontier under the budget**, not full semantic closure of every statutory dependency.


## If asked what comes next

The next research question is no longer simply retrieval depth.

The Section 274 result suggests separating the pipeline into two stages:

1. legal-rule discovery from retrieved statutory context;
2. faithful compilation of discovered rules into executable DFC constraints.

A natural next experiment is to improve the second stage while keeping the bounded retrieval frontier fixed.

