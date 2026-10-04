# Section 274 bounded statutory-dependency retrieval — meeting milestone v9

## Research question

Which statutory dependencies should enter the LLM policy-extraction context when recursive dereferencing can exceed the model context budget?

## Deterministic baseline

1. Begin with the target provision.
2. Map direct statutory references to structural units.
3. Remove repeated or contained targets.
4. Include the direct cross-code branch.
5. Expand later dependencies breadth-first.
6. Measure each complete candidate layer before admission.
7. Admit the complete layer only while cumulative statutory context remains within budget.
8. Stop before the first complete layer that does not fit.

The baseline does not partially select from an over-budget dependency layer.

## Section 274 worked example

Direct dependency layer:

- 19 mapped Title-26 units
- 15 Title-26 sections
- 15,119 Title-26 tokens
- 3 direct cross-code provisions
- 9,179 cross-code tokens
- 24,298 combined statutory tokens

Structural hierarchy validation:

- Section 267: 88 / 88 expected structural identifiers
- Section 274: 142 / 142 expected structural identifiers
- zero missing and zero extra structural identifiers in those hierarchy checks

## Exact budget boundary

Working statutory-context capacity:

- 992,498 tokens

Complete-layer trace:

- Layer 1: 24,298 tokens — fits
- Layer 2: 171,340 tokens — fits
- Layer 3: 627,165 tokens — fits
- Layer 4 candidate: 1,197,630 tokens — does not fit

Therefore Layer 3 is the last complete structural dependency layer admitted under the baseline budget.

This is a budget-complete admitted frontier of resolvable structural units. It is not a claim that every ambiguous, missing, or deeper external reference has achieved semantic closure.

## Three-condition policy experiment

Model:

- gpt-4.1-mini
- temperature 0
- judge disabled during generation
- same Section 274 target and extraction instructions
- 3 generations per condition

Frozen prompts:

- target-only: 5,926 prompt tokens
- direct: 30,260 prompt tokens
- bounded: 671,210 prompt tokens

## Three-replicate result

Mean eligible statutory-family discovery recall:

- target-only: 28.57%
- direct: 23.81%
- bounded: 35.71%

Mean usable-family recall:

- target-only: 23.81%
- direct: 23.81%
- bounded: 21.43%

Pooled exact candidate precision:

- target-only: 6.25%
- direct: 5.88%
- bounded: 0.00%

Exact normalized-constraint stability:

- target-only mean Jaccard: 0.0000
- direct mean Jaccard: 0.4266
- bounded mean Jaccard: 1.0000

Bounded reproduced the same seven unique normalized constraints in all three generations.

## Case-study conclusion

For Section 274, bounded recursive statutory retrieval increased statutory-family discovery breadth, but the additional legal context was not converted into more executable-correct DFC constraints.

The bounded failures were highly reproducible under the frozen prompt, which suggests that faithful rule-to-DFC encoding — rather than retrieval breadth alone — is now the main bottleneck.

## Scope

This is a Section 274 case study with three generations per condition.

Do not generalize the result to other statutes or models without additional experiments.

## Meeting interpretation

The current work answers the mentor request for:

- a concrete context-construction algorithm;
- a Section 274 worked example;
- recursive dereferencing and deduplication;
- exact tokenizer budgeting;
- a complete-layer fit test;
- a deterministic over-budget stopping rule;
- examination of structural edge cases;
- a compact presentation;
- an initial completeness/correctness experiment.

Ordering and semantic prioritization remain later experiments.

## Recommended next research stage

Do not deepen Section 274 retrieval before the meeting.

After feedback, choose between:

1. keeping the bounded retrieval frontier fixed and improving faithful DFC encoding; or
2. testing the same retrieval algorithm on additional statutes to measure generality.

The current Section 274 result should remain frozen as the meeting baseline.
