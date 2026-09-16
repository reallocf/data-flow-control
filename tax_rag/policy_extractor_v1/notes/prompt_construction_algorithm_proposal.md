# Prompt Construction Algorithm Proposal

## Foundation

The REJ16-style resolver serves as the cross-reference foundation.

For the federal tax code, the rerun covers 2,116 sections. The raw XML comparison gives precision 0.9879, recall 0.9980, and F1 0.9929. After adjudicating the identified disagreement categories, the estimate is precision 0.9882, recall 1.0000, and F1 0.9941.

The XML comparator is parser-derived rather than an independently hand-labeled gold set.

Cross-section structural mapping was checked across 17,508 resolved occurrences. After dash normalization, 9,683 map to the exact structural unit, 72 map to the nearest existing ancestor, 7,753 map at section level, and none remain unmapped.

Local structural mapping was checked across 28,851 targets from 28,261 resolved occurrences. Of these targets, 23,139 map exactly, 681 map to the nearest existing ancestor, 5,031 map at section level, and none remain unmapped.

## Goal

Given a target federal tax provision, construct a bounded legal-context prompt for generating candidate DFC policies.

## Reference index

Before extracting any statutory unit, scan each full section once with the REJ16-style resolver.

For every detected occurrence, retain:

- source section
- span start and span end
- source structural path at the occurrence
- reference class
- target section
- target structural path
- resolution status

The source structural path is derived from the section structural tree at the occurrence start position. This preserves the context needed for local references even after a smaller statutory unit is selected.

## Eligible references

Automatic statutory expansion admits only references with status `resolved_section` or `resolved_local`.

A target provision or admitted context containing `needs_structural_resolution`, `unresolved_missing_section`, or `resolved_external` is flagged for additional resolution rather than silently omitted.

Broader `resolved_structure` references are retained as structural metadata and are not expanded automatically in this initial algorithm because they can denote large chapter, subchapter, or part scopes.

## Structural mapping

For each eligible reference:

1. Normalize dash variants in statutory identifiers.
2. Match the target section and target structural path against the LM XML.
3. Select the exact structural unit when available.
4. Otherwise select the nearest existing ancestor.
5. When no target structural path is specified, select the referenced section.
6. Remove duplicate targets. When one selected unit contains another selected unit from the same section, retain the containing unit so statutory text is not repeated.

## Relevance and expansion

The initial relevance criterion is structural distance in the cross-reference graph.

Directly referenced units form layer 1 and have priority. References found inside admitted layer 1 units form layer 2, and the same rule continues breadth-first.

This gives a deterministic baseline without introducing an unvalidated semantic ranking model.

The complete direct layer is mandatory. If it does not fit the legal-context budget, return `DECOMPOSITION_REQUIRED` rather than dropping a direct reference.

After layer 1, admit a complete next layer only when that entire layer fits within the remaining legal-context budget. Pause before the first layer that does not fit.

## Legal-context budget

For the selected language model:

legal_context_budget =
    model_input_limit
    - fixed_instruction_tokens
    - target_provision_tokens
    - output_reserve
    - safety_margin

The model tokenizer determines the actual budget. The four-character token approximation below is diagnostic only.

Across 2,116 section-level starting points, direct mapped context has an approximate p90 of 30,178 tokens and p99 of 88,006 tokens. The maximum is approximately 255,902 tokens. Of 2,116 starting points, 2,108 remain below 128,000 approximate tokens.

For section 274, direct mapped context is approximately 17,825 tokens across 19 mapped blocks.

Prior unrestricted recursive expansion produced impractical context sizes. The bounded layer rule prevents that behavior while retaining every direct statutory reference when the direct layer fits.

## Prompt assembly

Construct the model prompt from:

- DFC policy instructions
- target provision text
- admitted statutory context, with citation and structural identifier for each unit
- unresolved-reference diagnostics, when any exist
- an instruction to generate candidate DFC policies supported by the supplied legal text and identify the supporting provisions

## Pseudocode

build_reference_index(sections):
    index = []

    for section in sections:
        tree = build_structure(section.text)

        for ref in scan_full_section(section.text):
            ref.source_path = path_at(tree, ref.span_start)
            index.append(ref)

    return index


build_prompt(target, index, xml, model_limits):
    target_text = statutory_text(target, xml)
    issues = unresolved_references(target, index)

    if issues is not empty:
        return ADDITIONAL_RESOLUTION_REQUIRED

    budget = legal_context_budget(
        model_limits,
        target_text
    )

    layer = map_merge(
        eligible_direct_references(target, index),
        xml
    )

    if token_count(layer) > budget:
        return DECOMPOSITION_REQUIRED

    admitted = layer
    frontier = layer

    while frontier is not empty:
        issues = unresolved_references(frontier, index)

        if issues is not empty:
            return ADDITIONAL_RESOLUTION_REQUIRED

        candidates = eligible_references_inside(frontier, index)
        next_layer = map_merge(candidates, xml)
        next_layer = remove_covered(next_layer, admitted)

        if next_layer is empty:
            break

        if token_count(admitted + next_layer) > budget:
            break

        admitted = admitted + next_layer
        frontier = next_layer

    return assemble_prompt(target_text, admitted)

