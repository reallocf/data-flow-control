# REJ16 baseline for section 274

## Purpose

This note records the small REJ16-style step added for section 274. The goal is not to reproduce the whole paper. The goal is to test whether a compact rule layer can connect structural references in section 274 to the matching provision nodes before policy extraction.

## What REJ16 contributes here

REJ16 separates reference handling into three tasks:

1. find reference phrases in the legal text;
2. read each phrase with its surrounding provision structure;
3. connect the phrase to the target provision.

For this project, the useful part is the second and third tasks. The earlier audit already found the phrases. The new step builds a section 274 provision tree and maps each structural reference to a target node.

## Current implementation

The script `resolve_refs_274.py` reads:

- `inputs/title26_sections.jsonl`
- `outputs/reference_audit_274_main.csv`

It writes:

- `outputs/struct_ref_resolution_274.csv`

The output has 49 structural references marked as resolved.

## Checked examples

I checked representative cases covering different levels of the section 274 tree:

| surface form | source node | resolved target |
|---|---|---|
| subparagraph (A) | section 274 opening area | 274(a)(1)(A) |
| subsection (i) | section 274 opening area | 274(i) |
| subsection (d) | section 274 opening area | 274(d) |
| subsection (e)(8) | section 274 opening area | 274(e)(8) |
| paragraph (1) | nested section 274 context | matching paragraph under the nearest valid parent |
| subparagraph (B) | nested section 274 context | matching subparagraph under the nearest valid parent |

## Scope

This is a baseline. It is meant to support discussion before adding it to the broader extraction flow. The next decision is whether these resolved references are accurate enough to feed into policy extraction, or whether some cases need a second review step.
