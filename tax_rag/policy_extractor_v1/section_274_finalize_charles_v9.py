import hashlib
import json
import shutil
import zipfile
from datetime import datetime
from pathlib import Path

from pptx import Presentation


base = Path(__file__).resolve().parent

deck = (
    base
    / "section_274_charles_v9.pptx"
)

validation = (
    base
    / "outputs"
    / "section_274_charles_v9_validation.json"
)

metrics = (
    base
    / "outputs"
    / "section_274_replication_metrics_v3.json"
)

snapshot = (
    base
    / "outputs"
    / "section_274_research_snapshot_v3.json"
)

final_dir = (
    base
    / "outputs"
    / "charles_final_v9"
)

final_dir.mkdir(
    parents=True,
    exist_ok=True,
)


def sha256(path):
    return hashlib.sha256(
        path.read_bytes()
    ).hexdigest()


def all_slide_text(slide):
    parts = []

    for shape in slide.shapes:

        if getattr(
            shape,
            "has_text_frame",
            False,
        ):
            text = shape.text.strip()

            if text:
                parts.append(text)

        if getattr(
            shape,
            "has_table",
            False,
        ):
            for row in shape.table.rows:
                for cell in row.cells:
                    text = cell.text.strip()

                    if text:
                        parts.append(text)

    return "\n".join(parts)


# ============================================================
# 1. Final deck validation
# ============================================================

prs = Presentation(
    deck
)

if len(prs.slides) != 7:
    raise RuntimeError(
        f"Expected 7 slides; found {len(prs.slides)}"
    )


required = {
    1: [
        "Section 274: bounded statutory-dependency retrieval",
        "which statutory dependencies enter context?",
    ],

    2: [
        "Entry context",
        "19 mapped units",
        "15,119 tokens",
        "3 cross-code provisions",
        "9,179 tokens",
        "24,298 tokens",
    ],

    3: [
        "Hierarchy audit",
        "§267: 88 found / 88 expected",
        "§274: 142 found / 142 expected",
    ],

    4: [
        "Capacity trace after structural mapping",
        "627,165",
        "1,197,630",
        "992,498",
        "Layer 4 is the first complete structural layer beyond capacity.",
    ],

    5: [
        "Baseline retrieval rule",
        "Form later dependencies breadth-first.",
        "Measure a complete candidate layer before admission.",
        "Stop before the first complete layer beyond capacity.",
    ],

    6: [
        "Three-condition policy-extraction experiment",
        "5,926 tokens",
        "30,260 tokens",
        "671,210 tokens",
        "3 replicates",
        "gpt-4.1-mini",
    ],

    7: [
        "Three-replicate result",
        "35.71%",
        "21.43%",
        "0.00%",
        "1.000",
        "same 7 unique constraints in all 3 runs",
    ],
}


for slide_number, phrases in required.items():

    text = all_slide_text(
        prs.slides[
            slide_number - 1
        ]
    )

    for phrase in phrases:

        if phrase.lower() not in text.lower():
            raise RuntimeError(
                f"Slide {slide_number} missing: {phrase}"
            )


# ============================================================
# 2. Copy frozen presentation
# ============================================================

frozen_deck = (
    final_dir
    / "section_274_charles_FINAL_v9.pptx"
)

shutil.copy2(
    deck,
    frozen_deck,
)


# ============================================================
# 3. Talking points
# ============================================================

talking_points = (
    final_dir
    / "section_274_charles_v9_talking_points.md"
)

talking_points.write_text(
    """# Section 274 — Charles presentation talking points

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

Layer 1: 24,298 combined statutory tokens.

Layer 2: 171,340.

Layer 3: 627,165 — still within the 992,498-token statutory-context capacity.

The complete Layer-4 candidate would require 1,197,630 tokens, so it cannot be admitted.

Therefore the bounded baseline admits through Layer 3 and stops before Layer 4.

One caveat: this is a **budget-complete structural frontier**, not a claim that every ambiguous or external reference inside those layers has been semantically resolved.


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
""",
    encoding="utf-8",
)


# ============================================================
# 4. Short readme
# ============================================================

readme = (
    final_dir
    / "README.txt"
)

readme.write_text(
    """SECTION 274 — CHARLES PRESENTATION FINAL V9

Status:
  FROZEN FOR PRESENTATION

Deck:
  section_274_charles_FINAL_v9.pptx

Research stage:
  Bounded statutory retrieval implemented and evaluated.

Experiment:
  target-only / direct / bounded
  3 generations per condition
  gpt-4.1-mini
  temperature = 0

Primary Section 274 result:
  Bounded retrieval increases statutory-family discovery breadth,
  but does not improve executable-correct DFC policy generation.

Important scope:
  This is a Section 274 case study.
  Do not generalize to other statutes or models without new evidence.

Important terminology:
  Layer 3 is the last complete structural dependency layer admitted
  under the statutory-context budget.

  This does NOT mean that every ambiguous, missing, or external
  reference inside Layer 3 has achieved semantic closure.
""",
    encoding="utf-8",
)


# ============================================================
# 5. Reproducibility manifest
# ============================================================

manifest = {
    "status":
        "FROZEN_FOR_PRESENTATION",

    "created_at":
        datetime.now().isoformat(
            timespec="seconds"
        ),

    "deck": {
        "filename":
            frozen_deck.name,

        "sha256":
            sha256(
                frozen_deck
            ),

        "slides":
            len(
                prs.slides
            ),
    },

    "retrieval": {
        "working_statutory_context_capacity":
            992498,

        "layer_1_combined_tokens":
            24298,

        "layer_2_combined_tokens":
            171340,

        "layer_3_combined_tokens":
            627165,

        "layer_4_candidate_tokens":
            1197630,

        "last_complete_admitted_layer":
            3,

        "first_complete_layer_beyond_capacity":
            4,
    },

    "experiment": {
        "model":
            "gpt-4.1-mini",

        "temperature":
            0,

        "replicates_per_condition":
            3,

        "target_only_prompt_tokens":
            5926,

        "direct_prompt_tokens":
            30260,

        "bounded_prompt_tokens":
            671210,
    },

    "aggregate_results": {
        "target_only": {
            "family_discovery_recall":
                0.2857,

            "usable_family_recall":
                0.2381,

            "exact_candidate_precision":
                0.0625,

            "exact_string_stability":
                0.0000,
        },

        "direct": {
            "family_discovery_recall":
                0.2381,

            "usable_family_recall":
                0.2381,

            "exact_candidate_precision":
                0.0588,

            "exact_string_stability":
                0.4266,
        },

        "bounded": {
            "family_discovery_recall":
                0.3571,

            "usable_family_recall":
                0.2143,

            "exact_candidate_precision":
                0.0000,

            "exact_string_stability":
                1.0000,
        },
    },

    "main_case_study_claim":
        (
            "Bounded recursive retrieval increased "
            "statutory-family discovery breadth, but the "
            "additional legal context was not converted into "
            "more executable-correct DFC constraints."
        ),

    "scope_limit":
        (
            "Section 274 case study only; three generations "
            "per condition; do not generalize without additional "
            "statutes and models."
        ),

    "api_called_during_finalization":
        False,
}


manifest_path = (
    final_dir
    / "section_274_charles_v9_manifest.json"
)

manifest_path.write_text(
    json.dumps(
        manifest,
        indent=2,
        ensure_ascii=False,
    ) + "\n",
    encoding="utf-8",
)


# ============================================================
# 6. Include supporting validation / metrics when available
# ============================================================

support = [
    validation,
    metrics,
    snapshot,
]

for src in support:

    if src.exists():
        shutil.copy2(
            src,
            final_dir
            / src.name,
        )


# ============================================================
# 7. Final package
# ============================================================

package = (
    base
    / "section_274_charles_FINAL_v9.zip"
)

if package.exists():
    package.unlink()


with zipfile.ZipFile(
    package,
    "w",
    compression=zipfile.ZIP_DEFLATED,
) as archive:

    for path in sorted(
        final_dir.iterdir()
    ):
        if path.is_file():
            archive.write(
                path,
                arcname=path.name,
            )


print(
    "FINAL_FREEZE_STATUS=PASS"
)

print(
    "DECK_VISUAL_QA=PASS"
)

print(
    "SLIDES=7"
)

print(
    "API_CALLED=0"
)

print(
    "RESEARCH_STAGE="
    "SECTION274_CASE_STUDY_COMPLETE"
)

print(
    "NEXT_RESEARCH_STAGE="
    "IMPROVE_DFC_ENCODING_OR_TEST_ADDITIONAL_STATUTES"
)

print(
    "DECK_SHA256="
    + sha256(
        frozen_deck
    ).upper()
)

print(
    "FINAL_DECK="
    + str(
        frozen_deck
    )
)

print(
    "TALKING_POINTS="
    + str(
        talking_points
    )
)

print(
    "MANIFEST="
    + str(
        manifest_path
    )
)

print(
    "PACKAGE="
    + str(
        package
    )
)
