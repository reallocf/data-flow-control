import ast
import csv
import hashlib
import json
import os
import re
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import requests
from bs4 import BeautifulSoup

base = Path(__file__).resolve().parent
inputs = base / "inputs"
outputs = base / "outputs"
notes = base / "notes"

outputs.mkdir(exist_ok=True)
notes.mkdir(exist_ok=True)

import section_274_structural_trace as trace

budget = json.loads(
    (
        outputs
        / "section_274_budget_exact.json"
    ).read_text(
        encoding="utf-8"
    )
)

enc = trace.enc

capacity = int(
    budget["legal_context_capacity"]
)

context_window = int(
    budget["context_window"]
)

output_reserve = int(
    budget["output_reserve"]
)

safety_margin = int(
    budget["safety_margin"]
)

working_input_limit = (
    context_window
    - output_reserve
    - safety_margin
)


def tokens(text):
    return len(enc.encode(text))


def clean(value):
    return re.sub(
        r"\s+",
        " ",
        value,
    ).strip()


def fetch_page_text(url):
    response = requests.get(
        url,
        timeout=45,
        headers={
            "User-Agent":
                "Mozilla/5.0"
        },
    )

    response.raise_for_status()

    soup = BeautifulSoup(
        response.text,
        "html.parser",
    )

    for tag in soup([
        "script",
        "style",
        "nav",
        "header",
        "footer",
        "aside",
    ]):
        tag.decompose()

    heading = soup.find("h1")

    if heading is None:
        raise RuntimeError(
            f"Heading absent at {url}"
        )

    heading_text = clean(
        heading.get_text(
            " ",
            strip=True,
        )
    )

    flat = clean(
        soup.get_text(
            " ",
            strip=True,
        )
    )

    start = flat.find(
        heading_text
    )

    if start < 0:
        raise RuntimeError(
            f"Heading location absent at {url}"
        )

    return flat[
        start + len(heading_text):
    ].strip()


def extract_between(
    text,
    start_pattern,
    end_pattern,
):
    start = re.search(
        start_pattern,
        text,
        flags=re.I | re.S,
    )

    if start is None:
        raise RuntimeError(
            "External start boundary absent: "
            + start_pattern
        )

    tail = text[start.end():]

    end = re.search(
        end_pattern,
        tail,
        flags=re.I | re.S,
    )

    if end is None:
        raise RuntimeError(
            "External end boundary absent: "
            + end_pattern
        )

    return tail[
        :end.start()
    ].strip()


external_specs = [
    {
        "surface":
            "section 16(a) of the Securities Exchange Act of 1934",
        "citation":
            "15 U.S.C. § 78p(a)",
        "url":
            "https://www.law.cornell.edu/uscode/text/15/78p",
        "start":
            r"\(a\)\s*Disclosures required\b",
        "end":
            r"\(b\)\s*Profits from purchase and sale of security within six months\b",
        "checks": (
            "beneficial owner",
            "shall file",
        ),
        "file":
            "15usc_78p_a.txt",
    },
    {
        "surface":
            "section 212(a)(1)(A) of the Caribbean Basin Economic Recovery Act",
        "citation":
            "19 U.S.C. § 2702(a)(1)(A)",
        "url":
            "https://www.law.cornell.edu/uscode/text/19/2702",
        "start": (
            r"\(a\)\s*Definitions; termination of designation\s*"
            r"\(1\)\s*For purposes of this chapter\s*[—-]\s*"
            r"\(A\)"
        ),
        "end":
            r"\(B\)\s*The term",
        "checks": (
            "beneficiary country",
            "proclamation",
        ),
        "file":
            "19usc_2702_a_1_A.txt",
    },
    {
        "surface":
            "section 2101 of title 46",
        "citation":
            "46 U.S.C. § 2101",
        "url":
            "https://www.law.cornell.edu/uscode/text/46/2101",
        "start":
            r"In this subtitle\s*[—-]",
        "end":
            r"Editorial Notes\b",
        "checks": (
            "associated equipment",
            "vessel",
        ),
        "file":
            "46usc_2101.txt",
    },
]


snapshot_dir = (
    inputs
    / "cross_title"
)

snapshot_dir.mkdir(
    parents=True,
    exist_ok=True,
)

refresh_external = (
    os.getenv(
        "REFRESH_CROSS_TITLE",
        "0",
    ).strip().lower()
    in {
        "1",
        "true",
        "yes",
    }
)

external_rows = []
external_blocks = []

for spec in external_specs:
    path = (
        snapshot_dir
        / spec["file"]
    )

    retrieved_utc = None

    if (
        refresh_external
        or not path.exists()
    ):
        page = fetch_page_text(
            spec["url"]
        )

        body = extract_between(
            page,
            spec["start"],
            spec["end"],
        )

        retrieved_utc = (
            datetime.now(
                timezone.utc
            ).isoformat()
        )

        path.write_text(
            body + "\n",
            encoding="utf-8",
        )
    else:
        body = path.read_text(
            encoding="utf-8"
        ).strip()

    body = clean(body)
    lower = body.lower()

    if not all(
        term.lower() in lower
        for term
        in spec["checks"]
    ):
        raise RuntimeError(
            "External text validation "
            "failed for "
            + spec["citation"]
        )

    rendered = (
        spec["citation"]
        + "\n"
        + body
    )

    external_rows.append({
        "surface":
            spec["surface"],
        "citation":
            spec["citation"],
        "url":
            spec["url"],
        "snapshot_file":
            str(
                path.relative_to(
                    base
                )
            ),
        "retrieved_utc":
            retrieved_utc,
        "characters":
            len(body),
        "tokens":
            tokens(rendered),
        "sha256":
            hashlib.sha256(
                body.encode(
                    "utf-8"
                )
            ).hexdigest(),
    })

    external_blocks.append(
        rendered
    )


external_context = (
    "\n\n".join(
        external_blocks
    )
)

external_tokens = tokens(
    external_context
)


direct_external = [
    row
    for row
    in trace.diagnostic_rows
    if (
        int(
            row.get(
                "layer",
                -1,
            )
        ) == 1
        and row.get(
            "status"
        ) == "resolved_external"
    )
]

if (
    len(direct_external)
    != len(external_specs)
):
    raise RuntimeError(
        "Unexpected direct "
        "external-reference count: "
        f"{len(direct_external)}"
    )

expected_surfaces = {
    spec["surface"].lower()
    for spec in external_specs
}

observed_surfaces = {
    str(
        row.get(
            "surface",
            "",
        )
    ).lower()
    for row in direct_external
}

if (
    expected_surfaces
    != observed_surfaces
):
    raise RuntimeError(
        "Direct external-reference "
        "set does not match the "
        "materialized cross-title branch."
    )


def render_tax_context(
    items,
):
    chunks = []

    for item in items:
        row = (
            trace.section_by_norm[
                item["section"]
            ]
        )

        chunks.append(
            row.get(
                "citation",
                "",
            )
            + " "
            + item["identifier"]
            + "\n"
            + trace.node_text(
                item["node"]
            )
        )

    return "\n\n".join(
        chunks
    )


root = trace.map_unit(
    "274",
    (),
)

if root is None:
    raise RuntimeError(
        "Section 274 XML root missing."
    )

coverage = [root]
admitted = []
frontier = [root]

admitted_diagnostics = []
layer_manifest = []

last_fit = 0
first_over = None

for next_layer in range(
    1,
    20,
):
    candidates, diagnostics = (
        trace.expand(
            frontier,
            next_layer,
        )
    )

    # These records originate in the
    # current frontier, which is already
    # part of the materialized prompt.
    for row in diagnostics:
        item = dict(row)

        item[
            "source_dependency_layer"
        ] = next_layer - 1

        admitted_diagnostics.append(
            item
        )

    candidates = [
        item
        for item in candidates
        if not any(
            trace.contains(
                old,
                item,
            )
            for old in coverage
        )
    ]

    before = {
        (
            item["section"],
            item["path"],
        )
        for item in admitted
    }

    merged = trace.merge_units(
        admitted
        + candidates
    )

    next_frontier = [
        item
        for item in merged
        if (
            item["section"],
            item["path"],
        ) not in before
    ]

    tax_context = (
        render_tax_context(
            merged
        )
    )

    tax_tokens = tokens(
        tax_context
    )

    statutory_context = (
        "\n\n".join(
            part
            for part in (
                tax_context,
                external_context,
            )
            if part
        )
    )

    statutory_tokens = tokens(
        statutory_context
    )

    fits = (
        statutory_tokens
        <= capacity
    )

    layer_manifest.append({
        "layer":
            next_layer,
        "new_units":
            len(
                next_frontier
            ),
        "cumulative_units":
            len(merged),
        "tax_code_context_tokens":
            tax_tokens,
        "cross_title_context_tokens":
            external_tokens,
        "combined_statutory_context_tokens":
            statutory_tokens,
        "legal_context_capacity":
            capacity,
        "fits":
            fits,
    })

    if not fits:
        first_over = (
            next_layer
        )
        break

    last_fit = next_layer
    admitted = merged

    coverage = (
        trace.merge_units(
            [root]
            + admitted
        )
    )

    frontier = next_frontier

    if not frontier:
        break


if last_fit < 1:
    raise RuntimeError(
        "Direct statutory layer "
        "does not fit."
    )


trace_summary = json.loads(
    (
        outputs
        / "section_274_structural_trace.json"
    ).read_text(
        encoding="utf-8"
    )
)

if (
    last_fit
    != int(
        trace_summary[
            "conditional_last_complete_structural_layer"
        ]
    )
):
    raise RuntimeError(
        "Last-fit layer disagrees "
        "with structural trace."
    )

if (
    first_over
    != int(
        trace_summary[
            "conditional_first_structural_layer_beyond_capacity"
        ]
    )
):
    raise RuntimeError(
        "First-over layer disagrees "
        "with structural trace."
    )


trace_layer = next(
    row
    for row
    in trace_summary["layers"]
    if int(
        row["layer"]
    ) == last_fit
)

final_tax_context = (
    render_tax_context(
        admitted
    )
)

final_tax_tokens = tokens(
    final_tax_context
)

if (
    final_tax_tokens
    != int(
        trace_layer[
            "context_tokens"
        ]
    )
):
    raise RuntimeError(
        "Tax-code materialization "
        "does not match structural trace: "
        f"{final_tax_tokens} != "
        f"{trace_layer['context_tokens']}"
    )


diagnostic_rows = []

for row in admitted_diagnostics:
    if (
        int(
            row.get(
                "source_dependency_layer",
                -1,
            )
        ) == 0
        and row.get(
            "status"
        ) == "resolved_external"
        and str(
            row.get(
                "surface",
                "",
            )
        ).lower()
        in expected_surfaces
    ):
        continue

    diagnostic_rows.append(
        row
    )


diagnostic_counter = Counter()

for row in diagnostic_rows:
    source_path = row.get(
        "source_path",
        (),
    )

    if isinstance(
        source_path,
        (
            tuple,
            list,
        ),
    ):
        source_path = (
            ".".join(
                source_path
            )
        )

    key = (
        int(
            row.get(
                "source_dependency_layer",
                -1,
            )
        ),
        str(
            row.get(
                "status",
                "",
            )
        ),
        str(
            row.get(
                "source_section",
                "",
            )
        ),
        str(source_path),
        str(
            row.get(
                "surface",
                "",
            )
        ),
        str(
            row.get(
                "target_section",
                "",
            )
        ),
        str(
            row.get(
                "target_path",
                "",
            )
        ),
    )

    diagnostic_counter[key] += 1


compact_diagnostics = []

for key, count in sorted(
    diagnostic_counter.items()
):
    (
        source_layer,
        status,
        source_section,
        source_path,
        surface,
        target_section,
        target_path,
    ) = key

    compact_diagnostics.append({
        "source_dependency_layer":
            source_layer,
        "status":
            status,
        "source_section":
            source_section,
        "source_path":
            source_path,
        "surface":
            surface,
        "target_section":
            target_section,
        "target_path":
            target_path,
        "occurrences":
            count,
    })


statutory_context = (
    "\n\n".join(
        part
        for part in (
            final_tax_context,
            external_context,
        )
        if part
    )
)

diagnostic_lines = [
    "REFERENCE DIAGNOSTICS",
    (
        "The records below are dependency "
        "metadata, not statutory text. "
        "Do not use a diagnostic record "
        "itself as support for a policy."
    ),
]

for row in compact_diagnostics:
    source = (
        row["source_section"]
    )

    if row["source_path"]:
        source += (
            ":"
            + row[
                "source_path"
            ]
        )

    target = (
        row["target_section"]
    )

    if row["target_path"]:
        target += (
            ":"
            + row[
                "target_path"
            ]
        )

    diagnostic_lines.append(
        (
            "- source_layer="
            "{source_dependency_layer}; "
            "status={status}; "
            "source={source}; "
            "surface={surface!r}; "
            "target={target}; "
            "occurrences={occurrences}"
        ).format(
            source=source,
            target=target,
            **row,
        )
    )

diagnostic_text = (
    "\n".join(
        diagnostic_lines
    )
)

context = (
    "ADMITTED STATUTORY CONTEXT\n"
    + statutory_context
    + "\n\n"
    + diagnostic_text
)


def prompt_parts(path):
    tree = ast.parse(
        path.read_text(
            encoding="utf-8-sig"
        )
    )

    for node in tree.body:
        if (
            isinstance(
                node,
                ast.FunctionDef,
            )
            and node.name
            == "generate_policy"
        ):
            for stmt in node.body:
                if (
                    isinstance(
                        stmt,
                        ast.Assign,
                    )
                    and any(
                        isinstance(
                            target,
                            ast.Name,
                        )
                        and target.id
                        == "prompt"
                        for target
                        in stmt.targets
                    )
                ):
                    if not isinstance(
                        stmt.value,
                        ast.JoinedStr,
                    ):
                        raise RuntimeError(
                            "Prompt form changed."
                        )

                    return (
                        stmt.value.values
                    )

    raise RuntimeError(
        "generate_policy "
        "prompt not found."
    )


def section_key(node):
    if (
        isinstance(
            node,
            ast.Subscript,
        )
        and isinstance(
            node.value,
            ast.Name,
        )
        and node.value.id
        == "section"
        and isinstance(
            node.slice,
            ast.Constant,
        )
    ):
        return (
            node.slice.value
        )

    return None


def render_prompt(
    parts,
    section,
    body,
    referenced_context,
):
    result = []

    for part in parts:
        if isinstance(
            part,
            ast.Constant,
        ):
            result.append(
                str(
                    part.value
                )
            )
            continue

        if not isinstance(
            part,
            ast.FormattedValue,
        ):
            raise RuntimeError(
                "Unexpected prompt element."
            )

        value = part.value

        if (
            isinstance(
                value,
                ast.Name,
            )
            and value.id
            == "context"
        ):
            result.append(
                referenced_context
            )

        elif (
            section_key(value)
            == "citation"
        ):
            result.append(
                section[
                    "citation"
                ]
            )

        elif (
            isinstance(
                value,
                ast.Subscript,
            )
            and section_key(
                value.value
            ) == "text"
        ):
            result.append(
                body
            )

        else:
            raise RuntimeError(
                "Unexpected prompt expression."
            )

    return "".join(result)


target = (
    trace.section_by_norm[
        "274"
    ]
)

target_body = (
    trace.texts["274"]
)

parts = prompt_parts(
    base
    / "extract_v1.py"
)

prompt = render_prompt(
    parts,
    target,
    target_body,
    context,
)

prompt_tokens = tokens(
    prompt
)

headroom = (
    working_input_limit
    - prompt_tokens
)

status_counts = Counter(
    row["status"]
    for row
    in compact_diagnostics
)

prompt_status = (
    "READY_FOR_POLICY_GENERATION"
    if not compact_diagnostics
    else "CONDITIONAL_MATERIALIZATION"
)

if (
    prompt_tokens
    > working_input_limit
):
    raise RuntimeError(
        "Assembled prompt exceeds "
        "working input limit: "
        f"{prompt_tokens} > "
        f"{working_input_limit}"
    )


manifest_units = []

for item in admitted:
    row = (
        trace.section_by_norm[
            item["section"]
        ]
    )

    rendered = (
        row.get(
            "citation",
            "",
        )
        + " "
        + item[
            "identifier"
        ]
        + "\n"
        + trace.node_text(
            item["node"]
        )
    )

    manifest_units.append({
        "section":
            item["section"],
        "path":
            ".".join(
                item["path"]
            ),
        "identifier":
            item["identifier"],
        "mapping":
            item["mapping"],
        "citation":
            row.get(
                "citation",
                "",
            ),
        "tokens":
            tokens(rendered),
    })


metrics = {
    "status":
        prompt_status,
    "model":
        budget["model"],
    "tokenizer":
        budget["tokenizer"],
    "context_window":
        context_window,
    "output_reserve":
        output_reserve,
    "safety_margin":
        safety_margin,
    "working_input_limit":
        working_input_limit,
    "legal_context_capacity":
        capacity,
    "last_complete_layer":
        last_fit,
    "first_layer_beyond_capacity":
        first_over,
    "admitted_tax_code_units":
        len(admitted),
    "tax_code_context_tokens":
        final_tax_tokens,
    "cross_title_provisions":
        len(external_rows),
    "cross_title_context_tokens":
        external_tokens,
    "combined_statutory_context_tokens":
        tokens(
            statutory_context
        ),
    "diagnostic_occurrences":
        sum(
            diagnostic_counter.values()
        ),
    "diagnostic_distinct_records":
        len(
            compact_diagnostics
        ),
    "diagnostic_status_counts":
        dict(
            sorted(
                status_counts.items()
            )
        ),
    "assembled_prompt_tokens":
        prompt_tokens,
    "working_headroom_tokens":
        headroom,
    "overlay_entries":
        len(
            trace.resolution_overlay
        ),
    "overlay_applied":
        len(
            trace.overlay_applied
        ),
    "api_called":
        False,
}


manifest = {
    "metrics":
        metrics,
    "layers":
        layer_manifest,
    "tax_code_units":
        manifest_units,
    "cross_title":
        external_rows,
    "diagnostics":
        compact_diagnostics,
}


(
    outputs
    / "section_274_bounded_context.txt"
).write_text(
    context + "\n",
    encoding="utf-8",
)

(
    outputs
    / "section_274_bounded_prompt.txt"
).write_bytes(
    prompt.encode("utf-8")
)

(
    outputs
    / "section_274_bounded_prompt_metrics.json"
).write_text(
    json.dumps(
        metrics,
        indent=2,
        ensure_ascii=False,
    ) + "\n",
    encoding="utf-8",
)

(
    outputs
    / "section_274_bounded_context_manifest.json"
).write_text(
    json.dumps(
        manifest,
        indent=2,
        ensure_ascii=False,
    ) + "\n",
    encoding="utf-8",
)

(
    outputs
    / "section_274_cross_title_snapshot.json"
).write_text(
    json.dumps(
        {
            "tokenizer":
                enc.name,
            "combined_tokens":
                external_tokens,
            "provisions":
                external_rows,
        },
        indent=2,
        ensure_ascii=False,
    ) + "\n",
    encoding="utf-8",
)


with (
    outputs
    / "section_274_bounded_diagnostics.csv"
).open(
    "w",
    encoding="utf-8",
    newline="",
) as handle:

    fields = [
        "source_dependency_layer",
        "status",
        "source_section",
        "source_path",
        "surface",
        "target_section",
        "target_path",
        "occurrences",
    ]

    writer = csv.DictWriter(
        handle,
        fieldnames=fields,
    )

    writer.writeheader()
    writer.writerows(
        compact_diagnostics
    )


note = [
    "# Section 274 bounded prompt materialization",
    "",
    f"Status: {prompt_status}.",
    (
        "Last complete admitted "
        f"dependency layer: {last_fit}."
    ),
    (
        "First complete layer "
        f"beyond capacity: {first_over}."
    ),
    (
        "Admitted Title 26 "
        f"structural units: {len(admitted)}."
    ),
    (
        "Title 26 context tokens: "
        f"{final_tax_tokens:,}."
    ),
    (
        "Direct cross-title provisions: "
        f"{len(external_rows)} "
        f"({external_tokens:,} tokens)."
    ),
    (
        "Assembled prompt tokens: "
        f"{prompt_tokens:,}."
    ),
    (
        "Working input limit: "
        f"{working_input_limit:,}."
    ),
    (
        "Working headroom: "
        f"{headroom:,} tokens."
    ),
    (
        "Distinct diagnostic records "
        "retained: "
        f"{len(compact_diagnostics)}."
    ),
    "",
    (
        "The prompt is materialized "
        "without an API call. "
        "Diagnostic metadata is kept "
        "separate from statutory text "
        "and is not itself legal support "
        "for policy generation."
    ),
]

(
    notes
    / "section_274_bounded_prompt.md"
).write_text(
    "\n".join(note)
    + "\n",
    encoding="utf-8",
)


print(
    "MATERIALIZATION_STATUS="
    + prompt_status
)

print(
    f"LAST_COMPLETE_LAYER={last_fit}"
)

print(
    f"FIRST_LAYER_BEYOND={first_over}"
)

print(
    "ADMITTED_TAX_UNITS="
    f"{len(admitted)}"
)

print(
    "TAX_CONTEXT_TOKENS="
    f"{final_tax_tokens}"
)

print(
    "CROSS_TITLE_TOKENS="
    f"{external_tokens}"
)

print(
    "DIAGNOSTIC_OCCURRENCES="
    f"{sum(diagnostic_counter.values())}"
)

print(
    "DIAGNOSTIC_DISTINCT="
    f"{len(compact_diagnostics)}"
)

print(
    "ASSEMBLED_PROMPT_TOKENS="
    f"{prompt_tokens}"
)

print(
    "WORKING_INPUT_LIMIT="
    f"{working_input_limit}"
)

print(
    f"HEADROOM={headroom}"
)

print(
    "OVERLAY="
    f"{len(trace.overlay_applied)}/"
    f"{len(trace.resolution_overlay)}"
)

print("API_CALLED=0")
