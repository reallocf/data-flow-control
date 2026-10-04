import ast
import hashlib
import json
import re
from pathlib import Path

base = Path(__file__).resolve().parent
outputs = base / "outputs"

import section_274_structural_trace as trace

enc = trace.enc


def tokens(text):
    return len(enc.encode(text))


def clean(value):
    return re.sub(
        r"\s+",
        " ",
        value,
    ).strip()


def render_tax_context(items):
    chunks = []

    for item in items:
        row = trace.section_by_norm[
            item["section"]
        ]

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

    return "\n\n".join(chunks)


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

                    return stmt.value.values

    raise RuntimeError(
        "generate_policy prompt "
        "not found."
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
        return node.slice.value

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
                str(part.value)
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
            result.append(body)

        else:
            raise RuntimeError(
                "Unexpected prompt expression."
            )

    return "".join(result)


# ------------------------------------------------------------
# Recover exact direct Title-26 dependency layer.
# ------------------------------------------------------------

root = trace.map_unit(
    "274",
    (),
)

if root is None:
    raise RuntimeError(
        "Section 274 root missing."
    )

candidates, diagnostics = trace.expand(
    [root],
    1,
)

direct_units = [
    item
    for item in candidates
    if not trace.contains(
        root,
        item,
    )
]

direct_units = trace.merge_units(
    direct_units
)

if len(direct_units) != 19:
    raise RuntimeError(
        "Expected 19 direct Title-26 "
        f"units; found {len(direct_units)}"
    )

direct_tax_context = (
    render_tax_context(
        direct_units
    )
)

direct_tax_tokens = tokens(
    direct_tax_context
)

if direct_tax_tokens != 15119:
    raise RuntimeError(
        "Direct Title-26 token count "
        f"changed: {direct_tax_tokens}"
    )


# ------------------------------------------------------------
# Reuse the frozen direct cross-title snapshots.
# ------------------------------------------------------------

snapshot = json.loads(
    (
        outputs
        / "section_274_cross_title_snapshot.json"
    ).read_text(
        encoding="utf-8"
    )
)

external_blocks = []

for item in snapshot["provisions"]:
    path = (
        base
        / item["snapshot_file"]
    )

    if not path.exists():
        raise FileNotFoundError(path)

    body = clean(
        path.read_text(
            encoding="utf-8"
        )
    )

    sha = hashlib.sha256(
        body.encode("utf-8")
    ).hexdigest()

    if sha != item["sha256"]:
        raise RuntimeError(
            "Cross-title snapshot hash "
            "changed: "
            + str(path)
        )

    external_blocks.append(
        item["citation"]
        + "\n"
        + body
    )

external_context = (
    "\n\n".join(
        external_blocks
    )
)

external_tokens = tokens(
    external_context
)

if (
    external_tokens
    != int(
        snapshot["combined_tokens"]
    )
):
    raise RuntimeError(
        "Cross-title token count "
        "changed."
    )


# ------------------------------------------------------------
# Direct context:
# direct dependencies only, plus direct cross-title text.
# ------------------------------------------------------------

direct_statutory_context = (
    direct_tax_context
    + "\n\n"
    + external_context
)

direct_context = (
    "ADMITTED STATUTORY CONTEXT\n"
    + direct_statutory_context
    + "\n\n"
    + "REFERENCE DIAGNOSTICS\n"
    + (
        "All direct references selected for "
        "materialization are represented above. "
        "No unresolved direct structural dependency "
        "is used as statutory support."
    )
)


# ------------------------------------------------------------
# Render target-only and direct prompts from current
# extract_v1.py prompt template.
# ------------------------------------------------------------

target = trace.section_by_norm[
    "274"
]

target_body = trace.texts[
    "274"
]

parts = prompt_parts(
    base
    / "extract_v1.py"
)

target_prompt = render_prompt(
    parts,
    target,
    target_body,
    "",
)

direct_prompt = render_prompt(
    parts,
    target,
    target_body,
    direct_context,
)


# ------------------------------------------------------------
# Persist canonical comparison artifacts.
# ------------------------------------------------------------

target_prompt_path = (
    outputs
    / "section_274_target_only_prompt.txt"
)

direct_context_path = (
    outputs
    / "section_274_direct_context.txt"
)

direct_prompt_path = (
    outputs
    / "section_274_direct_prompt.txt"
)

target_prompt_path.write_bytes(
    target_prompt.encode("utf-8")
)

direct_context_path.write_text(
    direct_context + "\n",
    encoding="utf-8",
)

direct_prompt_path.write_bytes(
    direct_prompt.encode("utf-8")
)


direct_manifest_units = []

for item in direct_units:
    row = trace.section_by_norm[
        item["section"]
    ]

    rendered = (
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

    direct_manifest_units.append({
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


direct_metrics = {
    "status":
        "DIRECT_MATERIALIZATION",
    "dependency_layer":
        1,
    "tax_code_units":
        len(direct_units),
    "tax_code_context_tokens":
        direct_tax_tokens,
    "cross_title_provisions":
        len(
            snapshot["provisions"]
        ),
    "cross_title_context_tokens":
        external_tokens,
    "combined_statutory_context_tokens":
        tokens(
            direct_statutory_context
        ),
    "assembled_prompt_tokens":
        tokens(direct_prompt),
    "api_called":
        False,
}


direct_manifest = {
    "metrics":
        direct_metrics,
    "tax_code_units":
        direct_manifest_units,
    "cross_title":
        snapshot["provisions"],
}


(
    outputs
    / "section_274_direct_prompt_metrics.json"
).write_text(
    json.dumps(
        direct_metrics,
        indent=2,
        ensure_ascii=False,
    ) + "\n",
    encoding="utf-8",
)

(
    outputs
    / "section_274_direct_context_manifest.json"
).write_text(
    json.dumps(
        direct_manifest,
        indent=2,
        ensure_ascii=False,
    ) + "\n",
    encoding="utf-8",
)


bounded_prompt_path = (
    outputs
    / "section_274_bounded_prompt.txt"
)

bounded_prompt = (
    bounded_prompt_path.read_text(
        encoding="utf-8"
    )
)

expected = {
    "target_only": {
        "prompt_file":
            target_prompt_path.name,
        "tokens":
            tokens(target_prompt),
        "sha256":
            hashlib.sha256(
                target_prompt.encode(
                    "utf-8"
                )
            ).hexdigest(),
    },
    "direct": {
        "prompt_file":
            direct_prompt_path.name,
        "tokens":
            tokens(direct_prompt),
        "sha256":
            hashlib.sha256(
                direct_prompt.encode(
                    "utf-8"
                )
            ).hexdigest(),
    },
    "bounded": {
        "prompt_file":
            bounded_prompt_path.name,
        "tokens":
            tokens(bounded_prompt),
        "sha256":
            hashlib.sha256(
                bounded_prompt.encode(
                    "utf-8"
                )
            ).hexdigest(),
    },
}


if not (
    expected["target_only"]["tokens"]
    < expected["direct"]["tokens"]
    < expected["bounded"]["tokens"]
):
    raise RuntimeError(
        "Expected prompt token ordering "
        "does not hold."
    )


(
    outputs
    / "section_274_comparison_expected.json"
).write_text(
    json.dumps(
        expected,
        indent=2,
    ) + "\n",
    encoding="utf-8",
)


print("PREPARE_STATUS=PASS")
print(
    "TARGET_ONLY_TOKENS="
    f"{expected['target_only']['tokens']}"
)
print(
    "DIRECT_TAX_UNITS="
    f"{len(direct_units)}"
)
print(
    "DIRECT_TAX_TOKENS="
    f"{direct_tax_tokens}"
)
print(
    "DIRECT_CROSS_TITLE_TOKENS="
    f"{external_tokens}"
)
print(
    "DIRECT_PROMPT_TOKENS="
    f"{expected['direct']['tokens']}"
)
print(
    "BOUNDED_PROMPT_TOKENS="
    f"{expected['bounded']['tokens']}"
)
print("API_CALLED=0")
