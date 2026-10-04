import hashlib
import json
from pathlib import Path

import tiktoken

base = Path(__file__).resolve().parent
outputs = base / "outputs"

enc = tiktoken.get_encoding(
    "o200k_base"
)

expected = {
    "target_only":
        outputs
        / "section_274_target_only_prompt.txt",
    "direct":
        outputs
        / "section_274_direct_prompt.txt",
    "bounded":
        outputs
        / "section_274_bounded_prompt.txt",
}

integrated = {
    "target_only":
        outputs
        / "section_274_target_only_prompt_from_extract.txt",
    "direct":
        outputs
        / "section_274_direct_prompt_from_extract.txt",
    "bounded":
        outputs
        / "section_274_bounded_prompt_from_extract.txt",
}


def inspect(path):
    data = path.read_bytes()

    text = data.decode(
        "utf-8"
    )

    return {
        "bytes":
            len(data),
        "tokens":
            len(
                enc.encode(text)
            ),
        "sha256":
            hashlib.sha256(
                data
            ).hexdigest(),
    }


report = {
    "api_called":
        False,
    "conditions": {},
}

for mode in (
    "target_only",
    "direct",
    "bounded",
):
    left = inspect(
        expected[mode]
    )

    right = inspect(
        integrated[mode]
    )

    report[
        "conditions"
    ][mode] = {
        "expected":
            left,
        "integrated":
            right,
        "hash_match":
            (
                left["sha256"]
                == right["sha256"]
            ),
        "token_match":
            (
                left["tokens"]
                == right["tokens"]
            ),
        "byte_match":
            (
                left["bytes"]
                == right["bytes"]
            ),
    }


report["all_hashes_match"] = all(
    item["hash_match"]
    for item
    in report[
        "conditions"
    ].values()
)

report["all_token_counts_match"] = all(
    item["token_match"]
    for item
    in report[
        "conditions"
    ].values()
)

report["all_byte_counts_match"] = all(
    item["byte_match"]
    for item
    in report[
        "conditions"
    ].values()
)

target_tokens = (
    report[
        "conditions"
    ]["target_only"][
        "expected"
    ]["tokens"]
)

direct_tokens = (
    report[
        "conditions"
    ]["direct"][
        "expected"
    ]["tokens"]
)

bounded_tokens = (
    report[
        "conditions"
    ]["bounded"][
        "expected"
    ]["tokens"]
)

report["strict_token_order"] = (
    target_tokens
    < direct_tokens
    < bounded_tokens
)

report["ready_for_generation"] = (
    report["all_hashes_match"]
    and report[
        "all_token_counts_match"
    ]
    and report[
        "all_byte_counts_match"
    ]
    and report[
        "strict_token_order"
    ]
)


path = (
    outputs
    / "section_274_three_mode_preflight.json"
)

path.write_text(
    json.dumps(
        report,
        indent=2,
    ) + "\n",
    encoding="utf-8",
)


print(
    "TARGET_ONLY_TOKENS="
    f"{target_tokens}"
)

print(
    "DIRECT_TOKENS="
    f"{direct_tokens}"
)

print(
    "BOUNDED_TOKENS="
    f"{bounded_tokens}"
)

print(
    "TARGET_HASH_MATCH="
    + str(
        report[
            "conditions"
        ]["target_only"][
            "hash_match"
        ]
    ).upper()
)

print(
    "DIRECT_HASH_MATCH="
    + str(
        report[
            "conditions"
        ]["direct"][
            "hash_match"
        ]
    ).upper()
)

print(
    "BOUNDED_HASH_MATCH="
    + str(
        report[
            "conditions"
        ]["bounded"][
            "hash_match"
        ]
    ).upper()
)

print(
    "STRICT_TOKEN_ORDER="
    + str(
        report[
            "strict_token_order"
        ]
    ).upper()
)

print(
    "READY_FOR_GENERATION="
    + str(
        report[
            "ready_for_generation"
        ]
    ).upper()
)

print("API_CALLED=0")

if not report[
    "ready_for_generation"
]:
    raise RuntimeError(
        "Three-mode preflight failed."
    )
