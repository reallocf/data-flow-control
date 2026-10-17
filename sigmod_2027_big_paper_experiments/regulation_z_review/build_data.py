"""Rebuild the offline review snapshot from saved CFPB HTML and the live catalog."""

from __future__ import annotations

import ast
from datetime import datetime, timezone
import hashlib
from html.parser import HTMLParser
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from vldb_experiments.strategies.regulation_z_53_full_policy_catalog import (
    FULL_REGULATION_Z_53_POLICY_SPECS,
)

ROOT = Path(__file__).resolve().parent
PROJECT = ROOT.parent


class Paragraphs(HTMLParser):
    def __init__(self):
        super().__init__()
        self.key = None
        self.parts = []
        self.rows = {}

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if tag == "p" and "regdown-block" in attrs.get("class", ""):
            self.key = attrs.get("id")
            self.parts = []

    def handle_data(self, data):
        if self.key:
            self.parts.append(data)

    def handle_endtag(self, tag):
        if tag == "p" and self.key:
            self.rows[self.key] = " ".join("".join(self.parts).split())
            self.key = None


sources = {}
metadata = []
for section in (53, 16):
    path = ROOT / f"sources/1026-{section}.html"
    raw = path.read_bytes()
    parser = Paragraphs()
    parser.feed(raw.decode())
    url = f"https://www.consumerfinance.gov/rules-policy/regulations/1026/{section}/"
    metadata.append(
        {
            "url": url,
            "retrieved": "2026-10-14",
            "version": "CFPB page marked current regulation at retrieval; no effective-date revision inferred",
            "sha256": hashlib.sha256(raw).hexdigest(),
            "file": f"sources/1026-{section}.html",
        }
    )
    for anchor, text in parser.rows.items():
        if section == 16 and anchor not in {"h-2", "16-h-Interp-1", "16-h-Interp-2"}:
            continue
        sources[f"{section}:{anchor}"] = {
            "anchor": anchor,
            "text": text,
            "url": url + "#" + anchor,
            "kind": "Official interpretation" if "Interp" in anchor else "Regulation",
            "external": section == 16,
        }

# Each tuple is the proposition's source reference(s), exact highlight phrases,
# and an analysis paraphrase. Whitespace is normalized; source wording is retained.
A = "53:53-a-Interp-"
B = "53:53-b-Interp-"
MAP = {
    1: (
        ["53:a"],
        ["highest annual percentage rate"],
        "Except for paragraph (b), allocate excess payment first to the highest-APR balance on an in-scope account.",
    ),
    2: (
        ["53:a"],
        ["any remaining portion", "descending order"],
        "Under the general rule, allocate the remaining excess to other balances in descending APR order.",
    ),
    3: (
        ["53:b-1", B + "1"],
        ["not obligated to pay interest", "paid in full"],
        "The deferred-program provisions apply when accrued interest is avoided through full payoff within the specified period.",
    ),
    4: (
        ["53:b-1", "53:b-1-i"],
        ["consistent with paragraph (a)", "except that"],
        "Under the default branch, use ordinary allocation outside the deferred-priority window.",
    ),
    5: (
        ["53:b-1", "53:b-1-i"],
        ["two billing cycles", "allocated first"],
        "Under the default branch, prioritize the deferred balance during the two billing cycles immediately preceding expiration.",
    ),
    6: (
        ["53:b-1", "53:b-1-i"],
        ["any remaining portion"],
        "After deferred priority, allocate the remainder to other balances under the general rule.",
    ),
    7: (
        ["53:b-1", "53:b-1-ii"],
        ["may at its option", "manner requested"],
        "For a qualifying deferred-program account, the issuer may elect the consumer's requested allocation among account balances.",
    ),
    8: (
        ["53:b-2"],
        ["is secured", "at its option", "if requested by the consumer"],
        "For a balance secured by a security interest, the issuer may elect the consumer's requested allocation to that balance.",
    ),
    9: (
        [A + "1"],
        ["in excess of the minimum periodic payment"],
        "The section addresses payment amounts above the required minimum.",
    ),
    10: (
        [A + "1"],
        ["does not limit or otherwise address", "amount of the required minimum"],
        "This section does not determine the required minimum amount; other applicable law and guidance remain relevant.",
    ),
    11: (
        [A + "1"],
        ["how that payment is allocated"],
        "This section does not prescribe allocation of the required minimum payment.",
    ),
    12: (
        [A + "1"],
        ["may, but is not required to"],
        "The issuer may voluntarily apply these allocation requirements to the minimum, consistently with other law and guidance.",
    ),
    13: (
        [A + "2"],
        [
            "day the preceding billing cycle ends",
            "day the payment is credited",
            "any day in between",
        ],
        "The issuer may use rates and balances from cycle end, credit date, or an intervening date.",
    ),
    14: (
        [A + "2"],
        ["generally must be consistent"],
        "The reference day generally must be consistent across billing cycles.",
    ),
    15: (
        [A + "2"],
        ["may adjust this day from time to time"],
        "The issuer may adjust the reference day from time to time.",
    ),
    16: (
        [A + "2-i", A + "2-ii"],
        ["remaining $50", "$100 purchase made on April 5"],
        "The historical-snapshot example applies residual payment to a later purchase after the historical balances are paid. Generalizing this example requires a modeling choice.",
    ),
    17: (
        [A + "3"],
        ["must apply", "avoids or minimizes"],
        "When the stated claim, defense, or billing-error conditions apply, allocate payment to avoid or minimize reduction of the disputed amount.",
    ),
    18: (
        [A + "3-i", A + "3-ii"],
        ["If there are no new transactions or other amounts", "may apply"],
        "In the example, the issuer may apply the residual to the disputed amount when no new transaction or other amount can receive it.",
    ),
    19: (
        [A + "3-i", A + "3-ii"],
        ["subsequently determines", "must credit", "related finance or other charges"],
        "If the asserted billing error is subsequently confirmed, credit the disputed amount and related charges.",
    ),
    20: (
        [A + "3-i", A + "3-ii"],
        ["send a correction notice", "1026.13(e)"],
        "Following the stated error determination, send a correction notice consistent with §1026.13(e).",
    ),
    21: (
        [A + "4"],
        ["generally does not require", "any particular method"],
        "When the stated equal-rate and other-rate conditions hold, no particular allocation method among equal-rate balances is generally required.",
    ),
    22: (
        [A + "4", A + "5-iv"],
        ["single balance or separate balances", "manner the card issuer deems appropriate"],
        "Under the stated equal-rate circumstances, the issuer may treat equal-rate balances together or separately.",
    ),
    23: (
        [A + "4"],
        ["annual percentage rate of zero"],
        "During the deferred period, treat the qualifying balance as zero APR for allocation purposes.",
    ),
    24: (
        [B + "1", "16:h-2", "16:16-h-Interp-1", "16:16-h-Interp-2"],
        [
            "same meaning",
            "waived or refunded",
            "recurring grace period",
            "skip payment",
            "courtesy period",
        ],
        "Use the incorporated §1026.16(h)(2) deferred-interest definition and associated commentary. External text is shown for review, not counted as additional propositions.",
    ),
    25: (
        [B + "1"],
        ["regardless of whether"],
        "Interim payment requirements do not remove a qualifying balance from deferred-program applicability.",
    ),
    26: (
        [B + "1"],
        ["a grace period", "is not a deferred interest"],
        "The described grace period is excluded from deferred-program treatment.",
    ),
    27: (
        [B + "1"],
        ["temporary annual percentage rate of zero", "unless"],
        "A temporary zero APR is excluded unless accrued interest can become payable on failure to pay in full by expiration.",
    ),
    28: (
        [B + "2"],
        ["does not constitute", "precedes the payment due date"],
        "Do not count a cycle among the last two if program expiration precedes its payment due date.",
    ),
    29: (
        [B + "2"],
        ["April and May", "before June 15"],
        "The early-expiration example extends priority beyond the counted cycles to payments before expiration.",
    ),
    30: (
        [B + "3-i"],
        ["may decline consumer requests", "as a general matter"],
        "The issuer may decline requests while allocating under the applicable default rule.",
    ),
    31: (
        [B + "3-i"],
        ["does not comply with requirements set by the card issuer"],
        "The issuer may decline nonconforming requests while using the applicable default rule.",
    ),
    32: (
        [B + "3-i"],
        ["if the consumer does not submit a request"],
        "An issuer that accepts requests must use the applicable default when no request is submitted.",
    ),
    33: (
        [B + "3-i"],
        ["cannot comply", "unless the consumer submits an additional request"],
        "Use the applicable default for an unfulfillable request unless a further fulfillable request is submitted.",
    ),
    34: (
        [B + "3-ii", B + "3-ii-A"],
        ["orally, electronically, or in writing", "specifically requests"],
        "Specific oral, electronic, or written contact during the stated program/security period constitutes a qualifying request.",
    ),
    35: (
        [B + "3-ii", B + "3-ii-B"],
        ["completes and submits", "form or payment coupon"],
        "Completing and submitting a request form or coupon for the stated period constitutes a qualifying request.",
    ),
    36: (
        [B + "3-ii", B + "3-ii-C"],
        ["previously allocated", "different manner"],
        "Specific contact requesting a different allocation of a previously default-allocated payment is a qualifying request.",
    ),
    37: (
        [B + "3-iii", B + "3-iii-A"],
        ["has not made a request", "preprinted language"],
        "The described preprinted account-agreement language does not establish a consumer request.",
    ),
    38: (
        [B + "3-iii", B + "3-iii-B"],
        ["preselected check box", "does not deselect"],
        "An untouched issuer-preselected application checkbox does not establish a request.",
    ),
    39: (
        [B + "3-iii", B + "3-iii-C"],
        ["preprinted language", "preselected check box"],
        "The described default payment-coupon language or checkbox does not establish a request.",
    ),
    40: (
        [B + "3-iii", B + "3-iii-D"],
        ["requires a consumer to accept", "as a condition"],
        "Imposed acceptance of an allocation method as a condition of the listed activities does not establish a request.",
    ),
}

# Explicit review edges. These are curated explanations of implementation
# dependencies, not machine-proven legal entailment or an expanded denominator.
deps = {s.number: {} for s in FULL_REGULATION_Z_53_POLICY_SPECS if s.constraint}


def link(policies, propositions, reason):
    for policy in policies:
        for proposition in propositions:
            if proposition != policy:
                deps[policy][proposition] = reason


link(
    [1, 2, 4, 5, 6, 7, 8, 16, 29, 33],
    [9, 10, 11, 12],
    "Scopes the allocation to excess payment and preserves freedom over the minimum; minimum amount is supplied externally.",
)
link(
    [1, 2, 4, 6],
    [21, 22],
    "Preserves equal-rate discretion through strict higher-rate comparisons; grouped input remains limited to underlying detailed lots.",
)
link(
    [3, 4, 5, 6, 7, 23, 25, 26, 27, 28, 29],
    [3, 24, 25, 26, 27],
    "Defines qualifying programs used by the shared classifier; the incorporated external definition is incompletely modeled.",
)
link(
    [1, 2, 4, 5, 6, 16, 29],
    [13, 16, 17, 18, 23, 28, 29],
    "Supplies reference-date, dispute, effective-rate, or window meaning used by the shared allocation comparison. Priority interactions are implementation choices shown below.",
)
link(
    [14, 15],
    [13, 14, 15],
    "Together define the permitted reference date and practice. Recorded authorization is an implementation default, not wording in the source.",
)
link(
    [17, 18],
    [17, 18],
    "P17 rejects avoidable disputed reduction; P18 addresses the unavoidable-residual branch.",
)
link(
    [19, 20],
    [17, 18, 19, 20],
    "The correction obligations arise in the disputed-allocation example after confirmation of the alleged billing error.",
)
link(
    [1, 2, 4, 5, 6, 16, 29, 7, 8, 32, 33, 34, 35, 36, 37, 38, 39, 40],
    [30, 31],
    "Keeps the default branch available when the issuer declines a request; request-specific checks apply only when a request branch is elected.",
)
link(
    [7, 8],
    [32, 33, 34, 35, 36, 37, 38, 39, 40],
    "Companion checks establish whether the elected request exception has qualifying evidence and fulfillable instructions.",
)
link(
    [32, 33, 34, 35, 36, 37, 38, 39, 40],
    [7, 8],
    "Applies to the deferred-program or secured-balance request exception; these companion checks supply its legal setting.",
)

link(
    [4, 6],
    [1, 2],
    "The cross-reference to paragraph (a) imports highest-rate and descending-remainder ordering.",
)
link(
    [32, 33],
    [1, 2, 5, 6],
    "The required fallback is the applicable ordinary or deferred-window default allocation, enforced by companion policies.",
)

runtime_path = PROJECT / "src/vldb_experiments/strategies/regulation_z_53_runtime.py"
runtime_text = runtime_path.read_text()
functions = {
    n.name: {"line": n.lineno, "code": ast.get_source_segment(runtime_text, n)}
    for n in ast.parse(runtime_text).body
    if isinstance(n, ast.FunctionDef)
}
propositions = []
policies = []
for spec in FULL_REGULATION_Z_53_POLICY_SPECS:
    refs, terms, proposition = MAP[spec.number]
    assert all(ref in sources for ref in refs), (spec.number, refs)
    for term in terms:
        assert any(term.casefold() in sources[ref]["text"].casefold() for ref in refs), (
            spec.number,
            term,
        )
    propositions.append(
        {
            "id": spec.proposition_id,
            "name": spec.name,
            "authority": spec.authority,
            "text": proposition,
            "refs": refs,
            "terms": terms,
            "implementation": spec.explanation,
            "limitation": spec.limitation,
            "standalone": bool(spec.constraint),
        }
    )
    if spec.constraint:
        number = spec.number
        funcs = ["derive_allocation_context"]
        if number in [1, 2, 4, 5, 6, 16, 29]:
            funcs += ["_priority_facts"]
        if number in [3, 23, 25, 26, 27]:
            funcs += ["_qualifies"]
        if number in [13, 14, 15]:
            funcs += ["_practice"]
        if number in [28, 29]:
            funcs += ["_window"]
        if number in [7, 8, 32, 33, 34, 35, 36, 37, 38, 39, 40]:
            funcs += ["_request_facts"]
        if number in [19, 20]:
            funcs = ["derive_correction_context"]
        policies.append(
            {
                "id": f"C{number:02}",
                "primary": spec.proposition_id,
                "name": spec.name,
                "constraint": spec.constraint,
                "edges": [
                    {
                        "id": spec.proposition_id,
                        "role": "Direct motivation",
                        "reason": "This is the catalog's primary motivating proposition. The constraint operationalizes it subject to the stated assumptions.",
                    }
                ]
                + [
                    {"id": f"P{n:02}", "role": "Shared support", "reason": reason}
                    for n, reason in sorted(deps[number].items())
                ],
                "functions": funcs,
            }
        )

assert len(policies) == 31
assert len(propositions) == 40
assert {e["id"] for p in policies for e in p["edges"]} == {p["id"] for p in propositions}
data = {
    "generated": datetime.now(timezone.utc).isoformat(),
    "sources": sources,
    "metadata": metadata,
    "propositions": propositions,
    "policies": policies,
    "functions": functions,
    "catalogSha256": hashlib.sha256(
        (
            PROJECT / "src/vldb_experiments/strategies/regulation_z_53_full_policy_catalog.py"
        ).read_bytes()
    ).hexdigest(),
    "runtimeSha256": hashlib.sha256(runtime_path.read_bytes()).hexdigest(),
}
(ROOT / "data.js").write_text(
    "window.REVIEW_DATA = " + json.dumps(data, ensure_ascii=False, indent=2) + ";\n"
)
(ROOT / "data.json").write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n")
print(
    f"Validated {len(policies)} policies, {len(propositions)} propositions, {len(sources)} source paragraphs and every highlight phrase."
)
