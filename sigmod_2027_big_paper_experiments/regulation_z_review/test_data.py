"""Integrity checks for the saved review graph and implementation snapshot."""

import hashlib
import json
from pathlib import Path

from vldb_experiments.strategies.regulation_z_53_full_policy_catalog import (
    FULL_REGULATION_Z_53_POLICY_SPECS,
)

ROOT = Path(__file__).resolve().parent
DATA = json.loads((ROOT / "data.json").read_text())


def test_policy_predicates_match_current_catalog():
    expected = {
        f"C{s.number:02}": s.constraint for s in FULL_REGULATION_Z_53_POLICY_SPECS if s.constraint
    }
    assert {p["id"]: p["constraint"] for p in DATA["policies"]} == expected
    assert len(expected) == 31


def test_all_propositions_are_reachable_and_highlights_are_exact():
    propositions = {p["id"]: p for p in DATA["propositions"]}
    assert set(propositions) == {f"P{n:02}" for n in range(1, 41)}
    assert {e["id"] for p in DATA["policies"] for e in p["edges"]} == set(propositions)
    for p in propositions.values():
        texts = [DATA["sources"][ref]["text"].casefold() for ref in p["refs"]]
        for phrase in p["terms"]:
            assert any(phrase.casefold() in text for text in texts), (p["id"], phrase)


def test_saved_source_and_code_hashes_match():
    for source in DATA["metadata"]:
        assert hashlib.sha256((ROOT / source["file"]).read_bytes()).hexdigest() == source["sha256"]
    for filename, key in [
        ("regulation_z_53_full_policy_catalog.py", "catalogSha256"),
        ("regulation_z_53_runtime.py", "runtimeSha256"),
    ]:
        path = ROOT.parent / "src/vldb_experiments/strategies" / filename
        assert hashlib.sha256(path.read_bytes()).hexdigest() == DATA[key]


def test_every_policy_has_primary_edge_and_real_computation():
    for policy in DATA["policies"]:
        assert policy["edges"][0]["id"] == policy["primary"]
        assert policy["edges"][0]["role"] == "Direct motivation"
        assert len({e["id"] for e in policy["edges"]}) == len(policy["edges"])
        assert policy["functions"]
        for name in policy["functions"]:
            assert DATA["functions"][name]["code"].startswith(f"def {name}(")
