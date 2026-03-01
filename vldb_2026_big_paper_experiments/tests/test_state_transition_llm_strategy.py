"""Tests for the state-transition GPT-gated strategy helpers."""

from vldb_experiments.strategies.state_transition_llm_strategy import (
    _build_state_transition_llm_prompt,
    _parse_allow_update,
)


def test_build_state_transition_llm_prompt_includes_rules_and_state() -> None:
    prompt = _build_state_transition_llm_prompt(
        sql="UPDATE t AS t2 SET state = 'B' FROM t WHERE t.id = t2.id AND t.id = 1",
        current_state="A",
        row_id=1,
    )

    expected = (
        "You are validating whether a single SQL UPDATE should be allowed.\n"
        "The UPDATE always targets exactly one row.\n"
        "Allowed state transitions for that row are:\n"
        "- A -> B is allowed\n"
        "- B -> A is allowed\n"
        "- B -> C is allowed\n"
        "- A -> C is NOT allowed\n"
        "- C -> A is NOT allowed\n"
        "- C -> B is NOT allowed\n"
        "- Any transition not listed as allowed should be rejected\n\n"
        "Current row id: 1\n"
        "Current row state before the UPDATE: A\n"
        "SQL UPDATE:\n"
        "UPDATE t AS t2 SET state = 'B' FROM t WHERE t.id = t2.id AND t.id = 1\n\n"
        'Return JSON only: {"allow_update": true|false}\n'
    )

    assert prompt == expected


def test_parse_allow_update_accepts_json_true() -> None:
    assert _parse_allow_update('{"allow_update": true}') is True


def test_parse_allow_update_accepts_json_false() -> None:
    assert _parse_allow_update('{"allow_update": false}') is False


def test_parse_allow_update_returns_none_for_unparseable_text() -> None:
    assert _parse_allow_update("maybe") is None
