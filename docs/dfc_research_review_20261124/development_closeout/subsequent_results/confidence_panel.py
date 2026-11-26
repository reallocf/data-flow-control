import json
import re


def parse_judgment(raw):

    def unique_fields(pairs):

        result = {}

        for key, value in pairs:

            if key in result:
                raise ValueError("Repeated field")

            result[key] = value

        return result

    try:
        item = json.loads(
            raw,
            object_pairs_hook=unique_fields
        )

    except (ValueError, TypeError):
        return None

    if not isinstance(item, dict):
        return None

    required = {
        "candidate_sha256",
        "legal_context_sha256",
        "confidence",
        "reason"
    }

    if set(item) != required:
        return None

    for field in (
        "candidate_sha256",
        "legal_context_sha256"
    ):

        if not (
            isinstance(item[field], str)
            and re.fullmatch(
                r"[0-9a-f]{64}", item[field]
            )
        ):
            return None

    if item["confidence"] not in ("HIGH", "LOW"):
        return None

    if not (
        isinstance(item["reason"], str)
        and item["reason"].strip()
    ):
        return None

    return item


def confidence_decision(
    candidate_sha256,
    legal_context_sha256,
    producer_model,
    authorized_models,
    structural_check,
    assessments
):

    def hold(reason):
        return {
            "confidence": "LOW",
            "disposition": "REVIEW_REQUIRED",
            "reason": reason,
            "automatic_approval": False
        }

    if structural_check != "PASS":
        return hold("STRUCTURAL_CHECK_REQUIRED")

    expected = set(authorized_models)

    if (
        len(authorized_models) != 3
        or len(expected) != 3
        or producer_model in expected
        or not producer_model
        or any(not x for x in expected)
    ):
        return hold("THREE_DISTINCT_MODELS_REQUIRED")

    if len(assessments) != 3:
        return hold("INCOMPLETE_MODEL_PANEL")

    seen_models = set()
    seen_requests = set()
    labels = []

    for item in assessments:

        model = item.get("model")
        request_id = item.get("request_id")

        if (
            model not in expected
            or model in seen_models
            or not request_id
            or request_id in seen_requests
        ):
            return hold("IDENTITY_CHECK_FAILED")

        parsed = parse_judgment(
            item.get("response")
        )

        if parsed is None:
            return hold("FORMAT_CHECK_FAILED")

        if (
            parsed["candidate_sha256"]
                != candidate_sha256
            or parsed["legal_context_sha256"]
                != legal_context_sha256
        ):
            return hold("EVIDENCE_BINDING_FAILED")

        seen_models.add(model)
        seen_requests.add(request_id)
        labels.append(parsed["confidence"])

    if seen_models != expected:
        return hold("MODEL_PANEL_DIFFERS")

    if labels != ["HIGH", "HIGH", "HIGH"]:
        return hold("MODEL_DISAGREEMENT_OR_LOW")

    return {
        "confidence": "HIGH",
        "disposition": "REVIEW_ELIGIBLE",
        "reason": "THREE_MODEL_AGREEMENT",
        "automatic_approval": False
    }