EXTERNAL_MAP = (
    (
        "section 16(a) of the Securities Exchange Act of 1934",
        "15 USC 78p(a)"
    ),
    (
        "section 212(a)(1)(A) of the Caribbean Basin Economic Recovery Act",
        "19 USC 2702(a)(1)(A)"
    ),
    (
        "section 2101 of title 46 , United States Code",
        "46 USC 2101"
    )
)


def classify_reference(text, row):

    try:
        start = int(row["span_start"])
        end = int(row["span_end"])
        surface = row["surface"]

    except (KeyError, ValueError, TypeError):
        return "INVALID_SPAN", ""

    if not (
        isinstance(surface, str)
        and 0 <= start < end <= len(text)
        and text[start:end] == surface
    ):
        return "OFFSET_MISMATCH", ""

    for phrase, destination in EXTERNAL_MAP:

        if (
            text.startswith(phrase, start)
            and phrase.startswith(surface)
        ):
            return "EXTERNAL_MAPPED", destination

    return "OTHER_NOT_ADJUDICATED", ""


def internal_route_decision(text, row):

    status, destination = classify_reference(
        text, row
    )

    if status == "EXTERNAL_MAPPED":
        return {
            "decision": "EXCLUDE_INTERNAL_PATH",
            "external_destination": destination
        }

    if status in {
        "INVALID_SPAN",
        "OFFSET_MISMATCH"
    }:
        return {
            "decision": "HOLD_FOR_REVIEW",
            "external_destination": ""
        }

    return {
        "decision": "DEFER_TO_RESOLVER",
        "external_destination": ""
    }