import re


def clean(value):
    return re.sub(r"\s+", " ", str(value or "").strip())


def normalize_path(value):
    value = clean(value)
    if not value:
        return ""
    pieces = [piece.strip() for piece in value.split(",") if piece.strip()]
    if len(pieces) > 1:
        return ", ".join(normalize_path(piece) for piece in pieces)
    value = re.sub(r"(?i)^(section|subsection|paragraph|subparagraph|clause)\s+", "", value).strip()
    value = value.strip("() ")
    parts = re.findall(r"[A-Za-z]+|[0-9]+", value)
    if parts:
        return ".".join(part.lower() for part in parts)
    return value.lower()


def split_target_section(value):
    value = clean(value)
    match = re.fullmatch(r"([0-9][0-9A-Za-z-]*)(\s*(?:\([^)]+\))+)", value)
    if match:
        return match.group(1).lower(), normalize_path(match.group(2))
    return value.lower(), ""


def normalize_target(target_section, target_path):
    target_section, embedded_path = split_target_section(target_section)
    target_path = normalize_path(target_path)
    if embedded_path and target_path:
        if target_path == embedded_path or target_path.startswith(embedded_path + "."):
            return target_section, target_path
        return target_section, embedded_path + "|" + target_path
    if embedded_path:
        return target_section, embedded_path
    return target_section, target_path
