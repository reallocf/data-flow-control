import argparse
import csv
import json
import os
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path
from xml.etree import ElementTree as ET

BASE = "https://www.law.cornell.edu"
TITLE_URL = BASE + "/uscode/text/26"
XML_PATH = Path(os.environ.get("USLM_XML_PATH", "inputs/usc26.xml"))
CACHE_PATH = Path("inputs/cornell_title26_refs.jsonl")
COMPARE_PATH = Path("outputs/cornell_reference_compare.csv")
FETCH_PATH = Path("outputs/cornell_title26_fetch_status.csv")
SUMMARY_PATH = Path("outputs/cornell_reference_summary.json")
USLM_RELEASE = os.environ.get("USLM_RELEASE", "119-95")
EXPECTED_CORNELL_RELEASE = "119-84"
DELAY = 10.5
UA = "data-flow-control-research/1.0"


def local(tag):
    return tag.rsplit("}", 1)[-1]


def section_key(value):
    m = re.fullmatch(r"(\d+)(.*)", value)
    if m:
        return int(m.group(1)), m.group(2)
    return 10**9, value


ATOM = r"[0-9][0-9A-Za-z-]*(?:\s*\([A-Za-z0-9-]+\))*"
SECTION_EXPR_RE = re.compile(
    rf"\b(?:sections?|\u00a7{{1,2}})\s+"
    rf"(?P<items>{ATOM}(?:(?:\s*,\s*(?:(?:and|or)\s+)?|\s+(?:and|or)\s+)"
    rf"(?:sections?\s+)?{ATOM})*)",
    re.I,
)
SECTION_HEAD_RE = re.compile(r"^[0-9][0-9A-Za-z-]*")
EXTERNAL_SUFFIX_RE = re.compile(
    r"^\s+of\s+(?:the\s+)?(?:"
    r"such\s+Act\b|"
    r"Public\s+Law\b|"
    r"Pub\.?\s*L\.?\b|"
    r"[A-Z][A-Za-z0-9&,'\u2019.\- ]{1,100}\s+Act\b"
    r")",
    re.I,
)


def statute_text(node):
    parts = []

    def walk(cur):
        if local(cur.tag) in {"notes", "sourceCredit"}:
            if cur.tail:
                parts.append(cur.tail)
            return

        if cur.text:
            parts.append(cur.text)

        for child in cur:
            walk(child)

        if cur.tail:
            parts.append(cur.tail)

    walk(node)
    return re.sub(r"\s+", " ", " ".join(parts))


def load_xml():
    root = ET.parse(XML_PATH).getroot()
    sections = {}

    for sec in root.iter():
        if local(sec.tag) != "section":
            continue

        m = re.fullmatch(
            r"/us/usc/t26/s([^/]+)",
            sec.attrib.get("identifier", ""),
        )
        if m:
            sections[m.group(1)] = sec

    known = set(sections)
    xml_pairs = set()

    for source, sec in sections.items():
        text = statute_text(sec)

        for match in SECTION_EXPR_RE.finditer(text):
            suffix = text[match.end():match.end() + 140]
            if EXTERNAL_SUFFIX_RE.match(suffix):
                continue

            for item in re.finditer(ATOM, match.group("items"), re.I):
                head = SECTION_HEAD_RE.match(item.group(0))
                if not head:
                    continue

                target = head.group(0)
                if target in known and target != source:
                    xml_pairs.add((source, target))

    return list(sections), xml_pairs


def target_from_href(href):
    if not href:
        return None

    url = urllib.parse.urljoin(BASE, href)
    p = urllib.parse.urlparse(url)

    if p.netloc.lower() not in {"www.law.cornell.edu", "law.cornell.edu"}:
        return None

    parts = [urllib.parse.unquote(x) for x in p.path.split("/") if x]
    if len(parts) < 4 or parts[:3] != ["uscode", "text", "26"]:
        return None

    return parts[3]


class PageParser(HTMLParser):
    blocked_classes = {
        "notes",
        "note",
        "sourceCredit",
        "source-credit",
    }

    def __init__(self, source):
        super().__init__(convert_charrefs=True)
        self.source = source
        self.div_stack = []
        self.targets = []

    def in_statute(self):
        classes = set()

        for item in self.div_stack:
            classes.update(item)

        return (
            "text" in classes
            and "section" in classes
            and not (classes & self.blocked_classes)
        )

    def handle_starttag(self, tag, attrs):
        tag = tag.lower()
        attrs = dict(attrs)

        if tag == "div":
            self.div_stack.append(
                set((attrs.get("class") or "").split())
            )

        if tag == "a" and self.in_statute():
            target = target_from_href(attrs.get("href"))
            if target and target != self.source:
                self.targets.append(target)

    def handle_endtag(self, tag):
        if tag.lower() == "div" and self.div_stack:
            self.div_stack.pop()


_last_request = None


def get_page(url):
    global _last_request

    max_attempts = 5
    retry_http = {429, 500, 502, 503, 504}

    for attempt in range(1, max_attempts + 1):
        now = time.monotonic()

        if _last_request is not None:
            wait = DELAY - (now - _last_request)
            if wait > 0:
                time.sleep(wait)

        req = urllib.request.Request(url, headers={"User-Agent": UA})
        _last_request = time.monotonic()

        try:
            with urllib.request.urlopen(req, timeout=60) as response:
                charset = response.headers.get_content_charset() or "utf-8"
                return response.read().decode(charset, "replace")

        except urllib.error.HTTPError as e:
            if e.code not in retry_http or attempt == max_attempts:
                raise

            retry_after = e.headers.get("Retry-After")
            try:
                pause = max(DELAY, float(retry_after))
            except (TypeError, ValueError):
                pause = min(20 * (2 ** (attempt - 1)), 180)

            print(
                f"request retry {attempt}/{max_attempts} "
                f"after HTTP {e.code}; waiting {pause:.0f}s"
            )
            time.sleep(pause)

        except (urllib.error.URLError, TimeoutError, OSError) as e:
            if attempt == max_attempts:
                raise

            pause = min(20 * (2 ** (attempt - 1)), 180)

            print(
                f"request retry {attempt}/{max_attempts} "
                f"after {type(e).__name__}; waiting {pause:.0f}s"
            )
            time.sleep(pause)

    raise RuntimeError("request attempts exhausted")


def observed_release():
    page = get_page(TITLE_URL)
    m = re.search(
        r"Current.{0,100}?through.{0,100}?(119-\d+)",
        page,
        re.I | re.S
    )
    return m.group(1) if m else None


def read_cache():
    out = {}

    if not CACHE_PATH.exists():
        return out

    with CACHE_PATH.open(encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            record = json.loads(line)
            if record.get("cornell_release") == EXPECTED_CORNELL_RELEASE:
                out[record["source_section"]] = record

    return out


def append_cache(record):
    CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)

    with CACHE_PATH.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")


def fetch_section(source):
    url = f"{TITLE_URL}/{urllib.parse.quote(source, safe='')}"
    now = datetime.now(timezone.utc).isoformat()

    try:
        page = get_page(url)
        parser = PageParser(source)
        parser.feed(page)

        targets = sorted(set(parser.targets), key=section_key)

        return {
            "source_section": source,
            "status": "ok",
            "targets": targets,
            "link_occurrences": len(parser.targets),
            "url": url,
            "fetched_utc": now,
            "cornell_release": EXPECTED_CORNELL_RELEASE,
        }

    except urllib.error.HTTPError as e:
        return {
            "source_section": source,
            "status": f"http_{e.code}",
            "targets": [],
            "link_occurrences": 0,
            "url": url,
            "fetched_utc": now,
            "cornell_release": EXPECTED_CORNELL_RELEASE,
        }

    except Exception as e:
        return {
            "source_section": source,
            "status": "error",
            "error": f"{type(e).__name__}: {e}",
            "targets": [],
            "link_occurrences": 0,
            "url": url,
            "fetched_utc": now,
            "cornell_release": EXPECTED_CORNELL_RELEASE,
        }


def write_outputs(sections, xml_pairs, cache, release):
    COMPARE_PATH.parent.mkdir(parents=True, exist_ok=True)

    good_sources = {
        source
        for source, record in cache.items()
        if record.get("status") == "ok"
    }

    cornell_pairs = {
        (source, target)
        for source in good_sources
        for target in cache[source].get("targets", [])
        if target != source
    }

    xml_scoped = {
        pair for pair in xml_pairs
        if pair[0] in good_sources
    }

    known_sections = set(sections)
    cornell_section_pairs = {
        pair for pair in cornell_pairs
        if pair[1] in known_sections
    }
    cornell_target_not_in_uslm = cornell_pairs - cornell_section_pairs
    all_pairs = cornell_pairs | xml_scoped

    with COMPARE_PATH.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["source_section", "target_section", "status"])

        for source, target in sorted(
            all_pairs,
            key=lambda x: (section_key(x[0]), section_key(x[1]))
        ):
            if (source, target) in cornell_pairs and (source, target) in xml_scoped:
                status = "both"
            elif (source, target) in cornell_pairs:
                status = (
                    "cornell_only"
                    if target in known_sections
                    else "cornell_target_not_in_uslm"
                )
            else:
                status = "uslm_only"

            w.writerow([source, target, status])

    ordered = sorted(
        cache.values(),
        key=lambda r: section_key(r["source_section"])
    )

    with FETCH_PATH.open("w", newline="", encoding="utf-8") as f:
        fields = [
            "source_section",
            "status",
            "link_occurrences",
            "target_count",
            "fetched_utc",
            "url",
            "error",
        ]

        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()

        for record in ordered:
            w.writerow({
                "source_section": record["source_section"],
                "status": record.get("status", ""),
                "link_occurrences": record.get("link_occurrences", 0),
                "target_count": len(record.get("targets", [])),
                "fetched_utc": record.get("fetched_utc", ""),
                "url": record.get("url", ""),
                "error": record.get("error", ""),
            })

    both = cornell_pairs & xml_scoped
    both_section = cornell_section_pairs & xml_scoped
    cornell_only = cornell_section_pairs - xml_scoped
    uslm_only = xml_scoped - cornell_pairs

    statuses = {}
    for record in cache.values():
        status = record.get("status", "unknown")
        statuses[status] = statuses.get(status, 0) + 1

    summary = {
        "uslm_release": USLM_RELEASE,
        "cornell_release_expected": EXPECTED_CORNELL_RELEASE,
        "cornell_release_observed": release,
        "uslm_sections": len(sections),
        "cornell_pages_cached": len(cache),
        "fetch_status": statuses,
        "compared_source_sections": len(good_sources),
        "cornell_pairs": len(cornell_pairs),
        "cornell_pairs_with_uslm_section_target": len(cornell_section_pairs),
        "cornell_target_not_in_uslm": len(cornell_target_not_in_uslm),
        "uslm_pairs_same_sources": len(xml_scoped),
        "both": len(both),
        "cornell_only": len(cornell_only),
        "uslm_only": len(uslm_only),
        "cornell_section_overlap_pct": (
            round(100 * len(both_section) / len(cornell_section_pairs), 2)
            if cornell_section_pairs else None
        ),
        "cornell_overlap_pct": (
            round(100 * len(both) / len(cornell_pairs), 2)
            if cornell_pairs else None
        ),
        "uslm_overlap_pct": (
            round(100 * len(both) / len(xml_scoped), 2)
            if xml_scoped else None
        ),
        "section_274_to_212": {
            "cornell": ("274", "212") in cornell_pairs,
            "uslm": ("274", "212") in xml_scoped,
        },
    }

    with SUMMARY_PATH.open("w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, sort_keys=True)
        f.write("\n")

    print(json.dumps(summary, indent=2, sort_keys=True))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sections", nargs="*")
    ap.add_argument("--limit", type=int)
    ap.add_argument("--refresh", action="store_true")
    ap.add_argument("--offline", action="store_true")
    args = ap.parse_args()

    sections, xml_pairs = load_xml()

    if args.sections:
        requested = []
        for value in args.sections:
            requested.extend(x for x in value.split(",") if x)

        missing = [x for x in requested if x not in sections]
        if missing:
            raise SystemExit("Unknown section(s): " + ", ".join(missing))

        selected = requested
    else:
        selected = sections

    if args.limit is not None:
        selected = selected[:args.limit]

    if args.offline:
        release = EXPECTED_CORNELL_RELEASE
        print("Cornell release: cached", release)
    else:
        release = observed_release()
        print("Cornell release:", release or "not found")

        if release != EXPECTED_CORNELL_RELEASE:
            raise SystemExit(
                f"Expected Cornell {EXPECTED_CORNELL_RELEASE}; observed {release!r}"
            )

    cache = read_cache()

    if args.offline:
        write_outputs(sections, xml_pairs, cache, release)
        return

    total = len(selected)

    for index, source in enumerate(selected, 1):
        old = cache.get(source)

        if (
            old
            and not args.refresh
            and old.get("status") in {"ok", "http_404"}
        ):
            print(f"[{index}/{total}] {source}: cached {old['status']}")
            continue

        record = fetch_section(source)
        append_cache(record)
        cache[source] = record

        print(
            f"[{index}/{total}] {source}: {record['status']} "
            f"{len(record.get('targets', []))} targets"
        )

        if record["status"] not in {"ok", "http_404"}:
            raise SystemExit(
                f"Stopped after {source}: {record['status']}"
            )

        if record["status"] not in {"ok", "http_404"}:
            raise SystemExit(
                f"Stopped after {source}: {record['status']}"
            )

    write_outputs(sections, xml_pairs, cache, release)


if __name__ == "__main__":
    main()
