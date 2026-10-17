#!/usr/bin/env python3
"""Apply the SIGMOD export manifest only in its dedicated Git branch.

Supply private literal redactions as a JSON object using --redactions. Keep that
file outside this worktree: publishing the replacement keys defeats redaction.
Git history is deliberately preserved for the anonymization service workflow.
"""

import argparse
import hashlib
import json
from pathlib import Path
import shutil
import subprocess


def git(root, *args):
    return subprocess.check_output(["git", "-C", str(root), *args], text=True).strip()


def prepare(root, redactions_path, apply):
    if git(root, "branch", "--show-current") != "sigmod-2027-anonymous":
        raise SystemExit("Run only on sigmod-2027-anonymous; no files changed.")
    if redactions_path.is_relative_to(root):
        raise SystemExit("Private redaction terms must be outside the worktree.")
    plan_path = root / "scripts/sigmod_2027_export_manifest.json"
    plan = json.loads(plan_path.read_text())
    replacements = json.loads(redactions_path.read_text())
    replacements.update(plan["text_path_replacements"])
    prefixes = tuple(plan["include_tracked_directories"])
    renamed_prefixes = tuple(plan["directory_renames"].values())
    root_files = {".gitignore", "LICENSE", "README.md", ".pre-commit-config.yaml"}
    tracked = git(root, "ls-files", "-z").split("\0")
    removed = [
        name
        for name in tracked
        if name
        and name not in root_files
        and not name.startswith((*prefixes, *renamed_prefixes, "scripts/"))
    ]
    print(f"Remove {len(removed)} tracked paths outside the export scope.")
    if not apply:
        print("Dry run; pass --apply to prepare the branch.")
        return
    for name in removed:
        path = root / name
        if path.is_dir():
            path.rmdir()  # Fresh worktree gitlinks must be empty, never delete nested repos.
        else:
            path.unlink(missing_ok=True)
    for old, new in plan["directory_renames"].items():
        source, destination = root / old, root / new
        if source.exists():
            if destination.exists():
                raise SystemExit(f"Rename destination already exists: {new}")
            shutil.move(str(source), str(destination))
    candidates = [root / name for name in root_files]
    visible = git(root, "ls-files", "--cached", "--others", "--exclude-standard", "-z")
    for name in visible.split("\0"):
        if name.startswith((*prefixes, *renamed_prefixes, "scripts/")):
            candidates.append(root / name)
    changed = 0
    for path in sorted(set(candidates)):
        if not path.is_file() or path == plan_path or path == Path(__file__).resolve():
            continue
        try:
            original = path.read_bytes().decode("utf-8")
        except UnicodeDecodeError:
            continue
        text = original
        for old, new in sorted(replacements.items(), key=lambda item: -len(item[0])):
            text = text.replace(old, new)
        if text != original:
            path.write_bytes(text.encode("utf-8"))
            changed += 1
    results_path = root / "final_results/manifest.json"
    results = json.loads(results_path.read_text())
    results.pop("source_commit", None)
    results["description"] = (
        "Paper experiment data. Identifying metadata has been anonymized; "
        "measurements are preserved. Checksums describe the anonymized files."
    )
    for group in results["groups"]:
        for record in group["files"]:
            data = (root / "final_results" / record["file"]).read_bytes()
            record["sha256"] = hashlib.sha256(data).hexdigest()
            record["bytes"] = len(data)
    results_path.write_text(json.dumps(results, indent=2) + "\n")
    print(f"Redacted/translated {changed} text files; refreshed result checksums.")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--redactions", type=Path, required=True)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    root = Path(git(Path.cwd(), "rev-parse", "--show-toplevel")).resolve()
    prepare(root, args.redactions.resolve(), args.apply)


if __name__ == "__main__":
    main()
