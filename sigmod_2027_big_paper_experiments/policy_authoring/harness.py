"""Generate once through subscription Codex; automatically check workflow execution."""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
import csv
import hashlib
import itertools
import json
from pathlib import Path
import random
import sqlite3
import subprocess
import sys
from threading import Timer
import time
from uuid import uuid4

ROOT = Path(__file__).resolve().parent
PROJECT = ROOT.parent
APPROACHES = ("dfc", "gprom", "provsql")
IDS = tuple(f"P{i:02}" for i in (*range(1, 9), *range(13, 21), 23, *range(25, 30), *range(32, 41)))
BOOLS = {
    "issuer_elected",
    "request_present",
    "affirmative",
    "contact_channel",
    "form_completed",
    "prior_default_allocation",
    "required_condition",
}
DATES = {"snapshot_date", "cycle_end", "credited_date", "expected_snapshot_date"}
IMAGE = "regz-policy-authoring:workflow-v1-codex-0.163.0"


def digest(data):
    return hashlib.sha256(data).hexdigest()


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2, default=str) + "\n")


def run(command, *, timeout=120, **kwargs):
    return subprocess.run(
        command, capture_output=True, text=True, timeout=timeout, check=True, **kwargs
    )


def run_streaming(command, *, destination, input, timeout):
    """Write child output directly to disk as emitted, including on timeout/failure."""
    stdout_path = destination / "events.jsonl"
    stderr_path = destination / "stderr.txt"
    with (
        stdout_path.open("wb", buffering=0) as stdout,
        stderr_path.open("wb", buffering=0) as stderr,
    ):
        completed = subprocess.run(
            command,
            input=input,
            text=True,
            stdout=stdout,
            stderr=stderr,
            timeout=timeout,
            check=True,
        )
    return subprocess.CompletedProcess(
        command, completed.returncode, stdout_path.read_text(), stderr_path.read_text()
    )


def build_prompts(benchmark="predicate"):
    """Freeze complete prompts for review; no reference predicates enter these files."""
    prompt_root = ROOT / "workflow/prompts" if benchmark == "workflow" else ROOT / "prompts"
    for approach in APPROACHES:
        parts = [
            "task.md",
            "specification.md",
            "evidence.md",
            "schema.md" if benchmark == "workflow" else "clarifications.md",
            f"{approach}.md",
        ]
        prompt = "\n\n".join((prompt_root / name).read_text().strip() for name in parts) + "\n"
        (prompt_root / f"exact-{approach}.txt").write_text(prompt)


def validate_submission(value):
    if not isinstance(value, dict) or set(value) != {"policies"}:
        raise ValueError("Expected exactly the policies JSON field")
    policies = value["policies"]
    if not isinstance(policies, list) or len(policies) != len(IDS):
        raise ValueError("Expected exactly 31 policies")
    if any(not isinstance(p, dict) or set(p) != {"id", "code"} for p in policies):
        raise ValueError("Each policy requires exactly id and code")
    if [p["id"] for p in policies] != list(IDS):
        raise ValueError("Missing, duplicated, or unordered policy IDs")
    if any(not isinstance(p["code"], str) or not p["code"].strip() for p in policies):
        raise ValueError("Policy code must be nonempty text")
    return policies


def validate_workflow_submission(value, approach):
    """Check the artifact envelope only; this does not establish executable correctness."""
    if not isinstance(value, dict) or set(value) != {"files"}:
        raise ValueError(
            "Workflow output requires exactly files; per-policy snippets are not accepted"
        )
    files = value["files"]
    if not isinstance(files, list) or not files:
        raise ValueError("Expected a nonempty artifact list")
    paths = []
    for file in files:
        if not isinstance(file, dict) or set(file) != {"path", "content"}:
            raise ValueError("Each artifact requires exactly path and content")
        path, content = file["path"], file["content"]
        if not isinstance(path, str) or not isinstance(content, str) or not content.strip():
            raise ValueError("Artifact paths and nonempty content must be strings")
        if path.startswith("/") or ".." in path.split("/") or "\\" in path:
            raise ValueError("Unsafe artifact path")
        paths.append(path)
    if len(paths) != len(set(paths)):
        raise ValueError("Duplicate artifact paths")
    if approach == "dfc":
        if any(
            len(path.split("/")) != 2
            or not path.startswith("policies/")
            or not path.endswith(".pgn")
            or path == "policies/.pgn"
            for path in paths
        ):
            raise ValueError("DFC artifacts must be policies/<name>.pgn")
    elif approach == "gprom":
        if set(paths) != {"candidate.sql", "reenact.sql", "check.sql"}:
            raise ValueError("GProM requires candidate.sql, reenact.sql, and check.sql")
    elif approach == "provsql":
        if paths != ["transaction.sql"]:
            raise ValueError("ProvSQL requires transaction.sql")
    else:
        raise ValueError("Unknown workflow approach")
    return files


def parse_events(stdout):
    events = [json.loads(line) for line in stdout.splitlines() if line.strip()]
    completions = [e for e in events if e.get("type") == "turn.completed"]
    messages = [
        e["item"]["text"]
        for e in events
        if e.get("type") == "item.completed" and e.get("item", {}).get("type") == "agent_message"
    ]
    tool_items = [
        e
        for e in events
        if e.get("type", "").startswith("item.")
        and e.get("item", {}).get("type") not in {"agent_message", "reasoning", "error"}
    ]
    if len(completions) != 1 or not messages:
        raise ValueError("Expected one completed turn and a final response")
    if tool_items:
        raise ValueError("Tool activity invalidates this no-tools trial")
    return completions[0].get("usage"), messages[-1]


@contextmanager
def isolated_network(image):
    suffix = uuid4().hex[:12]
    network, proxy = f"regz-internal-{suffix}", f"regz-proxy-{suffix}"
    run(["docker", "network", "create", "--internal", network])
    try:
        run(
            [
                "docker",
                "run",
                "-d",
                "--name",
                proxy,
                "--network",
                "bridge",
                "--read-only",
                "--cap-drop=ALL",
                "--security-opt=no-new-privileges",
                "--pids-limit=64",
                "--memory=128m",
                "--entrypoint",
                "python3",
                image,
                "/opt/experiment/proxy.py",
            ]
        )
        run(["docker", "network", "connect", "--alias", "inference-proxy", network, proxy])
        yield network
    finally:
        subprocess.run(["docker", "rm", "-f", proxy], capture_output=True, check=False)
        subprocess.run(["docker", "network", "rm", network], capture_output=True, check=False)


def container_command(image, network, name):
    return [
        "docker",
        "run",
        "--rm",
        "-i",
        "--name",
        name,
        "--network",
        network,
        "--read-only",
        "--cap-drop=ALL",
        "--security-opt=no-new-privileges",
        "--pids-limit=128",
        "--memory=2g",
        "--cpus=2",
        "--tmpfs",
        "/tmp:rw,nosuid,nodev,size=128m,mode=1777",
        "--tmpfs",
        "/home/node:rw,nosuid,nodev,size=256m,uid=1000,gid=1000,mode=0700",
        "-e",
        "HTTPS_PROXY=http://inference-proxy:8080",
        "-e",
        "HTTP_PROXY=http://inference-proxy:8080",
        image,
    ]


def generate(args):
    # Never copy a home directory, configuration, skills, or historical sessions.
    auth = json.loads(args.auth.read_text())
    if auth.get("OPENAI_API_KEY") or not auth.get("tokens"):
        raise ValueError("A saved ChatGPT login is required; API-key auth is refused")
    image_id = run(["docker", "image", "inspect", args.image, "--format", "{{.Id}}"]).stdout.strip()
    args.output.mkdir(parents=True, exist_ok=False)
    benchmark = getattr(args, "benchmark", "predicate")
    prompt_root = ROOT / "workflow/prompts" if benchmark == "workflow" else ROOT / "prompts"
    schedule = list(
        itertools.product(range(1, args.trials + 1), getattr(args, "approaches", APPROACHES))
    )
    random.Random(args.seed).shuffle(schedule)
    write_json(args.output / "schedule.json", schedule)

    def generate_trial(trial, approach, network):
        destination = args.output / f"{approach}-{trial:03}"
        destination.mkdir()
        base_prompt = (prompt_root / f"exact-{approach}.txt").read_text()
        prompt_nonce = uuid4().hex
        prompt = (
            f"Trial nonce: {prompt_nonce} (ignore this identifier when answering).\n\n{base_prompt}"
        )
        (destination / "prompt.txt").write_text(prompt)
        payload = {
            "auth": auth,
            "prompt": prompt,
            "model": args.model,
            "effort": args.effort,
            "benchmark": benchmark,
        }
        name = "regz-trial-" + uuid4().hex[:12]
        metadata = {
            "benchmark": benchmark,
            "approach": approach,
            "trial": trial,
            "model": args.model,
            "effort": args.effort,
            "image_id": image_id,
            "prompt_sha256": digest(prompt.encode()),
            "timeout_seconds": args.timeout,
            "repair_rounds": 0,
            "parallelism": args.parallelism,
        }
        metadata.update(
            status="running",
            container_name=name,
            prompt_nonce=prompt_nonce,
            base_prompt_sha256=digest(base_prompt.encode()),
            started_at_utc=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        )
        write_json(destination / "metadata.json", metadata)
        print(f"{approach} trial {trial}: starting {name}; logs: {destination}", flush=True)
        started = time.perf_counter()
        try:
            completed = run_streaming(
                container_command(image_id, network, name),
                destination=destination,
                input=json.dumps(payload),
                timeout=args.timeout,
            )
            metadata["wall_seconds_including_cli_startup"] = time.perf_counter() - started
            records = [json.loads(line) for line in completed.stdout.splitlines() if line.strip()]
            metadata["usage"] = next(
                (e.get("usage") for e in reversed(records) if e.get("type") == "turn.completed"),
                None,
            )
            usage, answer = parse_events(completed.stdout)
            metadata["usage"] = usage
            (destination / "answer.txt").write_text(answer)
            submission = json.loads(answer)
            if benchmark == "workflow":
                files = validate_workflow_submission(submission, approach)
                for file in files:
                    path = destination / "artifacts" / file["path"]
                    path.parent.mkdir(parents=True, exist_ok=True)
                    path.write_text(file["content"])
            else:
                validate_submission(submission)
            write_json(destination / "submission.json", submission)
            metadata["status"] = "submitted"
        except (ValueError, OSError, subprocess.SubprocessError) as exc:
            metadata["wall_seconds_including_cli_startup"] = time.perf_counter() - started
            metadata["status"] = (
                "timeout"
                if isinstance(exc, subprocess.TimeoutExpired)
                else "infrastructure_error"
                if isinstance(exc, (OSError, subprocess.CalledProcessError))
                else "invalid_submission"
            )
            metadata["error"] = str(exc)[:2000]
            # Command arguments contain no auth. Never persist stdin/payload.
            for attr, filename in (("stdout", "events.jsonl"), ("stderr", "stderr.txt")):
                content = getattr(exc, attr, None)
                if content:
                    (destination / filename).write_bytes(
                        content.encode() if isinstance(content, str) else content
                    )
        finally:
            subprocess.run(["docker", "rm", "-fv", name], capture_output=True, check=False)
            write_json(destination / "metadata.json", metadata)
        print(f"{approach} trial {trial}: {metadata['status']}", flush=True)
        if benchmark == "workflow":
            evaluate_workflow_trial(destination)

    with (
        isolated_network(image_id) as network,
        ThreadPoolExecutor(max_workers=args.parallelism) as executor,
    ):
        futures = [
            executor.submit(generate_trial, trial, approach, network)
            for trial, approach in schedule
        ]
        for future in futures:
            future.result()


def evaluate_workflow_trial(destination):
    """Keep generation and offline execution results separate; never repair outputs."""
    metadata_path = destination / "metadata.json"
    metadata = json.loads(metadata_path.read_text())
    started = time.perf_counter()
    result = {
        "suite": "workflow_integration_v1",
        "cases_total": 8,
        "coverage": "limited_integration_not_full_regulatory_correctness",
    }
    if metadata["status"] != "submitted":
        result.update(
            status="not_executable",
            cases_passed=0,
            reason=metadata.get("error", metadata["status"]),
        )
    else:
        result["status"] = "running"
        write_json(destination / "execution-summary.json", result)
        try:
            with (destination / "execution-checker.log").open("w") as log:
                subprocess.run(
                    [
                        sys.executable,
                        "-m",
                        "policy_authoring.workflow_execution",
                        str(destination.resolve()),
                    ],
                    cwd=PROJECT,
                    stdout=log,
                    stderr=subprocess.STDOUT,
                    check=True,
                    timeout=1800,
                )
            rows = json.loads((destination / "execution" / "results.json").read_text())
            if len(rows) != 8:
                raise ValueError("Execution evaluator must return eight cases")
            passed = sum(row.get("matches") is True for row in rows)
            result.update(status="passed" if passed == 8 else "failed", cases_passed=passed)
        except (OSError, ValueError, subprocess.SubprocessError) as exc:
            result.update(status="evaluator_error", error=str(exc))
    result["wall_seconds"] = time.perf_counter() - started
    write_json(destination / "execution-summary.json", result)
    metadata["execution"] = result
    write_json(metadata_path, metadata)
    print(f"{destination.name}: execution {result['status']}", flush=True)
    return result


def prepare_oracle(destination):
    """Snapshot current generated PGN, not a claimed legal ground truth."""
    from data_flow_control import Policy
    import duckdb

    destination.mkdir(parents=True, exist_ok=False)
    reference = PROJECT / "results/regulation_z_dfc"
    constraints = {}
    hashes = {}
    for policy_id in IDS:
        source = reference / f"{policy_id}.pgn"
        content = source.read_bytes()
        (destination / source.name).write_bytes(content)
        hashes[str(source.relative_to(PROJECT))] = digest(content)
        constraints[policy_id] = Policy.from_pgn(content.decode()).constraint
    for relative in (
        "src/vldb_experiments/strategies/regulation_z_53_runtime.py",
        "src/vldb_experiments/strategies/regulation_z_53_full_policy_catalog.py",
        "results/regulation_z_gprom/policy.template.sql",
        "src/vldb_experiments/strategies/regulation_z_provsql.sql",
    ):
        source = PROJECT / relative
        hashes[relative] = digest(source.read_bytes())
        snapshot = destination / "sources" / relative
        snapshot.parent.mkdir(parents=True, exist_ok=True)
        snapshot.write_bytes(source.read_bytes())
    source = PROJECT / "results/regulation_z_gprom/data.sqlite"
    hashes[str(source.relative_to(PROJECT))] = digest(source.read_bytes())
    (destination / "source-fixture.sqlite").write_bytes(source.read_bytes())
    with sqlite3.connect(source.as_uri() + "?mode=ro", uri=True) as database:
        database.row_factory = sqlite3.Row
        concrete = [
            dict(row) for row in database.execute("SELECT * FROM regz_context ORDER BY batch_id")
        ]
    columns = list(concrete[0])
    types = {
        key: "BOOLEAN"
        if key in BOOLS
        else "DATE"
        if key in DATES
        else "BIGINT"
        if isinstance(concrete[0][key], int)
        else "VARCHAR"
        for key in columns
    }
    base = {
        key: False
        if key in BOOLS
        else "2027-01-30"
        if key in DATES
        else 0
        if types[key] == "BIGINT"
        else ""
        for key in columns
    }
    base.update(kind="allocation", method="default", credited_date="2027-02-10")
    contexts = [("concrete", row) for row in concrete]
    for kind, method, basis in [
        ("allocation", "default", ""),
        ("correction", "", ""),
        *[
            ("allocation", method, basis)
            for method in ("deferred_request", "secured_request")
            for basis in (
                "contact",
                "form",
                "reallocation",
                "agreement_default",
                "application_default",
                "coupon_default",
            )
        ],
    ]:
        seed = dict(base, kind=kind, method=method, request_basis=basis)
        contexts.append(("abstract_context_probe", seed))
        for key, dtype in types.items():
            values = (
                [False, True]
                if dtype == "BOOLEAN"
                else [0, 1, 2, 100]
                if dtype == "BIGINT"
                else ["2027-01-29", "2027-02-10", "2027-02-11"]
                if dtype == "DATE"
                else []
            )
            for value in values:
                contexts.append(("abstract_context_probe", dict(seed, **{key: value})))
        for active, window, violations in itertools.product((0, 1, 2), repeat=3):
            contexts.append(
                (
                    "abstract_context_probe",
                    dict(
                        seed,
                        active_programs=active,
                        window_programs=window,
                        ordinary_violations=violations,
                    ),
                )
            )
        # Positive request/correction cases and boundary interactions, not just failing seeds.
        positive = dict(
            seed,
            issuer_elected=True,
            request_present=True,
            affirmative=True,
            contact_channel=True,
            form_completed=True,
            prior_default_allocation=True,
            instruction_count=1,
            active_programs=1,
            notice_count=1,
        )
        contexts.append(("abstract_context_probe", positive))
        for key in sorted(BOOLS):
            contexts.append(("abstract_context_probe", dict(positive, **{key: False})))
        for actual, unavoidable, remaining in itertools.product((0, 1, 2), repeat=3):
            contexts.append(
                (
                    "abstract_context_probe",
                    dict(
                        seed,
                        protected_reduction=actual,
                        unavoidable_protected_reduction=unavoidable,
                        other_capacity_remaining=remaining,
                    ),
                )
            )
    seen, cases = set(), []
    for origin, row in contexts:
        signature = json.dumps({k: v for k, v in row.items() if k != "batch_id"}, sort_keys=True)
        if (origin, signature) in seen:
            continue
        seen.add((origin, signature))
        cases.append({"origin": origin, "context": dict(row, batch_id=f"CASE{len(cases):05}")})
    connection = duckdb.connect(config={"enable_external_access": False})
    load_duckdb(connection, types, cases)
    expected = {}
    for policy_id, condition in constraints.items():
        expected[policy_id] = dict(
            connection.execute(
                f"SELECT batch_id, ({condition}) IS TRUE FROM regz_context"
            ).fetchall()
        )
    connection.close()
    write_json(destination / "suite.json", {"types": types, "cases": cases, "expected": expected})
    write_json(
        destination / "manifest.json",
        {
            "oracle_status": "provisional_reference_not_legal_ground_truth",
            "source_hashes": hashes,
            "suite_sha256": digest((destination / "suite.json").read_bytes()),
            "case_count": len(cases),
            "policy_outcome_coverage": {
                pid: sorted(set(values.values())) for pid, values in expected.items()
            },
            "notes": "Abstract probes test predicate logic; correlations need not be ledger-realizable. Reference outputs must be adjudicated on disagreement.",
        },
    )


def load_duckdb(connection, types, cases):
    connection.execute(
        "CREATE TABLE regz_context ("
        + ",".join(f"{key} {dtype}" for key, dtype in types.items())
        + ")"
    )
    connection.executemany(
        "INSERT INTO regz_context VALUES (" + ",".join("?" for _ in types) + ")",
        [[case["context"][key] for key in types] for case in cases],
    )


def query_only(sql, dialect):
    import sqlglot
    from sqlglot import exp

    expressions = sqlglot.parse(sql, read=dialect)
    if len(expressions) != 1 or not isinstance(expressions[0], exp.Query):
        raise ValueError("Expected exactly one read-only SELECT")
    if any(
        isinstance(
            node, (exp.Insert, exp.Update, exp.Delete, exp.Create, exp.Drop, exp.Command, exp.Into)
        )
        for node in expressions[0].walk()
    ):
        raise ValueError("Query contains a prohibited operation")
    return sql.strip().rstrip(";")


def normalize_rows(rows, expected_ids):
    if any(len(row) != 2 or row[1] not in (True, False, 0, 1, "t", "f") for row in rows):
        raise ValueError("Expected batch_id and non-null Boolean passed")
    ids = [row[0] for row in rows]
    if len(ids) != len(set(ids)) or set(ids) != set(expected_ids):
        raise ValueError("Query must return exactly one result for every context")
    return {row[0]: row[1] in (True, 1, "t") for row in rows}


@contextmanager
def postgres_suite(suite, image):
    name = "regz-grader-" + uuid4().hex[:12]
    try:
        run(
            [
                "docker",
                "run",
                "-d",
                "--name",
                name,
                "--network",
                "none",
                "--memory=1g",
                "--cpus=2",
                "--pids-limit=128",
                "-e",
                "POSTGRES_PASSWORD=" + uuid4().hex,
                image,
            ]
        )
        for _ in range(60):
            ready = subprocess.run(
                ["docker", "exec", name, "pg_isready", "-h", "127.0.0.1", "-U", "postgres"],
                capture_output=True,
                check=False,
            )
            if ready.returncode == 0:
                break
            time.sleep(0.5)
        else:
            raise RuntimeError("PostgreSQL did not become ready")

        def execute(sql):
            return run(
                [
                    "docker",
                    "exec",
                    "-i",
                    name,
                    "psql",
                    "-X",
                    "-qAt",
                    "-v",
                    "ON_ERROR_STOP=1",
                    "-U",
                    "postgres",
                ],
                input=sql,
                timeout=30,
            ).stdout

        ddl = (
            "CREATE TABLE regz_context ("
            + ",".join(f"{key} {dtype}" for key, dtype in suite["types"].items())
            + ");\n"
        )

        def literal(value):
            if isinstance(value, bool):
                return "TRUE" if value else "FALSE"
            if isinstance(value, int):
                return str(value)
            return "'" + str(value).replace("'", "''") + "'"

        values = []
        for case in suite["cases"]:
            row = case["context"]
            values.append(
                "("
                + ",".join(
                    literal(bool(row[key]) if key in BOOLS else row[key]) for key in suite["types"]
                )
                + ")"
            )
        execute(
            ddl
            + "INSERT INTO regz_context VALUES "
            + ",".join(values)
            + "; CREATE ROLE policy_reader; GRANT SELECT ON regz_context TO policy_reader;"
        )

        def evaluate(sql):
            import io

            sql = query_only(sql, "postgres")
            output = execute(
                "SET ROLE policy_reader; SET statement_timeout='10s'; BEGIN READ ONLY; COPY ("
                + sql
                + ") TO STDOUT WITH CSV; ROLLBACK;"
            )
            return list(csv.reader(io.StringIO(output)))

        yield evaluate
    finally:
        subprocess.run(["docker", "rm", "-fv", name], capture_output=True, check=False)


def grade(args):
    from data_flow_control import Policy, Resolution
    import duckdb

    if args.output.exists():
        raise FileExistsError("Refusing to overwrite a grade or its adjudication notes")
    suite = json.loads((args.oracle / "suite.json").read_text())
    manifest = json.loads((args.oracle / "manifest.json").read_text())
    if digest((args.oracle / "suite.json").read_bytes()) != manifest["suite_sha256"]:
        raise ValueError("Oracle suite changed after snapshot")
    submission = json.loads(args.submission.read_text())
    if "files" in submission:
        raise ValueError(
            "Workflow artifacts require full execution grading; the predicate grader is not applicable"
        )
    policies = validate_submission(submission)
    output = {
        "policy_results": {},
        "approach": args.approach,
        "oracle_status": manifest["oracle_status"],
        "oracle_manifest": manifest,
        "submission_sha256": digest(args.submission.read_bytes()),
        "disagreements": [],
        "errors": [],
        "adjudication_status": "unreviewed",
        "scope": "policy_layer_only",
    }
    ids = [case["context"]["batch_id"] for case in suite["cases"]]
    by_id = {case["context"]["batch_id"]: case for case in suite["cases"]}
    output["evaluation_versions"] = {"duckdb": duckdb.__version__, "sqlite": sqlite3.sqlite_version}
    if args.approach == "provsql":
        output["evaluation_versions"]["postgres_image_id"] = run(
            ["docker", "image", "inspect", args.postgres_image, "--format", "{{.Id}}"]
        ).stdout.strip()
        args.postgres_image = output["evaluation_versions"]["postgres_image_id"]
    database = duckdb.connect(config={"enable_external_access": False})
    load_duckdb(database, suite["types"], suite["cases"])
    sqlite = sqlite3.connect(":memory:")
    sqlite.execute("CREATE TABLE regz_context (" + ",".join(suite["types"]) + ")")
    sqlite.executemany(
        "INSERT INTO regz_context VALUES (" + ",".join("?" for _ in suite["types"]) + ")",
        [[case["context"][key] for key in suite["types"]] for case in suite["cases"]],
    )
    sqlite.set_authorizer(
        lambda action, *_: sqlite3.SQLITE_OK
        if action
        in (
            sqlite3.SQLITE_SELECT,
            sqlite3.SQLITE_READ,
            sqlite3.SQLITE_FUNCTION,
            sqlite3.SQLITE_RECURSIVE,
        )
        else sqlite3.SQLITE_DENY
    )

    def evaluate_all(pg=None):
        comparisons = 0
        for item in policies:
            policy_id, code = item["id"], item["code"]
            try:
                if args.approach == "dfc":
                    policy = Policy.from_pgn(code)
                    if (
                        policy.sources != ["regz_context"]
                        or policy.sink != "payment_allocations"
                        or policy.sink_alias != "allocation"
                        or policy.on_fail != Resolution.REMOVE
                    ):
                        raise ValueError("Incorrect DFC source/sink/alias/resolution")
                    query = query_only(
                        f"SELECT batch_id, ({policy.constraint}) IS TRUE FROM regz_context",
                        "duckdb",
                    )
                    timer = Timer(10, database.interrupt)
                    timer.start()
                    try:
                        rows = database.execute(query).fetchall()
                    finally:
                        timer.cancel()
                elif args.approach == "gprom":
                    deadline = time.monotonic() + 10
                    sqlite.set_progress_handler(
                        lambda end=deadline: int(time.monotonic() > end), 1000
                    )
                    rows = sqlite.execute(query_only(code, "sqlite")).fetchall()
                else:
                    rows = pg(code)
                actual = normalize_rows(rows, ids)
                output["policy_results"][policy_id] = actual
                expected = suite["expected"][policy_id]
                for case_id in ids:
                    comparisons += 1
                    if actual[case_id] != expected[case_id]:
                        output["disagreements"].append(
                            {
                                "policy_id": policy_id,
                                "case_id": case_id,
                                **by_id[case_id],
                                "reference_passed": expected[case_id],
                                "generated_passed": actual[case_id],
                                "classification": "unresolved",
                                "possible_causes": [
                                    "generated_policy",
                                    "reference_policy",
                                    "specification",
                                    "fixture",
                                    "adapter",
                                ],
                                "review_note": "",
                            }
                        )
            except Exception as exc:  # Preserve per-policy failures without aborting other checks.
                output["errors"].append({"policy_id": policy_id, "error": str(exc)[:4000]})
        output["comparisons"] = comparisons

    try:
        if args.approach == "provsql":
            with postgres_suite(suite, args.postgres_image) as evaluator:
                evaluate_all(evaluator)
        else:
            evaluate_all()
    finally:
        database.close()
        sqlite.close()
    output["overall_disagreements"] = []
    if not output["errors"]:
        for case_id in ids:
            actual = all(output["policy_results"][pid][case_id] for pid in IDS)
            expected = all(suite["expected"][pid][case_id] for pid in IDS)
            if actual != expected:
                output["overall_disagreements"].append(
                    {"case_id": case_id, "reference_passed": expected, "generated_passed": actual}
                )
    output["reference_agreement"] = not output["disagreements"] and not output["errors"]
    output["disagreement_count"] = len(output["disagreements"])
    write_json(args.output, output)
    print(
        json.dumps(
            {
                key: output[key]
                for key in ("reference_agreement", "comparisons", "disagreement_count")
            }
        )
    )


def smoke(image):
    with isolated_network(image) as network:
        command = container_command(image, network, "regz-smoke-" + uuid4().hex[:12])
        command[-1:] = ["--entrypoint", "python3", image, "/opt/experiment/smoke.py"]
        print(run(command).stdout.strip())


def score_run(args):
    """Offline grading never supplies feedback to a generating session."""
    metadata_paths = sorted(args.run.glob("*/metadata.json"))
    if any(json.loads(path.read_text()).get("benchmark") == "workflow" for path in metadata_paths):
        raise ValueError(
            "Workflow grading is not implemented: do not score full artifacts as independent predicates"
        )
    rows = []
    for metadata_path in metadata_paths:
        folder = metadata_path.parent
        metadata = json.loads(metadata_path.read_text())
        row = {
            key: metadata.get(key)
            for key in ("approach", "trial", "status", "wall_seconds_including_cli_startup")
        }
        usage = metadata.get("usage") or {}
        row.update(
            {
                key: usage.get(key)
                for key in (
                    "input_tokens",
                    "cached_input_tokens",
                    "output_tokens",
                    "reasoning_output_tokens",
                )
            }
        )
        row.update(
            reference_agreement=None if metadata["status"] == "infrastructure_error" else False,
            disagreement_count=None,
            evaluation_error=None,
        )
        if metadata["status"] == "submitted":
            try:
                output = folder / "grade.json"
                if output.exists():
                    raise FileExistsError(
                        "Existing grade.json: preserve it for adjudication; use a fresh run copy to regrade"
                    )
                grade(
                    argparse.Namespace(
                        approach=metadata["approach"],
                        submission=folder / "submission.json",
                        oracle=args.oracle,
                        output=output,
                        postgres_image=args.postgres_image,
                    )
                )
                report = json.loads(output.read_text())
                row.update(
                    reference_agreement=report["reference_agreement"],
                    disagreement_count=report["disagreement_count"],
                    evaluation_error=json.dumps(report["errors"]) if report["errors"] else None,
                )
            except Exception as exc:
                row["reference_agreement"] = None
                row["evaluation_error"] = str(exc)
        rows.append(row)
    if not rows:
        raise ValueError("No trial metadata found")
    with (args.run / "summary.csv").open("x", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    prompts = commands.add_parser("prompts")
    prompts.add_argument("--benchmark", choices=("predicate", "workflow"), default="workflow")
    probe = commands.add_parser("smoke")
    probe.add_argument("--image", default=IMAGE)
    score = commands.add_parser("score-run")
    score.add_argument("run", type=Path)
    score.add_argument("oracle", type=Path)
    score.add_argument("--postgres-image", default="postgres:16")
    build = commands.add_parser("build")
    build.add_argument("--image", default=IMAGE)
    oracle = commands.add_parser("prepare-oracle")
    oracle.add_argument("output", type=Path)
    trial = commands.add_parser("generate")
    trial.add_argument("output", type=Path)
    trial.add_argument("--benchmark", choices=("predicate", "workflow"), default="workflow")
    trial.add_argument("--auth", type=Path, default=Path.home() / ".codex/auth.json")
    trial.add_argument("--image", default=IMAGE)
    trial.add_argument("--model", default="gpt-6-luna")
    trial.add_argument("--effort", default=None, help="Omit to use the model default")
    trial.add_argument("--approaches", nargs="+", choices=APPROACHES, default=list(APPROACHES))
    trial.add_argument("--trials", type=int, default=5)
    trial.add_argument("--seed", type=int, default=20261017)
    trial.add_argument("--timeout", type=int, default=3600)
    trial.add_argument("--parallelism", type=int, default=15)
    check = commands.add_parser("grade")
    check.add_argument("approach", choices=APPROACHES)
    check.add_argument("submission", type=Path)
    check.add_argument("oracle", type=Path)
    check.add_argument("output", type=Path)
    check.add_argument("--postgres-image", default="postgres:16")
    args = parser.parse_args()
    if args.command == "prompts":
        build_prompts(args.benchmark)
    elif args.command == "smoke":
        smoke(args.image)
    elif args.command == "score-run":
        score_run(args)
    elif args.command == "build":
        subprocess.run(["docker", "build", "-t", args.image, str(ROOT / "container")], check=True)
    elif args.command == "prepare-oracle":
        prepare_oracle(args.output)
    elif args.command == "generate":
        if args.trials < 1 or args.timeout < 1 or args.parallelism < 1:
            parser.error("trials, timeout, and parallelism must be positive")
        generate(args)
    else:
        grade(args)


if __name__ == "__main__":
    main()
