"""Eight-case workflow integration evaluator; run in a separate process per trial."""

import argparse
from dataclasses import replace
import hashlib
import json
from pathlib import Path
import re
import sqlite3
import subprocess
from threading import Timer
import time
from uuid import uuid4

from data_flow_control import Policy
from data_flow_control.adapters.duckdb import DuckDBAdapter
from data_flow_control.connection import Connection
import duckdb

from vldb_experiments.strategies.regulation_z_gprom import export_gprom_workload
from vldb_experiments.strategies.regulation_z_payment_allocation_benchmark import (
    build_regulation_z_53_policies,
    generate_payment_allocation_workload,
    reset_payment_allocation_database,
)
from vldb_experiments.strategies.regulation_z_provsql import export_provsql_workload


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("trial", type=Path)
    args = parser.parse_args()
    trial = args.trial.resolve()

    container_name = "regz-execution-" + uuid4().hex[:12]

    def save(p, v):
        p.write_text(json.dumps(v, indent=2, default=str) + "\n")

    def run(cmd, **kw):
        return subprocess.run(cmd, capture_output=True, text=True, timeout=120, **kw)

    def lit(s):
        return "'" + s.replace("'", "''") + "'"

    def pg(sql, db="postgres"):
        return run(
            [
                "docker",
                "exec",
                "-i",
                container_name,
                "psql",
                "-X",
                "-qAt",
                "-U",
                "postgres",
                "-d",
                db,
                "-v",
                "ON_ERROR_STOP=1",
            ],
            input="SET statement_timeout='15s'; SET jit=off; SET provsql.active=off;\n" + sql,
        )

    def checked(r):
        if r.returncode:
            raise RuntimeError(r.stderr[-4000:])
        return r.stdout

    workload = generate_payment_allocation_workload(num_accounts=4, num_updates=4, no_ties=True)
    first = workload[0]
    aligned = []
    for c in workload:
        delta = c.cycle_start - first.cycle_start
        aligned.append(
            replace(
                c,
                cycle_start=c.cycle_start - delta,
                cycle_end=c.cycle_end - delta,
                payment_date=c.payment_date - delta,
                transactions=tuple(
                    replace(
                        t,
                        transaction_date=t.transaction_date - delta,
                        post_date=t.post_date - delta,
                    )
                    for t in c.transactions
                ),
                sql=c.sql.replace(c.cycle_end.isoformat(), first.cycle_end.isoformat()),
            )
        )
    workload = aligned
    valid = next(c for c in workload if c.is_valid)
    other = next(c for c in workload if c.allocation_id != valid.allocation_id)
    cases = [
        {
            "name": "base_" + str(c.case_number),
            "case": c,
            "target": c.allocation_id,
            "setup": "",
            "allow": c.is_valid,
            "match": True,
        }
        for c in workload
    ]
    cases += [
        {
            "name": "missing_target",
            "case": valid,
            "target": "MISSING",
            "setup": "",
            "allow": False,
            "match": False,
        },
        {
            "name": "missing_candidate",
            "case": valid,
            "target": valid.allocation_id,
            "setup": f"DELETE FROM daily_balances WHERE account_id={lit(valid.account_id)};",
            "allow": False,
            "match": False,
        },
        {
            "name": "missing_selected_context",
            "case": valid,
            "target": valid.allocation_id,
            "setup": f"DELETE FROM regz_context WHERE batch_id={lit(valid.expected_selected_transaction_id)};",
            "allow": False,
            "match": False,
        },
        {
            "name": "unrelated_bad_context",
            "case": valid,
            "target": valid.allocation_id,
            "setup": f"UPDATE regz_context SET unauthorized_changes=1 WHERE batch_id={lit(other.expected_selected_transaction_id)};",
            "allow": True,
            "match": True,
        },
    ]
    fixture = trial / "execution-fixtures"
    fixture.mkdir(exist_ok=True)
    if not (fixture / "gprom/data.sqlite").exists():
        export_gprom_workload(fixture / "gprom", workload)
    if not (fixture / "provsql/data.sql").exists():
        export_provsql_workload(fixture / "provsql", workload)
    db = duckdb.connect()
    reset_payment_allocation_database(db, workload)
    tables = [r[0] for r in db.execute("SHOW TABLES").fetchall()]
    db.close()

    def snapshot(db):
        return {t: sorted(db.execute("SELECT * FROM " + t).fetchall(), key=repr) for t in tables}

    def expected(db, s, write):
        before = snapshot(db)
        if write:
            db.execute(
                f"UPDATE payment_allocations SET cash_advance_allocation=100 WHERE allocation_id={lit(s['target'])}"
            )
        after = snapshot(db)
        if write:
            db.execute(
                f"UPDATE payment_allocations SET cash_advance_allocation=0 WHERE allocation_id={lit(s['target'])}"
            )
        assert snapshot(db) == before
        return after

    # Check expected outcomes with existing reference policies as a provisional oracle.
    refs = []
    for s in cases:
        db = duckdb.connect()
        reset_payment_allocation_database(db, workload)
        if s["setup"]:
            db.execute(s["setup"])
        want = expected(db, s, s["allow"])
        conn = Connection(DuckDBAdapter(db))
        for p in build_regulation_z_53_policies():
            conn.register_policy(p)
        sql = s["case"].sql.replace(s["case"].allocation_id, s["target"])
        db.execute(conn.transform_query(sql))
        refs.append({"case": s["name"], "matches": snapshot(db) == want})
        db.close()
    save(fixture / "reference-results.json", refs)
    assert all(r["matches"] for r in refs), refs
    save(
        fixture / "cases.json",
        [{k: (v.allocation_id if k == "case" else v) for k, v in s.items()} for s in cases],
    )
    report = {}
    for a in [trial / "artifacts"] if trial.name.startswith("dfc-") else []:
        out = a.parent / "execution"
        out.mkdir(exist_ok=True)
        rows = []
        parse_started = time.monotonic()
        try:
            policies = [
                Policy.from_pgn(p.read_text()) for p in sorted((a / "policies").glob("*.pgn"))
            ]
        except Exception as exc:
            rows = [
                {"case": s["name"], "matches": False, "error": str(exc), "stage": "policy_parse"}
                for s in cases
            ]
            save(out / "results.json", rows)
            return
        save(out / "parse-timing.json", {"seconds": time.monotonic() - parse_started})
        for s in cases:
            item = {"case": s["name"], "expected_allow": s["allow"]}
            db = duckdb.connect()
            try:
                reset_payment_allocation_database(db, workload)
                if s["setup"]:
                    db.execute(s["setup"])
                want = expected(db, s, s["allow"])
                before = snapshot(db)
                conn = Connection(DuckDBAdapter(db))
                for policy in policies:
                    conn.register_policy(policy)
                sql = conn.transform_query(
                    s["case"].sql.replace(s["case"].allocation_id, s["target"])
                )
                (out / (s["name"] + ".sql")).write_text(sql)
                # Bound native DuckDB execution just as SQLite/PostgreSQL are bounded.
                timer = Timer(20, db.interrupt)
                timer.start()
                try:
                    db.execute(sql)
                finally:
                    timer.cancel()
                after = snapshot(db)
                item.update(
                    matches=after == want,
                    changed_tables=[t for t in tables if before[t] != after[t]],
                )
                if after != want:
                    item["differences"] = {
                        t: {"expected": want[t], "actual": after[t]}
                        for t in tables
                        if want[t] != after[t]
                    }
            except Exception as e:
                item.update(matches=False, error=str(e))
            finally:
                db.close()
            rows.append(item)
        report[a.parent.name] = rows
        save(out / "results.json", rows)
        print(a.parent.name, sum(r["matches"] for r in rows), len(rows), flush=True)
    for a in [trial / "artifacts"] if trial.name.startswith("gprom-") else []:
        out = a.parent / "execution"
        out.mkdir(exist_ok=True)
        rows = []
        for s in cases:
            item = {"case": s["name"], "expected_allow": s["allow"]}
            folder = fixture / ("gprom_" + s["name"])
            folder.mkdir(exist_ok=True)
            path = folder / "data.sqlite"
            if not path.exists():
                path.write_bytes((fixture / "gprom/data.sqlite").read_bytes())
                db = sqlite3.connect(path)
                if s["setup"]:
                    db.executescript(s["setup"])
                db.commit()
                db.close()
            before = hashlib.sha256(path.read_bytes()).hexdigest()
            db = sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)
            db.row_factory = sqlite3.Row
            db.execute("PRAGMA query_only=ON")
            try:
                db.set_progress_handler(
                    lambda cutoff=time.monotonic() + 20: int(time.monotonic() > cutoff), 1000
                )
                found = [
                    dict(r)
                    for r in db.execute(
                        (a / "candidate.sql").read_text(), {"allocation_id": s["target"]}
                    )
                ]
                item["candidates"] = found
                if not s["match"]:
                    item["matches"] = found == []
                else:
                    if len(found) != 1:
                        raise ValueError("Expected exactly one candidate")
                    candidate = found[0]
                    c = s["case"]
                    want = {
                        "allocation_id": s["target"],
                        "account_id": c.account_id,
                        "statement_id": c.statement_id,
                        "payment_id": c.payment_transaction_id,
                        "batch_id": c.expected_selected_transaction_id,
                        "proposed_cash_dollars": 100,
                    }
                    if candidate != want:
                        raise ValueError(
                            "Wrong frozen candidate: " + repr(candidate) + " expected " + repr(want)
                        )
                    request = (
                        (a / "reenact.sql")
                        .read_text()
                        .replace("{{allocation_id_literal}}", lit(s["target"]))
                        .replace(
                            "{{proposed_cash_literal}}", str(candidate["proposed_cash_dollars"])
                        )
                    )
                    req = folder / (a.parent.name + ".sql")
                    req.write_text(request)
                    result = run(
                        [
                            "docker",
                            "run",
                            "--rm",
                            "--network",
                            "none",
                            "--memory=1g",
                            "--cpus=2",
                            "-v",
                            str(folder) + ":/research:ro",
                            "iitdbgroup/gprom:latest",
                            "-backend",
                            "sqlite",
                            "-db",
                            "/research/data.sqlite",
                            "-queryFile",
                            "/research/" + req.name,
                            "-Pexecutor",
                            "sql",
                        ]
                    )
                    (out / (s["name"] + ".engine.log")).write_text(result.stdout + result.stderr)
                    checked(result)
                    generated = re.sub(r"\x1b\[[0-9;]*m", "", result.stdout).strip().rstrip(";")
                    db.set_progress_handler(
                        lambda cutoff=time.monotonic() + 20: int(time.monotonic() > cutoff), 1000
                    )
                    hypothetical = [
                        {k.lower(): v for k, v in dict(r).items()} for r in db.execute(generated)
                    ]
                    baseline = [dict(r) for r in db.execute("SELECT * FROM payment_allocations")]
                    for row in baseline:
                        if row["allocation_id"] == s["target"]:
                            row["cash_advance_allocation"] = 100
                    keys = list(baseline[0])
                    observed = [{k: r[k] for k in keys} for r in hypothetical]
                    item["proposed_state_matches"] = sorted(
                        observed, key=lambda r: r["allocation_id"]
                    ) == sorted(baseline, key=lambda r: r["allocation_id"])
                    query = (a / "check.sql").read_text().replace("{{reenacted_select}}", generated)
                    (out / (s["name"] + ".sql")).write_text(query)
                    actual = [dict(r) for r in db.execute(query, candidate)]
                    item.update(
                        result=actual,
                        matches=item["proposed_state_matches"]
                        and actual
                        == [
                            {
                                "allocation_id": s["target"],
                                "batch_id": c.expected_selected_transaction_id,
                                "proposed_cash_dollars": 100,
                                "decision": "ALLOW" if s["allow"] else "REJECT",
                            }
                        ],
                    )
            except Exception as e:
                item.update(matches=False, error=str(e))
            finally:
                db.close()
            item["fixture_unchanged"] = before == hashlib.sha256(path.read_bytes()).hexdigest()
            item["matches"] = item["matches"] and item["fixture_unchanged"]
            rows.append(item)
        report[a.parent.name] = rows
        save(out / "results.json", rows)
        print(a.parent.name, sum(r["matches"] for r in rows), len(rows), flush=True)
    if not trial.name.startswith("provsql-"):
        save(trial / "execution-results.json", report)
        return
    # Each script gets a fresh database with real ProvSQL and no network/host mounts.
    checked(
        run(
            [
                "docker",
                "run",
                "-d",
                "--name",
                container_name,
                "--network",
                "none",
                "--memory=2g",
                "--cpus=2",
                "inriavalda/provsql:latest",
            ]
        )
    )
    try:
        for _ in range(60):
            if pg("SELECT 1;").returncode == 0:
                break
            time.sleep(0.5)
        else:
            raise RuntimeError("ProvSQL did not start")
        dump = "\n".join(
            "SELECT json_build_object('table',"
            + lit(t)
            + ",'rows',COALESCE(json_agg(x ORDER BY row_to_json(x)::text),'[]'::json))::jsonb::text FROM public."
            + t
            + " x;"
            for t in tables
        )

        def pgstate(name):
            return {
                r["table"]: r["rows"]
                for line in checked(pg(dump, name)).splitlines()
                if line.startswith("{")
                for r in [json.loads(line)]
            }

        for a in [trial / "artifacts"] if trial.name.startswith("provsql-") else []:
            out = a.parent / "execution"
            out.mkdir(exist_ok=True)
            rows = []
            for i, s in enumerate(cases):
                item = {"case": s["name"], "expected_allow": s["allow"]}
                dbname = "trial_" + a.parent.name.replace("-", "_") + "_" + str(i)
                try:
                    checked(pg("CREATE DATABASE " + dbname + ";"))
                    checked(pg((fixture / "provsql/data.sql").read_text(), dbname))
                    if s["setup"]:
                        checked(pg(s["setup"], dbname))
                    before = pgstate(dbname)
                    want = json.loads(json.dumps(before))
                    if s["allow"]:
                        for row in want["payment_allocations"]:
                            if row["allocation_id"] == s["target"]:
                                row["cash_advance_allocation"] = 100
                    sql = (
                        (a / "transaction.sql")
                        .read_text()
                        .replace("{{allocation_id_literal}}", lit(s["target"]))
                    )
                    (out / (s["name"] + ".sql")).write_text(sql)
                    result = pg(sql, dbname)
                    (out / (s["name"] + ".execution.log")).write_text(result.stdout + result.stderr)
                    after = pgstate(dbname)

                    def normalize(state):
                        return {
                            t: sorted(rs, key=lambda record: json.dumps(record, sort_keys=True))
                            for t, rs in state.items()
                        }

                    lines = [
                        line
                        for line in result.stdout.splitlines()
                        if line.startswith(s["case"].expected_selected_transaction_id + "|")
                    ]
                    provenance_ok = bool(lines) if s["match"] else True
                    item.update(
                        returncode=result.returncode,
                        stderr=result.stderr,
                        state_matches=normalize(after) == normalize(want),
                        provenance_lines=lines,
                        provenance_present=provenance_ok,
                        changed_tables=[t for t in tables if before[t] != after[t]],
                        matches=result.returncode == 0
                        and normalize(after) == normalize(want)
                        and provenance_ok,
                    )
                    if not item["state_matches"]:
                        item["differences"] = {
                            t: {"expected": want[t], "actual": after[t]}
                            for t in tables
                            if normalize(want)[t] != normalize(after)[t]
                        }
                except Exception as e:
                    item.update(matches=False, error=str(e))
                rows.append(item)
            report[a.parent.name] = rows
            save(out / "results.json", rows)
            print(a.parent.name, sum(r["matches"] for r in rows), len(rows), flush=True)
    finally:
        run(["docker", "rm", "-fv", container_name])
    save(trial / "execution-results.json", report)


if __name__ == "__main__":
    main()
