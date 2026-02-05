"""Shared helpers for multi-database integrations."""

from __future__ import annotations

from decimal import Decimal
import subprocess
import time
from typing import Any

import pg8000


def run_docker(args: list[str]) -> str:
    result = subprocess.run(
        ["docker", *args],
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        stderr = result.stderr.strip()
        stdout = result.stdout.strip()
        details = stderr or stdout or f"exit code {result.returncode}"
        raise RuntimeError(f"docker {' '.join(args)} failed: {details}")
    return result.stdout.strip()


def wait_for_pg_ready(
    *,
    host: str,
    port: int,
    user: str,
    password: str,
    database: str,
    ssl_context=None,
    timeout_s: int = 60,
) -> None:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            conn = pg8000.connect(
                host=host,
                port=port,
                user=user,
                password=password,
                database=database,
                timeout=5,
                ssl_context=ssl_context,
            )
            conn.close()
            return
        except Exception:
            time.sleep(1)
    raise RuntimeError("Database did not become ready in time.")


def normalize_results(results: list[tuple[Any, ...]]) -> list[tuple[Any, ...]]:
    normalized: list[tuple[Any, ...]] = []
    for row in results:
        normalized_row = []
        for val in row:
            if isinstance(val, Decimal):
                normalized_row.append(float(val))
            elif isinstance(val, str):
                stripped = val.strip()
                if stripped.isdigit():
                    normalized_row.append(int(stripped))
                else:
                    try:
                        normalized_row.append(float(stripped))
                    except ValueError:
                        normalized_row.append(val)
            else:
                normalized_row.append(val)
        normalized.append(tuple(normalized_row))
    return normalized
