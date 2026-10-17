"""Incremental, concurrent runner for the LLM validation benchmark."""

from __future__ import annotations

from concurrent.futures import Future, ThreadPoolExecutor, as_completed
import csv
from dataclasses import dataclass
import os
from typing import TYPE_CHECKING

from experiment_harness import ExperimentContext, ExperimentResult

from vldb_experiments.strategies.llm_validation_common import APPROACH_DFC_1PHASE

if TYPE_CHECKING:
    from collections.abc import Iterable
    from pathlib import Path

    from vldb_experiments.strategies.llm_validation_strategy import LLMValidationStrategy

MAX_INFERENCE_CONCURRENCY = 50
BASE_FIELDS = ["execution_number", "timestamp", "duration_ms", "error"]


@dataclass(frozen=True)
class ValidationTask:
    """One measured execution in the deterministic benchmark schedule."""

    execution_number: int
    query_num: int
    policy_count: int
    approach: str
    run_num: int

    @property
    def key(self) -> tuple[int, int, str, int]:
        return self.query_num, self.policy_count, self.approach, self.run_num


def build_tasks(strategy: LLMValidationStrategy) -> list[ValidationTask]:
    """Expand strategy settings into stable execution-numbered tasks."""
    tasks: list[ValidationTask] = []
    execution_number = 0
    for query_num, policy_count, approach in strategy.settings:
        for run_num in range(1, strategy.runs_per_setting + 1):
            execution_number += 1
            tasks.append(
                ValidationTask(
                    execution_number=execution_number,
                    query_num=query_num,
                    policy_count=policy_count,
                    approach=approach,
                    run_num=run_num,
                )
            )
    return tasks


class IncrementalResultWriter:
    """Append and fsync each result while tracking resumable task keys."""

    def __init__(self, path: Path, metric_names: Iterable[str], *, resume: bool) -> None:
        self.path = path
        self.fieldnames = [*BASE_FIELDS, *metric_names]
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if self.path.exists() and not resume:
            self.path.unlink()
        self.completed_keys = self._read_completed_keys() if self.path.exists() else set()
        if not self.path.exists():
            with self.path.open("w", newline="") as output:
                csv.DictWriter(output, fieldnames=self.fieldnames).writeheader()
                output.flush()
                os.fsync(output.fileno())

    def _read_completed_keys(self) -> set[tuple[int, int, str, int]]:
        completed: set[tuple[int, int, str, int]] = set()
        with self.path.open(newline="") as source:
            reader = csv.DictReader(source)
            if reader.fieldnames != self.fieldnames:
                raise ValueError(
                    f"Cannot resume {self.path}: CSV columns do not match this benchmark version. "
                    "Use --overwrite or choose a new output filename."
                )
            for row in reader:
                if row.get("error", "").strip():
                    continue
                completed.add(
                    (
                        int(row["query_num"]),
                        int(row["policy_count"]),
                        row["approach"],
                        int(row["run_num"]),
                    )
                )
        return completed

    def append(self, task: ValidationTask, result: ExperimentResult) -> None:
        row = {
            "execution_number": task.execution_number,
            "timestamp": result.timestamp.isoformat() if result.timestamp else "",
            "duration_ms": result.duration_ms,
            "error": result.error or "",
        }
        row.update(result.custom_metrics)
        with self.path.open("a", newline="") as output:
            writer = csv.DictWriter(output, fieldnames=self.fieldnames)
            writer.writerow(row)
            output.flush()
            os.fsync(output.fileno())
        if not result.error:
            self.completed_keys.add(task.key)


def _execute_task(
    strategy: LLMValidationStrategy,
    strategy_config: dict[str, object],
    task: ValidationTask,
) -> ExperimentResult:
    context = ExperimentContext(
        execution_number=task.execution_number,
        is_warmup=False,
        strategy_config=strategy_config,
    )
    return strategy.execute(context)


def run_incremental_parallel(
    strategy: LLMValidationStrategy,
    strategy_config: dict[str, object],
    output_path: Path,
    *,
    max_concurrency: int,
    resume: bool = True,
) -> tuple[int, int]:
    """Run local DFC tasks serially and Bedrock requests concurrently."""
    if not 1 <= max_concurrency <= MAX_INFERENCE_CONCURRENCY:
        raise ValueError(f"max_concurrency must be between 1 and {MAX_INFERENCE_CONCURRENCY}")

    setup_context = ExperimentContext(strategy_config=strategy_config)
    strategy.setup(setup_context)
    try:
        print("Preparing local query-result samples...")
        strategy.prepare_inputs()
        tasks = build_tasks(strategy)
        writer = IncrementalResultWriter(output_path, strategy.get_metrics(), resume=resume)
        pending = [task for task in tasks if task.key not in writer.completed_keys]
        skipped = len(tasks) - len(pending)
        if skipped:
            print(f"Resuming: skipping {skipped} successful executions already in the CSV.")

        dfc_tasks = [task for task in pending if task.approach == APPROACH_DFC_1PHASE]
        inference_tasks = [task for task in pending if task.approach != APPROACH_DFC_1PHASE]
        completed = skipped

        for task in dfc_tasks:
            result = _execute_task(strategy, strategy_config, task)
            writer.append(task, result)
            completed += 1
            status = "ok" if not result.error else "failed"
            print(f"Saved {completed}/{len(tasks)} ({task.approach}, {status})")

        if inference_tasks:
            strategy.prepare_ground_truth()
            worker_count = min(max_concurrency, len(inference_tasks))
            print(
                f"Running {len(inference_tasks)} inference requests with up to "
                f"{worker_count} concurrent requests..."
            )
            with ThreadPoolExecutor(max_workers=worker_count) as executor:
                futures: dict[Future[ExperimentResult], ValidationTask] = {
                    executor.submit(_execute_task, strategy, strategy_config, task): task
                    for task in inference_tasks
                }
                for future in as_completed(futures):
                    task = futures[future]
                    try:
                        result = future.result()
                    except Exception as exc:
                        result = ExperimentResult(duration_ms=0.0, error=str(exc))
                    writer.append(task, result)
                    completed += 1
                    status = "ok" if not result.error else "failed"
                    print(f"Saved {completed}/{len(tasks)} ({task.approach}, {status})")
        return completed, len(tasks)
    finally:
        strategy.teardown(setup_context)
