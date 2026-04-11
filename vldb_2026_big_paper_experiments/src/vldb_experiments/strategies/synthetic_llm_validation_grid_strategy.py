"""Synthetic LLM validation grid over fixed 3-table join/aggregation query."""

from __future__ import annotations

from dataclasses import replace
import pathlib
import random
import time
from typing import Any
import uuid

from experiment_harness import ExperimentContext, ExperimentResult, ExperimentStrategy
from sql_rewriter import DFCPolicy, Resolution, SQLRewriter

from agent_harness.config import HarnessConfig
from agent_harness.llm_factory import create_chat_model
from vldb_experiments.strategies.llm_validation_common import (
    APPROACH_DFC_1PHASE,
    APPROACH_GPT_QUERY_ONLY,
    APPROACH_GPT_QUERY_RESULTS,
    APPROACH_OPUS_QUERY_ONLY,
    APPROACH_OPUS_QUERY_RESULTS,
    DEFAULT_CLAUDE_MODEL,
    DEFAULT_GPT_MODEL,
    DEFAULT_RUNS_PER_SETTING,
    build_query_only_prompt,
    build_query_results_prompt,
    format_result_rows,
    message_text,
    parse_violation_prediction,
)
from vldb_experiments.strategies.tpch_strategy import _ensure_smokedduck

DEFAULT_SYNTHETIC_DATASET_COUNT = 8
DEFAULT_SYNTHETIC_ROWS_PER_TABLE = 1000
DEFAULT_SYNTHETIC_POLICY_THRESHOLD = 25.0
DEFAULT_SYNTHETIC_QUERY_NUM = 1
DEFAULT_SYNTHETIC_QUERY_NUMS = [1, 2, 3, 4, 5]


def synthetic_dataset_specs(
    dataset_count: int = DEFAULT_SYNTHETIC_DATASET_COUNT,
    base_filename_prefix: str = "synthetic_llm_validation_grid",
) -> list[dict[str, Any]]:
    return [
        {
            "label": f"dataset{i + 1:02d}",
            "dataset_index": i,
            "db_path": f"./results/{base_filename_prefix}_dataset{i + 1:02d}.db",
        }
        for i in range(dataset_count)
    ]


def synthetic_validation_queries() -> dict[int, str]:
    return {
        1: """
SELECT
    c.region,
    p.category,
    SUM(o.quantity * p.unit_price) AS total_revenue,
    AVG(c.loyalty_score) AS avg_loyalty
FROM customers AS c
JOIN sales_orders AS o
    ON c.customer_id = o.customer_id
JOIN products AS p
    ON o.product_id = p.product_id
WHERE
    p.category IN ('B', 'C')
    AND c.region = 'EAST'
GROUP BY
    c.region,
    p.category
ORDER BY
    c.region,
    p.category
""".strip(),
        2: """
SELECT
    c.region,
    c.segment,
    SUM(o.quantity) AS total_units,
    AVG(c.loyalty_score) AS avg_loyalty
FROM customers AS c
JOIN sales_orders AS o
    ON c.customer_id = o.customer_id
JOIN products AS p
    ON o.product_id = p.product_id
WHERE
    p.category IN ('B', 'C')
    AND c.region = 'WEST'
GROUP BY
    c.region,
    c.segment
ORDER BY
    c.region,
    c.segment
""".strip(),
        3: """
SELECT
    c.region,
    p.category,
    SUM(p.unit_price * p.product_score) AS weighted_price,
    AVG(c.loyalty_score) AS avg_loyalty
FROM customers AS c
JOIN sales_orders AS o
    ON c.customer_id = o.customer_id
JOIN products AS p
    ON o.product_id = p.product_id
WHERE
    p.category IN ('B', 'C')
    AND c.region = 'SOUTH'
GROUP BY
    c.region,
    p.category
ORDER BY
    c.region,
    p.category
""".strip(),
        4: """
SELECT
    p.category,
    c.segment,
    SUM(o.quantity * p.unit_price) AS total_revenue,
    AVG(p.product_score) AS avg_product_score
FROM customers AS c
JOIN sales_orders AS o
    ON c.customer_id = o.customer_id
JOIN products AS p
    ON o.product_id = p.product_id
WHERE
    p.category = 'D'
GROUP BY
    p.category,
    c.segment
ORDER BY
    p.category,
    c.segment
""".strip(),
        5: """
SELECT
    p.category,
    c.region,
    SUM(o.quantity) AS total_units,
    AVG(c.loyalty_score) AS avg_loyalty
FROM customers AS c
JOIN sales_orders AS o
    ON c.customer_id = o.customer_id
JOIN products AS p
    ON o.product_id = p.product_id
WHERE
    p.category = 'A'
GROUP BY
    p.category,
    c.region
ORDER BY
    p.category,
    c.region
""".strip(),
    }


def synthetic_validation_query(query_num: int = DEFAULT_SYNTHETIC_QUERY_NUM) -> str:
    queries = synthetic_validation_queries()
    if query_num not in queries:
        raise ValueError(f"Unsupported synthetic query_num={query_num}; expected one of {sorted(queries)}")
    return queries[query_num]


def synthetic_policy_catalog(
    threshold: float = DEFAULT_SYNTHETIC_POLICY_THRESHOLD,
) -> list[tuple[str, str]]:
    threshold_sql = int(threshold) if float(threshold).is_integer() else threshold
    return [
        (
            f"avg(sales_orders.quantity) <= {threshold_sql}",
            f"Average order quantity should remain at or below {threshold_sql}.",
        )
    ]


def build_synthetic_policies(
    policy_count: int,
    threshold: float = DEFAULT_SYNTHETIC_POLICY_THRESHOLD,
) -> list[DFCPolicy]:
    selected = synthetic_policy_catalog(threshold)[:policy_count]
    return [
        DFCPolicy(
            sources=["customers", "sales_orders", "products"],
            constraint=constraint,
            on_fail=Resolution.INVALIDATE,
            description=description,
        )
        for constraint, description in selected
    ]


def populate_synthetic_dataset(
    conn: Any,
    dataset_index: int,
    rows_per_table: int = DEFAULT_SYNTHETIC_ROWS_PER_TABLE,
) -> None:
    rng = random.Random(10_000 + dataset_index)

    conn.execute("DROP TABLE IF EXISTS sales_orders")
    conn.execute("DROP TABLE IF EXISTS customers")
    conn.execute("DROP TABLE IF EXISTS products")

    conn.execute(
        """
        CREATE TABLE customers (
            customer_id INTEGER PRIMARY KEY,
            segment VARCHAR,
            region VARCHAR,
            loyalty_score DOUBLE
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE products (
            product_id INTEGER PRIMARY KEY,
            category VARCHAR,
            unit_price DOUBLE,
            product_score DOUBLE
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE sales_orders (
            order_id INTEGER PRIMARY KEY,
            customer_id INTEGER,
            product_id INTEGER,
            quantity DOUBLE
        )
        """
    )

    segments = ["SMB", "ENT", "PUBLIC", "CONSUMER"]
    regions = ["NORTH", "SOUTH", "EAST", "WEST"]
    categories = ["A", "B", "C", "D"]

    customers_by_id: dict[int, tuple[str, str]] = {}
    products_by_id: dict[int, str] = {}

    customer_rows = [
        (
            customer_id,
            segments[(customer_id - 1) % len(segments)],
            regions[(customer_id - 1) % len(regions)],
            round(rng.uniform(20.0, 100.0), 2),
        )
        for customer_id in range(1, rows_per_table + 1)
    ]
    for customer_id, segment, region, _loyalty_score in customer_rows:
        customers_by_id[customer_id] = (segment, region)
    product_rows = [
        (
            product_id,
            categories[(product_id - 1) % len(categories)],
            round(rng.uniform(10.0, 250.0), 2),
            round(rng.uniform(1.0, 10.0), 2),
        )
        for product_id in range(1, rows_per_table + 1)
    ]
    for product_id, category, _unit_price, _product_score in product_rows:
        products_by_id[product_id] = category

    def quantity_for_order(customer_id: int, product_id: int) -> float:
        _segment, region = customers_by_id[customer_id]
        category = products_by_id[product_id]
        if category == "A":
            return float(rng.randint(18, 22))
        if category == "D":
            return float(rng.randint(28, 32))
        if region == "WEST":
            return float(rng.randint(18, 22) if dataset_index < 2 else rng.randint(28, 32))
        if region == "EAST":
            return float(rng.randint(18, 22) if dataset_index < 4 else rng.randint(28, 32))
        if region == "SOUTH":
            return float(rng.randint(18, 22) if dataset_index < 6 else rng.randint(28, 32))
        return float(rng.randint(18, 22))

    order_rows = []
    for order_id in range(1, rows_per_table + 1):
        customer_id = rng.randint(1, rows_per_table)
        product_id = rng.randint(1, rows_per_table)
        order_rows.append(
            (
                order_id,
                customer_id,
                product_id,
                quantity_for_order(customer_id, product_id),
            )
        )

    conn.executemany("INSERT INTO customers VALUES (?, ?, ?, ?)", customer_rows)
    conn.executemany("INSERT INTO products VALUES (?, ?, ?, ?)", product_rows)
    conn.executemany("INSERT INTO sales_orders VALUES (?, ?, ?, ?)", order_rows)


class SyntheticLLMValidationGridStrategy(ExperimentStrategy):
    """Evaluate fixed synthetic queries across many random datasets."""

    def setup(self, context: ExperimentContext) -> None:
        self.policy_counts = [int(v) for v in context.strategy_config.get("policy_counts", [1])]
        self.runs_per_setting = int(context.strategy_config.get("runs_per_setting", DEFAULT_RUNS_PER_SETTING))
        self.rows_per_table = int(
            context.strategy_config.get("rows_per_table", DEFAULT_SYNTHETIC_ROWS_PER_TABLE)
        )
        self.policy_threshold = float(
            context.strategy_config.get("policy_threshold", DEFAULT_SYNTHETIC_POLICY_THRESHOLD)
        )
        query_nums = context.strategy_config.get("query_nums")
        if query_nums is None:
            if "query_num" in context.strategy_config:
                self.query_nums = [int(context.strategy_config["query_num"])]
            else:
                self.query_nums = list(DEFAULT_SYNTHETIC_QUERY_NUMS)
        else:
            self.query_nums = [int(v) for v in query_nums]
        self.include_bedrock = bool(context.strategy_config.get("include_bedrock", True))
        self.include_openai = bool(context.strategy_config.get("include_openai", True))
        self.include_gpt_query_only = bool(context.strategy_config.get("include_gpt_query_only", self.include_openai))
        self.include_gpt_query_results = bool(
            context.strategy_config.get("include_gpt_query_results", self.include_openai)
        )
        self.include_opus_query_only = bool(context.strategy_config.get("include_opus_query_only", self.include_bedrock))
        self.include_opus_query_results = bool(
            context.strategy_config.get("include_opus_query_results", self.include_bedrock)
        )
        self.claude_model = str(context.strategy_config.get("claude_model", DEFAULT_CLAUDE_MODEL))
        self.gpt_model = str(context.strategy_config.get("gpt_model", DEFAULT_GPT_MODEL))
        self.queries = {query_num: synthetic_validation_query(query_num) for query_num in self.query_nums}
        self.local_duckdb = _ensure_smokedduck()

        dataset_count = int(
            context.strategy_config.get("dataset_count", DEFAULT_SYNTHETIC_DATASET_COUNT)
        )
        dataset_specs = context.strategy_config.get("dataset_specs")
        self.dataset_specs = dataset_specs or synthetic_dataset_specs(dataset_count)

        self.no_policy_conns: dict[str, Any] = {}
        self.dfc_conns: dict[str, Any] = {}
        self.dfc_rewriters: dict[str, SQLRewriter] = {}
        self.dataset_path_by_label: dict[str, str] = {}
        self.dataset_index_by_label: dict[str, int] = {}

        for spec in self.dataset_specs:
            label = str(spec["label"])
            dataset_index = int(spec["dataset_index"])
            db_path = str(spec["db_path"])
            pathlib.Path(db_path).parent.mkdir(parents=True, exist_ok=True)
            conn = self.local_duckdb.connect(db_path)
            populate_synthetic_dataset(conn, dataset_index=dataset_index, rows_per_table=self.rows_per_table)
            self.no_policy_conns[label] = conn
            self.dfc_conns[label] = conn
            self.dfc_rewriters[label] = SQLRewriter(conn=conn)
            self.dataset_path_by_label[label] = db_path
            self.dataset_index_by_label[label] = dataset_index

        base_cfg = HarnessConfig.from_env()
        self.llm_clients: dict[str, Any] = {}
        if self.include_openai and (self.include_gpt_query_only or self.include_gpt_query_results):
            openai_cfg = replace(base_cfg, provider="openai", openai_model=self.gpt_model)
            self.llm_clients["gpt"] = create_chat_model(openai_cfg)
        if self.include_bedrock and (self.include_opus_query_only or self.include_opus_query_results):
            bedrock_cfg = replace(base_cfg, provider="bedrock", bedrock_model_id=self.claude_model)
            self.llm_clients["opus"] = create_chat_model(bedrock_cfg)

        self.query_result_cache: dict[tuple[str, int], tuple[list[str], list[tuple[Any, ...]], float]] = {}
        self.truth_cache: dict[tuple[str, int, int], tuple[bool, float, float, int]] = {}
        self.settings: list[tuple[str, int, int, str]] = []
        for spec in self.dataset_specs:
            label = str(spec["label"])
            for query_num in self.query_nums:
                for policy_count in self.policy_counts:
                    self.settings.append((label, query_num, policy_count, APPROACH_DFC_1PHASE))
                    if self.include_opus_query_only:
                        self.settings.append((label, query_num, policy_count, APPROACH_OPUS_QUERY_ONLY))
                    if self.include_gpt_query_only:
                        self.settings.append((label, query_num, policy_count, APPROACH_GPT_QUERY_ONLY))
                    if self.include_opus_query_results:
                        self.settings.append((label, query_num, policy_count, APPROACH_OPUS_QUERY_RESULTS))
                    if self.include_gpt_query_results:
                        self.settings.append((label, query_num, policy_count, APPROACH_GPT_QUERY_RESULTS))

    def _setting_and_run(self, execution_number: int) -> tuple[str, int, int, str, int]:
        setting_index = (execution_number - 1) // self.runs_per_setting
        run_num = ((execution_number - 1) % self.runs_per_setting) + 1
        dataset_label, query_num, policy_count, approach = self.settings[setting_index]
        return dataset_label, query_num, policy_count, approach, run_num

    def _clear_and_register_policies(self, dataset_label: str, policy_count: int) -> list[Any]:
        rewriter = self.dfc_rewriters[dataset_label]
        existing = rewriter.get_dfc_policies()
        for old_policy in existing:
            rewriter.delete_policy(
                sources=old_policy.sources,
                constraint=old_policy.constraint,
                on_fail=old_policy.on_fail,
            )
        policies = build_synthetic_policies(policy_count, threshold=self.policy_threshold)
        for policy in policies:
            rewriter.register_policy(policy)
        return policies

    def _query_result_sample(self, dataset_label: str, query_num: int) -> tuple[list[str], list[tuple[Any, ...]], bool]:
        key = (dataset_label, query_num)
        if key in self.query_result_cache:
            columns, rows, _ = self.query_result_cache[key]
            return columns, rows, True
        start = time.perf_counter()
        cursor = self.no_policy_conns[dataset_label].execute(self.queries[query_num])
        all_rows = cursor.fetchall()
        query_time_ms = (time.perf_counter() - start) * 1000.0
        columns = [d[0] for d in (cursor.description or [])]
        rows = all_rows
        self.query_result_cache[key] = (columns, rows, query_time_ms)
        return columns, rows, False

    def _dfc_truth(self, dataset_label: str, query_num: int, policy_count: int) -> tuple[bool, float, float, int]:
        key = (dataset_label, query_num, policy_count)
        if key in self.truth_cache:
            return self.truth_cache[key]
        policies = self._clear_and_register_policies(dataset_label, policy_count)
        rewriter = self.dfc_rewriters[dataset_label]
        conn = self.dfc_conns[dataset_label]

        rewrite_start = time.perf_counter()
        transformed = rewriter.transform_query(self.queries[query_num], use_two_phase=False)
        rewrite_ms = (time.perf_counter() - rewrite_start) * 1000.0

        exec_start = time.perf_counter()
        cursor = conn.execute(transformed)
        rows = cursor.fetchall()
        exec_ms = (time.perf_counter() - exec_start) * 1000.0
        columns = [d[0] for d in (cursor.description or [])]
        lower_cols = [c.lower() for c in columns]
        violation = False
        if "valid" in lower_cols:
            valid_idx = lower_cols.index("valid")
            violation = any(not bool(row[valid_idx]) for row in rows)
        result = (violation, rewrite_ms, exec_ms, len(policies))
        self.truth_cache[key] = result
        return result

    def _run_llm(
        self,
        approach: str,
        policy_descriptions: list[str],
        sample_json: str | None,
        run_nonce: str,
    ) -> tuple[bool | None, float, int, str]:
        is_query_results = approach in {APPROACH_OPUS_QUERY_RESULTS, APPROACH_GPT_QUERY_RESULTS}
        if is_query_results:
            prompt = build_query_results_prompt(
                self.queries[self.current_query_num],
                policy_descriptions,
                sample_json or "[]",
                run_nonce=run_nonce,
                results_label="All result rows:",
            )
        else:
            prompt = build_query_only_prompt(self.queries[self.current_query_num], policy_descriptions, run_nonce=run_nonce)
        prompt_chars = len(prompt)
        model = self.llm_clients["opus"] if approach.startswith("opus") else self.llm_clients["gpt"]
        start = time.perf_counter()
        response = model.invoke(prompt)
        runtime_ms = (time.perf_counter() - start) * 1000.0
        raw = message_text(getattr(response, "content", response))
        predicted = parse_violation_prediction(raw)
        return predicted, runtime_ms, prompt_chars, raw

    def execute(self, context: ExperimentContext) -> ExperimentResult:
        dataset_label, query_num, policy_count, approach, run_num = self._setting_and_run(context.execution_number)
        self.current_query_num = query_num
        phase = "warmup" if context.is_warmup else f"run {run_num}"
        print(
            f"[Execution {context.execution_number}] synthetic_llm_validation_grid "
            f"dataset={dataset_label} query=Q{query_num:02d} policies={policy_count} "
            f"approach={approach} ({phase})"
        )

        truth_violation, rewrite_ms, dfc_exec_ms, effective_policy_count = self._dfc_truth(
            dataset_label=dataset_label,
            query_num=query_num,
            policy_count=policy_count,
        )
        policy_descriptions = [
            p.description or p.constraint for p in build_synthetic_policies(policy_count, threshold=self.policy_threshold)
        ]
        try:
            if approach == APPROACH_DFC_1PHASE:
                custom = {
                    "database_label": dataset_label,
                    "database_tpch_sf": "",
                    "database_path": self.dataset_path_by_label[dataset_label],
                    "dataset_index": self.dataset_index_by_label[dataset_label],
                    "query_num": query_num,
                    "policy_count": policy_count,
                    "effective_policy_count": effective_policy_count,
                    "approach": approach,
                    "provider": "none",
                    "model_name": "none",
                    "runtime_ms": dfc_exec_ms,
                    "dfc_1phase_rewrite_time_ms": rewrite_ms,
                    "dfc_1phase_exec_time_ms": dfc_exec_ms,
                    "ground_truth_violation": truth_violation,
                    "predicted_violation": truth_violation,
                    "correct_identification": True,
                    "llm_chars_sent": 0,
                    "query_results_cache_hit": True,
                    "query_results_rows": 0,
                    "prompt_nonce": "",
                    "raw_response": "",
                }
                return ExperimentResult(duration_ms=dfc_exec_ms, custom_metrics=custom)

            sample_json = None
            cache_hit = True
            sample_rows_count = 0
            if approach in {APPROACH_OPUS_QUERY_RESULTS, APPROACH_GPT_QUERY_RESULTS}:
                cols, rows, cache_hit = self._query_result_sample(dataset_label, query_num)
                sample_rows_count = len(rows)
                sample_json = format_result_rows(cols, rows)

            run_nonce = str(uuid.uuid4())
            predicted, runtime_ms, chars_sent, raw_response = self._run_llm(
                approach=approach,
                policy_descriptions=policy_descriptions,
                sample_json=sample_json,
                run_nonce=run_nonce,
            )
            custom = {
                "database_label": dataset_label,
                "database_tpch_sf": "",
                "database_path": self.dataset_path_by_label[dataset_label],
                "dataset_index": self.dataset_index_by_label[dataset_label],
                "query_num": query_num,
                "policy_count": policy_count,
                "effective_policy_count": effective_policy_count,
                "approach": approach,
                "provider": "bedrock" if approach.startswith("opus") else "openai",
                "model_name": self.claude_model if approach.startswith("opus") else self.gpt_model,
                "runtime_ms": runtime_ms,
                "dfc_1phase_rewrite_time_ms": 0.0,
                "dfc_1phase_exec_time_ms": 0.0,
                "ground_truth_violation": truth_violation,
                "predicted_violation": predicted,
                "correct_identification": predicted == truth_violation,
                "llm_chars_sent": chars_sent,
                "query_results_cache_hit": cache_hit,
                "query_results_rows": sample_rows_count,
                "prompt_nonce": run_nonce,
                "raw_response": raw_response[:2000],
            }
            return ExperimentResult(duration_ms=runtime_ms, custom_metrics=custom)
        except Exception as exc:
            return ExperimentResult(
                duration_ms=0.0,
                error=str(exc),
                custom_metrics={
                    "database_label": dataset_label,
                    "database_tpch_sf": "",
                    "database_path": self.dataset_path_by_label[dataset_label],
                    "dataset_index": self.dataset_index_by_label[dataset_label],
                    "query_num": self.query_num,
                    "policy_count": policy_count,
                    "effective_policy_count": effective_policy_count,
                    "approach": approach,
                    "provider": "bedrock" if approach.startswith("opus") else ("openai" if approach.startswith("gpt") else "none"),
                    "model_name": self.claude_model if approach.startswith("opus") else (self.gpt_model if approach.startswith("gpt") else "none"),
                    "ground_truth_violation": truth_violation,
                    "predicted_violation": "",
                    "correct_identification": "",
                    "llm_chars_sent": 0 if approach == APPROACH_DFC_1PHASE else -1,
                    "query_results_cache_hit": "",
                    "query_results_rows": 0,
                    "prompt_nonce": "",
                    "raw_response": "",
                },
            )

    def teardown(self, _context: ExperimentContext) -> None:
        return None

    def get_metrics(self) -> list[str]:
        return [
            "database_label",
            "database_tpch_sf",
            "database_path",
            "dataset_index",
            "query_num",
            "policy_count",
            "effective_policy_count",
            "approach",
            "provider",
            "model_name",
            "runtime_ms",
            "dfc_1phase_rewrite_time_ms",
            "dfc_1phase_exec_time_ms",
            "ground_truth_violation",
            "predicted_violation",
            "correct_identification",
            "llm_chars_sent",
            "query_results_cache_hit",
            "query_results_rows",
            "prompt_nonce",
            "raw_response",
        ]

    def get_setting_key(self, context: ExperimentContext) -> Any | None:
        dataset_label, query_num, policy_count, approach, _ = self._setting_and_run(context.execution_number)
        return (dataset_label, query_num, policy_count, approach)
