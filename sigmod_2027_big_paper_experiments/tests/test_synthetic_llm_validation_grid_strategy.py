"""Tests for synthetic_llm_validation_grid strategy helpers."""

from vldb_experiments.strategies.synthetic_llm_validation_grid_strategy import (
    DEFAULT_SYNTHETIC_QUERY_NUMS,
    build_synthetic_policies,
    synthetic_dataset_specs,
    synthetic_validation_queries,
    synthetic_validation_query,
)


def test_synthetic_llm_validation_grid_default_dataset_specs_are_stable() -> None:
    specs = synthetic_dataset_specs(3, "synthetic_llm_validation_grid")

    assert specs == [
        {
            "label": "dataset01",
            "dataset_index": 0,
            "db_path": "./results/synthetic_llm_validation_grid_dataset01.db",
        },
        {
            "label": "dataset02",
            "dataset_index": 1,
            "db_path": "./results/synthetic_llm_validation_grid_dataset02.db",
        },
        {
            "label": "dataset03",
            "dataset_index": 2,
            "db_path": "./results/synthetic_llm_validation_grid_dataset03.db",
        },
    ]


def test_synthetic_llm_validation_grid_default_query_nums_are_stable() -> None:
    assert DEFAULT_SYNTHETIC_QUERY_NUMS == [1, 2, 3, 4, 5]


def test_synthetic_llm_validation_grid_query_catalog_uses_three_tables_and_aggregation() -> None:
    assert synthetic_validation_queries() == {
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


def test_synthetic_llm_validation_grid_query_lookup_works_for_all_queries() -> None:
    queries = synthetic_validation_queries()
    for query_num, query_sql in queries.items():
        assert synthetic_validation_query(query_num) == query_sql


def test_synthetic_llm_validation_grid_policy_uses_three_sources_and_threshold_25() -> None:
    policies = build_synthetic_policies(1, threshold=25)

    assert len(policies) == 1
    policy = policies[0]
    assert policy.sources == ["customers", "sales_orders", "products"]
    assert policy.constraint == "avg(sales_orders.quantity) <= 25"
    assert policy.description == "Average order quantity should remain at or below 25."
