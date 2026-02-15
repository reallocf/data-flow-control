"""Query definitions for testing different relational operators."""


def get_query_definitions() -> dict[str, str]:
    """Get query definitions for each core relational operator.

    Returns:
        Dictionary mapping operator names to SQL query strings
    """
    queries = {
        "SELECT": """
            SELECT * FROM test_data
        """,

        "WHERE": """
            SELECT * FROM test_data WHERE value > 50
        """,

        "JOIN": """
            SELECT test_data.id, other.value
            FROM test_data
            JOIN test_data other ON test_data.id = other.id
        """,

        "GROUP_BY": """
            SELECT category, COUNT(*), SUM(amount)
            FROM test_data
            GROUP BY category
        """,

        # Real query is generated dynamically in MicrobenchmarkStrategy based on
        # variation_join_count. This placeholder keeps query type registration simple.
        "JOIN_GROUP_BY": """
            SELECT test_data.category, COUNT(*), SUM(test_data.amount + j1.amount)
            FROM test_data
            JOIN join_data_1 j1 ON test_data.id = j1.id
            GROUP BY test_data.category
        """,

        "ORDER_BY": """
            SELECT * FROM test_data ORDER BY value DESC
        """,
    }

    # Normalize whitespace
    return {k: " ".join(v.split()) for k, v in queries.items()}


def get_query_order() -> list[str]:
    """Get the order in which queries should be executed.

    Returns:
        List of operator names in execution order
    """
    return ["SELECT", "WHERE", "JOIN", "GROUP_BY", "JOIN_GROUP_BY", "ORDER_BY"]
