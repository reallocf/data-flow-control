"""Query definitions for testing different relational operators."""

from typing import Dict, List


def get_query_definitions() -> Dict[str, str]:
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

        "ORDER_BY": """
            SELECT * FROM test_data ORDER BY value DESC
        """,
    }

    # Normalize whitespace
    return {k: " ".join(v.split()) for k, v in queries.items()}


def get_query_order() -> List[str]:
    """Get the order in which queries should be executed.
    
    Returns:
        List of operator names in execution order
    """
    return ["SELECT", "WHERE", "JOIN", "GROUP_BY", "ORDER_BY"]
