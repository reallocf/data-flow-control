# shared_sql_utils

Small, reusable SQL helpers used across the data-flow-control repo.

Currently includes:
- `combine_constraints_balanced` / `combine_constraints_balanced_expr`: combine many SQL predicate strings into a balanced AND expression to avoid deep recursion for large policy counts.

Usage:
```python
from shared_sql_utils import combine_constraints_balanced

sql = combine_constraints_balanced([
    "max(lineitem.l_quantity) >= 1",
    "max(lineitem.l_extendedprice) > 0",
])
```

Tests:
- `python3 -m pytest` (run inside `shared_sql_utils/`)
