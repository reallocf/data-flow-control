# Boolean Grouping: Offline SQL Check

Prior Rust Passant tests identified a discrepancy between intended Boolean grouping and an ungrouped SQL form for two policy conditions containing OR.

Intended SQL:

```sql
(a = 1 OR b = 1) AND (c = 1 OR d = 1)
```

Ungrouped SQL:

```sql
a = 1 OR b = 1 AND c = 1 OR d = 1
```

When a = 0, b = 0, c = 0, and d = 1, the intended condition evaluates to FALSE, while the ungrouped form evaluates to TRUE.

The Stage 34 local audit found 16 discrepancies in 64 tests with an ungrouped DSL form, 16 in 64 with Policy objects, and zero in 64 with explicit full-expression parentheses.

These are historical offline results. The SQL example illustrates Boolean precedence, not a complete Rust Passant regression test. A source-level correction and regression check remain pending.
