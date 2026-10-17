# DFC pushdown probe for GProM and ProvSQL

This probe mirrors the running example from the DFC paper:

```text
Q = γ_sum(y)(T ⋈ S)
B = σ γ_Σ, instantiated as max(S.y) > 3
```

The policy used here is intentionally tiny:

> Keep an output group only if the maximum `S.y` among contributing `S` tuples is
> greater than `3`.

This is equivalent to a PGN-style source policy:

```text
SOURCE S
CONSTRAINT MAX(S.y) > 3
ON FAIL REMOVE
```

Files:

- `setup_sqlite.sql`: dataset for GProM/SQLite.
- `setup_postgres.sql`: dataset for ProvSQL/PostgreSQL.
- `queries.sql`: base, post-process-shaped, partial-push-shaped, and full-push-shaped queries.
- `provsql_probe.sql`: ProvSQL setup, query, and `EXPLAIN` probe.
- `run_probe.sh`: runs the GProM and ProvSQL probes end-to-end.

Run:

```sh
./third_party/provenance_engines/dfc_pushdown_probe/run_probe.sh
```

The main question is whether GProM or ProvSQL automatically produces either
pushdown shape for the policy-like computation. The short answer from the smoke
probe is:

- GProM materializes provenance-style annotations for `PROVENANCE OF (...)`.
  Its generated SQL computes the base aggregate, then joins back to the
  annotated join result to recover contributing source attributes. This matches
  the paper's logical/Post-process shape, not Partial-Push or Full-Push.
- ProvSQL produces provenance tokens/circuits through PostgreSQL. When we
  manually write the Full-Push SQL (`HAVING MAX(y) > 3`), PostgreSQL/ProvSQL
  plans a single grouped join with both aggregates. However, ProvSQL does not
  accept an external DFC policy and rewrite a provenance-token query into that
  Full-Push form for us.
