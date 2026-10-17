# Data Flow Control: SIGMOD 2027 artifact

This branch contains the implementations, experiment code, and recorded results
for the anonymous submission. Install the bundled local packages rather than a
published release to reproduce this artifact.

- `passant/`: Rust implementation and Python bindings used by CreditBot.
- `sql_rewriter/`: Python rewriter used by the performance experiments.
- `experiment_harness/` and `shared_sql_utils/`: shared support packages.
- `benchmarks/tpch/queries/`: TPC-H SQL inputs.
- `sigmod_2027_big_paper_experiments/`: complete experiment project, including
  historical experiments. Its Python import name remains `vldb_experiments`.
- `final_results/`: curated data for Figures 1 and 6-12. The JSON manifest maps
  each group to its source files, row counts, and anonymized-file checksums.

See the experiment project's README for setup and runners. CreditBot uses
`run_regulation_z_v3_benchmark.py`; the tied and no-ties runs differ by `--no-ties`.
Cloud model inference requires your own credentials and model/profile IDs.
Anonymized application-profile IDs and the SQL Server hostname are placeholders.
Use the CLI model-ID arguments and SQLSERVER_HOST configuration for your account.
