DATABASE SCHEMA
These are input relations, not views or query scaffolding you must generate.
The SQLite export declarations below describe available columns and keys. For
DuckDB/PostgreSQL, use DATE for date fields including snapshot_date, credited_date,
cycle_end and window_start, native BOOLEAN for evidence Boolean fields listed in
the evidence contract, and exact numeric dollar/APR fields. SQLite stores dates
as ISO YYYY-MM-DD strings and Booleans as integer 0/1. Evidence counts and cents
are BIGINT. All context fields are non-null. Do not infer other NOT NULL guarantees
absent from the declarations. The controlled allocation workload uses exactly
representable whole-dollar values. The original two-argument LEAST ignores a single
NULL argument; a dialect conversion must preserve that behavior.

```sql
CREATE TABLE "payment_allocations" ("allocation_id" TEXT NOT NULL, "account_id" TEXT, "transaction_id" TEXT, "statement_id" TEXT, "payment_date" TEXT, "total_payment" NUMERIC, "minimum_payment_portion" NUMERIC, "excess_payment_amount" NUMERIC, "purchase_allocation" NUMERIC, "cash_advance_allocation" NUMERIC, "balance_transfer_allocation" NUMERIC, "promo_allocation" NUMERIC, PRIMARY KEY ("allocation_id"));
```

```sql
CREATE TABLE "transactions" ("transaction_id" TEXT NOT NULL, "account_id" TEXT, "statement_id" TEXT, "transaction_date" TEXT, "post_date" TEXT, "transaction_type" TEXT, "balance_category" TEXT, "amount" NUMERIC, "merchant_name" TEXT, "is_disputed" INTEGER, PRIMARY KEY ("transaction_id"));
```

```sql
CREATE TABLE "daily_balances" ("account_id" TEXT NOT NULL, "statement_id" TEXT, "balance_date" TEXT NOT NULL, "balance_category" TEXT NOT NULL, "daily_balance" NUMERIC, "apr_applied" NUMERIC, "daily_periodic_rate" NUMERIC, PRIMARY KEY ("account_id", "balance_date", "balance_category"));
```

```sql
CREATE TABLE "balance_components" ("component_id" TEXT NOT NULL, "account_id" TEXT, "statement_id" TEXT, "outstanding_amount" NUMERIC, "effective_apr" NUMERIC, PRIMARY KEY ("component_id"));
```

```sql
CREATE TABLE "statements" ("statement_id" TEXT NOT NULL, "account_id" TEXT, "cycle_start" TEXT, "cycle_end" TEXT, "minimum_payment" NUMERIC, PRIMARY KEY ("statement_id"));
```

```sql
CREATE TABLE "allocation_snapshot_context" ("account_id" TEXT NOT NULL, "statement_id" TEXT NOT NULL, "snapshot_date" TEXT, "highest_effective_apr" NUMERIC, PRIMARY KEY ("account_id", "statement_id"));
```

```sql
CREATE TABLE "regz_context" ("batch_id" TEXT NOT NULL, "kind" TEXT NOT NULL, "method" TEXT NOT NULL, "request_basis" TEXT NOT NULL, "highest_violations" INTEGER NOT NULL, "remaining_violations" INTEGER NOT NULL, "ordinary_violations" INTEGER NOT NULL, "deferred_violations" INTEGER NOT NULL, "snapshot_violations" INTEGER NOT NULL, "early_expiry_violations" INTEGER NOT NULL, "active_programs" INTEGER NOT NULL, "window_programs" INTEGER NOT NULL, "unauthorized_changes" INTEGER NOT NULL, "protected_reduction" INTEGER NOT NULL, "unavoidable_protected_reduction" INTEGER NOT NULL, "other_capacity_remaining" INTEGER NOT NULL, "effective_apr_mismatches" INTEGER NOT NULL, "window_start_mismatches" INTEGER NOT NULL, "request_mismatches" INTEGER NOT NULL, "unsecured_request_targets" INTEGER NOT NULL, "request_feasibility_errors" INTEGER NOT NULL, "superseding_requests" INTEGER NOT NULL, "instruction_count" INTEGER NOT NULL, "missing_credit_cents" INTEGER NOT NULL, "notice_count" INTEGER NOT NULL, "classification_mismatches" INTEGER NOT NULL, "interim_payment_omissions" INTEGER NOT NULL, "grace_classifications" INTEGER NOT NULL, "zero_offer_classifications" INTEGER NOT NULL, "issuer_elected" INTEGER NOT NULL, "request_present" INTEGER NOT NULL, "affirmative" INTEGER NOT NULL, "contact_channel" INTEGER NOT NULL, "form_completed" INTEGER NOT NULL, "prior_default_allocation" INTEGER NOT NULL, "required_condition" INTEGER NOT NULL, "snapshot_date" TEXT NOT NULL, "cycle_end" TEXT NOT NULL, "credited_date" TEXT NOT NULL, "expected_snapshot_date" TEXT NOT NULL, PRIMARY KEY ("batch_id"));
```

```sql
CREATE TABLE "regz_proposals" ("batch_id" TEXT NOT NULL, "payment_id" TEXT NOT NULL, "snapshot_date" TEXT NOT NULL, "decision_date" TEXT NOT NULL, "method" TEXT NOT NULL, "request_id" TEXT, "replaces_batch_id" TEXT, PRIMARY KEY ("batch_id"));
```

```sql
CREATE TABLE "regz_proposal_lines" ("batch_id" TEXT NOT NULL, "balance_id" TEXT NOT NULL, "minimum_cents" INTEGER NOT NULL, "excess_cents" INTEGER NOT NULL, "effective_apr" NUMERIC NOT NULL, "window_start" TEXT, PRIMARY KEY ("batch_id", "balance_id"));
```

```sql
CREATE TABLE "regz_payments" ("payment_id" TEXT NOT NULL, "account_id" TEXT NOT NULL, "credited_date" TEXT NOT NULL, "cycle_end" TEXT NOT NULL, "total_cents" INTEGER NOT NULL, "minimum_cents" INTEGER NOT NULL, PRIMARY KEY ("payment_id"));
```

