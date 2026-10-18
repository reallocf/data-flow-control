-- Rollback-only reproduction of the native UPDATE tracking hurdle.
-- Run in a scratch database with ProvSQL installed; no production tables.
\set ON_ERROR_STOP on
BEGIN;
CREATE SCHEMA regz_native_update_probe;
SET LOCAL search_path TO regz_native_update_probe, public, provsql;
CREATE TABLE t(id integer PRIMARY KEY, amount integer);
CREATE TABLE s(id integer PRIMARY KEY, amount integer);
INSERT INTO t VALUES (1, 0);
INSERT INTO s VALUES (1, 100);
SELECT add_provenance('t');
SELECT add_provenance('s');
SET LOCAL provsql.update_provenance = on;
UPDATE t SET amount = s.amount FROM s WHERE t.id = s.id;
ROLLBACK;
-- On the tested build the UPDATE raises a duplicate t_pkey error because the
-- update_statement_trigger attempts to reinsert OLD_TABLE. Closing the psql
-- connection rolls back the transaction after ON_ERROR_STOP exits.
