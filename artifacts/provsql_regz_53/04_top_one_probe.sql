-- Rollback-only probe: ProvSQL may expose counterfactual top-one candidates.
\set ON_ERROR_STOP on
BEGIN;
CREATE SCHEMA regz_top_one_probe;
SET LOCAL search_path TO regz_top_one_probe, public, provsql;
CREATE TABLE t(id integer PRIMARY KEY);
CREATE TABLE s(id integer PRIMARY KEY, amount integer, good boolean);
INSERT INTO t VALUES (1);
INSERT INTO s VALUES (1, 100, false), (2, 100, true);
SELECT add_provenance('t');
SELECT add_provenance('s');
SELECT t.id, s.id AS selected_id, s.good,
       sr_boolean(provenance()) AS exists_in_actual_database,
       provenance() AS output_token
FROM t JOIN s ON s.id = (SELECT id FROM s ORDER BY amount DESC LIMIT 1)
WHERE t.id = 1;
ROLLBACK;
