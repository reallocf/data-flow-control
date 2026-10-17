DROP TABLE IF EXISTS dfc_t;
DROP TABLE IF EXISTS dfc_s;

CREATE TABLE dfc_t(
  t_id INTEGER PRIMARY KEY,
  x INTEGER NOT NULL
);

CREATE TABLE dfc_s(
  s_id INTEGER PRIMARY KEY,
  tid INTEGER NOT NULL,
  y INTEGER NOT NULL
);

INSERT INTO dfc_t(t_id, x) VALUES
  (1, 10),
  (2, 20),
  (3, 30);

INSERT INTO dfc_s(s_id, tid, y) VALUES
  (101, 1, 1),
  (102, 1, 2),
  (103, 2, 4),
  (104, 2, 5),
  (105, 3, 3);
