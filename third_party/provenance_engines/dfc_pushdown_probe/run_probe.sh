#!/usr/bin/env sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
REPO_ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/../../.." && pwd)

sqlite3 "$SCRIPT_DIR/dfc_probe.sqlite" < "$SCRIPT_DIR/setup_sqlite.sql"

echo "== GProM base query =="
"$REPO_ROOT/third_party/provenance_engines/scripts/gprom" \
  -backend sqlite \
  -db /workspace/third_party/provenance_engines/dfc_pushdown_probe/dfc_probe.sqlite \
  -sql 'SELECT t_id, SUM(y) AS sum_y FROM dfc_t JOIN dfc_s ON tid = t_id GROUP BY t_id ORDER BY t_id;'

echo "== GProM provenance query =="
"$REPO_ROOT/third_party/provenance_engines/scripts/gprom" \
  -backend sqlite \
  -db /workspace/third_party/provenance_engines/dfc_pushdown_probe/dfc_probe.sqlite \
  -sql 'PROVENANCE OF (SELECT t_id, SUM(y) AS sum_y FROM dfc_t JOIN dfc_s ON tid = t_id GROUP BY t_id ORDER BY t_id);'

echo "== GProM generated SQL =="
"$REPO_ROOT/third_party/provenance_engines/scripts/gprom" \
  -backend sqlite \
  -db /workspace/third_party/provenance_engines/dfc_pushdown_probe/dfc_probe.sqlite \
  -Pexecutor sql \
  -sql 'PROVENANCE OF (SELECT t_id, SUM(y) AS sum_y FROM dfc_t JOIN dfc_s ON tid = t_id GROUP BY t_id ORDER BY t_id);'

echo "== GProM backend plan for generated provenance SQL =="
"$REPO_ROOT/third_party/provenance_engines/scripts/gprom" \
  -backend sqlite \
  -db /workspace/third_party/provenance_engines/dfc_pushdown_probe/dfc_probe.sqlite \
  -Pexecutor explain \
  -sql 'PROVENANCE OF (SELECT t_id, SUM(y) AS sum_y FROM dfc_t JOIN dfc_s ON tid = t_id GROUP BY t_id ORDER BY t_id);'

echo "== ProvSQL probe =="
"$REPO_ROOT/third_party/provenance_engines/scripts/provsql-start"
python3 - "$SCRIPT_DIR" <<'PY' | docker exec -i data-flow-provsql psql -v ON_ERROR_STOP=1 -U test -d tutorial
from pathlib import Path
import sys

script_dir = Path(sys.argv[1])
setup = (script_dir / "setup_postgres.sql").read_text()
probe = (script_dir / "provsql_probe.sql").read_text()
probe = "\n".join(line for line in probe.splitlines() if not line.startswith("\\i "))
probe = probe.replace(
    "SET search_path TO public, provsql;\n",
    "SET search_path TO public, provsql;\n\n" + setup + "\n",
)
print(probe)
PY
