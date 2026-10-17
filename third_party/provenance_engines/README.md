# Provenance engines for experiments

This folder contains local clones plus Docker-based run helpers for:

- GProM: <https://github.com/IITDBGroup/gprom>
- ProvSQL: <https://github.com/PierreSenellart/provsql>

The helpers use the projects' published Docker images so the tools are runnable
without installing PostgreSQL headers, Oracle/SQLite/DuckDB client libraries, or
other native build dependencies on macOS.

## GProM

Run the bundled GProM CLI:

```sh
./third_party/provenance_engines/scripts/gprom -help
```

Smoke test against the image's bundled DuckDB sample database:

```sh
./third_party/provenance_engines/scripts/gprom -sql 'SELECT * FROM r;'
```

Run a provenance query:

```sh
./third_party/provenance_engines/scripts/gprom -sql 'PROVENANCE OF (SELECT A FROM r);'
```

The wrapper mounts the repository at `/workspace` inside the container. To use a
query file from this repository, pass its container path, for example:

```sh
./third_party/provenance_engines/scripts/gprom -queryFile /workspace/path/to/query.sql
```

## ProvSQL

Start the ProvSQL demo PostgreSQL server and Studio UI:

```sh
./third_party/provenance_engines/scripts/provsql-start
```

Connect with `psql` to the seeded tutorial database:

```sh
./third_party/provenance_engines/scripts/provsql-psql tutorial
```

Check the extension:

```sh
./third_party/provenance_engines/scripts/provsql-psql tutorial -c "SELECT extname, extversion FROM pg_extension WHERE extname = 'provsql';"
```

Studio will be available at <http://localhost:8001>. PostgreSQL is published on
host port `5433`, with user `test`; for example:

```sh
psql -h localhost -p 5433 -U test tutorial
```

Stop the ProvSQL container:

```sh
./third_party/provenance_engines/scripts/provsql-stop
```

Note: the published ProvSQL image is currently amd64-only, so the wrapper runs
it with Docker's `linux/amd64` platform setting on Apple Silicon.
