"""DataFusion database integration helpers."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from datafusion import SessionContext
import pyarrow as pa

from vldb_experiments.multi_db.common import normalize_results
from vldb_experiments.multi_db.tpch_data import TPCH_TABLES, export_tpch_csvs, get_table_schema

if TYPE_CHECKING:
    import pathlib

    import duckdb


def _map_duckdb_type(duckdb_type: str) -> pa.DataType:
    type_upper = duckdb_type.upper()
    if type_upper.startswith("DECIMAL"):
        return pa.float64()
    if type_upper.startswith("DOUBLE"):
        return pa.float64()
    if type_upper.startswith("DATE"):
        return pa.date32()
    if type_upper.startswith(("BIGINT", "HUGEINT", "INTEGER", "INT", "SMALLINT")):
        return pa.int64()
    return pa.string()


class DataFusionClient:
    """Helper for loading TPC-H CSVs into DataFusion."""

    def __init__(self, data_dir: pathlib.Path) -> None:
        self.data_dir = data_dir
        self.ctx: SessionContext | None = None

    def start(self) -> None:
        self.data_dir.mkdir(parents=True, exist_ok=True)

    def wait_ready(self, timeout_s: int = 60) -> None:
        _ = timeout_s

    def connect(self) -> SessionContext:
        self.ctx = SessionContext()
        return self.ctx

    def ensure_tpch_data(self, duckdb_conn: duckdb.DuckDBPyConnection) -> None:
        if self.ctx is None:
            raise RuntimeError("DataFusion context not initialized.")

        export_tpch_csvs(duckdb_conn, self.data_dir)

        for table in TPCH_TABLES:
            schema = get_table_schema(duckdb_conn, table)
            arrow_fields = [pa.field(name, _map_duckdb_type(col_type)) for name, col_type in schema]
            arrow_schema = pa.schema(arrow_fields)
            self.ctx.register_csv(
                table,
                str(self.data_dir / f"{table}.csv"),
                schema=arrow_schema,
                has_header=True,
            )

    def fetchall(self, query: str) -> list[tuple[Any, ...]]:
        if self.ctx is None:
            raise RuntimeError("DataFusion context not initialized.")
        dataframe = self.ctx.sql(query)
        batches = dataframe.collect()
        results: list[tuple[Any, ...]] = []
        for batch in batches:
            columns = batch.schema.names
            for row in batch.to_pylist():
                results.append(tuple(row[col] for col in columns))
        return normalize_results(results)

    def close(self) -> None:
        self.ctx = None
