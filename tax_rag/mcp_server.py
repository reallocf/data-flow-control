import os
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import AsyncIterator
import duckdb
from sentence_transformers import SentenceTransformer
from mcp.server.fastmcp import FastMCP, Context

MODEL_NAME = "sentence-transformers/all-MiniLM-L6-v2"

@dataclass
class AppContext:
    con: duckdb.DuckDBPyConnection
    model: SentenceTransformer

@asynccontextmanager
async def lifespan(server: FastMCP) -> AsyncIterator[AppContext]:
    db_path = os.environ.get("TAX_RAG_DB", "rag.duckdb")
    con = duckdb.connect(db_path, read_only=True)
    con.execute("INSTALL vss;")
    con.execute("LOAD vss;")
    con.execute("SET hnsw_enable_experimental_persistence = true;")
    model = SentenceTransformer(MODEL_NAME)
    try:
        yield AppContext(con=con, model=model)
    finally:
        con.close()

mcp = FastMCP("tax-rag", lifespan=lifespan)

@mcp.tool()
def tax_law_search(ctx: Context, query: str, top_k: int = 5):
    state = ctx.request_context.lifespan_context
    qvec = state.model.encode(query).tolist()
    sql = """
    SELECT id, source, text, array_cosine_distance(vec, ?::FLOAT[384]) AS dist
    FROM chunks
    ORDER BY dist
    LIMIT ?;
    """
    rows = state.con.execute(sql, [qvec, top_k]).fetchall()
    out = []
    for r in rows:
        out.append(
            {
                "id": r[0],
                "source": r[1],
                "distance": float(r[3]),
                "text": r[2][:1200],
            }
        )
    return out

if __name__ == "__main__":
    mcp.run(transport="stdio")