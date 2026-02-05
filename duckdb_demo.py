import json
import duckdb
from sentence_transformers import SentenceTransformer

DB = "rag.duckdb"
MODEL_NAME = "sentence-transformers/all-MiniLM-L6-v2"

def main():
    con = duckdb.connect(DB)
    con.execute("INSTALL vss;")
    con.execute("LOAD vss;")
    con.execute("SET hnsw_enable_experimental_persistence = true;")

    con.execute("DROP TABLE IF EXISTS chunks;")
    con.execute("CREATE TABLE chunks(id VARCHAR, source VARCHAR, text VARCHAR, vec FLOAT[384]);")

    model = SentenceTransformer(MODEL_NAME)

    rows = []
    with open("chunks.jsonl", "r", encoding="utf-8") as f:
        for i, line in enumerate(f):
            if i >= 200:
                break
            r = json.loads(line)
            vec = model.encode(r["text"]).tolist()
            rows.append((r["id"], r["source"], r["text"], vec))

    con.executemany("INSERT INTO chunks VALUES (?, ?, ?, ?);", rows)

    con.execute("CREATE INDEX chunks_hnsw_cosine ON chunks USING HNSW (vec) WITH (metric = 'cosine');")

    q = "standard deduction"
    qvec = model.encode(q).tolist()

    sql = """
    SELECT id, source, substr(text, 1, 120) AS snippet
    FROM chunks
    ORDER BY array_cosine_distance(vec, ?::FLOAT[384])
    LIMIT 5;
    """
    out = con.execute(sql, [qvec]).fetchall()

    print("Top 5 DuckDB matches:")
    for r in out:
        print(r)

    con.close()

if __name__ == "__main__":
    main()
