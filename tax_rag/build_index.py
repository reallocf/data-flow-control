import argparse
import json
import duckdb
from sentence_transformers import SentenceTransformer

MODEL_NAME = "sentence-transformers/all-MiniLM-L6-v2"

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--chunks", default="chunks.jsonl")
    ap.add_argument("--db", default="rag.duckdb")
    ap.add_argument("--limit", type=int, default=0)
    args = ap.parse_args()

    model = SentenceTransformer(MODEL_NAME)

    con = duckdb.connect(args.db)
    con.execute("INSTALL vss;")
    con.execute("LOAD vss;")
    con.execute("SET hnsw_enable_experimental_persistence = true;")
    con.execute("DROP TABLE IF EXISTS chunks;")
    con.execute("CREATE TABLE chunks(id VARCHAR, source VARCHAR, text VARCHAR, vec FLOAT[384]);")

    rows = []
    with open(args.chunks, "r", encoding="utf-8") as f:
        for i, line in enumerate(f):
            if args.limit and i >= args.limit:
                break
            r = json.loads(line)
            vec = model.encode(r["text"]).tolist()
            rows.append((r["id"], r["source"], r["text"], vec))

    con.executemany("INSERT INTO chunks VALUES (?, ?, ?, ?);", rows)
    con.execute("CREATE INDEX chunks_hnsw_cosine ON chunks USING HNSW (vec) WITH (metric = 'cosine');")
    con.close()

if __name__ == "__main__":
    main()