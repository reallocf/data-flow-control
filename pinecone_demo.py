import os, json, time
from sentence_transformers import SentenceTransformer
from pinecone.grpc import PineconeGRPC as Pinecone
from pinecone import ServerlessSpec

INDEX_NAME = "legal-tax-dense"
NAMESPACE = "tax"
MODEL_NAME = "sentence-transformers/all-MiniLM-L6-v2"  # 384 dims

def main():
    api_key = os.environ["PINECONE_API_KEY"]
    pc = Pinecone(api_key=api_key)

    if not pc.has_index(INDEX_NAME):
        pc.create_index(
            name=INDEX_NAME,
            vector_type="dense",
            dimension=384,
            metric="cosine",
            spec=ServerlessSpec(cloud="aws", region="us-east-1"),
            deletion_protection="disabled",
        )

    index = pc.Index(INDEX_NAME)
    model = SentenceTransformer(MODEL_NAME)

    batch = []
    with open("chunks.jsonl", "r", encoding="utf-8") as f:
        for i, line in enumerate(f):
            if i >= 200:
                break
            r = json.loads(line)
            vec = model.encode(r["text"]).tolist()
            batch.append({
                "id": r["id"],
                "values": vec,
                "metadata": {"source": r["source"], "text": r["text"][:500]}
            })

    index.upsert(vectors=batch, namespace=NAMESPACE)

    q = "standard deduction"
    qvec = model.encode(q).tolist()
    res = index.query(vector=qvec, top_k=5, include_metadata=True, namespace=NAMESPACE)

    print("Top 5 Pinecone matches:")
    for m in res["matches"]:
        md = m.get("metadata", {}) or {}
        print(m["score"], md.get("source"), md.get("text", "")[:120])

if __name__ == "__main__":
    main()
