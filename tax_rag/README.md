requirements

* windows/mac/linux
* python 3.10+
* uv

setup
cd tax\_rag
uv sync



download sources
powershell (run inside tax\_rag):
Invoke-WebRequest -Uri "https://www.govinfo.gov/bulkdata/ECFR/title-26/ECFR-title26.xml" -OutFile "ECFR-title26.xml"
Invoke-WebRequest -Uri "https://www.govinfo.gov/content/pkg/USCODE-2023-title26/html/USCODE-2023-title26.htm" -OutFile "USCODE-2023-title26.htm"



build chunks
uv run python make\_chunks.py



build duckdb index
uv run python build\_index.py --chunks chunks.jsonl --db rag.duckdb

run mcp server (tool)
uv run mcp dev mcp\_server.py



tool name
tax\_law\_search(query: str, top\_k: int = 5) -> list\[dict]

