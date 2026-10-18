"""Infrastructure-only smoke probe; never invokes model inference."""

import json
from pathlib import Path
import socket
import subprocess

checks = {}
checks["host_home_absent"] = not Path("/Users/charliesummers").exists()
checks["host_repository_absent"] = not Path("/work/AGENTS.md").exists()
checks["docker_socket_absent"] = not Path("/var/run/docker.sock").exists()
checks["no_codex_history"] = not Path("/home/node/.codex").exists()
try:
    with socket.create_connection(("1.1.1.1", 443), timeout=2):
        checks["direct_egress_blocked"] = False
except OSError:
    checks["direct_egress_blocked"] = True
with socket.create_connection(("inference-proxy", 8080), timeout=5) as connection:
    connection.sendall(b"CONNECT example.com:443 HTTP/1.1\r\nHost: example.com:443\r\n\r\n")
    checks["arbitrary_proxy_destination_blocked"] = (
        connection.recv(4096).split(b"\r\n")[0] == b"HTTP/1.1 403 Forbidden"
    )
with socket.create_connection(("inference-proxy", 8080), timeout=5) as connection:
    connection.sendall(b"CONNECT chatgpt.com:443 HTTP/1.1\r\nHost: chatgpt.com:443\r\n\r\n")
    checks["inference_host_reachable"] = (
        connection.recv(4096).split(b"\r\n")[0] == b"HTTP/1.1 200 Connection established"
    )
checks["cli_version"] = subprocess.run(
    ["codex", "--version"], capture_output=True, text=True, check=True
).stdout.strip()
print(json.dumps(checks))
if not all(value for value in checks.values()):
    raise SystemExit(1)
