"""CONNECT-only egress: inference/auth hosts, no arbitrary browsing or host access."""

import select
import socket
import socketserver

ALLOWED = {"chatgpt.com", "api.openai.com", "auth.openai.com"}


class Proxy(socketserver.StreamRequestHandler):
    def handle(self):
        line = self.rfile.readline(8192).decode("ascii", errors="replace").strip()
        parts = line.split()
        if len(parts) != 3:
            return
        method, authority, _ = parts
        while self.rfile.readline(8192).strip():
            pass
        host, _, port = authority.rpartition(":")
        if method != "CONNECT" or host not in ALLOWED or port != "443":
            self.wfile.write(b"HTTP/1.1 403 Forbidden\r\n\r\n")
            return
        try:
            remote = socket.create_connection((host, 443), timeout=30)
        except OSError:
            self.wfile.write(b"HTTP/1.1 502 Bad Gateway\r\n\r\n")
            return
        with remote:
            self.wfile.write(b"HTTP/1.1 200 Connection established\r\n\r\n")
            self.wfile.flush()
            sockets = [self.connection, remote]
            while True:
                ready, _, _ = select.select(sockets, [], [], 120)
                if not ready:
                    return
                for source in ready:
                    data = source.recv(65536)
                    if not data:
                        return
                    (remote if source is self.connection else self.connection).sendall(data)


class Server(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True


Server(("0.0.0.0", 8080), Proxy).serve_forever()
