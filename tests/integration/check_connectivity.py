#!/usr/bin/env python3
"""Quick TCP connectivity check for valkey-rs."""
import socket
import sys

host = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1"
port = int(sys.argv[2]) if len(sys.argv) > 2 else 6379

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.settimeout(3)
try:
    s.connect((host, port))
    # Send PING in RESP protocol
    s.sendall(b"*1\r\n$4\r\nPING\r\n")
    data = s.recv(1024)
    if b"PONG" in data:
        sys.exit(0)
    else:
        print(f"Unexpected response: {data!r}", file=sys.stderr)
        sys.exit(1)
except Exception as e:
    print(f"Connection failed: {e}", file=sys.stderr)
    sys.exit(1)
finally:
    s.close()
