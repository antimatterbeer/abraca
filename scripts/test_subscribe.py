#!/usr/bin/env python3
"""连接 8080，发送订阅请求，10 秒后发送退订请求并关闭连接。"""

import json
import socket
import sys
import threading
import time

HOST = "127.0.0.1"
PORT = 8080
TOPICS = ["btcusdt@depth5@500ms", "btcusdt@kline_1m"]


def recv_loop(sock: socket.socket) -> None:
    """在后台接收并打印服务端消息。"""
    try:
        while True:
            chunk = sock.recv(4096)
            if not chunk:
                break
            print("[recv]", chunk.decode("utf-8", errors="replace").strip())
    except (ConnectionResetError, BrokenPipeError, OSError):
        pass


def main() -> None:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.connect((HOST, PORT))
        print(f"Connected to {HOST}:{PORT}")
    except OSError as e:
        print(f"Connect failed: {e}", file=sys.stderr)
        sys.exit(1)

    t = threading.Thread(target=recv_loop, args=(sock,), daemon=True)
    t.start()

    # 订阅
    sub = {
        "exchange": "BinanceFutures",
        "req": "subscribe",
        "data": TOPICS,
        "id": 1,
    }
    msg = json.dumps(sub) + "\n"
    sock.sendall(msg.encode())
    print("[send]", msg.strip())

    time.sleep(10)

    # 退订
    unsub = {
        "exchange": "BinanceFutures",
        "req": "unsubscribe",
        "data": TOPICS,
        "id": 2,
    }
    msg = json.dumps(unsub) + "\n"
    sock.sendall(msg.encode())
    print("[send]", msg.strip())

    time.sleep(0.5)
    sock.close()
    print("Connection closed.")


if __name__ == "__main__":
    main()
