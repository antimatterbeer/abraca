import asyncio
import json

from loguru import logger

HOST = "127.0.0.1"
PORT = 8080


class Client:
    def __init__(self, host: str, port: int) -> None:
        self.host = host
        self.port = port
        self.reader, self.writer = None, None

    async def __aenter__(self) -> "Client":
        self.reader, self.writer = await asyncio.open_connection(self.host, self.port)
        return self

    async def __aexit__(self, exc_type, exc_value, traceback) -> None:
        self.writer.close()
        await self.writer.wait_closed()

    async def send(self, req: dict) -> None:
        msg = json.dumps(req) + "\n"
        self.writer.write(msg.encode())
        await self.writer.drain()

    async def get_symbol_info(self, exchange: str, symbols: list[str]) -> None:
        req = {
            "exchange": exchange,
            "req": "get_symbol_info",
            "data": symbols,
            "id": 0,
        }
        msg = json.dumps(req) + "\n"
        self.writer.write(msg.encode())
        await self.writer.drain()

    async def subscribe(self, exchange: str, topics: list[str]) -> None:
        req = {
            "exchange": exchange,
            "req": "subscribe",
            "data": topics,
            "id": 1,
        }
        msg = json.dumps(req) + "\n"
        self.writer.write(msg.encode())
        await self.writer.drain()

    async def unsubscribe(self, exchange: str, topics: list[str]) -> None:
        req = {
            "exchange": exchange,
            "req": "unsubscribe",
            "data": topics,
            "id": 2,
        }
        msg = json.dumps(req) + "\n"
        self.writer.write(msg.encode())
        await self.writer.drain()

    async def recv(self) -> None:
        while True:
            chunk = await self.reader.readline()
            if not chunk:
                break
            rsp = json.loads(chunk.decode("utf-8", errors="replace").strip())
            self.on_rsp(rsp)

    def on_rsp(self, rsp: dict) -> None:
        logger.info(f"Rsp: {rsp}")


async def main() -> None:
    async with Client(HOST, PORT) as client:
        await client.get_symbol_info("BinanceFutures", ["BTCUSDT", "ETHUSDT"])
        await client.subscribe("BinanceFutures", ["BTCUSDT@Depth", "ETHUSDT@Kline"])
        recv_task = asyncio.create_task(client.recv())
        await recv_task


if __name__ == "__main__":
    asyncio.run(main())
