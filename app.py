import os
import asyncio
import websockets

PORT = int(os.environ.get("PORT", 10000))

async def handler(ws):
    async for msg in ws:
        print("Received:", msg)
        await ws.send("echo: " + msg)

async def main():
    print("WebSocket running on port", PORT)

    async with websockets.serve(
        handler,
        "0.0.0.0",
        PORT
    ):
        await asyncio.Future()

asyncio.run(main())
