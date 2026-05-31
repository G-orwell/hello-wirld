import asyncio
import os
import websockets

clients = set()

async def handler(websocket):
    print("Client connected")
    clients.add(websocket)

    try:
        await websocket.send("hello from python server")

        async for message in websocket:
            print("Received:", message)
            await websocket.send(f"echo: {message}")

    except websockets.exceptions.ConnectionClosed:
        print("Client disconnected")

    finally:
        clients.remove(websocket)

async def main():
    port = int(os.environ.get("PORT", 8765))

    print(f"Server running on 0.0.0.0:{port}")

    async with websockets.serve(handler, "0.0.0.0", port):
        await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())
