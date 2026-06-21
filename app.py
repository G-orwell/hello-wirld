from fastapi import FastAPI, WebSocket, WebSocketDisconnect
import asyncio

class ConnectionManager:
    def __init__(self):
        self.active_connections: list[tuple[WebSocket, asyncio.Lock]] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        lock = asyncio.Lock()
        self.active_connections.append((websocket, lock))
        print("Client connected:", len(self.active_connections))

    def disconnect(self, websocket: WebSocket):
        # Remove the first tuple whose WebSocket matches
        self.active_connections = [
            (ws, lock) for ws, lock in self.active_connections if ws != websocket
        ]
        print("Client disconnected:", len(self.active_connections))

    async def broadcast_bytes(self, data: bytes, sender=None):
        tasks = []
        for ws, lock in list(self.active_connections):
            if ws == sender:
                continue
            tasks.append(self._safe_send(ws, lock, data))
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _safe_send(self, ws: WebSocket, lock: asyncio.Lock, data: bytes):
        try:
            async with lock:
                await ws.send_bytes(data)
        except Exception:
            self.disconnect(ws)

manager = ConnectionManager()

app = FastAPI()


@app.websocket("/ws")
async def ws(websocket: WebSocket):

    await manager.connect(websocket)

    try:

        while True:

            msg = await websocket.receive()

            if msg["type"] != "websocket.receive":
                continue

            data = None

            if msg.get("bytes") is not None:

                data = msg["bytes"]

                print(
                    "BYTES RECEIVED:",
                    len(data)
                )

            elif msg.get("text") is not None:

                data = msg["text"].encode()

                print(
                    "TEXT RECEIVED:",
                    len(data)
                )

            if data:
                await manager.broadcast_bytes(
                    data,
                    sender=None
                )

    except WebSocketDisconnect:

        manager.disconnect(websocket)
        
        print("DISCONNECTED")

    except Exception as e:
        print("SEND FAILED:", e)

        manager.disconnect(websocket)

        print("ERROR:", e)


@app.api_route(
    "/fetch_api_2",
    methods=["GET", "POST", "PUT"]
)
async def upload():
    return {"status": "ok"}
