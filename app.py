from fastapi import FastAPI, WebSocket, WebSocketDisconnect
import asyncio


class ConnectionManager:
    def __init__(self):
        self.active_connections = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        print("Client connected:", len(self.active_connections))

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
        print("Client disconnected:", len(self.active_connections))

    async def send_personal_bytes(self, data: bytes, websocket: WebSocket):
        await websocket.send_bytes(data)

    # async def broadcast_bytes(self, data: bytes):
    #     dead = []

    #     for connection in self.active_connections:
    #         try:
    #             await connection.send_bytes(data)
    #         except Exception:
    #             dead.append(connection)

    #     for d in dead:
    #         self.disconnect(d)    
    async def broadcast_bytes(self, data: bytes, timeout: float = 2.0):
        dead = []
        for connection in self.active_connections:
            try:
                await asyncio.wait_for(connection.send_bytes(data), timeout=timeout)
            except (asyncio.TimeoutError, Exception):
                dead.append(connection)
        for d in dead:
            self.disconnect(d)

manager = ConnectionManager()
app = FastAPI()
socket_app = app


@app.websocket("/ws")
async def ws_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    print("WebSocket connected")

    try:
        while True:
            data = await websocket.receive_bytes()
            print("BYTES RECEIVED:", len(data))

            # broadcast raw bytes to all clients
            await manager.broadcast_bytes(data)

    except WebSocketDisconnect:
        manager.disconnect(websocket)
        print("DISCONNECTED: client closed connection")

    except Exception as e:
        manager.disconnect(websocket)
        print("DISCONNECTED (error):", e)


# HTTP endpoint (separate, clean)
@app.api_route("/fetch_api_2", methods=["GET", "POST", "PUT"])
async def upload():
    return {"status": "ok"}
