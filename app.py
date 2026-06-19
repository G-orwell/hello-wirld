from fastapi import FastAPI, WebSocket , WebSocketDisconnect
import os



class ConnectionManager:
    def __init__(self):
        self.active_connections = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        print("Client connected:", len(self.active_connections))

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)
        print("Client disconnected:", len(self.active_connections))

    async def send_personal_message(self, message: str, websocket: WebSocket):
        await websocket.send_text(message)

    async def broadcast(self, message: str):
        for connection in self.active_connections:
            await connection.send_text(message)


manager = ConnectionManager()
app = FastAPI()
socket_app = app

@app.websocket("/ws")
async def ws_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    print("WebSocket connected")

    try:
        while True:
            data = await websocket.receive_text()
            print("MESSAGE RECEIVED:", data)

            # broadcast to ALL clients
            await manager.broadcast(data)
    except Exception as e:
        manager.disconnect(websocket)
        print("DISCONNECTED:", e)

# HTTP endpoint
@app.post("/fetch_api_2")
@app.get("/fetch_api_2")
@app.put("/fetch_api_2")
async def upload():
    # data = await request.body()
    # print("HTTP FILE RECEIVED:", len(data))
    # return {"status": "ok"}
    return ""

    # while True:
    #     data = await ws.receive_bytes()

    #     print("FILE RECEIVED:", len(data))

    #     with open("upload.bin", "wb") as f:
    #         f.write(data)
            
