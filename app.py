from fastapi import FastAPI, WebSocket
import os

app = FastAPI()
socket_app = app
@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await ws.accept()
    data = await ws.receive_text()
    print("MESSAGE RECEIVED:", data)
    # print("message received")

    # while True:
    #     data = await ws.receive_bytes()

    #     print("FILE RECEIVED:", len(data))

    #     with open("upload.bin", "wb") as f:
    #         f.write(data)
            
