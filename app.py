from fastapi import FastAPI, WebSocket
import os

app = FastAPI()
socket_app = app

@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await ws.accept()
    print("WebSocket connected")

    try:
        while True:
            data = await ws.receive_text()
            print("MESSAGE RECEIVED:", data)
    except Exception as e:
        print("DISCONNECTED:", e)

# HTTP endpoint
@app.post("/fetch_api_2")
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
            
