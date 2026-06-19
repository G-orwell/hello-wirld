import socketio
from fastapi import FastAPI
import os
import uuid

sio = socketio.AsyncServer(async_mode="asgi",cors_allowed_origins="*")

app = FastAPI()
socket_app = socketio.ASGIApp(sio, app)

UPLOAD_DIR = "uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)


@sio.event
async def connect(sid, environ, auth):
    print("Connected:", sid)


@sio.event
async def disconnect(sid):
    print("Disconnected:", sid)


@sio.event
async def message(sid, data):
    print("message event fired")

    # File upload (binary)
    if isinstance(data, bytes):
        filename = f"{uuid.uuid4()}.bin"
        path = os.path.join(UPLOAD_DIR, filename)

        with open(path, "wb") as f:
            f.write(data)

        print("File received:", path)
        await sio.emit(
            "message",
            {
                "status": "saved",
                "file": filename,
                "bytes": len(data)
            },
            to=sid
        )

        return

    # Normal text/json message
    print("Received:", data)

    await sio.emit(
        "message",
        {"echo": data},
        to=sid
    )


@app.get("/")
async def root():
    return {"status": "ok"}
from fastapi import Request

@app.put("/fetch_api_2")
async def fetch_api_2(request: Request):
    print("fetch api 2 called")
    # data = await request.body()

    # with open("received.bin", "wb") as f:
    #     f.write(data)

    # print("saved", len(data))

    return {
        "ok": True,
        "bytes": 2
    }
