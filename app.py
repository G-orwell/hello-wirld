import socketio
from fastapi import FastAPI

sio = socketio.AsyncServer(
    async_mode="asgi",
    cors_allowed_origins="*"
)

app = FastAPI()
socket_app = socketio.ASGIApp(sio, app)

@sio.event
async def connect(sid, environ, auth):
    print("Connected:", sid)

    # Send a Socket.IO event
    await sio.emit("message", {"text": "hello from render"}, to=sid)

@sio.event
async def disconnect(sid):
    print("Disconnected:", sid)

@sio.event
async def message(sid, data):
    print("Received:", data)

    await sio.emit(
        "message",
        {"echo": data},
        to=sid
    )

@app.get("/")
async def root():
    return {"status": "ok"}
