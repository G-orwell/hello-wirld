import os
import asyncio
import threading
from flask import Flask
import websockets

app = Flask(__name__)

# -------------------
# HTTP ROUTE (IMPORTANT FOR RENDER)
# -------------------
@app.route("/")
def home():
    return "Server alive"

# -------------------
# WEBSOCKET SERVER
# -------------------
PORT = int(os.environ.get("PORT", 10000))

async def handler(ws):
    async for msg in ws:
        await ws.send("echo: " + msg)

async def ws_main():
    async with websockets.serve(handler, "0.0.0.0", PORT):
        await asyncio.Future()

def run_ws():
    asyncio.run(ws_main())

threading.Thread(target=run_ws, daemon=True).start()

# -------------------
# START FLASK
# -------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
