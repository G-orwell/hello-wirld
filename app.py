from flask import Flask
from flask_socketio import SocketIO, send, emit

app = Flask(__name__)
# socketio = SocketIO(app, cors_allowed_origins="*")
socketio = SocketIO(
    app,
    cors_allowed_origins="*",
    logger=True,
    engineio_logger=True
)
@socketio.on("connect")
def handle_connect():
    print("Client connected")
    send("hello from Flask-SocketIO server")

@socketio.on("message")
def handle_message(msg):
    print("Received:", msg)
    send(f"echo: {msg}")

@socketio.on("disconnect")
def handle_disconnect():
    print("Client disconnected")

if __name__ == "__main__":
    socketio.run(app, host="0.0.0.0", port=5000)
