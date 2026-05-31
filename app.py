from flask import Flask
from flask_socketio import SocketIO, emit

app = Flask(__name__)
socketio = SocketIO(app, cors_allowed_origins="*")

@socketio.on("connect")
def connect():
    print("client connected")
    emit("message", "hello from server")

@socketio.on("message")
def handle(msg):
    print("received:", msg)
    emit("message", "echo: " + msg)

socketio.run(app, host="0.0.0.0", port=5000)
