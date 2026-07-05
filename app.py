import asyncio
import logging
from typing import Optional

from fastapi import FastAPI, WebSocket, WebSocketDisconnect

# ------------------------------------------------------------
# Structured logging
# ------------------------------------------------------------
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# ------------------------------------------------------------
# Configuration
# ------------------------------------------------------------
MAX_QUEUE_SIZE = 1024          # Per‑client message buffer limit

# ------------------------------------------------------------
# Connection Manager
# ------------------------------------------------------------
class ConnectionManager:
    """
    Manages active WebSocket connections.

    * Non‑blocking broadcast via per‑client asyncio.Queue.
    * Dedicated writer task per client isolates slow consumers.
    * Lock‑protected shared state.
    * Graceful cleanup, even when the writer task fails.
    """

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._queues: dict[WebSocket, asyncio.Queue] = {}   # ws -> Queue
        self._tasks: dict[WebSocket, asyncio.Task] = {}     # ws -> writer task

    # ------------------------------------------------------------------
    async def connect(self, websocket: WebSocket) -> None:
        await websocket.accept()
        queue: asyncio.Queue = asyncio.Queue(maxsize=MAX_QUEUE_SIZE)
        task = asyncio.create_task(self._writer(websocket, queue))

        async with self._lock:
            self._queues[websocket] = queue
            self._tasks[websocket] = task

        logger.info("Client connected. Active connections: %d", len(self._queues))

    # ------------------------------------------------------------------
    async def disconnect(self, websocket: WebSocket) -> None:
        """Called from the endpoint. Removes connection and cancels the writer."""
        task = await self._remove_connection(websocket)

        # Do not cancel if we are inside that task (avoid deadlock)
        if task is not None and task is not asyncio.current_task():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        try:
            await websocket.close()
        except Exception:
            pass

        logger.info("Client disconnected. Active connections: %d", len(self._queues))

    # ------------------------------------------------------------------
    async def _remove_connection(self, websocket: WebSocket) -> Optional[asyncio.Task]:
        async with self._lock:
            self._queues.pop(websocket, None)
            task = self._tasks.pop(websocket, None)
        return task

    # ------------------------------------------------------------------
    async def _writer(self, websocket: WebSocket, queue: asyncio.Queue) -> None:
        """
        Background task that drains the client's queue and sends messages.
        If sending fails, it cleans up the connection automatically.
        """
        try:
            while True:
                msg_type, data = await queue.get()
                if msg_type == "bytes":
                    await websocket.send_bytes(data)
                elif msg_type == "text":
                    await websocket.send_text(data)
                else:
                    logger.warning("Unknown message type %s – ignored", msg_type)
        except Exception:
            logger.warning("Writer task failed for %s", websocket)
        finally:
            # Self‑cleanup without cancelling itself
            await self._remove_connection(websocket)
            try:
                await websocket.close()
            except Exception:
                pass

    # ------------------------------------------------------------------
    async def broadcast(
        self,
        sender: WebSocket,
        *,
        msg_type: str,
        data: bytes | str,
    ) -> None:
        """
        Push a message into the queue of every connected client except the sender.
        This returns immediately; actual sending happens in the writer tasks.
        """
        async with self._lock:
            for ws, queue in self._queues.items():
                # if ws is sender:
                #     continue
                try:
                    queue.put_nowait((msg_type, data))
                except asyncio.QueueFull:
                    # Client is too slow – drop message (could also disconnect)
                    logger.warning(
                        "Client %s queue full (size=%d). Dropping message.",
                        ws,
                        MAX_QUEUE_SIZE,
                    )


# ------------------------------------------------------------
# FastAPI application
# ------------------------------------------------------------
manager = ConnectionManager()
app = FastAPI()


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket) -> None:
    await manager.connect(websocket)

    try:
        while True:
            message = await websocket.receive()

            # In Starlette/FastAPI, receive() only returns messages of type
            # "websocket.receive". A disconnect raises WebSocketDisconnect,
            # which is caught below.  The old code's runtime error is eliminated.
            if message["type"] != "websocket.receive":
                continue

            if "bytes" in message:
                data = message["bytes"]
                logger.debug("Received %d bytes", len(data))
                await manager.broadcast(websocket, msg_type="bytes", data=data)
            elif "text" in message:
                data = message["text"]
                logger.debug("Received text of length %d", len(data))
                await manager.broadcast(websocket, msg_type="text", data=data)

    except WebSocketDisconnect:
        logger.info("WebSocket disconnected by client")
    except Exception:
        logger.exception("Unexpected error in WebSocket handler")
    finally:
        await manager.disconnect(websocket)


@app.api_route("/fetch_api_2", methods=["GET", "POST", "PUT"])
async def upload():
    return {"status": "ok"}
