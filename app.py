import asyncio
import logging
from typing import Optional
import uuid
import time

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
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
                # if wss is sender:
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


def dict_to_sql_insert(data: dict, table_name: str, conflict_column: str = "uu_id") -> str:
    """
    Generate an SQLite INSERT ... ON CONFLICT ... DO UPDATE statement.
    All values are converted to strings and single quotes are escaped as ''.

    Args:
        data: dict where keys are column names and values are the data.
        table_name: str, name of the target table.
        conflict_column: str, the unique column that triggers the conflict.

    Returns:
        str: SQLite upsert query with all values as string literals.
    """
    # Column names – quoted for safety (reserved words, spaces, etc.)
    columns = ', '.join(f'"{col.lower()}"' for col in data.keys())

    # Values: EVERYTHING is forced to a string, then quoted and escaped
    values = []
    for val in data.values():
        # Convert to string, then escape single quotes by doubling them
        escaped = str(val).replace("'", "''")
        values.append(f"'{escaped}'")
    values_str = ', '.join(values)

    # Build the SET clause: simple identifiers without quotes (as you preferred)
    set_clauses = []
    for col in data.keys():
        if col != conflict_column:  # skip updating the conflict column itself
            set_clauses.append(f"{col.lower()} = excluded.{col.lower()}")
        # (Remove the 'if' if you also want to update the conflict column)
    set_str = ', '.join(set_clauses)

    return (f'INSERT INTO {table_name} ({columns}) VALUES ({values_str}) '
            f'ON CONFLICT({conflict_column}) DO UPDATE SET {set_str};')

def flatten_json_0(data, parent_key='', sep='.', array_style='indexed'):
    """
    Recursively flatten a JSON object (dict or list) into a single-level dict.

    :param data:        The JSON to flatten (dict or list)
    :param parent_key:  Used internally for recursion
    :param sep:         Separator between nested keys (default '.')
    :param array_style: How to handle arrays:
                        'indexed' -> uses index in key (e.g., items.0.name)
                        'merged'  -> flattens each item without index (dangerous if keys collide)
                        'skip'    -> ignores arrays (not recommended)
    :return:            Flat dictionary
    """
    items = {}
    if isinstance(data, dict):
        for k, v in data.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, (dict, list)):
                items.update(flatten_json(v, new_key, sep, array_style))
            else:
                items[new_key] = v
    elif isinstance(data, list):
        if array_style == 'indexed':
            for idx, item in enumerate(data):
                new_key = f"{parent_key}{sep}{idx}" if parent_key else str(idx)
                if isinstance(item, (dict, list)):
                    items.update(flatten_json(item, new_key, sep, array_style))
                else:
                    items[new_key] = item
        elif array_style == 'merged':
            for item in data:
                if isinstance(item, (dict, list)):
                    items.update(flatten_json(item, parent_key, sep, array_style))
                else:
                    # For primitive values in arrays, we use a special key or ignore?
                    # Here we add them with a suffix to avoid collisions.
                    items[f"{parent_key}_item"] = item  # Not ideal; you may want to collect them as list
        # else 'skip' -> do nothing
    return items
def flatten_json(data, parent_key="", sep=".", array_style="indexed"):
    """
    Flatten a JSON object into a single-level dictionary.

    Enhanced handling for lists of dicts with a 'Name' key:
        [{"Name": "A", "Value": 1}, {"Name": "B"}]
    becomes:
        parent.A = 1
        parent.B = None
    and extra fields are flattened under the same name.
    """
    items = {}

    def _flatten(obj, prefix):
        # ---------- Dictionary ----------
        if isinstance(obj, dict):
            for key, value in obj.items():
                new_key = f"{prefix}{sep}{key}" if prefix else key
                _flatten(value, new_key)

        # ---------- List ----------
        elif isinstance(obj, list):
            # Special: list of dicts all having a "Name" key
            if (obj and all(isinstance(x, dict) and "Name" in x for x in obj)):
                for element in obj:
                    name = str(element["Name"])
                    # Main value: use "Value" if present, else None
                    main_value = element.get("Value")  # None if missing
                    key = f"{prefix}{sep}{name}" if prefix else name
                    items[key] = main_value

                    # Flatten any extra fields (other than Name/Value)
                    for k, v in element.items():
                        if k in ("Name", "Value"):
                            continue
                        extra_key = f"{key}{sep}{k}" if prefix else f"{name}{sep}{k}"
                        _flatten(v, extra_key)
                return   # done with this list

            # Normal arrays: use array_style
            if array_style == "indexed":
                for idx, element in enumerate(obj):
                    new_key = f"{prefix}{sep}{idx}" if prefix else str(idx)
                    _flatten(element, new_key)
            elif array_style == "merged":
                for element in obj:
                    _flatten(element, prefix)
            # else "skip" -> ignore

        # ---------- Primitive ----------
        else:
            items[prefix] = obj

    _flatten(data, parent_key)
    return items

@app.post("/mpesa/callback")
async def mpesa_callback(request: Request):

    data = await request.json()
    try:
        full_data = await request.json()
    except json.JSONDecodeError as e:
        print(f"Error : decoding {e}")
        return {"ResultCode": 1, "ResultDesc": "Invalid JSON"}
    # print("json == ",data)
    flat_dict = flatten_json(full_data,sep='_',array_style='merged')
    # print("flat_dict == ",flat_dict)

    new_flat_dict = {}
    for k , v in flat_dict.items():
        kk = k.lower().replace("result_","")
        kk = kk.replace("body_stkcallback_callbackmetadata_item_","")
        kk = kk.replace("body_stkcallback_","")
        new_flat_dict[ kk ] = v
        
    if "originatorconversationid" in new_flat_dict:
        new_flat_dict["uu_id"] = new_flat_dict["originatorconversationid"]
    elif "merchantrequestid" in new_flat_dict:
        new_flat_dict["uu_id"] = new_flat_dict["merchantrequestid"]
        
    sql = dict_to_sql_insert(new_flat_dict,"mpesa")
    await manager.broadcast(None, msg_type="text", data=sql)

    return {"ResultCode": 0,"ResultDesc": "Accepted"}

import re
class my_server:
    machine_id = 1
    sequence = 0
    last_ts = 0
    
    def is_safe_column_name(self, name):
        # Allow only letters, numbers, underscores; avoid SQL keywords (optional)
        return re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', name) is not None
    def generate_id(self):    
        ts = int(time.time() * 1000)
    
        if ts == self.last_ts:
            self.sequence += 1
        else:
            self.sequence = 0
    
        self.last_ts = ts
    
        id = ((ts << 22) | (self.machine_id << 12) | self.sequence)
        return id
    
    def build_insert(self,data):
        timestamp = int(float(time.time()))
        data["uu_id"] = uuid.uuid4()
        data["created_at"] = timestamp
        data["updated_at"] = timestamp
        data["id"] = self.generate_id()
        data["online_post"] = "1"
    
        columns = []
        values  = []
        table_name = 'user'
    
        for key, value in data.items():
            if key in ["cl_name","table_name","curr_url"]:
                if key in ["cl_name","table_name"]:
                    table_name = value
                continue
            if self.is_safe_column_name(key) == False:
                continue
            columns.append(key)
            value = str(value)
    
            value = value.replace("'", "''")
            values.append(f"'{value}'")
    
        columns_str = ", ".join(columns)
        values_str = ", ".join(values)
        # sql = "\n".join(f"\nALTER TABLE {table_name} ADD COLUMN {col} TEXT" for col in columns)
        sql = f"\n INSERT INTO {table_name} ({columns_str}) VALUES ({values_str});"
        return sql


# @app.route("/post",methods=['GET','POST','PUT'])
# def _post():

@app.api_route("/post", methods=["GET", "POST"])
async def post(request: Request):
    form = {}

    if request.method == "POST":
        form = dict(await request.form())
    else:
        form = dict(request.query_params)

    # form.update( request.form.to_dict() )
    # form.update( request.args.to_dict() )

    plist = my_server()
    query = plist.build_insert( form)
    # binary = query.encode("utf-8")
    # plist.saveFile(binary,"sql_post",120)
    await manager.broadcast(None, msg_type="text", data=query)

    return {"ResultCode": 0,"ResultDesc": "Accepted"}

    
    return query

