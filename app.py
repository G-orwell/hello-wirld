from flask import Flask,Response,request,send_file,jsonify,abort, stream_with_context
import types

import os
import datetime
import time
from pathlib import Path
import uuid

from sortedcontainers import SortedList
from threading import Lock
from bisect import bisect_left
import calendar
import zstandard as zstd

class ThreadSafeFileProcessor:
    splitter  = "112u211"
    timestamp_format = "%Y%m%d%H%M%S"
    # SEPARATOR = b'\xDE\xAD\xBE\xEF'
    SEPARATOR  = b'\xde\xad\xbe\xef'
    CHUNK_SIZE = 256 * 1024
    MAX_FILES  = 150
    MAX_SECONDS = 15
    dname = ""
    total_files = 0

    SERVER = 0
    CLIENT = 1
    EXPIRY = 2
    FILENAME = 3

    total_sent = ""
    last_time_run = None
    INTERVAL = 5  # seconds


    def sortListRefresh(self):
        self.server_timestamp = SortedList(key=lambda x: x[0])   # (server_utc_timestamp, ...)
        self.client_timestamp = SortedList(key=lambda x: x[1])   # (client_utc_timestamp, ...)
        self.files_by_expiry  = SortedList(key=lambda x: x[2])   # (expire_ts, ...)

    def __init__(self):
        self.directory_path = str(Path(__file__).resolve().parent / "cache")
        self.me         = [ "me.html", "mg_invoicing.apk" ]

        self.lock = Lock()
        os.makedirs(self.directory_path, exist_ok=True)

        # for filename in os.listdir(self.directory_path):#slower
        #     current_files.append(filename)
        # with os.scandir(self.directory_path) as it:#not working
        #     filename = [entry.name for entry in it if entry.is_file()]
        #     current_files.append(filename)

        self.add( os.listdir(self.directory_path) ,True)

        self.app = Flask(
            __name__,
            static_folder   = self.directory_path,
            static_url_path = "/",
            template_folder = self.directory_path
        )
        self.app.secret_key = "wewe3erwr"
        self.app.config['SECRET_KEY'] = 'secret!'
        self.app.config["TEMPLATES_AUTO_RELOAD"] = True

        self.last_cleanup     = 0
        self.CLEANUP_INTERVAL = 60  # seconds

    def maybe_cleanup(self):
        """Only run cleanup if enough time has passed."""
        now = time.time()
        if now - self.last_cleanup > self.CLEANUP_INTERVAL:
            self.cleanup_old_files()
            self.last_cleanup = now
    def cleanup_old_files(self):
        with self.lock:
            now_ts = int(time.time())
            # Find all files with expire_ts < now_ts
            idx = self.files_by_expiry.bisect_left((0, 0, now_ts, ""))
            expired = self.files_by_expiry[:idx]
            if not expired:
                return 0
            for tup in expired:
                # Delete file from disk
                full_path = os.path.join(self.directory_path, tup[self.FILENAME])
                if os.path.exists(full_path) and ".html" not in full_path:
                    os.remove(full_path)
                # Remove from both lists
                # self.client_timestamp.remove(tup)
                self.remove_or_add(tup)
                # self.server_timestamp.remove(tup)
                # self.client_timestamp.remove(tup)

            # del self.files_by_expiry[:idx]
            self.total_files = len(self.client_timestamp)
            return len(expired)

    def remove_or_add(self, tup , add = False):
        # self.files.remove(tup)
        if add:
            self.server_timestamp.add(tup)
            self.client_timestamp.add(tup)
            self.files_by_expiry.add(tup)
        else:
            self.server_timestamp.remove(tup)
            self.client_timestamp.remove(tup)
            self.files_by_expiry.remove(tup)

    def getUTC(self,local_ts_str):
        # local_ts_str = "20260311234307"  # in UTC+3
        from datetime import timezone, timedelta
        dt_local = datetime.datetime.strptime(local_ts_str, "%Y%m%d%H%M%S")

        tz_offset = timezone(timedelta(hours=3))  # UTC+3
        dt_local = dt_local.replace(tzinfo=tz_offset)

        dt_utc = dt_local.astimezone(timezone.utc)

        return int(dt_utc.timestamp())
    def replaceFileName(self,filepath,old_string, new_string):
        # Read the file
        with open(filepath, "r", encoding="utf-8") as f:
            content = f.read()

        # Replace text
        content = content.replace(old_string, new_string)

        # Write back to the same file
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(content)

    def add(self, filenames,newList=False):
        with self.lock:
            if newList:
                self.sortListRefresh()

            for filename in filenames:
                src = os.path.join(self.directory_path, filename)
                if os.path.getsize(src) == 0:
                    print("Adding an empty file not allowed::>>", src)
                    continue

                if filename.find(".html") != -1:
                    des = os.path.join(self.directory_path, filename)
                    if filename.find(".comp") != -1:
                        des = des.replace(".comp","")
                        des = des.replace("upload_","")
                        try:
                            self.decompress_zstd_file(src,des)
                            self.replaceFileName(des,"is_android : true","is_android : false");
                            os.remove(src)
                        except Exception as e:
                            print("Failled decompress ",src , " to ", des , e)
                            os.remove(des)
                    continue
                if self.splitter not in filename:
                    continue

                server_utc_timestamp,client_utc_timestamp,device_id,TIME_IN_SERVER = filename.split(self.splitter)
                if  len(client_utc_timestamp) == 14 and client_utc_timestamp.startswith("20"):
                    try:
                        # dt = datetime.datetime.strptime(client_utc_timestamp, "%Y%m%d%H%M%S")
                        client_utc_timestamp = self.getUTC(client_utc_timestamp) #calendar.timegm(dt.timetuple())
                    except Exception as e:
                        print("Failed to parse YYYYMMDDHHMMSS:", e , client_utc_timestamp )
                        continue

                client_utc_timestamp = int(float( client_utc_timestamp  ))
                server_utc_timestamp = int(float( server_utc_timestamp  ))

                try:
                    TIME_IN_SERVER = int(float( TIME_IN_SERVER ))
                except Exception:
                    TIME_IN_SERVER = 15 #unkonow time

                # Compute file expiration timestamp
                expire_ts = client_utc_timestamp + TIME_IN_SERVER
                # print("DDED FILE ",(utc_timestamp,expire_ts,filename) )

                tup = (server_utc_timestamp,client_utc_timestamp,expire_ts,filename)
                self.remove_or_add(tup,True)
                if filename.find("updates.data") != -1:
                    src = os.path.join(self.directory_path, filename)
                    filename = filename.replace(".data",".decomp")
                    filename = filename.replace("upload_","")
                    des = os.path.join(self.directory_path, filename)
                    try:
                        self.decompress_zstd_file(src,des)
                        tup = (server_utc_timestamp,client_utc_timestamp,expire_ts,filename)
                        self.remove_or_add(tup,True)
                    except:
                        os.remove(des)
                        print("failled to decompress the updates.data")
            self.total_files = len(self.client_timestamp)


    def process_and_filter_files(self,utc_min_timestamp,filtered_files,device,os22,arc,bit32_62):
        """
            - Deletes files older than 30 days (based on filename timestamp).
            - Returns files that are:
                a) newer than `min_timestamp`, and
                b) not from `excluded_device_id`.
            Args:
                min_timestamp (str): Minimum timestamp in 'YYYYMMDDHHMMSS' format.
                excluded_device_id (str): Device ID to exclude.
            Returns:
                list: Filtered list of full file paths.
        """
        if  utc_min_timestamp == None:
            return

        # Suppose self.files is SortedList of tuples: (timestamp_str, ...)

        # idx = self.server_timestamp.bisect_left((utc_min_timestamp,))
        idx = self.server_timestamp.bisect_left((0, utc_min_timestamp, 0, ""))
        recent_files = self.server_timestamp[idx:]

        # for file_parts in self.files:
        for file_parts in recent_files:
            try:
                server_timestamp,client_timestamp,expire_ts,filename = file_parts

                # print("starts:" , timestamp_str   )
                # if device_id == device:
                #     # print("skiping 1")
                #     continue #allow for self update/but can cause duplicate insert

                # if device_os != ""  and device_os != os22:
                #     print("skiping 2")
                #     continue
                # if device_arch != ""  and device_arch != arc:
                #     print("skiping 3 : ",device_arch," = ",arc )
                #     continue
                # if device_32_64 != ""  and device_32_64 != bit32_62:
                #     print("skiping 4",device_32_64," = ", bit32_62)
                #     continue

                if server_timestamp >= utc_min_timestamp:
                    filtered_files.append( os.path.join(self.directory_path, filename) )
            except Exception as e:
                print(f"Skipped (error):  ({e})")
                continue
    def getParts(self,parts , pos , default=''):
        try:
            data = parts[pos]
            data = data.replace("-","")
            data = data.replace(":","")
            data = data.replace(" ","")
        except:
            print(f"form data {pos} not found ")
            data = default
        return data
    def getFiles(self, form):
        utc_time_now = int(time.time())
        utc_min_timestamp = int(float(self.getParts(form, 'Online', utc_time_now - 15)))

        with self.lock:
            # client-based candidates
            # idx_client = self.client_timestamp.bisect_left((utc_min_timestamp,))
            # client_candidates = set(self.client_timestamp[idx_client:])

            # server-based candidates
            idx_server = self.server_timestamp.bisect_left((utc_min_timestamp,0,0,""))
            server_candidates = set(self.server_timestamp[idx_server:])

            # intersection (safe)
            # final = client_candidates & server_candidates
            final = server_candidates

        # Now filter without holding the lock
        files = [os.path.join(self.directory_path, tup[self.FILENAME]) for tup in final]
        return files[:self.MAX_FILES]

    # def getFiles(self,form):
    #     files = []
    #     utc_time_now = int( float(time.time()) )
    #     try:
    #         utc_current_time_request_updates = utc_time_now - 15
    #         utc_min_timestamp                = int( float(self.getParts(form,'Online' , utc_current_time_request_updates)) )
    #         device_id                        = self.getParts( form , 'device_uu_id'   ,'')
    #         os2                              = self.getParts( form , "__os__"         ,'')
    #         arc                              = self.getParts( form , "_sys_arch"      ,'')
    #         bit32_62                         = self.getParts( form , "pointerSize"    ,'')
    #         # min_timestamp                  = "20250802084436"
    #         # if path.find("fetch_api") != -1:
    #         self.process_and_filter_files(utc_min_timestamp,files,device_id,os2,arc,bit32_62)

    #         self.total_sent = f"{utc_min_timestamp}:{len(files)}"
    #     except Exception as e:
    #         print("ERROR CLIENT DID NOT GIVE ", e )

    #     # if specific_file != '' and filename.find(specific_file) == -1:
    #     #     continue
    #     # elif filename.find(".decomp") != -1:
    #     #     continue
    #     return files

    def stream_file(self, path):
        if os.path.getsize(path) == 0:
            print("file size is 0 ", path)
            return False

        sent = False
        # total = 0
        with open(path, "rb") as f:
            while chunk := f.read(self.CHUNK_SIZE):
                sent = True
                yield chunk
                # toatl += size(chunk)
        # print("total sent = ",total," file:",path)
        return sent
    def decompress_zstd_file(self,input_file, output_file, chunk_size=262144):  # 256 KB
        """
            High-performance streaming ZSTD decompression.
        """
        dctx = zstd.ZstdDecompressor()
        with open(input_file, "rb") as f_in, open(output_file, "wb") as f_out:
            with dctx.stream_reader(f_in) as reader:
                buffer = bytearray(chunk_size)
                mv = memoryview(buffer)

                while True:
                    read_bytes = reader.readinto(mv)
                    if read_bytes == 0:
                        break
                    f_out.write(mv[:read_bytes])
    def rrr(self,callback , path_list=None):

        result = callback()
        if isinstance(result, types.GeneratorType):
            try:
                form = yield from result
            except StopIteration as e:
                form = e.value
        else:
            form = result

        start_time = time.time()
        path_list = self.getFiles(form)

        use_separator = True
        searchname      = self.getParts(form,"fname",'')
        _server         = self.getParts(form,"server",'')
        if searchname != '':
            use_separator = False
            path_list = [f for f in path_list if searchname in f]
            # print("seaching for file",searchname," ; ",path_list)
        else:
            utc_time_now = int( float(time.time()) )
            if (self.last_time_run is None) or (utc_time_now - self.last_time_run >= self.INTERVAL ):
                self.last_time_run = utc_time_now
                yield from yieldString(f"url::mg/web_page/me.html/processpage?yield_to_remote=1&_server{_server}")
                # yield from yieldString("url::mg/pythonanywhere_api/talentors2/webapps")
                # yield from yieldString("url::mg/pythonanywhere_api/talentors2/update")
                # update last run

        for path in path_list:
            # if time.time() - start_time >= self.MAX_SECONDS:
            #     break
            # print("Streaming file ", path)
            try:
                sent_any = False
                for chunk in self.stream_file(path) or []:
                    sent_any = True
                    yield chunk
                if use_separator and sent_any:
                    yield self.SEPARATOR
            except Exception as e:
                print("Error sending:", path, e)
                continue

        if use_separator:

            for f in self.me:
                try:
                    size = os.path.getsize( os.path.join( self.directory_path , f ) )
                    if size < 100:
                        print(f," not found",self.me)
                        if f.find(".html") != -1:
                            url = "url::mg/web_page/me.html/update"
                        else:
                            url = "mg/user/0/upload_a_file?_file_to_upload=C:/ProgramData/website/cache/apps/"+f+".nocompress"
                        yield from yieldString(url)
                except FileNotFoundError:
                    pass

        # now_ts = str(float(time.time()))
        # yield "url::mg/server/talentors.pythonanywhere.com/insert?last_time_online="+now_ts
        # yield self.SEPARATOR
        self.maybe_cleanup()

    def getServerTimeStamp_UTC(self):
        utc_timestamp_server = str( int(time.time()) )
        return utc_timestamp_server
    def saveFile(self,data,device_id='updates',TIME_IN_SERVER   = 15):
        timestamp_str    = datetime.datetime.now()

        form             = {}
        form['saveName'] = plist.getServerTimeStamp_UTC() + plist.splitter + str(int(float(time.time()))) + self.splitter + device_id + self.splitter + str(TIME_IN_SERVER)
        name             = self.getParts(form,"saveName",'')
        filepath         = os.path.join( self.directory_path , name )

        with open(filepath, "w") as file:
            file.write( data )
        self.add([name])
    def is_safe_column_name(self, name):
        # Allow only letters, numbers, underscores; avoid SQL keywords (optional)
        import re
        return re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', name) is not None
    def build_insert(self,data):
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
        sql = "\n".join(f"\nALTER TABLE {table_name} ADD COLUMN {col} TEXT" for col in columns)
        sql += f"\n INSERT INTO {table_name} ({columns_str}) VALUES ({values_str});"
        return sql


plist = ThreadSafeFileProcessor();
app = plist.app

@app.route("/debug")
def debug():
    # Replace 'yourusername' and 'your.domain.com' with your actual details
    log_file_path = "/var/log/talentors.pythonanywhere.com.error.log"
    log_file_path2 = "/var/log/talentors.pythonanywhere.com.server.log"
    log_file_path3 = "/var/log/talentors.pythonanywhere.com.access.log"

    try:
        # Overwrite the file with an empty string using a Bash command
        os.system(f'echo "" > {log_file_path}')
        os.system(f'echo "" > {log_file_path2}')
        os.system(f'echo "" > {log_file_path3}')
        print(f"Log file '{log_file_path}' content cleared.")
    except Exception as e:
        print(f"An error occurred: {e}")

    # Note: You can also use a similar approach to clear access.log and server.log

    plist.maybe_cleanup()
    disk_files = os.listdir(plist.directory_path)
    return jsonify({
        "static_folder"           : app.static_folder                             ,
        "static_url_path"         : app.static_url_path                           ,
        "template_folders"        : app.jinja_loader.searchpath                   ,
        "total Files"             : plist.total_files                             ,
        "total sent"              : plist.total_sent                              ,
        "disk_files"              : len(disk_files)                               ,
    })
import io
@app.route("/")
@app.route("/<string:name>")
def serve(name="me.html"):
    file_path = os.path.join(app.template_folder, name)
    dname = request.args.get("dname")
    if not os.path.isfile(file_path):
        return abort(404)
    if dname:
        return send_file(file_path,as_attachment=True , download_name=dname)
    return send_file(file_path)


import time

machine_id = 1
sequence = 0
last_ts = 0

def generate_id():
    global sequence, last_ts

    ts = int(time.time() * 1000)

    if ts == last_ts:
        sequence += 1
    else:
        sequence = 0

    last_ts = ts

    id = ((ts << 22) | (machine_id << 12) | sequence)
    return id


@app.route("/post",methods=['GET','POST','PUT'])
def _post():
    form = {}
    timestamp = int(float(time.time()))

    form["uu_id"] = uuid.uuid4()
    form["created_at"] = timestamp
    form["updated_at"] = timestamp
    form["id"] = generate_id()
    form["online_post"] = "1"
    form.update( request.form.to_dict() )
    form.update( request.args.to_dict() )

    query = plist.build_insert( form)
    plist.saveFile(query,"sql_post",120)

    return query


import os
import logging
from flask import request, abort

def yieldString(s):
    s = f"\n{s}"
    yield s.encode("utf-8")
    yield plist.SEPARATOR
def split_remove_first_join(text, splitter):
    parts = text.split(splitter)  # split the string
    if len(parts) <= 1:
        return ""  # nothing left after removing first element
    return splitter.join(parts[1:])  # remove first and re-join

def file_saved_on_server( filename):
    size = 0
    path = os.path.join(plist.directory_path, filename)
    if os.path.exists(path):
        try:
            size = os.path.getsize( path )
            plist.add([filename])
        except:
            pass

    filename = split_remove_first_join(filename,plist.splitter)
    filename2 = filename.replace(".comp","")
    s = f"url::mg/file/{filename2}/insert?saved_on_server=1&file_to_delete={filename}&time_reached_server={int(float(time.time()))}&upload_record=0&size_in_server={str(size)}"

    yield from yieldString(s)

def process1():
    form = {}
    # print("request.headers == ",request.headers)
    form.update(request.args.to_dict())
    form.update(dict(request.headers))

    stream = request.stream
    if not stream:
        logger.warning("No stream available")
        abort(400, description="No stream provided")

    SEPARATOR = b'\xde\xad\xbe\xef'
    SEP_LEN = len(SEPARATOR)

    STATE_FILENAME_LEN = 0
    STATE_FILENAME = 1
    STATE_CONTENT = 2

    state = STATE_FILENAME_LEN

    filename = ""
    filename_len_buf = bytearray()
    filename_buf = bytearray()

    pending = bytearray()

    current_file = None
    file_index = 1

    f_list = []
    def open_new_file():
        nonlocal filename,current_file, file_index, filename_buf

        try:
            filename = filename_buf.decode(errors='replace')
            if not filename:
                filename = f"output_{file_index}.bin"
                file_index += 1
            if filename.find(plist.splitter) != -1:
                filename = plist.getServerTimeStamp_UTC() + plist.splitter + filename

            filepath = os.path.join(plist.directory_path, filename)
            current_file = open(filepath, "wb")

            f_list.append( os.path.join(plist.directory_path, filename) )
        except Exception as e:
            logger.error(f"Failed to open file: {e}")
            abort(500, description="File creation error")
        finally:
            filename_buf.clear()

    while True:
        chunk = stream.read(plist.CHUNK_SIZE)
        if not chunk:
            break

        pending.extend(chunk)

        i = 0
        length = len(pending)

        while i < length:
            # ----------- STATE: FILENAME LENGTH (2 bytes) -----------
            if state == STATE_FILENAME_LEN:
                needed = 2 - len(filename_len_buf)
                take = min(needed, length - i)

                filename_len_buf.extend(pending[i:i+take])
                i += take

                if len(filename_len_buf) == 2:
                    filename_len = int.from_bytes(filename_len_buf, "big")
                    filename_len_buf.clear()
                    state = STATE_FILENAME

            # ----------- STATE: FILENAME -----------
            elif state == STATE_FILENAME:
                needed = filename_len - len(filename_buf)
                take = min(needed, length - i)

                filename_buf.extend(pending[i:i+take])
                i += take

                if len(filename_buf) == filename_len:
                    open_new_file()
                    state = STATE_CONTENT

            # ----------- STATE: CONTENT -----------
            elif state == STATE_CONTENT:
                # Look for separator
                sep_index = pending.find(SEPARATOR, i)

                if sep_index == -1:
                    # No separator → write safe portion
                    safe_end = length - (SEP_LEN - 1)

                    if safe_end > i:
                        if current_file:
                            current_file.write(pending[i:safe_end])
                        i = safe_end
                    else:
                        # Not enough data to safely check separator
                        break
                else:
                    # Separator found
                    if current_file:
                        current_file.write(pending[i:sep_index])
                        current_file.close()
                        current_file = None
                        yield from file_saved_on_server(filename)


                    i = sep_index + SEP_LEN

                    # Reset for next file
                    state = STATE_FILENAME_LEN
                    filename_len_buf.clear()
                    filename_buf.clear()

        # Keep only unprocessed tail (important for separator spanning chunks)
        pending = pending[i:]

    # ----------- END OF STREAM -----------

    if state == STATE_CONTENT and current_file:
        # Write remaining bytes (not a separator)
        current_file.write(pending)
        current_file.close()
        yield from file_saved_on_server(filename)

    elif state in (STATE_FILENAME_LEN, STATE_FILENAME):
        logger.warning("Incomplete header at end of stream")
        # abort(400, description="Incomplete upload stream")
        return form

    return form


# Configure logging (adjust as needed for your application)
logger = logging.getLogger(__name__)
@app.route("/fetch_api_2", methods=["POST", "PUT"])
def fetch_api_2():
    return Response(
        stream_with_context(plist.rrr(process1)),
        mimetype="application/octet-stream",
        direct_passthrough=True
    )
# @app.route("/fetch_api_23", methods=["POST","PUT"])
# def fetch_api_23():
#     stream = getattr(request, "stream", None)

#     if stream == None:
#         # print("No stream available")
#         return

#     SEPARATOR = b'\xde\xad\xbe\xef'
#     sep_len = len(SEPARATOR)

#     f = None
#     match_index = 0
#     filename = None
#     reading_filename = True
#     filename_len_bytes = bytearray()
#     filename_bytes = bytearray()
#     file_index = 1  # fallback if filename missing

#     for chunk in request.stream:
#         for byte in chunk:
#             # --- handle separator ---
#             if byte == SEPARATOR[match_index]:
#                 match_index += 1
#                 if match_index == sep_len:
#                     # full separator matched -> start new file
#                     if f:
#                         f.close()
#                     reading_filename = True
#                     filename_len_bytes.clear()
#                     filename_bytes.clear()
#                     match_index = 0
#                     continue
#             else:
#                 if match_index > 0:
#                     # write partially matched separator bytes to file
#                     if f:
#                         f.write(SEPARATOR[:match_index])
#                     match_index = 0

#             # --- handle reading filename header ---
#             if reading_filename:
#                 if len(filename_len_bytes) < 2:
#                     # first 2 bytes are filename length
#                     filename_len_bytes.append(byte)
#                     continue
#                 elif len(filename_bytes) < int.from_bytes(filename_len_bytes, "big"):
#                     filename_bytes.append(byte)
#                     continue
#                 else:
#                     # finished reading filename, open new file
#                     filename = filename_bytes.decode(errors="replace")
#                     if not filename:
#                         filename = f"output_{file_index}.bin"
#                         file_index += 1
#                     filepath = os.path.join( plist.directory_path , filename)
#                     f = open(filepath, "wb")
#                     plist.add([filename])
#                     reading_filename = False
#                     # current byte belongs to file content
#                     if f:
#                         f.write(bytes([byte]))
#             else:
#                 # normal file content
#                 if f:
#                     f.write(bytes([byte]))

#     # after finishing stream
#     if f:
#         f.close()

#     form = {}
#     form.update( request.args.to_dict() )
#     form.update(dict(request.headers))

#     return Response( plist.rrr(form) , mimetype="application/octet-stream" , direct_passthrough=True )

def process_2():
    form = {}
    try:
        for file_key in request.files:
            file = request.files[file_key]
            if file.filename == '':
                continue
            filename = plist.getServerTimeStamp_UTC() + plist.splitter + file.filename.replace("compeeecomp","")
            filepath = os.path.join( plist.directory_path , filename)
            file.save(filepath)


            plist.add([filename])
        form.update( request.form.to_dict() )
        form.update( request.args.to_dict() )



        # if "yields" in form :
        #     plist.cleanup_old_files()
        #     plist.saveFile(form["yields"],'updates.data')
        # form["session_ip"] = request.headers['X-Real-IP']
    except Exception as e:
        msg = f"Something went wrong e <br> {e}"
        print(msg)
    return form

@app.route("/fetch_api",methods=['GET','POST','PUT'])
def rw12r(others=None):

    mimeType = "application/octet-stream"
    if "fname" in request.args:
        mimeType = "text/plain"


    return Response( stream_with_context(plist.rrr(process_2)) , mimetype=mimeType , direct_passthrough=True )


