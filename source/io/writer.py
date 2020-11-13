import datetime
import time
import os
from pathlib import Path
import threading

# select the fastest json parser availabile
json_parser = None
try:
    import orjson as json
    json_parser = "orjson"
except ImportError:
    pass
if not json_parser:
    try:
        import ujson
        json_parser = "ujson"
    except ImportError:
        import json


def _worker_thread(data_writer=None):
    while True:
        if (time.time_ns() - data_writer.last_write) > (data_writer.wait_time_seconds * 1e9):
            if data_writer.file_writer:
                data_writer.on_partition_full(data_writer.file_writer.filename)
                del data_writer.file_writer
                data_writer.file_writer = None
        time.sleep(5)


class DataWriter():

    def __init__(self, 
                path="year_%Y/month_%m/day_%d", 
                partition_size=50000, 
                commit_on_write=False,
                schema=None,
                use_worker_thread=True,
                wait_time_seconds=5):
        """
        DataWriter

        parameters:
        - path: the path to save records to, this is a folder name
        - partition_size: the number of records per partition
        - commit_on_write: commit rather than cache writes - is 
            slower but less chance of loss of data
        - schema: Schema object - if set records are validated 
            before being written
        """
        self.path = path
        self.partition_size = partition_size
        self.records_to_write_in_partition = partition_size
        self.schema = schema
        self.commit_on_write = commit_on_write
        self.file_writer = None
        self.last_write = time.time_ns()
        self.wait_time_seconds = wait_time_seconds

        if use_worker_thread:
            self.thread = threading.Thread(target=_worker_thread, args=(self,))
            self.thread.daemon = True
            self.thread.start()

    def append(self, record={}):
        with threading.Lock():
            self.last_write = time.time_ns()
            # this is a killer - check this before bothering with 
            # anything else
            if self.schema:
                self.schema.validate(subject=record, raise_exception=True)

            if not self.file_writer:
                formatted_path = datetime.datetime.today().strftime(self.path)
                path = Path(formatted_path)
                os.makedirs(path, exist_ok=True)
                filename = path / (str(time.time_ns()) + ".jsonl")

                self.file_writer = _PartFileWriter(filename=filename, 
                            commit_on_write=self.commit_on_write)
                self.records_to_write_in_partition = self.partition_size

            self.file_writer.append(json.dumps(record).decode('utf8') + "\n")
            self.records_to_write_in_partition -= 1
            if self.records_to_write_in_partition == 0:
                self.on_partition_full(self.file_writer.filename)
                del self.file_writer
                self.file_writer = None


    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        pass

    def on_partition_full(self, partition_file):
        pass

    def __def__(self):
        thread.kill()


class _PartFileWriter():
    """ simple wrapper for file writing """
    def __init__(self, 
                filename="", 
                commit_on_write=False, 
                enconding="utf8"):
        self.filename = filename
        self.file = open(file=filename, 
                        mode="x", 
                        encoding=enconding)
        self.commit_on_write = commit_on_write
    def append(self, record=""):
        self.file.write(record)
        if self.commit_on_write:
            self.file.flush()
    def __del__(self):
        try:
            self.file.close()
        except:
            pass