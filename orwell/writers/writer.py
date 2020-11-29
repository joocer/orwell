"""
Data Writer

Writes records to a data set partitioned by write time.

Default behaviour is to create a folder structure for year, month
and day, and partitioning data into files of 50,000 records or
are written continuously without a 60 second gap.

When a partition is written a method is called (on_partition_closed),
this provides a mechanism for users to perform an action on the 
recently closed partition file, such as save to a permanent store.

Records can be validated against a schema and records can be 
committed to disk after every write. Schema validation helps 
enforce format for the data and commit after every write reduces
the probability of data loss but both come with a cost; results will
differ depending on exact data but as an approximation (from and 11
field test data set):

- cache commits and no validation      = ~100% speed
- commit every write and validation    = ~40% speed
- commit every write but no validation = ~66% speed
- cache commits and no validation      = ~50% speed

Paths for the data writer can contain datetime string formatting,
the string will be formatted before being created into folders. The
changing of dates is handled by the worker thread, this may lag a 
second before it forces the folder to change.
"""

import datetime
import time
import os
from pathlib import Path
import threading

import json
json_dumps = json.dumps
try:
    import orjson
    json_dumps = orjson.dumps
except ImportError: pass
try:
    import ujson
    json_dumps = ujson.dumps


def _worker_thread(data_writer=None):
    """
    Method to run an a separate thread performing two tasks
    - when the day changes, it closes the existing partition so a 
        new one is opened with today's date
    - to close partitions when new records haven't been recieved
        for a period of time (default 60 seconds)

    These are done in a separate thread so the 'append' method
    doesn't need to perform these checks every write.
    """
    while data_writer.use_worker_thread:
        change_partition = False
        if (time.time_ns() - data_writer.last_write) > (data_writer.wait_time_seconds * 1e9):
            change_partition = True
        if not data_writer.formatted_path == datetime.datetime.today().strftime(data_writer.path):
            change_partition = True

        # close the current partition
        if change_partition:
            with threading.Lock():
                if data_writer.file_writer:
                    data_writer.on_partition_closed(data_writer.file_writer.filename)
                    del data_writer.file_writer
                    data_writer.file_writer = None

        # try flushing writes
        try:
            if data_writer.file_writer:
                data_writer.file_writer.file.flush()
        except: pass #nosec - if it fails, it doesn't /really/ matter

        time.sleep(1)


class DataWriter():

    def __init__(self, 
                path="year_%Y/month_%m/day_%d", 
                partition_size=50000, 
                commit_on_write=False,
                schema=None,
                use_worker_thread=True,
                wait_time_seconds=60):
        """
        DataWriter

        parameters:
        - path: the path to save records to, this is a folder name
        - partition_size: the number of records per partition
            (-1) is unbounded
        - commit_on_write: commit rather than cache writes - is 
            slower but less chance of loss of data
        - schema: Schema object - if set records are validated 
            before being written
        - use_worker_thread: creates a thread which performs 
            regular checks and corrections
        - wait_time_seconds: the time with no new writes to a 
            partition before closing it and creating a new partition
            regardless of the records
        """
        self.path = path
        self.partition_size = partition_size
        self.records_to_write_in_partition = partition_size
        self.schema = schema
        self.commit_on_write = commit_on_write
        self.file_writer = None
        self.last_write = time.time_ns()
        self.wait_time_seconds = wait_time_seconds
        self.formatted_path = ""
        self.use_worker_thread = use_worker_thread

        if use_worker_thread:
            self.thread = threading.Thread(target=_worker_thread, args=(self,))
            self.thread.daemon = True
            self.thread.start()

    def append(self, record={}):
        """
        Saves new entries to the partition; creating a new partition
        if one isn't active.
        """
        # this is a killer - check the new record conforms to the
        # schema before bothering with anything else
        if self.schema:
            self.schema.validate(subject=record, raise_exception=True)

        with threading.Lock():
            self.last_write = time.time_ns()
            # if we don't have a current file to write to, create one
            if not self.file_writer:
                self.formatted_path = datetime.datetime.today().strftime(self.path)
                path = Path(self.formatted_path)
                os.makedirs(path, exist_ok=True)
                filename = path / (str(time.time_ns()) + ".jsonl")
                self.file_writer = _PartFileWriter(filename=filename, 
                            commit_on_write=self.commit_on_write)
                self.records_to_write_in_partition = self.partition_size
            # write the record to the file
            self.file_writer.append(json_dumps(record).decode('utf8') + "\n")
            # close the partition when the record count is reached
            self.records_to_write_in_partition -= 1
            if self.records_to_write_in_partition == 0:
                self.on_partition_closed(self.file_writer.filename)
                del self.file_writer
                self.file_writer = None


    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        pass

    def on_partition_closed(self, partition_file):
        """
        This is intended to be replaced
        """
        pass

    def __del__(self):
        self.use_worker_thread = False


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
            self.file.flush()
            self.file.close()
        except: pass #nosec ignore if it fails 

def save_file_to_bucket(source_file, project, bucket, path):
    # to be deprecated
    """
    Copy a local file to a storage bucket
    
    Parameters:
        source_file: file to be copied
        bucket_name: destination storage bucket
        destination_file: destination file within bucket, including any pseudo path
    """
    client = storage.Client(project=project)
    bucket = client.get_bucket(bucket)
    path = path.replace('%date', '%Y-%m-%d')
    blob = bucket.blob(datetime.datetime.today().strftime(path))
    blob.upload_from_filename(source_file)