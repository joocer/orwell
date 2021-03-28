"""
Writers are the target specific implementations which commit a temporary file
created by the PartitionWriter to different systems, such as the filesystem,
Google Cloud Storage or MinIO.

The primary activity is contained in the .commit() method.
"""
import os
import abc
import uuid
import time
from functools import lru_cache
from ....utils import paths


class BaseInnerWriter(abc.ABC):

    def __init__(
            self,
            dataset: str,
            **kwargs):

        self.bucket, path, _, _ = paths.get_parts(dataset)

        if self.bucket == '/':
            self.bucket = ''
        if path == '/':
            path = ''

        self.extension = '.jsonl'
        if kwargs.get('format', '') in ['lzma', 'zstd', 'parquet']:
            self.extension = '.jsonl.' + kwargs['format']

        self.filename = self.bucket + '/' + path + '{stem}' + self.extension
        self.filename_without_bucket = path + '{stem}' + self.extension

        if kwargs.get('suppress_path_expansion'):
            self.filename = self.bucket + '/' + path
            self.filename_without_bucket = path

    def _build_path(self):
        partition_id = F"{time.time_ns():x}-{self._get_node()}"
        return self.filename.replace('{stem}', F'{partition_id}')

    @lru_cache(1)
    def _get_node(self):
        return F"{uuid.getnode():x}-{os.getpid():x}"

    @abc.abstractclassmethod
    def commit(
            self,
            source_file_name):
        pass