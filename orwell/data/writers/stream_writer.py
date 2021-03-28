import time
import datetime
import threading
from typing import Any
from dateutil import parser
from .simple_writer import SimpleWriter
from .internals.writer_pool import WriterPool
from ..validator import Schema  # type:ignore
from ...utils import paths
from ...errors import ValidationError
from ...logging import get_logger


class StreamWriter(SimpleWriter):

    def __init__(
            self,
            *,
            dataset: str,
            schema: Schema = None,
            format: str = 'zstd',
            idle_timeout_seconds: int = 30,
            date: Any = None,
            writer_pool_capacity: int = 5,
            **kwargs):
        """
        Create a Data Writer to write data records into partitions.

        Parameters:
            dataset: string (optional)
                The name of the dataset - this is used to map to a path
            schema: orwell.validator.Schema (optional)
                Schema used to test records for conformity, default is no 
                schema and therefore no validation
            format: string (optional)
                - jsonl: raw json lines
                - lzma: lzma compressed json lines
                - zstd: zstandard compressed json lines (default)
                - parquet: Apache Parquet
            idle_timeout_seconds: integer (optional)
                The number of seconds to wait before evicting writers from the
                pool for inactivity, default is 30 seconds
            date: date or string (optional)
                A date, a string representation of a date to use for
                creating the dataset. The default is today's date
            writer_pool_capacity: integer (optional)
                The number of writers to leave in the writers pool before 
                writers are evicted for over capacity, default is 5
            partition_size: integer (optional)
                The maximum size of partitions, the default is 64Mb
            inner_writer: BaseWriter (optional)
                The component used to commit data, the default writer is the
                NullWriter

        Note:
            Different inner_writers may take or require additional parameters.
        """
        # add the values to kwargs
        kwargs['format'] = format
        kwargs['dataset'] = dataset
        self.dataset = dataset

        super().__init__(**kwargs)

        self.idle_timeout_seconds = idle_timeout_seconds

        # we have a pool of writers of size maximum_writers
        self.writer_pool_capacity = writer_pool_capacity
        self.writer_pool = WriterPool(
                pool_size=writer_pool_capacity,
                **kwargs)

        # establish the background thread responsible for the pool
        self.thread = threading.Thread(target=self.pool_attendant)
        self.thread.name = 'orwell-writer-pool-attendant'
        self.thread.daemon = True
        self.thread.start()


    def append(self, record: dict = {}):
        """
        Append a new record to the Writer

        Parameters:
            record: dictionary
                The record to append to the Writer

        Returns:
            integer
                The number of records in the current partition
        """
        # Check the new record conforms to the schema before continuing
        if self.schema and not self.schema.validate(subject=record, raise_exception=False):
            raise ValidationError(F'Schema Validation Failed ({self.schema.last_error})')

        # get the appropritate writer from the pool and append the record
        # the writer identity is the base of the path where the partitions
        # are written.
        data_date = self.batch_date
        if data_date is None:
            data_date = datetime.date.today()
        identity = paths.date_format(self.dataset, data_date)

        with threading.Lock():
            partition_writer = self.writer_pool.get_writer(identity)
            return partition_writer.append(record)

    def pool_attendant(self):
        """
        Writer Pool Management
        """
        while True:
            with threading.Lock():
                # search for pool occupants who haven't had a write recently
                for partition_writer_identity in self.writer_pool.get_stale_writers(self.idle_timeout_seconds):
                    get_logger().debug(F'Evicting {partition_writer_identity} from the writer pool due to inactivity - limit is {self.idle_timeout_seconds} seconds')
                    self.writer_pool.remove_writer(partition_writer_identity)
                # if we're over capacity, evict the LRU writers
                for partition_writer_identity in self.writer_pool.nominate_writers_to_evict():
                    get_logger().debug(F'Evicting {partition_writer_identity} from the writer pool due the pool being over its {self.writer_pool_capacity} capacity')
                    self.writer_pool.remove_writer(partition_writer_identity)
            time.sleep(1)