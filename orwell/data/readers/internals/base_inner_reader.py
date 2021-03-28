"""
Base Inner Reader
"""
import abc
import pathlib
import datetime
from io import IOBase
from typing import Iterable
from dateutil import parser
from ....utils import common, paths
from ....logging import get_logger

class BaseInnerReader(abc.ABC):

    VALID_EXTENSIONS = ['.zstd', '.lzma', '.jsonl', '.csv', '.lxml', '.parquet', '.ignore', '.profile', '.index']

    def _extract_date_part(self, value):
        if isinstance(value, str):
            value = parser.parse(value)
        if isinstance(value, (datetime.date, datetime.datetime)):
            return datetime.date(value.year, value.month, value.day)
        return datetime.date.today()

    def _extract_as_at(self, path):
        parts = path.split('/')
        for part in parts:
            if part.startswith('as_at_'):
                return part
        return ''

    def __init__(self, **kwargs):
        self.dataset = kwargs.get('dataset')
        if self.dataset is None:
            raise ValueError('Readers must have the `dataset` parameter set')
        if not self.dataset.endswith('/'):
            self.dataset += '/'
        if 'date' not in self.dataset and not kwargs.get('raw_path', False):
            self.dataset += '{datefolders}/'

        self.start_date = self._extract_date_part(kwargs.get('start_date'))
        self.end_date = self._extract_date_part(kwargs.get('end_date'))

        self.days_stepped_back = 0

    def step_back_a_day(self):
        """
        Steps back a day so data can be read from a previous day
        """
        self.days_stepped_back += 1
        self.start_date -= datetime.timedelta(days=1)
        self.end_date -= datetime.timedelta(days=1)
        return self.days_stepped_back

    def __del__(self):
        """
        Only here in case a helpful dev expects te base class to have it
        """
        pass

    @abc.abstractmethod
    def get_blobs_at_path(self, prefix=None) -> Iterable:
        pass 

    @abc.abstractmethod
    def get_blob_stream(self, blob: str) -> IOBase:
        """
        Return a filelike object
        """
        pass

    def get_records(self, blob) -> Iterable[str]:
        """
        Handle the different file formats.

        Handling here allows the blob stores to be pretty dumb, they
        just need to be able to store and recall blobs.
        """
        stream = self.get_blob_stream(blob)

        if blob.endswith('.zstd'):
            import zstandard
            with zstandard.open(stream, 'r', encoding='utf8') as file:
                yield from file
        elif blob.endswith('.lzma'):
            import lzma
            with lzma.open(stream, 'rb') as file:
                yield from file
        elif blob.endswith('.parquet'):
            import pyarrow.parquet as pq
            table = pq.read_table(stream)
            for batch in table.to_batches():
                dict_batch = batch.to_pydict()
                for index in range(len(batch)):
                    # expecting a string, but this is a dict??
                    yield {k:v[index] for k,v in dict_batch.items()}
        else:  # assume text in lines format
            text = stream.read().decode('utf8')
            lines = text.splitlines()
            yield from [item for item in lines if len(item) > 0]


    def get_list_of_blobs(self):

        blobs = []
        for cycle_date in common.date_range(self.start_date, self.end_date):
            # build the path name
            cycle_path = pathlib.Path(paths.build_path(path=self.dataset, date=cycle_date))
            blobs += list(self.get_blobs_at_path(path=cycle_path))

        # work out if there's an as_at part
        as_ats = { self._extract_as_at(blob) for blob in blobs if 'as_at_' in blob }
        if as_ats:
            as_at = sorted(as_ats).pop()
            # the .ignore file means the frame shouldn't be used 
            while as_at + '/.ignore' in blobs:
                as_at = sorted(as_ats).pop()
            get_logger().debug(F"Reading from DataSet frame {as_at}")
            blobs = [blob for blob in blobs if as_at in blob]

        return blobs
