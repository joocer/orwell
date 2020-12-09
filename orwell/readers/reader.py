"""
Reader
"""
from typing import Callable, Union
import json
json_parser:Callable = json.loads
try:
    import orjson
    json_parser = orjson.loads
except ImportError: pass
try:
    import ujson
    json_parser = ujson.loads
except ImportError: pass
from ..utilities import select_all, select_fields
from .blob_reader import blob_reader
import xmltodict  # type:ignore
import logging

logger = logging.getLogger("GVA")

FORMATTERS = {
    "json": json_parser,
    "text": lambda x: x,
    "xml":  lambda x: xmltodict.parse(x)
}

class Reader():

    def __init__(
        self,
        reader:Callable=blob_reader, 
        data_format:str="json",
        limit:int=-1,
        condition:Callable=select_all,
        fields:list=['*'],
        deduplicate_on:Union[list,None]=None,
        **kwargs):
        """
        Reader accepts a method which iterates over a data source and provides
        functionality to filter, select and truncate records which are
        returned. The default reader is a GCS blob reader, a file system
        reader is also implemented.
        """

        self.reader = reader(**kwargs)
        self.format = data_format
        self.formatter = FORMATTERS.get(self.format.lower())
        if not self.formatter:
            raise TypeError(F"data format unsupported: {self.format}.")
        self.fields = fields.copy()
        self.condition:Callable = condition
        self.limit:int = limit
        self.seen_hashs:list = []
        self.deduplicate_on = deduplicate_on
        logger.debug(F"Reader(reader={reader.__name__})")

    """
    Iterable

    Use this class as an iterable:

        for line in Reader("file"):
            print(line)
    """
    def _select_columns(self, record, fields):
        """
        Selects a subset of fields from a dictionary
        """
        return { k: record.get(k, None) for k in fields }

    def _ordered(self, record):
        if isinstance(record, dict):
            return sorted((key, self._ordered(value)) for key, value in record.items())
        if isinstance(record, list):
            return sorted((self._ordered(c) for x in record), key=lambda item: '' if not item else item)
        return record

    def _is_duplicate(self, record):
        # select the columns
        record = self._select_columns(record, self.deduplicate_on)
        # sort the fields
        record = self._ordered(record)
        # convert to a string
        record = json.dumps(record)
        # hash it
        _hash = hash(record)
        # test
        is_duplicate = _hash in self.seen_hashs
        # add the record to the dupe set
        if not is_duplicate:
            self.seen_hashs.append(_hash)
        return is_duplicate

    def __iter__(self):
        self.deduplication = []
        return self

    def __next__(self):
        """
        This wraps the primary filter and select logic
        """
        if self.limit == 0:
            raise StopIteration()
        self.limit -= 1
        while True:
            record = self.reader.__next__()
            record = self.formatter(record)
            if self.deduplicate_on and self._is_duplicate(record):
                continue
            if not self.condition(record):
                continue
            if self.fields != ['*']:
                record = select_fields(record, self.fields)
            return record

    """
    Context Manager

    Use this class using the 'with' statement:

        with Reader("file") as r:
            line = r.read_line()
            while line:
                print(line)
    """
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def read_line(self):
        try:
            return self.__next__()
        except StopIteration:
            return None

    """
    Exports
    """
    def to_pandas(self):
        try:
            import pandas as pd # type:ignore
        except ImportError:
            raise Exception("Pandas must be installed to use 'to_pandas'")
        return pd.DataFrame(self)
