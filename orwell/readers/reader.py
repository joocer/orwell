"""
Reader
"""
from typing import Callable
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


FORMATTERS = {
    "json": json_parser,
    "text": lambda x: x
}

class Reader():

    def __init__(self, reader:Callable=blob_reader, 
                       data_format:str="json",
                       limit:int=-1,
                       condition:Callable=select_all,
                       fields:list=['*'],
                       **kwargs): 
        self.reader = reader(**kwargs)
        self.format = data_format
        self.formatter = FORMATTERS.get(self.format.lower())
        if not self.formatter:
            raise TypeError(F"data format unsupported: {self.format}.")
        self.fields = fields.copy()
        self.condition = condition
        self.limit = limit

    """
    Iterable

    Use this class as an iterable:

        for line in Reader("file"):
            print(line)
    """
    def __iter__(self):
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
