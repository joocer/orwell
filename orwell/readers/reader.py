"""
Reader
"""
from file_reader import file_reader
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


FORMATTERS = {
    "json": json.loads,
    "text": lambda x: x
}

def select_all(x):
    return True

class Reader():

    def __init__(self, reader=file_reader, 
                       data_format:str="json",
                       limit:int=-1,
                       condition:callable=select_all,
                       fields=['*'],
                       **kwargs): 
        self.reader = reader(**kwargs)
        self.format = data_format
        self.formatter = FORMATTERS.get(self.format.lower())
        if not self.formatter:
            self._unknown_format(self.format)
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
                record = self._select_fields(record, self.fields)
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
            import pandas as pd
        except ImportError:
            raise Exception("Pandas must be installed to use 'to_pandas'")
        return pd.DataFrame(self)

    """
    Error Handlers
    """
    def _unknown_format(self, format_):
        raise Exception(F"{format_} is an unknown file format")


    """
    Helpers
    """
    def _select_fields(self, dic:dict, fields:list):
        """
        Selects items from a row, if the row doesn't exist, None is used.
        """
        return {field: dic.get(field, None) for field in fields}



from pprint import pprint
r = Reader(file_name="small.jsonl", limit=10, condition=lambda x: int(x['followers']) < 10, fields=['username', 'location'])

#with r as i:
#    line = i.read_line()
#    while line:
#        print(line)
#        line = i.read_line()
