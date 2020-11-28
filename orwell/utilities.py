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
from .readers.base_reader import BaseReader


def dict_reader(reader: BaseReader):
    for item in reader:
        yield json.loads(item)

def generator_chunker(generator, chunk_size: int):
    chunk = []
    for item in generator:
        if len(chunk) >= chunk_size:
            yield chunk
            chunk = [item]
        else:
            chunk.append(item)
    if chunk:
        yield chunk

def read_csv_lines(filename):
    with open(filename, "r", encoding="utf-8") as csvfile:
        datareader = csv.reader(csvfile)
        headers = next(datareader)
        row = next(datareader)
        while row:
            yield dict(zip(headers, row))
            try:
                row = next(datareader)
            except StopIteration:
                row = None