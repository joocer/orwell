
import json



def select_fields(dic:dict, fields:list):
    """
    Selects items from a row, if the row doesn't exist, None is used.
    """
    return {field: dic.get(field, None) for field in fields}

def select_all(x):
    return True

def generator_chunker(generator, chunk_size:int):
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