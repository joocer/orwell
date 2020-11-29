import datetime
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

#def get_view_path(view, date=None, extention=".jsonl"):
#    if not date:
#        date = datetime.datetime.today()
#    view_name = re.sub('[^0-9a-zA-Z]+', '_', view).lower().rstrip('_').lstrip('_')
#    path = f"{view_name}/{date:%Y_%m}/{view_name}_{date:%Y_%m_%d}{extention}"
#    return path

def get_view_path(template="%store/%view/year_%Y/month_%m/day_%d/", 
                  store="02_INTERMEDIATE",
                  view="none",
                  date=None, 
                  extention=".jsonl"):
    if not date:
        date = datetime.datetime.today()

    view_name = template
    view_name = view_name.replace('%store', store)
    view_name = view_name.replace('%view', view)
    view_name = view_name.replace('%ext', extention)
    view_name = view_name.replace('%date', '%Y-%m-%d')
    view_name = date.strftime(view_name)
    return view_name


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