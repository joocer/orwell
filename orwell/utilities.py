import datetime
import json
from typing import Iterator


def select_fields(dic:dict, fields:list) -> dict:
    """
    Selects items from a row, if the row doesn't exist, None is used.
    """
    return {field: dic.get(field, None) for field in fields}

def select_all(x):
    return True

def generator_chunker(generator:Iterator, chunk_size:int) -> Iterator:
    chunk:list = []
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

def get_view_path(template:str="%store/%view/year_%Y/month_%m/day_%d/", 
                  store:str="02_INTERMEDIATE",
                  view:str="none",
                  date:datetime.datetime=None, 
                  extention:str=".jsonl") -> str:
    if not date:
        date = datetime.datetime.today()

    view_name = template
    view_name = view_name.replace('%store', store)
    view_name = view_name.replace('%view', view)
    view_name = view_name.replace('%ext', extention)
    view_name = view_name.replace('%date', '%Y-%m-%d')
    view_name = date.strftime(view_name)
    return view_name

def get_project_init():
    """
    This gets the current project by querying gcloud using the command line.
    
    This should be fixed for the life of the script and calculating is somewhat
    expensive, so we only ever want to work this out once.
    
    We assign get_project to an initializer, which gets the project name,
    creates a method which returns this value and reassigns get_project to that
    method.
    
    This is probably an anti-pattern.

    #nosec - inputs are fixed, almost impossible to inject
    """
    import subprocess       #nosec
    global get_project

    result = subprocess.run(['gcloud', 'config', 'get-value', 'project'], stdout=subprocess.PIPE) #nosec
    project = result.stdout.decode('utf8').rstrip('\n')
    get_project = lambda: project
    return project

get_project = get_project_init

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