try:
    from google.cloud import storage
except ImportError: pass
from ..utilities import get_view_path
import lzma
import datetime


def get_blob(project, bucket, blob_name):
    if not project:
        project = get_project()
    client = storage.Client(project=project)
    bucket = client.get_bucket(bucket)
    blob = bucket.get_blob(blob_name)
    return blob

def _inner_blob_reader(project, bucket, blob_name, chunk_size=16*1024*1024, delimiter='\n'):
    """
    Reads lines from an arbitrarily long blob, line by line.

    Automatically detecting if the blob is compressed.
    """
    blob = get_blob(project=project, bucket=bucket, blob_name=blob_name)
    if blob:
        blob_size = blob.size
    else:
        blob_size = 0

    carry_forward = ''
    cursor = 0
    while (cursor < blob_size):
        chunk = _download_chunk(blob=blob, start=cursor, end=min(blob_size, cursor+chunk_size-1))
        cursor += len(chunk_size)   # was previously +len(chunk)
        chunk = chunk.decode('utf-8')
        # add the last line from the previous cycle
        chunk += carry_forward
        lines = chunk.split(delimiter)
        # the list line is likely to be incomplete, save it to carry forward
        carry_forward = lines.pop()
        yield from lines
    if len(carry_forward) > 0:
        yield carry_forward

def _download_chunk(blob, start, end):
    """
    Detects if a chunk is compressed by looking for a magic string
    """
    chunk = blob.download_as_string(start=start, end=end) 
    if chunk[:5] == "LZMA:":
        try:
            return lzma.decompress(chunk)
        except lzma.LZMAError:
            # if we fail maybe we're not compressed
            pass 
    return chunk


def get_project_init():
    """
    This gets the current project by querying gcloud using the
    command line.
    
    This should be fixed for the life of the script and calculating
    is somewhat expensive, so we only ever want to work this out 
    once.
    
    We assign get_project to an initializer, which gets the project
    name, creates a method which returns this value and reassigns
    get_project to that method.
    
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



def bytes_to_int(bytes):
    result = 0
    for b in bytes:
        result = result * 256 + int(b)
    return result

def int_to_bytes(value, length):
    result = []
    for i in range(0, length):
        result.append(value >> (i * 8) & 0xff)
    result.reverse()
    return bytes(result)



def blob_reader(view, 
                        project=None, 
                        bucket=None, 
                        extention=".jsonl",
                        start_date=None, 
                        end_date=None, 
                        chunk_size=16*1024*1024,
                        store="02_INTERMEDIATE"):
    """
    Build a virtual view across multiple files.
    """

    # if dates aren't provoded, use today
    if not end_date:
        end_date = datetime.date.today()
    if not start_date:
        start_date = datetime.date.today()

    # if the project is None, use the current project
    if not project:
        project = get_project()
        
    print(start_date, end_date, end_date - start_date, range(int((end_date - start_date).days) + 1))
        
    # cycle through the days, loading each days' file
    for cycle in range(int((end_date - start_date).days) + 1):
        cycle_date = start_date + datetime.timedelta(cycle)
        cycle_file_location = get_view_path(view, cycle_date, store=store, extention=extention)

        reader = blob_reader(project, bucket, cycle_file_location, chunk_size=chunk_size)
        yield from reader
