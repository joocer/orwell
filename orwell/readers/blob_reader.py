try:
    from google.cloud import storage  # type:ignore
except ImportError: pass
from ..utilities import get_view_path, get_project
import lzma
import datetime

def blob_reader(view='', 
                project=None, 
                bucket=None, 
                extention=".jsonl",
                start_date=None, 
                end_date=None, 
                chunk_size=16*1024*1024,
                template="%store/%view/year_%Y/month_%m/day_%d/",
                store="02_INTERMEDIATE",
                **kwargs):
    """
    Blob reader, will iterate over as set of blobs in a path.
    """
    # if dates aren't provided, use today
    if not end_date:
        end_date = datetime.date.today()
    if not start_date:
        start_date = datetime.date.today()

    # if the project is None, use the current project
    if not project:
        project = get_project()

    # cycle through the days, loading each days' file
    for cycle in range(int((end_date - start_date).days) + 1):
        cycle_date = start_date + datetime.timedelta(cycle)
        cycle_path = get_view_path(view=view, date=cycle_date, store=store, extention=extention, template=template)
        blobs_at_path = find_blobs_at_path(project=project, bucket=bucket, path=cycle_path, extention=extention)
        for blob in blobs_at_path:
            reader = _inner_blob_reader(blob_name=blob.name, project=project, bucket=bucket, chunk_size=chunk_size)
            yield from reader


def find_blobs_at_path(project, bucket, path, extention):
    client = storage.Client(project=project)
    bucket = client.get_bucket(bucket)
    blobs = client.list_blobs(bucket_or_name=bucket, prefix=path)
    blobs = [blob for blob in blobs if blob.name.endswith(extention)]
    yield from blobs


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
        cursor += chunk_size   # was previously +len(chunk)
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


def get_blob(project, bucket, blob_name):
    if not project:
        project = get_project()
    client = storage.Client(project=project)
    bucket = client.get_bucket(bucket)
    blob = bucket.get_blob(blob_name)
    return blob