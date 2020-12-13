import datetime
from ..helpers import BlobPaths
try:
    from google.cloud import storage
except ImportError:
    pass

def blob_writer(
        project: str = None,
        path: str = None):


    bucket, gcs_path, filename, extention = BlobPaths.split_filename(path)

    client = storage.Client(project=project)
    bucket = client.get_bucket(bucket)
    path = path.replace('%date', '%Y-%m-%d')
    blob = bucket.blob(datetime.datetime.today().strftime(path))
    blob.upload_from_filename(source_file)
