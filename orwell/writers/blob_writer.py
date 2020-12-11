
def blob_writer(

):
    pass




def save_file_to_bucket(source_file, project, bucket, path):
    # to be deprecated
    """
    Copy a local file to a storage bucket
    
    Parameters:
        source_file: file to be copied
        bucket_name: destination storage bucket
        destination_file: destination file within bucket, including any pseudo path
    """
    client = storage.Client(project=project)
    bucket = client.get_bucket(bucket)
    path = path.replace('%date', '%Y-%m-%d')
    blob = bucket.blob(datetime.datetime.today().strftime(path))
    blob.upload_from_filename(source_file)