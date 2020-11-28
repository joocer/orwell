




#def get_view_path(view, date=None, extention=".jsonl"):
#    if not date:
#        date = datetime.datetime.today()
#    view_name = re.sub('[^0-9a-zA-Z]+', '_', view).lower().rstrip('_').lstrip('_')
#    path = f"{view_name}/{date:%Y_%m}/{view_name}_{date:%Y_%m_%d}{extention}"
#    return path

def get_view_path(view, date=None, extention=".jsonl", store="02_INTERMEDIATE"):
    if not date:
        date = datetime.datetime.today()
    view_name = f"{store}/{view}/{view}_{date:%Y-%m-%d}{extention}"
    return view_name



def get_latest_blob(view, project=None, bucket=None, max_days=5):
    # if the project is None, use the current project
    if not project:
        project = get_project()

    # count backward from today for max_days days
    for cycle in range(max_days):
        cycle_date = datetime.datetime.today() - datetime.timedelta(cycle)
        blob_name = get_view_path(view, date=cycle_date)
        if get_blob(project, bucket, blob_name):
            return blob_name
    return None







def get_blob(project, bucket, blob_name):
    client = storage.Client(project=project)
    bucket = client.get_bucket(bucket)
    blob = bucket.get_blob(blob_name)
    return blob



def find_cves(string):
    tokens = re.findall(r"(?i)CVE.\d{4}-\d{4,7}", string) 
    result = []
    for token in tokens:
        token = token.upper().strip()
        token = token[:3] + '-' + token[4:]  # snort rules list cves as CVE,2009-0001
        result.append(token)
    return result



class temp_file(object):
    """
    A class to handle some of the repeat activities for dealing
    with temporary files.
    """
    
    def __init__(self, job_name, auditor=None):
        # work out a temp file name
        TEMP_PATH = os.path.expanduser('~/TEMP/')
        os.makedirs(TEMP_PATH, exist_ok=True)
        self.file_name = os.path.join(TEMP_PATH, job_name)
        
        # delete any previous instances of the file
        if os.path.exists(self.file_name):
            os.remove(self.file_name)

        # open the file for writing
        self.file = open(self.file_name, 'w', encoding='utf-8')

        # we're writing the records, so we can count them
        self.auditor = auditor


    def write_text_line(self, line):
        try:
            if self.auditor:
                self.auditor.records_written = self.auditor.records_written + 1
        except:
            pass
        
        line = str(line).rstrip('\n|\r') + '\n'
        self.file.write(line)


    def write_json_line(self, record):
        self.write_text_line(json.dumps(record, ensure_ascii=False))


    def save_to_bucket(self, project, bucket, blob_name):
        try:
            self.file.flush()
            self.file.close()
        except:
            pass
        
        client = storage.Client(project=project)
        bucket = client.get_bucket(bucket)
        path = blob_name.replace('%date', '%Y-%m-%d')
        blob = bucket.blob(datetime.datetime.today().strftime(path))
        blob.upload_from_filename(self.file_name)


    def __del__(self):
        self.file.close()
        if os.path.exists(self.file_name):
            os.remove(self.file_name)