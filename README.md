# orwell

## What Is It?
A data reader and writer helper library.

The reader can read across multiple blobs in a GCS bucket with filter, select and and name pattern matching functionality.

The writer writes to buckets, with tempfile caching, and data validation functionality.

## How Do I Use It?

**READER**
~~~
critical_errors = Reader(
        select=['server', 'error_level'],
        from_path="error_logs/year_%Y/month_%m/day_%d/",
        where=lambda r: r['error_level'] == 'critical',
        date_range=(datetime.date(2020, 1, 1), None)
    )
critical_errors.to_pandas()
~~~

**WRITER**
TBC

## How Do I Get It?
~~~
pip install --upgrade git+https://github.com/joocer/orwell
~~~
or in your requirements.txt
~~~
git+https://github.com/joocer/orwell
~~~
