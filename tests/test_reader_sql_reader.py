import os
import sys
sys.path.insert(1, os.path.join(sys.path[0], '..'))
from orwell.data import Reader, BatchWriter
from orwell.adapters.local import FileWriter, FileReader
from orwell.data.readers.internals.experimental_sql_reader import SqlReader
from orwell.logging import get_logger
import shutil
try:
    from rich import traceback
    traceback.install()
except ImportError:   # pragma: no cover
    pass

get_logger().setLevel(5)

def do_writer():
    w = BatchWriter(
        inner_writer=FileWriter,
        dataset='_temp/twitter',
        raw_path=True
    )
    r = Reader(
        inner_reader=FileReader,
        dataset='tests/data/tweets',
        raw_path=True
    )
    for tweet in r:
        w.append(tweet)
    w.finalize()


def test_most_basic_sql():

    shutil.rmtree('_temp', ignore_errors=True)
    do_writer()
    s = SqlReader(
            "SELECT * FROM _temp.twitter",
            inner_reader=FileReader,
            raw_path=True)
    findings = list(s)
    assert len(findings) == 50, len(findings)
    shutil.rmtree('_temp', ignore_errors=True)


def test_sql():

    shutil.rmtree('_temp', ignore_errors=True)
    do_writer()
    s = SqlReader(
            "SELECT username FROM _temp.twitter WHERE sentiment != 0",
            inner_reader=FileReader,
            raw_path=True)
    findings = list(s)
    assert len(findings) == 39, len(findings)
    shutil.rmtree('_temp', ignore_errors=True)


if __name__ == "__main__":
    test_sql()
    test_most_basic_sql()

    print('okay')
