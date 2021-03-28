"""
Test the file reader
"""
import datetime
import pytest
import os
import sys
sys.path.insert(1, os.path.join(sys.path[0], '..'))
from orwell.adapters.local import FileReader
from orwell.data import Reader
try:
    from rich import traceback
    traceback.install()
except ImportError:   # pragma: no cover
    pass

from orwell.logging import get_logger
get_logger().setLevel(5)

def test_can_find_files():
    """
    ensure we can find the test files
    the test folder has two files in it
    """
    # with a trailing /
    r = FileReader(dataset='tests/data/tweets/', raw_path=True)
    assert len(list(r.get_list_of_blobs())) == 2

    # without a trailing /
    r = FileReader(dataset='tests/data/tweets', raw_path=True)
    assert len(list(r.get_list_of_blobs())) == 2


def test_can_read_files():
    """ ensure we can read the test files """
    r = FileReader(dataset='tests/data/tweets/', raw_path=True)
    for file in r.get_list_of_blobs():
        for index, item in enumerate(r.get_records(file)):
            pass
        assert index == 24

def test_step_back():
    # step back through time
    r = Reader(
            inner_reader=FileReader,
            dataset='tests/data/dated/{date}',
            start_date=datetime.date(2021,1,1),
            end_date=datetime.date(2021,1,1),
            step_back_days=30)
    assert len(list(r)) == 50

def test_step_past():
    # step back through time
    r = Reader(
            inner_reader=FileReader,
            dataset='tests/data/dated/{date}',
            start_date=datetime.date(2021,1,1),
            end_date=datetime.date(2021,1,1),
            step_back_days=5)
    with pytest.raises(SystemExit):
        assert len(list(r)) == 0

if __name__ == "__main__":
    test_can_find_files()
    test_can_read_files()
    test_step_back()
    test_step_past()

    print('okay')
    