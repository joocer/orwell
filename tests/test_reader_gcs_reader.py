import datetime
import pytest
import os
import sys
sys.path.insert(1, os.path.join(sys.path[0], '..'))
from orwell.adapters.google import GoogleCloudStorageReader
try:
    from rich import traceback
    traceback.install()
except ImportError:   # pragma: no cover
    pass


def test_blockers():

    # project is required
    with pytest.raises( (ValueError, TypeError) ):
        r = GoogleCloudStorageReader(dataset='path')

    # path is required
    with pytest.raises( (ValueError, TypeError) ):
        r = GoogleCloudStorageReader(project='project')


if __name__ == "__main__":
    test_blockers()

    print('okay')
    