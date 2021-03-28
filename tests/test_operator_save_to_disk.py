import os
import sys
import shutil
sys.path.insert(1, os.path.join(sys.path[0], '..'))
from orwell.operators.local import SaveSnapshotToDiskOperator
try:
    from rich import traceback
    traceback.install()
except ImportError:
    pass
 

def test_save_to_disk_operator():

    shutil.rmtree('_temp', ignore_errors=True)

    n = SaveSnapshotToDiskOperator(
            dataset="_temp/",
            format='zstd')
    n.execute(data={"this":"is", "a":"record"}, context={})
    n.finalize()

    assert os.path.exists("_temp")

    shutil.rmtree('_temp', ignore_errors=True)
    

if __name__ == "__main__":
    test_save_to_disk_operator()

    print('okay')
