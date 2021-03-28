import os
import sys
sys.path.insert(1, os.path.join(sys.path[0], '..'))
from orwell.errors.render_error_stack import _build_error_stack, RenderErrorStack
try:
    from rich import traceback
    traceback.install()
except ImportError:  # pragma: no cover
    pass


def i_fail():
    raise NotImplementedError('This is a forced error to test the error stack')

def create_stack():
    variable = True
    i_fail()

def test_error_stack():

    stack = ''
    try:
        create_stack()
    except:
        stack = RenderErrorStack()

    # don't check the entire stack, look for key words
    assert 'i_fail' in stack
    assert 'This is a forced error to test the error stack' in stack, stack
    assert 'test_error_stack' in stack

    print(stack)

if __name__ == "__main__":
    test_error_stack()

    print("okay")