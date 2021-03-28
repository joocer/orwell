"""
LZMA compression

Command Line handing adapted from:
https://github.com/joocer/trosi
http://www.apache.org/licenses/LICENSE-2.0
"""
from lzma import LZMAFile
import sys
import os

def safe_get(arr, index, default=None):
    return arr[index] if index < len(arr) else default

def get_input_stream():
    """
    If first paramter is - use standard in, if it's a existing
    file, open an use that
    Return a stream (or None) for processing.
    """
    if input_filename == '-':
        if not sys.stdin.isatty():
            return sys.stdin    
    if os.path.isfile(input_filename):
        return open(input_filename, 'r')
    return None

def get_parameter_value(label):
    """
    Look for labelled parameters in the command line, the pattern is:
        -a value
    Where '-a' is the label, and 'value' is the returned value.
    """
    if label in sys.argv:
        idx = sys.argv.index(label)
        return safe_get(sys.argv, idx + 1)
    return None

# Initialize variables
 # be lenient on people asking for help
show_help = '-?' in sys.argv or '-h' in sys.argv or '--help' in sys.argv
no_headers = '-no' in sys.argv
out_file = get_parameter_value('-o')
input_filename = safe_get(sys.argv, 1, '')
input_stream = get_input_stream()

# if help requested, display help and exit with no error
if show_help:
    print("Usage: lzma [FILE] [-o OUTPUTFILE] [--help]")
    print("Compress INPUTFILE with LZMA")
    print("Example: lzma data.csv")
    print()
    print("  FILE\t\tfile to compress")
    print("  -o\t\tfile to save to")
    print("  -h, --help\tdisplay this help text and exit")
    print()
    print("When FILE is -, standard input in read.")
    print("When no OUTPUTFILE is specified, .lzma is appended to the FILE name")
    sys.exit(0)

# if we have nothing to processes, display an error and how to get help
if not input_stream:
    print("No input specified.")
    print(f"Try '{safe_get(sys.argv, 0)} --help' for usage information.")
    sys.exit(1)

# if we have nothing to processes, display an error and how to get help
if not out_file and not input_filename:
    print("No output file specified.")
    print(f"Try '{safe_get(sys.argv, 0)} --help' for usage information.")
    sys.exit(1)

if not out_file:
    out_file = input_filename + '.lzma'

with open(out_file, 'wb') as python_file:
    with LZMAFile(python_file, 'wb') as lzma_file:
        for line in input_stream.readlines():
            lzma_file.write(line.encode())
