"""
JSON LISTS

A group of functions to assist with handling lists of dictionaries,
which is how JSONL files will tend to be handled.

You would use these methods instead of something like Pandas if you
are dealing with potentially huge datasets and only require to be able to 
do simple processes on the dataset, row at a time.

Methods provide a limited approximation of SQL functionality:

SELECT - select_json_list
UNION  - union_json_lists
WHERE  - select_json_list
JOIN   - join_json_lists

All methods return generators, which mean they can only be iterated
once, to iterate again they need to be regenerated. create_index is
the only exception, as the index is for look-ups rather than
iteration. If you know you're going to need to iterate more than 
once, you can use list() or similar to cache the values, however
this may cause problems if the list is large.



Example usage:

# open two jsonl files
list1 = open_json_list_file('list1.jsonl')
list2 = open_json_list_file('list2.jsonl')

# filter the two lists
filtered_list1 = select_json_list(list1, ['id', 'name'], lambda x: x['height'] > 2)
filtered_list2 = select_json_list(list2, ['id', 'last_seen'])

# join the two lists on id
for record in join_json_lists(filtered_list1, filtered_list2, 'id'):
    print(record)

"""

import warnings
try:
    import ujson as json
except ImportError:
    import json


INNER_JOIN = 1
LEFT_JOIN = 2


def _select_all(dummy):
    """
    Returns True
    """
    return True

def select_record_fields(dic, fields):
    """
    Selects a subset of fields from a dictionary
    """
    return { k: dic.get(k, None) for k in fields }


def join_json_lists(left, right, column, join_type=INNER_JOIN):
    """
    Iterates over the left table, matching records fron the right 
    table.

    INNER_JOIN, the default, will discard records unless they appear 
    in both tables, LEFT_JOIN will keep all the records fron the 
    left table and add records for the right table if a match is 
    found.

    It is recommended that the left table be the larger of the two 
    tables as the right table is loaded into memory to perform the
    matching and look ups.

    NOTES:
    - where columns are in both tables - I don't know what happens.
    - resultant records may have inconsistent columns (same as 
      source lists)

    Approximate SQL:

    SELECT * FROM left JOIN right ON left.column = right.column
    """
    index = create_index(right, column)
    for record in left:
        value = record.get('QID')
        if index.get(value):
            yield { **record, **index[value] }
        elif join_type == LEFT_JOIN:
            yield record


def union_json_lists(json_list_1, json_list_2):
    """
    Append the records from a set of lists together, doesn't 
    ensure columns align.

    Approximate SQL:

    SELECT * FROM json_list_1
    UNION
    SELECT * FROM json_list_2
    """
    for record in json_list_1:
        yield record
    for record in json_list_2:
        yield record


def create_index(json_list, index_column):
    """
    Create an index of a file to speed up look-ups.
    """
    index = { }
    for record in json_list:
        index_value = record[index_column]
        index[index_value] = record
    return index


def select_json_list(json_list, columns=['*'], condition=_select_all):
    """
    Scan a json lines blob, filtering rows and selecting columns.

    Basic implementation of SQL SELECT statement for a single table

    Approximate SQL:

    SELECT columns FROM json_list WHERE condition
    """
    for record in json_list:
        if condition(record):
            if columns != ['*']:
                record = select_record_fields(record, columns)
            yield record


def set_column(json_list, column, setter):
    """
    Performs set_value on each row in a set
    """
    for record in json_list:
        yield set_value(record, column, setter)


def set_value(record, column, setter):
    """
    Sets the value of a column to either a fixed value or as the
    result of a function which recieves the row as a parameter
    """
    if type(setter).__name__ == 'function':
        record[column] = setter(record)
    else:
        record[column] = setter
    return record



def save_as_csv(json_list, filename, columns=['first_row'], pipe=False):
    """
    Saves a json_list as a CSV

    By default it gets the list of columns from the first_row, otherwise these
    columns can be manually specified. Missing values get set to None, columns
    are trimmed to the specified set of columns (or first row's set).
    """
    import csv

    with open (filename, 'w', encoding='utf8', newline='') as file:
        
        # get the first record
        record = json_list.__next__()

        # get the columns from the record
        if columns==['first_row']:
            columns=record.keys()
        
        # write the headers
        csv_file = csv.DictWriter(file, fieldnames=columns)
        csv_file.writeheader()

        # cycle the rest of the file
        while record:
            record = select_record_fields(record, columns)
            csv_file.writerow(record)
            record = json_list.__next__()

