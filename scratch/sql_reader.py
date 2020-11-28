"""
SQL Reader

A nowhere near fully featured SQL SELECT interface for
accessing the log.

SELECT [columns] FROM [dataset]

The following are not supported at the moment
* WHERE clauses
* DISTINCT
* COUNT
* SUM, MIN, MAX
* JOINS
"""


class SqlReader():
    def __init__(self): pass

    def estimate_complexity(self, statement): pass
        """
        complexity 
        """

    def calculate_execution_plan(self, statement): pass





import re

SQL = "SELECT MIN(username), password FROM TABLE WHERE (X == Y) AND (G != 1)"

def get_plus_one(parts, label):
    if label in parts:
        idx = parts.index(label)
        if (idx + 1) < len(parts):
            return parts[idx + 1]
    return None

def get_between(parts, start_marker, end_marker):
    start = end = 0
    if start_marker in parts:
        start = parts.index(start_marker) + 1
    if end_marker in parts:
        end = parts.index(end_marker)
    if start_marker != 0 and end_marker != 0:
        return parts[start:end]
    return ['*']

def get_after(parts, label):
    if label in parts:
        start = parts.index(label) + 1
        return parts[start:]
    return None

def build_predicate(parts):
    query = ' '.join(parts)
    parts = re.split('(\W)', query)
    parts = [p for p in parts if p not in ['', ' ']]
    parts = [('=' + parts[i-1] if parts[i] == '=' else parts[i]) for i,x in enumerate(parts)]
    return parts


COMPARISONS = {
    "==": lambda x, y: x == y,
    "!=": lambda x, y: x != y,
    ">": lambda x, y: x > y,
    ">=": lambda x, y: x >= y,
    "<": lambda x, y: x < y,
    "<=": lambda x, y: x <= y,
}

LOGIC = {
    "AND": lambda x, y: x and y,
    "OR": lambda x, y: x or y
}

def sql_parse(query):

    parts = re.split(' |,', query)
    parts = [p for p in parts if p]

    table = get_plus_one(parts, 'FROM')
    fields = get_between(parts, 'SELECT', 'FROM')
    predicate = build_predicate(get_after(parts, 'WHERE'))


    print({
        "fields": fields,
        "table": table,
        "filter": predicate
        })
    

sql_parse(SQL)
