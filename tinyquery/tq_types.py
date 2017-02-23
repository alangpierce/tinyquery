"""Defines the valid types. Currently we just uses strings to identify them.
"""
import arrow

# TODO(Samantha): Structs.

INT = 'INTEGER'
FLOAT = 'FLOAT'
BOOL = 'BOOLEAN'
STRING = 'STRING'
TIMESTAMP = 'TIMESTAMP'
# TODO: Investigate this further. At the very least, we need to make sure this
# doesn't end up in a table and gets converted to bool instead.
NONETYPE = 'NONETYPE'

TYPE_SET = set([INT, FLOAT, BOOL, STRING, TIMESTAMP])
INT_TYPE_SET = set([INT, BOOL, TIMESTAMP])
NUMERIC_TYPE_SET = set([FLOAT]) | INT_TYPE_SET
CAST_FUNCTION_MAP = {
    INT: int,
    FLOAT: float,
    BOOL: bool,
    STRING: unicode,
    TIMESTAMP: lambda val: arrow.get(val).to('UTC').naive,
    NONETYPE: lambda _: None,
    'null': lambda _: None
}
DATETIME_TYPE_SET = set([INT, STRING, TIMESTAMP])

TYPE_TYPE = basestring
