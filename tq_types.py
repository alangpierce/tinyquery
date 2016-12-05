"""Defines the valid types. Currently we just uses strings to identify them.
"""

# TODO(Samantha): Structs.

INT = 'INTEGER'
FLOAT = 'FLOAT'
BOOL = 'BOOLEAN'
STRING = 'STRING'
# TODO: Investigate this further. At the very least, we need to make sure this
# doesn't end up in a table and gets converted to bool instead.
NONETYPE = 'NONETYPE'

TYPE_SET = set([INT, FLOAT, BOOL, STRING])
CAST_FUNCTION_MAP = {INT: int, FLOAT: float, BOOL: bool, STRING: str,
                     NONETYPE: lambda _: None, 'null': lambda _: None}

TYPE_TYPE = basestring
