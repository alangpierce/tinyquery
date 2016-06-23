"""Defines the valid types. Currently we just uses strings to identify them.

In bigquery, types are specified as nullable or required. In tinyquery, we
make them all nullable for simplicity.
"""

INT = 'INTEGER'
FLOAT = 'FLOAT'
BOOL = 'BOOLEAN'
STRING = 'STRING'
# TODO: Investigate this further. At the very least, we need to make sure this
# doesn't end up in a table and gets converted to bool instead.
NONETYPE = 'NONETYPE'

TYPE_TYPE = basestring
