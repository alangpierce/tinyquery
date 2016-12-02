#!/usr/bin/env python
"""Use real bigquery data to create a tinyquery table.

This makes it easier to generate tests for existing queries, since we don't
have to construct the data by hand.  Yay!

We assume that you've created application default gcloud credentials, and that
you have the project set to the one you want to use.

Usage: tools/make_test_data.py <dataset> <table>

This will print out python code suitable for creating the tinyquery table
object containing 5 rows of data from the specified bigquery table.
"""
import json
import subprocess
import sys


def json_bq_command(*args):
    return json.loads(subprocess.check_output(
        ['bq', '--format', 'prettyjson'] + list(args)))


def fetch_table_schema(dataset, table):
    return json_bq_command(
        'show', '%s.%s' % (dataset, table))['schema']['fields']


def sample_table_data(dataset, table):
    return json_bq_command(
        'head', '-n', '5', '%s.%s' % (dataset, table))


def get_column_data(rows, column):
    return [
        repr(row.get(column['name']))
        for row in rows
    ]


def make_column(column, data):
    return "('%s', context.Column(type='%s', mode='%s', values=[%s]))," % (
        column['name'],
        column['type'],
        column['mode'],
        ', '.join(data)
    )


def write_sample_table_code(dataset, table):
    schema = fetch_table_schema(dataset, table)
    rows = sample_table_data(dataset, table)
    indent = ' ' * 8
    column_lines = [
        indent + make_column(column,
                             get_column_data(rows, column))
        for column in schema
    ]
    return """tinyquery.table(
    '%s',
    %d,
    collections.OrderedDict([
%s
    ]))
""" % (table, len(rows), '\n'.join(column_lines))


if __name__ == '__main__':
    print write_sample_table_code(sys.argv[1], sys.argv[2])
