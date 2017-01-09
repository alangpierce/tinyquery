#!/usr/bin/env python
"""Use real bigquery data to create a tinyquery table.

This makes it easier to generate tests for existing queries, since we don't
have to construct the data by hand.  Yay!

We assume that you've created application default gcloud credentials, and that
you have the project set to the one you want to use.

Usage: tools/make_test_data.py <dataset> <table>

This will print out a JSON formatted schema, a blank line, and then
newline-delimited JSON containing some data from the table.
"""
import json
import subprocess
import sys


def json_bq_command(*args):
    return json.loads(subprocess.check_output(
        ['bq', '--format', 'json'] + list(args)))


def fetch_table_schema(dataset, table):
    return json_bq_command(
        'show', '%s.%s' % (dataset, table))['schema']['fields']


def sample_table_data(dataset, table):
    return json_bq_command(
        'head', '-n', '5', '%s.%s' % (dataset, table))


def write_sample_table_code(dataset, table):
    schema = fetch_table_schema(dataset, table)
    rows = sample_table_data(dataset, table)
    return (json.dumps(schema), map(json.dumps, rows))


if __name__ == '__main__':
    schema, rows = write_sample_table_code(sys.argv[1], sys.argv[2])
    print schema
    print ''
    for row in rows:
        print row
