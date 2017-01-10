#!/usr/bin/env python
"""Use real bigquery data to create a tinyquery table.

This makes it easier to generate tests for existing queries, since we don't
have to construct the data by hand.  Yay!

We assume that you've created application default gcloud credentials, and that
you have the project set to the one you want to use.

For usage instructions, run `make_test_data.py --help`

This will print out a JSON formatted schema, a blank line, and then
newline-delimited JSON containing some data from the table.
"""
import argparse
import json
import subprocess


def json_bq_command(*args):
    return json.loads(subprocess.check_output(
        ['bq', '--format', 'json'] + list(args)))


def fetch_table_schema(dataset, table):
    return json_bq_command(
        'show', '%s.%s' % (dataset, table))['schema']['fields']


def sample_table_data(dataset, table, num_rows):
    return json_bq_command(
        'head', '-n', str(num_rows), '%s.%s' % (dataset, table))


def write_sample_table_code(dataset, table, num_rows):
    schema = fetch_table_schema(dataset, table)
    rows = sample_table_data(dataset, table, num_rows)
    return (json.dumps(schema), map(json.dumps, rows))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='fetch data from bigquery for use with tinyquery')
    parser.add_argument('dataset', type=str, help='the bigquery dataset')
    parser.add_argument('table', type=str, help='the bigquery table')
    parser.add_argument('-n', '--num-rows', help='number of rows to fetch',
                        default=5)
    args = parser.parse_args()
    schema, rows = write_sample_table_code(
        args.dataset, args.table, args.num_rows)
    print schema
    print ''
    for row in rows:
        print row
