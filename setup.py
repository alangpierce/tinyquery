#!/usr/bin/env python

from setuptools import setup

setup(
    name='Tinyquery',
    version='1.0',
    description='In-memory test stub for bigquery',
    author='Khan Academy',
    url='https://github.com/Khan/tinyquery',
    packages=['tinyquery'],
    install_requires=['arrow', 'ply'])
