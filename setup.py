# encoding: utf-8
# THIS FILE IS AUTOGENERATED!
from __future__ import unicode_literals
from setuptools import setup
setup(
    description=str(u'Extract Parse Tree from SQL'),
    license=str(u'MPL 2.0'),
    author=str(u'Kyle Lahnakoski'),
    author_email=str(u'kyle@lahnakoski.com'),
    long_description_content_type=str(u'text/markdown'),
    include_package_data=True,
    classifiers=["Development Status :: 3 - Alpha","Topic :: Software Development :: Libraries","Topic :: Software Development :: Libraries :: Python Modules","Programming Language :: SQL","Programming Language :: Python :: 2.7","Programming Language :: Python :: 3.6","License :: OSI Approved :: Mozilla Public License 2.0 (MPL 2.0)"],
    install_requires=["mo-future>=2.44.19084","pyparsing"],
    version=str(u'2.44.19084'),
    url=str(u'https://github.com/mozilla/moz-sql-parser'),
    zip_safe=True,
    packages=["moz_sql_parser"],
    long_description=str(u'# Moz SQL Parser\n\nLet\'s make a SQL parser so we can provide a familiar interface to non-sql datastores!\n\n\n|Branch      |Status   |\n|------------|---------|\n|master      | [![Build Status](https://travis-ci.org/mozilla/moz-sql-parser.svg?branch=master)](https://travis-ci.org/mozilla/moz-sql-parser) |\n|dev         | [![Build Status](https://travis-ci.org/mozilla/moz-sql-parser.svg?branch=dev)](https://travis-ci.org/mozilla/moz-sql-parser)    |\n\n\n## Problem Statement\n\nSQL is a familiar language used to access databases. Although, each database vendor has its quirky implementation, the average developer does not know enough SQL to be concerned with those quirks. This familiar core SQL (lowest common denominator, if you will) is useful enough to explore data in primitive ways. It is hoped that, once programmers have reviewed a datastore with basic SQL queries, and they see the value of that data, they will be motivated to use the datastore\'s native query format.\n\n## Objectives\n\nThe primary objective of this library is to convert some subset of [SQL-92](https://en.wikipedia.org/wiki/SQL-92) queries to JSON-izable parse trees. A big enough subset to provide superficial data access via SQL, but not so much as we must deal with the document-relational impedance mismatch.\n\n## Non-Objectives \n\n* No plans to provide update statements, like `update` or `insert`\n* No plans to expand the language to all of SQL:2011\n* No plans to provide data access tools \n\n\n## Project Status\n\nThere are [over 160 tests, all passing](https://github.com/mozilla/moz-sql-parser/tree/dev/tests). This parser is good enough for basic usage, including inner queries.\n\nYou can see the parser in action at [https://sql.telemetry.mozilla.org/](https://sql.telemetry.mozilla.org/) while using the ActiveData datasource\n\n## Install\n\n    pip install moz-sql-parser\n\n## Usage\n\n    >>> from moz_sql_parser import parse\n    >>> import json\n    >>> json.dumps(parse("select count(1) from jobs"))\n    \'{"from": "jobs", "select": {"value": {"count": {"literal": 1}}}}\'\n    \nEach SQL query is parsed to an object: Each clause is assigned to an object property of the same name. \n\n    >>> json.dumps(parse("select a as hello, b as world from jobs"))\n    \'{"from": "jobs", "select": [{"name": "hello", "value": "a"}, {"name": "world", "value": "b"}]}\'\n\nThe `SELECT` clause is an array of objects containing `name` and `value` properties. \n\n## Run Tests\n\nSee [the tests directory](https://github.com/mozilla/moz-sql-parser/tree/dev/tests) for instructions running tests, or writing new ones.\n\n## More about implementation\n\nSQL queries are translated to JSON objects: Each clause is assigned to an object property of the same name.\n\n    \n    # SELECT * FROM dual WHERE a>b ORDER BY a+b\n    {\n        "select": "*",\n        "from": "dual"\n        "where": {"gt": ["a","b"]},\n        "orderby": {"add": ["a", "b"]}\n    }\n        \nExpressions are also objects, but with only one property: The name of the operation, and the value holding (an array of) parameters for that operation. \n\n    {op: parameters}\n\nand you can see this pattern in the previous example:\n\n    {"gt": ["a","b"]}\n\n\n### Notes\n\n* Uses the glorious `pyparsing` library (see https://github.com/pyparsing/pyparsing) to define the grammar, and define the shape of the tokens it generates. \n* [sqlparse](https://pypi.python.org/pypi/sqlparse) does not provide a tree, rather a list of tokens. \n'),
    name=str(u'moz-sql-parser')
)
