#!/usr/bin/env python
#from mo_future import text_type
from moz_sql_parser import parse
import json


def main():
    res = json.dumps(parse("select count(1) from jobs to s"))
    print(res)
    res = json.dumps(parse("select a as hello, b as world from jobs"))
    print(res)


if __name__ == "__main__":
    main()