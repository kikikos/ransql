#!/usr/bin/env python
#from mo_future import text_type
from moz_sql_parser import parse
import json


def main():
    res = json.dumps(parse("select count(1)  from jobs as jb to app;"))
    #print(res)
    res = json.dumps(parse("select a as hello, b as world from jobs"))
    #print(res)
    """
    SELECT SUM(O.TotalPrice), C.FirstName, C.LastName
    FROM [Order] O JOIN Customer C 
    ON O.CustomerId = C.Id
    GROUP BY C.FirstName, C.LastName
    ORDER BY SUM(O.TotalPrice) DESC

    """
    res = json.dumps(parse("SELECT COUNT(arrdelay)  FROM table1 LIMIT 10 ;"))
    print(res)


    res = json.dumps(parse("SELECT AVG(col) FROM table1 LIMIT 10 ;"))
    print(res)

    res = json.dumps(parse("SELECT MAX(arrdelay) FROM table1 LIMIT (1,10);"))
    print(res)
    res = json.dumps(parse("SELECT MIN(arrdelay) FROM table1 LIMIT 10;"))
    print(res)

    res = json.dumps(parse("SELECT SUM(arrdelay) FROM table1 LIMIT 10;"))
    print(res)

    """
    TODO: time 0.1, and TO app..
    """

    res = json.dumps(parse("SELECT AVG(total_pdu_bytes_rx) FROM eNB1 WHERE crnti=0 ;"))
    print(res)

    res = json.dumps(parse("SELECT ADD(ul, dl) as total FROM eNB ORDER BY total DESC LIMIT (1,10);"))
    print(res)
if __name__ == "__main__":
    main()