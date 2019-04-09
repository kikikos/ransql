# Ransql Parser, based on moz sql parser


## Requirements: 
```
$pip3 install mo-future
$pip3 install pyparsing
$pip3 install websockets
$pip3 install kafka-python

```

## Usage example
```python
from moz_sql_parser import parse
import json

res1 = json.dumps(parse("select count(1) from jobs"))
res2 = json.dumps(parse("select a as hello, b as world from jobs"))

print(res1)
print(res2)

```

The `SELECT` clause is an array of objects containing `name` and `value` properties. 

## More examples: 
```
res = json.dumps(ransql_parse("select count(1)  from jobs TIME 1 TO app(websocket,locathost,5000);"))
print(res)

res = json.dumps(ransql_parse("select a as hello, b as world from jobs TIME 1 TO app;"))
print(res)
   
res = json.dumps(ransql_parse("SELECT COUNT(arrdelay)  FROM table1 LIMIT 10 TIME 1 TO app(websocket,locathost,5000);"))
print(res)

res = json.dumps(ransql_parse("SELECT AVG(col) FROM table1 LIMIT 10 TIME 1 TO app(websocket,locathost,5000);"))
print(res)

res = json.dumps(ransql_parse("SELECT MAX(arrdelay) FROM table1 LIMIT (1,10) TIME 1 TO app(websocket,locathost,5000);"))
print(res)

res = json.dumps(ransql_parse("SELECT MIN(arrdelay) FROM table1 LIMIT 10 TIME 1 TO app(websocket,locathost,5000);"))
print(res)

res = json.dumps(ransql_parse("SELECT SUM(arrdelay) FROM table1 LIMIT 10 TIME 1 TO app(websocket,locathost,5000);"))
print(res)
     
res = json.dumps(ransql_parse("SELECT AVG(total_pdu_bytes_rx) FROM eNB1 WHERE crnti=0 TIME 1 TO app(websocket,locathost,5000);"))
print(res)

res = json.dumps(ransql_parse("SELECT ADD(ul, dl) as total FROM eNB ORDER BY total DESC LIMIT (1,10) TIME 1 TO app(websocket,locathost,5000);"))
print(res)
```

## Run Tests

See [the tests directory](https://github.com/mozilla/moz-sql-parser/tree/dev/tests) for instructions running tests, or writing new ones.

## More about implementation

SQL queries are translated to JSON objects: Each clause is assigned to an object property of the same name.

    
    # SELECT * FROM dual WHERE a>b ORDER BY a+b
    {
        "select": "*", 
        "from": "dual", 
        "where": {"gt": ["a", "b"]}, 
        "orderby": {"value": {"add": ["a", "b"]}}
    }
        
Expressions are also objects, but with only one property: The name of the operation, and the value holding (an array of) parameters for that operation. 

    {op: parameters}

and you can see this pattern in the previous example:

    {"gt": ["a","b"]}


### Notes

* Uses the glorious `pyparsing` library (see https://github.com/pyparsing/pyparsing) to define the grammar, and define the shape of the tokens it generates. 
* [sqlparse](https://pypi.python.org/pypi/sqlparse) does not provide a tree, rather a list of tokens. 
