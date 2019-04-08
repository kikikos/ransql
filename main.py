#!/usr/bin/env python
#from mo_future import text_type
from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer
from moz_sql_parser import parse
import json
import SocketServer


class HttpServerHandler(BaseHTTPRequestHandler):
    def _set_headers(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.end_headers()

    def do_GET(self):
        self._set_headers()
        self.wfile.write("<html><body><h1>hi!</h1></body></html>")

    def do_HEAD(self):
        self._set_headers()
        
    def do_POST(self):
        # Doesn't do anything with posted data
        self._set_headers()
        self.wfile.write("<html><body><h1>POST!</h1></body></html>")
        
def run_http(server_class=HTTPServer, handler_class=HttpServerHandler, port=8888):
    server_address = ('', port)
    httpd = server_class(server_address, handler_class)
    print 'Starting httpd...'
    httpd.serve_forever()


def main():
    res = json.dumps(parse("select count(1)  from jobs TIME 1 TO app(websocket,locathost,5000);"))
    print(res)
    res = json.dumps(parse("select a as hello, b as world from jobs TIME 1 TO app;"))
    print(res)
   
    res = json.dumps(parse("SELECT COUNT(arrdelay)  FROM table1 LIMIT 10 TIME 1 TO app(websocket,locathost,5000);"))
    print(res)


    res = json.dumps(parse("SELECT AVG(col) FROM table1 LIMIT 10 TIME 1 TO app(websocket,locathost,5000);"))
    print(res)

    res = json.dumps(parse("SELECT MAX(arrdelay) FROM table1 LIMIT (1,10) TIME 1 TO app(websocket,locathost,5000);"))
    print(res)
    res = json.dumps(parse("SELECT MIN(arrdelay) FROM table1 LIMIT 10 TIME 1 TO app(websocket,locathost,5000);"))
    print(res)

    res = json.dumps(parse("SELECT SUM(arrdelay) FROM table1 LIMIT 10 TIME 1 TO app(websocket,locathost,5000);"))
    print(res)
    

    """
    TODO: time 0.1, and TO app..
    """

    res = json.dumps(parse("SELECT AVG(total_pdu_bytes_rx) FROM eNB1 WHERE crnti=0 TIME 1 TO app(websocket,locathost,5000);"))
    print(res)

    res = json.dumps(parse("SELECT ADD(ul, dl) as total FROM eNB ORDER BY total DESC LIMIT (1,10) TIME 1 TO app(websocket,locathost,5000);"))
    print(res)
    
if __name__ == "__main__":
    main()

    from sys import argv

    if len(argv) == 2:
        run(port=int(argv[1]))
    else:
        run_http()