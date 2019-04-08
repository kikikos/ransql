#!/usr/bin/env python
#from mo_future import text_type
from moz_sql_parser import parse
import json
import SocketServer
import SimpleHTTPServer
import re

class RequestHandler(SimpleHTTPServer.SimpleHTTPRequestHandler):
    def _set_headers(self, status_code):
        self.send_response(status_code)
        self.send_header('Content-type', 'text/html')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()

    def do_GET(self):
        if None != re.search('/*', self.path):
            num = self.path.split('/')[-1]
            print self.path.split('/')
            #This URL will trigger our sample function and send what it returns back to the browser
            self._set_headers(200)
            self.wfile.write("str(num*num)") #call sample function here

        else:
            #serve files, and directory listings by following self.path from
            #current working directory
            #SimpleHTTPServer.SimpleHTTPRequestHandler.do_GET(self)
            self._set_headers(500)
            self.wfile.write("not ok") #call sample function here


    def do_HEAD(self):
        self._set_headers()

        
def run_api_server(port=8888):
    #server_address = ('', port)
    httpd = SocketServer.ThreadingTCPServer(('', port),RequestHandler) #server_class(server_address, handler_class)
    print 'Starting httpd:'
    httpd.serve_forever()
    print '??'


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
        run_api_server(port=int(argv[1]))
    else:
        run_api_server()