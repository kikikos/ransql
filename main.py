#!/usr/bin/env python3
#from mo_future import text_type
from moz_sql_parser import parse as ransql_parse
import json
#import SocketServer
import http.server
import socketserver
#import re
import urllib.parse
#import urllib


class RequestHandler(http.server.SimpleHTTPRequestHandler):
    def do_HEAD(self):
        self._set_headers()

    def _set_headers(self, status_code):
        self.send_response(status_code)
        self.send_header('Content-type', 'text/html')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()

    def do_GET(self):
        request_path = self.path
        #print("request_path:",urllib.parse.unquote(request_path))
        payload =  urllib.parse.unquote(request_path)
        payload = payload[5:-1]
        
        print('req params:', payload ) #remove /?q=" at the begin and " at the end

        if payload == "cancel-usecase-1":
            #TODO
            self._set_headers(200)
        elif payload =="cancel-usecase-2":
            #TODO
            self._set_headers(200)
        else:
            try:
                #res = json.dumps(ransql_parse(payload))
                
                self._set_headers(200)
            except Exception as e:
                e.print()
                self.respond({'status': 500})

            
            


        
        #self.wfile.write("hi") #call sample function here




        
def run_api_server(port=8888):
    requestHandler = RequestHandler#http.server.SimpleHTTPRequestHandler
    with socketserver.TCPServer(("", port), requestHandler) as httpd:
        print("serving at port", port)
        httpd.serve_forever()



def main():
    """
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
    """
    
if __name__ == "__main__":
    main()

    from sys import argv

    if len(argv) == 2:
        run_api_server(port=int(argv[1]))
    else:
        run_api_server()