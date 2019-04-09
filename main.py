#!/usr/bin/env python3

from moz_sql_parser import parse as ransql_parse
import sys
import http.server
import socketserver
import urllib.parse as url_parse
import json
import asyncio
import datetime
import random
import websockets
import threading


#import re

class RequestHandler(http.server.SimpleHTTPRequestHandler):
    def do_HEAD(self):
        self._set_headers(200)

    def _set_headers(self, status_code):
        self.send_response(status_code)
        self.send_header('Content-type', 'text/html')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()

    def do_GET(self):
        request_path = self.path
        #print("request_path:",urllib.parse.unquote(request_path))
        payload =  url_parse.unquote(request_path)
        payload = payload[5:-1]
        
        print('request params:', payload ) #remove /?q=" at the begin and " at the end

        if payload == "cancel-usecase-1":
            #TODO
            content = self._handle_http(201, "cancel_usecase_1_ok")
            self.wfile.write(content)

        elif payload =="cancel-usecase-2":
            #TODO
            content = self._handle_http(202, "cancel_usecase_2_ok")
            self.wfile.write(content)

        else:
            try:
                res = json.dumps(ransql_parse(payload))
                print(res)
                content = self._handle_http(200, "parse_ok")
                
                self.wfile.write(content)

            except Exception as e:
                print(e)
                content = self._handle_http(404, "parse_error")
                self.wfile.write(content)
                

    def _handle_http(self, status_code, message):
        #self.send_response(status_code)
        self._set_headers(status_code)
        content = message
        return bytes(content, 'UTF-8')

        
def run_http_server(port=8888):
    requestHandler = RequestHandler#http.server.SimpleHTTPRequestHandler
    with socketserver.TCPServer(("", port), requestHandler) as httpd:
        print("serving at port", port)
        httpd.serve_forever()

def ws_send():
    pass

async def time(websocket, path):
    
    
    now = datetime.datetime.utcnow().isoformat() + 'Z'
    await websocket.send(now)
    
    """
    while True:
        now = datetime.datetime.utcnow().isoformat() + 'Z'
        await websocket.send(now)
        await asyncio.sleep(random.random() * 3)
    """


def main():
    pass
    
if __name__ == "__main__":
    #https://stackoverflow.com/questions/26270681/can-an-asyncio-event-loop-run-in-the-background-without-suspending-the-python-in
    #loop = asyncio.get_event_loop()
    #t = threading.Thread(target=loop_in_thread, args=(loop,))
    #t.start()


    start_server = websockets.serve(time, '127.0.0.1', 5678)
    asyncio.get_event_loop().run_until_complete(start_server)
    loop = asyncio.get_event_loop()
    t = threading.Thread(target=loop_in_thread, args=(loop,))
    t.start()

    #asyncio.get_event_loop().run_forever()
    

    if len(sys.argv) == 2:
        run_http_server(port=int(argv[1]))
    else:
        run_http_server()

    