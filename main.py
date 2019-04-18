#!/usr/bin/env python3

from moz_sql_parser import parse as ransql_parse
from kafka import KafkaConsumer
import websockets
import http.server
import socketserver
import urllib.parse as url_parse
import json
import asyncio
import datetime
import random
import threading
import subprocess


#import re

def exe_cmd(cmd):
    try:
        print("shell: ", cmd)    
        subprocess.call(cmd, shell=True)
        
    except Exception as e:    
        print("exe_cmd error:", e)

def dispath_service(service_items):
    print("services to dispatch:",service_items)
    """sql to map
    request params: SELECT AVG(total_pdu_bytes_rx) FROM eNB1 WHERE crnti=0 TIME second(1) TO app(websocket, locathost, 5000);
    - services to dispatch: [{"select": {"value": {"avg": "total_pdu_bytes_rx"}}, "from": "eNB1", "where": {"eq": ["crnti", 0]}}, {"time": {"second": 1}}, {"to": {"app": ["websocket", "locathost", 5000]}}]

    request params: SELECT ADD(ul, dl) as total FROM eNB ORDER BY total DESC LIMIT (1,10) TIME ms(1000) TO app(websocket, locathost, 5000);
    - services to dispatch: [{"select": {"value": {"add": ["ul", "dl"]}, "name": "total"}, "from": "eNB", "orderby": {"value": "total", "sort": "desc"}, "limit": [1, 10]}, {"time": {"ms": 1000}}, {"to": {"app": ["websocket", "locathost", 5000]}}]
    
    case 1: 
    (pre*)  counter how many services: oai -> oai-1, oai-1 -> oai-2 etc., oai-final
    (0) from -> for source kafka topic
    (1) where -> map to filter 
    (2) operation: sum* & sum/items (ie, avg*) with time()
    (3) show cols: select - show cols
    (4) to : websocket -

    case 2:
    (pre*) counter services to topics:
    (0) from -> getTopic()
    (1) operation: ul + dl = total (add one col)
    (2) sorting: *orderby,  with *time
    (3) show rows vertical : *limit
    (4) show cols: select - show all cols, with crnti
    (5) to app
    """


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

                for phrase in payload.split("|"):
                    print("phrase:",phrase)

                    sql_in_json = json.dumps(ransql_parse(phrase))
                    
                    dispath_service(sql_in_json)
                content = self._handle_http(200, "parse_ok")
                
                self.wfile.write(content)

            except Exception as e:
                print("parse error:", e)
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


async def websocket_handler(websocket, path):
    kafka_topic = "oai-final"
    kafka_group = "oai"
    kafka_brokers = "192.168.200.3:9092"
    
    #consumer = KafkaConsumer(kafka_topic, auto_offset_reset='latest',enable_auto_commit=False, group_id=kafka_group, bootstrap_servers=[kafka_brokers])

    while True:
        now = datetime.datetime.utcnow().isoformat() + 'Z'
        await websocket.send(now)
        await asyncio.sleep(random.random() * 3)
    
async def myfun1():    
    print('-- start {}th'.format(2))    
    await asyncio.sleep(3)    
    #time.sleep(1)
    print('-- finish {}th'.format(2))



    
if __name__ == "__main__":
    phrase="SELECT AVG(total_pdu_bytes_rx)  FROM eNB1 WHERE crnti=0 time second(1) TO app(websocket, locathost, 5000);"

    print("phrase:",phrase)

    sql_in_json = json.dumps(ransql_parse(phrase))
                    
    dispath_service(sql_in_json)
    """

    phrase="SELECT AVG(total_pdu_bytes_rx)  FROM eNB1 WHERE crnti=0"

    print("phrase:",phrase)

    sql_in_json = json.dumps(ransql_parse(phrase))
                    
    dispath_service(sql_in_json)
"""


    """
    t_http_server = threading.Thread(target=run_http_server)
    t_http_server.daemon = True
    t_http_server.start()

    websocket_server = websockets.serve(websocket_handler, '0.0.0.0', 5678)
    coroutines = (websocket_server, myfun1())


    asyncio.get_event_loop().run_until_complete(asyncio.gather(*coroutines))
    asyncio.get_event_loop().run_forever()
    """
