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
input_topics = []
output_topics = []
sessions = []
operators = {
    'filter': {'value': 'where', 'priority': 0, 'app': 'FlinkFilter.java'},
    'obj': {'value': 'obj', 'priority': 1, 'app': ''},
    'avg': {'value': 'avg', 'priority': 1, 'app': ''},
    'add': {'value': 'add', 'priority': 1, 'app': ''},
    'sorter': {'value': 'orderby', 'priority': 2, 'app': ''},
}


flink_services = []

class Flink():
    def __init__(self):
        self.session = {  # for kafka group.id
            'ip_port': '',
            'submit_button': '',
            'id': 0,
            'phrase': 0
        }
        self.input_topic = ''
        self.output_topic = ''
        self.brokers = ''
        self.zookeeper = ''
        self.log4j2 = '-Dlog4j.configurationFile="./conf/log4j2.xml"'
        self.operator = {
            'type': '',  # filter -> avg, add, obj -> sorter(orderby)
            'cols': [],
            'values': [],
            'col_alias': '',
            'sign': '',  # for filter: eq, lt, gt, gte, lte, neq
            'order_col': '',
            'sorter': {'limit': [], 'order': ''},  # desc/asc
            # 'limit': [],
            'from': '',
            't_unit': '',
            't_value': 0,
            'to_type': '',
            'to_conf': []
        }


def exe_cmd(cmd):
    try:
        print("shell: ", cmd)
        subprocess.call(cmd, shell=True)

    except Exception as e:
        print("exe_cmd error:", e)


def get_dispatcher():

    return {
        'log4j2': '-Dlog4j.configurationFile="./conf/log4j2.xml"',
        'operator': '',
        'cols': [],
        'col_alias': '',
        'order_col': '',
        'sort': '',  # desc/asc
        'limit': [],
        'from': '',
        'filter': False,
        'sign': '',
        't_unit': '',
        't_value': 0,
        'to_type': '',
        'to_conf': []
    }


def chain_services():
    """
    (1) where -> filter: *required: from and to
    (2) operator -> avg, obj, add: *required: from and to
    (3) orderby/limit/asc -> sorter: *required: from and to
    """

    pass


def dispatch_select(service):
    slt = get_dispatcher()

    for k, v in service.items():
        #print("**select: k:{}, v:{}".format(k, v))
        if k == 'select':

            for operation, col in v['value'].items():
                slt['operator'] = operation
                if operation == 'obj':
                    """
                    obj -> map and flatMap
                    """
                    #print("obj op: {}".format(col))

                    slt['cols'].append(col)  # =[col]
                    # select_operator=operation
                    # select_col=col

                elif operation == 'avg':
                    """
                    avg -> map with windows
                    """
                    #print("avg op: {}".format(col))
                    slt['cols'].append(col)

                elif operation == 'add':
                    #print("add op: {}".format(col))
                    slt['cols'].append(col[0])
                    slt['cols'].append(col[1])
                    slt['col_alias'] = v['name']

        elif k == 'from':
            #print("***from: k:{}, v:{}".format(k, v))
            slt['from'] = v

        elif k == 'limit':
            #print("***from: k:{}, v:{}".format(k, v))
            slt['limit'] = v

        elif k == 'orderby':
            #print("***from: k:{}, v:{}".format(k, v))
            slt['order_col'] = v['value']
            slt['sort'] = v['sort']

        elif k == 'where':
            #print("***from: k:{}, v:{}".format(k, v))
            slt['filter'] = True
            slt['sign'] = "eq"

        elif k == 'time':
            #print("select time: k:{}, v:{}".format(k, v))
            for t_unit, t_value in v.items():
                slt['t_unit'] = t_unit
                slt['t_value'] = t_value

    print("*****slt: {} ".format(slt))

    # flink_services.append(flink)


def dispatch_to(service):
    for k, v in service.items():
        print("**to: k:{}, v:{}".format(k, v))
        if k == 'app':
            print("to app: k:{}, v:{}".format(k, v))
        elif k == 'table' or k == 'sink':
            print("to table: k:{}, v:{}".format(k, v))


def dispatch_where(service):
    for k, v in service.items():
        if k == "eq":
            print("where: k:{}, v:{}".format(k, v))


def dispatch_orderby(service):
    for k, v in service.items():
        print("orderby: k:{}, v:{}".format(k, v))


def dispatch_limit(service):
    for k, v in service.items():

        print("limit: k:{}, v:{}".format(k, v))


def dispatch_from(service):
    for k, v in service.items():
        print("where: k:{}, v:{}".format(k, v))


def dispatch_time(service):
    for k, v in service.items():
        print("time: k:{}, v:{}".format(k, v))


def chain_topics(services):
    """
    (pre*)  counter how many services: oai -> oai-1, oai-1 -> oai-2 etc., oai-final
    (0) [+1 topic]: from + operatoin(obj + time, avg, add) -> for source kafka topic
    (1) [+1 topic]: where -> filter 
    (2) [+1 topic]: orderby, time, desc/asc
    (4) [+1 topic]: limit[1,10],
    (5) [+1 topic]: to : app/table
    """
    return []


def dispath_service(services, session, phrase):
    global flink_services
    print("services to dispatch {} with session {}, phrase {}".format(
        services, session, phrase))

    for service in json.loads(services):
        #print("service:{}".format( service))
        flink = Flink()
        for key, value in service.items():
            
            flink.session['id'] = session
            flink.session['phrase'] = phrase

            print("***service key: {} and value: {}".format(key, value))

            if key == "select":
                # dispatch_select(service)
                for op, col in value['value'].items():
                    flink.operator['type'] = op
                    
                    if op == 'obj':                    
                        flink.operator['cols'].append(col)
                    elif op == 'avg':                    
                        flink.operator['cols'].append(col)
                    elif op == 'add':
                        flink.operator['cols'].append(col[0])
                        flink.operator['cols'].append(col[1])                    
                        flink.operator['col_alias'] =  value['name']

            elif key == "where":
                # dispatch_from(service)
                flink.operator['type'] =  'filter'
                for sign, col in value.items():
                    flink.operator['sign'] =  sign
                    #print("+++col : {}".format(col))
                    flink.operator['cols'].append(col[0]) 
                    flink.operator['values'].append(col[1])

            elif key == "from":
                flink.operator['from'] =  value

            elif key == "time":
                for t_unit, t_value in value.items():
                    flink.operator['t_unit'] = t_unit
                    flink.operator['t_value'] = t_value

            elif key == "orderby":
                # dispatch_orderby(service)
                flink.operator['type'] = 'sorter'
                flink.operator['order_col'] = value['value']
                flink.operator['sorter']['order'] = value['sort']

            elif key == "limit":
                # dispatch_limit(service)
                flink.operator['sorter']['limit'] = value

            elif key == "to":
                # dispatch_to(service)
                for to_type, to_conf in value.items():
                    flink.operator['to_type'] = to_type
                    flink.operator['to_conf'] = to_conf

        flink_services.append(flink)

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
        # print("request_path:",urllib.parse.unquote(request_path))
        payload = url_parse.unquote(request_path)

        for p in payload.split("&"):
            if "q=" in p:
                payload = p[5:-1]
            elif "field=" in p:
                session = p
                print("session: {}".format(session))

        # remove /?q=" at the begin and " at the end
        print('request params:', payload)

        if payload == "cancel-usecase-1":
            # TODO
            content = self._handle_http(201, "cancel_usecase_1_ok")
            self.wfile.write(content)

        elif payload == "cancel-usecase-2":
            # TODO
            content = self._handle_http(202, "cancel_usecase_2_ok")
            self.wfile.write(content)

        else:
            try:

                for phrase in payload.split("|"):
                    print("phrase:", phrase)

                    sql_in_json = json.dumps(ransql_parse(phrase))

                    dispath_service(sql_in_json)
                content = self._handle_http(200, "parse_ok")

                self.wfile.write(content)

            except Exception as e:
                print("parse error:", e)
                content = self._handle_http(404, "parse_error")
                self.wfile.write(content)

    def _handle_http(self, status_code, message):
        # self.send_response(status_code)
        self._set_headers(status_code)
        content = message
        return bytes(content, 'UTF-8')


def run_http_server(port=8888):
    requestHandler = RequestHandler  # http.server.SimpleHTTPRequestHandler
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
    # time.sleep(1)
    print('-- finish {}th'.format(2))


if __name__ == "__main__":

    statements = []

    phrase = "SELECT OBJ(ue_list) FROM eNB1 TO table(ues)"
    phrase += "| SELECT AVG(total_pdu_bytes_rx) TIME second(1) FROM ues WHERE crnti=0  TO app(websocket, locathost, 5000);"
    statements.append(phrase)

    phrase = "SELECT OBJ(ue_list) FROM eNB1 TO table(ues)"
    phrase += "| SELECT ADD(rbs_used, rbs_used_rx) as total FROM ues ORDER BY total DESC LIMIT (1,10) TIME ms(1000) TO app(websocket, locathost, 5000);"
    statements.append(phrase)

    for textfield in range(0, 2):
        phrase_counter = 0
        for phrase in statements[textfield].split("|"):
            sql_in_json = json.dumps(ransql_parse(phrase))

            dispath_service(sql_in_json, textfield, phrase_counter)
            phrase_counter += 1

    for f in flink_services:
      print( "flink_services: {}".format(f.__dict__))


    """
    t_http_server = threading.Thread(target=run_http_server)
    t_http_server.daemon = True
    t_http_server.start()

    websocket_server = websockets.serve(websocket_handler, '0.0.0.0', 5678)
    coroutines = (websocket_server) 
    asyncio.get_event_loop().run_until_complete(asyncio.gather(coroutines))    

    # or more coroutines example: 
    # coroutines = (websocket_server, myfun1())
    # asyncio.get_event_loop().run_until_complete(asyncio.gather(*coroutines))

    asyncio.get_event_loop().run_forever()
    """
