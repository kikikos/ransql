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
import logging

#import re
MAX_OPERATOR_PRIORITY = 10

all_topics=[]

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
sessions = []

class Session():
    def __init__(self):
        """
        1 session - * statements
        1 statement - * flinks 
        """
        self.id=0
        self.statements=[]
        #self.flinks = []

class Statement():
    def __init__(self):
        self.id=0
        self.value=''
        self.flinks=[] #Flinks()    

class Topic():
    def __init__(self):
         self.in_topic="" 
         self.out_topic=""
         self.operator=""
         self.conf= {}    
         self.zookeeper= "127.0.0.1:2181"

class FlinkOperator():
    def __init__(self):
        pass
        

class Flink(): # a Flink represents a statement, which can contains multiple flink apps
    def __init__(self):
        self.path = '/home/user/app'
        self.log4j2 = '-Dlog4j.configurationFile="./conf/log4j2.xml"'

        self.session = {  # for kafka group.id
            'ip_port': '',
            'submit_button': '',
            'id': 0,
            'statement': 0 #depriciated
        }
        self.input_topic = ''
        self.input_topic_conf={}
        self.output_topic = ''
        self.output_topic_type=''
        self.output_topic_conf={}
        self.brokers = ''
        self.zookeeper = ''
        self.group = ''

        """
        Note: Do not change the operators order, it counts
        """
        self.filter = {
            'name':'filter',
            'is_mapped': False,
            'is_conf_complete': False,
            'in_type': 'stream',  # stream = simple_stream, basic type
            'out_type': 'stream',
            'priority': 0,
            'app': 'FlinkFilter.java',
            'conditions': [{'col': '', 'value': '', 'sign': ''}],
            'in_topic': '',
            'out_topic': {'value':'','type': '', 'conf': []}
        }

        self.avg = {
            'name':'avg',
            'is_mapped': False,
            'is_conf_complete': False,
            'in_type': 'stream',
            'out_type': 'timed_stream',
            'priority': 1,
            'col': '',
            'time': {'unit': '', 'value': 0},
            'in_topic': '',
            'out_topic': {'value':'','type': '', 'conf': []}
        }

        self.add = {
            'name':'add',
            'is_mapped': False,
            'is_conf_complete': False,
            'in_type': 'stream',
            'out_type': 'stream',  # expand auto one more col as_col3, keep the remain cols
            'priority': 1,
            'col1': '',
            'col2': '',
            'as_col3': '',
            'in_topic': '',
            'out_topic': {'value':'','type': '', 'conf': []}
        }

        self.obj = {
            'name':'obj',
            'is_mapped': False,
            'is_conf_complete': False,
            'in_type': 'listed_stream',
            'out_type': 'stream',
            'priority': 1,
            'col': '',
            'in_topic': '',
            'out_topic': {'value':'','type': '', 'conf': []}
        }

        self.sorter = {
            'name':'sorter',
            'is_mapped': False,
            'is_conf_complete': False,
            'in_type': 'stream',
            'out_type': 'timed_stream',
            'priority': 2,
            'col': '',
            'order': '',  # desc/asc
            'time': {'unit': '', 'value': 0},
            'limit':{'bottom':0,'ceil':0},
            'in_topic': '',
            'out_topic': {'value':'','type': '', 'conf': []}
        }
        """
        self.limiter = {
            'is_mapped': False,
            'is_conf_complete': False,
            'in_type': 'stream',
            'out_type': 'headed_stream',  # add two cols  "total" and "rest" as the heads
            'priority': 3,
            'col': '',
            'range': [],
            'total': 0,
            'rest': 0,
            'time': {'unit': '', 'value': 0},
            'in_topic': '',
            'out_topic': {'value':'','type': '', 'conf': []}
        }
        """

        self.app = {
            'name':'app',
            'is_mapped': False,
            'is_conf_complete': False,
            'in_type': 'stream',
            'out_type': 'app',  # add two cols  "total" and "rest" as the heads
            'priority': 4,
            'conf':[],
            'in_topic': '',
            'out_topic': {'value':'','type': '', 'conf': []}
        }


def exe_cmd(cmd):
    try:
        print("shell: ", cmd)
        subprocess.call(cmd, shell=True)

    except Exception as e:
        print("exe_cmd error:", e)


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


def map_services(services, session, statement, flink):
    global flink_services
    print("services to dispatch {} with session {}, statement {}".format(
        services, session, statement))

    for service in json.loads(services):
        #print("service:{}".format( service))

        for key, value in service.items():

            flink.session['id'] = session
            flink.session['statement'] = statement

            #print("***service key: {} and value: {}".format(key, value))

            if key == "select":
                # dispatch_select(service)
                for op, col in value['value'].items():

                    if op == 'obj':
                        flink.obj['is_mapped'] = True
                        flink.obj['col'] = col
                    elif op == 'avg':
                        flink.avg['is_mapped'] = True
                        flink.avg['col'] = col

                    elif op == 'add':
                        flink.add['is_mapped'] = True
                        flink.add['col1'] = col[0]
                        flink.add['col2'] = col[1]
                        flink.add['as_col3'] = value['name']

            elif key == "where":
                # dispatch_from(service)
                flink.filter['is_mapped'] = True
                flink.filter['conditions'] = []
                for sign, col in value.items():
                    condition = {'col': col[0], 'value': col[1], 'sign': sign}
                    flink.filter['conditions'].append(condition)

            elif key == "orderby":
                # dispatch_orderby(service)
                flink.sorter['is_mapped'] = True

                flink.sorter['col'] = value['value']
                flink.sorter['order'] = value['sort']

            elif key == "limit":
                #flink.limiter['is_mapped'] = True
                # dispatch_limit(service)
                #flink.limiter['range'] = value
                flink.sorter['limit']['bottom'] = value[0]
                flink.sorter['limit']['ceil'] = value[1]

            elif key == "time":
                if flink.avg['is_mapped']:
                    for t_unit, t_value in value.items():
                        flink.avg['time']['unit'] = t_unit
                        flink.avg['time']['value'] = t_value

                if flink.sorter['is_mapped']:
                    for t_unit, t_value in value.items():
                        flink.sorter['time']['unit'] = t_unit
                        flink.sorter['time']['value'] = t_value
                """
                if flink.limiter['is_mapped']:
                    for t_unit, t_value in value.items():
                        flink.limiter['time']['unit'] = t_unit
                        flink.limiter['time']['value'] = t_value
                """

            elif key == "from":
                flink.input_topic = value

            elif key == "to":
                # dispatch_to(service)
                for to_type, to_conf in value.items():
                    flink.output_topic_type = to_type
                    flink.output_topic_conf = to_conf
                    if to_type == 'app':
                        flink.app['is_mapped'] = True
                        flink.app['conf'] = to_conf


                        if to_conf[0] == 'websocket':
                            flink.output_topic = 'websocket'
                    elif to_type == 'table':
                        flink.output_topic = to_conf


def map_session_id():
    global sessions
    pass


def chain_operators(flink):
    operators = []
    for i in range(0, MAX_OPERATOR_PRIORITY):  # make sure the operator priority correct

        if flink.filter['is_mapped'] and flink.filter['priority'] == i:
            operators.append('filter')
            continue

        if flink.avg['is_mapped'] and flink.avg['priority'] == i:
            operators.append('avg')
            continue

        if flink.obj['is_mapped'] and flink.obj['priority'] == i:
            operators.append('obj')
            continue

        if flink.add['is_mapped'] and flink.add['priority'] == i:
            operators.append('add')
            continue

        if flink.sorter['is_mapped'] and flink.sorter['priority'] == i:
            operators.append('sorter')
            continue
        """
        if flink.limiter['is_mapped'] and flink.limiter['priority'] == i:
            operators.append('limiter')
            continue
        """

        if flink.app['is_mapped'] and flink.app['priority'] == i:
            operators.append('app')
            continue


    return operators


def chain_topics(flink):
    topics = []  # { 'from':'','to':''}
    topics.append(flink.input_topic)
    topics.append(flink.output_topic)

    return topics

def is_topic_existed(topic):
    global all_topics
    for t in all_topics:
        if t.in_topic == topic.in_topic and t.out_topic == topic.out_topic and t.operator == topic.operator and t.conf == topic.conf and t.zookeeper ==  topic.zookeeper:
            return True


def config_topics(flinks_per_session):
    global all_topics
    topic=Topic()
    flinks=[]
    for flink in flinks_per_session:
        topics = []
        print("count_mapped_services: {}".format(chain_operators(flink)))

        total_operators = len(chain_operators(flink))

        # a statement, ie, a flink, has 2 topics for 'from' and 'to'
        topics = chain_topics(flink)

        max_middle_topics = len(chain_operators(flink)) + \
            1 - len(chain_topics(flink))


        #general case
        middle_topic_counter=0
        for op in chain_operators(flink):
            if middle_topic_counter < max_middle_topics or max_middle_topics == 0:
                
                if op == 'filter':    
                    topic.in_topic = topics[-2]
                    #TODO: how to define a new topic?
                    topic.out_topic = str(topics[-2] + "-" + op)
                    topic.operator = op
                    #TODO: how to pass conf?

                    topics.insert(-1,topic.out_topic)
                    #print("topic:{} ".format(topic.__dict__))
                    
                    flink.filter['in_topic'] = topic.in_topic 
                    flink.filter['out_topic']['value'] = topic.out_topic

                    flink.filter['is_conf_complete'] = True


                    if not is_topic_existed(topic):
                        all_topics.append(topic)

                elif op == 'obj':
                    topic.in_topic = topics[-2]
                    #TODO: how to define a new topic?
                    topic.out_topic = str(topics[-2] + "-" + op)
                    topic.operator = op
                    #TODO: how to pass conf?

                    topics.insert(-1,topic.out_topic)
                    #print("topic:{} ".format(topic.__dict__))
                    flink.obj['in_topic'] = topic.in_topic 
                    flink.obj['out_topic']['value'] = topic.out_topic

                    flink.obj['is_conf_complete'] = True
                    #print("****Done flink.obj['is_conf_complete']: {}".format(flink.obj['is_conf_complete']))
                    
                    if not is_topic_existed(topic):
                        all_topics.append(topic)
                
                elif op == 'add':
                    topic.in_topic = topics[-2]
                    #TODO: how to define a new topic?
                    topic.out_topic = str(topics[-2] + "-" + op)
                    topic.operator = op
                    #TODO: how to pass conf?

                    topics.insert(-1,topic.out_topic)
                    flink.add['in_topic'] = topic.in_topic 
                    flink.add['out_topic']['value'] = topic.out_topic

                    flink.add['is_conf_complete'] = True
                    
                    if not is_topic_existed(topic):
                        all_topics.append(topic)                
                        
                elif op == 'avg':
                    topic.in_topic = topics[-2]
                    #TODO: how to define a new topic?
                    topic.out_topic = str(topics[-2] + "-" + op)
                    topic.operator = op
                    #TODO: how to pass conf?

                    topics.insert(-1,topic.out_topic)

                    flink.avg['in_topic'] = topic.in_topic 
                    flink.avg['out_topic']['value'] = topic.out_topic

                    flink.avg['is_conf_complete'] = True
                    
                    if not is_topic_existed(topic):
                        all_topics.append(topic)

                elif op == 'sorter':
                    topic.in_topic = topics[-2]
                    #TODO: how to define a new topic?
                    topic.out_topic = str(topics[-2] + "-" + op)
                    topic.operator = op
                    #TODO: how to pass conf?

                    topics.insert(-1,topic.out_topic)
                    
                    flink.sorter['in_topic'] = topic.in_topic 
                    flink.sorter['out_topic']['value'] = topic.out_topic

                    flink.sorter['is_conf_complete'] = True
                    
                    if not is_topic_existed(topic):
                        all_topics.append(topic)
                """
                elif op == 'limiter':
                    topic.in_topic = topics[-2]
                    #TODO: how to define a new topic?
                    topic.out_topic = str(topics[-2] + "-" + op)
                    topic.operator = op
                    #TODO: how to pass conf?

                    topics.insert(-1,topic.out_topic)

                    flink.limiter['in_topic'] = topic.in_topic 
                    flink.limiter['out_topic']['value'] = topic.out_topic

                    flink.limiter['is_conf_complete'] = True
                    
                    if not is_topic_existed(topic):
                        all_topics.append(topic)
                """
        
        #only last operator case
        for op in chain_operators(flink):
            if op == 'app' :
                    topic.in_topic = topics[-2]
                    topic.out_topic = topics[-1]
                    #topic.operator = op
                    #TODO: how to pass conf?

                    flink.app['in_topic'] = topic.in_topic 
                    flink.app['out_topic']['value'] = topic.out_topic
                    flink.app['out_topic']['conf'] = flink.output_topic_conf
                    flink.app['out_topic']['type'] = flink.output_topic_type

                    flink.app['is_conf_complete'] = True                    
                

            middle_topic_counter+=1
        

        flinks.append(flink)
        print("chained topics: {}".format(topics))

    return flinks#flinks_per_session


def dispatch_services(flinks_per_session):
    
    for flink in flinks_per_session:
        print("flink  to dispatch: {}".format(flink.__dict__))
        if flink.filter['is_conf_complete']:
            print("filter ready to dispatch")
        if flink.avg['is_conf_complete']:
            print("avg ready to dispatch")
        if flink.add['is_conf_complete']:
            print("add ready to dispatch")
        if flink.obj['is_conf_complete']:
            print("obj ready to dispatch")
        if flink.sorter['is_conf_complete']:
            print("sorter ready to dispatch")
        """
        if flink.limiter['is_conf_complete']:
            print("limiter ready to dispatch")
        """
        if flink.app['is_conf_complete']:
            print("app ready to dispatch")
        



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
                session = Session()
                
                #flinks_per_session = []
                statement_counter = 0
                for statement in payload.split("|"):
                    session.statements=[]
                    
                    flink = Flink()
                    #print("statement:", statement)

                    sql_in_json = json.dumps(ransql_parse(statement))

                    map_services(sql_in_json, session, statement_counter, flink)
                    statement_counter += 1
                    
                    #session.statements.append(flinks)
                
                flinks_per_session = config_topics(flinks_per_session)
                dispatch_services(flinks_per_session)

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
    logging.basicConfig(    format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',level=logging.INFO)
    sessions = []

    statements_sessions = []
    #statement1 = "SELECT OBJ(ue_list) FROM eNB1 TO table(ues)"
    #statement2 = "SELECT AVG(total_pdu_bytes_rx) TIME second(1) FROM ues WHERE crnti=0  TO app(websocket, 5000, col1, col2, col3);"

    statement1 = "SELECT OBJ(ue_list) FROM eNB1 TO table(ues)"
    statement2 = "SELECT ADD(rbs_used, rbs_used_rx) as total FROM ues ORDER BY total DESC LIMIT (1,10) TIME ms(1000) TO app('name'='websocket','cols'='*','class'='12345', 5000, col1, col2, col3);"

    statements = statement1 + "|" + statement2
    statements_sessions.append(statements)
    
    session_counter = 0
    flinks_per_session = []
    for statements in statements_sessions:
        session = Session()


        statement_counter = 0
        session_counter += 1
        for statement in statements.split("|"):
            # a statement map to a flink app
            # a session can owns multiple flinks app/statements
            flink = Flink()
            sql_in_json = json.dumps(ransql_parse(statement))

            map_services(sql_in_json, session_counter,
                         statement_counter, flink)
            statement_counter += 1
            flinks_per_session.append(flink)

    flinks_per_session = config_topics(flinks_per_session)
    
    #for flink in flinks_per_session:
    #    print("****statement conf done: object {} with value: {}".format(flink, flink.__dict__ ))
    
    dispatch_services(flinks_per_session)




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
    
