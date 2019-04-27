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
flink_services = []


class Session():
    """
    1 session - * statements
    1 statement - * flinks 
    """
    def __init__(self,value):
        self.id=0
        self.value=value
        self.statements= self.get_statements(self.value)
        #self.flinks = []

    def get_statements(self, value):
        statements  =[]
        for stm_str in value.split("|"):
            statements.append(Statement(stm_str))
        return statements #Statement(self.value.split('|') )

    def print(self):
        j_str= str(json.dumps(self, default=lambda o: o.__dict__))
        print("session {}".format(j_str))

            
class Statement():
    def __init__(self, value):
        self.id=0
        self.value = value
        self.input_topic={'id':0, 'value':'', 'type':'', 'conf':{}}
        self.output_topic={'id':0, 'value':'', 'type':'', 'conf':{}}
        self.flinks = self.config_flinks()#self.map_flinks() #Flinks()
        #self.chained_flinks = self.chain_flinks()

    def config_flinks(self):
        #pass
        flinks = self.map_flinks()
        return self.chain_flinks(flinks)
    
    def map_flinks(self):
        global flink_services
        flinks = []

        for service in ransql_parse(self.value):
            for key, value in service.items():            
                if key == "select":
                # dispatch_select(service)
                    for op, col in value['value'].items():

                        if op == 'obj':
                            flink = Obj()
                            flink.is_mapped = True
                            flink.col = col
                            flinks.append(flink)

                        elif op == 'avg':
                            flink = Avg()
                            flink.is_mapped = True
                            flink.col = col
                            flinks.append(flink)

                        elif op == 'add':
                            flink = Add()
                            flink.is_mapped = True
                            flink.col1 = col[0]
                            flink.col2 = col[1]
                            flink.as_col3 = value['name']
                            flinks.append(flink)

                elif key == "where":
                    # dispatch_from(service)
                    flink = Filter()
                    flink.is_mapped = True
                    flink.conditions = []
                    for sign, col in value.items():
                        condition = {'col': col[0], 'value': col[1], 'sign': sign}
                        flink.conditions.append(condition)
                    flinks.append(flink)

                elif key == "orderby":
                    # dispatch_orderby(service)
                    flink = Sorter()
                    flink.is_mapped = True

                    flink.col = value['value']
                    flink.order = value['sort']
                    flinks.append(flink)

                elif key == "limit":
                    for f in flinks:
                        if isinstance(f, Sorter):
                            f.limit['bottom'] = value[0]
                            f.limit['ceil'] = value[1]

                elif key == "time":
                    for f in flinks:
                        if isinstance(f, Avg) or  isinstance(f, Sorter):
                            for t_unit, t_value in value.items():
                                f.time['unit'] = t_unit
                                f.time['value'] = t_value
                elif key == "from":
                    #flink.input_topic = value
                    self.input_topic['value'] = value

                elif key == "to":
                    # dispatch_to(service)
                    flink = AppConnector()
                    for to_type, to_conf in value.items():
                        
                        self.output_topic['type'] = to_type
                        self.output_topic['conf'] = to_conf
                        
                        #flink.output_topic_type = to_type
                        #flink.output_topic_conf = to_conf
                        if to_type == 'app':
                            flink.is_mapped = True
                            flink.conf = to_conf
                            #TODO: temp, parser shoud be improved
                            self.output_topic['value'] = 'websocket'


                            if to_conf[0] == 'websocket':
                                self.output_topic['value'] = 'websocket'
                            
                            flinks.append(flink)
                        
                        elif to_type == 'table':
                            self.output_topic['value'] = to_conf
                            """
                            We dont add new flink if it's just a table, which will be configured as a topic in kafka
                            """
                    
        return flinks

    def chain_topics(self, chain_operators, flinks):
        topics =[]
        operators_num = len(flinks )
        #print("***self.input_topic['value']:",self.input_topic)
        topics.append(self.input_topic['value'])
        topics.append(self.output_topic['value'])  
        
        if operators_num > 1 :
            cnt = 0
            for op in chain_operators:
                if cnt < operators_num-1:
                    topics.insert(-1, str( topics[-2] ) + "-" +op)

                """
                if op == 'app':
                    topics[-1] = self.output_topic['value']
                """
                cnt +=1

        return topics

    def chain_operators(self, flinks):
        operators = []
        for i in range(0, MAX_OPERATOR_PRIORITY):  
            for flink in flinks:
                #print("****flink in chain ops:{}".format(flink ))
                if flink.operation['priority'] == i:
                    operators.append(flink.operation['name'])

        return operators


    def chain_flinks(self, flinks):
        global all_topics
        #_flinks = flinks 
        chained_flinks=[]
        chain_operators=self.chain_operators(flinks)
        chain_topics = self.chain_topics(chain_operators, flinks)

        print("chain operators: {}".format(chain_operators))
        print("chain topics: {}, flinks types {}".format(chain_topics, type(flinks)))

        for f_idx, f_val in enumerate(flinks) :
            if f_idx <= len(flinks )-1 : #ignore the last one
            #    for  ct_idx, ct_val in enumerate(chain_topics):
                flinks[f_idx].input_topic['value'] = chain_topics[f_idx]
                flinks[f_idx].output_topic['value'] = chain_topics[f_idx+1]
                f_val.is_conf_complete = True
            

                chained_flinks.append(f_val)

        
        return chained_flinks



    def get_in_topic(self):
        pass

    def get_out_topic(self):
        pass


class Flink():
    STREAM_OPERATIONS={
        'filter':{'name':'filter', 'priority':0,'transformation':{'from':'STREAM','to':'STREAM'},'java_app':'FlinkFilter.java'}, 
        'obj':{'name':'obj', 'priority':1,'transformation':{'from':'STREAM','to':'STREAM'},'java_app':'FlinkObjectinize.java'}, 
        'avg':{'name':'avg', 'priority':1,'transformation':{'from':'STREAM','to':'ACC_STREAM'},'java_app':'FlinkAvg.java'}, 
        'add':{'name':'add', 'priority':1,'transformation':{'from':'STREAM','to':'STREAM'},'java_app':'FlinkAdder.java'}, 
        'sorter':{'name':'sorter', 'priority':2,'transformation':{'from':'STREAM','to':'WINDOWSED_STREAM'},'java_app':'FlinkSorter.java'}, 
        'app':{'name':'app', 'priority':3,'transformation':{'from':'STREAM','to':'UNKNOWN'},'java_app':''}
        }
    
    def __init__(self):
        self.java_app_path = '/home/user/app'
        self.log4j2 = '-Dlog4j.configurationFile="./conf/log4j2.xml"'

        self.input_topic = {'id':0, 'value':''}
        self.output_topic = {'id':0, 'value':'', 'type':'', 'conf':{}}

        self.brokers = ''
        self.zookeeper = ''
        self.group = ''        
        
        self.operation=''
        self.is_mapped =  False
        self.is_conf_complete = False
        
    def __str__(self):
        return str(json.dumps(self, default=lambda o: o.__dict__))
        
        
class Filter(Flink): # a Flink represents a statement, which can contains multiple flink apps
    def __init__(self):
        Flink.__init__(self)
        self.operation=Flink.STREAM_OPERATIONS['filter']
        self.conditions = [{'col': '', 'value': '', 'sign': ''}]

    def __str__(self):
        return super().__str__()


class Avg(Flink): 
    def __init__(self):
        Flink.__init__(self)
        self.operation=Flink.STREAM_OPERATIONS['avg']
        self.col = ''
        self.time = {'unit': '', 'value': 0}

    def __str__(self):
        return super().__str__()

class Add(Flink): 
    def __init__(self):
        Flink.__init__(self)
        self.operation=Flink.STREAM_OPERATIONS['add']
        self.col1 = ''
        self.col2 = ''
        self.as_col3 = ''

    def __str__(self):
        return super().__str__()

class Obj(Flink):
    def __init__(self):
        Flink.__init__(self)
        self.operation=Flink.STREAM_OPERATIONS['obj']
        self.col = ''

    def __str__(self):
        return super().__str__()

class Sorter(Flink):
    def __init__(self):
        Flink.__init__(self)
        self.operation=Flink.STREAM_OPERATIONS['sorter']
        self.col = ''
        self.order=''
        self.time = {'unit': '', 'value': 0}
        self.limit ={'bottom':0,'ceil':0}

    def __str__(self):
        return super().__str__()

class AppConnector(Flink):
    def __init__(self):
        Flink.__init__(self)
        self.operation=Flink.STREAM_OPERATIONS['app']
        self.conf = {}

    def __str__(self):
        return super().__str__()

class Topic():
    def __init__(self):
        self.id=0
        self.in_topic="" 
        self.out_topic=""
        self.operator=""
        self.conf= {}    
        self.zookeeper= "127.0.0.1:2181"


def exe_cmd(cmd):
    try:
        print("shell: ", cmd)
        subprocess.call(cmd, shell=True)
    except Exception as e:
        print("exe_cmd error: {}".format(e))


def dispatch_(flink):
    pass


def map_session_id():
    global sessions
    pass


def chain_operators(flinks):
    operators = []
    for i in range(0, MAX_OPERATOR_PRIORITY):  
        for flink in flinks:
            #print("****flink in chain ops:{}".format(flink ))
            if flink.operation['priority'] == i:
                operators.append(flink.operation['name'])

    return operators


def chain_topics(flinks, operators):
    topics = []  
    flink_num = len(flinks)
    op_num = len(operators)
    
    counter =0
    for flink in flinks:
        for op in operators:
            if counter < flink_num + 1 - op_num:
                
                topics.append(flink.input_topic['value'] + "-" + op)

                counter +=1
        
    return topics

def is_topic_existed(topic):
    global all_topics
    for t in all_topics:
        if t.in_topic == topic.in_topic and t.out_topic == topic.out_topic and t.operator == topic.operator and t.conf == topic.conf and t.zookeeper ==  topic.zookeeper:
            return True


def config_topics(flinks):
    global all_topics
    topic=Topic()
    flinks=[]
    for flink in flinks:
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
    logging.basicConfig(format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',level=logging.INFO)
    sessions = []

    
    #stm1 = "SELECT OBJ(ue_list) FROM eNB1 TO table(ues)"
    #stm2 = "SELECT AVG(total_pdu_bytes_rx) TIME second(1) FROM ues WHERE crnti=0  TO app(websocket, 5000, col1, col2, col3);"

    stm1 = "SELECT OBJ(ue_list) FROM eNB1 TO table(ues)"
    stm2 = "SELECT ADD(rbs_used, rbs_used_rx) as total FROM ues ORDER BY total DESC LIMIT (1,10) TIME ms(1000) TO app('name'='websocket','cols'='*','class'='12345', 5000, col1, col2, col3);"

    stms = stm1 + "|" + stm2

    session = Session(stms) 
    #print("session statements: {}".format( session))
    #session.print()



    sessions.append(session)
    
    for sess in sessions:
        sess.print()
            #stm.value = stm
            #print("session statements: {}".format(stm))
        
    """    

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
    """

    #flinks_per_session = config_topics(flinks_per_session)
    
    #for flink in flinks_per_session:
    #    print("****statement conf done: object {} with value: {}".format(flink, flink.__dict__ ))
    
    #dispatch_services(flinks_per_session)




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
    
