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
logging.basicConfig(format='%(asctime)s, line:%(lineno)d:%(levelname)s:%(message)s',level=logging.INFO)

all_topics=[]
sessions = []


class Session():
    """
    1 session - * statements
    1 statement - * flinks 
    """
    def __init__(self,value, id):
        self.id=id
        self.value=value
        self.statements= self.get_statements(self.value, self.id)
        #self.flinks = []

    def get_statements(self, value, session_id):
        statements  =[]
        for stm_str in value.split("|"):
            statements.append(Statement(stm_str, session_id))
        return statements #Statement(self.value.split('|') )

    def print(self):
        j_str= str(json.dumps(self, default=lambda o: o.__dict__))

        logging.debug("session config: %s", j_str)
        

            
class Statement():
    def __init__(self, value, session_id):
        self.session_id=  session_id
        self.value = value
        self.input_topic={'id':0, 'value':'', 'type':'', 'conf':{}}
        self.output_topic={'id':0, 'value':'', 'type':'', 'conf':{}}
        self.flinks = self.config_flinks()#self.map_flinks() #Flinks()
        #self.chained_flinks = self.chain_flinks()
    
    def dispatch_flinks(self,flinks):
        for flink in flinks:
            logging.debug("flink to dispatch: %s", flink)
            if isinstance(flink, Filter):
                self.dispatch_filter(flink)
            elif isinstance(flink, Obj):
                self.dispatch_obj(flink)
            elif isinstance(flink, Avg):
                self.dispatch_avg(flink)
            elif isinstance(flink, Add):
                self.dispatch_add(flink)
            if isinstance(flink, Sorter):
                self.dispatch_sorter(flink)
            elif isinstance(flink, AppConnector):
                self.dispatch_app(flink)
    
    def config_basic_dispatcher(self, flink):
        logging.debug('config basic dispatcher %s, type %s', flink, type(flink) )
        config =  "java "+ " -Dlog4j.configurationFile=\"" + flink.java_app_path + flink.log4j2 + "\""+ " -Xmx256m "+ " -jar " + flink.java_app_path +  "/target/" + flink.operation['java_app'] +" --zookeeper.connect " + flink.zookeeper+ " --bootstrap.servers " + flink.brokers+  " --group.id " + flink.group+  " --input-topic " + flink.input_topic['value'] + " --output-topic "+flink.output_topic['value'] + " --thread.nums " + str(flink.thread_num )
        return config
        
    def dispatch_filter(self,filter):
        logging.debug("disp filer %s", filter)
        cmd = 'xterm  -T "filter" -hold  -e ' 
        cmd += self.config_basic_dispatcher(filter) +  " --conditions " + str(filter.conditions) + " &"
        logging.debug('filter -- %s', cmd)
        exe_cmd(cmd)

    def dispatch_obj(self, obj):
        logging.debug("disp obj %s", obj)
        cmd = 'xterm  -T "obj" -hold  -e ' 
        cmd += self.config_basic_dispatcher(obj) +  " --col " + obj.col + " &"
        logging.debug('obj -- %s', cmd)
        exe_cmd(cmd)

    def dispatch_avg(self, avg):
        logging.debug("disp avg %s", avg)
        cmd = 'xterm  -T "avg" -hold  -e ' 
        cmd += self.config_basic_dispatcher(avg) + " --operation avg "+ " --col " + avg.col + " --time.unit " + avg.time['unit'] + " --time.value " + str(avg.time['value'] )+" &"
        logging.debug('avg -- %s', cmd)
        exe_cmd(cmd)
        

    def dispatch_add(self, add):
        logging.debug("disp add %s", add)
        cmd = 'xterm  -T "add" -hold  -e ' 
        cmd += self.config_basic_dispatcher(add) +  " --col1 " + add.col1 + " --col2 " + add.col2 + " --as_col3 " + add.as_col3  +" &"
        logging.debug('add -- %s', cmd)
        exe_cmd(cmd)

    def dispatch_sorter(self,sorter):
        logging.debug("disp sorter %s", sorter)
        cmd = 'xterm  -T "sorter" -hold  -e ' 
        cmd += self.config_basic_dispatcher(sorter) +  " --col " + sorter.col + " --time.unit " + sorter.time['unit'] + " --time.value " + str(sorter.time['value']) + " --order "+ sorter.order + " --limit.bottom " +  str(sorter.limit['bottom']) + " --limit.ceil " +str(sorter.limit['ceil'])+" &"
        logging.debug('add -- %s', cmd)
        exe_cmd(cmd)

    def dispatch_app(self,app):
        if isinstance(app, WebsocketConnector):
            self.dispatch_ws(app)
    
    def dispatch_ws(self, ws):
        logging.debug("disp ws %s", ws)
        cmd = 'xterm  -T "ws" -hold  -e ' 
        cmd += self.config_basic_dispatcher(ws) +  " --html " + ws.html +  " --cols " + ws.cols +  " --class " + ws.css_class +  " --port " + ws.port + " &"
        logging.debug('ws -- %s', cmd)
        exe_cmd(cmd)

    def prioritize_flinks(self, flinks):
        flinks.sort(key=lambda x: x.operation['priority'])
        return flinks

    def config_zookeeper(self, flinks):
        #TODO: optimization, eg, cluster
        for flink in flinks:
            flink.zookeeper ="127.0.0.1:2181"
        return flinks

    def config_brokers(self, flinks):
        #TODO: optimization, eg, cluster
        for flink in flinks:
            flink.brokers ="127.0.0.1:9092"
        return flinks

    def config_thread_nums(self, flinks):
        #TODO: optimization
        for flink in flinks:
            flink.thread_num = 1
        return flinks

    def config_groups(self, flinks):
        #TODO: optimization
        for flink in flinks:
            flink.group = self.session_id
        return flinks




    def config_flinks(self):
        
        flinks = self.map_flinks()
        flinks = self.config_zookeeper(flinks)
        flinks = self.config_brokers(flinks)        
        flinks = self.config_thread_nums(flinks)
        flinks = self.config_groups(flinks)
        flinks = self.prioritize_flinks(flinks)
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

                            for app_conf in flink.conf:
                                key=app_conf["eq"]["literal"][0]
                                value=app_conf["eq"]["literal"][1]
                                if key == "name":
                                    if value == "websocket":
                                        ws = WebsocketConnector()
                                        ws.conf = to_conf
                                        ws.name = value
                                        for ws_conf in flink.conf:
                                            ws_key=ws_conf["eq"]["literal"][0]
                                            ws_value=ws_conf["eq"]["literal"][1]
                                            
                                            if ws_key =="html":
                                                #print("****ws_key html :{} and val :{}".format(ws_key,ws_value))
                                                ws.html = ws_value
                                            
                                            if ws_key =="cols":
                                                #print("****ws_key cols :{} and val :{}".format(ws_key,ws_value))
                                                
                                                if ws_value =="*":
                                                    ws.cols =="\*"
                                                else:
                                                    ws.cols = ws_value


                                            if ws_key == "class" or ws_key =="css_class":
                                                #print("****ws_key class :{} and val :{}".format(ws_key,ws_value))
                                                ws.css_class = ws_value
                                            
                                            if ws_key =="port":
                                                ws.port = ws_value


                                        flinks.append(ws)
                                        logging.info("ws app: %s", ws)
                            
                            self.output_topic['value'] = 'websocket'
                            

                            """
                            if to_conf[0] == 'websocket':
                                self.output_topic['value'] = 'websocket'
                            """
                            
                            
                        
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
        
        if len(topics) > 2:
            logging.debug(topics)
            #exit(0)

        return topics

    def chain_operators(self, flinks):
        operators = []
        for i in range(0, MAX_OPERATOR_PRIORITY):  
            for flink in flinks:
                #logging.debug("****flink in chain ops: %s", flink )
                if flink.operation['priority'] == i:
                    operators.append(flink.operation['name'])
        
        logging.debug(operators)
        

        return operators


    def chain_flinks(self, flinks):
        global all_topics
        #_flinks = flinks 
        chained_flinks=[]
        chain_operators=self.chain_operators(flinks)
        chain_topics = self.chain_topics(chain_operators, flinks)
        logging.debug("****chain_operators: %s", chain_operators )
        logging.debug("****chain_topics: %s", chain_topics )

        #print("chain operators: {}".format(chain_operators))
        #print("chain topics: {}, flinks types {}".format(chain_topics, type(flinks)))

        for f_idx, f_val in enumerate(flinks) :
            if f_idx <= len(flinks )-1 : #ignore the last one
            #    for  ct_idx, ct_val in enumerate(chain_topics):
                flinks[f_idx].input_topic['value'] = chain_topics[f_idx]
                flinks[f_idx].output_topic['value'] = chain_topics[f_idx+1]
                f_val.is_conf_complete = True
            

                chained_flinks.append(f_val)
        """
        if len(chained_flinks) > 2:
            for f in chained_flinks:
                logging.debug(f)
            exit(0)        
        """

        
        return chained_flinks



    def get_in_topic(self):
        pass

    def get_out_topic(self):
        pass


class Flink():
    STREAM_OPERATIONS={
        'filter':{'name':'filter', 'priority':0,'transformation':{'from':'STREAM','to':'STREAM'},'java_app':'FlinkFilter.jar'}, 
        'obj':{'name':'obj', 'priority':1,'transformation':{'from':'STREAM','to':'STREAM'},'java_app':'FlinkListObjectizer.jar'}, 
        'avg':{'name':'avg', 'priority':1,'transformation':{'from':'STREAM','to':'ACC_STREAM'},'java_app':'FlinkAccumulator.jar'}, 
        'add':{'name':'add', 'priority':1,'transformation':{'from':'STREAM','to':'STREAM'},'java_app':'FlinkColAdder.jar'}, 
        'sorter':{'name':'sorter', 'priority':2,'transformation':{'from':'STREAM','to':'WINDOWSED_STREAM'},'java_app':'FlinkSorter.jar'}, 
        'app':{'name':'app', 'priority':3,'transformation':{'from':'STREAM','to':'UNKNOWN'},'java_app':'WebsocketConnector.jar'}
        }
    
    def __init__(self):
        self.java_app_path = '/home/yuchia/workspace/flink-stc-spaas'
        self.log4j2 = "/conf/log4j2.xml"

        self.input_topic = {'id':0, 'value':''}
        self.output_topic = {'id':0, 'value':'', 'type':'', 'conf':{}}
        self.thread_num = 1 

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
        self.conditions = [{"col": "", "value": "", "sign": ""}]

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

class WebsocketConnector(AppConnector):
    def __init__(self):
        AppConnector.__init__(self)
        self.operation=Flink.STREAM_OPERATIONS['app']
        self.name = "websocket"
        self.html ="table"
        self.cols ="\*"
        self.css_class=""
        self.port = "50000"

        #self.conf = {}


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
        logging.info("shell cmd: %s", cmd)
        subprocess.call(cmd, shell=True)
    except Exception as e:
        logging.error("exe_cmd error: %s", str(e))




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
        sessionId=""

        for p in payload.split("&"):
            if "q=" in p:
                payload = p[5:-1]
            elif "field=" in p:
                sessionId = p[6:]
                logging.info('session: %s', sessionId)
                #print("session: {}".format(session))

        # remove /?q=" at the begin and " at the end
        logging.info('request params: %s', payload)

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
                global sessions
                session = Session(payload, sessionId)

                for stm in session.statements:
                    flinks = stm.flinks
                    stm.dispatch_flinks(flinks)

                content = self._handle_http(200, "parse_ok")
                self.wfile.write(content)

                sessions.append(session)

            except Exception as e:
                logging.error("parser error: %s", str(e))
                e.print()
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

def websocket_send(websocket, msg):
    websocket.send( msg)



async def websocket_handler(websocket, path):
    kafka_topic = "websocket"
    kafka_group = "oai"
    kafka_brokers = "127.0.0.1:9092"

    consumer = KafkaConsumer(kafka_topic, auto_offset_reset='latest',enable_auto_commit=False, group_id=kafka_group, bootstrap_servers=[kafka_brokers])
    
    while True:
        for message in consumer:
            print(message.value.decode("utf-8"))
            await websocket.send( message.value.decode("utf-8"))
            cnt +=1
    


async def myfun1():
    print('-- start {}th'.format(2))
    await asyncio.sleep(3)
    # time.sleep(1)
    print('-- finish {}th'.format(2))


if __name__ == "__main__":
    
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
    
    #test codes
    """
    sessions = []

    stm1 = "SELECT OBJ(ue_list) FROM eNB1 TO table(ues)"
    stm2 = "SELECT AVG(total_pdu_bytes_rx) TIME second(0.1) FROM ues WHERE m_id=0  TO app('name'='websocket','cols'='*,col1,col2','class'='12345', 'port'= '5000');"

    #stm1 = "SELECT OBJ(ue_list) FROM eNB1 TO table(ues)"
    #stm2 = "SELECT ADD(rbs_used, rbs_used_rx) as total FROM ues ORDER BY total DESC LIMIT (1,10) TIME ms(1000) TO app('name'='websocket','cols'='*,col1,col2','class'='12345', 'port'= '5000');"

    stms = stm1 + "|" + stm2

    session = Session(stms) 
    #print("session statements: {}".format( session))
    #session.print()

    sessions.append(session)
    
    for sess in sessions:
        sess.print()
        for stm in sess.statements:
            flinks = stm.flinks 
            stm.dispatch_flinks(flinks)
            #stm.value = stm
            #print("session statements: {}".format(stm))
    """    
