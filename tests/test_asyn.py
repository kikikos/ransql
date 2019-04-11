#!/usr/bin/env python3

import asyncio
import datetime
import random
import websockets
import asyncio
import time

async def websocket_handler(websocket, path):
    while True:
        now = datetime.datetime.utcnow().isoformat() + 'Z'
        await websocket.send(now)
        await asyncio.sleep(random.random() * 3)

async def run_wb_server():
    start_server = websockets.serve(websocket_handler, '127.0.0.1', 5678)
    #asyncio.get_event_loop().run_until_complete(start_server)
    #asyncio.get_event_loop().run_forever()

async def myfun1():    
    print('- start {}th'.format(1))    
    await asyncio.sleep(30)    
    #time.sleep(1)
    print('- finish {}th'.format(1))


async def myfun2():    
    print('-- start {}th'.format(2))    
    await asyncio.sleep(1)    
    #time.sleep(1)
    print('-- finish {}th'.format(2))



    
loop= asyncio.get_event_loop()
#myfun_list = (myfun(i) for i in range(10))
myfun_list = (myfun1(),myfun2(), websockets.serve(websocket_handler, '127.0.0.1', 5678))
print(".") 
loop.run_until_complete(asyncio.gather(*myfun_list))
loop.run_forever()
print("..")
