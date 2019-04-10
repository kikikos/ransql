#!/usr/bin/env python3

import asyncio
import time
import datetime

async def display_date2(counter):
    #end_time = loop.time() + 5.0
    #print(counter,":::",datetime.datetime.now())
    print(counter,"::","*")

    time.sleep(5)
    #await asyncio.sleep(5)
    print(counter,"::","**")
    

async def display_date(loop):
    end_time = loop.time() + 5.0
    counter =0
    while True:
        #print(counter,":",datetime.datetime.now())
        if (loop.time() + 1.0) >= end_time:
            break
        print(counter,":","*")
        
        display_date2(counter)#asyncio.sleep(1)
        #time.sleep(1)
        print(counter,":","**")
        counter +=1

loop = asyncio.get_event_loop()
# Blocking call which returns when the display_date() coroutine is done
loop.run_until_complete(display_date(loop))

loop.close()
