#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import websocket
import json
import time
from datetime import datetime, timedelta
from tzlocal import get_localzone_name
import pytz
import pandas as pd
from threading import Thread

from web3 import Web3

from utils import RED, GREEN, BLUE, RESET_COLOR

KEY_FILE = '../keys/alchemy.sec'

with open(KEY_FILE, 'r') as f:
    k1 = f.readline()
    alchemy_url = k1.strip('\n')
    k2 = f.readline()
    alchemy_wss = k2.strip('\n')

d = datetime.now()
UTC_OFFSET = d.timestamp() - datetime.utcfromtimestamp(d.timestamp()).timestamp()

# global storages
pending_transactions = {}
pending_transactions_2 = {}

# w3 = Web3(Web3.HTTPProvider(alchemy_url))

w3 = Web3(Web3.HTTPProvider(alchemy_url))

PAYLOAD = {"jsonrpc":"2.0",
           "id": 2, 
           "method": "eth_subscribe", 
           # "params": ["alchemy_pendingTransactions", {}],
            "params": ["newPendingTransactions"],
           }


context = {"w3_url": alchemy_url,
           "w3_wss": alchemy_wss,
           "payload": PAYLOAD,
           "w3": w3,
           "pending_transactions": pending_transactions,
           "name": "alchemy",
          }

URL_2 = "http://3.68.124.134:8545"
w3_2 = Web3(Web3.HTTPProvider(URL_2, request_kwargs={'verify': False}))

PAYLOAD_2 = {"jsonrpc":"2.0",
           "id": 2, 
           "method": "eth_subscribe", 
           # "params": ["newHeads"],
            "params": ["newPendingTransactions"],
           }

context_2 = {"w3_url": URL_2,
             "w3_wss": "ws://3.68.124.134:8546",
             "payload": PAYLOAD_2,
             "w3": w3_2,
             "pending_transactions": pending_transactions_2,
             "name": "local",
            }

class CollectorComparer():
    def __init__(self, name1, name2):
        self.name1 = name1
        self.name2 = name2
        self.set1 = set()
        self.set2 = set()
        self.common_set = set()
        
    def add_hash(self, name, hash_to_add):
        if name == self.name1:
            if hash_to_add in self.set2:
                self.set2.remove(hash_to_add)
                self.common_set.add(hash_to_add)
            else:
                self.set1.add(hash_to_add)
        else:
            if hash_to_add in self.set1:
                self.set1.remove(hash_to_add)
                self.common_set.add(hash_to_add)
            else:
                self.set2.add(hash_to_add)
                
    def re_check(self):
        for hash_to_add in list(self.set1):
            if hash_to_add in self.set2:
                self.set1.remove(hash_to_add)
                self.set2.remove(hash_to_add)
                self.common_set.add(hash_to_add)
        for hash_to_add in list(self.set2):
            if hash_to_add in self.set1:
                self.set1.remove(hash_to_add)
                self.set2.remove(hash_to_add)
                self.common_set.add(hash_to_add)
                
    def compare(self):
        return {"in " + self.name1 + " only": len(self.set1), 
                "in " + self.name2 + " only": len(self.set2), 
                "available from both": len(self.common_set)}
            

class WebSocketListener:
    def __init__(self, context):
        self.context = context
        self.uri = context["w3_wss"]
        self.w3 = context["w3"]
        self.name = context["name"]
        self.ws_pending = websocket.WebSocketApp(
            self.uri,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close,
            on_open=self.on_open)
        
        self.pending_transactions = context["pending_transactions"]
        self.payload = context["payload"]
        
        self._thread_websocket = Thread(target=self.ws_pending.run_forever)
        self.run_threads = True
        self.run_thread_close = True
        
        self.collector_comparer = context["collector_comparer"]

        
    def on_message(self, ws, message_string):
        # print(message_string)
        try:
            res = json.loads(message_string)
            self.collector_comparer.add_hash(self.name, res["params"]["result"])
        except:
            pass
        # tx_json = json.loads(message_string)
        # if not 'params' in tx_json:
        #     return

        # tx = tx_json['params']['result']
        # tx["received"] = datetime.utcnow().timestamp()
        # self.pending_transactions[tx["hash"]] = tx

    def on_error(self, ws, error):
        print(RED)
        print("error", error)
        print(RESET_COLOR)
    
    def on_close(self, ws, *args):
        if self.run_threads:
            time.sleep(1)
            print("restart after 1 sec", flush=True)
            ws.run_forever()

    def on_open(self, ws):
        payload_json = json.dumps(self.payload)
        self.ws_pending.send(payload_json)

    def start(self):
        self._thread_websocket.start()

    def finish(self):
        self.run_threads = False
        self.ws_pending.keep_running = False
        self.ws_pending.close()

    def stop(self):
        self.run_threads = False
        self.run_thread_close = False
        self.ws_pending.keep_running = False
        self.ws_pending.close()

    def healthy(self):
       return not (self._thread_websocket._is_stopped)


def main():

    collector_comparer = CollectorComparer("local", "alchemy")

    context["collector_comparer"] = collector_comparer
    wsl = WebSocketListener(context)
    wsl.start()    ### to run

    context_2["collector_comparer"] = collector_comparer
    wsl_2 = WebSocketListener(context_2)
    wsl_2.start()    ### to run

    while True:  
        time.sleep(60)
        collector_comparer.re_check()
        print(datetime.now(), collector_comparer.compare())


def stats(w3_2, collector_comparer):
    
    EASTERN = pytz.timezone('US/Eastern'); JERUSALEM = pytz.timezone('Asia/Jerusalem'); UTC = pytz.UTC
    LOCAL_TIMEZONE = pytz.timezone(get_localzone_name())
    
    latest_block = w3_2.eth.get_block('latest')
    latest_block_number = latest_block["number"]
    latest_block_timestamp = latest_block["timestamp"]
    date_now = datetime.fromtimestamp(latest_block_timestamp, LOCAL_TIMEZONE).astimezone(UTC)
    
    date_end = date_now
    n_days = 1
    date_start = date_now - timedelta(days=n_days) # -  timedelta(hours=4)
    block_start_number = latest_block_number - int((date_now - date_start).total_seconds() / 12)
    
    block_stats = {}
    for block_number in range(block_start_number, latest_block_number + 1):
        block = w3_2.eth.get_block(block_number, full_transactions=False)
        block_transactions = block["transactions"]
    
        local_count = 0
        alchemy_count = 0
        crossing_count = 0
        for transaction in block_transactions:
            if transaction.hex() in collector_comparer.set1:
                local_count += 1
            elif transaction.hex() in collector_comparer.set2:
                alchemy_count += 1
            elif transaction.hex() in collector_comparer.common_set:
                crossing_count += 1
            out_from_pools = len(block_transactions) - local_count - alchemy_count - crossing_count
        block_stats[block_number] = {"local": local_count, "alchemy": alchemy_count, "crossing": crossing_count, "out": out_from_pools}
            
    df_block_stats = pd.DataFrame.from_records(block_stats).T
    df_block_stats.to_csv("/media/Data/csv/df_block_stats.csv")



def garbage_zone(wsl, wsl_2):
    
    wsl.finish()    ### to stop
    wsl.stop()    ### to stop

    wsl_2.finish()    ### to stop
    wsl_2.stop()    ### to stop

    wsl_2._thread_websocket
 
    latest_block = w3_2.eth.get_block('latest')
    w3_2.eth.get_block('latest').number
 
    block_num = 17765571
    tx_num = w3_2.eth.get_block_transaction_count(block_num)

