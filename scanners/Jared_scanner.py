#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
os.sys.path.append(os.path.dirname(os.path.abspath('.')))
import websocket
import requests
from web3 import Web3
import json
import time
from datetime import datetime
import pandas as pd
import numpy as np
from threading import Thread
from queue import Queue

from _utils.utils import hex_to_gwei, hex_to_eth, RED, GREEN, BLUE, RESET_COLOR, gwei_to_wei, AtomicInteger


KEY_FILE = '../keys/alchemy.sec'

PAYLOAD_P = {"jsonrpc":"2.0",
                        "id": 2, 
                        "method": "eth_subscribe", 
                        "params": ["newPendingTransactions"]
                        }

PAYLOAD_B = {"jsonrpc":"2.0",
                        "id": 2, 
                        "method": "eth_subscribe", 
                        "params": ["newHeads"]}
JARED = "0x6b75d8AF000000e20B7a7DDf000Ba900b4009A80"

d = datetime.now()
UTC_OFFSET = round(d.timestamp() - datetime.utcfromtimestamp(d.timestamp()).timestamp(), 0)


with open(KEY_FILE, 'r') as f:
    k1 = f.readline()
    alchemy_url = k1.strip('\n')
    k2 = f.readline()
    alchemy_wss = k2.strip('\n')

def connect():
    w3_0 = Web3(Web3.HTTPProvider(alchemy_url))
    latest_block = w3_0.eth.get_block("latest")
    return w3_0, latest_block

class WebSocketListener:
    def __init__(self, context):
        self.context = context
        self.uri = context["alchemy_wss"]
        self.w3 = context["w3"]
        self.ws_pending = websocket.WebSocketApp(
            self.uri,
            on_message=self.on_message_p,
            on_error=self.on_error,
            on_close=self.on_close,
            on_open=self.on_open_p)
        self.ws_blocks = websocket.WebSocketApp(
            self.uri,
            on_message=self.on_message_b,
            on_error=self.on_error,
            on_close=self.on_close,
            on_open=self.on_open_b)

        setattr(self.ws_pending, "name", "pending")
        setattr(self.ws_blocks, "name", "blocks")
        
        self.pending_transactions = context["pending_transactions"]
        self.jared_transactions = context["jared_transactions"]
        self.all_blocks = context["all_blocks"]

        self.payload_p = PAYLOAD_P
        # self.payload_p["params"][1]["toAddress"] = list(TARGET_ADRESSES.values())
        
        self._thread_websocket_p = Thread(target=self.ws_pending.run_forever)
        self._thread_websocket_b = Thread(target=self.ws_blocks.run_forever)
        self.queue_blocks = Queue()
        self._thread_blocks = Thread(target=self.block_loop)

        self.run_threads = True
        self.run_thread_close = True

    def on_message_p(self, ws, message_string):
        tx_json = json.loads(message_string)
        if not 'params' in tx_json:
            return
        tx_hash = tx_json['params']['result']
        self.pending_transactions[tx_hash] = {"received": datetime.utcnow().timestamp()}

    def on_message_b(self, ws, message_string):
        b_json = json.loads(message_string)
        if not 'params' in b_json:
            return
        block_data = b_json["params"]["result"]
        block_number = int(block_data["number"], 0)
        self.all_blocks[block_number] = block_data
        self.all_blocks["latest"] = block_number
        self.all_blocks["latest_timestamp"] = int(block_data["timestamp"], 0) - UTC_OFFSET
        self.queue_blocks.put((block_number))

    def on_error(self, ws, error):
        print(RED)
        print("error", ws.name, error)
        print(RESET_COLOR)
    
    def on_close(self, ws, *args):
        if self.run_threads:
            time.sleep(1)
            print("restart after 1 sec", flush=True)
            ws.run_forever()

    def on_open_p(self, ws):
        payload_json = json.dumps(self.payload_p)
        self.ws_pending.send(payload_json)

    def on_open_b(self, ws):
        payload_json = json.dumps(PAYLOAD_B)
        self.ws_blocks.send(payload_json)

    def block_loop(self, ):
        while self.run_threads:
            while self.queue_blocks.qsize() > 0:
                (block_number) = self.queue_blocks.get()
                block = self.w3.eth.get_block(block_number, full_transactions=True)
                
                i_start = None
                for i, t in enumerate(block["transactions"]):
                    if t["to"] == JARED:
                        i_start = i + 1
                        break
                else:
                    continue
                if i_start is None:
                    continue
                   
                i_end = None
                for i, t in enumerate(block["transactions"][::-1]):
                    if t["to"] == JARED:
                        i_end = len(block["transactions"]) - i - 1
                        break
                if i_end is None:
                    continue
                block_timestamp = block["timestamp"] - UTC_OFFSET
                for i in range(i_start, i_end):
                    tx_hash = block["transactions"][i]["hash"].hex()
                    self.jared_transactions[tx_hash] = {"block_number": block_number,
                                                        "timestamp": block_timestamp,
                                                        "to": block["transactions"][i]["to"],
                                                        "mempool": False}
                    if tx_hash in self.pending_transactions:
                        self.jared_transactions[tx_hash]["mempool"] = True
                        self.jared_transactions[tx_hash]["time_gap"] = block_timestamp - self.pending_transactions[tx_hash]["received"]
               
            time.sleep(0.1)

    def start(self):
        self._thread_websocket_p.start()
        time.sleep(100)
        self._thread_websocket_b.start()
        self._thread_blocks.start()

    def stop(self):
        self.run_threads = False
        self.ws_pending.keep_running = False
        self.ws_pending.close()
        self.ws_blocks.keep_running = False
        self.ws_blocks.close()

    def healthy(self):
       return not (self._thread_websocket_p._is_stopped or
           self._thread_blocks._is_stopped or self._thread_websocket_b._is_stopped)

    def count_jared_transactions(self):
        counter = 0; gaps = 0; later = 0
        infected = set()
        for t in self.jared_transactions:
            if self.jared_transactions[t]["mempool"]:
                counter += 1
                infected.add(self.jared_transactions[t]["block_number"])
                gaps += self.jared_transactions[t]["time_gap"]
            elif t in self.pending_transactions:
                later += 1
        return {"mempool": counter,
                "total": len(self.jared_transactions),
                "mempool_share": "{:.2%}".format(counter/len(self.jared_transactions)),
                "num_blocks": len(self.all_blocks),
                "blocks_share": "{:.2%}".format(len(infected)/len(self.all_blocks)),
                "average_gap": gaps/counter,
                "arrived_later": later}

    def gap_distribution(self):
        gaps = []
        for t in self.jared_transactions:
            if self.jared_transactions[t]["mempool"]:
                gaps.append(self.jared_transactions[t]["time_gap"])
                if self.jared_transactions[t]["time_gap"] > 100:
                    print(t, self.jared_transactions[t])
        gaps.sort()
        gaps.reverse()
        return gaps

    def to_distribution(self):
        to = {}
        for t in self.jared_transactions:
            if self.jared_transactions[t]["mempool"]:
                if not self.jared_transactions[t]["to"] in to:
                    to[self.jared_transactions[t]["to"]] = 1
                else:
                    to[self.jared_transactions[t]["to"]] += 1
                    
        return to



def main():
    w3, _= connect()
    
    pending_transactions = {}
    jared_transactions = {}
    all_blocks = {}

    run_context = {"w3": w3,
                "alchemy_wss": alchemy_wss,
                "pending_transactions": pending_transactions,
                "jared_transactions": jared_transactions,
                "all_blocks": all_blocks,
                }

    wsl = WebSocketListener(run_context)

    wsl.start()    ### to run

    while True:
        time.sleep(60)
        print(wsl.count_jared_transactions())


def garbage_zone(wsl):
    
    wsl.stop()

    wsl.healthy()

#   print(count_jared_transactions(wsl))

    print(wsl.count_jared_transactions())
    
    to = to_distribution(wsl)


if __name__ == '__main__':
    main()
