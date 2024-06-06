#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import websocket
import json
import pandas as pd

KEY_FILE = 'alchemy.sec'
RED = "\033[1;31m"
RESET_COLOR = "\033[0;0m"

try:
    with open(KEY_FILE, 'r') as f:
        k1 = f.readline()
        alchemy_url = k1.strip('\n')
        k2 = f.readline()
        alchemy_wss = k2.strip('\n')
except:
    alchemy_url = ""
    alchemy_wss = ""


Uniswap_V2_Router_2 = "0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D".lower()
Uniswap_Universal_Router = "0xef1c6e67703c7bd7107eed8303fbe6ec2554bf6b".lower()
Jared_favorite_mev_bot = "0x6b75d8af000000e20b7a7ddf000ba900b4009a80".lower()
Jared = "0xae2Fc483527B8EF99EB5D9B44875F005ba1FaE13".lower()

payload = {"jsonrpc":"2.0",
           "id": 2, 
           "method": "eth_subscribe", 
           "params": ["alchemy_pendingTransactions", 
                      {"toAddress": [Uniswap_V2_Router_2, Uniswap_Universal_Router, Jared_favorite_mev_bot]}]}

dict_pending = {}
messages = []

class WebSocketListener:

    def __init__(self, alchemy_wss, payload):
        self.ws = websocket.WebSocketApp(
            alchemy_wss,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close,
            on_open=self.on_open)
        self.payload = payload

    def on_message(self, ws, message_string):
        messages.append(message_string)
        try:
            tx = json.loads(message_string)['params']['result']
            dict_pending[tx["hash"]] = tx
        except:
            return
    
        if tx['to'].lower() == Uniswap_Universal_Router:
            print("to Uniswap_Universal_Router", tx["hash"])
        elif tx['to'].lower() == Uniswap_V2_Router_2:
            print("to Uniswap_V2_Router_2", tx["hash"])
        elif tx['from'].lower() == Jared:
            print(RED, "from Jared", RESET_COLOR, tx["hash"])
        print(len(dict_pending))

    def on_error(self, ws, error):
        print(error)
    
    def on_close(self, ws, **args):
        print("on_close", **args)

    def on_open(self, ws):
        payload_json = json.dumps(self.payload)
        self.ws.send(payload_json)

    def __enter__(self):
        self.ws.run_forever()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        print("WebSocket closed")
        self.ws.close()


with WebSocketListener(alchemy_wss, payload) as ws:
    print("indefinite run")

# print("hash=", list(dict_pending.keys())[-1])
# print("transaction=", dict_pending[list(dict_pending.keys())[-1]])

# tx = dict_pending[list(dict_pending.keys())[-1]]
# address = tx["to"]
# requrl = 'http://api.etherscan.io/api?module=contract&action=getabi&address=' + address

# import requests

# res = requests.get(requrl, headers={'Content-Type': "application/json"})
# d = res.json()
# abi = d["result"]

# from web3 import Web3

# provider = Web3.HTTPProvider(alchemy_url)
# w3 = Web3(provider)
# contract = w3.eth.contract(address=Web3.to_checksum_address(address), abi=abi)

# iiii = contract.decode_function_input(tx["input"])
# address1 = iiii[1]['path'][0]
# requrl = 'http://api.etherscan.io/api?module=contract&action=getabi&address=' + address1
# res = requests.get(requrl, headers={'Content-Type': "application/json"})
# d = res.json()
# abi1 = d["result"]
# contract1 = w3.eth.contract(address=Web3.to_checksum_address(address1), abi=abi1)
# token_name = contract1.functions.name().call() 
# token_symbol = contract1.functions.symbol().call() 

