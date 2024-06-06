#!/usr/bin/env python3
# -*- coding: utf-8 -*-


# https://docs.uniswap.org/contracts/v2/reference/smart-contracts/router-02
# https://docs.uniswap.org/contracts/universal-router/technical-reference

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
from functools import partial

from utils import hex_to_gwei, hex_to_eth, get_contract_sync, get_contract_standard_token, RED, GREEN, BLUE, RESET_COLOR, gwei_to_wei, AtomicInteger
from token_abi import token_abi
import uniswap

KEY_FILE = '../keys/alchemy.sec'
ETHERSCAN_KEY_FILE = '../keys/etherscan.sec'
HEADERS = {'Content-Type': "application/json"}
ETHERSCAN_GETABI = 'http://api.etherscan.io/api?module=contract&action=getabi&address={}&apikey={}'
PAYLOAD_P = {"jsonrpc":"2.0",
                        "id": 2, 
                        "method": "eth_subscribe", 
                        "params": ["alchemy_pendingTransactions", 
                                   {"toAddress": []}]}

PAYLOAD_M = {"jsonrpc":"2.0",
                        "id": 2, 
                        "method": "eth_subscribe", 
                        "params": ["alchemy_minedTransactions", 
                                   {}]}

PAYLOAD_B = {"jsonrpc":"2.0",
                        "id": 2, 
                        "method": "eth_subscribe", 
                        "params": ["newHeads"]}

d = datetime.now()
UTC_OFFSET = d.timestamp() - datetime.utcfromtimestamp(d.timestamp()).timestamp()

abi_storage = {}
contract_storage = {}
pending_transactions = {}
mined_transactions = {}
token_storage = {}
pair_storage = {}
all_blocks = {}

with open(KEY_FILE, 'r') as f:
    k1 = f.readline()
    alchemy_url = k1.strip('\n')
    k2 = f.readline()
    alchemy_wss = k2.strip('\n')

with open(ETHERSCAN_KEY_FILE, 'r') as f:
    k1 = f.readline()
    etherscan_key = k1.strip('\n')

context = {"abi_storage": abi_storage,
              "contract_storage": contract_storage,
              "etherscan_key": etherscan_key,
              "w3_url": alchemy_url,
              "w3_wss": alchemy_wss,
              "w3": Web3(Web3.HTTPProvider(alchemy_url)),
              "pending_transactions": pending_transactions,
              "mined_transactions": mined_transactions,
              "token_storage": token_storage,
              "all_blocks": all_blocks,
              "pair_storage": pair_storage,
              "gas_price": (0, 0),
              }

WETH = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"
_WETH = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2".lower()
AVAILABLE_TOKENS = {"WETH": _WETH}

token_storage[_WETH] = {"price": 1, "timestamp": 0, "u_contract": None,
                        "t_contract": get_contract_sync(context, WETH),
                        "decimals" :18, "address": "", "suspicious": False}
UNISWAP_V2_FACTORY = "0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f"


TARGET_ADRESSES = {"Uniswap_V2_Router_2": "0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D".lower(),
            "Uniswap_Universal_Router": "0xef1c6e67703c7bd7107eed8303fbe6ec2554bf6b".lower(),
            "UniswapV3SwapRouter02": "0x68b3465833fb72A70ecDF485E0e4C7bD8665Fc45".lower(),
            "UniversalRouter": "0x3fC91A3afd70395Cd496C647d5a6CC9D4B2b7FAD".lower()}

PARSE_FUNCTIONS = {"Uniswap_V2_Router_2": uniswap.uniswap_transaction_decode,
            "Uniswap_Universal_Router": uniswap.uniswap_transaction_decode,
            "UniswapV3SwapRouter02": uniswap.uniswap_transaction_decode,
            "UniversalRouter": uniswap.uniswap_transaction_decode}

EVALUATION_FUNCTIONS = {"Uniswap_V2_Router_2": partial(uniswap.uniswap_evaluate, context=context),
            "Uniswap_Universal_Router": partial(uniswap.uniswap_evaluate, context=context),
            "UniswapV3SwapRouter02": partial(uniswap.uniswap_evaluate, context=context),
            "UniversalRouter": partial(uniswap.uniswap_evaluate, context=context)}


class WebSocketListener:
    def __init__(self, context):
        self.context = context
        self.uri = context["w3_wss"]
        self.w3 = context["w3"]
        self.ws_pending = websocket.WebSocketApp(
            self.uri,
            on_message=self.on_message_p,
            on_error=self.on_error,
            on_close=self.on_close,
            on_open=self.on_open_p)
        self.ws_mined = websocket.WebSocketApp(
            self.uri,
            on_message=self.on_message_m,
            on_error=self.on_error,
            on_close=self.on_close,
            on_open=self.on_open_m)
        self.ws_blocks = websocket.WebSocketApp(
            self.uri,
            on_message=self.on_message_b,
            on_error=self.on_error,
            on_close=self.on_close,
            on_open=self.on_open_b)
        setattr(self.ws_pending, "name", "pending")
        setattr(self.ws_mined, "name", "mined")
        setattr(self.ws_blocks, "name", "blocks")
        
        self.pending_transactions = context["pending_transactions"]
        self.mined_transactions = context["mined_transactions"]
        self.abi_storage = context["abi_storage"]
        self.contract_storage = context["contract_storage"]
        self.token_storage = context["token_storage"]
        self.pair_storage = context["pair_storage"]
        self.payload_p = PAYLOAD_P
        self.payload_p["params"][1]["toAddress"] = list(TARGET_ADRESSES.values())
        self.all_blocks = context["all_blocks"]
        
        self._thread_websocket_p = Thread(target=self.ws_pending.run_forever)
        self._thread_websocket_m = Thread(target=self.ws_mined.run_forever)
        self._thread_websocket_b = Thread(target=self.ws_blocks.run_forever)
        self.run_threads = True
        self.run_thread_close = True
        self.Uniswap_V2_factory_contract = get_contract_sync(context, UNISWAP_V2_FACTORY)

    def on_message_p(self, ws, message_string):
        tx_json = json.loads(message_string)
        if not 'params' in tx_json:
            return

        tx = tx_json['params']['result']
        self.pending_transactions[tx["hash"]] = tx
        tx["scanner_processed"] = self.process_transaction(tx)
        tx["received"] = datetime.utcnow().timestamp()

    def on_message_m(self, ws, message_string):
        tx_json = json.loads(message_string)
        if not 'params' in tx_json:
            return

        tx = tx_json['params']['result']['transaction']
        tx["received"] = datetime.utcnow().timestamp()
        block_number = int(tx['blockNumber'], 0)
        while not block_number in self.all_blocks:
            time.sleep(0.1)
        tx["mined"] = int(self.all_blocks[block_number]["timestamp"], 0) - UTC_OFFSET
        self.mined_transactions[tx["hash"]] = tx
        if not tx["to"] is None:
            self.process_mined_transaction(tx)

    def on_message_b(self, ws, message_string):
        b_json = json.loads(message_string)
        if not 'params' in b_json:
            return
        block_data = b_json["params"]["result"]
        int(block_data["number"], 0)
        self.all_blocks[int(block_data["number"], 0)] = block_data
        self.all_blocks["latest"] = int(block_data["number"], 0)
        self.all_blocks["latest_timestamp"] = int(block_data["timestamp"], 0) - UTC_OFFSET
        self.context["gas_price"] = (self.w3.eth.gas_price/1e9, datetime.utcnow().timestamp())

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

    def on_open_m(self, ws):
        payload_json = json.dumps(PAYLOAD_M)
        self.ws_mined.send(payload_json)

    def on_open_b(self, ws):
        payload_json = json.dumps(PAYLOAD_B)
        self.ws_blocks.send(payload_json)

    # def abi_queue(self):
    #     def get_abi(address):
    #         try:
    #             res = requests.get(ETHERSCAN_GETABI.format(address, etherscan_key), headers=HEADERS)
    #     #!!! no error processing
    #             d = res.json()
    #             abi = d["result"]
    #             return abi
    #         except:
    #             return None

    #     while self.run_threads:
    #         if self.queue_etherscan.qsize() > 0:
    #             address = self.queue_etherscan.get()
    #             self.abi_storage[address] = get_abi(address)
    #             self.requested_abi.remove(address)
    #             time.sleep(0.3)
    #         time.sleep(0.1)

    # def contract_queue(self):
    #     def get_contract(w3, address, abi):
    #         return w3.eth.contract(address=Web3.to_checksum_address(address), abi=abi)
    #     #!!! no error processing

    #     while self.run_threads:
    #         address_list = []
    #         while self.queue_contract.qsize() > 0:
    #             address_list.append(self.queue_contract.get())
            
    #         for address in address_list:
    #             if address in abi_storage:
    #                 retry_address = False
    #                 try:
    #                     self.contract_storage[address] = get_contract(self.w3, address, abi_storage[address])
    #                 except:
    #                     self.queue_contract.put(address)
    #                     continue
    #                 tx_processed_list = []
    #                 for tx_hash in self.unprocessed_pool:
    #                     if not tx_hash in self.pending_transactions:
    #                         retry_address = True
    #                         print("no tx", tx_hash, flush=True)
    #                     elif address == self.pending_transactions[tx_hash]["to"]:
    #                         self._process_transaction(self.pending_transactions[tx_hash])
    #                         self.pending_transactions[tx_hash]["scanner_processed"] = True
    #                         tx_processed_list.append(tx_hash)
    #                 for tx_hash in tx_processed_list:
    #                     self.unprocessed_pool.remove(tx_hash)

    #                 if address in self.unknoun_tokens_pools:
    #                     found_token_tx = []
    #                     for tx_hash in self.unknoun_tokens_pools[address]:
    #                         if not tx_hash in self.pending_transactions:
    #                             retry_address = True
    #                             print("no tx", tx_hash, flush=True)
    #                         else:
    #                             self._process_transaction_fill_tokens(self.pending_transactions[tx_hash])
    #                             found_token_tx.append(tx_hash)
    #                     for tx_hash in found_token_tx:
    #                         self.unknoun_tokens_pools[address].remove(tx_hash)
    #                 if retry_address:
    #                     self.queue_contract.put(address)
    #             else:
    #                 self.queue_contract.put(address)
    #                 if not address in self.requested_abi:
    #                     self.queue_etherscan.put(address)
    #                     self.requested_abi.add(address)
    #         time.sleep(0.1)

    # def try_fill_symbol(self, t):
    #     try:
    #         return self.contract_storage[t].functions.symbol().call()
    #     except:
    #         try:
    #             print("no symbol", t, self.contract_storage[t].functions.name().call(), flush=True)
    #         except:
    #             try:
    #                 symbol_dict = {'inputs': [], 'name': 'symbol', 'outputs': [{'internalType': 'string', 'name': '', 'type': 'string'}], 'stateMutability': 'view', 'type': 'function'}
    #                 lame_abi = json.loads(self.abi_storage[t])
    #                 lame_abi.append(symbol_dict)
    #                 abi_str = json.dumps(lame_abi)
    #                 fixed_contract = self.w3.eth.contract(address=Web3.to_checksum_address(t), abi=abi_str)
    #                 symbol = fixed_contract.functions.symbol().call()
    #                 self.abi_storage[t] = abi_str
    #                 self.contract_storage[t] = fixed_contract
    #                 return symbol
    #             except:
    #                 print("no symbol", t, "no name", flush=True)
    #         return("noname")

    # def _process_transaction_fill_tokens(self, tx):
    #     not_found = 0
    #     if not "analytics" in tx or "tokens" in tx["analytics"]:
    #         return 0
    #     for t in tx["analytics"]["tokens"]:
    #         if t in self.contract_storage:
    #             tx["analytics"]["tokens"][t] = self.try_fill_symbol(t)
    #         if not tx["analytics"]["tokens"][t]:
    #             not_found += 1
    #     return not_found

    # def _process_transaction(self, tx):
    #     address = tx["to"].lower()
    #     tx["decoded_input"] = self.contract_storage[address].decode_function_input(tx["input"])

    #     for ta in target_adresses:
    #         if tx['to'].lower() == target_adresses[ta]:
    #             analytics = self.parse_functions[ta](tx)
    #             analytics["pool"] = ta
                
    #             if "tokens" in analytics:
    #                 for t in analytics["tokens"]:
    #                     if t in self.contract_storage:0xa44671dc7e3bf0e0b9c33bf7c12fd742cb5cec65e46ce1efb476e5f88cf97610
    #                         analytics["tokens"][t] = self.try_fill_symbol(t)
    #                     else:
    #                         self.queue_contract.put(t)
    #                         if not t in self.unknoun_tokens_pools:
    #                             self.unknoun_tokens_pools[t] = set()
    #                         self.unknoun_tokens_pools[t].add(tx["hash"])
    #             tx["analytics"] = analytics

    #             # print(tx["hash"], analytics)
    #     # print(len(self.pending_transactions))
        

    def _process_transaction(self, tx):
        address = tx["to"]
        tx["decoded_input"] = self.contract_storage[address].decode_function_input(tx["input"])

        for ta in TARGET_ADRESSES:
            if address == TARGET_ADRESSES[ta]:
                analytics = PARSE_FUNCTIONS[ta](tx)
                analytics["pool"] = ta
                tx["analytics"] = analytics
                # reserves = self.update_pair(target_pair["token0"], target_pair["token1"])
                
                # if "V2_detected" in analytics and analytics["V2_detected"]:
                    # good_target = EVALUATION_FUNCTIONS[tx["analytics"]["pool"]](tx)
                    # if good_target:
                    #     datetime_utcnow = datetime.utcnow().timestamp()
                    #     last_block = self.all_blocks["last_block"]
                    #     for target_pair in tx["target_pairs"]:
                    #         good_to_try = False

                    #         reserves = self.update_pair(target_pair["token0"], target_pair["token1"])
                    #         result_for_test_amount = target_pair["calculate"](working_amount, reserves[0], reserves[1])


                    #             if (result_for_test_amount > MIN_PROFIT * working_amount and
                    #                 datetime_utcnow - self.all_blocks["last_timestamp"] < TIMEWINDOW_AFTER_BLOCK and 
                    #                 tx["analytics"]["gas_price"] > context["gas_price"][0] * 1.1 and
                    #                 target_pair["optimal_amount"] >= working_amount):
                    #                 good_to_try = True
                    #             if "maxPriorityFeePerGas" in tx and hex_to_gwei(tx["maxPriorityFeePerGas"]) > MAX_MAX_PRIORITY_FEE_PER_GAS:
                    #                 good_to_try = False
                    #             if self.token_storage[target_pair["token0"]]["suspicious"] or self.token_storage[target_pair["token1"]]["suspicious"]:
                    #                 print(RED)
                    #                 print("suspicious token - fee functions detected")
                    #                 print(RESET_COLOR)
                    #                 if result_for_test_amount < MIN_PROFIT_FOR_SUSPICIOUS * working_amount:
                    #                     good_to_try = False


    def process_transaction(self, tx):
        address = tx["to"].lower()
        if address in contract_storage:
            self._process_transaction(tx)
            return True
        else:
            # self.queue_contract.put(address)
            return False

    def process_mined_transaction(self, tx):
        address = tx["to"].lower()
        if tx["hash"] in self.pending_transactions:
            txp = self.pending_transactions[tx["hash"]]
            txp["mined"] = tx["mined"]
            print("tx", tx["hash"],
                  "to", tx["to"],
                  "timespan", tx["mined"] - txp["received"])
            print(txp["analytics"])
        return True

    def start(self):
        self._thread_websocket_p.start()
        self._thread_websocket_m.start()
        self._thread_websocket_b.start()
        # self._thread_etherscan.start()
        # self._thread_contract.start()

    def stop(self):
        self.run_threads = False
        self.ws_pending.keep_running = False
        self.ws_pending.close()
        self.ws_mined.keep_running = False
        self.ws_mined.close()
        self.ws_blocks.keep_running = False
        self.ws_blocks.close()

    def healthy(self):
       return not (self._thread_websocket_p._is_stopped or
           self._thread_websocket_m._is_stopped or self._thread_websocket_b._is_stopped)


def main():

    wsl = WebSocketListener(context)
    
    for a in TARGET_ADRESSES:
        if not TARGET_ADRESSES[a] in contract_storage:
            contract_storage[TARGET_ADRESSES[a]] = get_contract_sync(context, TARGET_ADRESSES[a])
            if not contract_storage[TARGET_ADRESSES[a]]:
                print("contract request failed for address", TARGET_ADRESSES[a])
    wsl.uniswap_router = contract_storage[TARGET_ADRESSES["Uniswap_V2_Router_2"]]
     
    wsl.start()    ### to run :


def garbage_zone(wsl):
    
    wsl.stop()

    wsl.healthy()

    