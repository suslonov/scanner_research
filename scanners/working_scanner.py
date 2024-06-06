#!/usr/bin/env python3
# -*- coding: utf-8 -*-


# https://docs.uniswap.org/contracts/v2/reference/smart-contracts/router-02
# https://docs.uniswap.org/contracts/universal-router/technical-reference

import websocket
import json
import time
from datetime import datetime
import pandas as pd
from threading import Thread
from queue import Queue
from functools import partial

from web3 import Web3

from utils import hex_to_gwei, hex_to_eth, get_contract_sync, get_contract_standard_token, RED, GREEN, BLUE, RESET_COLOR, gwei_to_wei, AtomicInteger
from token_abi import token_abi
import uniswap

KEY_FILE = '../keys/alchemy.sec'
ETHERSCAN_KEY_FILE = '../keys/etherscan.sec'
GAS_PRICE_RENEW = 60
PRICE_RENEW = 600
MIN_PROFIT = 0.01
MIN_PROFIT_FOR_SUSPICIOUS = 0.1
TEST_AMOUNT = 0.1
TIMEWINDOW_AFTER_BLOCK = 11
OPENNING_GAS_PRICE_MULTIPLIER = 1.4
ALLOWANCE_GAS_PRICE = 1.1
CLOSING_GAS_MULTIPLIER = 1.4
MIN_ETH_BALANCE = 0.15
MAX_PRIORITY_FEE_PER_GAS = 2
MAX_MAX_PRIORITY_FEE_PER_GAS = 10
MIN_TOKEN0_BALANCE = 0.7
MAX_PRIORITY_FEE_PER_GAS_CLOSE = 2

d = datetime.now()
UTC_OFFSET = d.timestamp() - datetime.utcfromtimestamp(d.timestamp()).timestamp()

PAYLOAD = {"jsonrpc":"2.0",
                        "id": 2, 
                        "method": "eth_subscribe", 
                        "params": ["alchemy_pendingTransactions", 
                                   {"toAddress": []}]}

PAYLOAD_B = {"jsonrpc":"2.0",
                        "id": 2, 
                        "method": "eth_subscribe", 
                        "params": ["newHeads"]}
# global storages
abi_storage = {}
contract_storage = {}
pending_transactions = {}
all_messages = []
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
              "all_messages": all_messages,
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
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close,
            on_open=self.on_open)
        self.ws_blocks = websocket.WebSocketApp(
            self.uri,
            on_message=self.on_message_b,
            on_error=self.on_error,
            on_close=self.on_close,
            on_open=self.on_open_b)
        
        self.all_messages = context["all_messages"]
        self.pending_transactions = context["pending_transactions"]
        self.abi_storage = context["abi_storage"]
        self.contract_storage = context["contract_storage"]
        self.token_storage = context["token_storage"]
        self.pair_storage = context["pair_storage"]
        self.payload = PAYLOAD
        self.payload["params"][1]["toAddress"] = list(TARGET_ADRESSES.values())
        self.all_blocks = context["all_blocks"]
        
        self._thread_websocket = Thread(target=self.ws_pending.run_forever)
        self._thread_websocket_b = Thread(target=self.ws_blocks.run_forever)
        self.run_threads = True
        self.run_thread_close = True
        self.fire = True
        self.Uniswap_V2_factory_contract = get_contract_sync(context, UNISWAP_V2_FACTORY)
        
    def renew_prices(self):
        while self.run_threads:
            datetime_utcnow = datetime.utcnow().timestamp()
            if datetime_utcnow - self.context["gas_price"][1] > GAS_PRICE_RENEW:
                self.context["gas_price"] = (self.w3.eth.gas_price/1e9, datetime.utcnow().timestamp())
            for token in self.token_storage.copy():
                if token == _WETH:
                    continue
                if datetime_utcnow - self.token_storage[token]["timestamp"] > PRICE_RENEW:
                    if self.token_storage[token]["u_contract"] is None:
                        u_contract = None
                        if not self.token_storage[token]["timestamp"]:
                            u_contract = get_contract_sync(self.context, self.Uniswap_V2_factory_contract.functions.getPair(Web3.to_checksum_address(token), WETH).call())
                            t_contract = get_contract_sync(self.context, Web3.to_checksum_address(token))
                            if u_contract is None or t_contract is None:
                                continue
                            try:
                                token_decimals = t_contract.functions.decimals().call()
                            except:
                                try:
                                    t_contract = get_contract_standard_token(self.w3, Web3.to_checksum_address(token))
                                    token_decimals = t_contract.functions.decimals().call()
                                except:
                                    print("incorrect token decimals", token)
                                    continue
                            suspicious = False
                            for f_t in dir(t_contract.functions):
                                if f_t.lower().find("tax") >= 0 or f_t.lower().find("fee") >= 0 or f_t.lower().find("limit") >= 0:
                                    suspicious = True
                                    break
                            token0 = u_contract.functions.token0().call()
                    else:
                        u_contract = self.token_storage[token]["u_contract"]
                        t_contract = self.token_storage[token]["t_contract"]
                        token_decimals = self.token_storage[token]["decimals"]
                        token0 = self.token_storage[token]["address"]

                    if not u_contract is None:
                        reserves = u_contract.functions.getReserves().call()
                        if token0 == WETH:
                            if reserves[1] != 0:
                                self.token_storage[token] = {"price": reserves[0]/reserves[1] / (10 ** (18 - token_decimals)),
                                                     "timestamp": datetime_utcnow, "u_contract": u_contract, "t_contract": t_contract, "decimals" :token_decimals, "address": token0, "suspicious": suspicious}
                        else:
                            if reserves[0] != 0:
                                self.token_storage[token] = {"price": reserves[1]/reserves[0] / (10 ** (18 - token_decimals)),
                                                     "timestamp": datetime_utcnow, "u_contract": u_contract, "t_contract": t_contract, "decimals" :token_decimals, "address": token0, "suspicious": suspicious}
            for pair in self.pair_storage.copy():
                if datetime_utcnow - self.pair_storage[pair][1] > PRICE_RENEW:
                    if self.pair_storage[pair][2] is None:
                        u_contract = None
                        if not self.pair_storage[pair][1]:
                            # try:
                            u_contract = get_contract_sync(self.context, self.Uniswap_V2_factory_contract.functions.getPair(Web3.to_checksum_address(pair[0]), Web3.to_checksum_address(pair[1])).call())
                            if not u_contract is None:
                                token0 = u_contract.functions.token0().call().lower()
                            # except:
                                # pass
                    else:
                        u_contract = self.pair_storage[pair][2]
                        token0 = self.pair_storage[pair][3]
                    if not u_contract is None and self.token_storage[pair[0]]["decimals"] and self.token_storage[pair[1]]["decimals"]:
                        reserves = u_contract.functions.getReserves().call()
                        if token0 == pair[0]:
                            self.pair_storage[pair] = ((reserves[0] / (10 ** self.token_storage[pair[0]]["decimals"]), reserves[1] / (10 ** self.token_storage[pair[1]]["decimals"])),
                                                     datetime_utcnow, u_contract, token0)
                        else:
                            self.pair_storage[pair] = ((reserves[1] / (10 ** self.token_storage[pair[0]]["decimals"]), reserves[0] / (10 ** self.token_storage[pair[1]]["decimals"])),
                                                     datetime_utcnow, u_contract, token0)
            time.sleep(1)

    def on_message(self, ws, message_string):
        self.all_messages.append(message_string)
        tx_json = json.loads(message_string)
        if not 'params' in tx_json:
            return

        tx = tx_json['params']['result']
        tx["received"] = datetime.utcnow().timestamp()
        self.pending_transactions[tx["hash"]] = tx
        tx["scanner_processed"] = self.process_transaction(tx)

    def on_message_b(self, ws, message_string):
        b_json = json.loads(message_string)
        if not 'params' in b_json:
            return
        block_data = b_json["params"]["result"]
        int(block_data["number"], 0)
        self.all_blocks[int(block_data["number"], 0)] = block_data
        self.all_blocks["last_block"] = int(block_data["number"], 0)
        self.all_blocks["last_timestamp"] = int(block_data["timestamp"], 0) - UTC_OFFSET

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

    def on_open_b(self, ws):
        payload_json = json.dumps(PAYLOAD_B)
        self.ws_blocks.send(payload_json)

    def update_pair(self, token0, token1):
        u_contract = self.pair_storage[(token0, token1)][2]
        base_token0 = self.pair_storage[(token0, token1)][3]
        reserves = u_contract.functions.getReserves().call()
        datetime_utcnow = datetime.utcnow().timestamp()
        if token0 == base_token0:
            self.pair_storage[(token0, token1)] = ((reserves[0] / (10 ** self.token_storage[token0]["decimals"]), reserves[1] / (10 ** self.token_storage[token1]["decimals"])),
                    datetime_utcnow, u_contract, base_token0)
        else:
            self.pair_storage[(token0, token1)] = ((reserves[1] / (10 ** self.token_storage[token0]["decimals"]), reserves[0] / (10 ** self.token_storage[token1]["decimals"])),
                    datetime_utcnow, u_contract, base_token0)
        return self.pair_storage[(token0, token1)][0]

    def _process_transaction(self, tx):
        address = tx["to"]
        tx["decoded_input"] = self.contract_storage[address].decode_function_input(tx["input"])

        for ta in TARGET_ADRESSES:
            if address == TARGET_ADRESSES[ta]:
                analytics = PARSE_FUNCTIONS[ta](tx)
                analytics["pool"] = ta
                tx["analytics"] = analytics
                
                if "V2_detected" in analytics and analytics["V2_detected"]:
                    good_target = EVALUATION_FUNCTIONS[tx["analytics"]["pool"]](tx)
                    if good_target:
                        datetime_utcnow = datetime.utcnow().timestamp()
                        last_block = self.all_blocks["last_block"]
                        for target_pair in tx["target_pairs"]:
                            
                            # if not self.fire:
                                # working_amount = target_pair["optimal_amount"]
                            # else:
                            working_amount = TEST_AMOUNT
                            good_to_try = False
                            result_for_test_amount = None
                            result_for_test_amount_old = None
                            if target_pair["token0"] in AVAILABLE_TOKENS.values():
                                print(BLUE)
                                print(tx["hash"])
                                print(RESET_COLOR)
                                reserves_old = self.pair_storage[(target_pair["token0"], target_pair["token1"])][0]
                                result_for_test_amount_old = target_pair["calculate"](working_amount, reserves_old[0], reserves_old[1])
                                reserves = self.update_pair(target_pair["token0"], target_pair["token1"])
                                result_for_test_amount = target_pair["calculate"](working_amount, reserves[0], reserves[1])

                                # print(result_for_test_amount > MIN_PROFIT,
                                #     datetime_utcnow - self.all_blocks["last_timestamp"] < TIMEWINDOW_AFTER_BLOCK,
                                #     tx["analytics"]["gas_price"] > context["gas_price"][0] * 1.1,
                                #     target_pair["optimal_amount"] >= working_amount,
                                #     not "maxPriorityFeePerGas" in tx or not hex_to_gwei(tx["maxPriorityFeePerGas"]) > MAX_MAX_PRIORITY_FEE_PER_GAS)

                                if (result_for_test_amount > MIN_PROFIT * working_amount and
                                    datetime_utcnow - self.all_blocks["last_timestamp"] < TIMEWINDOW_AFTER_BLOCK and 
                                    tx["analytics"]["gas_price"] > context["gas_price"][0] * 1.1 and
                                    target_pair["optimal_amount"] >= working_amount):
                                    good_to_try = True
                                if "maxPriorityFeePerGas" in tx and hex_to_gwei(tx["maxPriorityFeePerGas"]) > MAX_MAX_PRIORITY_FEE_PER_GAS:
                                    good_to_try = False
                                if self.token_storage[target_pair["token0"]]["suspicious"] or self.token_storage[target_pair["token1"]]["suspicious"]:
                                    print(RED)
                                    print("suspicious token - fee functions detected")
                                    print(RESET_COLOR)
                                    if result_for_test_amount < MIN_PROFIT_FOR_SUSPICIOUS * working_amount:
                                        good_to_try = False

                                if good_to_try:
                                    print(GREEN)
                                    print("start", datetime_utcnow, "block", last_block, self.all_blocks["last_timestamp"], "difference", datetime_utcnow - self.all_blocks["last_timestamp"])

                                    token0 = target_pair["token0"]
                                    token1 = target_pair["token1"]
                                    
                                    self.queue_open.put((tx, token0, token1, working_amount, last_block))

                            print(target_pair["token0"])
                            print(target_pair["token1"])
                            print(target_pair["value"],
                                  tx["analytics"]["gas_price"],
                                  target_pair["optimal_amount"], 
                                  target_pair["profit"], 
                                  result_for_test_amount,
                                  "////",
                                  result_for_test_amount_old)
                            # print(reserves, "////", reserves_old)

                            if good_to_try:
                                print(RESET_COLOR)
                                # if not self.fire:
                                #     self.stop()  # !!! temp
        
    def process_transaction(self, tx):
        tx["to"] = tx["to"].lower()
        if tx["to"] in contract_storage:
            self._process_transaction(tx)
            return True
        else:
            return False
    
    def start(self):
        self._thread_websocket.start()
        self._thread_websocket_b.start()

    def finish(self):
        self.run_threads = False
        self.ws_pending.keep_running = False
        self.ws_pending.close()
        self.ws_blocks.keep_running = False
        self.ws_blocks.close()

    def stop(self):
        self.run_threads = False
        self.run_thread_close = False
        self.ws_pending.keep_running = False
        self.ws_pending.close()
        self.ws_blocks.keep_running = False
        self.ws_blocks.close()

    def healthy(self):
       return not (self._thread_websocket._is_stopped or
                   self._thread_websocket_b._is_stopped)

def main():


    wsl = WebSocketListener(context)

    for a in TARGET_ADRESSES:
        if not TARGET_ADRESSES[a] in contract_storage:
            contract_storage[TARGET_ADRESSES[a]] = get_contract_sync(context, TARGET_ADRESSES[a])
            if not contract_storage[TARGET_ADRESSES[a]]:
                print("contract request failed for address", TARGET_ADRESSES[a])
    wsl.uniswap_router = contract_storage[TARGET_ADRESSES["Uniswap_V2_Router_2"]]
    
    
    wsl.start()    ### to run



def garbage_zone(wsl):
    
    wsl.finish()    ### to stop
    wsl.stop()    ### to stop

    wsl._thread_websocket
    wsl._thread_websocket_b
    wsl._thread_prices
    wsl._thread_open
    wsl._thread_close
    
    tx = pending_transactions["0x8b90851c74ade401e519d949ebe91bf803457d4a8d5a49ff878b0af106943eef"]

