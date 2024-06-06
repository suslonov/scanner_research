#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import requests
import time
import json

from web3 import Web3
from _utils.token_abi import token_abi
from _utils.kermit_abi import kermit_abi
from _utils.UniswapV2Pair import pair_abi
from _utils.UniswapV3Pool import pool_abi

MAX_RETRY = 10

USDC_LIKE = ["0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48".lower(), '0x0000000000085d4780b73119b644ae5ecd22b376'.lower()]

HEADERS = {'Content-Type': "application/json"}
ETHERSCAN_GETABI = 'http://api.etherscan.io/api?module=contract&action=getabi&address={}&apikey={}'
ETHERSCAN_GETINTERNALS = 'http://api.etherscan.io/api?module=account&action=txlistinternal&address={}&startblock={}&endblock={}&apikey={}'
ETHERSCAN_GETINTERNALS_TX = 'http://api.etherscan.io/api?module=account&action=txlistinternal&txhash={}&startblock={}&endblock={}&apikey={}'
ETHERSCAN_GETLOGS = 'http://api.etherscan.io/api?module=logs&action=getLogs&address={}&page={}&offset={}&apikey={}'
ETHERSCAN_GETLOGS1 = 'http://api.etherscan.io/api?module=logs&action=getLogs&fromBlock={}&address={}&page={}&offset={}&apikey={}'
ETHERSCAN_GETTOKENS = 'http://api.etherscan.io/api?module=account&action=tokentx&address={}&page={}&offset={}&apikey={}'
ETHERSCAN_GETTOKENS1 = 'http://api.etherscan.io/api?module=account&action=tokentx&startblock={}&address={}&page={}&offset={}&apikey={}'

TRACE_TRANSACTION = {
    "id": 3,
    "jsonrpc": "2.0",
    "params": [],
    "method": "trace_transaction"
    }

def _get_abi(address, etherscan_key, session=None, delay=0, abi_type=None):
    if abi_type:
        if abi_type == "token":
            return token_abi
        elif abi_type == "kermit":
            return kermit_abi
        elif abi_type == "pair":
            return pair_abi
        elif abi_type == "pool":
            return pool_abi
    try:
        if session:
            res = session.get(ETHERSCAN_GETABI.format(address, etherscan_key), headers=HEADERS, force_refresh=(delay != 0))
        else:
            res = requests.get(ETHERSCAN_GETABI.format(address, etherscan_key), headers=HEADERS)
        d = res.json()
        abi = d["result"]
        if abi == 'Max rate limit reached' and not session is None:
            time.sleep(0.2)
            res = session.get(ETHERSCAN_GETABI.format(address, etherscan_key), headers=HEADERS, force_refresh=1)
            d = res.json()
            abi = d["result"]
            
        return abi
    except:
        return None

def _get_contract(w3, abi, address):
    return w3.eth.contract(address=address, abi=abi)

def get_contract_sync(address, context=None, w3=None, session=None, delay=0, abi_type=None):
    if address in context["contract_storage"]:
        return context["contract_storage"][address], context["abi_storage"][address]
        
    _address = Web3.to_checksum_address(address)
    abi = None
    if address in USDC_LIKE:
        abi = token_abi
    elif abi_type:
        if abi_type == "token":
            abi = token_abi
        elif abi_type == "kermit":
            abi = kermit_abi
        elif abi_type == "pair":
            abi = pair_abi
        elif abi_type == "pool":
            abi = pool_abi        
    if abi is None:
        for i in range(MAX_RETRY):
            if not abi is None:
                break
            abi = _get_abi(_address, context["etherscan_key"], session, delay)
           
            if not abi and i < MAX_RETRY-1:
                time.sleep(1)
        else:
            return None, None
    try:
        contract = _get_contract(w3, abi, _address)
    except:
        contract = None
    if not contract is None:
        context["abi_storage"][address] = abi
        context["contract_storage"][address] = contract
    return contract, abi

def get_contract_standard_token(w3, address):
    return w3.eth.contract(address=Web3.to_checksum_address(address), abi=token_abi)


def etherscan_get_internals(etherscan_key, block_number, address=None, txhash=None, session=None):
    if address:
        param = address
        etherscan_request = ETHERSCAN_GETINTERNALS
    elif txhash:
        param = txhash
        etherscan_request = ETHERSCAN_GETINTERNALS_TX
    try:
        if session:
            res = session.get(etherscan_request.format(param, block_number, block_number, etherscan_key), headers=HEADERS)
            d = res.json()
            if d["result"] == 'Max rate limit reached':
                time.sleep(0.2)
                res = session.get(etherscan_request.format(param, block_number, block_number, etherscan_key), headers=HEADERS, force_refresh=True)
                d = res.json()
        else:
            res = requests.get(etherscan_request.format(param, block_number, block_number, etherscan_key), headers=HEADERS)
            d = res.json()
        return d["result"]
    except:
        print("etherscan error", res.status_code)
        return None

def trace_transaction(url, tx_hash, session=None):
    TRACE_TRANSACTION["params"] = [tx_hash]
    # try:
    for i in range(MAX_RETRY):
        if session:
            res = session.post(url, headers=HEADERS, data=json.dumps(TRACE_TRANSACTION), force_refresh = (i > 0))
        else:
            res = requests.post(url, headers=HEADERS, data=json.dumps(TRACE_TRANSACTION))
        d = res.json()
        if "result" in d:
            return d["result"]
        else:
            time.sleep(0.1)
    return None
    # except:
    #     print("trace_transaction error", res.status_code)
    #     return None


def get_token_transactions(address, etherscan_key, session=None, timeout=0.1, start_block=None):
    last_block = start_block
    try:
        trnx = []
        page = 1
        # last_block = None
        trnx_set = set()
        while True:
            if session:
                if not last_block:
                    res = session.get(ETHERSCAN_GETLOGS.format(address, page, 1000, etherscan_key), headers=HEADERS, force_refresh=True)
                else:
                    res = session.get(ETHERSCAN_GETLOGS1.format(last_block, address, page, 1000, etherscan_key), headers=HEADERS, force_refresh=True)
            else:
                if not last_block:
                    res = requests.get(ETHERSCAN_GETLOGS.format(address, page, 1000, etherscan_key), headers=HEADERS)
                else:
                    res = requests.get(ETHERSCAN_GETLOGS1.format(last_block, address, page, 1000, etherscan_key), headers=HEADERS)
            d = res.json()
            if d["result"] is None or len(d["result"]) == 0:
                break
            
            new_set = set()
            for t in d["result"]:
                if not (t["transactionHash"], t["logIndex"]) in trnx_set:
                    new_set.add((t["transactionHash"], t["logIndex"]))
                    trnx.append(t)
            if len(new_set) == 0:
                break
            trnx_set = trnx_set.union(new_set)
            last_block = int(d["result"][-1]['blockNumber'], 0)
            # page +=1
            time.sleep(timeout)
        return trnx
    except:
        print("etherscan error", res.status_code)
        return None

def get_token_transfers(address, etherscan_key, session=None, timeout=0.1, start_block = None):
    last_block = start_block
    try:
        trnx = []
        page = 1
        trnx_set = set()
        while True:
            if session:
                if not last_block:
                    res = session.get(ETHERSCAN_GETTOKENS.format(address, page, 1000, etherscan_key), headers=HEADERS, force_refresh=True)
                else:
                    res = session.get(ETHERSCAN_GETTOKENS1.format(last_block, address, page, 1000, etherscan_key), headers=HEADERS, force_refresh=True)
            else:
                if not last_block:
                    res = requests.get(ETHERSCAN_GETTOKENS.format(address, page, 1000, etherscan_key), headers=HEADERS)
                else:
                    res = requests.get(ETHERSCAN_GETTOKENS1.format(last_block, address, page, 1000, etherscan_key), headers=HEADERS)
            d = res.json()
            if d["result"] is None or len(d["result"]) == 0:
                break
            
            new_set = set()
            for t in d["result"]:
                if not t["hash"] in trnx_set:
                    new_set.add(t["hash"])
                    trnx.append(t)
            if len(new_set) == 0:
                break
            trnx_set = trnx_set.union(new_set)
            last_block = int(d["result"][-1]['blockNumber'], 0)
            # page +=1
            time.sleep(timeout)
        return trnx
    except:
        print("etherscan error", res.status_code)
        return None
