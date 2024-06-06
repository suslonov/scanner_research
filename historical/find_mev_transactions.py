#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
os.sys.path.append(os.path.dirname(os.path.abspath('.')))
from requests_cache import CachedSession, SQLiteCache, NEVER_EXPIRE
import requests
import pandas as pd
from datetime import datetime
from web3 import Web3

from _utils.utils import HTTPProviderCached
from _utils.utils import etherscan_get_internals, trace_transaction, s64

KEY_FILE = '../keys/alchemy.sec'
KEY_FILE_QUICKNODE = '../keys/quicknode.sec'
ETHERSCAN_KEY_FILE = '../keys/etherscan.sec'
REQUEST_CACHE = '/media/Data/eth/eth'
HEADERS = {'Content-Type': "application/json"}
ETHERSCAN_GETABI = 'http://api.etherscan.io/api?module=contract&action=getabi&address={}&apikey={}'
NUMBER_BLOCKS_IN_CHUNK = 20000
MAX_RETRY = 10

with open(KEY_FILE, 'r') as f:
    k1 = f.readline()
    alchemy_url = k1.strip('\n')
    k2 = f.readline()
    alchemy_wss = k2.strip('\n')

# with open(KEY_FILE_QUICKNODE, 'r') as f:
#     k1 = f.readline()
#     quicknode_url = k1.strip('\n')
#     k2 = f.readline()
#     quicknode_wss = k2.strip('\n')

with open(ETHERSCAN_KEY_FILE, 'r') as f:
    k1 = f.readline()
    etherscan_key = k1.strip('\n')


def get_contract_sync(address, _w3, session=None):
    # for i in range(MAX_RETRY):
    #     if address abi_storage:
    #         break
    #     abi = _get_abi(context, address)
    #     if not abi and i < MAX_RETRY-1:
    #         time.sleep(5)
    #     else:
    #         context["abi_storage"][address] = abi
    # else:
    #     return None
    if session:
        res = session.get(ETHERSCAN_GETABI.format(address, etherscan_key), headers=HEADERS)
    else:
        res = requests.get(ETHERSCAN_GETABI.format(address, etherscan_key), headers=HEADERS)
    abi = res.json()["result"]

    try:
        contract = _w3.eth.contract(address=Web3.to_checksum_address(address), abi=abi)
        return contract, abi
    except:
        print(address, abi)
        return None, abi

w3_direct = Web3(Web3.HTTPProvider(alchemy_url))
latest_block_number = w3_direct.eth.get_block('latest')["number"]
print(latest_block_number)


backend = SQLiteCache(REQUEST_CACHE)
session = CachedSession(backend=backend, expire_after=NEVER_EXPIRE, allowable_methods=('GET', 'POST', 'HEAD'))
# w3_quicknode = Web3(Web3.HTTPProvider(quicknode_url))
w3 = Web3(HTTPProviderCached(alchemy_url, request_kwargs={'timeout': 1e9}, session=session))
#!!! web3._utils.request.py.cache_and_return_session: cache_and_return_session

block_number = latest_block_number

ii = 0
interesting_blocks = set()
interesting_blocks_list = []
t0 = datetime.now()

while True:
    from_to_hashes = {}
    block = w3.eth.get_block(block_number, full_transactions=True)
    miner = block["miner"].lower()

    internals = etherscan_get_internals(etherscan_key, block_number, address=miner, session=session)
    internals_set = set()
    for t in internals:
        internals_set.add(t["hash"])
    
    for transaction in block["transactions"]:
        transaction_hash = transaction["hash"].hex()
        if "to" in transaction:
            if not (transaction["from"], transaction["to"]) in from_to_hashes:
                from_to_hashes[(transaction["from"], transaction["to"])] = []
            from_to_hashes[(transaction["from"], transaction["to"])].append((transaction["transactionIndex"], transaction_hash))
        
    print(ii, block_number, (datetime.now()-t0).total_seconds())
    for from_to in from_to_hashes:
        if len(from_to_hashes[from_to]) > 1:
            if sum([1 if (ff[1] in internals_set) else 0 for ff in from_to_hashes[from_to]]):
                print(from_to, from_to_hashes[from_to])
                interesting_blocks.add(block_number)
                interesting_blocks_list.append((block_number, from_to, from_to_hashes[from_to]))

    block_number -= 1
    ii += 1
    t0 = datetime.now()
    if ii >= NUMBER_BLOCKS_IN_CHUNK:
        break


def garbage_zone():
    
    internals12 = etherscan_get_internals(18297328, address="0x00FC00900000002C00BE4EF8F49c000211000c43")

    tx = "0x1ed4161163a0890a765e3361538a07d504d415e39df249c5f5fdbfd2408aecef"
    internals13 = etherscan_get_internals(18297328, txhash=tx, session=session)
    tx = "0x1e82647e36a90cd0df2408d93405f5423c7b39b72e73d94d19d43ff32f410a48"
    internals14 = etherscan_get_internals(18297328, txhash=tx, session=session)
    
    for b in interesting_blocks_list:
        block = w3.eth.get_block(block_number, full_transactions=True)
        print(b)

    tx = "0x1ed4161163a0890a765e3361538a07d504d415e39df249c5f5fdbfd2408aecef"
    internals_a = trace_transaction(alchemy_url, tx)
    for t in internals_a:
        print(t["action"], int(t["action"]["value"],0))

    tx = "0x1e82647e36a90cd0df2408d93405f5423c7b39b72e73d94d19d43ff32f410a48"
    internals_b = trace_transaction(alchemy_url, tx)
    for t in internals_b:
        print(t["action"], int(t["action"]["value"],0))
        
    UniversalRouter = "0x3fC91A3afd70395Cd496C647d5a6CC9D4B2b7FAD".lower()
    UniversalRouter_contract, UniversalRouter_abi = get_contract_sync(UniversalRouter, w3, session)

