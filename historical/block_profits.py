#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
os.sys.path.append(os.path.dirname(os.path.abspath('.')))

from requests_cache import CachedSession, SQLiteCache, NEVER_EXPIRE
import requests
import pandas as pd
from web3 import Web3

from _utils.utils import HTTPProviderCached
from _utils.utils import etherscan_get_internals

KEY_FILE = '../keys/alchemy.sec'
ETHERSCAN_KEY_FILE = '../keys/etherscan.sec'
HEADERS = {'Content-Type': "application/json"}
REQUEST_CACHE = '/media/Data/eth/eth'
NUMBER_BLOCKS_IN_CHUNK = 1000

with open(KEY_FILE, 'r') as f:
    k1 = f.readline()
    alchemy_url = k1.strip('\n')
    k2 = f.readline()
    alchemy_wss = k2.strip('\n')

with open(ETHERSCAN_KEY_FILE, 'r') as f:
    k1 = f.readline()
    etherscan_key = k1.strip('\n')

URL_2 = "http://3.68.124.134:8545"
w3_2 = Web3(Web3.HTTPProvider(URL_2, request_kwargs={'verify': False}))


w3_direct = Web3(Web3.HTTPProvider(alchemy_url))
latest_block_number = w3_direct.eth.get_block('latest')["number"]
print(latest_block_number)

backend = SQLiteCache(REQUEST_CACHE)
session = CachedSession(backend=backend, expire_after=NEVER_EXPIRE, allowable_methods=('GET', 'POST', 'HEAD'))
w3 = Web3(HTTPProviderCached(alchemy_url, request_kwargs={'timeout': 1e9}, session=session))

blocks = {}
for chunk in [0, 10, 100, 200]:
    block_number = latest_block_number - 7200 * chunk
    ii = 0
    while True:
        block = w3.eth.get_block(block_number, full_transactions=True)
        base_fee_per_gas = block["baseFeePerGas"]
        gas_used = block["gasUsed"]
        miner = block["miner"].lower()
        burnt_fees = base_fee_per_gas * gas_used / 1e18
        
        gas_from_txs = 0
        burnt_fees_from_txs = 0
        total_fees_from_txs = 0
        bribes = 0
        
        internals = etherscan_get_internals(etherscan_key, block_number, miner, session)
        if internals is None:
            print("can't get internal transactions from etherscan, block", block_number)
        else:
            for transaction in internals:
                if transaction["to"].lower() == miner:
                    bribes += int(transaction["value"])
        
        for transaction in block["transactions"]:
            transaction_hash = transaction["hash"].hex()
            receipt = w3.eth.get_transaction_receipt(transaction_hash)
            gas_from_txs += receipt["gasUsed"]
            burnt_fees_from_txs += receipt["gasUsed"] * base_fee_per_gas
            total_fees_from_txs += receipt["gasUsed"] * receipt["effectiveGasPrice"]
    
        if gas_from_txs == 0:
            print ("block", block_number, "no transactions")
            block_number -= 1
            continue
    
        if gas_from_txs != gas_used:
            print(block_number, gas_from_txs, gas_used)
        else:
            print(block_number, "OK")
            
        if transaction["from"].lower() == miner and transaction["input"] == '0x':
            block_fee = transaction["value"] / 1e18
            validator_built = 0
            proposer = transaction["to"]
        else:
            block_fee = 0
            validator_built = 1
            proposer = ""
        
        blocks[block_number] = {"timestamp": block["timestamp"],
                                "builder": miner,
                                "proposer": proposer,
                                "validator_built": validator_built,
                                "gas_used": gas_used,
                                "burnt_fees": burnt_fees,
                                "total_fees": total_fees_from_txs / 1e18,
                                "bribes": bribes / 1e18,
                                "block_fee": block_fee,
                                "block_profit": (total_fees_from_txs + bribes) / 1e18 - block_fee - burnt_fees,
                                }
        # print(blocks[block_number])
        block_number -= 1
        ii += 1
        if ii >= NUMBER_BLOCKS_IN_CHUNK:
            break

df_blocks_profits = pd.DataFrame.from_records(blocks).T
df_blocks_profits.to_csv("/media/Data/csv/df_blocks_profits.csv")


def garbage_zone():
    bal_18182540 = w3.eth.get_balance("0x3Bee5122E2a2FbE11287aAfb0cB918e22aBB5436", 18182540)/1e18
    bal_18182541 = w3.eth.get_balance("0x3Bee5122E2a2FbE11287aAfb0cB918e22aBB5436", 18182541)/1e18

    block = w3_2.eth.get_block(18182541, full_transactions=True)
    txs = block["transactions"]

    block1 = w3_2.eth.get_block(18191564, full_transactions=True)
    block = dict(block)
    block1 = dict(block1)
    
    address = Web3.to_checksum_address("0x4838b106fce9647bdf1e7877bf73ce8b0bad5f97")
    bals = {}
    for bn in range (18207864, 18207872):
        bals[bn] = w3_2.eth.get_balance(address, bn)/1e18
    
    w3_2.eth.get_balance(Web3.to_checksum_address("0x4838b106fce9647bdf1e7877bf73ce8b0bad5f97"),
                         18207871)
