#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
os.sys.path.append(os.path.dirname(os.path.abspath('.')))
from web3 import Web3
from _utils.uniswap import V2_FACTORY, WETH
from _utils.etherscan import get_contract_sync

KEY_FILE = '../keys/alchemy.sec'
ETHERSCAN_KEY_FILE = '../keys/etherscan.sec'

with open(KEY_FILE, 'r') as f:
    k1 = f.readline()
    alchemy_url = k1.strip('\n')
    k2 = f.readline()
    alchemy_wss = k2.strip('\n')
   
with open(ETHERSCAN_KEY_FILE, 'r') as f:
    k1 = f.readline()
    ETHERSCAN_KEY = k1.strip('\n')


def connect():
    w3_0 = Web3(Web3.HTTPProvider(alchemy_url))
    latest_block = w3_0.eth.get_block("latest")
    return w3_0, latest_block


w3, latest_block = connect()

abi_storage = {}
contract_storage = {}
run_context = {"w3": w3,
               "delay": 0,
               "etherscan_key": ETHERSCAN_KEY,
               "abi_storage": abi_storage,
               "contract_storage": contract_storage,
               }

pair = '0x7F0F61453fAF5d31C459C5310843E3666D1824b3'
block_number = 18364724

block = w3.eth.get_block(block_number)
block_hash = block["hash"]
pair_contract, _ = get_contract_sync(pair, w3=w3, context=run_context)

reserves = pair_contract.functions.getReserves().call(block_identifier=block_hash)
print(reserves)


