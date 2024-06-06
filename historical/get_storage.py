#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
os.sys.path.append(os.path.dirname(os.path.abspath('.')))
from web3 import Web3
from _utils.etherscan import get_contract_sync, get_token_transactions, get_token_transfers

KEY_FILE = '../keys/alchemy.sec'

test_token = Web3.to_checksum_address("0x7f13cc695185a06f3db199557024acb2f5a4ef95")

with open(KEY_FILE, 'r') as f:
    k1 = f.readline()
    alchemy_url = k1.strip('\n')
    k2 = f.readline()
    alchemy_wss = k2.strip('\n')
   


def connect():
    w3_0 = Web3(Web3.HTTPProvider(alchemy_url))
    latest_block = w3_0.eth.get_block("latest")
    return w3_0, latest_block


w3, latest_block = connect()

for index in range(50):
   print(index,
         # index in tax_indexes,
         w3.eth.get_storage_at(test_token, index).hex())
   
