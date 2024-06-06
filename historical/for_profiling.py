#!/usr/bin/env python3
# -*- coding: utf-8 -*-


import os
os.sys.path.append(os.path.dirname(os.path.abspath('.')))
from datetime import datetime
from _utils.web3connect import web3connect2

KEY_FILE = '../keys/alchemy.sec'
KEY_FILE1 = '../keys/alchemy1.sec'

w3_direct, cached_session, w3, latest_block, _ = web3connect2(KEY_FILE, KEY_FILE1)

latest_block = w3_direct.eth.get_block("latest")

transactionHash = '0x1e250e5d71d578faa841165a285c57e6353f917be757b6b4a1370ca97f1c5e96'

t0 = datetime.now()
for i in range(100):
    transaction = w3_direct.eth.get_transaction_receipt(transactionHash)
print((datetime.now()-t0).total_seconds())
    
t0 = datetime.now()
for i in range(100):
    transaction = w3.eth.get_transaction_receipt(transactionHash)
print((datetime.now()-t0).total_seconds())

b = w3.eth.get_block(19116060)