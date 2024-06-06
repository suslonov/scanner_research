#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from web3 import Web3, HTTPProvider

KEY_FILE = 'quicknode.sec'

try:
    with open(KEY_FILE, 'r') as f:
        k1 = f.readline()
        quicknode_url = k1.strip('\n')
except:
    quicknode_url = ""

w3 = Web3(HTTPProvider(quicknode_url))

print ("Latest Ethereum block number" , w3.eth.get_block_number())

tx = w3.eth.get_transaction("0x8666564921e050865b806e63fdc0cf4a02b670da18690255b75c78791384d916")

print(tx)
