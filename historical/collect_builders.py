#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
os.sys.path.append(os.path.dirname(os.path.abspath('.')))
from web3 import Web3

KEY_FILE = '../keys/alchemy.sec'

with open(KEY_FILE, 'r') as f:
    k1 = f.readline()
    alchemy_url = k1.strip('\n')
    k2 = f.readline()
    alchemy_wss = k2.strip('\n')

def connect():
    w3_0 = Web3(Web3.HTTPProvider(alchemy_url))
    latest_block = w3_0.eth.get_block("latest")
    return w3_0, latest_block

def main():
    N_blocks = 5*60*24*30

    w3, latest_block = connect()
    builders = {}
    last_block = latest_block["number"]
    block_number = last_block

    ii = 0
    while block_number > last_block - N_blocks:
        print(ii)
        block = w3.eth.get_block(block_number, full_transactions=False)
        miner = block["miner"]
        if not miner in builders:
            builders[miner] = 1
        else:
            builders[miner] += 1
        block_number -= 1
        ii += 1


    qqq = sorted(builders.items(), key=lambda t: t[1])
    
    qqq[-10:]
