#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from web3 import Web3, HTTPProvider
import time
import json

KEY_FILE = 'quicknode.sec'
RED = "\033[1;31m"
RESET_COLOR = "\033[0;0m"

Uniswap_V2_Router_2 = "0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D".lower()
Uniswap_Universal_Router = "0xef1c6e67703c7bd7107eed8303fbe6ec2554bf6b".lower()
Jared_favorite_mev_bot = "0x6b75d8af000000e20b7a7ddf000ba900b4009a80".lower()
Jared = "0xae2Fc483527B8EF99EB5D9B44875F005ba1FaE13".lower()

try:
    with open(KEY_FILE, 'r') as f:
        k1 = f.readline()
        quicknode_url = k1.strip('\n')
except:
    quicknode_url = ""

w3 = Web3(HTTPProvider(quicknode_url))
dict_pending = {}
dict_latest = {}

filters = {"pending": dict_pending, "latest": dict_latest}
# filters = {"pending": dict_pending}
# filters = {"latest": dict_latest}

def handle_event(event, collect):
    try:
        collect[event.hex()] = w3.eth.get_transaction(event.hex())
        if collect[event.hex()]["to"] == Uniswap_V2_Router_2:
            print(collect[event.hex()])
        if collect[event.hex()]["to"].lower() == Uniswap_Universal_Router:
            print("to Uniswap_Universal_Router", event.hex())
        elif collect[event.hex()]["to"].lower() == Uniswap_V2_Router_2:
            print("to Uniswap_V2_Router_2", event.hex())
        elif collect[event.hex()]['from'].lower() == Jared:
            print(RED, "from Jared", RESET_COLOR, event.hex())
            
    except:
        collect[event.hex()] = None

def log_loop(event_filters, poll_interval):
    ii = 0
    while True:
        for f in event_filters:
            for event in event_filters[f].get_new_entries():
                handle_event(event, filters[f])
        if len(dict_pending) > 300:
            break
        ii +=1
        time.sleep(poll_interval)

def main():
    event_filters = {f: w3.eth.filter(f) for f in filters}
    poll_interval = 2
    log_loop(event_filters, poll_interval)


if __name__ == '__main__':
    main()
