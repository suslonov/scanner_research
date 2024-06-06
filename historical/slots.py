#!/usr/bin/env python3
# -*- coding: utf-8 -*-



import requests

URL = "https://boost-relay.flashbots.net/relay/v1/data/bidtraces/builder_blocks_received?slot={}"
HEADERS = {'Content-Type': "application/json"}

N = 10
start_slot = 7999880

slot = start_slot
ii = 0
while ii < N:
    res = requests.get(URL.format(slot), headers=HEADERS)
    d = res.json()
    print(slot, len(d))
    slot += 1
    ii += 1