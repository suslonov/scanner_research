#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import requests
import json
import pandas as pd
from web3 import Web3

KEY_FILE = 'alchemy.sec'

try:
    with open(KEY_FILE, 'r') as f:
        k1 = f.readline()
        alchemy_url = k1.strip('\n')
        k2 = f.readline()
        alchemy_wss = k2.strip('\n')
except:
    alchemy_url = ""
    alchemy_wss = ""


# w3 = Web3(Web3.HTTPProvider(alchemy_url))
# latest_block = w3.eth.get_block("latest")
# print(latest_block["number"])


Uniswap_V2_Router_2 = "0xe8b2d01ffa0a15736b2370b6e5064f9702c891b6"
Jared = "0xae2Fc483527B8EF99EB5D9B44875F005ba1FaE13"
X = "0xf96E1F621263621319Ef50959F03Ce9840D49011"

start_block = "0x0"
start_block = hex(17370200)

request_data_dict = {"jsonrpc":"2.0",
                     "method":"alchemy_getAssetTransfers",
                     "params":[
                         {"fromBlock": start_block,
                          "category": ["external","erc20","erc721"],
                          "fromAddress": Jared}],"id":0}


request_data = json.dumps(request_data_dict)
headers = {'Content-Type': "application/json"}

res = requests.post(url=alchemy_url, headers=headers, data=request_data)
print(res.status_code)

result = res.json()["result"]
df = pd.DataFrame.from_records(result["transfers"])

