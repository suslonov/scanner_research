#!/usr/bin/env python3
# -*- coding: utf-8 -*-

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import requests
import pandas as pd
from web3 import Web3

KEY_FILE = '../keys/alchemy.sec'
ETHERSCAN_KEY_FILE = '../keys/etherscan.sec'
HEADERS = {'Content-Type': "application/json"}
ETHERSCAN_GETINTERNALS = 'http://api.etherscan.io/api?module=account&action=txlistinternal&address={}'
ETHERSCAN_GETINTERNALS +='&startblock={}&endblock={}&apikey={}'
NUMBER_BLOCKS_IN_CHUNK = 1000
COLUMNS = ['blockNumber', 'transactionIndex', 'hash', 'from', 'to', 'type', 'gas', 'gasPrice', 'maxFeePerGas', 'maxPriorityFeePerGas', 'value']
COLUMNS_INTERNAL = ['blockNumber', 'hash', 'from', 'to', 'contractAddress', 'type', 'gas', 'gasUsed', 'value']

with open(KEY_FILE, 'r') as f:
    k1 = f.readline()
    alchemy_url = k1.strip('\n')
    k2 = f.readline()
    alchemy_wss = k2.strip('\n')

with open(ETHERSCAN_KEY_FILE, 'r') as f:
    k1 = f.readline()
    etherscan_key = k1.strip('\n')

w3 = Web3(Web3.HTTPProvider(alchemy_url))

URL_2 = "http://3.68.124.134:8545"
w3_2 = Web3(Web3.HTTPProvider(URL_2, request_kwargs={'verify': False}))

def etherscan_get_internals(block_number, address):
    try:
        res = requests.get(ETHERSCAN_GETINTERNALS.format(address, block_number, block_number, etherscan_key), headers=HEADERS)
        d = res.json()
        txs = d["result"]
        return txs
    except:
        print("etherscan error", res.status_code)
        return None

def transactions_for_block(_w3, bn):
    block = w3.eth.get_block(bn, full_transactions=True)
    base_fee_per_gas = block["baseFeePerGas"]
    miner = block["miner"].lower()

    df_tx = pd.DataFrame.from_records(block["transactions"], columns = COLUMNS)
    df_tx["hash"] = df_tx["hash"].apply(lambda x: x.hex())
        
    _block = {"timestamp": block["timestamp"],
                            "builder": miner,
                            "base_fee_per_gas": base_fee_per_gas,
                            }

    
    internals = etherscan_get_internals(bn, miner)
    if len(internals):
        df_internals = pd.DataFrame.from_records(internals, columns = COLUMNS_INTERNAL)
    else:
        df_internals = None

    return _block, df_tx, df_internals


blocks = {}
df_txs = None
df_itxs = None

for bn in [17766768, 17766747] + list(range(18207866, 18207872)):
    _block, df_tx, df_internals = transactions_for_block(w3, bn)

    blocks[bn] = _block
    
    if not df_tx is None:
        if df_txs is None:
            df_txs = df_tx
        else:
            df_txs = pd.concat([df_txs, df_tx])
        
    if not df_internals is None:
        if df_itxs is None:
            df_itxs = df_internals
        else:
            df_itxs = pd.concat([df_itxs, df_internals])
    
df_txs["gasPrice"] = df_txs["gasPrice"] / 1e9
df_txs["maxFeePerGas"] = df_txs["maxFeePerGas"] / 1e9
df_txs["maxPriorityFeePerGas"] = df_txs["maxPriorityFeePerGas"] / 1e9
df_txs["value"] = df_txs["value"] / 1e18
df_itxs["value"] = df_itxs["value"].astype(int) / 1e18

df_blocks = pd.DataFrame.from_records(blocks).T
df_blocks["base_fee_per_gas"] = df_blocks["base_fee_per_gas"] / 1e9

df_blocks.to_csv("/media/Data/csv/df_blocks_sort_check.csv")
df_txs.to_csv("/media/Data/csv/df_txs_sort_check.csv")
df_itxs.to_csv("/media/Data/csv/df_itxs_sort_check.csv")

