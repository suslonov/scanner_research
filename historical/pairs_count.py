#!/usr/bin/env python3
# -*- coding: utf-8 -*-

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
os.sys.path.append(os.path.dirname(os.path.abspath('.')))
from web3 import Web3

from _utils.uniswap import V2_FACTORY
from _utils.etherscan import get_contract_sync, get_token_transactions

KEY_FILE = '../keys/alchemy.sec'
KEY_FILE_QUICKNODE = '../keys/quicknode.sec'
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

def get_new_pair_tokens(i, run_context):
    pair_address = run_context['contract_storage'][V2_FACTORY["UNISWAP_V2_FACTORY"]].functions.allPairs(i).call()
    get_contract_sync(pair_address, w3=run_context["w3"], context=run_context, abi_type="pair")
    token0 = run_context['contract_storage'][pair_address].functions.token0().call()
    token1 = run_context['contract_storage'][pair_address].functions.token1().call()
    get_contract_sync(token0, w3=run_context["w3"], context=run_context, abi_type="token")
    get_contract_sync(token1, w3=run_context["w3"], context=run_context, abi_type="token")
    token0_symbol = run_context['contract_storage'][token0].functions.symbol().call()
    token0_name = run_context['contract_storage'][token0].functions.name().call()
    token1_symbol = run_context['contract_storage'][token1].functions.symbol().call()
    token1_name = run_context['contract_storage'][token1].functions.name().call()
    
    return pair_address, token0, token0_symbol, token0_name, token1, token1_symbol, token1_name

def main():
    N_blocks = 10000

    w3, latest_block = connect()
    abi_storage = {}
    contract_storage = {}
    reserves = {}
    run_context = { "w3": w3,
                    "delay": 0,
                    "etherscan_key": ETHERSCAN_KEY,
                    "abi_storage": abi_storage,
                    "contract_storage": contract_storage,
                    "reserves": reserves,
                    }

    last_block = latest_block["number"]

    get_contract_sync(V2_FACTORY["UNISWAP_V2_FACTORY"], w3=w3, context=run_context)
    block_number = last_block

    number_pairs_prev = None
    number_pairs_latest = None
    ii = 0
    while block_number > last_block - N_blocks:
        print(ii)
        block = w3.eth.get_block(block_number, full_transactions=False)
        block_hash = block["hash"].hex()
        number_pairs_this_block = run_context['contract_storage'][V2_FACTORY["UNISWAP_V2_FACTORY"]].functions.allPairsLength().call(block_identifier=block_hash)
        if not number_pairs_latest:
            number_pairs_latest = number_pairs_this_block
        elif number_pairs_prev != number_pairs_this_block:
            print("block", block["number"],
                  "new pairs in the block", number_pairs_prev - number_pairs_this_block,
                  "new pairs from the top", number_pairs_latest - number_pairs_this_block)
            for i in range(number_pairs_this_block - 1, number_pairs_prev):
                (pair_address, new_pair_token0, new_pair_token0_symbol, new_pair_token0_name,
                new_pair_token1, new_pair_token1_symbol, new_pair_token1_name) = get_new_pair_tokens(i, run_context)
                print("pair address", pair_address, "\n", new_pair_token0, new_pair_token0_symbol, new_pair_token0_name, "\n", 
                new_pair_token1, new_pair_token1_symbol, new_pair_token1_name)
                
        number_pairs_prev = number_pairs_this_block
        block_number -= 1
        ii += 1

