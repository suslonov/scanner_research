#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
os.sys.path.append(os.path.dirname(os.path.abspath('.')))
from requests_cache import CachedSession, SQLiteCache, NEVER_EXPIRE
import pandas as pd
from datetime import datetime
import time
from web3 import Web3

from _utils.utils import HTTPProviderCached, s64, wrap_with_try
from _utils.etherscan import etherscan_get_internals, trace_transaction
from _utils.etherscan import get_contract_standard_token, get_contract_sync, get_token_transactions
from _utils.uniswap import uniswap_transaction_decode, profit_function, optimal_amount_formula
from _utils.uniswap import V2_FACTORY, V3_FACTORY, WETH

KEY_FILE = '../keys/alchemy.sec'
KEY_FILE_QUICKNODE = '../keys/quicknode.sec'
ETHERSCAN_KEY_FILE = '../keys/etherscan.sec'
REQUEST_CACHE = '/media/Data/eth/eth'
CSV_FILE = "/media/Data/csv/searcher_profit"

TARGET_ADRESSES = {"Uniswap_V2_Router_2": "0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D".lower(),
            "Uniswap_Universal_Router": "0xef1c6e67703c7bd7107eed8303fbe6ec2554bf6b".lower(),
            "UniswapV3SwapRouter02": "0x68b3465833fb72A70ecDF485E0e4C7bD8665Fc45".lower(),
            "UniversalRouter": "0x3fC91A3afd70395Cd496C647d5a6CC9D4B2b7FAD".lower(),
            "SushiSwapRouter": "0xd9e1cE17f2641f24aE83637ab66a2cca9C378B9F".lower(),}
            # "BananaGunRouter": "0xdb5889e35e379ef0498aae126fc2cce1fbd23216",}

with open(KEY_FILE, 'r') as f:
    k1 = f.readline()
    alchemy_url = k1.strip('\n')
    k2 = f.readline()
    alchemy_wss = k2.strip('\n')

with open(ETHERSCAN_KEY_FILE, 'r') as f:
    k1 = f.readline()
    ETHERSCAN_KEY = k1.strip('\n')

def power_divider(tokenX, token, token_decimals):
    if tokenX == token:
        return 10 ** token_decimals
    elif tokenX == WETH:
        return 10 ** 18
    else:
        return None

def connect():
    w3_0 = Web3(Web3.HTTPProvider(alchemy_url))
    backend = SQLiteCache(REQUEST_CACHE)
    session = CachedSession(backend=backend, expire_after=NEVER_EXPIRE, allowable_methods=('GET', 'POST', 'HEAD'))
    # w3_quicknode = Web3(Web3.HTTPProvider(quicknode_url))
    w3_1 = Web3(HTTPProviderCached(alchemy_url, request_kwargs={'timeout': 1e9}, session=session))
    #!!! web3._utils.request.py.cache_and_return_session: cache_and_return_session
    return w3_0, session, w3_1

def process_transaction(transaction_hash, transaction, run_context, debug_level=0, not_get_internals=False):
    cached_session = run_context["cached_session"]
    w3 = run_context["w3"]
    analytics = {'role': ""}

    trnx_to_address = transaction["to"].lower()
    if trnx_to_address in TARGET_ADRESSES.values():
        transaction["decoded_input"] = run_context["contract_storage"][trnx_to_address].decode_function_input(transaction["input"])
        _analytics = uniswap_transaction_decode(transaction)
        analytics['function'] = _analytics['function']
        analytics['value'] = _analytics['value']
        analytics['V3_detected'] = _analytics['V3_detected'] if "V3_detected" in _analytics else False
        analytics['V2_detected'] = _analytics['V2_detected'] if "V2_detected" in _analytics else False
        if "deadline" in _analytics:
            analytics['deadline'] = _analytics['deadline']
        if 'operations' in _analytics:
            analytics['role'] = "target"
            analytics['operations'] = _analytics['operations']
            for o in analytics['operations']:
                if not o["command"]:
                    continue
                if o["command"].find("SWAP") >= 0 or o["command"].find("swap") >= 0:
                    analytics['swap_command'] = o
                    if "amount_in" in o:
                        analytics['amount_in'] = o["amount_in"]
                    if "amount_out_min" in o:
                        analytics['amount_out_min'] = o["amount_out_min"]
                    break
        if not_get_internals:
            return analytics

    internals = trace_transaction(alchemy_url, transaction_hash, session=cached_session)

    if internals is None:
        internals = trace_transaction(alchemy_url, transaction_hash)
    if internals is None:
        return analytics
    run_context["internals"] = internals #!!! debug


    for i, t in enumerate(internals):
        if not "to" in t["action"]:
            continue
        addr = t["action"]["to"].lower()
        if t["action"]["to"] == run_context["miner"]:
            if "value" in t["action"]:
                analytics["bribe"] = int(t["action"]["value"], 0) / 1e18
            continue
        
        addr_contract, _ = get_contract_sync(addr, w3=w3, session=cached_session, context=run_context)
        

        run_context["addr_contract"] = addr_contract
        if addr_contract is None:
            continue
        # print(i, wrap_with_try(addr_contract.decode_function_input, t["action"]["input"]))

        if t["action"]["input"][:10] == "0xa9059cbb" or t["action"]["input"][:10] == "0x23b872dd":
            if not "decimals" in addr_contract.functions:
                if not "name" in addr_contract.functions:
                    addr_contract = get_contract_standard_token(w3, addr)
                else:
                    continue
            # name = addr_contract.functions.name().call()
            # try:
            #     decimals = addr_contract.functions.decimals().call()
            # except:
            #     continue

            decoded_input = wrap_with_try(addr_contract.decode_function_input, t["action"]["input"])
            if decoded_input is None:
                print(i, "incorrect input for decode", transaction_hash)
                continue

            list_decoded_input = list(decoded_input[1].keys())
            if t["action"]["input"][:10] == "0xa9059cbb":
                to_field = list_decoded_input[0]
                amount_field = list_decoded_input[1]
                from_field = None
            elif t["action"]["input"][:10] == "0x23b872dd":
                to_field = list(decoded_input[1].keys())[1]
                from_field = list(decoded_input[1].keys())[0]
                amount_field = list_decoded_input[2]
            
            analytics[i] = {"type": "transfer",
                            "transfered_amount": decoded_input[1][amount_field],
                            "transfered_token": t["action"]["to"].lower(),
                            "to_field": decoded_input[1][to_field].lower(),
                            "from_field": decoded_input[1][from_field].lower() if from_field else None
                            }

            continue
        elif t["action"]["input"][:10] in ["0x70a08231", "0xd06ca61f", "0x7739cbe7", "0x330deb9f", "0x"]:
            continue
        else:
            if (not addr_contract is None 
                and "_functions" in addr_contract.functions.__dict__
                and "factory" in addr_contract.functions 
                and addr_contract.functions.factory().call().lower() in V2_FACTORY.values()):

                decoded_input = wrap_with_try(addr_contract.decode_function_input, t["action"]["input"])
                if decoded_input is None:
                    continue
                if not "token0" in addr_contract.functions:
                    continue
                token0 = addr_contract.functions.token0().call().lower()
                token1 = addr_contract.functions.token1().call().lower()
                if not "amount0Out" in decoded_input[1]:
                    continue

                analytics[i] = {"type": "V2 swap",
                                "pair": addr.lower(),
                                "token0": token0.lower(),
                                "token1": token1.lower(),
                                "amount0Out": decoded_input[1]["amount0Out"],
                                "amount1Out": decoded_input[1]["amount1Out"]}

            elif (not addr_contract is None 
                  and "_functions" in addr_contract.functions.__dict__
                  and "factory" in addr_contract.functions 
                  and addr_contract.functions.factory().call().lower() in V3_FACTORY.values()):
                
                # print(i, t)

                if not "token0" in addr_contract.functions:
                    continue

                if not "result" in t or len(t["result"]["output"]) < 130:
                    continue
                token0 = addr_contract.functions.token0().call()
                token1 = addr_contract.functions.token1().call()

                amount0 = s64(int(t["result"]["output"][:66], 0))
                amount1 = s64(int(t["result"]["output"][:2] + t["result"]["output"][66:], 0))

                analytics[i] = {"type": "V3 swap",
                                "pair": addr.lower(),
                                "token0": token0.lower(),
                                "token1": token1.lower(),
                                "amount0": amount0,
                                "amount1": amount1}

                # print(i, analytics[i])
            else:
                continue

    return analytics


def process_block(block_number, from_address, to_address, run_context, debug_level=0, not_get_internals=False):
    w3 = run_context["w3"]

    block = w3.eth.get_block(block_number, full_transactions=True)
    run_context["miner"] = block["miner"].lower()
    base_fee_per_gas = block["baseFeePerGas"]
  
    if len(block["transactions"]) == 0:
        return {}

    transaction_dict = {}
    transaction_dict1 = {}
    state = 0
    for transaction in block["transactions"]:
        if "to" in transaction and not transaction["to"] is None and transaction["to"].lower() == to_address and transaction["from"].lower() == from_address:
            state = 1
        if state == 1:
            transaction_dict[transaction["transactionIndex"]] = transaction

    if len(transaction_dict) == 0:
        return {}
            
    for transaction in block["transactions"][::-1]:
        if "to" in transaction and not transaction["to"] is None and transaction["to"].lower() == to_address and transaction["from"].lower() == from_address:
            break
        else:
            del transaction_dict[transaction["transactionIndex"]]
            
    for transaction_index in transaction_dict:
        transaction = dict(transaction_dict[transaction_index])
        transaction_hash = transaction["hash"].hex()
        analytics = None
        analytics = process_transaction(transaction_hash, transaction, run_context, debug_level, not_get_internals)
        if analytics is None:
            analytics = {}
            
        analytics["block_number"] = block_number
        analytics["transactionIndex"] = transaction_index
        analytics["from"] = transaction["from"].lower()
        analytics["to"] = transaction["to"].lower()
        if analytics["to"] == to_address and analytics["from"] == from_address:
            analytics["role"] = "sandwich"
        analytics["function"] = transaction["input"][:10]
        analytics["base_fee_per_gas"] = base_fee_per_gas
        analytics["gas"] = transaction["gas"]
            # if "reserves" in run_context["blocks"][block_number]:
            #     block_token_transactions[transaction_hash]["reserves"] = run_context["blocks"][block_number]["reserves"]
            # else:
            #     block_token_transactions[transaction_hash]["reserves"] = {}
        transaction_dict1[transaction["hash"].hex()] = analytics
    return transaction_dict1


def main():

    from_address = "0xae2Fc483527B8EF99EB5D9B44875F005ba1FaE13".lower()
    to_address = "0x6b75d8AF000000e20B7a7DDf000Ba900b4009A80".lower()
    attacker_name = "jared"
    
    # from_address = "0x77ad3a15b78101883AF36aD4A875e17c86AC65d1".lower()
    # to_address = "0x00000000a991c429ee2ec6df19d40fe0c80088b8".lower()
    # attacker_name = "bot-3pct"
    
    last_block = None
    N_blocks = 1000

    w3_direct, cached_session, w3 = connect()
    abi_storage = {}
    contract_storage = {}
    run_context = {"w3_direct": w3_direct, 
                    "cached_session": cached_session, 
                    "w3": w3,
                    "delay": 0,
                    "etherscan_key": ETHERSCAN_KEY,
                    "abi_storage": abi_storage,
                    "contract_storage": contract_storage,
                    }

    latest_block_number = w3_direct.eth.get_block('latest')["number"]
    print(latest_block_number)
    if last_block is None:
        last_block = latest_block_number

    get_contract_sync(V2_FACTORY["UNISWAP_V2_FACTORY"], w3=w3, session=cached_session, context=run_context)
    get_contract_sync(to_address, w3=w3, session=cached_session, context=run_context)
    get_contract_sync(WETH, w3=w3, session=cached_session, context=run_context)

    for a in TARGET_ADRESSES:
        if not TARGET_ADRESSES[a] in contract_storage:
            get_contract_sync(TARGET_ADRESSES[a], w3=w3, session=cached_session, context=run_context)

    all_transactions = {}
    block_number = last_block

            # reserves = {token_name: 0, "WETH": 0}
            # reserves[token_name] = contract_storage[token].functions.balanceOf(pair_address).call(block_identifier=block_hash) / (10 ** token_decimals)
            # reserves["WETH"] = contract_storage[WETH].functions.balanceOf(pair_address).call(block_identifier=block_hash) / 1e18
        
    while block_number > last_block - N_blocks:
        block_transactions = process_block(block_number, from_address, to_address, run_context, not_get_internals=False)
        all_transactions = all_transactions | block_transactions
        print(block_number, len(block_transactions))
        block_number -= 1

    data_dict = []
    for tx in all_transactions:
        itx = 0
        for txi in all_transactions[tx]:
            data_add = None
            if isinstance(txi, int):
                if not itx:
                    data_add = {'txi_itxi': (all_transactions[tx]['block_number'], all_transactions[tx]['transactionIndex'], txi),
                                'from': all_transactions[tx]['from'],
                                'to': all_transactions[tx]['to'],
                                'function': all_transactions[tx]['function'],
                                'role': all_transactions[tx]["role"],
                                'bribe': all_transactions[tx]["bribe"] if "bribe" in all_transactions[tx] else None,
                                'base_fee_per_gas': all_transactions[tx]['base_fee_per_gas'],
                                'value': all_transactions[tx]['value'] if 'value' in all_transactions[tx] else None,
                                'gas': all_transactions[tx]["gas"],
                                'swap_command': all_transactions[tx]['swap_command']["command"] if 'swap_command' in all_transactions[tx] else None,
                                'amount_in': all_transactions[tx]['amount_in'] if 'amount_in' in all_transactions[tx] else None,
                                'amount_out_min': all_transactions[tx]['amount_out_min'] if 'amount_out_min' in all_transactions[tx] else None,
                                'calculated': None,
                                'profit': None,
                                # } | all_transactions[tx]["reserves"] | all_transactions[tx]["analytics"][txi] | {"transaction_hash": tx}
                                } | all_transactions[tx][txi] | {"transaction_hash": tx}
                else:
                    data_add = {'txi_itxi': (all_transactions[tx]['block_number'], all_transactions[tx]['transactionIndex'], txi),
                                'from': None,
                                'to': None,
                                'function': None,
                                'role': None,
                                'bribe': None,
                                'base_fee_per_gas': None,
                                'value': None,
                                'gas': None,
                                'swap_command': None,
                                'amount_in': None,
                                'amount_out_min': None,
                                'calculated': None,
                                'profit': None,
                                } | all_transactions[tx][txi] | {"transaction_hash": tx}
                                # } | all_transactions[tx]["reserves"] | all_transactions[tx]["analytics"][txi] | {"transaction_hash": tx}

                itx += 1
            if data_add:
                data_dict.append(data_add)

    df = pd.DataFrame.from_records(data_dict, index=["txi_itxi"])
    df.to_csv("/media/Data/csv/df_txs_" + attacker_name + ".csv")


def garbage_zone(w3, cached_session, address):
    blocks ={} #############
    
    df = pd.DataFrame.from_records([{"number_tx":len(blocks[b]["transactions"]), 
                          "first_tx": list(blocks[b]["transactions"].values())[0]["transactionIndex"],
                          "reserve_token": blocks[b]["reserves"]["Among Us"] / 1e9, 
                          "reserve_weth": blocks[b]["reserves"]["WETH"] / 1e18,
            } for b in blocks.keys() if len(blocks[b]["transactions"]) > 0],
                                index = [b for b in blocks.keys() if len(blocks[b]["transactions"]) > 0])

    df.to_csv("/media/Data/csv/df_blocks_amongus.csv")
    