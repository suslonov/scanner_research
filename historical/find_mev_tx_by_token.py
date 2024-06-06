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

    to_address = transaction["to"].lower()
    if to_address in TARGET_ADRESSES.values():
        transaction["decoded_input"] = run_context["contract_storage"][to_address].decode_function_input(transaction["input"])
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

        if t["action"]["input"][:10] == "0xa9059cbb" or t["action"]["input"][:10] == "0x23b872dd":
            if not "decimals" in addr_contract.functions:
                if not "name" in addr_contract.functions:
                    addr_contract = get_contract_standard_token(w3, addr)
                else:
                    continue
            # name = addr_contract.functions.name().call()
            try:
                decimals = addr_contract.functions.decimals().call()
            except:
                continue

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

            else:
                continue

    return analytics


def process_block(block_number, block_token_transactions, run_context, debug_level=0, not_get_internals=False):
    w3 = run_context["w3"]

    from_to_hashes = {}
    block = w3.eth.get_block(block_number, full_transactions=False)
    run_context["miner"] = block["miner"].lower()
    base_fee_per_gas = block["baseFeePerGas"]
  
    if len(block["transactions"]) == 0:
        return

    for transaction_hash in block_token_transactions.keys():
        transaction = dict(w3.eth.get_transaction(transaction_hash))
        if "to" in transaction:
            if not (transaction["from"], transaction["to"]) in from_to_hashes:
                from_to_hashes[(transaction["from"], transaction["to"])] = []
            from_to_hashes[(transaction["from"], transaction["to"])].append((transaction["transactionIndex"], transaction_hash))

            analytics = process_transaction(transaction_hash, transaction, run_context, debug_level, not_get_internals)
            block_token_transactions[transaction_hash]["analytics"] = analytics if analytics else {}
            block_token_transactions[transaction_hash]["block_number"] = block_number
            block_token_transactions[transaction_hash]["from"] = transaction["from"].lower()
            block_token_transactions[transaction_hash]["to"] = transaction["to"].lower()
            block_token_transactions[transaction_hash]["function"] = transaction["input"][:10]
            block_token_transactions[transaction_hash]["base_fee_per_gas"] = base_fee_per_gas
            block_token_transactions[transaction_hash]["gas"] = transaction["gas"]
            if "reserves" in run_context["blocks"][block_number]:
                block_token_transactions[transaction_hash]["reserves"] = run_context["blocks"][block_number]["reserves"]
            else:
                block_token_transactions[transaction_hash]["reserves"] = {}


    for from_to in from_to_hashes:
        if len(from_to_hashes[from_to]) > 1:
            for th in from_to_hashes[from_to]:
                block_token_transactions[th[1]]["analytics"]["role"] = "sandwich"

def add_calculated_data(transaction, txi, token, token_name, token_decimals):
    _data_add = {}
    if ((transaction["analytics"][txi]["token0"] == token and 
         transaction["analytics"][txi]['amount0Out'] > 0) or
        (transaction["analytics"][txi]["token1"] == token and 
         transaction["analytics"][txi]['amount1Out'] > 0)):
            if 'amount_in' in transaction['analytics']:
                Xv = transaction['analytics']['amount_in'] / 1e18
            else:
                Xv = transaction['analytics']['value']
                    
            if 'amount_out_min' in transaction['analytics']:
                Yv = transaction['analytics']['amount_out_min']
            else:
                Yv = transaction["analytics"][txi]['amount0Out'] + transaction["analytics"][txi]['amount1Out']
            Yv = Yv / (10 ** token_decimals)
            Xa = optimal_amount_formula(transaction["reserves"]["WETH"],
                                        transaction["reserves"][token_name],
                                        Xv,
                                        Yv)
            _data_add['calculated'] = Xa
            if Xa > 0:
                _data_add['profit'] = profit_function(Xa,
                                                    transaction["reserves"]["WETH"],
                                                    transaction["reserves"][token_name],
                                                    Xv)
    else:
        _data_add['calculated'] = "backside"
    return _data_add

def add_calculated_data2(transaction, token, token_name, token_decimals):
    _data_add = {}
# works for special cases only
    token0 = transaction["analytics"]["swap_command"]["tokens"][0].lower()
    token1 = transaction["analytics"]["swap_command"]["tokens"][1].lower()
    if token1 == token and token0 == WETH:
        Xv = 0
        if "amount_in" in transaction["analytics"]["swap_command"]:
            Xv = transaction["analytics"]["swap_command"]["amount_in"] / power_divider(token0, token, token_decimals)
        else:
            Xv = transaction["analytics"]['value']
        if "amount_out_min" in transaction["analytics"]["swap_command"]:
            amount_out_min = transaction["analytics"]["swap_command"]["amount_out_min"]
        elif "amount_out" in transaction["analytics"]["swap_command"]:
            amount_out_min = transaction["analytics"]["swap_command"]["amount_out"]
        else:
            return _data_add
        if Xv == 0:
            return _data_add
        Yv = amount_out_min / power_divider(token1, token, token_decimals)
    
        Xa = optimal_amount_formula(transaction["reserves"]["WETH"],
                                    transaction["reserves"][token_name],
                                    Xv,
                                    Yv)
        _data_add['calculated'] = Xa
        if Xa > 0:
            _data_add['profit'] = profit_function(Xa,
                                                  transaction["reserves"]["WETH"],
                                                  transaction["reserves"][token_name],
                                                  Xv)
    elif token0 == token and token1 == WETH:
        _data_add['calculated'] = "backside"
    return _data_add

def main():

    # token_name = "Among Us"
    # token = "0x06997789943ba32eB6E73b0a9A424971C3d2E23e".lower()

    # token_name = "ELONGATE"
    # token = "0xcC6c4F450f1d4aeC71C46f240a6bD50c4E556B8A".lower()
    
    token_name = "BEAST"
    token = "0xDA07797A75eC922394FB6a9De7F90Ee38b1c9160".lower()

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
                    "blocks": None}

    latest_block_number = w3_direct.eth.get_block('latest')["number"]
    print(latest_block_number)

    get_contract_sync(V2_FACTORY["UNISWAP_V2_FACTORY"], w3=w3, session=cached_session, context=run_context)
    pair_address = contract_storage[V2_FACTORY["UNISWAP_V2_FACTORY"]].functions.getPair(Web3.to_checksum_address(token), Web3.to_checksum_address(WETH)).call()
    get_contract_sync(pair_address, w3=w3, session=cached_session, context=run_context)
    get_contract_sync(token, w3=w3, session=cached_session, context=run_context)
    token_decimals = contract_storage[token].functions.decimals().call()
    # contract_storage[WETH] = get_contract_standard_token(w3, WETH)
    get_contract_sync(WETH, w3=w3, session=cached_session, context=run_context)

    for a in TARGET_ADRESSES:
        if not TARGET_ADRESSES[a] in contract_storage:
            get_contract_sync(TARGET_ADRESSES[a], w3=w3, session=cached_session, context=run_context)

    token_trnx = get_token_transactions(Web3.to_checksum_address(token), ETHERSCAN_KEY, cached_session)

    blocks = {}
    all_transactions = {}
    reserves = {token_name: 0, "WETH": 0}
    block_hash = token_trnx[0]["blockHash"]
    block_number = int(token_trnx[0]["blockNumber"], 0)
    last_block = block_number
    blocks[block_number] = {"block_hash": block_hash, "transactions": {}, "reserves": reserves}

    for t in token_trnx:
        gas_used = int(t["gasUsed"], 0)
        if gas_used < 50000 or gas_used > 1000000:
            continue

        block_number = int(t["blockNumber"], 0)
       
        if block_number > last_block:
            print(block_number, len(blocks[last_block]["transactions"]))
            block_hash = t["blockHash"]
            blocks[block_number] = {"block_hash": t["blockHash"], "transactions": {}, "reserves": reserves}
            last_block = block_number

            reserves = {token_name: 0, "WETH": 0}
            reserves[token_name] = contract_storage[token].functions.balanceOf(pair_address).call(block_identifier=block_hash) / (10 ** token_decimals)
            reserves["WETH"] = contract_storage[WETH].functions.balanceOf(pair_address).call(block_identifier=block_hash) / 1e18
        
        if t["transactionIndex"] == "0x":
            transaction_index = 0
        else:
            transaction_index = int(t["transactionIndex"], 0)
        blocks[block_number]["transactions"][t["transactionHash"]] = {"transactionIndex": transaction_index}
        all_transactions[t["transactionHash"]] = blocks[block_number]["transactions"][t["transactionHash"]]

    run_context["blocks"] = blocks

    for ib, block_number in enumerate(blocks.keys()):
        if len(blocks[block_number]["transactions"]) > 0:
            process_block(block_number, blocks[block_number]["transactions"], run_context, not_get_internals=False)
        if ib%10 == 0:
            print(ib, "blocks")

    data_dict = []
    for tx in all_transactions:
        itx = 0
        for txi in all_transactions[tx]["analytics"]:
            data_add = None
            if isinstance(txi, int):
                if not itx:
                    data_add = {'txi_itxi': (all_transactions[tx]['block_number'], all_transactions[tx]['transactionIndex'], txi),
                                'from': all_transactions[tx]['from'],
                                'to': all_transactions[tx]['to'],
                                'function': all_transactions[tx]['function'],
                                'role': all_transactions[tx]['analytics']["role"],
                                'bribe': all_transactions[tx]['analytics']["bribe"] if "bribe" in all_transactions[tx]['analytics'] else None,
                                'base_fee_per_gas': all_transactions[tx]['base_fee_per_gas'],
                                'value': all_transactions[tx]['analytics']['value'] if 'value' in all_transactions[tx]['analytics'] else None,
                                'gas': all_transactions[tx]["gas"],
                                'swap_command': all_transactions[tx]['analytics']['swap_command']["command"] if 'swap_command' in all_transactions[tx]['analytics'] else None,
                                'amount_in': all_transactions[tx]['analytics']['amount_in'] if 'amount_in' in all_transactions[tx]['analytics'] else None,
                                'amount_out_min': all_transactions[tx]['analytics']['amount_out_min'] if 'amount_out_min' in all_transactions[tx]['analytics'] else None,
                                'calculated': None,
                                'profit': None,
                                } | all_transactions[tx]["reserves"] | all_transactions[tx]["analytics"][txi] | {"transaction_hash": tx}
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
                                } | all_transactions[tx]["reserves"] | all_transactions[tx]["analytics"][txi] | {"transaction_hash": tx}

                if all_transactions[tx]["analytics"][txi]["type"] == 'V2 swap' and 'swap_command' in all_transactions[tx]['analytics']:
                    data_add = data_add | add_calculated_data(all_transactions[tx], txi, token, token_name, token_decimals)
                itx += 1
            if data_add:
                data_dict.append(data_add)
# if not get internals for router transactions, this branch is idle
        if not itx and 'swap_command' in all_transactions[tx]['analytics']:
            data_add = {'txi_itxi': (all_transactions[tx]['block_number'], all_transactions[tx]['transactionIndex'], -1),
                        'from': all_transactions[tx]['from'],
                        'to': all_transactions[tx]['to'],
                        'function': all_transactions[tx]['function'],
                        'role': all_transactions[tx]['analytics']["role"],
                        'bribe': all_transactions[tx]['analytics']["bribe"] if "bribe" in all_transactions[tx]['analytics'] else None,
                        'base_fee_per_gas': all_transactions[tx]['base_fee_per_gas'],
                        'value': all_transactions[tx]['analytics']['value'] if 'value' in all_transactions[tx]['analytics'] else None,
                        'gas': all_transactions[tx]["gas"],
                        'swap_command': all_transactions[tx]['analytics']['swap_command']["command"] if 'swap_command' in all_transactions[tx]['analytics'] else None,
                        'amount_in': all_transactions[tx]['analytics']['amount_in'] if 'amount_in' in all_transactions[tx]['analytics'] else None,
                        'amount_out_min': all_transactions[tx]['analytics']['amount_out_min'] if 'amount_out_min' in all_transactions[tx]['analytics'] else None,
                        'calculated': None,
                        'profit': None,
                        } | all_transactions[tx]["reserves"] | {"transaction_hash": tx} | add_calculated_data2(all_transactions[tx], token, token_name, token_decimals)
            data_dict.append(data_add)

    df = pd.DataFrame.from_records(data_dict, index=["txi_itxi"])
    df.to_csv("/media/Data/csv/df_txs_" + token_name + ".csv")


def garbage_zone(w3, cached_session, address):
    blocks ={} #############
    
    df = pd.DataFrame.from_records([{"number_tx":len(blocks[b]["transactions"]), 
                          "first_tx": list(blocks[b]["transactions"].values())[0]["transactionIndex"],
                          "reserve_token": blocks[b]["reserves"]["Among Us"] / 1e9, 
                          "reserve_weth": blocks[b]["reserves"]["WETH"] / 1e18,
            } for b in blocks.keys() if len(blocks[b]["transactions"]) > 0],
                                index = [b for b in blocks.keys() if len(blocks[b]["transactions"]) > 0])

    df.to_csv("/media/Data/csv/df_blocks_amongus.csv")
    