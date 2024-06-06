#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
os.sys.path.append(os.path.dirname(os.path.abspath('.')))
from requests_cache import CachedSession, SQLiteCache, NEVER_EXPIRE
import pandas as pd
from datetime import datetime
import time
from web3 import Web3

from _utils.utils import HTTPProviderCached, s64, wrap_with_try, bytes_to_int
from _utils.etherscan import etherscan_get_internals, trace_transaction
from _utils.etherscan import get_contract_standard_token, get_contract_sync, get_token_transactions
from _utils.uniswap import uniswap_transaction_decode, profit_function, optimal_amount_formula
from _utils.uniswap import V2_FACTORY, V3_FACTORY, WETH

from V3.sandwich_calc_V3 import sandwich_exact_input_single, sandwich_exact_output_single
from V3.sandwich_calc_V3 import sandwich_V3_SWAP_EXACT_IN, sandwich_V3_SWAP_EXACT_OUT

KEY_FILE = '../keys/alchemy.sec'
KEY_FILE_QUICKNODE = '../keys/quicknode.sec'
ETHERSCAN_KEY_FILE = '../keys/etherscan.sec'
REQUEST_CACHE = '/media/Data/eth/eth'
CSV_FILE = "/media/Data/csv/searcher_profit"

TARGET_ADRESSES = {"Uniswap_V2_Router_2": "0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D".lower(),
            "Uniswap_Universal_Router": "0xef1c6e67703c7bd7107eed8303fbe6ec2554bf6b".lower(),
            "UniswapV3SwapRouter02": "0x68b3465833fb72A70ecDF485E0e4C7bD8665Fc45".lower(),
            "UniswapV3SwapRouter": "0xE592427A0AEce92De3Edee1F18E0157C05861564".lower(),
            "UniversalRouter": "0x3fC91A3afd70395Cd496C647d5a6CC9D4B2b7FAD".lower(),
            "SushiSwapRouter": "0xd9e1cE17f2641f24aE83637ab66a2cca9C378B9F".lower(),
            "OKX": "0x3b3ae790Df4F312e745D270119c6052904FB6790".lower(), 
            "something1": '0xF3dE3C0d654FDa23daD170f0f320a92172509127'.lower(),
            "something2": '0x3c11F6265Ddec22f4d049Dde480615735f451646'.lower(),
            # "BananaGunRouter": "0xdb5889e35e379ef0498aae126fc2cce1fbd23216",
            }

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
    latest_block = w3_0.eth.get_block("latest")
    # backend0 = SQLiteCache(REQUEST_CACHE)
    # session0 = CachedSession(backend=backend0, expire_after=1, allowable_methods=('GET', 'POST', 'HEAD'))
    # w3_0 = Web3(HTTPProviderCached(alchemy_url, request_kwargs={'timeout': 10}, session=session0))
    backend = SQLiteCache(REQUEST_CACHE)
    session = CachedSession(backend=backend, expire_after=NEVER_EXPIRE, allowable_methods=('GET', 'POST', 'HEAD'))
    # w3_quicknode = Web3(Web3.HTTPProvider(quicknode_url))
    w3_1 = Web3(HTTPProviderCached(alchemy_url, request_kwargs={'timeout': 1e9}, session=session))
    # web3._utils.request.py.cache_and_return_session: cache_and_return_session
    return w3_0, session, w3_1, latest_block

def process_transaction(transaction_hash, transaction, run_context, debug_level=0, not_get_internals=False):
    cached_session = run_context["cached_session"]
    w3 = run_context["w3"]
    analytics = {'role': ""}

    trnx_to_address = transaction["to"].lower()
    if trnx_to_address in TARGET_ADRESSES.values():
        try:
            transaction["decoded_input"] = run_context["contract_storage"][trnx_to_address].decode_function_input(transaction["input"])
            _analytics = uniswap_transaction_decode(transaction)
        except:
            _analytics = None
        if not _analytics is None:
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
                        if analytics['V2_detected']:
                            analytics['swap_command']["type"] = "V2 swap"
                        elif analytics['V3_detected']:
                            analytics['swap_command']["type"] = "V3 swap"

                        if "amount_in" in o:
                            analytics['amount_in'] = o["amount_in"]
                        if "amount_out_min" in o:
                            analytics['amount_out_min'] = o["amount_out_min"]
                        if "amount_out" in o:
                            analytics['amount_out'] = o["amount_out"]
                        if "amount_in_max" in o:
                            analytics['amount_in_max'] = o["amount_in_max"]
                        if "sqrt_price_limit_X96" in o:
                            analytics['sqrt_price_limit_X96'] = o["sqrt_price_limit_X96"]
                        if "token_pairs" in o:
                            analytics["path"] = sum([[p[0], p[1] if isinstance(p[1], int) else bytes_to_int(p[1])] for p in o["token_pairs"]], [])
                        break

        if not_get_internals or ('V3_detected' in analytics and analytics['V3_detected']) or ('V2_detected' in analytics and analytics['V2_detected']):
            return analytics

    internals = trace_transaction(alchemy_url, transaction_hash, session=cached_session)

    if internals is None:
        internals = trace_transaction(alchemy_url, transaction_hash)
    if internals is None:
        return analytics
    run_context["internals"] = internals #!!! debug

    last_transfered_amount = None
    last_transfered_token = None
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
            continue #!!!
            addr_contract = get_contract_standard_token(w3, addr)
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
            
            last_transfered_amount = decoded_input[1][amount_field]
            last_transfered_token = t["action"]["to"].lower()
            analytics[i] = {"type": "transfer",
                            "transfered_amount": last_transfered_amount,
                            "transfered_token": last_transfered_token,
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

                continue #!!!

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
                                "transfered_amount": last_transfered_amount,
                                "transfered_token": last_transfered_token,
                                "amount0Out": decoded_input[1]["amount0Out"],
                                "amount1Out": decoded_input[1]["amount1Out"]}

            elif (not addr_contract is None 
                  and "_functions" in addr_contract.functions.__dict__
                  and "factory" in addr_contract.functions 
                  and addr_contract.functions.factory().call().lower() in V3_FACTORY.values()):
                
                # print(i, t)

                if not "token0" in addr_contract.functions:
                    continue

                if not "result" in t or t["result"] is None or len(t["result"]["output"]) < 130:
                    continue
                token0 = addr_contract.functions.token0().call()
                token1 = addr_contract.functions.token1().call()
                token1 = addr_contract.functions.token1().call()
                fee = addr_contract.functions.fee().call()

                amount0 = s64(int(t["result"]["output"][:66], 0))
                amount1 = s64(int(t["result"]["output"][:2] + t["result"]["output"][66:], 0))

                analytics[i] = {"type": "V3 swap",
                                "pool": addr.lower(),
                                "fee": fee,
                                "token0": token0.lower(),
                                "token1": token1.lower(),
                                "amount0": amount0,
                                "amount1": amount1}

                # print(i, analytics[i])
            else:
                continue

    return analytics


def process_block(block_number, run_context, debug_level=0, not_get_internals=False):
    w3 = run_context["w3"]

    block = w3.eth.get_block(block_number, full_transactions=True)
    block_hash = block["hash"].hex()
    miner = block["miner"].lower()
    run_context["miner"] = miner
    # base_fee_per_gas = block["baseFeePerGas"]
  
    if len(block["transactions"]) == 0:
        return {}

    from_to_count = {}
    for transaction in block["transactions"]:
        if not "to" in transaction:
            continue
        if (transaction["from"], transaction["from"]) in from_to_count:
            from_to_count[(transaction["from"], transaction["from"])] += 1
        else:
            from_to_count[(transaction["from"], transaction["from"])] = 1

    transactions_dict = {}
    for transaction in block["transactions"]:
    # transaction = block["transactions"][146]
        if not "to" in transaction or transaction["to"] is None:
            continue
        if not (transaction["to"].lower() in TARGET_ADRESSES.values() or from_to_count[(transaction["from"], transaction["from"])] > 1):
            continue
        transaction = dict(transaction)
        transaction_hash = transaction["hash"].hex()
        analytics = process_transaction(transaction_hash, transaction, run_context, debug_level, not_get_internals)
        V2_count = 0
        V3_count = 0
        for a in analytics:
            # if type(analytics[a]) == dict and "type" in analytics[a] and analytics[a]["type"] == "V2 swap":
            #     reserv = run_context["contract_storage"][analytics[a]["pair"]].functions.getReserves().call(block_identifier=block_hash)
            #     if not block_number in run_context["reserves"]:
            #         run_context["reserves"][block_number] = {}
            #     run_context["reserves"][block_number][analytics[a]["pair"]] = {0: reserv[0], 1: reserv[1]}
            #     V2_count += 1
            if type(analytics[a]) == dict and "type" in analytics[a] and analytics[a]["type"] == "V3 swap":
                V3_count += 1
                if "pool" in analytics[a]:
                    slot0 = run_context["contract_storage"][analytics[a]["pool"]].functions.slot0().call(block_identifier=block_hash)
                    liquidity = run_context["contract_storage"][analytics[a]["pool"]].functions.liquidity().call(block_identifier=block_hash)
                    if not block_number in run_context["reserves"]:
                        run_context["reserves"][block_number] = {}
                    run_context["reserves"][block_number][analytics[a]["pool"]] = {"sqrtp": slot0[0], "liquidity": liquidity}
        # V2_count = max(V2_count, 1 if "V2_detected" in analytics and analytics["V2_detected"] else 0)
        V3_count = max(V3_count, 1 if "V3_detected" in analytics and analytics["V3_detected"] else 0)
                       
        if V2_count + V3_count == 0:
            continue

        if transaction["to"].lower() in TARGET_ADRESSES.values():
            analytics["role"] = "target"
            analytics["to"] = transaction["to"].lower()
        if from_to_count[(transaction["from"], transaction["from"])] > 1:
            analytics["role"] = "sandwich"
        transaction["analytics"] = analytics
        transactions_dict[transaction_hash] = transaction

    return transactions_dict


def main():

    last_block = None
    N_blocks = 100

    w3_direct, cached_session, w3, latest_block = connect()
    # run_context["w3_direct"] =  w3_direct
    # run_context["cached_session"] =  cached_session
    # run_context["w3"] = w3

    abi_storage = {}
    contract_storage = {}
    reserves = {}
    run_context = {"w3_direct": w3_direct, 
                    "cached_session": cached_session, 
                    "w3": w3,
                    "delay": 0,
                    "etherscan_key": ETHERSCAN_KEY,
                    "abi_storage": abi_storage,
                    "contract_storage": contract_storage,
                    "reserves": reserves,
                    }

    latest_block_number = latest_block["number"]
    print(latest_block_number)
    if last_block is None:
        last_block = latest_block_number

    get_contract_sync(V2_FACTORY["UNISWAP_V2_FACTORY"], w3=w3, session=cached_session, context=run_context)
    get_contract_sync(WETH, w3=w3, session=cached_session, context=run_context)

    for a in TARGET_ADRESSES:
        if not TARGET_ADRESSES[a] in contract_storage:
            get_contract_sync(TARGET_ADRESSES[a], w3=w3, session=cached_session, context=run_context)

    all_transactions = {}
    block_number = last_block

    while block_number > last_block - N_blocks:
        block_transactions = process_block(block_number, run_context)
        all_transactions = all_transactions | block_transactions
        print(block_number, len(block_transactions))
        block_number -= 1
        
    for tx in all_transactions:
        print(tx, (all_transactions[tx]['blockNumber'], all_transactions[tx]['transactionIndex']),
              all_transactions[tx]["analytics"]["function"] if "function" in all_transactions[tx]["analytics"] else None,
              all_transactions[tx]["analytics"]['swap_command']["command"] if 'swap_command' in all_transactions[tx]["analytics"] else None)
#!!! >>>>>>>>>>>>>>>>>>>>>>>>>

    data_dict = []
    for tx in all_transactions:
        # tx = "0x6f5cc83664debc72a8b53c2763933309bd9bee1784a5bd9252f67893a2cc5986"
        # tx = "0xa446ac8619edff227f94e705a6223a916aeb24bc46ea2f263c49b63683f14244"
        
        itx = 0
        for txi in all_transactions[tx]["analytics"]:
            data_add = None
            if isinstance(txi, int):
                if not itx:
                    data_add = {'txi_itxi': (all_transactions[tx]['blockNumber'], all_transactions[tx]['transactionIndex'], txi),
                                'from': all_transactions[tx]['from'],
                                'to': all_transactions[tx]['to'],
                                'function': all_transactions[tx]["analytics"]["function"] if "function" in all_transactions[tx]["analytics"] else None,
                                'role': all_transactions[tx]["analytics"]["role"],
                                'value': all_transactions[tx]['value'] if 'value' in all_transactions[tx] else None,
                                'swap_command': all_transactions[tx]["analytics"]['swap_command']["command"] if 'swap_command' in all_transactions[tx]["analytics"] else None,
                                'amount_in': all_transactions[tx]["analytics"]['amount_in'] if 'amount_in' in all_transactions[tx]["analytics"] else None,
                                'amount_out_min': all_transactions[tx]["analytics"]['amount_out_min'] if 'amount_out_min' in all_transactions[tx]["analytics"] else None,
                                'amount_out': all_transactions[tx]["analytics"]['amount_out'] if 'amount_out' in all_transactions[tx]["analytics"] else None,
                                'amount_in_max': all_transactions[tx]["analytics"]['amount_in_max'] if 'amount_in_max' in all_transactions[tx]["analytics"] else None,
                                'sqrtp': reserves[all_transactions[tx]["blockNumber"]][all_transactions[tx]["analytics"][txi]["pool"]]["sqrtp"] if all_transactions[tx]["analytics"][txi]["type"] == "V3 swap" else None,
                                'liquidity': reserves[all_transactions[tx]["blockNumber"]][all_transactions[tx]["analytics"][txi]["pool"]]["liquidity"] if all_transactions[tx]["analytics"][txi]["type"] == "V3 swap" else None,
                                # } | all_transactions[tx]["reserves"] | all_transactions[tx]["analytics"][txi] | {"transaction_hash": tx}
                                } | all_transactions[tx]["analytics"][txi] | {"transaction_hash": tx}
                else:
                    data_add = {'txi_itxi': (all_transactions[tx]['blockNumber'], all_transactions[tx]['transactionIndex'], txi),
                                'from': None,
                                'to': None,
                                'function': None,
                                'role': all_transactions[tx]["analytics"]['role'] if 'swap_command' in all_transactions[tx]["analytics"] else None,
                                'value': all_transactions[tx]['value'] if 'value' in all_transactions[tx] else None,
                                'swap_command': all_transactions[tx]["analytics"]['swap_command']["command"] if all_transactions[tx]["analytics"][txi]["type"] in ["V2 swap", "V3 swap"] and 'swap_command' in all_transactions[tx]["analytics"] else None,
                                'amount_in': all_transactions[tx]["analytics"]['amount_in'] if all_transactions[tx]["analytics"][txi]["type"] in ["V2 swap", "V3 swap"] and 'amount_in' in all_transactions[tx]["analytics"] else None,
                                'amount_out_min': all_transactions[tx]["analytics"]['amount_out_min'] if all_transactions[tx]["analytics"][txi]["type"] in ["V2 swap", "V3 swap"] and 'amount_out_min' in all_transactions[tx]["analytics"] else None,
                                'amount_out': all_transactions[tx]["analytics"]['amount_out'] if all_transactions[tx]["analytics"][txi]["type"] in ["V2 swap", "V3 swap"] and 'amount_out' in all_transactions[tx]["analytics"] else None,
                                'amount_in_max': all_transactions[tx]["analytics"]['amount_in_max'] if all_transactions[tx]["analytics"][txi]["type"] in ["V2 swap", "V3 swap"] and 'amount_in_max' in all_transactions[tx]["analytics"] else None,
                                'sqrtp': reserves[all_transactions[tx]["blockNumber"]][all_transactions[tx]["analytics"][txi]["pool"]]["sqrtp"] if all_transactions[tx]["analytics"][txi]["type"] == "V3 swap" else None,
                                'liquidity': reserves[all_transactions[tx]["blockNumber"]][all_transactions[tx]["analytics"][txi]["pool"]]["liquidity"] if all_transactions[tx]["analytics"][txi]["type"] == "V3 swap" else None,
                                } | all_transactions[tx]["analytics"][txi] | {"transaction_hash": tx}
                                # } | all_transactions[tx]["reserves"] | all_transactions[tx]["analytics"][txi] | {"transaction_hash": tx}
                itx += 1
            # print(data_add)
            if data_add:
                data_dict.append(data_add)
        if not itx:
            calc_result = {}
            if 'swap_command' in all_transactions[tx]["analytics"]:
                data_add = {'txi_itxi': (all_transactions[tx]['blockNumber'], all_transactions[tx]['transactionIndex'], 0),
                            'from': all_transactions[tx]['from'],
                            'to': all_transactions[tx]['to'],
                            'function': all_transactions[tx]["analytics"]["function"] if "function" in all_transactions[tx]["analytics"] else None,
                            'role': all_transactions[tx]["analytics"]["role"],
                            'value': all_transactions[tx]['value'] if 'value' in all_transactions[tx] else None,
                            'swap_command': all_transactions[tx]["analytics"]['swap_command']["command"],
                            'amount_in': all_transactions[tx]["analytics"]['amount_in'] if 'amount_in' in all_transactions[tx]["analytics"] else None,
                            'amount_out_min': all_transactions[tx]["analytics"]['amount_out_min'] if 'amount_out_min' in all_transactions[tx]["analytics"] else None,
                            'amount_out': all_transactions[tx]["analytics"]['amount_out'] if 'amount_out' in all_transactions[tx]["analytics"] else None,
                            'amount_in_max': all_transactions[tx]["analytics"]['amount_in_max'] if 'amount_in_max' in all_transactions[tx]["analytics"] else None,
                            } | {"transaction_hash": tx}
                if all_transactions[tx]["analytics"]['swap_command']['command'] == 'V3_SWAP_EXACT_IN':
                    (calc_result["calc_status"], 
                     calc_result["optimal_amount"], 
                     calc_result["profit"], 
                     victim_amount,
                     calc_result["token_in"], 
                     calc_result["fee"], 
                     calc_result["token_out"]) = sandwich_V3_SWAP_EXACT_IN(all_transactions[tx]["analytics"]['amount_in'],
                                                                  all_transactions[tx]["analytics"]['amount_out_min'],
                                                                  all_transactions[tx]["analytics"]['path'],
                                                                  w3, all_transactions[tx]['blockHash'].hex())
            else:
                data_add = {'txi_itxi': (all_transactions[tx]['blockNumber'], all_transactions[tx]['transactionIndex'], 0),
                            'from': all_transactions[tx]['from'],
                            'to': all_transactions[tx]['to'],
                            'function': all_transactions[tx]["analytics"]["function"] if "function" in all_transactions[tx]["analytics"] else None,
                            'role': all_transactions[tx]["analytics"]["role"],
                            'value': all_transactions[tx]['value'] if 'value' in all_transactions[tx] else None,
                            'swap_command': None,
                            'amount_in': all_transactions[tx]["analytics"]['operations'][0]['amount_in'] if 'amount_in' in all_transactions[tx]["analytics"]['operations'][0] else None,
                            'amount_out_min': all_transactions[tx]["analytics"]['operations'][0]['amount_out_min'] if 'amount_out_min' in all_transactions[tx]["analytics"]['operations'][0] else None,
                            'amount_out': all_transactions[tx]["analytics"]['operations'][0]['amount_out'] if 'amount_out' in all_transactions[tx]["analytics"]['operations'][0] else None,
                            'amount_in_max': all_transactions[tx]["analytics"]['operations'][0]['amount_in_max'] if 'amount_in_max' in all_transactions[tx]["analytics"]['operations'][0] else None,
                            } | {"transaction_hash": tx}
            if data_add['function'] == 'exactInputSingle':
                (calc_result["calc_status"], 
                 calc_result["optimal_amount"], 
                 calc_result["profit"],
                 victim_amount) = sandwich_exact_input_single(all_transactions[tx]["analytics"]['operations'][0]['token_pairs'][0][0],
                                            all_transactions[tx]["analytics"]['operations'][0]['token_pairs'][1][0],
                                            all_transactions[tx]["analytics"]['operations'][0]['token_pairs'][0][1],
                                            all_transactions[tx]["analytics"]['operations'][0]['amount_in'],
                                            all_transactions[tx]["analytics"]['operations'][0]['amount_out_min'],
                                            all_transactions[tx]["analytics"]['operations'][0]['sqrt_price_limit_X96'],
                                            w3, all_transactions[tx]['blockHash'].hex())
            elif data_add['function'] == 'exactOutputSingle':
                (calc_result["calc_status"], 
                 calc_result["optimal_amount"], 
                 calc_result["profit"],
                 victim_amount) = sandwich_exact_output_single(all_transactions[tx]["analytics"]['operations'][0]['token_pairs'][0][0],
                                            all_transactions[tx]["analytics"]['operations'][0]['token_pairs'][1][0],
                                            all_transactions[tx]["analytics"]['operations'][0]['token_pairs'][0][1],
                                            all_transactions[tx]["analytics"]['operations'][0]['amount_out'],
                                            all_transactions[tx]["analytics"]['operations'][0]['amount_in_max'],
                                            all_transactions[tx]["analytics"]['operations'][0]['sqrt_price_limit_X96'],
                                            w3, all_transactions[tx]['blockHash'].hex())
            data_dict.append(data_add | calc_result)
               
    df = pd.DataFrame.from_records(data_dict, index=["txi_itxi"])

    # df.to_csv("/media/Data/csv/df_txs_v3" + str(N_blocks) + ".csv")


def garbage_zone(w3, cached_session, address):
    pass