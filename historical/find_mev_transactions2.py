#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
os.sys.path.append(os.path.dirname(os.path.abspath('.')))
from requests_cache import CachedSession, SQLiteCache, NEVER_EXPIRE
import requests
import pandas as pd
from datetime import datetime
import time
from web3 import Web3
from http.client import RemoteDisconnected

from _utils.utils import HTTPProviderCached
from _utils.utils import etherscan_get_internals, trace_transaction, s64
from _utils.token_abi import token_abi

KEY_FILE = '../keys/alchemy.sec'
KEY_FILE_QUICKNODE = '../keys/quicknode.sec'
ETHERSCAN_KEY_FILE = '../keys/etherscan.sec'
REQUEST_CACHE = '/media/Data/eth/eth'
CSV_FILE = "/media/Data/csv/searcher_profit"
HEADERS = {'Content-Type': "application/json"}
ETHERSCAN_GETABI = 'http://api.etherscan.io/api?module=contract&action=getabi&address={}&apikey={}'
NUMBER_BLOCKS_IN_CHUNK = 10000
MAX_RETRY = 10
V2_FACTORY = ["0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f".lower(), "0xC0AEe478e3658e2610c5F7A4A2E1777cE9e4f2Ac".lower()]
V3_FACTORY = ["0x1F98431c8aD98523631AE4a59f267346ea31F984".lower()]
WETH = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2".lower()
USDC_LIKE = ["0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48".lower(), '0x0000000000085d4780b73119b644ae5ecd22b376'.lower()]

with open(KEY_FILE, 'r') as f:
    k1 = f.readline()
    alchemy_url = k1.strip('\n')
    k2 = f.readline()
    alchemy_wss = k2.strip('\n')

# with open(KEY_FILE_QUICKNODE, 'r') as f:
#     k1 = f.readline()
#     quicknode_url = k1.strip('\n')
#     k2 = f.readline()
#     quicknode_wss = k2.strip('\n')

with open(ETHERSCAN_KEY_FILE, 'r') as f:
    k1 = f.readline()
    etherscan_key = k1.strip('\n')

class Sandwich():
    def __init__(self, block_number, miner, attacker0, attacker1, block_fee):
        self.block_fee = block_fee
        self.block_number = block_number
        self.miner = miner
        self.attacker0 = attacker0
        self.attacker1 = attacker1
        self.tokens = {}
        self.prices = {}
        self.bribes = 0
        self.total_gas = 0
        self.burnt_gas = 0
        self.count = 0

    def add_amount(self, token, amount):
        token = token.lower()
        if not token in self.tokens:
            self.tokens[token] = amount
        else:
            self.tokens[token] += amount

    def add_price(self, token0, token1, amount0, amount1):
        token0 = token0.lower()
        token1 = token1.lower()
        if (token1, token0) in self.prices:
            if amount0 != 0:
                self.prices[(token1, token0)] = abs(amount1 / amount0)
        else:
            if amount1 != 0:
                self.prices[(token0, token1)] = abs(amount0 / amount1)

    def add_bribe(self, bribe):
        self.bribes += bribe

    def add_gas_fee(self, burnt_gas, total_gas):
        self.count += 1
        self.total_gas += total_gas
        self.burnt_gas += burnt_gas

    def _price(self, token0, token1):
        for (t0, t1) in self.prices:
            if t0 == token0 and t1 == token1:
                return self.prices[(t0, t1)]
            elif t1 == token0 and t0 == token1:
                if self.prices[(t0, t1)] == 0:
                    return None
                else:
                    return 1 / self.prices[(t0, t1)]
        return None

    def evaluate(self):
        value = 0
        if WETH in self.tokens:
            for t in self.tokens:
                if t == WETH:
                    value += self.tokens[t]
                else:
                    price = self._price(t, WETH)
                    if not price:
                        return None, None, None, None     #!!! indirect swaps excluded
                    value += self.tokens[t] * self._price(WETH, t)
            return value, self.tokens[WETH], self.total_gas, self.bribes
        else:
            return None, None, None, None
        
    def report(self):
        value, weth, total_gas, bribes = self.evaluate()
        return {"block_number": self.block_number, 
                "miner": self.miner, 
                "attaker": self.attacker1, 
                "value": value, 
                "weth": weth, 
                "block_fee": self.block_fee, 
                "total_gas": total_gas, 
                "burnt_gas": self.burnt_gas, 
                "bribes": bribes}

def get_contract_sync(address, _w3, session=None, delay=0):
    if address in USDC_LIKE:
        abi = token_abi
    else:
        if session:
            res = session.get(ETHERSCAN_GETABI.format(address, etherscan_key), headers=HEADERS, force_refresh=(delay != 0))
        else:
            res = requests.get(ETHERSCAN_GETABI.format(address, etherscan_key), headers=HEADERS)
        abi = res.json()["result"]

    try:
        contract = _w3.eth.contract(address=Web3.to_checksum_address(address), abi=abi)
        return contract, abi
    except:
        print(address, abi)
        if delay:
            time.sleep(0.1)
        return None, abi

def get_contract_standard_abi(address, _w3):
    return _w3.eth.contract(address=Web3.to_checksum_address(address), abi=token_abi)

def find_token_in_transfer_input(t, decoded_input, decimals, attakers):
    if t["action"]["input"][:10] == "0xa9059cbb":
        to_field = list(decoded_input[1].keys())[0]
        transfered_token = t["action"]["to"].lower()
        if ((decoded_input[1][to_field].lower() in attakers and t["action"]["from"].lower() in attakers) or
            (not decoded_input[1][to_field].lower() in attakers and not t["action"]["from"].lower() in attakers)):
            transfered_amount = 0
        else:
            amount_field = list(decoded_input[1].keys())[1]
            transfered_amount = decoded_input[1][amount_field] * (1 if decoded_input[1][to_field].lower() in attakers else -1) / (10 ** decimals)
    elif t["action"]["input"][:10] == "0x23b872dd":
        to_field = list(decoded_input[1].keys())[1]
        from_field = list(decoded_input[1].keys())[0]
        transfered_token = t["action"]["to"].lower()
        if ((decoded_input[1][to_field].lower() in attakers and decoded_input[1][from_field].lower() in attakers) or
            (not decoded_input[1][to_field].lower() in attakers and not decoded_input[1][from_field].lower() in attakers)):
            transfered_amount = 0
        else:
            amount_field = list(decoded_input[1].keys())[2]
            transfered_amount = decoded_input[1][amount_field] * (1 if decoded_input[1][to_field].lower() in attakers else -1) / (10 ** decimals)
    else:
        transfered_token = None
        transfered_amount = None
    return transfered_token, transfered_amount

def wrap_with_try(f, *args, **kwargs):
    try:
        return f(*args, **kwargs)
    except:
        return None

def connect():
    w3_0 = Web3(Web3.HTTPProvider(alchemy_url))
    backend = SQLiteCache(REQUEST_CACHE)
    session = CachedSession(backend=backend, expire_after=NEVER_EXPIRE, allowable_methods=('GET', 'POST', 'HEAD'))
    # w3_quicknode = Web3(Web3.HTTPProvider(quicknode_url))
    w3_1 = Web3(HTTPProviderCached(alchemy_url, request_kwargs={'timeout': 1e9}, session=session))
    #!!! web3._utils.request.py.cache_and_return_session: cache_and_return_session
    return w3_0, session, w3_1


def save_data(sandwiches, nums):
    records = []
    indexes = []
    for s in sandwiches:
        if sandwiches[s].count < 2:
            continue
        try:
            r = sandwiches[s].report()
            if r["total_gas"] and (r["value"] or r["weth"]) and len(sandwiches[s].prices) > 0:
                records.append(r)
                indexes.append(s)
        except:
            pass
    df = pd.DataFrame.from_records(records, index=indexes)
    df.to_csv(CSV_FILE + nums + ".csv")

def debug_print(debug_level, *args):
    if debug_level > 0:
        print(*args, flush=True)

def process_transaction(transaction_hash, sandwich, bundle_context, run_context, debug_level=0):
    cached_session = run_context["cached_session"]
    w3 = run_context["w3"]
    transfered_token = None
    transfered_amount = None
    internals = trace_transaction(alchemy_url, transaction_hash, session=cached_session)
    if internals is None:
        internals = trace_transaction(alchemy_url, transaction_hash)
    if internals is None:
        return
    run_context["internals"] = internals #debug

    receipt = w3.eth.get_transaction_receipt(transaction_hash)
    if receipt["status"] != 1:
        return
    sandwich.add_gas_fee(receipt["gasUsed"] * bundle_context["base_fee_per_gas"] / 1e18,
                         receipt["gasUsed"] * receipt["effectiveGasPrice"] / 1e18)

    for i, t in enumerate(internals):
        if not "to" in t["action"]:
            continue
        addr = t["action"]["to"].lower()
        if t["action"]["to"] == bundle_context["miner"]:
            if "value" in t["action"]:
                sandwich.add_bribe(int(t["action"]["value"], 0) / 1e18)
            continue
        if t["action"]["to"] == bundle_context["attacker"][1]:
            debug_print(debug_level, i, "command to ", bundle_context["attacker"][1], int(t["action"]["value"],0))
            continue
        addr_contract, addr_abi = get_contract_sync(addr, w3, cached_session, run_context["delay"])
        run_context["addr_contract"] = addr_contract
        if addr_contract is None:
            continue
        if t["action"]["input"][:10] == "0xa9059cbb" or t["action"]["input"][:10] == "0x23b872dd":
            if not "decimals" in addr_contract.functions:
                if not "name" in addr_contract.functions:
                    addr_contract = get_contract_standard_abi(addr, w3)
                else:
                    continue
            # name = addr_contract.functions.name().call()
            try:
                decimals = addr_contract.functions.decimals().call()
            except:
                continue
            decoded_input = wrap_with_try(addr_contract.decode_function_input, t["action"]["input"])
            if decoded_input is None:
                print(i, "incorrect input for decode", transaction_hash, bundle_context["attacker"])
                continue
            transfered_token, transfered_amount = find_token_in_transfer_input(t, decoded_input, decimals, bundle_context["attacker"])
            if not transfered_token is None and not transfered_amount is None:
                sandwich.add_amount(transfered_token, transfered_amount)
            debug_print(debug_level, i, transfered_token , "transfer", transfered_amount, decoded_input, int(t["action"]["value"],0))
            continue
        elif t["action"]["input"][:10] == "0x70a08231":
            debug_print(debug_level, i, "balanceOF", t["action"]["to"])
            continue
        elif t["action"]["input"][:10] == "0xd06ca61f":
            debug_print(debug_level, i, "getAmountOut", t["action"]["to"])
            continue
        elif t["action"]["input"][:10] in ["0x7739cbe7", "0x330deb9f", "0x"]:
            continue
        else:
            if (not addr_contract is None 
                and "_functions" in addr_contract.functions.__dict__
                and "factory" in addr_contract.functions 
                and addr_contract.functions.factory().call().lower() in V2_FACTORY):
                if transfered_amount is None:
                    print(i, "None transfered_amount for V2", transaction_hash, bundle_context["attacker"])
                    continue
                decoded_input = wrap_with_try(addr_contract.decode_function_input, t["action"]["input"])
                if decoded_input is None:
                    print(i, "incorrect input for decode", transaction_hash, bundle_context["attacker"])
                    continue
                if not "token0" in addr_contract.functions:
                    continue
                token0 = addr_contract.functions.token0().call().lower()
                token1 = addr_contract.functions.token1().call().lower()
                if not "amount0Out" in decoded_input[1]:
                    continue
                if transfered_token == token0:
                    # sandwich.add_amount(token1, decoded_input[1]["amount1Out"] / 1e18)
                    token1_contract = get_contract_standard_abi(token1, w3)
                    decimals1 = token1_contract.functions.decimals().call()
                    sandwich.add_price(token0, token1, transfered_amount, (decoded_input[1]["amount0Out"] + decoded_input[1]["amount1Out"]) / (10 ** decimals1))
                else:
                    # sandwich.add_amount(token0, decoded_input[1]["amount0Out"] / 1e18)
                    token0_contract = get_contract_standard_abi(token0, w3)
                    decimals0 = token0_contract.functions.decimals().call()
                    sandwich.add_price(token0, token1, (decoded_input[1]["amount0Out"] + decoded_input[1]["amount1Out"]) / (10 ** decimals0), transfered_amount)
                debug_print(debug_level, i, t["action"]["to"], "swap V2", transfered_token, transfered_amount, token0, token1, decoded_input, int(t["action"]["value"], 0))
            elif (not addr_contract is None 
                  and "_functions" in addr_contract.functions.__dict__
                  and "factory" in addr_contract.functions 
                  and addr_contract.functions.factory().call().lower() in V3_FACTORY):
                if not "token0" in addr_contract.functions:
                    continue
                # if t["action"]["input"][:10] == "0x0dfe1681":
                if not "result" in t or len(t["result"]["output"]) < 130:
                    continue
                token0 = addr_contract.functions.token0().call()
                token1 = addr_contract.functions.token1().call()
                token0_contract = get_contract_standard_abi(token0, w3)
                token1_contract = get_contract_standard_abi(token1, w3)
                decimals0 = token0_contract.functions.decimals().call()
                decimals1 = token1_contract.functions.decimals().call()
                amount0 = s64(int(t["result"]["output"][:66], 0)) / (10 ** decimals0)
                amount1 = s64(int(t["result"]["output"][:2] + t["result"]["output"][66:], 0)) / (10 ** decimals1)
                # sandwich.add_amount(token0, -amount0)
                # sandwich.add_amount(token1, -amount1)
                sandwich.add_price(token0, token1, amount0, amount1)
                debug_print(debug_level, i, t["action"]["to"], "swap V3", amount0, token0, amount1, token1, int(t["action"]["value"], 0))
            else:
                print(i, "something else", t["action"]["input"][:10], transaction_hash)
                continue

def process_bundle(bundle_context, run_context, debug_level=0):

    sandwich = Sandwich(bundle_context["block_number"], 
                        bundle_context["miner"], 
                        bundle_context["attacker"][0], 
                        bundle_context["attacker"][1],
                        bundle_context["block_fee"])
    for (transaction_index, transaction_hash) in bundle_context["transactions"]:
        process_transaction(transaction_hash, sandwich, bundle_context, run_context, debug_level)
    if len(sandwich.tokens) > 0:
        run_context["sandwiches"][transaction_hash] = sandwich

def process_block(block_number, run_context, debug_level=0):
    w3 = run_context["w3"]
    from_to_hashes = {}
    block = w3.eth.get_block(block_number, full_transactions=True)
    miner = block["miner"].lower()
    base_fee_per_gas = block["baseFeePerGas"]
  
    if len(block["transactions"]) == 0:
        return
    for transaction in block["transactions"]:
        transaction_hash = transaction["hash"].hex()
        if "to" in transaction:
            if not (transaction["from"], transaction["to"]) in from_to_hashes:
                from_to_hashes[(transaction["from"], transaction["to"])] = []
            from_to_hashes[(transaction["from"], transaction["to"])].append((transaction["transactionIndex"], transaction_hash))

    if transaction["from"].lower() == miner and transaction["input"] == '0x':
        block_fee = transaction["value"] / 1e18
    else:
        block_fee = 0

    for from_to in from_to_hashes:
        if len(from_to_hashes[from_to]) > 1:
            bundle_context = {"attacker": (from_to[0].lower(), from_to[1].lower()),
                              "miner": miner,
                              "block_number": block_number,
                              "block_fee": block_fee,
                              "base_fee_per_gas": base_fee_per_gas,
                              "transactions": from_to_hashes[from_to]}
            run_context["bundle_context"] = bundle_context #debug
            process_bundle(bundle_context, run_context, debug_level)

def main(start_block=None, number_blocks=None, delay=0):
    if number_blocks is None:
        number_blocks = NUMBER_BLOCKS_IN_CHUNK
    w3_direct, cached_session, w3 = connect()
    latest_block_number = w3_direct.eth.get_block('latest')["number"]
    print(latest_block_number)
    
    if start_block is None:
        start_block = latest_block_number
    block_number = start_block

    ii = 0
    t0 = datetime.now()
    sandwiches = {}
    run_context = {"w3_direct": w3_direct, 
                   "cached_session": cached_session, 
                   "w3": w3, 
                   "sandwiches": sandwiches,
                   "delay": delay}

    while True:
        try:
            while True:
                # block_number = 18269986 # !!!
                process_block(block_number, run_context, debug_level=0)
                
                print(ii, block_number, (datetime.now()-t0).total_seconds())
                t0 = datetime.now()
                block_number -= 1; ii += 1

                if ii >= number_blocks:
                    break
                if ii % 100 == 0:
                    save_data(run_context["sandwiches"], str(start_block) + "-" + str(start_block-number_blocks))

        except RemoteDisconnected:
            w3_direct, cached_session, w3 = connect()
            run_context["cached_session"] = cached_session
            run_context["w3_direct"] = w3_direct
            run_context["w3"] = w3
            time.sleep(1)
        # except Exception as e:
        #     print(e)
        #     break

        if ii >= number_blocks:
            break

    save_data(run_context["sandwiches"], str(start_block) + "-" + str(start_block-number_blocks))

def tester():
    w3_direct, cached_session, w3 = connect()
    sandwiches = {}
    run_context = {"w3_direct": w3_direct, "cached_session": cached_session, "w3": w3, "sandwiches": sandwiches}

    debug_level = 1
    block_number = 18313522
    transaction_hash = "0x58f06cc1bfdc689014385922253e222ef52f6010d6420cc118f26faed933af43"
    block_number = 18315246
    transaction_hash = "0xa55744960b9cdfa120d66c3401a4443e9c0609c411fe258c1381cbb2237df462"

    block = w3.eth.get_block(block_number, full_transactions=True)
    miner = block["miner"].lower()
    base_fee_per_gas = block["baseFeePerGas"]
  
    for transaction in block["transactions"]:
        if transaction_hash == transaction["hash"].hex():
            from_to = (transaction["from"], transaction["to"])
            from_to_hash = [(transaction["transactionIndex"], transaction_hash)]

    if transaction["from"].lower() == miner and transaction["input"] == '0x':
        block_fee = transaction["value"] / 1e18
    else:
        block_fee = 0

    bundle_context = {"attacker": (from_to[0].lower(), from_to[1].lower()),
                              "miner": miner,
                              "block_number": block_number,
                              "block_fee": block_fee,
                              "base_fee_per_gas": base_fee_per_gas,
                              "transactions": from_to_hash}
    run_context["bundle_context"] = bundle_context #debug
    
    sandwich = Sandwich(bundle_context["block_number"], 
                        bundle_context["miner"], 
                        bundle_context["attacker"][0], 
                        bundle_context["attacker"][1],
                        bundle_context["block_fee"])
    (transaction_index, transaction_hash) = from_to_hash[0]
    
    process_transaction(transaction_hash, sandwich, bundle_context, run_context, debug_level)
    internals = run_context["internals"]

def garbage_zone(w3, cached_session, address, sandwiches):
    
    sandwiches["0x58f06cc1bfdc689014385922253e222ef52f6010d6420cc118f26faed933af43"].evaluate()
    
    sandwiches["0xbcb14dad9f5d0c9006f93955fc1df84fb7f49b0bc4e77d179f2819ffa1555ddb"].report()
    
    address = "0x000000000c923384110e9dca557279491e00f521"
    a_contract, a_abi = get_contract_sync(address, w3, cached_session)

    internals12 = etherscan_get_internals(18297328, address="0x00FC00900000002C00BE4EF8F49c000211000c43")

    tx = "0x1ed4161163a0890a765e3361538a07d504d415e39df249c5f5fdbfd2408aecef"
    internals13 = etherscan_get_internals(18297328, txhash=tx, session=cached_session)
    tx = "0x1e82647e36a90cd0df2408d93405f5423c7b39b72e73d94d19d43ff32f410a48"
    internals14 = etherscan_get_internals(18297328, txhash=tx, session=cached_session)
    
    tx = "0x1ed4161163a0890a765e3361538a07d504d415e39df249c5f5fdbfd2408aecef"
    internals_a = trace_transaction(alchemy_url, tx)
    for t in internals_a:
        print(t["action"], int(t["action"]["value"],0))

    tx = "0x1e82647e36a90cd0df2408d93405f5423c7b39b72e73d94d19d43ff32f410a48"
    internals_b = trace_transaction(alchemy_url, tx)
    for t in internals_b:
        print(t["action"], int(t["action"]["value"],0))
        
    UniversalRouter = "0x3fC91A3afd70395Cd496C647d5a6CC9D4B2b7FAD".lower()
    UniversalRouter_contract, UniversalRouter_abi = get_contract_sync(UniversalRouter, w3, cached_session)

    v2pair1 = "0x3681a3feed68414617d61aeed92ca5c13eb2ae4a"
    v2pair1_c, v2pair1_a = get_contract_sync(v2pair1, w3, cached_session)
    v2pair1_c.functions.name().call()
    
    internals_b[1]["action"]["input"]
    
    itx_decoded_input = UniversalRouter_contract.decode_function_input(internals_b[1]["action"]["input"])

    v2pair1_c.decode_function_input(internals_b[2]["action"]["input"])



if __name__ == '__main__':
    if len(sys.argv) >= 3:
        start_block = int(sys.argv[2])
    else:
        start_block = None
    if len(sys.argv) >= 2:
        num_blocks = int(sys.argv[1])
    else:
        num_blocks = None
    if len(sys.argv) >= 4 and int(sys.argv[3]) != 0:
        delay = 1
    else:
        delay = 0
    main(start_block, num_blocks, delay)
