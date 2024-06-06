#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import eth_abi
import numpy as np
import pandas as pd
import requests
from web3 import Web3
import math

KEY_FILE = '../keys/alchemy.sec'
ETHERSCAN_KEY_FILE = '../keys/etherscan.sec'

HEADERS = {'Content-Type': "application/json"}
ETHERSCAN_GETABI = 'http://api.etherscan.io/api?module=contract&action=getabi&address={}&apikey={}'

UNISWAP_UNIVERSAL_ROUTER = "0xef1c6e67703c7bd7107eed8303fbe6ec2554bf6b".lower()
V3_FACTORY = "0x1F98431c8aD98523631AE4a59f267346ea31F984".lower()
QUOTER_V2 = "0x0209c4Dc18B2A1439fD2427E34E7cF3c6B91cFB9".lower()
WETH = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2".lower()
USDC = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48".lower()

with open(KEY_FILE, 'r') as f:
    k1 = f.readline()
    ALCHEMY_URL = k1.strip('\n')
    k2 = f.readline()
    ALCHEMY_WSS = k2.strip('\n')

with open(ETHERSCAN_KEY_FILE, 'r') as f:
    k1 = f.readline()
    ETHERSCAN_KEY = k1.strip('\n')


def get_abi(address):
    try:
        res = requests.get(ETHERSCAN_GETABI.format(address, ETHERSCAN_KEY), headers=HEADERS)
        d = res.json()
        abi = d["result"]
        return abi
    except:
        return None

def get_contract(w3, abi, address):
    return w3.eth.contract(address=Web3.to_checksum_address(address), abi=abi)

def bytes_to_int(s):
    res = 0
    for b in s:
        res = res * 256 + b
    return res

def extract_path_from_V3(str_path):
    path = []
    i = 0
    while i < len(str_path):
        path.append(("0x" + str_path[i:i+20].hex().lower(), bytes_to_int(str_path[i+20:i+23])))
        i = i + 23
    return path

UNISWAP_UNIVERSAL_ROUTER_COMMANDS = [
    ("V3_SWAP_EXACT_IN", 0x00, ["address", "uint256", "uint256", "bytes", "bool"]),
    ("V3_SWAP_EXACT_OUT", 0x01, ["address", "uint256", "uint256", "bytes", "bool"]),
]

def uniswap_universal_router_command_abi(command):
    for p in UNISWAP_UNIVERSAL_ROUTER_COMMANDS:
        if p[0] == command and len(p) > 2:
            return p[2]
    return None

def uniswap_universal_router_code_to_command(code):
    for p in UNISWAP_UNIVERSAL_ROUTER_COMMANDS:
        if p[1] == code:
            return p[0]
    return None

def uniswap_transaction_decode(transaction):
    fn_name = transaction["decoded_input"][0].abi["name"]
    operations = []

    if fn_name != 'execute':
        return operations
    command_codes = transaction["decoded_input"][1]["commands"]
    for i, command_code in enumerate(command_codes):
        command = uniswap_universal_router_code_to_command(command_code)
        abi = uniswap_universal_router_command_abi(command)
        if abi:
            none_zero_input = eth_abi.abi.decode(abi, transaction["decoded_input"][1]["inputs"][i])
            if command == "V3_SWAP_EXACT_IN":
                operations.append({"command": command,
                                      "amount_in": none_zero_input[1],
                                      "amount_out_min": none_zero_input[2],
                                      "token_pairs": extract_path_from_V3(none_zero_input[3])
                                      })
            elif command == "V3_SWAP_EXACT_OUT":
                operations.append({"command": command,
                                      "amount_out": none_zero_input[1],
                                      "amount_in_max": none_zero_input[2],
                                      "token_pairs": extract_path_from_V3(none_zero_input[3])
                                      })

    return operations


w3 = Web3(Web3.HTTPProvider(ALCHEMY_URL))

transaction_hash = "0x128069e933e8d276e23da8743dd842f189de47c7aca499bb69b0421e21419293"
# transaction_hash = "0x6e33e113fc2309a1e775e5722f13f56d9b81cf4c0fa861948ee93c6d65866499"
# transaction_hash = "0x9a445eeb5af225915a91959ef0d34c313ee5a9338c6b9d118b886cfb08549c70"

transaction = dict(w3.eth.get_transaction(transaction_hash))
contract_address = transaction["to"]
contract_abi = get_abi(contract_address)
contract_to = get_contract(w3, contract_abi, contract_address)
factory_abi = get_abi("0x1F98431c8aD98523631AE4a59f267346ea31F984")
factory_contract = get_contract(w3, factory_abi, "0x1F98431c8aD98523631AE4a59f267346ea31F984")

quoter_v2_abi = get_abi(QUOTER_V2)
quoter_v2 = get_contract(w3, quoter_v2_abi, QUOTER_V2)

transaction["decoded_input"] = contract_to.decode_function_input(transaction["input"])
operations = uniswap_transaction_decode(transaction)
token_in = Web3.to_checksum_address(operations[0]["token_pairs"][0][0])
token_out = Web3.to_checksum_address(operations[0]["token_pairs"][1][0])
fee = operations[0]["token_pairs"][0][1]
amount_in = operations[0]["amount_in"]
amount_out_min = operations[0]["amount_out_min"]

pool_address = factory_contract.functions.getPool(token_in, token_out, fee).call()
pool_abi = get_abi(pool_address)
pool_contract = get_contract(w3, pool_abi, pool_address)

token_out_abi = get_abi(token_out)
token_out_contract = get_contract(w3, token_out_abi, token_out)
token_out_decimals = token_out_contract.functions.decimals().call()
token_in_abi = get_abi(token_in)
token_in_contract = get_contract(w3, token_in_abi, token_in)
token_in_decimals = token_in_contract.functions.decimals().call()

quote_params = {"tokenIn": token_in, "tokenOut": token_out, "amountIn": amount_in, "fee": fee, "sqrtPriceLimitX96": 0}
current_quote = quoter_v2.functions.quoteExactInputSingle(quote_params).call()

block_number = transaction["blockNumber"]
prev_block = w3.eth.get_block(block_number-1, full_transactions=False)
quote = quoter_v2.functions.quoteExactInputSingle(quote_params).call(block_identifier=prev_block["hash"].hex())
slot0 = pool_contract.functions.slot0().call(block_identifier=prev_block["hash"].hex())

loss = (quote[0] - amount_out_min) / (quote[0] / amount_in) / 10 ** token_in_decimals