#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
os.sys.path.append(os.path.dirname(os.path.abspath('.')))
from web3 import Web3
from _utils.etherscan import _get_abi, _get_contract
from _utils.utils import RED, BLUE, GREEN, RESET_COLOR

from contract_caller import swap, init_context
from swap_math import MAX_UINT_160

HEADERS_E = {'Content-Type': "application/json"}
ETHERSCAN_GETABI = 'http://api.etherscan.io/api?module=contract&action=getabi&address={}&apikey={}'

KEY_FILE = '../keys/alchemy.sec'
ETHERSCAN_KEY_FILE = '../keys/etherscan.sec'

with open(KEY_FILE, 'r') as f:
    k1 = f.readline()
    ALCHEMY_URL = k1.strip('\n')
    k2 = f.readline()
    ALCHEMY_WSS = k2.strip('\n')

with open(ETHERSCAN_KEY_FILE, 'r') as f:
    k1 = f.readline()
    ETHERSCAN_KEY = k1.strip('\n')

def get_abi(address, abi_type=None):
    return _get_abi(address, ETHERSCAN_KEY, abi_type=abi_type)

def get_contract(w3, abi, address):
    return _get_contract(w3, abi, Web3.to_checksum_address(address))

w3 = Web3(Web3.HTTPProvider(ALCHEMY_URL))
ticks_dict = {}
tick_bitmap_dict = {}
context = {"ticks_dict": ticks_dict,
           "tick_bitmap_dict": tick_bitmap_dict,
           "slot0": None,
           "w3": w3,
           "pool_address": None,
           "pool_contract": None,
           }


#################################  example 1

POOL = "0x0a1665e3f54eeb364bec6954e4497dd802840a01" 
pool_abi = get_abi(POOL, "pool")
context["pool_contract"] = get_contract(w3, pool_abi, POOL)
context["pool_address"] = POOL

print(GREEN, "exact token0 in", RESET_COLOR)
# amount_in = 0.00000001
amount_in = 0.0001
amountSpecified = int(amount_in * 1e18)
print(BLUE, amountSpecified, RESET_COLOR)
zeroForOne = 1
sqrtPriceLimitX96 = 0

init_context(context)
amount0, amount1, tick_diff = swap(zeroForOne, amountSpecified, sqrtPriceLimitX96, context)
print(amount0, amount1, tick_diff)
print(BLUE, -amount1, RESET_COLOR)
print(-amount1/amount0, tick_diff)
print()

print(GREEN, "exact token1 in", RESET_COLOR)
amountSpecified = -amount1
print(BLUE, amountSpecified, RESET_COLOR)
zeroForOne = 0
sqrtPriceLimitX96 = 0

amount0, amount1, tick_diff = swap(zeroForOne, amountSpecified, sqrtPriceLimitX96, context)
print(amount0, amount1, tick_diff)
print(BLUE, -amount0, RESET_COLOR)
print(-amount1/amount0, tick_diff)
print()

print(GREEN, "exact token1 out", RESET_COLOR)
amountSpecified = -amountSpecified
print(BLUE, amountSpecified, RESET_COLOR)
zeroForOne = 1
sqrtPriceLimitX96 = 0

amount0, amount1, tick_diff = swap(zeroForOne, amountSpecified, sqrtPriceLimitX96, context)
print(amount0, amount1, tick_diff)
print(BLUE, amount0, RESET_COLOR)
print(-amount1/amount0, tick_diff)
print()

print(GREEN, "exact token0 out", RESET_COLOR)
amountSpecified = -amount0
print(BLUE, amountSpecified, RESET_COLOR)
zeroForOne = 0
sqrtPriceLimitX96 = 0

amount0, amount1, tick_diff = swap(zeroForOne, amountSpecified, sqrtPriceLimitX96, context)
print(amount0, amount1, tick_diff)
print(BLUE, amount1, RESET_COLOR)
print(-amount1/amount0, tick_diff)
print()




#################################  example 2
POOL = "0xc2e9f25be6257c210d7adf0d4cd6e3e881ba25f8"
pool_abi = get_abi(POOL, "pool")
context["pool_contract"] = get_contract(w3, pool_abi, POOL)
context["pool_address"] = POOL

print(GREEN, "exact token0 in", RESET_COLOR)
amountSpecified = 1000000000000000000000000
print(BLUE, amountSpecified, RESET_COLOR)
zeroForOne = 1
sqrtPriceLimitX96 = 0

init_context(context)
amount0, amount1, tick_diff = swap(zeroForOne, amountSpecified, sqrtPriceLimitX96, context)
print(amount0, amount1, tick_diff)
print(BLUE, -amount1, RESET_COLOR)
print(context["slot0"])
print(-amount1/amount0, tick_diff)
print()

print(GREEN, "exact token1 in", RESET_COLOR)
amountSpecified = -amount1*100
print(BLUE, amountSpecified, RESET_COLOR)
zeroForOne = 0
sqrtPriceLimitX96 = 0

amount0, amount1, tick_diff = swap(zeroForOne, amountSpecified, sqrtPriceLimitX96, context)
print(amount0, amount1, tick_diff)
print(BLUE, -amount0, RESET_COLOR)
print(context["slot0"])
print(-amount1/amount0, tick_diff)
print()

print(GREEN, "exact token1 out", RESET_COLOR)
amountSpecified = -479170994483115609895
print(BLUE, amountSpecified, RESET_COLOR)
zeroForOne = 1
sqrtPriceLimitX96 = 0

amount0, amount1, tick_diff = swap(zeroForOne, amountSpecified, sqrtPriceLimitX96, context)
print(amount0, amount1, tick_diff)
print(BLUE, amount0, RESET_COLOR)
print(context["slot0"])
print(-amount1/amount0, tick_diff)
print()

print(GREEN, "exact token0 out", RESET_COLOR)
amountSpecified = -100000000000000000000
print(BLUE, amountSpecified, RESET_COLOR)
zeroForOne = 0
sqrtPriceLimitX96 = 0

amount0, amount1, tick_diff = swap(zeroForOne, amountSpecified, sqrtPriceLimitX96, context)
print(amount0, amount1, tick_diff)
print(BLUE, amount1, RESET_COLOR)
print(context["slot0"])
print(-amount1/amount0, tick_diff)
print()




