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
           "block_hash": None,
           }


#################################  example 1

POOL = "0xC50f5f0E2421c307B3892a103B45B54f05259668"
context["pool_address"] = POOL
pool_abi = get_abi(POOL, "pool")
context["pool_contract"] = get_contract(w3, pool_abi, POOL)
tx_block = 19430728
block = w3.eth.get_block(tx_block - 1)
prev_block_hash = block["hash"].hex()
context["block_hash"] = prev_block_hash

print(GREEN, "only target exact token0 in", RESET_COLOR)
amount_in = 9
amountSpecified = int(amount_in * 1e18)
print(BLUE, amountSpecified, RESET_COLOR)
zeroForOne = 0
sqrtPriceLimitX96 = 0
init_context(context)
amountt0, amountt1, tick_diff = swap(zeroForOne, amountSpecified, sqrtPriceLimitX96, context)
print(amountt0, amountt1, tick_diff)

# amount_in = 0.000001
# amountSpecified = int(amount_in * 1e18)
# zeroForOne = 0
# sqrtPriceLimitX96 = 0
# amountq0, amountq1, tick_diff = swap(zeroForOne, amountSpecified, sqrtPriceLimitX96, context)
# print(amountq0, amountq1, tick_diff, -amountq0/amountq1)

print(GREEN, "attack exact token0 in", RESET_COLOR)
amount_in = 17
amountSpecified = int(amount_in * 1e18)
print(BLUE, amountSpecified, RESET_COLOR)
zeroForOne = 0
sqrtPriceLimitX96 = 0
init_context(context)
amounta0, amounta1, tick_diff = swap(zeroForOne, amountSpecified, sqrtPriceLimitX96, context)
print(amounta0, amounta1, tick_diff)

print(GREEN, "victim exact token0 in", RESET_COLOR)
amount_in = 9
amountSpecified = int(amount_in * 1e18)
print(BLUE, amountSpecified, RESET_COLOR)
amountv0, amountv1, tick_diff = swap(zeroForOne, amountSpecified, sqrtPriceLimitX96, context)
print(amountv0, amountv1, tick_diff)

print(GREEN, "attack backrun exact token0 in", RESET_COLOR)
zeroForOne = 1
amountSpecified = -amounta0
print(BLUE, amountSpecified, RESET_COLOR)
amountb0, amountb1, tick_diff = swap(zeroForOne, amountSpecified, sqrtPriceLimitX96, context)
print(amountb0, amountb1, tick_diff)

# amount_in = 0.000001
# amountSpecified = int(amount_in * 1e18)
# zeroForOne = 0
# sqrtPriceLimitX96 = 0
# init_context(context)
# amountq0, amountq1, tick_diff = swap(zeroForOne, amountSpecified, sqrtPriceLimitX96, context)
# print(amountq0, amountq1, tick_diff, -amountq0/amountq1)

print("attacker result")
print(-(amountb1 + amounta1)/1e18)


zeroForOne = 1
amountSpecified = -amounta0
print(BLUE, amountSpecified, RESET_COLOR)
amountb0, amountb1, tick_diff = swap(zeroForOne, amountSpecified, sqrtPriceLimitX96, context)
print(amountb0, amountb1, tick_diff)


#################################  compare sequence example



print("compare sequence")
zeroForOne = 0
sqrtPriceLimitX96 = 0
amount_in = 1000
amountSpecified = int(amount_in * 1e18)
init_context(context)
amount10, amount11, tick_diff = swap(zeroForOne, amountSpecified, sqrtPriceLimitX96, context)
amount_in = 1000
amountSpecified = int(amount_in * 1e18)
amount20, amount21, tick_diff = swap(zeroForOne, amountSpecified, sqrtPriceLimitX96, context)

amount_in = 2000
amountSpecified = int(amount_in * 1e18)
init_context(context)
amount30, amount31, tick_diff = swap(zeroForOne, amountSpecified, sqrtPriceLimitX96, context)

print(amount10/1e18, amount11)
print(amount20/1e18, amount21)
print(amount30/1e18, amount31)
print((amount30 - amount20 - amount10)/1e18)

#################################  example 2


for i in range(1, 100):
    amount_in = i
    amountSpecified = int(amount_in * 1e18)
    zeroForOne = 0
    sqrtPriceLimitX96 = 0
    init_context(context)
    amounta0, amounta1, tick_diff1 = swap(zeroForOne, amountSpecified, sqrtPriceLimitX96, context)

    amount_in = 9
    amountSpecified = int(amount_in * 1e18)
    amountv0, amountv1, tick_diff2 = swap(zeroForOne, amountSpecified, sqrtPriceLimitX96, context)
    print(i, -amountv0, tick_diff1, tick_diff2)
    if -amountv0 < 64962205457969021554449:
        break

    zeroForOne = 1
    amountSpecified = -amounta0
    amountb0, amountb1, tick_diff = swap(zeroForOne, amountSpecified, sqrtPriceLimitX96, context)
    print(-(amountb1 + amounta1)/1e18)


#################################  example 2

POOL = "0xCD423F3ab39a11ff1D9208B7D37dF56E902C932B"
tx_block = 19457054
block = w3.eth.get_block(tx_block - 1)
prev_block_hash = block["hash"].hex()

pool_abi = get_abi(POOL, "pool")
context["pool_contract"] = get_contract(w3, pool_abi, POOL)
context["pool_address"] = POOL
context["block_hash"] = prev_block_hash

print(GREEN, "only target exact token0 in", RESET_COLOR)
amount_in = 0.259687432657685029
amountSpecified = int(amount_in * 1e18)
print(BLUE, amountSpecified, RESET_COLOR)
zeroForOne = 0
sqrtPriceLimitX96 = 0
init_context(context)
amountt0, amountt1, tick_diff = swap(zeroForOne, amountSpecified, sqrtPriceLimitX96, context)
print(amountt0, amountt1, tick_diff)
print(context["slot0"])

amount_in = 15
amountSpecified = int(amount_in * 1e18)
print(BLUE, amountSpecified, RESET_COLOR)
amountt0, amountt1, tick_diff = swap(zeroForOne, amountSpecified, sqrtPriceLimitX96, context)
print(amountt0, amountt1, tick_diff)
print(context["slot0"])

