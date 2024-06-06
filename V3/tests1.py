#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from web3 import Web3

from one_tick_swap_caller import sandwich_pool
from swap_math import MAX_UINT_160

RED = "\033[1;31m"
GREEN = "\033[0;32m"
BLUE = "\033[0;34m"
RESET_COLOR = "\033[0;0m"

KEY_FILE = '../keys/alchemy.sec'

with open(KEY_FILE, 'r') as f:
    k1 = f.readline()
    ALCHEMY_URL = k1.strip('\n')
    k2 = f.readline()
    ALCHEMY_WSS = k2.strip('\n')


w3 = Web3(Web3.HTTPProvider(ALCHEMY_URL))


#################################  test 1
pool_address = "0x0a1665e3f54eeb364bec6954e4497dd802840a01" 

print(GREEN, "exact token0 in", RESET_COLOR)
amount_in = 0.000000001
amountSpecified = int(amount_in * 1e18)
print(BLUE, amountSpecified, RESET_COLOR)
zeroForOne = 1
sqrtPriceLimitX96 = 234914710100657381076610454999097 * 6400 // 10000

(amount0, amount1), context = sandwich_pool(pool_address, zeroForOne, amountSpecified, sqrtPriceLimitX96, w3)
print(amount0, amount1)
print(BLUE, -amount1, RESET_COLOR)
print(-amount1/amount0)
print()

print(GREEN, "exact token1 in", RESET_COLOR)
amountSpecified = -amount1
print(BLUE, amountSpecified, RESET_COLOR)
zeroForOne = 0
sqrtPriceLimitX96 = MAX_UINT_160

amount0, amount1 = sandwich_pool(pool_address, zeroForOne, amountSpecified, sqrtPriceLimitX96, w3)
print(amount0, amount1)
print(BLUE, amount0, RESET_COLOR)
print(-amount1/amount0)
print()

print(GREEN, "exact token1 out", RESET_COLOR)
amountSpecified = -amountSpecified
print(BLUE, amountSpecified, RESET_COLOR)
zeroForOne = 1
sqrtPriceLimitX96 = 0

amount0, amount1 = sandwich_pool(pool_address, zeroForOne, amountSpecified, sqrtPriceLimitX96, w3)
print(amount0, amount1)
print(BLUE, amount0, RESET_COLOR)
print(-amount1/amount0)
print()

print(GREEN, "exact token0 out", RESET_COLOR)
amountSpecified = -amount0
print(BLUE, amountSpecified, RESET_COLOR)
zeroForOne = 0
sqrtPriceLimitX96 = MAX_UINT_160

amount0, amount1 = sandwich_pool(pool_address, zeroForOne, amountSpecified, sqrtPriceLimitX96, w3)
print(amount0, amount1)
print(BLUE, amount1, RESET_COLOR)
print(-amount1/amount0)
print()





#################################  example 2
pool_address = "0xc2e9f25be6257c210d7adf0d4cd6e3e881ba25f8"

print(GREEN, "exact token0 in", RESET_COLOR)
amountSpecified = 1000000000000000000000
print(BLUE, amountSpecified, RESET_COLOR)
zeroForOne = 1
sqrtPriceLimitX96 = 0

amount0, amount1 = sandwich_pool(pool_address, zeroForOne, amountSpecified, sqrtPriceLimitX96, w3)
print(amount0, amount1)
if amount0:
    print(BLUE, -amount1, RESET_COLOR)
    print(-amount1/amount0)
print()

print(GREEN, "exact token1 in", RESET_COLOR)
amountSpecified = 10000000000000
print(BLUE, amountSpecified, RESET_COLOR)
zeroForOne = 0
sqrtPriceLimitX96 = 0

amount0, amount1 = sandwich_pool(pool_address, zeroForOne, amountSpecified, sqrtPriceLimitX96, w3)
print(amount0, amount1)
if amount0:
    print(BLUE, -amount0, RESET_COLOR)
    print(-amount1/amount0)
print()

print(GREEN, "exact token1 out", RESET_COLOR)
amountSpecified = -100000000000000000
print(BLUE, amountSpecified, RESET_COLOR)
zeroForOne = 1
sqrtPriceLimitX96 = 0

amount0, amount1 = sandwich_pool(pool_address, zeroForOne, amountSpecified, sqrtPriceLimitX96, w3)
print(amount0, amount1)
if amount0:
    print(BLUE, amount0, RESET_COLOR)
    print(-amount1/amount0)
print()

print(GREEN, "exact token0 out", RESET_COLOR)
amountSpecified = -500000000000000000000
print(BLUE, amountSpecified, RESET_COLOR)
zeroForOne = 0
sqrtPriceLimitX96 = 0

amount0, amount1 = sandwich_pool(pool_address, zeroForOne, amountSpecified, sqrtPriceLimitX96, w3)
print(amount0, amount1)
if amount0:
    print(BLUE, amount1, RESET_COLOR)
    print(-amount1/amount0)
print()



#################################  example 3
pool_address = "0x83695f776f5c8de31690e8a54e242a4567ba7a4b"

print(GREEN, "exact token0 in", RESET_COLOR)
amountSpecified = 800000000000000000
print(BLUE, amountSpecified, RESET_COLOR)
zeroForOne = 1
sqrtPriceLimitX96 = 0

amount0, amount1 = sandwich_pool(pool_address, zeroForOne, amountSpecified, sqrtPriceLimitX96, w3)
print(amount0, amount1)
if amount0:
    print(BLUE, -amount1, RESET_COLOR)
    print(-amount1/amount0)
print()

print(GREEN, "exact token1 in", RESET_COLOR)
amountSpecified = 12000000000000000000000
print(BLUE, amountSpecified, RESET_COLOR)
zeroForOne = 0
sqrtPriceLimitX96 = 0

amount0, amount1 = sandwich_pool(pool_address, zeroForOne, amountSpecified, sqrtPriceLimitX96, w3)
print(amount0, amount1)
if amount0:
    print(BLUE, -amount0, RESET_COLOR)
    print(-amount1/amount0)
print()

print(GREEN, "exact token1 out", RESET_COLOR)
amountSpecified = -27000000000000000000000
print(BLUE, amountSpecified, RESET_COLOR)
zeroForOne = 1
sqrtPriceLimitX96 = 0

amount0, amount1 = sandwich_pool(pool_address, zeroForOne, amountSpecified, sqrtPriceLimitX96, w3)
print(amount0, amount1)
if amount0:
    print(BLUE, amount0, RESET_COLOR)
    print(-amount1/amount0)
print()

print(GREEN, "exact token0 out", RESET_COLOR)
amountSpecified = -100000000000000000
print(BLUE, amountSpecified, RESET_COLOR)
zeroForOne = 0
sqrtPriceLimitX96 = 0

amount0, amount1 = sandwich_pool(pool_address, zeroForOne, amountSpecified, sqrtPriceLimitX96, w3)
print(amount0, amount1)
if amount0:
    print(BLUE, amount1, RESET_COLOR)
    print(-amount1/amount0)
print()

