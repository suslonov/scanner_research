#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Attack calculation funtions:

1) sandwich_pool(pool_address, zeroForOne, amountSpecified, sqrtPriceLimitX96, w3) for direct pool calls
2)  sandwich_exact_input_single(token_in, token_out, fee, amount_in, amount_out_minimum, sqrtPriceLimitX96, w3)
    sandwich_exact_output_single(token_in, token_out, fee, amount_out, amount_in_maximum, sqrtPriceLimitX96, w3)    
    sandwich_exact_input() TBD
    sandwich_exact_output() TBD
    - for V3 router calls
    https://docs.uniswap.org/contracts/v3/reference/periphery/interfaces/ISwapRouter
3)  sandwich_V3_SWAP_EXACT_IN(amount_in, amount_out_minimum, path, w3)
    sandwich_V3_SWAP_EXACT_OUT(amount_out, amount_in_maximum, path, w3)
    - for universal router calls, currently for one-step paths only

all functions require initialized web3 object


output: (status, optimal_attack_amount, expected_profit)

where status:
    0 - success
    -1 - calculation error
    1 - not implemented
        for Stage1: if the trade crosses ticks


TODO: what to do if the output limit is zero?
TODO: calculation for chained swap on routers. How to calculate the limit?
TODO: stage 2 = crossing ticks

"""

import os
os.sys.path.append(os.path.dirname(os.path.abspath('.')))
from web3 import Web3
from eth_abi import abi
from hexbytes import HexBytes

from V3.one_tick_swap import one_tick_swap
from V3.libs_V3 import MIN_TICK, MAX_TICK, MIN_SQRT_RATIO, MAX_SQRT_RATIO
from V3.libs_V3 import s64
from V3.Multicall2_abi import Multicall2_abi
from V3.UniswapV3Pool_abi import pool_abi
from V3.V3_factory_abi import V3_factory_abi
from V3.QUOTER_V2_abi import quoter_v2_abi

from V3.attack_functions import token0_optimum, token1_optimum, token0_profit, token1_profit
from V3.attack_functions import token0_victim_output, token1_victim_output, token0_victim_input, token1_victim_input

MULTICALL2_ADDRESS = "0x5BA1e12693Dc8F9c48aAD8770482f4739bEeD696"
V3_FACTORY_ADDRESS = "0x1F98431c8aD98523631AE4a59f267346ea31F984"
QUOTER_V2 = "0x0209c4Dc18B2A1439fD2427E34E7cF3c6B91cFB9"

def get_all_for_V3(w3, pool_address, pool_contract, block_hash=None):
    contract = pool_contract
    Multicall2_contract = w3.eth.contract(address=MULTICALL2_ADDRESS, abi=Multicall2_abi)
    if block_hash:
        tick_spacing = contract.functions.tickSpacing().call(block_identifier=block_hash)
    else:
        tick_spacing = contract.functions.tickSpacing().call()
    if tick_spacing == 1:
        return None
    bitmap_range = (MIN_TICK//tick_spacing) >> 8

    calls = [
             (pool_address, HexBytes(contract.functions.fee()._encode_transaction_data())),
             (pool_address, HexBytes(contract.functions.liquidity()._encode_transaction_data())),
             (pool_address, HexBytes(contract.functions.slot0()._encode_transaction_data())),
             ]

    for i in range(bitmap_range, -bitmap_range):
        calls.append((pool_address, HexBytes(contract.functions.tickBitmap(i)._encode_transaction_data())))

    if block_hash:
        Multicall2_results = Multicall2_contract.functions.aggregate(calls).call(block_identifier=block_hash)
    else:
        Multicall2_results = Multicall2_contract.functions.aggregate(calls).call()
    slot0 = abi.decode(["uint256", "uint256", "uint256", "uint256", "uint256", "uint256", "bool"], Multicall2_results[1][2])
    results = {
        "fee": abi.decode(["uint256"], Multicall2_results[1][0])[0],
        "liquidity": abi.decode(["uint256"], Multicall2_results[1][1])[0],
        "slot0": (slot0[0], s64(slot0[1]), slot0[2], slot0[3], slot0[4], slot0[5], slot0[6]),
        }
    
    tick_bitmap = {}
    for i, q in enumerate(Multicall2_results[1][len(results):]):
        tick_bitmap[i + bitmap_range] =  abi.decode(["uint256"], q)[0]
    results["tickBitmap"] = tick_bitmap
    results["tick_spacing"] = tick_spacing

    return results

def extract_path_from_V3(str_path):
    path = []
    i = 0
    while i < len(str_path):
        path.append("0x" + str_path[i:i+20].hex().lower())
        path.append(str_path[i+20:i+23])
        i = i + 23
    return path



def _sandwich_pool(V3_pool_data, context, zeroForOne, amountSpecified, sqrtPriceLimitX96, w3, amount_out=None):
    
    if sqrtPriceLimitX96 == 0:
        if zeroForOne == 0:
            _sqrtPriceLimitX96 = MAX_SQRT_RATIO
        else:
            _sqrtPriceLimitX96 = MIN_SQRT_RATIO 
    else:
        _sqrtPriceLimitX96 = sqrtPriceLimitX96

    status, amount0, amount1 = one_tick_swap(zeroForOne, amountSpecified, _sqrtPriceLimitX96, context)
    if amount0 is None:
        return -1 if status == 0 else status, None, None, None
    
    fee = V3_pool_data["fee"] / 1e6
    P_sqrt = context["slot0"]["sqrtPriceX96"]
    L = V3_pool_data["liquidity"]

    if zeroForOne:
        Y_v = (sqrtPriceLimitX96 >> 96) ** 2 * abs(amountSpecified) # works for both exaxt input and exec output
        if amount_out:
            Y_v = max(Y_v, amount_out)
        optimal_attack_amount = int(token0_optimum(abs(amountSpecified), Y_v, fee, L, P_sqrt))
        if amountSpecified < 0:
            expected_profit = token0_profit(-amountSpecified, Y_v, optimal_attack_amount, fee, L, P_sqrt)
            victim_output_input = token0_victim_input(-amountSpecified, optimal_attack_amount, fee, L, P_sqrt)
        else:
            expected_profit = token0_profit(amountSpecified, Y_v, optimal_attack_amount, fee, L, P_sqrt)
            victim_output_input = token0_victim_output(amountSpecified, optimal_attack_amount, fee, L, P_sqrt)
    else:
        if sqrtPriceLimitX96 == 0:
            Y_v = 0
        else:
            Y_v = abs(amountSpecified) / ((sqrtPriceLimitX96 >> 96) ** 2)
        if amount_out:
            Y_v = max(Y_v, amount_out)   # !!!!! not correct
        optimal_attack_amount = int(token1_optimum(abs(amountSpecified), Y_v, fee, L, P_sqrt))
        if amountSpecified < 0:
            expected_profit = token1_profit(-amountSpecified, Y_v, optimal_attack_amount, fee, L, P_sqrt)
            victim_output_input = token1_victim_input(-amountSpecified, optimal_attack_amount, fee, L, P_sqrt)
        else:
            expected_profit = token1_profit(amountSpecified, Y_v, optimal_attack_amount, fee, L, P_sqrt)
            victim_output_input = token1_victim_output(amountSpecified, optimal_attack_amount, fee, L, P_sqrt)

    if zeroForOne == 0:
        _sqrtPriceLimitX96 = MAX_SQRT_RATIO
    else:
        _sqrtPriceLimitX96 = MIN_SQRT_RATIO 
    status, amount10, amount11 = one_tick_swap(zeroForOne, optimal_attack_amount, _sqrtPriceLimitX96, context)
    if amount10 is None: #!!! test it
        return -1 if status == 0 else status, optimal_attack_amount, expected_profit, victim_output_input

    return 0, optimal_attack_amount, expected_profit, victim_output_input


def sandwich_pool(pool_address, zeroForOne, amountSpecified, sqrtPriceLimitX96, w3, amount_out=None, block_hash=None):
    if not amount_out and not sqrtPriceLimitX96 and zeroForOne:
        return -1, None, None, None
    
    _pool_address = Web3.to_checksum_address(pool_address)
    pool_contract = w3.eth.contract(address=_pool_address, abi=pool_abi)
   
    V3_pool_data = get_all_for_V3(w3, _pool_address, pool_contract, block_hash)
    if V3_pool_data is None:
        return -1, None, None, None
   
    slot0 = V3_pool_data["slot0"]
    context = {"ticks_dict": None,
           "tick_bitmap_dict": V3_pool_data["tickBitmap"],
           "tick_spacing": V3_pool_data["tick_spacing"],
           "fee": V3_pool_data["fee"],
           "slot0": {"feeProtocol": slot0[-2],
                        "sqrtPriceX96": slot0[0],
                        "tick": slot0[1],
                        "liquidity": V3_pool_data["liquidity"],
                    },
           "w3": w3,
           "block_hash": block_hash,
           "pool_address": pool_address,
           "pool_contract": pool_contract,
           }

    return _sandwich_pool(V3_pool_data, context, zeroForOne, amountSpecified, sqrtPriceLimitX96, w3, amount_out)

def sandwich_exact_input_single(token_in, token_out, fee, amount_in, amount_out_minimum, sqrtPriceLimitX96, w3, block_hash=None):
    V3_factory_contract = w3.eth.contract(address=V3_FACTORY_ADDRESS, abi=V3_factory_abi)
    pool_address = V3_factory_contract.functions.getPool(token_in, token_out, fee).call()
    
    zeroForOne = token_in < token_out
    status, optimal_attack_amount, expected_profit, victim_output_input = sandwich_pool(pool_address, zeroForOne, amount_in, sqrtPriceLimitX96, w3, amount_out=amount_out_minimum, block_hash=block_hash)
    if status != 0:
        return status, optimal_attack_amount, expected_profit, victim_output_input
    else:
        if victim_output_input < amount_out_minimum:
            return -1, optimal_attack_amount, expected_profit, victim_output_input
        else:
            return status, optimal_attack_amount, expected_profit, victim_output_input

def sandwich_exact_output_single(token_in, token_out, fee, amount_out, amount_in_maximum, sqrtPriceLimitX96, w3, block_hash=None):
    V3_factory_contract = w3.eth.contract(address=V3_FACTORY_ADDRESS, abi=V3_factory_abi)
    pool_address = V3_factory_contract.functions.getPool(token_in, token_out, fee).call()
    
    zeroForOne = token_in < token_out
#!!!!!!! not correct
    status, optimal_attack_amount, expected_profit, victim_output_input = sandwich_pool(pool_address, zeroForOne, -amount_out, sqrtPriceLimitX96, w3, amount_out=amount_in_maximum, block_hash=block_hash)
    if status != 0:
        return status, optimal_attack_amount, expected_profit, victim_output_input
    else:
        if victim_output_input > amount_in_maximum:
            return -1, optimal_attack_amount, expected_profit, victim_output_input
        else:
            return status, optimal_attack_amount, expected_profit, victim_output_input

HIGH_LIQUID = ["0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599".lower(),
               "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2".lower(),
               "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48".lower(),
               "0x6B175474E89094C44Da98b954EedeAC495271d0F".lower(),
               "0xdAC17F958D2ee523a2206206994597C13D831ec7".lower()]
HIGH_LIQUID.sort()
LIQUID_PAIRS = [(t0, t1) for i, t0 in enumerate(HIGH_LIQUID) for t1 in HIGH_LIQUID[i+1:]]

def pair_to_attack(path_list):
    i_left = None
    for i in range(len(path_list) // 2 - 1):
        if not (path_list[i*2], path_list[i*2+2]) in LIQUID_PAIRS and not (path_list[i*2+2], path_list[i*2]) in LIQUID_PAIRS:
            i_left = i
            break

    i_right = None
    for i in range(len(path_list) // 2 - 1):
        if not (path_list[len(path_list) - i*2 - 2], path_list[len(path_list) - i*2 - 4]) in LIQUID_PAIRS and not (path_list[len(path_list) - i*2 - 4], path_list[len(path_list) - i*2 - 2]) in LIQUID_PAIRS:
            i_right = i
            break

    if i_left is None or i_right is None or i_left + i_right + 2 != len(path_list)//2:
        return (None, None, None), None, None
    return (path_list[i_left*2], path_list[i_left*2+1], path_list[i_left*2+2]), path_list[:i_left*2+2], path_list[len(path_list) - i_right*2 - 2:]

def path_to_bytes(path_list):
    path = b''
    for i, p in enumerate(path_list):
        if not i%2:
            path = path + bytearray.fromhex(p[2:])
        else:
            path = path + bytearray([p>>16, (p>>8)%256, p%256])
    return path

def sandwich_V3_SWAP_EXACT_IN(amount_in, amount_out_minimum, path_list, w3, block_hash=None):
    
    (token_in, fee, token_out), pre_path, post_path = pair_to_attack(path_list)
    if token_in is None:
         return 1, None, None, None, None, None, None
    V3_factory_contract = w3.eth.contract(address=V3_FACTORY_ADDRESS, abi=V3_factory_abi)
    pool_address = V3_factory_contract.functions.getPool(Web3.to_checksum_address(token_in), Web3.to_checksum_address(token_out), fee).call()
    
    try: #!!! a crunch
        QUOTER_V2_contract = None
        if len(pre_path) > 2:
            QUOTER_V2_contract = w3.eth.contract(address=QUOTER_V2, abi=quoter_v2_abi)
            quote = QUOTER_V2_contract.functions.quoteExactInput(path_to_bytes(pre_path), amount_in).call()
            amount_in = quote[0]
        if len(post_path) > 2:
            if QUOTER_V2_contract is None:
                QUOTER_V2_contract = w3.eth.contract(address=QUOTER_V2, abi=quoter_v2_abi)
            quote = QUOTER_V2_contract.functions.quoteExactOutput(path_to_bytes(post_path), amount_out_minimum).call()
            amount_out_minimum = quote[0]
    except:
         return 2, None, None, None, None, None, None
    
    zeroForOne = token_in < token_out
    status, optimal_attack_amount, expected_profit, victim_output_input = sandwich_pool(pool_address, zeroForOne, amount_in, 0, w3,  amount_out=amount_out_minimum, block_hash=block_hash) #!!! sqrtPriceLimitX96 set to 0
    if status != 0:
        return status, optimal_attack_amount, expected_profit, victim_output_input, token_in, fee, token_out
    else:
        if victim_output_input < amount_out_minimum:
            return -1, optimal_attack_amount, expected_profit, victim_output_input, token_in, fee, token_out
        else:
            return status, optimal_attack_amount, expected_profit, victim_output_input, token_in, fee, token_out

#!!! not ready
def sandwich_V3_SWAP_EXACT_OUT(amount_out, amount_in_maximum, path_list, w3, block_hash=None):

    (token_in, token_out, fee) = pair_to_attack(path_list)
    V3_factory_contract = w3.eth.contract(address=V3_FACTORY_ADDRESS, abi=V3_factory_abi)
    pool_address = V3_factory_contract.functions.getPool(token_in, token_out, fee).call()
    
    zeroForOne = token_in < token_out
    status, optimal_attack_amount, expected_profit, victim_output_input = sandwich_pool(pool_address, zeroForOne, -amount_out, 0, w3, block_hash=block_hash) #!!! sqrtPriceLimitX96 set to 0
    if status != 0:
        return status, optimal_attack_amount, expected_profit, victim_output_input
    else:
        if victim_output_input > amount_in_maximum:
            return -1, optimal_attack_amount, expected_profit, victim_output_input
        else:
            return status, optimal_attack_amount, expected_profit, victim_output_input
