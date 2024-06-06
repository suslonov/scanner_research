#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from web3 import Web3
from eth_abi import abi
from hexbytes import HexBytes
from one_tick_swap import one_tick_swap
from libs_V3 import MIN_TICK, MAX_TICK, get_sqrt_ratio_at_tick
from libs_V3 import s64
from Multicall2_abi import Multicall2_abi
from UniswapV3Pool_abi import pool_abi


Multicall2_address = Web3.to_checksum_address("0x5BA1e12693Dc8F9c48aAD8770482f4739bEeD696")

def get_all_for_V3(w3, pool_address, pool_contract):
    contract = pool_contract
    Multicall2_contract = w3.eth.contract(address=Multicall2_address, abi=Multicall2_abi)
    tick_spacing = contract.functions.tickSpacing().call()
    bitmap_range = (MIN_TICK//tick_spacing) >> 8

    calls = [
             (pool_address, HexBytes(contract.functions.fee()._encode_transaction_data())),
             (pool_address, HexBytes(contract.functions.liquidity()._encode_transaction_data())),
             (pool_address, HexBytes(contract.functions.slot0()._encode_transaction_data())),
             ]

    for i in range(bitmap_range, -bitmap_range):
        calls.append((pool_address, HexBytes(contract.functions.tickBitmap(i)._encode_transaction_data())))

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

def sandwich_pool(pool_address, zeroForOne, amountSpecified, sqrtPriceLimitX96, w3):
# Direct pool request case
    
    _pool_address = Web3.to_checksum_address(pool_address)
    pool_contract = w3.eth.contract(address=_pool_address, abi=pool_abi)
   
    V3_pool_data = get_all_for_V3(w3, _pool_address, pool_contract)
    
    slot0 = V3_pool_data["slot0"]
    context = {"ticks_dict": None,
           "tick_bitmap_dict": V3_pool_data["tickBitmap"],
           "slot0": {"feeProtocol": slot0[-2],
                        "sqrtPriceX96": slot0[0],
                        "tick": slot0[1],
                        "liquidity": V3_pool_data["liquidity"],
                        "tick_spacing": V3_pool_data["tick_spacing"],
                        "fee": V3_pool_data["fee"],
                    },
           "w3": w3,
           "pool_address": pool_address,
           "pool_contract": pool_contract,
           }

    if sqrtPriceLimitX96 == 0:
        if zeroForOne == 0:
            _sqrtPriceLimitX96 = get_sqrt_ratio_at_tick(MAX_TICK)
        else:
            _sqrtPriceLimitX96 = get_sqrt_ratio_at_tick(MIN_TICK)
    else:
        _sqrtPriceLimitX96 = sqrtPriceLimitX96

    return one_tick_swap(zeroForOne, amountSpecified, _sqrtPriceLimitX96, context), context
