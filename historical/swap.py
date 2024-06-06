#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# https://docs.uniswap.org/contracts/v3/reference/core/interfaces/pool/IUniswapV3PoolState
# https://uniswapv3book.com/docs/milestone_3/
# https://github.com/Uniswap/v3-core/blob/main/contracts/UniswapV3Pool.sol

from functools import partial
import requests
import pandas as pd
import numpy as np
from math import sqrt, isqrt
from web3 import Web3
from libs import get_tick_at_sqrt_ratio, get_sqrt_ratio_at_tick
from libs import next_initialized_tick_within_one_word
from libs import MIN_TICK, MAX_TICK
from swap_math import compute_swap_step, Q128

def list_to_dict(l, field):
    return {v[field]: v for v in l if field in v}

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

def get_abi(address):
    try:
        res = requests.get(ETHERSCAN_GETABI.format(address, ETHERSCAN_KEY), headers=HEADERS_E)
        d = res.json()
        abi = d["result"]
        return abi
    except:
        return None

def get_contract(w3, abi, address):
    return w3.eth.contract(address=Web3.to_checksum_address(address), abi=abi)

TICKS_DICT = {}
TICK_BITMAP_DICT = {}
SLOT0 = {}

def tick_bitmap(pool_contract, pos):
    if not pos in TICK_BITMAP_DICT:
        TICK_BITMAP_DICT[pos] = pool_contract.functions.tickBitmap(pos).call()
    return TICK_BITMAP_DICT[pos]

def ticks(pool_contract, tick):
    if not tick in TICKS_DICT:
        one_tick_list = pool_contract.functions.ticks(tick).call()
        TICKS_DICT[tick] = {
            "liquidityGross": one_tick_list[0],
            "liquidityNet": one_tick_list[1],
            "feeGrowthOutside0X128": one_tick_list[2],
            "feeGrowthOutside1X128": one_tick_list[3],
            "tickCumulativeOutside": one_tick_list[4],
            "secondsPerLiquidityOutsideX128": one_tick_list[5],
            "secondsOutside": one_tick_list[6],
            "initialized": one_tick_list[7]}
    return TICKS_DICT[tick]

def ticks_cross(tick_info, tick, secondsPerLiquidityCumulativeX128, tickCumulative):
    tick_info["secondsPerLiquidityOutsideX128"] = secondsPerLiquidityCumulativeX128 - tick_info["secondsPerLiquidityOutsideX128"]
    tick_info["tickCumulativeOutside"] = tickCumulative - tick_info["tickCumulativeOutside"]
    return tick_info["liquidityNet"]

def _swap(zeroForOne, amountSpecified, sqrtPriceLimitX96, pool_contract):
    if not SLOT0:
        slot0 = pool_contract.functions.slot0().call()
        SLOT0["feeProtocol"] = slot0[-2]
        SLOT0["sqrtPriceX96"] = slot0[0]
        SLOT0["tick"] = slot0[1]
        SLOT0["liquidity"] = pool_contract.functions.liquidity().call()

    tick_spacing = pool_contract.functions.tickSpacing().call()
    fee = pool_contract.functions.fee().call()
    
    swap_cache = {
                "liquidityStart": SLOT0["liquidity"],
                "feeProtocol": (SLOT0["feeProtocol"] % 16) if zeroForOne else (SLOT0["feeProtocol"] >> 4),
                "secondsPerLiquidityCumulativeX128": 0,
                "tickCumulative": 0,
                }
    
    exactInput = amountSpecified > 0

    swap_state = {
                "amountSpecifiedRemaining": amountSpecified,
                "amountCalculated": 0,
                "sqrtPriceX96": SLOT0["sqrtPriceX96"],
                "tick": SLOT0["tick"],
                "protocolFee": 0,
                "liquidity": swap_cache["liquidityStart"],
                "feeGrowthGlobalX128": 0
                }


    while swap_state["amountSpecifiedRemaining"] != 0 and swap_state["sqrtPriceX96"] != sqrtPriceLimitX96:
        step_computations = {
            "sqrtPriceStartX96": 0,
            "tickNext": 0,
            "initialized": False,
            "sqrtPriceNextX96": 0,
            "amountIn": 0,
            "amountOut": 0,
            "feeAmount": 0,
            }
            
        step_computations["sqrtPriceStartX96"] = swap_state["sqrtPriceX96"]

        step_computations["tickNext"], step_computations["initialized"] = next_initialized_tick_within_one_word(
                partial(tick_bitmap, pool_contract), swap_state["tick"], tick_spacing, zeroForOne)

        if step_computations["tickNext"] < MIN_TICK:
            step_computations["tickNext"] = MIN_TICK
        elif step_computations["tickNext"] > MAX_TICK:
            step_computations["tickNext"] = MAX_TICK

        step_computations["sqrtPriceNextX96"] = get_sqrt_ratio_at_tick(step_computations["tickNext"])

        (swap_state["sqrtPriceX96"],
         step_computations["amountIn"],
         step_computations["amountOut"],
         step_computations["feeAmount"]) = compute_swap_step(
                swap_state["sqrtPriceX96"],
                sqrtPriceLimitX96 if (step_computations["sqrtPriceNextX96"] < sqrtPriceLimitX96
                                      if zeroForOne 
                                      else step_computations["sqrtPriceNextX96"] > sqrtPriceLimitX96) else step_computations["sqrtPriceNextX96"],
                swap_state["liquidity"],
                swap_state["amountSpecifiedRemaining"],
                fee)

        if exactInput:
            swap_state["amountSpecifiedRemaining"] -= step_computations["amountIn"] + step_computations["feeAmount"]
            swap_state["amountCalculated"] = swap_state["amountCalculated"] - step_computations["amountOut"]
        else:
            swap_state["amountSpecifiedRemaining"] += step_computations["amountOut"]
            swap_state["amountCalculated"] = swap_state["amountCalculated"] + step_computations["amountIn"] + step_computations["feeAmount"]

        if swap_cache["feeProtocol"] > 0:
            delta = step_computations["feeAmount"] // swap_cache["feeProtocol"]
            step_computations["feeAmount"] -= delta
            swap_state["protocolFee"] += delta

        if swap_state["sqrtPriceX96"] == step_computations["sqrtPriceNextX96"]:
            if step_computations["initialized"]:
                liquidityNet = ticks_cross(ticks(pool_contract, step_computations["tickNext"]),
                        step_computations["tickNext"],
                        swap_cache["secondsPerLiquidityCumulativeX128"],
                        swap_cache["tickCumulative"])

                if zeroForOne:
                    liquidityNet = -liquidityNet

                swap_state["liquidity"] = swap_state["liquidity"] + liquidityNet
            swap_state["tick"] = step_computations["tickNext"] - 1 if zeroForOne else step_computations["tickNext"]
        elif swap_state["sqrtPriceX96"] != step_computations["sqrtPriceStartX96"]:
            swap_state["tick"] = get_tick_at_sqrt_ratio(swap_state["sqrtPriceX96"])
      
    if swap_state["tick"] != SLOT0["tick"]:
        SLOT0["sqrtPriceX96"], SLOT0["tick"] = swap_state["sqrtPriceX96"], swap_state["tick"]
    else:
        SLOT0["sqrtPriceX96"] = swap_state["sqrtPriceX96"]
    
    if swap_cache["liquidityStart"] != swap_state["liquidity"]:
        SLOT0["liquidity"] = swap_state["liquidity"]

    if zeroForOne == exactInput:
        amount0, amount1 = amountSpecified - swap_state["amountSpecifiedRemaining"], swap_state["amountCalculated"]
    else:
        amount0, amount1 = swap_state["amountCalculated"], amountSpecified - swap_state["amountSpecifiedRemaining"]

    return amount0, amount1

def swap(zeroForOne, amountSpecified, sqrtPriceLimitX96, pool_contract):
    TICKS_DICT = {}
    TICK_BITMAP_DICT = {}
    SLOT0 = {}
    return _swap(zeroForOne, amountSpecified, sqrtPriceLimitX96, pool_contract)

POOL = "0x0a1665e3f54eeb364bec6954e4497dd802840a01"
w3 = Web3(Web3.HTTPProvider(ALCHEMY_URL))
pool_abi = get_abi(POOL)
pool_contract = get_contract(w3, pool_abi, POOL)

zeroForOne = 1
amountSpecified = int(0.001 * 1e18)
sqrtPriceLimitX96 = 0

amount0, amount1 = swap(zeroForOne, amountSpecified, sqrtPriceLimitX96, pool_contract)

-amount1/amount0
