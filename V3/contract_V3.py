#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# https://docs.uniswap.org/contracts/v3/reference/core/interfaces/pool/IUniswapV3PoolState
# https://uniswapv3book.com/docs/milestone_3/
# https://github.com/Uniswap/v3-core/blob/main/contracts/UniswapV3Pool.sol
# https://uniswap.org/whitepaper-v3.pdf
# https://atiselsts.github.io/pdfs/uniswap-v3-liquidity-math.pdf

"""
How it works:
1) exact token0 in
        input:
            zeroForOne = 1; sqrtPriceLimitX96 = 0; amountSpecified > 0
        output:
            (token0, token1 < 0)
            
2) exact token1 in
        input:
            zeroForOne = 0; sqrtPriceLimitX96 = get_sqrt_ratio_at_tick(MAX_TICK); amountSpecified > 0
        output:
            (token0 < 0, token1)

3) exact token0 out
        input:
            zeroForOne = 0; sqrtPriceLimitX96 = get_sqrt_ratio_at_tick(MAX_TICK); amountSpecified < 0
        output:
            (token0 < 0, token1)

4) exact token1 out
        input:
            zeroForOne = 1; sqrtPriceLimitX96 = 0; amountSpecified < 0
        output:
            (token0, token1 < 0)

"""

from functools import partial
from libs_V3 import get_tick_at_sqrt_ratio, get_sqrt_ratio_at_tick
from libs_V3 import next_initialized_tick_within_one_word
from libs_V3 import MIN_TICK, MAX_TICK
from swap_math import compute_swap_step

def tick_bitmap(context, pos):
    if not pos in context["tick_bitmap_dict"]:
        if context["block_hash"] is None:
            context["tick_bitmap_dict"][pos] = context["pool_contract"].functions.tickBitmap(pos).call()
        else:
            context["tick_bitmap_dict"][pos] = context["pool_contract"].functions.tickBitmap(pos).call(block_identifier=context["block_hash"])
    return context["tick_bitmap_dict"][pos]

def ticks(context, tick):
    if not tick in context["ticks_dict"]:
        if context["block_hash"] is None:
            one_tick = context["pool_contract"].functions.ticks(tick).call()
        else:
            one_tick = context["pool_contract"].functions.ticks(tick).call(block_identifier=context["block_hash"])
        context["ticks_dict"][tick] = {
            "liquidityGross": one_tick[0],
            "liquidityNet": one_tick[1],
            "feeGrowthOutside0X128": one_tick[2],
            "feeGrowthOutside1X128": one_tick[3],
            "tickCumulativeOutside": one_tick[4],
            "secondsPerLiquidityOutsideX128": one_tick[5],
            "secondsOutside": one_tick[6],
            "initialized": one_tick[7]}
    return context["ticks_dict"][tick]

def tick_cross(tick_info, tick, secondsPerLiquidityCumulativeX128, tickCumulative):
    tick_info["secondsPerLiquidityOutsideX128"] = secondsPerLiquidityCumulativeX128 - tick_info["secondsPerLiquidityOutsideX128"]
    tick_info["tickCumulativeOutside"] = tickCumulative - tick_info["tickCumulativeOutside"]
    return tick_info["liquidityNet"]

def _swap(zeroForOne, amountSpecified, sqrtPriceLimitX96, context):
    stored_slot0 = context["slot0"]
    tick_spacing = context["tick_spacing"]
    fee = context["fee"]
    swap_cache = {
                "liquidityStart": stored_slot0["liquidity"],
                "feeProtocol": (stored_slot0["feeProtocol"] % 16) if zeroForOne else (stored_slot0["feeProtocol"] >> 4),
                "secondsPerLiquidityCumulativeX128": 0,
                "tickCumulative": 0,
                }
    
    exactInput = amountSpecified > 0

    swap_state = {
                "amountSpecifiedRemaining": amountSpecified,
                "amountCalculated": 0,
                "sqrtPriceX96": stored_slot0["sqrtPriceX96"],
                "tick": stored_slot0["tick"],
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
                partial(tick_bitmap, context),
                swap_state["tick"],
                tick_spacing,
                zeroForOne
                )
        
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
                liquidityNet = tick_cross(ticks(context, step_computations["tickNext"]),
                        step_computations["tickNext"],
                        swap_cache["secondsPerLiquidityCumulativeX128"],
                        swap_cache["tickCumulative"])

                if zeroForOne:
                    liquidityNet = -liquidityNet
                swap_state["liquidity"] = swap_state["liquidity"] + liquidityNet
            swap_state["tick"] = step_computations["tickNext"] - 1 if zeroForOne else step_computations["tickNext"]
        elif swap_state["sqrtPriceX96"] != step_computations["sqrtPriceStartX96"]:
            swap_state["tick"] = get_tick_at_sqrt_ratio(swap_state["sqrtPriceX96"])
      
    tick_diff = swap_state["tick"] - stored_slot0["tick"]
    if swap_state["tick"] != stored_slot0["tick"]:
        stored_slot0["sqrtPriceX96"], stored_slot0["tick"] = swap_state["sqrtPriceX96"], swap_state["tick"]
    else:
        stored_slot0["sqrtPriceX96"] = swap_state["sqrtPriceX96"]
    
    if swap_cache["liquidityStart"] != swap_state["liquidity"]:
        stored_slot0["liquidity"] = swap_state["liquidity"]

    if zeroForOne == exactInput:
        amount0, amount1 = amountSpecified - swap_state["amountSpecifiedRemaining"], swap_state["amountCalculated"]
    else:
        amount0, amount1 = swap_state["amountCalculated"], amountSpecified - swap_state["amountSpecifiedRemaining"]

    return amount0, amount1, tick_diff

