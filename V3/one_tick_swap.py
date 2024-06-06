#!/usr/bin/env python3
# -*- coding: utf-8 -*-


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
from V3.libs_V3 import get_tick_at_sqrt_ratio, get_sqrt_ratio_at_tick
from V3.libs_V3 import next_initialized_tick
from V3.libs_V3 import MIN_TICK, MAX_TICK
from V3.swap_math import compute_swap_step

def tick_bitmap(context, pos):
    if pos in context["tick_bitmap_dict"]:
        return context["tick_bitmap_dict"][pos]
    else:
        return None

def one_tick_swap(zeroForOne, amountSpecified, sqrtPriceLimitX96, context):

    tick_spacing = context["tick_spacing"]
    fee = context["fee"]
    sqrtPriceX96 = context["slot0"]["sqrtPriceX96"]

    exactInput = amountSpecified > 0

    (sqrtPriceX96,
     amountIn,
     amountOut,
     feeAmount) = compute_swap_step(
            sqrtPriceX96,
            sqrtPriceLimitX96,
            context["slot0"]["liquidity"],
            amountSpecified,
            fee)

    # print(context["slot0"]["tick"])
    new_tick = get_tick_at_sqrt_ratio(sqrtPriceX96)
    # print(new_tick, (sqrtPriceX96 >> 96)**2)

    tickNext = next_initialized_tick(
            partial(tick_bitmap, context),
            context["slot0"]["tick"],
            new_tick,
            tick_spacing,
            zeroForOne
            )
    # print(tickNext)

    if not tickNext is None:
        if (zeroForOne and tickNext > new_tick) or (not zeroForOne and tickNext < new_tick):
            return 1, None, None
      
    if exactInput:
        amountSpecifiedRemaining = amountSpecified - amountIn - feeAmount
        amountCalculated = -amountOut
    else:
        amountSpecifiedRemaining = amountSpecified + amountOut
        amountCalculated = amountIn + feeAmount

    if zeroForOne == exactInput:
        amount0, amount1 = amountSpecified - amountSpecifiedRemaining, amountCalculated
    else:
        amount0, amount1 = amountCalculated, amountSpecified - amountSpecifiedRemaining

    context["slot0"]["sqrtPriceX96"] = sqrtPriceX96
    return 0, amount0, amount1

