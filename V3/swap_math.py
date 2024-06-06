#!/usr/bin/env python3
# -*- coding: utf-8 -*-

MAX_UINT_160 = 1461501637330902918203684832716283019655932542975
Q96 = 0x1000000000000000000000000
Q128 = 0x100000000000000000000000000000000

def get_next_sqrt_price_from_amount0_rounding_up(sqrtPX96, liquidity, amount, add):
    if amount == 0:
        return sqrtPX96
    numerator1 = liquidity << 96

    if add:
        product = amount * sqrtPX96
        if product // amount == sqrtPX96:
            denominator = numerator1 + product
            if denominator >= numerator1:
                return int((numerator1 * sqrtPX96 + denominator - 1) // denominator)
        denominator = (numerator1 / sqrtPX96 + amount)
        return int((numerator1 + denominator - 1) // denominator)
    else:
        product = amount * sqrtPX96
        denominator = numerator1 - product
        return int((numerator1 * sqrtPX96 + denominator - 1) // denominator)

def get_next_sqrt_price_from_amount1_rounding_down(sqrtPX96, liquidity, amount, add):
    if add:
        quotient = ((amount << 96) // liquidity) if amount <= MAX_UINT_160 else (amount * Q96 // liquidity)
        return sqrtPX96 + quotient
    else:
        quotient = (((amount << 96) + liquidity - 1) // liquidity) if amount <= MAX_UINT_160 else ((amount * Q96 + liquidity - 1) // liquidity)
        return sqrtPX96 - quotient

def get_next_sqrt_price_from_input(sqrtPX96, liquidity, amountIn, zeroForOne):
        if zeroForOne:
            return get_next_sqrt_price_from_amount0_rounding_up(sqrtPX96, liquidity, amountIn, True)
        else:
            return get_next_sqrt_price_from_amount1_rounding_down(sqrtPX96, liquidity, amountIn, True)


def get_next_sqrt_price_from_output(sqrtPX96, liquidity, amountOut, zeroForOne):
    if zeroForOne:
        return get_next_sqrt_price_from_amount1_rounding_down(sqrtPX96, liquidity, amountOut, False)
    else:
        return get_next_sqrt_price_from_amount0_rounding_up(sqrtPX96, liquidity, amountOut, False)

def get_amount0_delta(sqrtRatioAX96, sqrtRatioBX96, liquidity, roundUp):
    if sqrtRatioAX96 > sqrtRatioBX96:
        sqrtRatioAX96, sqrtRatioBX96 = sqrtRatioBX96, sqrtRatioAX96

    numerator1 = liquidity << 96
    numerator2 = sqrtRatioBX96 - sqrtRatioAX96

    if roundUp:
        return ((numerator1 * numerator2 + sqrtRatioBX96 - 1) // sqrtRatioBX96 + sqrtRatioAX96 - 1) // sqrtRatioAX96
    else:
        return numerator1 * numerator2 // sqrtRatioBX96 // sqrtRatioAX96

def get_amount1_delta(sqrtRatioAX96, sqrtRatioBX96, liquidity, roundUp):
    if sqrtRatioAX96 > sqrtRatioBX96:
        sqrtRatioAX96, sqrtRatioBX96 = sqrtRatioBX96, sqrtRatioAX96

    if roundUp:
        return (liquidity * (sqrtRatioBX96 - sqrtRatioAX96) + Q96 - 1) // Q96
    else:
        return liquidity * (sqrtRatioBX96 - sqrtRatioAX96) // Q96

def get_amount0_delta_(sqrtRatioAX96, sqrtRatioBX96, liquidity):
    if liquidity < 0:
        return -get_amount0_delta(sqrtRatioAX96, sqrtRatioBX96, -liquidity, False)
    else:
        return get_amount0_delta(sqrtRatioAX96, sqrtRatioBX96, liquidity, True)

def get_amount1_delta_(sqrtRatioAX96, sqrtRatioBX96, liquidity):
    if liquidity < 0:
        return -get_amount1_delta(sqrtRatioAX96, sqrtRatioBX96, -liquidity, False)
    else:
        return get_amount1_delta(sqrtRatioAX96, sqrtRatioBX96, liquidity, True)


def compute_swap_step(sqrtRatioCurrentX96, sqrtRatioTargetX96, liquidity, amountRemaining, feePips):
    zeroForOne = sqrtRatioCurrentX96 >= sqrtRatioTargetX96
    exactIn = amountRemaining >= 0

    if exactIn:
        amountRemainingLessFee = int(amountRemaining * (1e6 - feePips) // 1e6)
        if zeroForOne:
            amountIn = get_amount0_delta(sqrtRatioTargetX96, sqrtRatioCurrentX96, liquidity, True)
        else:
            amountIn = get_amount1_delta(sqrtRatioCurrentX96, sqrtRatioTargetX96, liquidity, True)
        if amountRemainingLessFee >= amountIn:
            sqrtRatioNextX96 = sqrtRatioTargetX96
        else:
            sqrtRatioNextX96 = get_next_sqrt_price_from_input(sqrtRatioCurrentX96, liquidity, amountRemainingLessFee, zeroForOne)
    else:
        if zeroForOne:
            amountOut = get_amount1_delta(sqrtRatioTargetX96, sqrtRatioCurrentX96, liquidity, False)
        else:
            amountOut = get_amount0_delta(sqrtRatioCurrentX96, sqrtRatioTargetX96, liquidity, False)
        if -amountRemaining >= amountOut:
            sqrtRatioNextX96 = sqrtRatioTargetX96
        else:
            sqrtRatioNextX96 = get_next_sqrt_price_from_output(sqrtRatioCurrentX96, liquidity, -amountRemaining, zeroForOne)

    max_ratio = sqrtRatioTargetX96 == sqrtRatioNextX96

    if zeroForOne:
        amountIn = amountIn if (max_ratio and exactIn) else get_amount0_delta(sqrtRatioNextX96, sqrtRatioCurrentX96, liquidity, True)
        amountOut = amountOut if (max_ratio and not exactIn) else get_amount1_delta(sqrtRatioNextX96, sqrtRatioCurrentX96, liquidity, False)
    else:
        amountIn = amountIn if (max_ratio and exactIn) else get_amount1_delta(sqrtRatioCurrentX96, sqrtRatioNextX96, liquidity, True)
        amountOut = amountOut if (max_ratio and not exactIn) else get_amount0_delta(sqrtRatioCurrentX96, sqrtRatioNextX96, liquidity, False)

    if not exactIn and amountOut > -amountRemaining :
        amountOut = -amountRemaining

    if exactIn and sqrtRatioNextX96 != sqrtRatioTargetX96:
        feeAmount = amountRemaining - amountIn
    else:
        feeAmount = int((amountIn * feePips + (1e6 - feePips) - 1) // (1e6 - feePips))

    return sqrtRatioNextX96, amountIn, amountOut, feeAmount

