#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from web3 import Web3
from eth_abi import abi
from hexbytes import HexBytes
from contract_V3 import _swap
from libs_V3 import MIN_TICK, MAX_TICK, MIN_SQRT_RATIO, MAX_SQRT_RATIO
from _utils.etherscan import _get_contract
from _utils.Multicall2 import Multicall2_abi
from _utils.utils import s64

Multicall2_address = "0x5BA1e12693Dc8F9c48aAD8770482f4739bEeD696".lower()

def get_all_for_V3(context):
    contract = context["pool_contract"]
    Multicall2_contract = _get_contract(context["w3"],
                                        Multicall2_abi,
                                        Web3.to_checksum_address(Multicall2_address))
    tick_spacing = contract.functions.tickSpacing().call()
    bitmap_range = (MIN_TICK//tick_spacing) >> 8

    _address = Web3.to_checksum_address(context["pool_address"],)

    calls = [
             (_address, HexBytes(contract.functions.fee()._encode_transaction_data())),
             (_address, HexBytes(contract.functions.liquidity()._encode_transaction_data())),
             (_address, HexBytes(contract.functions.slot0()._encode_transaction_data())),
             ]

    for i in range(bitmap_range, -bitmap_range):
        calls.append((_address, HexBytes(contract.functions.tickBitmap(i)._encode_transaction_data())))

    if context["block_hash"] is None:
        Multicall2_results = Multicall2_contract.functions.aggregate(calls).call()
    else:
        Multicall2_results = Multicall2_contract.functions.aggregate(calls).call(block_identifier=context["block_hash"])
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

def init_context(context):
    V3_swap_data = get_all_for_V3(context)
    
    context["ticks_dict"] = {}
    context["tick_bitmap_dict"] = V3_swap_data["tickBitmap"]
    context["tick_spacing"] = V3_swap_data["tick_spacing"]
    context["fee"] = V3_swap_data["fee"]
    slot0 = V3_swap_data["slot0"]
    context["slot0"] = {"feeProtocol": slot0[-2],
                        "sqrtPriceX96": slot0[0],
                        "tick": slot0[1],
                        "liquidity": V3_swap_data["liquidity"],
                        }

def swap(zeroForOne, amountSpecified, sqrtPriceLimitX96, context):
    if sqrtPriceLimitX96 == 0:
        if zeroForOne == 0:
            sqrtPriceLimitX96 = MAX_SQRT_RATIO
        else:
            sqrtPriceLimitX96 = MIN_SQRT_RATIO 

    return _swap(zeroForOne, amountSpecified, sqrtPriceLimitX96, context)
