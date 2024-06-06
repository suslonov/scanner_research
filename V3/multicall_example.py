#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# https://github.com/uniswap-python/uniswap-python/blob/ad8c8a14d7539c615795174ef0bcd52f6f73c614/uniswap/constants.py#L80


import requests
from web3 import Web3
from eth_abi import abi
from hexbytes import HexBytes
from _utils.utils import s64

HEADERS_E = {'Content-Type': "application/json"}
ETHERSCAN_GETABI = 'http://api.etherscan.io/api?module=contract&action=getabi&address={}&apikey={}'

KEY_FILE = '../keys/alchemy.sec'
ETHERSCAN_KEY_FILE = '../keys/etherscan.sec'

MIN_TICK = -887272
MAX_TICK = -MIN_TICK

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


w3 = Web3(Web3.HTTPProvider(ALCHEMY_URL))

UniswapV3SwapRouter02 = "0x68b3465833fb72A70ecDF485E0e4C7bD8665Fc45".lower()
Multicall = "0xeefBa1e63905eF1D7ACbA5a8513c70307C1cE441".lower()
Multicall2 = "0x5BA1e12693Dc8F9c48aAD8770482f4739bEeD696".lower()

UniswapV3SwapRouter02_abi = get_abi(UniswapV3SwapRouter02)
UniswapV3SwapRouter02_contract = get_contract(w3, UniswapV3SwapRouter02_abi, UniswapV3SwapRouter02)

Multicall_abi = get_abi(Multicall)
Multicall_contract = get_contract(w3, Multicall_abi, Multicall)

Multicall2_abi = get_abi(Multicall2)
Multicall2_contract = get_contract(w3, Multicall2_abi, Multicall2)

# # Source: https://github.com/Uniswap/v3-core/blob/v1.0.0/contracts/UniswapV3Factory.sol#L26-L31
# _tick_spacing = {100: 1, 500: 10, 3_000: 60, 10_000: 200}
# {_tick_spacing[t]: t for t in _tick_spacing}[tick_spacing]

# # Derived from (MIN_TICK//tick_spacing) >> 8 and (MAX_TICK//tick_spacing) >> 8
# _tick_bitmap_range = {
#     100: (-3466, 3465),
#     500: (-347, 346),
#     3_000: (-58, 57),
#     10_000: (-18, 17),
# }

def get_all_for_V3(Multicall2_contract, address, contract):
    tick_spacing = contract.functions.tickSpacing().call()
    bitmap_range = (MIN_TICK//tick_spacing) >> 8

    _address = Web3.to_checksum_address(address)

    calls = [
             (_address, HexBytes(contract.functions.token0()._encode_transaction_data())),
             (_address, HexBytes(contract.functions.token1()._encode_transaction_data())),
             (_address, HexBytes(contract.functions.fee()._encode_transaction_data())),
             (_address, HexBytes(contract.functions.liquidity()._encode_transaction_data())),
             (_address, HexBytes(contract.functions.slot0()._encode_transaction_data())),
             ]

    for i in range(bitmap_range, -bitmap_range):
        calls.append((_address, HexBytes(contract.functions.tickBitmap(i)._encode_transaction_data())))

    Multicall2_results = Multicall2_contract.functions.aggregate(calls).call()
    slot0 = abi.decode(["uint256", "uint256", "uint256", "uint256", "uint256", "uint256", "bool"], Multicall2_results[1][4])
    results = {
        "token0": abi.decode(["address"], Multicall2_results[1][0])[0],
        "token1": abi.decode(["address"], Multicall2_results[1][1])[0],
        "fee": abi.decode(["uint256"], Multicall2_results[1][2])[0],
        "liquidity": abi.decode(["uint256"], Multicall2_results[1][3])[0],
        "slot0": (slot0[0], s64(slot0[1]), slot0[2], slot0[3], slot0[4], slot0[5], slot0[6]),
        }
    
    tick_bitmap = {}
    for i, q in enumerate(Multicall2_results[1][len(results):]):
        tick_bitmap[i + bitmap_range] =  abi.decode(["uint256"], q)[0]
    results["tickBitmap"] = tick_bitmap

    return results


WBTC_WETH = "0xcbcdf9626bc03e24f779434178a73a0b4bad62ed".lower()
WBTC_WETH_abi = get_abi(WBTC_WETH)
WBTC_WETH_contract = get_contract(w3, WBTC_WETH_abi, WBTC_WETH)

qqq = get_all_for_V3(Multicall2_contract, WBTC_WETH, WBTC_WETH_contract)
print(qqq)


ELONGATE = "0x0a1665e3F54eeB364BeC6954e4497dD802840A01".lower()
ELONGATE_abi = get_abi(ELONGATE)
ELONGATE_contract = get_contract(w3, ELONGATE_abi, ELONGATE)

qqq = get_all_for_V3(Multicall2_contract, ELONGATE, ELONGATE_contract)
print(qqq)

DAI = "0xc2e9f25be6257c210d7adf0d4cd6e3e881ba25f8"
DAI_abi = get_abi(DAI)
DAI_contract = get_contract(w3, DAI_abi, DAI)

qqq = get_all_for_V3(Multicall2_contract, DAI, DAI_contract)
print(qqq)
