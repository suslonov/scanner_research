#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import requests
from web3 import Web3
import math

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

V3_FACTORY = "0x1F98431c8aD98523631AE4a59f267346ea31F984".lower()
QUOTER_V2 = "0x0209c4Dc18B2A1439fD2427E34E7cF3c6B91cFB9".lower()
WETH = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2".lower()
USDC = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48".lower()


w3 = Web3(Web3.HTTPProvider(ALCHEMY_URL))

quoter_v2_abi = get_abi(QUOTER_V2)
quoter_v2 = get_contract(w3, quoter_v2_abi, QUOTER_V2)
factory_abi = get_abi("0x1F98431c8aD98523631AE4a59f267346ea31F984")
factory_contract = get_contract(w3, factory_abi, "0x1F98431c8aD98523631AE4a59f267346ea31F984")


fee = 10000
token_in = Web3.to_checksum_address('0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2')
token_out = Web3.to_checksum_address('0xcC6c4F450f1d4aeC71C46f240a6bD50c4E556B8A')

amount_in = int(0.00000001 * 1e18)
quote_params = {"tokenIn": token_in, "tokenOut": token_out, "amountIn": amount_in, "fee": fee, "sqrtPriceLimitX96": 0}
current_quote = quoter_v2.functions.quoteExactInputSingle(quote_params).call()
print(current_quote)
print(amount_in, current_quote[0])

token_out = Web3.to_checksum_address('0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2')
token_in = Web3.to_checksum_address('0xcC6c4F450f1d4aeC71C46f240a6bD50c4E556B8A')

amount_in = 87033833432730362
quote_params = {"tokenIn": token_in, "tokenOut": token_out, "amountIn": amount_in, "fee": fee, "sqrtPriceLimitX96": 0}
current_quote = quoter_v2.functions.quoteExactInputSingle(quote_params).call()
print(current_quote)
print(amount_in, current_quote[0])

token_in = Web3.to_checksum_address('0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2')
token_out = Web3.to_checksum_address('0xcC6c4F450f1d4aeC71C46f240a6bD50c4E556B8A')

amount_out = 87033833432730362
quote_params = {"tokenIn": token_in, "tokenOut": token_out, "amount": amount_out, "fee": fee, "sqrtPriceLimitX96": 0}
current_quote = quoter_v2.functions.quoteExactOutputSingle(quote_params).call()
print(current_quote)
print(current_quote[0], amount_out)

token_out = Web3.to_checksum_address('0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2')
token_in = Web3.to_checksum_address('0xcC6c4F450f1d4aeC71C46f240a6bD50c4E556B8A')

amount_out = int(0.00000001 * 1e18)
quote_params = {"tokenIn": token_in, "tokenOut": token_out, "amount": amount_out, "fee": fee, "sqrtPriceLimitX96": 0}
current_quote = quoter_v2.functions.quoteExactOutputSingle(quote_params).call()
print(current_quote)
print(current_quote[0], amount_out)
















token_out = Web3.to_checksum_address('0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2')
token_in = Web3.to_checksum_address('0x6B175474E89094C44Da98b954EedeAC495271d0F') #DAI
fee = 3000
amount_in = int(1 * 1e18)
quote_params = {"tokenIn": token_in, "tokenOut": token_out, "amountIn": amount_in, "fee": fee, "sqrtPriceLimitX96": 0}
current_quote = quoter_v2.functions.quoteExactInputSingle(quote_params).call()
print(current_quote)
print(current_quote[0]/amount_in)




pool_address = factory_contract.functions.getPool(token_in, token_out, fee).call()
pool_abi = get_abi(pool_address)
pool_contract = get_contract(w3, pool_abi, pool_address)

token_out_abi = get_abi(token_out)
token_out_contract = get_contract(w3, token_out_abi, token_out)
token_out_decimals = token_out_contract.functions.decimals().call()

token_in_abi = get_abi(token_in)
token_in_contract = get_contract(w3, token_in_abi, token_in)
token_in_decimals = token_in_contract.functions.decimals().call()

slot0 = pool_contract.functions.slot0().call()

amount_in = int(0.001 * 1e18)
# amount_out_min = 
quote_params = {"tokenIn": token_in, "tokenOut": token_out, "amountIn": amount_in, "fee": fee, "sqrtPriceLimitX96": 0}
current_quote = quoter_v2.functions.quoteExactInputSingle(quote_params).call()

print(current_quote)
print(int(current_quote[0])/1e18)



token_in = Web3.to_checksum_address('0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2')
token_out = Web3.to_checksum_address('0x6B175474E89094C44Da98b954EedeAC495271d0F') #DAI
fee = 10000

pool_address = factory_contract.functions.getPool(token_in, token_out, fee).call()


