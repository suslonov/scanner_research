#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
experimental code with a lot of garbage

"""



import requests
from web3 import Web3
import eth_abi
import json

HEADERS = {'Content-Type': "application/json"}
ETHERSCAN_GETABI = 'http://api.etherscan.io/api?module=contract&action=getabi&address={}&apikey={}'
UNISWAP_V2_FEE = 0.003

Uniswap_V2_factory = "0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f"

QUOTER_CONTRACT_ADDRESS_V3 = "0xb27308f9F90D607463bb33eA1BeBb41C27CE5AB6"

Uniswap_V2_USDC_ETH = "0xB4e16d0168e52d35CaCD2c6185b44281Ec28C9Dc"
Uniswap_V2_WBTC_ETH = "0xbb2b8038a1640196fbe3e38816f3e67cba72d940"
Uniswap_V3_USDC_ETH = "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640"
uniswap_V3_ABI = "0x68b3465833fb72A70ecDF485E0e4C7bD8665Fc45"
UniswapV3SwapRouter02 = "0x68b3465833fb72A70ecDF485E0e4C7bD8665Fc45"

abi_storage = {}
contract_storage = {}

KEY_FILE = '../keys/alchemy.sec'
ETHERSCAN_KEY_FILE = '../keys/etherscan.sec'

with open(KEY_FILE, 'r') as f:
    k1 = f.readline()
    alchemy_url = k1.strip('\n')
    k2 = f.readline()
    alchemy_wss = k2.strip('\n')

with open(ETHERSCAN_KEY_FILE, 'r') as f:
    k1 = f.readline()
    etherscan_key = k1.strip('\n')

w3 = Web3(Web3.HTTPProvider(alchemy_url))

def get_abi_and_contract(address, abi_address=None):
    if not abi_address:
        abi_address = address
    res = requests.get(ETHERSCAN_GETABI.format(abi_address, etherscan_key), headers=HEADERS)
    d = res.json()
    abi = d["result"]
    contract = w3.eth.contract(address=Web3.to_checksum_address(address), abi=abi)
    return abi, contract

# address = Uniswap_V3_USDC_ETH
# abi_storage[address], contract_storage[address] = get_abi_and_contract(address)


address = "0x93Bc2fFfB39935e2CE4904a30605de34806Fec8c"
abi_storage[address], contract_storage[address] = get_abi_and_contract(address)

reserves = contract_storage[address].functions.getReserves().call()
total_supply = contract_storage[address].functions.totalSupply().call()
print(reserves)

abi_storage[Uniswap_V2_factory], contract_storage[Uniswap_V2_factory] = get_abi_and_contract(Uniswap_V2_factory)


usdc_token = contract_storage[Uniswap_V2_USDC_ETH].functions.token0().call()
weth_token = contract_storage[Uniswap_V2_USDC_ETH].functions.token1().call()
abi_storage[weth_token], contract_storage[weth_token] = get_abi_and_contract(weth_token)
abi_storage[usdc_token], contract_storage[usdc_token] = get_abi_and_contract(usdc_token)

symbol_dict = {'inputs': [], 'name': 'symbol', 'outputs': [{'internalType': 'string', 'name': '', 'type': 'string'}], 'stateMutability': 'view', 'type': 'function'}
lame_abi = json.loads(abi_storage[usdc_token])
lame_abi.append(symbol_dict)
abi_storage[usdc_token] = json.dumps(lame_abi)
contract_storage[usdc_token] =w3.eth.contract(address=Web3.to_checksum_address(usdc_token), abi=abi_storage[usdc_token])

contract_storage[usdc_token].functions.decimals().call()

dai = "0x6b175474e89094c44da98b954eedeac495271d0f"
abi_storage[dai], contract_storage[dai] = get_abi_and_contract(dai)
contract_storage[dai].functions.decimals().call()

# contract_storage[Uniswap_V2_factory].functions.allPairsLength().call()
# contract_storage[Uniswap_V2_factory].functions.allPairs(200).call()
# getPair(address tokenA, address tokenB)

pair = Uniswap_V2_USDC_ETH

def amount_out_v2(x, x0, y0):
    return y0 * x * (1 - UNISWAP_V2_FEE) / (x0 + x * (1 - UNISWAP_V2_FEE))

def price_impact_v2(x, x0, y0):
    return x / amount_out_v2(x, x0, y0) - x0 / y0

def profit_v2(pair, x, our, gas):
    reserves = contract_storage[pair].functions.getReserves().call()    
    x0 = reserves[0] / 1e6 # for USDC and USDT only
    y0 = reserves[1] / 1e18
    profit = (price_impact_v2(x, x0, y0) - 2 * UNISWAP_V2_FEE) * our - gas  #supposing  our << x
    return profit


# V2 profit estimator
gas = 200000 / 1e9
profit = profit_v2(Uniswap_V2_USDC_ETH, 100, 1, gas)
print(profit)



abi_storage[uniswap_V3_ABI], contract_storage[Uniswap_V3_USDC_ETH] = get_abi_and_contract(Uniswap_V3_USDC_ETH, uniswap_V3_ABI)
dir(contract_storage[Uniswap_V3_USDC_ETH].functions)
abi_storage[uniswap_V3_ABI], contract_storage[UniswapV3SwapRouter02] = get_abi_and_contract(UniswapV3SwapRouter02, uniswap_V3_ABI)
dir(contract_storage[UniswapV3SwapRouter02].functions)

abi_storage[QUOTER_CONTRACT_ADDRESS_V3], contract_storage[QUOTER_CONTRACT_ADDRESS_V3] = get_abi_and_contract(QUOTER_CONTRACT_ADDRESS_V3)
dir(contract_storage[QUOTER_CONTRACT_ADDRESS_V3].functions)
fee = 0.003
amount_in = 10
amount_out = contract_storage[QUOTER_CONTRACT_ADDRESS_V3].functions.quoteExactInputSingle(usdc_token,
                                                                          weth_token,
                                                                          int(fee * 1e6),
                                                                          int(amount_in * 1e6), 0).call() / 1e18
print(amount_out)
amount_in/amount_out


# V2 swap
WINGS_TOKEN_ADDRESS = "0x667088b212ce3d06a1b553a7221E1fD19000d9aF"
USDC_TOKEN_ADDRESS = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"

abi_storage[QUOTER_CONTRACT_ADDRESS_V3], contract_storage[QUOTER_CONTRACT_ADDRESS_V3] = get_abi_and_contract(QUOTER_CONTRACT_ADDRESS_V3)
abi_storage[Uniswap_V2_factory], contract_storage[Uniswap_V2_factory] = get_abi_and_contract(Uniswap_V2_factory)

wings_usdc_pair_address = contract_storage[Uniswap_V2_factory].functions.getPair(WINGS_TOKEN_ADDRESS, USDC_TOKEN_ADDRESS).call()




uniswap_router = contract_storage[UniswapV3SwapRouter02]
abi_storage[WINGS_TOKEN_ADDRESS], contract_storage[WINGS_TOKEN_ADDRESS] = get_abi_and_contract(WINGS_TOKEN_ADDRESS)
token0_contract = contract_storage[WINGS_TOKEN_ADDRESS]
token0_allowance = token0_contract.functions.allowance(sender_address, uniswap_router_address).call()

# Approve the Uniswap router to spend the token
if token0_allowance < amount_in:
    approve_transaction = token0_contract.functions.approve(uniswap_router_address, 2**256 - 1).buildTransaction({
        'from': sender_address,
        'nonce': w3.eth.getTransactionCount(sender_address),
        'gas': 100000,  # Adjust the gas limit as per your requirements
        'gasPrice': w3.eth.gas_price,
    })
    signed_approve_txn = Account.sign_transaction(approve_transaction, private_key)
    w3.eth.send_raw_transaction(signed_approve_txn.rawTransaction)

# Build the transaction
transaction = uniswap_router.functions.swapExactTokensForTokens(
    amount_in,
    amount_out_min,
    [token0_address, token1_address],
    sender_address,
    w3.eth.getBlock('latest')['timestamp'] + 1000  # Set a deadline for the transaction
).buildTransaction({
    'from': sender_address,
    'nonce': w3.eth.getTransactionCount(sender_address),
    'gas': 300000,  # Adjust the gas limit as per your requirements
    'gasPrice': w3.eth.gas_price,
})

    
address = "0x93Bc2fFfB39935e2CE4904a30605de34806Fec8c"
abi_storage[address], contract_storage[address] = get_abi_and_contract(address)

loong = "0x79b6dd708d209b540274dc898a23d89db294d7f0"
abi_storage[loong], contract_storage[loong] = get_abi_and_contract(loong)

weth_token = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
abi_storage[weth_token], contract_storage[weth_token] = get_abi_and_contract(weth_token)

loong_balance = contract_storage[address].functions.balanceOf(Web3.to_checksum_address(weth_token)).call()

