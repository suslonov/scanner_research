#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import time
from datetime import datetime
from web3 import Web3
import eth_abi
import json
import requests
import commands_sol
from token_abi import token_abi
import pandas as pd

from utils import hex_to_gwei, hex_to_eth, get_contract_sync, get_contract_standard_token, RED, GREEN, RESET_COLOR, gwei_to_wei, eth_to_wei, AtomicInteger

R_DIR = "/media/Data/csv/"

# KEY_FILE = 'alchemy_sepolia.sec'
# WALLET_FILE = "sepolia_wallet.sec"
KEY_FILE = '../keys/alchemy.sec'
# KEY_FILE = 'alchemy_goerli.sec'
WALLET_FILE = "../keys/wallet.sec"
ETHERSCAN_KEY_FILE = '../keys/etherscan.sec'
HEADERS = {'Content-Type': "application/json"}
HEADERS2 = {"accept": "application/json", 'Content-Type': "application/json"}

PAYLOAD_SENDPRIVATETRANSACTION = {"jsonrpc":"2.0",
                        "id": 3, 
                        "method": "eth_sendPrivateTransaction", 
                        "params": [{"tx": ""}]}

PAYLOAD_CANCELPRIVATETRANSACTION = {"jsonrpc":"2.0",
                        "id": 3, 
                        "method": "eth_cancelPrivateTransaction", 
                        "params": [{"txHash": ""}]}

PAYLOAD_EVALUATE_TRANSACTION = {"jsonrpc":"2.0",
                        "id": 3, 
                        "method": "alchemy_simulateExecution", 
                        "params": []}

with open(KEY_FILE, 'r') as f:
    k1 = f.readline()
    net_url = k1.strip('\n')
    k2 = f.readline()
    net_wss = k2.strip('\n')

with open(ETHERSCAN_KEY_FILE, 'r') as f:
    k1 = f.readline()
    etherscan_key = k1.strip('\n')

w3 = Web3(Web3.HTTPProvider(net_url))

with open(WALLET_FILE, 'r') as f:
    wallet_accounts = json.load(f)

context = {"w3": w3,
           "etherscan_key": etherscan_key,
           "w3_url": net_url,
           "w3_wss": net_wss,
           "abi_storage": {}}


def get_account(index):
    return wallet_accounts["accounts"][index]["key"], wallet_accounts["accounts"][index]["account"]

def get_balance(address):
    return w3.eth.get_balance(address)/1000000000000000000

def create_account():
    account = w3.eth.account.create()
    return account.key.hex(), account.address

def write_accounts(key, address):
    wallet_accounts["accounts"].append((key, address))
    with open(WALLET_FILE, 'w') as f:
        json.dump(f, wallet_accounts)

def transfer_eth():
    nonce = w3.eth.get_transaction_count(Web3.to_checksum_address(wallet_accounts["accounts"][1]["account"]))
    tx = {
        "from": Web3.to_checksum_address(wallet_accounts["accounts"][1]["account"]),
        "to": Web3.to_checksum_address(wallet_accounts["accounts"][2]["account"]),
        "value": eth_to_wei(0.001),
        "data": "",
        "gas": 21000,
        "maxPriorityFeePerGas": gwei_to_wei(0.1),
        "maxFeePerGas": int(w3.eth.gas_price * 1.1),
        "nonce": nonce,
        "type": 2,
        "chainId": 1}
    signed_txn = w3.eth.account.sign_transaction(tx, private_key=wallet_accounts["accounts"][1]["key"])
    tx_hash = w3.eth.send_raw_transaction(signed_txn.rawTransaction)
    
def transfer_tokens():
    WINGS_TOKEN_ADDRESS = "0x667088b212ce3d06a1b553a7221E1fD19000d9aF"
    USDC_TOKEN_ADDRESS = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"

    contract = w3.eth.contract(address=Web3.to_checksum_address(USDC_TOKEN_ADDRESS), abi=token_abi)
    
    my_balance = contract.functions.balanceOf(Web3.to_checksum_address(wallet_accounts["accounts"][1]["account"])).call()

    amount = int(1e6)
    # nonce = w3.eth.get_transaction_count(Web3.to_checksum_address(wallet_accounts["accounts"][1]["account"]))
    nonce = 48

    tx = contract.functions.transfer(Web3.to_checksum_address(wallet_accounts["accounts"][2]["account"]), amount).build_transaction({
        "from": Web3.to_checksum_address(wallet_accounts["accounts"][1]["account"]),
        "gas": 100000,
        # "maxPriorityFeePerGas": gwei_to_wei(0.1),
        "maxFeePerGas": int(w3.eth.gas_price * 1.1),
        "nonce": nonce,
        "chainId": 1})

    gas_estimation = w3.eth.estimate_gas(tx)

    signed_txn = w3.eth.account.sign_transaction(tx, private_key=wallet_accounts["accounts"][1]["key"])
    tx_hash = w3.eth.send_raw_transaction(signed_txn.rawTransaction)

def swap_tokens():

    WINGS_TOKEN_ADDRESS = "0x667088b212ce3d06a1b553a7221E1fD19000d9aF"
    USDC_TOKEN_ADDRESS = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
    Uniswap_V2_Router_2 = "0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D"
    UNISWAP_UNIVERSAL_ROUTER ="0x3fC91A3afd70395Cd496C647d5a6CC9D4B2b7FAD"
    
    WETH = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"
    POPEYE = "0x90b69Cc6E4068C93d11Ae0d7a5647360DA548ac4"

    uniswap_router = get_contract_sync(context, Uniswap_V2_Router_2)
    token0_contract = get_contract_sync(context, WETH)
    token1_contract = get_contract_sync(context, POPEYE)
    sender_address = Web3.to_checksum_address(wallet_accounts["accounts"][1]["account"])
    token0_allowance = token0_contract.functions.allowance(sender_address, Uniswap_V2_Router_2).call()
    token1_allowance = token1_contract.functions.allowance(sender_address, Uniswap_V2_Router_2).call()

# Approve the Uniswap router to spend the token
    amount_in = int(0.01 * 1e18)
    amount_out = int(100000  * 1e18)
    nonce = w3.eth.get_transaction_count(Web3.to_checksum_address(wallet_accounts["accounts"][1]["account"]))
    if token0_allowance < amount_in:
        approve_transaction = token0_contract.functions.approve(Uniswap_V2_Router_2, 2**256 - 1).build_transaction({
            'from': sender_address,
            'nonce': nonce,
            'gas': 100000,
            'gasPrice': w3.eth.gas_price,
        })
        signed_approve_txn = w3.eth.account.sign_transaction(approve_transaction, private_key=wallet_accounts["accounts"][1]["key"])
        w3.eth.send_raw_transaction(signed_approve_txn.rawTransaction)

# Build the transaction
    nonce = w3.eth.get_transaction_count(Web3.to_checksum_address(wallet_accounts["accounts"][1]["account"]))
    transaction = uniswap_router.functions.swapExactTokensForTokens(
        amount_in,
        amount_out,
        [Web3.to_checksum_address(WETH), Web3.to_checksum_address(POPEYE)],
        sender_address,
        w3.eth.get_block('latest')['timestamp'] + 1000
        ).build_transaction({
        'from': sender_address,
        'nonce': nonce,
        'gas': 300000,
        # "maxPriorityFeePerGas": gwei_to_wei(0.1),
        # "maxFeePerGas": int(w3.eth.gas_price * 1.4)})
        'gasPrice': int(w3.eth.gas_price * 1.1)})


    signed_tx = w3.eth.account.sign_transaction(transaction, private_key=wallet_accounts["accounts"][1]["key"])
    tx_hash = w3.eth.send_raw_transaction(signed_tx.rawTransaction)

    amount_in = int(200148.41865 * 1e18)
    amount_out = int(0.001  * 1e18)

    nonce = w3.eth.get_transaction_count(Web3.to_checksum_address(wallet_accounts["accounts"][1]["account"]))
    transaction1 = uniswap_router.functions.swapExactTokensForETHSupportingFeeOnTransferTokens(
        amount_in,
        amount_out,
        [Web3.to_checksum_address(POPEYE), Web3.to_checksum_address(WETH)],
        sender_address,
        w3.eth.get_block('latest')['timestamp'] + 1000
        ).build_transaction({
        'from': sender_address,
        'nonce': nonce,
        'gas': 300000,
        # "maxPriorityFeePerGas": gwei_to_wei(0.1),
        # "maxFeePerGas": int(w3.eth.gas_price * 1.4)})
        'gasPrice': int(w3.eth.gas_price * 1.1)})

    signed_tx1 = w3.eth.account.sign_transaction(transaction1, private_key=wallet_accounts["accounts"][1]["key"])
    tx_hash1 = w3.eth.send_raw_transaction(signed_tx1.rawTransaction)



def good_tx_example():
    good_tx_hash = "0x627ce648a207bc9f2ee0fba34f1a28fc558bcd12846d57b35ba0ddea0f3c27a2"
    good_tx = dict(w3.eth.get_transaction(good_tx_hash))
    
    UNISWAP_UNIVERSAL_ROUTER ="0x3fC91A3afd70395Cd496C647d5a6CC9D4B2b7FAD"

    uniswap_router = get_contract_sync(context, UNISWAP_UNIVERSAL_ROUTER)
    
    good_tx["decoded_input"] = uniswap_router.decode_function_input(good_tx["input"])

    index = 1
    command = commands_sol.uniswap_universal_router_code_to_command(good_tx["decoded_input"][1]["commands"][index])
    abi = commands_sol.uniswap_universal_router_get_abi(command)
    eth_abi.abi.decode(abi, good_tx["decoded_input"][1]["inputs"][index])


def swap_tokens_private():
    WALLET_ACCOUNT = 4
    SENDER_ADDRESS = Web3.to_checksum_address(wallet_accounts["accounts"][WALLET_ACCOUNT]["account"])
    PRIVATE_KEY = wallet_accounts["accounts"][WALLET_ACCOUNT]["key"]

    # UNISWAP_UNIVERSAL_ROUTER ="0x3fC91A3afd70395Cd496C647d5a6CC9D4B2b7FAD".lower()
    Uniswap_V2_Router_2 = "0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D".lower()
    WETH = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2".lower()
    USDC_TOKEN_ADDRESS = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48".lower()

    uniswap_router = get_contract_sync(context, Web3.to_checksum_address(Uniswap_V2_Router_2))
    token0_contract = get_contract_standard_token(w3, Web3.to_checksum_address(WETH))
    token1_contract = get_contract_standard_token(w3, Web3.to_checksum_address(USDC_TOKEN_ADDRESS))

    token0_allowance = token0_contract.functions.allowance(SENDER_ADDRESS, Web3.to_checksum_address(Uniswap_V2_Router_2)).call()
    token1_allowance = token1_contract.functions.allowance(SENDER_ADDRESS, Web3.to_checksum_address(Uniswap_V2_Router_2)).call()

    nonce = w3.eth.get_transaction_count(Web3.to_checksum_address(SENDER_ADDRESS))
    if token0_allowance == 0:
        approve_transaction = token0_contract.functions.approve(Web3.to_checksum_address(Uniswap_V2_Router_2), 2**256 - 1).build_transaction({
            'from': SENDER_ADDRESS,
            'nonce': nonce,
            'gas': 100000,
            'gasPrice': w3.eth.gas_price,
        })
        tx_hash = signed_approve_txn = w3.eth.account.sign_transaction(approve_transaction, private_key=PRIVATE_KEY).hex()
        w3.eth.send_raw_transaction(signed_approve_txn.rawTransaction)

        tx = w3.eth.get_transaction_receipt(tx_hash)
        print(tx.status)

    nonce = w3.eth.get_transaction_count(Web3.to_checksum_address(SENDER_ADDRESS))
    if token1_allowance == 0:
        approve_transaction = token1_contract.functions.approve(Web3.to_checksum_address(Uniswap_V2_Router_2), 2**256 - 1).build_transaction({
            'from': SENDER_ADDRESS,
            'nonce': nonce,
            'gas': 100000,
            'gasPrice': w3.eth.gas_price,
        })
        signed_approve_txn = w3.eth.account.sign_transaction(approve_transaction, private_key=PRIVATE_KEY)
        tx_hash = w3.eth.send_raw_transaction(signed_approve_txn.rawTransaction).hex()

        tx = w3.eth.get_transaction_receipt(tx_hash)
        print(tx.status)

    print(token0_contract.functions.decimals().call())
    print(token1_contract.functions.decimals().call())

    amount = 0.1
    amount_in = int(amount * 1e18)
    amount_out = int(amount * 2000 / 2  * 1e6)

    nonce = w3.eth.get_transaction_count(Web3.to_checksum_address(SENDER_ADDRESS))
    transaction2 = uniswap_router.functions.swapExactTokensForTokens(
        amount_in,
        amount_out,
        [Web3.to_checksum_address(WETH), Web3.to_checksum_address(USDC_TOKEN_ADDRESS)],
        SENDER_ADDRESS,
        w3.eth.get_block('latest')['timestamp'] + 1000
        ).build_transaction({
        'from': SENDER_ADDRESS,
        'nonce': nonce,
        'gas': 300000,
        "maxPriorityFeePerGas": gwei_to_wei(2),
        "maxFeePerGas": int(w3.eth.gas_price * 1.4)})
        # 'gasPrice': int(w3.eth.gas_price * 1.1)})
    signed_tx2 = w3.eth.account.sign_transaction(transaction2, private_key=PRIVATE_KEY)

    PAYLOAD_SENDPRIVATETRANSACTION["params"][0]["tx"] = signed_tx2.rawTransaction.hex()
    res = requests.post(net_url, headers=HEADERS, data=json.dumps(PAYLOAD_SENDPRIVATETRANSACTION))

    tx_hash2 = res.json()["result"]

    while True:
        try:
            tx_hash2 = res.json()["result"]
            tx = w3.eth.get_transaction_receipt(tx_hash2)
            print(tx.status)
            break
        except:
            print("try")
        time.sleep(30)

    # PAYLOAD_CANCELPRIVATETRANSACTION["params"][0]["txHash"] = tx_hash2
    # res = requests.post(net_url, headers=HEADERS, data=json.dumps(PAYLOAD_CANCELPRIVATETRANSACTION))


    amount = 193.00325
    amount_in = int(amount * 1e6)
    amount_out = int(amount / 2000 / 2  * 1e18)

    nonce = w3.eth.get_transaction_count(Web3.to_checksum_address(SENDER_ADDRESS))
    transaction3 = uniswap_router.functions.swapExactTokensForTokens(
        amount_in,
        amount_out,
        [Web3.to_checksum_address(USDC_TOKEN_ADDRESS), Web3.to_checksum_address(WETH)],
        SENDER_ADDRESS,
        w3.eth.get_block('latest')['timestamp'] + 1000
        ).build_transaction({
        'from': SENDER_ADDRESS,
        'nonce': nonce,
        'gas': 300000,
        "maxPriorityFeePerGas": gwei_to_wei(4),
        "maxFeePerGas": int(w3.eth.gas_price * 1.4)})
        # 'gasPrice': int(w3.eth.gas_price * 1.1)})
    print(datetime.utcnow().timestamp())
    signed_tx3 = w3.eth.account.sign_transaction(transaction3, private_key=PRIVATE_KEY)

    PAYLOAD_SENDPRIVATETRANSACTION["params"][0]["tx"] = signed_tx3.rawTransaction.hex()
    res = requests.post(net_url, headers=HEADERS, data=json.dumps(PAYLOAD_SENDPRIVATETRANSACTION))
    tx_hash3 = res.json()["result"]
    print(datetime.utcnow().timestamp())

    while True:
        try:
            tx_hash3 = res.json()["result"]
            tx = w3.eth.get_transaction_receipt(tx_hash3)
            print(tx.status)
            break
        except:
            print("try")
        time.sleep(30)

    print(datetime.utcnow().timestamp())


def transfer_tokens_estimate_gas():
    WINGS_TOKEN_ADDRESS = "0x667088b212ce3d06a1b553a7221E1fD19000d9aF"
    USDC_TOKEN_ADDRESS = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
    AWOKE = "0xd75C5AA683485780940cf0F78C08AaC051e5573D"
    WETH = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"

    amount = 0
    nonce = 48

    token = AWOKE
    contract = w3.eth.contract(address=Web3.to_checksum_address(token), abi=token_abi)

    tx = contract.functions.transfer(Web3.to_checksum_address(wallet_accounts["accounts"][2]["account"]), amount).build_transaction({
        "from": Web3.to_checksum_address(wallet_accounts["accounts"][1]["account"]),
        "gas": 100000,
        # "maxPriorityFeePerGas": gwei_to_wei(0.1),
        "maxFeePerGas": int(w3.eth.gas_price * 1.1),
        "nonce": nonce,
        "chainId": 1})

    gas_estimation = w3.eth.estimate_gas(tx)
    print(gas_estimation)




def evaluate_swap():
    WALLET_ACCOUNT = 1
    SENDER_ADDRESS = Web3.to_checksum_address(wallet_accounts["accounts"][WALLET_ACCOUNT]["account"])
    PRIVATE_KEY = wallet_accounts["accounts"][WALLET_ACCOUNT]["key"]

    # UNISWAP_UNIVERSAL_ROUTER ="0x3fC91A3afd70395Cd496C647d5a6CC9D4B2b7FAD".lower()
    Uniswap_V2_Router_2 = "0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D".lower()
    WETH = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2".lower()
    USDC_TOKEN_ADDRESS = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48".lower()
    AWOKE = "0xd75C5AA683485780940cf0F78C08AaC051e5573D".lower()

    uniswap_router = get_contract_sync(context, Web3.to_checksum_address(Uniswap_V2_Router_2))
    token0_contract = get_contract_standard_token(w3, Web3.to_checksum_address(WETH))
    token1_contract = get_contract_standard_token(w3, Web3.to_checksum_address(USDC_TOKEN_ADDRESS))

    # token0_allowance = token0_contract.functions.allowance(SENDER_ADDRESS, Web3.to_checksum_address(Uniswap_V2_Router_2)).call()
    # token1_allowance = token1_contract.functions.allowance(SENDER_ADDRESS, Web3.to_checksum_address(Uniswap_V2_Router_2)).call()

    # nonce = w3.eth.get_transaction_count(Web3.to_checksum_address(SENDER_ADDRESS))
    # if token0_allowance == 0:
    #     approve_transaction = token0_contract.functions.approve(Web3.to_checksum_address(Uniswap_V2_Router_2), 2**256 - 1).build_transaction({
    #         'from': SENDER_ADDRESS,
    #         'nonce': nonce,
    #         'gas': 100000,
    #         'gasPrice': w3.eth.gas_price,
    #     })
    #     tx_hash = signed_approve_txn = w3.eth.account.sign_transaction(approve_transaction, private_key=PRIVATE_KEY).hex()
    #     w3.eth.send_raw_transaction(signed_approve_txn.rawTransaction)

    #     tx = w3.eth.get_transaction_receipt(tx_hash)
    #     print(tx.status)

    # nonce = w3.eth.get_transaction_count(Web3.to_checksum_address(SENDER_ADDRESS))
    # if token1_allowance == 0:
    #     approve_transaction = token1_contract.functions.approve(Web3.to_checksum_address(Uniswap_V2_Router_2), 2**256 - 1).build_transaction({
    #         'from': SENDER_ADDRESS,
    #         'nonce': nonce,
    #         'gas': 100000,
    #         'gasPrice': w3.eth.gas_price,
    #     })
    #     signed_approve_txn = w3.eth.account.sign_transaction(approve_transaction, private_key=PRIVATE_KEY)
    #     tx_hash = w3.eth.send_raw_transaction(signed_approve_txn.rawTransaction).hex()

    #     tx = w3.eth.get_transaction_receipt(tx_hash)
    #     print(tx.status)

    print(token0_contract.functions.decimals().call())
    print(token1_contract.functions.decimals().call())

    amount = 0.1
    amount_in = int(amount * 1e18)
    amount_out = int(amount * 2000 / 2  * 1e6)

    nonce = w3.eth.get_transaction_count(Web3.to_checksum_address(SENDER_ADDRESS))
    transaction2 = uniswap_router.functions.swapExactTokensForTokens(
        amount_in,
        amount_out,
        [Web3.to_checksum_address(WETH), Web3.to_checksum_address(USDC_TOKEN_ADDRESS)],
        SENDER_ADDRESS,
        w3.eth.get_block('latest')['timestamp'] + 1000
        ).build_transaction({
        'from': SENDER_ADDRESS,
        'nonce': nonce,
        'gas': 300000,
        "maxPriorityFeePerGas": gwei_to_wei(2),
        "maxFeePerGas": int(w3.eth.gas_price * 1.4)})
        # 'gasPrice': int(w3.eth.gas_price * 1.1)})
    # transaction2["value"] = hex(0)
    # signed_tx2 = w3.eth.account.sign_transaction(transaction2, private_key=PRIVATE_KEY)

    PAYLOAD_EVALUATE_TRANSACTION["params"] = [transaction2]
    res = requests.post(net_url, headers=HEADERS, data=json.dumps(PAYLOAD_EVALUATE_TRANSACTION))

    tx_res = res.json()["result"]


def contract_functions():

    a = "0x48c87cdacb6bb6bf6e5cd85d8ee5c847084c7410"
    contract = get_contract_sync(context, Web3.to_checksum_address(a))
    for f in dir(contract.functions):
        if f.lower().find("tax") >= 0 or f.lower().find("fee") >= 0:
            print(f)
    print()

    a = "0xf58a978aa68515178583036d6fbd11d3afb83f2a"
    contract = get_contract_sync(context, Web3.to_checksum_address(a))
    for f in dir(contract.functions):
        if f.lower().find("tax") >= 0 or f.lower().find("fee") >= 0:
            print(f)
    print()

    a = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
    contract = get_contract_sync(context, Web3.to_checksum_address(a))
    for f in dir(contract.functions):
        if f.lower().find("tax") >= 0 or f.lower().find("fee") >= 0:
            print(f)
    
    a = "0x667088b212ce3d06a1b553a7221E1fD19000d9aF"
    contract = get_contract_sync(context, Web3.to_checksum_address(a))
    for f in dir(contract.functions):
        if f.lower().find("tax") >= 0 or f.lower().find("fee") >= 0:
            print(f)


def get_block():
    block_num = 17765571
    tx_num = w3.eth.get_block_transaction_count(block_num)
    
    tx_list = []
    for i in range(tx_num):
        t = w3.eth.get_transaction_by_block(block_num, i)
        tt = dict(t)
        tt["gasPrice"] = tt["gasPrice"] / 1e9
        if "maxPriorityFeePerGas" in tt:
            tt["maxPriorityFeePerGas"] = tt["maxPriorityFeePerGas"] / 1e9
        if "maxFeePerGas" in tt:
            tt["maxFeePerGas"] = tt["maxFeePerGas"] / 1e9
        tx_list.append(tt)

    for t in tx_list:
        t["hash"] = t["hash"].hex()
        # t["maxPriorityFeePerGas"] = t["maxPriorityFeePerGas"] / 1e9

    # df17870977 = pd.DataFrame.from_records(tx_list)
    # df17870977.set_index("transactionIndex", inplace=True)
    
    df17765571 = pd.DataFrame.from_records(tx_list)
    df17765571.set_index("transactionIndex", inplace=True)
    
    df17765571[["hash", 'gasPrice', 'maxFeePerGas', 'maxPriorityFeePerGas']].to_csv(R_DIR+"block_17765571.csv")
    
    
    
    