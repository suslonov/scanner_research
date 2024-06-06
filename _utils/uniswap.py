#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import numpy as np
from functools import partial
from web3 import Web3
import eth_abi
from eth_abi.packed import encode_packed

from _utils.commands_sol import uniswap_universal_router_code_to_command, uniswap_universal_router_command_abi
from _utils.utils import hex_to_gwei, hex_to_eth, RED, RESET_COLOR
from _utils.etherscan import get_contract_sync

MIN_GAS_PRICE_LIMIT = 1.0
MAX_GAS_PRICE_LIMIT = 2
UNISWAP_V2_FEE = 0.003
GAS_USAGE = 150000
GAS_LIMIT = 400000

V2_FACTORY = {"UNISWAP_V2_FACTORY": "0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f".lower(), "SUSHI_FACTORY": "0xC0AEe478e3658e2610c5F7A4A2E1777cE9e4f2Ac".lower()}
V3_FACTORY = {"V3_FACTORY": "0x1F98431c8aD98523631AE4a59f267346ea31F984".lower()}
WETH = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2".lower()
CREATION_CODE = bytes.fromhex('96e8ac4277198ff8b6f785478aa9a39f403cb768dd02cbee326c3e7da348845f')
PREFIX = bytes.fromhex('ff')

MONITOR_TOKENS = {"WETH": WETH,
                    "USDC": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48".lower()}

def extract_path_from_V3(str_path):
    path = []
    i = 0
    while i < len(str_path):
        path.append(("0x" + str_path[i:i+20].hex().lower(), str_path[i+20:i+23]))
        i = i + 23
    return path

def uniswap_pair_address(token0, token1):

    if token0 > token1:
        token0, token1 = token1, token0

    abiEncoded_1 = encode_packed(['address', 'address'], [token0, token1])
    _salt = Web3.keccak(abiEncoded_1)
    abiEncoded_2 = encode_packed([ 'bytes', 'address', 'bytes', 'bytes'], [PREFIX, V2_FACTORY["UNISWAP_V2_FACTORY"], _salt, CREATION_CODE])
    return Web3.keccak(abiEncoded_2)[12:].hex()


def uniswap_transaction_decode(tx):
    fn_name = tx["decoded_input"][0].abi["name"]
    analytics = {"function": fn_name,
                "gas_price": hex_to_gwei(tx["gasPrice"]),
                "value": hex_to_eth(tx["value"])}

    if fn_name == 'execute':
        command_codes = tx["decoded_input"][1]["commands"]
        operations = []
        V3_detected = False
        V2_detected = False
        for i, command_code in enumerate(command_codes):
            command = uniswap_universal_router_code_to_command(command_code)
            abi = uniswap_universal_router_command_abi(command)
            if abi:
                none_zero_input = eth_abi.abi.decode(abi, tx["decoded_input"][1]["inputs"][i])
                if command == "V3_SWAP_EXACT_IN":
                    V3_detected = True
                    operations.append({"command": command,
                                          "amount_in": none_zero_input[1],
                                          "amount_out_min": none_zero_input[2],
                                          "token_pairs": extract_path_from_V3(none_zero_input[3])
                                          })
                elif command == "V3_SWAP_EXACT_OUT":
                    V3_detected = True
                    operations.append({"command": command,
                                          "amount_out": none_zero_input[1],
                                          "amount_in_max": none_zero_input[2],
                                          "token_pairs": extract_path_from_V3(none_zero_input[3])
                                          })
                elif command == "V2_SWAP_EXACT_IN":
                    V2_detected = True
                    operations.append({"command": command,
                                          "amount_in": none_zero_input[1],
                                          "amount_out_min": none_zero_input[2],
                                          "tokens": [a.lower() for a in none_zero_input[3]]
                                          })
                elif command == "V2_SWAP_EXACT_OUT":
                    V2_detected = True
                    operations.append({"command": command,
                                          "amount_out": none_zero_input[1],
                                          "amount_in_max": none_zero_input[2],
                                          "tokens": [a.lower() for a in none_zero_input[3]]
                                          })

            else:
                operations.append({"command": command})

        analytics["operations"] = operations
        analytics["V3_detected"] = V3_detected
        analytics["V2_detected"] = V2_detected
    else:
        if fn_name in ["exactInputSingle", "exactOutputSingle"]:
            operation = {"command": fn_name,
                                          "token_pairs": [(tx["decoded_input"][1]["params"]["tokenIn"], tx["decoded_input"][1]["params"]["fee"]),
                                                          (tx["decoded_input"][1]["params"]["tokenOut"], 0)],
                                          }
            if "amountIn" in tx["decoded_input"][1]["params"]:
                operation["amount_in"] = tx["decoded_input"][1]["params"]["amountIn"]
            if "amountInMaximum" in tx["decoded_input"][1]["params"]:
                operation["amount_in_max"] = tx["decoded_input"][1]["params"]["amountInMaximum"]
            if "amountOut" in tx["decoded_input"][1]["params"]:
                operation["amount_out"] = tx["decoded_input"][1]["params"]["amountOut"]
            if "amountOutMinimum" in tx["decoded_input"][1]["params"]:
                operation["amount_out_min"] = tx["decoded_input"][1]["params"]["amountOutMinimum"]
            if "sqrtPriceLimitX96" in tx["decoded_input"][1]["params"]:
                operation["sqrt_price_limit_X96"] = tx["decoded_input"][1]["params"]["sqrtPriceLimitX96"]
            analytics["V3_detected"] = True
            analytics["operations"] = [operation]
        if fn_name in ["exactInput", "exactOutput"]:
            operation = {"command": fn_name,
                                          "token_pairs": extract_path_from_V3(tx["decoded_input"][1]["params"]["path"]),
                                          }
            if "amountIn" in tx["decoded_input"][1]["params"]:
                operation["amount_in"] = tx["decoded_input"][1]["params"]["amountIn"]
            if "amountInMaximum" in tx["decoded_input"][1]["params"]:
                operation["amount_in_max"] = tx["decoded_input"][1]["params"]["amountInMaximum"]
            if "amountOut" in tx["decoded_input"][1]["params"]:
                operation["amount_out"] = tx["decoded_input"][1]["params"]["amountOut"]
            if "amountOutMinimum" in tx["decoded_input"][1]["params"]:
                operation["amount_out_min"] = tx["decoded_input"][1]["params"]["amountOutMinimum"]
            analytics["V3_detected"] = True
            analytics["operations"] = [operation]
        elif fn_name in ["swapExactTokensForTokens",
                       "swapTokensForExactTokens",
                       "swapExactETHForTokens",
                       "swapTokensForExactETH",
                       "swapExactTokensForETH",
                       "swapETHForExactTokens",
                       "swapExactTokensForTokensSupportingFeeOnTransferTokens",
                       "swapExactETHForTokensSupportingFeeOnTransferTokens",
                       "swapExactTokensForETHSupportingFeeOnTransferTokens"]:
            analytics["V2_detected"] = True
            operation = {"command": fn_name,
                                          "tokens": [a for a in tx["decoded_input"][1]["path"]] if "path" in tx["decoded_input"][1] else [],
                                          }
            if "amountIn" in tx["decoded_input"][1]:
                operation["amount_in"] = tx["decoded_input"][1]["amountIn"]
            if "amountInMax" in tx["decoded_input"][1]:
                operation["amount_in_max"] = tx["decoded_input"][1]["amountInMax"]
            if "amountOut" in tx["decoded_input"][1]:
                operation["amount_out"] = tx["decoded_input"][1]["amountOut"]
            if "amountOutMin" in tx["decoded_input"][1]:
                operation["amount_out_min"] = tx["decoded_input"][1]["amountOutMin"]
            analytics["operations"] = [operation]
        
    if "operations" in analytics and "deadline" in tx["decoded_input"][1]:
        analytics["deadline"] = tx["decoded_input"][1]["deadline"]
    return analytics

def evaluate_pair(token, amount, context):
    if token == WETH:
        return amount / 1e18
    if token in context["token_storage"]:
        token_price = context["token_storage"][token]["price"]
        if token_price:
            return token_price * amount / (10 ** context["token_storage"][token]["decimals"])
        else:
            return None
    else:
        context["token_storage"][token] = {"price": None, "timestamp": 0, "u_contract": None, "t_contract": None, "decimals" :18, "address": ""}
        return None

def amount_out_v2(x, x0, y0):
    return y0 * x * (1 - UNISWAP_V2_FEE) / (x0 + x * (1 - UNISWAP_V2_FEE))

def amount_in_v2(y, x0, y0):
    return x0 * y / (y0 - y) / (1 - UNISWAP_V2_FEE)
   

def optimal_amount_formula(X0, Y0, Xv, Yv):
    return (-Yv * (1997000 * X0 + 994009 * Xv)
              + np.sqrt(Yv * (9000000 * X0**2 * Yv
                             + 3976036000000 * X0 * Xv * Y0
                             - 5964054000 * X0 * Xv * Yv
                             + 988053892081 * Xv**2 * Yv)
                    )
            ) / (1994000 * Yv)

def profit_function(Xa, X0, Y0, Xv):
    Ya = amount_out_v2(Xa, X0, Y0)
    
    X1 = X0 + Xa
    Y1 = Y0 - Ya

    Yv = amount_out_v2(Xv, X1, Y1)

    X2 = X1 + Xv
    Y2 = Y1 - Yv

    return amount_out_v2(Ya, Y2, X2) - Xa    

def _profit_for_amount(Xa, X0, Y0, Xv, gas_price):
    # Ya = amount_out_v2(Xa, X0, Y0)
    # X = X0 + Xa * (1 - UNISWAP_V2_FEE)
    # Y = Y0 - Ya
    # Yv = amount_out_v2(Xv, X, Y)
    # X = X + Xv * (1 - UNISWAP_V2_FEE)
    # Y = Y - Yv
    # Xe = amount_out_v2(Ya, Y, X)
    
    return profit_function(Xa, X0, Y0, Xv) - 2 * GAS_USAGE * gas_price / 1e9

def optimal_amount(token0, token1, _Xv, _Yv, do_decimals, context):
    if token0 == WETH:
        token0_price = 1
    elif token0 in context["token_storage"]:
        token0_price = context["token_storage"][token0]["price"]
    else:
        context["token_storage"][token0] = {"price": 1, "timestamp": 0, "u_contract": None, "t_contract": None, "decimals" :18, "address": ""}
        return None, None, None

    if not token1 in context["token_storage"]:
        context["token_storage"][token1] = {"price": 1, "timestamp": 0, "u_contract": None, "t_contract": None, "decimals" :18, "address": ""}
        return None, None, None
    
    if context["token_storage"][token0]["t_contract"] is None or context["token_storage"][token1]["t_contract"] is None:
        return None, None, None
    
    if do_decimals:
        Xv = _Xv / (10 ** context["token_storage"][token0]["decimals"])
    else:
        Xv = _Xv
    Yv = _Yv / (10 ** context["token_storage"][token1]["decimals"])
    if (token0, token1) in context["pair_storage"]:
        reserves = context["pair_storage"][(token0, token1)][0]
        if reserves[1] and token0_price:
            # Xa = (Yv * (2 * reserves[0] - Xv * UNISWAP_V2_FEE + Xv) * (UNISWAP_V2_FEE**2 - 2 * UNISWAP_V2_FEE + 1) 
            #       + np.sqrt(-Xv * Yv * (UNISWAP_V2_FEE - 1)**3 * (4 * reserves[0] * reserves[1] - Xv * Yv * UNISWAP_V2_FEE + Xv * Yv))
            #           *(UNISWAP_V2_FEE - 1)) / (2 * Yv * (UNISWAP_V2_FEE - 1) * (UNISWAP_V2_FEE**2 - 2 * UNISWAP_V2_FEE + 1))
            # old incorrect version
            X0 = reserves[0]
            Y0 = reserves[1]
            f = UNISWAP_V2_FEE
            Xa = (X0 * Yv * f - 2 * X0 * Yv - Xv * Yv * f**2 + 2 * Xv * Yv * f - Xv * Yv
                  + np.sqrt(Yv * (X0**2 * Yv * f**2 + 4 * X0 * Xv * Y0 * f**2 - 8 * X0 * Xv * Y0 * f
                                 + 4 * X0 * Xv * Y0 - 2 * X0 * Xv * Yv * f**3 + 4 * X0 * Xv * Yv * f**2
                                 - 2 * X0 * Xv * Yv * f + Xv**2 * Yv * f**4 - 4 * Xv**2 * Yv * f**3
                                 + 6 * Xv**2 * Yv * f**2 - 4 * Xv**2 * Yv * f + Xv**2 * Yv)))/ (2 * Yv * (1 - f))


            Ya = amount_out_v2(Xa, reserves[0], reserves[1])
            X = reserves[0] + Xa * (1 - UNISWAP_V2_FEE)
            Y = reserves[1] - Ya
            # Yv = amount_out_v2(Xv, X, Y)
            X = X + Xv * (1 - UNISWAP_V2_FEE)
            Y = Y - Yv
            Xe = amount_out_v2(Ya, Y, X)
            max_profit = Xe - Xa - 2 * GAS_USAGE * context["gas_price"][0] / 1e9
            profit_for_amount = partial(_profit_for_amount,
                                        # X0=reserves[0],
                                        # Y0=reserves[1],
                                        Xv=Xv,
                                        gas_price=context["gas_price"][0])
            
            return Xa, max_profit, profit_for_amount
        else:
            return None, None, None
    else:
        context["pair_storage"][token0, token1] = ((0, 0), 0, None, "")
        return None, None, None


def target_pairs_from_transaction(operations, context):
    pairs = []
    for o in operations:
        command_pairs = []
        if o["command"] in ["swapExactTokensForTokens",
                       "swapTokensForExactTokens",
                       # "swapExactETHForTokens",
                       # "swapTokensForExactETH",
                       # "swapExactTokensForETH",
                       # "swapETHForExactTokens",
                       "swapExactTokensForTokensSupportingFeeOnTransferTokens",
                       # "swapExactETHForTokensSupportingFeeOnTransferTokens",
                       # "swapExactTokensForETHSupportingFeeOnTransferTokens",
                       "V2_SWAP_EXACT_IN",
                       "V2_SWAP_EXACT_OUT"]:
            good_base_token = False
            for t in o["tokens"]:
                if t in MONITOR_TOKENS.values():
                    good_base_token = True
                    break
            if good_base_token:
                if "amount_in" in o:
                    value = evaluate_pair(o["tokens"][0], o["amount_in"], context)
                else:
                    value = evaluate_pair(o["tokens"][-1], o["amount_out"], context)
            
                for i, t in enumerate(o["tokens"]):
                    if not "amount_in" in o:
                        # command_pairs.append((t, o["tokens"][i+1], value, None, None))
                        continue  # !!! temporarary limitation
                    if i + 1 < len(o["tokens"]) and t in MONITOR_TOKENS.values():
                        try:
                            if i == 0:
                                amount_in = o["amount_in"]
                            else:
                                amount_in = value
                            amount, profit, calculate_function = optimal_amount(t, o["tokens"][i+1], amount_in, o["amount_out_min"], i==0, context)
### !!! incorrect if the pair is not the last pair in the chain
                        except:
                            amount = None
                        if amount and amount > 0:
                            command_pairs.append({"token0": t, 
                                                  "token1":o["tokens"][i+1], 
                                                  "value": value, 
                                                  "optimal_amount": amount, 
                                                  "profit": profit,
                                                  "calculate": calculate_function})
        pairs.extend(command_pairs)
    return pairs

def uniswap_evaluate(tx, context):
    if (tx["analytics"]["gas_price"] < context["gas_price"][0] * MIN_GAS_PRICE_LIMIT or
        tx["analytics"]["gas_price"] > context["gas_price"][0] * MAX_GAS_PRICE_LIMIT or
        not "V2_detected" in tx["analytics"] or not tx["analytics"]["V2_detected"]):
        return False

    target_pairs = target_pairs_from_transaction(tx["analytics"]["operations"], context)
    if not target_pairs:
        return False
    tx["target_pairs"] = target_pairs
    return not tx["target_pairs"][0]["profit"] is None


def garbage_zone():


    from web3 import Web3    

    KEY_FILE = 'alchemy.sec'
    ETHERSCAN_KEY_FILE = 'etherscan.sec'

    with open(KEY_FILE, 'r') as f:
        k1 = f.readline()
        alchemy_url = k1.strip('\n')
        k2 = f.readline()
        alchemy_wss = k2.strip('\n')


    with open(ETHERSCAN_KEY_FILE, 'r') as f:
        k1 = f.readline()
        etherscan_key = k1.strip('\n')

    w3 = Web3(Web3.HTTPProvider(alchemy_url))
    my_context = {"abi_storage": {},
              "w3": w3,
              "etherscan_key": etherscan_key,
              }

    UniversalRouter = "0x3fC91A3afd70395Cd496C647d5a6CC9D4B2b7FAD".lower()
    UniversalRouter_contract = get_contract_sync(my_context, UniversalRouter)
    Uniswap_V2_factory = "0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f"
    Uniswap_V2_factory_contract = get_contract_sync(my_context, Uniswap_V2_factory)

    tx_hash = "0xb0724e38485e3bcc9e9db8608d9ec7fcd161a810152e50d3fbef6e262f2e08db"
    tx3 = dict(w3.eth.get_transaction(tx_hash))

    tx_hash = "0x4158969512ccb8fca31418ae0573eefb33f1be9a029486664f394b7ad7186f80"
    tx4 = dict(w3.eth.get_transaction(tx_hash))

    tx_hash = "0x205d9e86d232029ffbcc2a4a5d9a1314b19d1a01e5698334b2f4591eed6df135"
    tx1 = dict(w3.eth.get_transaction(tx_hash))
    tx1["decoded_input"] = UniversalRouter_contract.decode_function_input(tx1["input"])
    analytics1 = uniswap_transaction_decode(tx1)
    
    tx_hash = "0xa9b3c9a34992d5453637511767dcb27a7ddefc9f42a96ebd741817b6845ac01b"
    tx2 = dict(w3.eth.get_transaction(tx_hash))
    tx2["decoded_input"] = UniversalRouter_contract.decode_function_input(tx2["input"])
    analytics2 = uniswap_transaction_decode(tx2)
    
    WINGS_TOKEN_ADDRESS = "0x667088b212ce3d06a1b553a7221E1fD19000d9aF"
    USDC_TOKEN_ADDRESS = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"

    wings_usdc_pair_address = Uniswap_V2_factory_contract.functions.getPair(WINGS_TOKEN_ADDRESS, USDC_TOKEN_ADDRESS).call()

    wings_usdc_pair = get_contract_sync(my_context, Uniswap_V2_factory_contract.functions.getPair(WINGS_TOKEN_ADDRESS, USDC_TOKEN_ADDRESS).call())
    wings_pair = get_contract_sync(my_context, Uniswap_V2_factory_contract.functions.getPair(WINGS_TOKEN_ADDRESS, WETH).call())

    usdc_pair = get_contract_sync(my_context, Uniswap_V2_factory_contract.functions.getPair(USDC_TOKEN_ADDRESS, WETH).call())

    c1 = get_contract_sync(my_context,
                      Uniswap_V2_factory_contract.functions.getPair(
                          Web3.to_checksum_address('0x34be5b8c30ee4fde069dc878989686abe9884470'),
                          Web3.to_checksum_address('0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'),
                          ).call())

    c2 = get_contract_sync(my_context,
                      Uniswap_V2_factory_contract.functions.getPair(
                          Web3.to_checksum_address('0xe8ECaBECD7A418f30efc03B703e6801Ac2845C50'),
                          Web3.to_checksum_address('0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'),
                          ).call())

    print(c1.functions.getReserves().call())
    print(c2.functions.getReserves().call())
    
    rrr = c2.functions.getReserves().call()
    print(rrr[0]/1e18, rrr[1]/1e18)
    rrr[0]/rrr[1]


