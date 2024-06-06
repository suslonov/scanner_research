#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import json
import requests
from openai import OpenAI
import pyparsing
# from openai.embeddings_utils import distances_from_embeddings

PARAMETERS_FILE = "~/git/scanner_research/parameters.json"
HEADERS = {'Content-Type': "application/json"}
ETHERSCAN_CONTRACT_CODE_REQUEST = "https://api.etherscan.io/api?module=contract&action=getsourcecode&address={}&apikey={}"
BSCSCAN_CONTRACT_CODE_REQUEST = "https://api.bscscan.com/api?module=contract&action=getsourcecode&address={}&apikey={}"

with open(os.path.expanduser(PARAMETERS_FILE), "r") as f:
    parameters = json.load(f)
with open(os.path.expanduser(parameters["OPENAI_KEY_FILE"]), 'r') as f:
    openai_key = f.read().strip()
with open(os.path.expanduser(parameters["ETHERSCAN_KEY_FILE"]), 'r') as f:
    etherscan_key = f.read().strip()
with open(os.path.expanduser(parameters["BSCSCAN_KEY_FILE"]), 'r') as f:
    bscscan_key = f.read().strip()

good_templates = {}
for good_template_name in parameters["GOOD_TEMPLATE_FILES"]:
    with open(good_template_name, 'r') as f:
        good_templates[good_template_name.split(".")[0]] = f.read()

commentFilter = pyparsing.cppStyleComment.suppress()

tokens = {}; networks = {}
def add_token_to_compare_dict(name, address, network="ETH"):
    tokens[name] = address
    networks[name] = network

add_token_to_compare_dict("KERMIT", "0x7f13cc695185a06f3db199557024acb2f5a4ef95")
add_token_to_compare_dict("ELONGATE", "0xcC6c4F450f1d4aeC71C46f240a6bD50c4E556B8A")
add_token_to_compare_dict("squid_game", "0x561Cf9121E89926c27FA1cfC78dFcC4C422937a4")
add_token_to_compare_dict("DAI", "0x6B175474E89094C44Da98b954EedeAC495271d0F")
add_token_to_compare_dict("METAMOONMARS", "0x8ed9C7E4D8DfE480584CC7EF45742ac302bA27D7", "BSC")
add_token_to_compare_dict("CHUGJUG", "0xb9065690dc79ea0e59b8567ba7823608d5739474")
add_token_to_compare_dict("COTI", "0xDDB3422497E61e13543BeA06989C0789117555c5")
add_token_to_compare_dict("GAMER", "0xf89674f18309a2e97843c6e9b19c07c22caef6d5")
add_token_to_compare_dict("FAKE_CHAINLINK", "0x849569f2e27c4ff3e8c148cb896cd944f28a6f63")
add_token_to_compare_dict("CHAINLINK", "0x514910771AF9Ca656af840dff83E8264EcF986CA")
add_token_to_compare_dict("MEMEAI", "0x6D2d8D9A13A9F5ab4B2CD7ae982C2A22e029500A")
add_token_to_compare_dict("DRAKE", "0x288334E049fB3C7950E56E41FE4F24cDce72290c")
add_token_to_compare_dict("CLOUT", "0xdf72ac9118b3775822e42bedf849d606f8a74b0b")
add_token_to_compare_dict("BBL", "0xe736B7F48D8eD81604CCE36f67E47B38B6d6a6D2")
add_token_to_compare_dict("LINEA", "0x7e7595DB6DF0Df892B412A1839FF1Ac060Cac85A")


json_questions = {
             "is this token fraudlent or high risk of fraud": "boolean",
             "is it honey pot token": "boolean",
             "initial buy tax constant in the code, exactly": "numeric",
             "initial sell tax constant in the code, exactly": "numeric",
             "final buy tax constant in the code, exactly": "numeric",
             "final sell tax constant in the code, exactly": "numeric",
             "reduce buy tax at constant in the code, exactly": "numeric",
             "reduce sell tax at constant in the code, exactly": "numeric",
             "prevent swap before constant in the code, exactly": "numeric",
             "tax levels changing function name from contract code, exactly": "string",
             "suspicious functions name list from contract code": "string",
             "can owner change balance": "boolean",
             "is trading restricted by block Number": "boolean",
             "max wallet size if available else minus one": "numeric",
             "is there white list of addresses": "boolean",
             "is there black list of addresses": "boolean",
             "is it bot protected": "boolean",
             "description of token your expert opinion": "text",
             "non standard features your expert opinion": "text",
             "unusual trade restrictions your expert opinion": "text",
             "function with non standard behaviour your expert opinion": "text",
             }

# p_list = [
# "act as a Solidity programmer",
# "be very conservative",
# "I will provide you with a solidity contract of an erc20 token",
# "focus on the transfer and transferFrom functions and the variable balances",
# "Fill the isMaliciousToken field in the response as true if the code deviate from the standard erc20, especially if transfer or trade are limited by a variable that can be changed by the owner and block.number or if the owner can change the balances map",
# "for isMaliciousToken ignore openTrading as a parameter",
# "return an answer only after I provide the contract",
# "I want you to provide a only a json response in this format",
# ]

# json_request = {
#     "isTaxIncreaseAfterBlockNumber": "boolean",
#     "preventSwapAfterBlockNumber": "number",
#     "isThereTaxIncreaseFunction": "boolean",
#     "isThereMaxWalletSizeDecreaseFunction": "boolean",
#     "isThereMaxTransactionAmountDecreaseFunction": "boolean",
#     "isThereAFunctionWhereTheAdminChangeAnotherUserBalance": "boolean",
#     "isTradingRestrictedByBlockNumber": "boolean",
#     "isWhiteList": "boolean",
#     "isBlackList": "boolean",
#     "isBotProtected": "boolean",
#     "isMaliciousToken": "boolean",
#     }

results_prompt1 = {}
results_prompt2 = {}
results_usage1 = {}
results_usage2 = {}

# %clear
request_token = "KERMIT"
# for request_token in tokens:
if request_token in tokens:
    
    if networks[request_token] == "BSC":
        res = requests.get(BSCSCAN_CONTRACT_CODE_REQUEST.format(tokens[request_token], bscscan_key), headers=HEADERS)
    else:
        res = requests.get(ETHERSCAN_CONTRACT_CODE_REQUEST.format(tokens[request_token], etherscan_key), headers=HEADERS)
    d = res.json()
    source_code = d["result"][0]["SourceCode"]
    
    source_code_filtered = commentFilter.transformString(source_code)
    source_code_list0 = source_code_filtered.split("\n")
    source_code_list1 = sum([s.split("\\n") for s in source_code_list0 if len(s) > 0], [])
    source_code_filtered = "".join([s.strip() + "\n" for s in source_code_list1 if len(s.strip()) > 0])

    # %clear
    messages1 = [
        {"role": "user", "content": "make analysis of the token " + request_token +
              " reading contract code " + source_code_filtered},
        {"role": "user", "content": "possible fraudlent feature: transfering balances is possible for someone else than account owner"},
        {"role": "user", "content": "possible fraudlent feature: unathorized changing or adjusting balances"},
        {"role": "user", "content": "possible fraudlent feature: act in not transparent or obfuscated way"},
        {"role": "user", "content": "fraudlent token features can be implemented in non-standard contract functions"},
        {"role": "user", "content": "high risk of fraud: hidden obstacles for transferring of balances"},
        # {"role": "user", "content": "high risk of fraud: token balance can be transfered only under conditions can hardly be met"},
        {"role": "user", "content": "high risk of fraud: a token has non-standard functions changing or adjusting many balances at once"},
        {"role": "user", "content": "\"tax\" in this context means the fee collected buy a smart contract from token transfers"},
        {"role": "user", "content": "a standard ERC-20 token contract is not fraudlent, focus on non-standard features"},
        {"role": "user", "content": "taxes, a possibility of tax management, and manual function of changing the contract own balance are not fraudlent"},
        {"role": "user", "content": "provide detailed explanation in text fields, " +
              "give all suspicious things"},
        {"role": "user", "content": "respond as json " + json.dumps(json_questions)},
        ]

    # for m in messages1:
        # print(m["content"])

    all_content = ""
    for m in messages1:
        all_content = all_content + "\n" + m["content"]
    messages2 = [{"role": "user", "content": all_content}]

    client = OpenAI(api_key=openai_key)
    completion = client.chat.completions.create(
      model="gpt-3.5-turbo-1106",
      temperature=0,
        response_format={ "type": "json_object" },
      # max_tokens=parameters["MAX_TOKENS"],
      messages=messages2)
   
    print(completion.choices[0].message.content)
    print(completion.usage)

    # results_prompt1[request_token] = completion.choices[0].message.content
    # results_usage1[request_token] = completion.usage


    # messages2 = [{"role": "user", "content":p} for p in p_list]
    # messages2.append({"role": "user", "content":json.dumps(json_request)})
    # messages2.append({"role": "user", "content": "the contract code is" + source_code_filtered})
  
    # client = OpenAI(api_key=openai_key)
    # completion = client.chat.completions.create(
    #   model="gpt-3.5-turbo-1106",
    #   temperature=0,
    #     response_format={ "type": "json_object" },
    #   # max_tokens=parameters["MAX_TOKENS"],
    #   messages=messages2)
    # results_prompt2[request_token] = completion.choices[0].message.content
    # results_usage2[request_token] = completion.usage

    

# print(completion.choices[0].message.content)
# print(completion.usage)


