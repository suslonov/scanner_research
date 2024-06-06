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


template_token = list(good_templates.keys())[0]   #!!! only one
source_code_template = good_templates[template_token]

request_token = "LINEA"

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

json_list = {"initial buy tax constant in the code": "number",
             "initial sell tax constant in the code": "number",
             "final buy tax constant in the code": "number",
             "final sell tax constant in the code": "number",
             "reduce buy tax at constant in the code": "number",
             "reduce sell tax at constant in the code": "number",
             "prevent swap before constant in the code": "number",
             "tax levels changing function name from contract code": "string",
             "can owner change balance your opinion": "boolean",
             "is trading restricted by block Number": "boolean",
             "max wallet size if available else minus one": "number",
             "is there white list": "boolean",
             "is there black list": "boolean",
             "is it bot protected": "boolean",
             # "grade": "number",
             "is malicious token your opinion": "boolean",
             "is honey pot token your opinion": "boolean",
             "description of token your expert opinion": "text",
             "non standard features your expert opinion": "text",
             "unusual trade restrictions your expert opinion": "text",
             "function with non standard behaviour your expert opinion": "text",
             }


messages=[
    {"role": "user", "content": "compare two following token contracts"},
    {"role": "user", "content": template_token + " template token is " + source_code_template},
    {"role": "user", "content": request_token + " request token is " + source_code_filtered},
    {"role": "user", "content": "give the measure of likeness from 0 to 1000 based on the code similarity and functionality, ignore names and constant values"},
    {"role": "user", "content": "similar tokens should have the measure of likeness near 1000, very different tokens near 0"},
    {"role": "user", "content": "give report on differences as detailed as possible, put all to JSON fields"},
    {"role": "user", "content": "a malicious token typically tries to transfer my tokens to the contract owner wallet using malicious transfer, transferFrom or approve function"},
    {"role": "user", "content": "also malicious token can have ownable non-standard functions changing balances or chechs preventing transfering purchased tokens"},
    {"role": "user", "content": "also malicious token can set fee to be a lion's share of approved tokens in the approve function. Is " + request_token + " token malicious?"},
    {"role": "user", "content": "give descriptions, differences and non-standard features for the request token only"},
    {"role": "user", "content": "add values of Solidity variables related to the buying tax and the selling tax only from the contract code of " + request_token},
    {"role": "user", "content": "add to the JSON the name only of a function changing any of these variables after the contract creation if it exists in the contract code of " + request_token},
    {"role": "user", "content": 'fields for JSON shoud be: ' + json.dumps(json_list)},
    ]

############### compare tokens
client = OpenAI(api_key=openai_key)
completion = client.chat.completions.create(
  model="gpt-3.5-turbo-1106",
  temperature=0,
  response_format={ "type": "json_object" },
  # max_tokens=parameters["MAX_TOKENS"],
  messages=messages
)

print(completion.choices[0].message.content)
print(completion.usage)



############### check one token

# messages=[
#     {"role": "user", "content": "make analysis of the token " + request_token +
#          " reading contract code as you are a Solidity programmer" + source_code_filtered},
#     {"role": "user", "content": "possible malicious features are: unauthorized transfer of tokens or reduce token balances, "
#          "preventing transfering purchased tokens, unreasonable limitations of token transfer"},
#     {"role": "user", "content": "respond as json " + json.dumps(json_list)},
#     {"role": "user", "content": "\"tax\" in this context means the fee collected buy a smart contract from token transfers"},
#     {"role": "user", "content": "make your best conservative opinion on questions in json fiels, " +
#          "always suppose the worst "},
#     {"role": "user", "content": "provide detailed explanation in text fields, " +
#          "give all suspicious things"},
#     ]

# client = OpenAI(api_key=openai_key)
# completion = client.chat.completions.create(
#   model="gpt-3.5-turbo-1106",
#   temperature=0.2,
#     response_format={ "type": "json_object" },
#   # max_tokens=parameters["MAX_TOKENS"],
#   messages=messages
# )

# print(completion.choices[0].message.content)
# print(completion.usage)

# # for m in messages:
# #     print(m["content"])







# #### 4.0 -> 3.5 prompt

# p1 = 'I have an ERC-20 token contract written in Solidity that I suspect may contain potential security risks or malicious features. I am particularly concerned about aspects that could be abused by the contract owner, traps hidden in the contract, unauthorized token transfers, and unannounced taxes or fees on transactions. Could you help analyze the contract with these focus areas in mind?'
# p2 = 'Owner Abuses: Are there any functions or mechanisms in the contract that grant the owner excessive control or privileges, especially regarding altering token supply, changing critical contract parameters, or freezing/unfreezing user assets?'
# p3 = 'Hidden Traps: Does the contract contain any hidden traps or functions that could be activated under certain conditions to the detriment of token holders, such as functions that could lock or redirect funds without clear consent?'
# p4 = 'Transfer Function Stealing: Can you identify any vulnerabilities in the transfer or transferFrom functions that would allow unauthorized access to user funds, such as reentrancy attacks or logic flaws that enable token stealing?'
# p5 = 'Token "Taxes": Are there any undisclosed fees or "taxes" applied to transactions? Look for modifications in the transfer and transferFrom functions that divert a percentage of the transaction to another address or modify the token amount sent versus received.'
# p6 = 'Please provide a detailed analysis of these concerns, highlighting specific lines of code or patterns that could indicate malicious intent or design flaws'


# messages=[
#     {"role": "user", "content": p1},
#     {"role": "user", "content": "token "+ request_token +" contract code is " + source_code_filtered},
#     {"role": "user", "content": p2},
#     {"role": "user", "content": p3},
#     {"role": "user", "content": p4},
#     {"role": "user", "content": p5},
#     {"role": "user", "content": p6},
#     ]


# client = OpenAI(api_key=openai_key)
# completion = client.chat.completions.create(
#   model="gpt-3.5-turbo-1106",
#   temperature=0.2,
#     # response_format={ "type": "json_object" },
#   # max_tokens=parameters["MAX_TOKENS"],
#   messages=messages
# )

# print(completion.choices[0].message.content)
# print(completion.usage)

