import os
import json
import requests
from openai import OpenAI
import pyparsing
# from openai.embeddings_utils import distances_from_embeddings

PARAMETERS_FILE = "~/git/scanner_research/llm_contract_scanner/parameters.json"
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
add_token_to_compare_dict("cyb3rgam3r420_Gamer", "0xf89674f18309a2e97843c6e9b19c07c22caef6d5")
add_token_to_compare_dict("FAKE_CHAINLINK", "0x849569f2e27c4ff3e8c148cb896cd944f28a6f63")
add_token_to_compare_dict("CHAINLINK", "0x514910771AF9Ca656af840dff83E8264EcF986CA")
add_token_to_compare_dict("MEMEAI", "0x6D2d8D9A13A9F5ab4B2CD7ae982C2A22e029500A")
add_token_to_compare_dict("DRAKE", "0x288334E049fB3C7950E56E41FE4F24cDce72290c")

request_token = "KERMIT"

if networks[request_token] == "BSC":
    res = requests.get(BSCSCAN_CONTRACT_CODE_REQUEST.format(tokens[request_token], bscscan_key), headers=HEADERS)
else:
    res = requests.get(ETHERSCAN_CONTRACT_CODE_REQUEST.format(tokens[request_token], etherscan_key), headers=HEADERS)
d = res.json()
source_code = d["result"][0]["SourceCode"]

commentFilter = pyparsing.cppStyleComment.suppress()
source_code_filtered = commentFilter.transformString(source_code)
source_code_list0 = source_code_filtered.split("\n")
source_code_list1 = sum([s.split("\\n") for s in source_code_list0 if len(s) > 0], [])
source_code_filtered = "".join([s.strip() + "\n" for s in source_code_list1 if len(s.strip()) > 0])


client = OpenAI(api_key=openai_key)
completion = client.chat.completions.create(
  model="gpt-3.5-turbo-1106",
  temperature=0,
  response_format={ "type": "json_object" },
  # max_tokens=parameters["MAX_TOKENS"],
  messages=[
    {"role": "user", "content": source_code_filtered},
    # {"role": "user", "content": "make a detailed resume of this smart contract according to what you know about Solidity contracts"},
    {"role": "user", "content": "and make me JSON with all the names and values of Solidity variables related to the buying tax and the selling tax from the contract code given"},
    {"role": "user", "content": "add to the JSON the name only of a function changing any of these variables after the contract creation if it exists in the contract code given"},
    {"role": "user", "content": "a malicious token typically tries to transfer my tokens to the contract owner wallet using malicious transfer, transferFrom or approve function"},
    {"role": "user", "content": "also malicious token can set fee to be a lion's share of approved tokens in the approve function. Is this token malicious?"},
    # {"role": "user", "content": '"gradeToken" field evaluates is the token OK to buy as an floating value from 0 to 100'},
    {"role": "user", "content": 'fields for JSON shoud be: "initialBuyTax", "initialSellTax", "finalBuyTax", "finalSellTax", "reduceBuyTaxAt", "reduceSellTaxAt", "preventSwapBefore", "taxChangingFunction", "maliciousToken", "descriptionOfToken"'},
  ]
)

print(completion.choices[0].message.content)
print(completion.usage)




# completion = client.chat.completions.create(
#   model="gpt-3.5-turbo",
#   # max_tokens=parameters["MAX_TOKENS"],
#   messages=[
#     {"role": "user", "content": "JSON fields in the answer should be named: AAA for the question, BBB for the answer"},
#     {"role": "user", "content": "please make me JSON for the question who are you and answer I am a monkey"}
#   ]
# )

