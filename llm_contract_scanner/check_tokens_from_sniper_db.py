#!/usr/bin/env python3
# -*- coding: utf-8 -*-

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
os.sys.path.append(os.path.dirname(os.path.abspath('.')))
from datetime import datetime
import json
import MySQLdb
import pandas as pd
import numpy as np
from db.bot_db import DBMySQL
from db.remote import open_remote_port, close_remote_port, RemoteServer
from openai import OpenAI
import pyparsing


PARAMETERS_FILE = "~/git/scanner_research/parameters.json"
CSV_FILE = '/media/Data/csv/'
WETH = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"

with open(os.path.expanduser(PARAMETERS_FILE), "r") as f:
    parameters = json.load(f)
with open(os.path.expanduser(parameters["OPENAI_KEY_FILE"]), 'r') as f:
    openai_key = f.read().strip()

if "DB_SERVER" in parameters and parameters["DB_SERVER"] != "":
    REMOTE = parameters["DB_SERVER"]
else:
    REMOTE = None

def fetch_with_description(cursor):
    return [{n[0]: v for n, v in zip(cursor.description, row)} for row in cursor.fetchall()]

def fetch_to_df(cursor, table_name, index_list):
    sql = "select * from " + table_name
    cursor.execute(sql)
    res = fetch_with_description(cursor)
    return pd.DataFrame.from_records(res, index = index_list)

def get_pair_range_history(pair_id_start, pair_id_end):
    if pair_id_end == pair_id_start:
        pair_id_end = pair_id_start + 1
    with RemoteServer(remote=REMOTE) as server:
        with DBMySQL(port=server.local_bind_port) as db:
            return db.get_event_history_many(pair_id_start, pair_id_end)
        
def get_pairs_with_contracts(pair_id_start=None, pair_id_end=None):
    with RemoteServer(remote=REMOTE) as server:
        with DBMySQL(port=server.local_bind_port) as db:
            return db.get_pairs_with_contracts(pair_id_start, pair_id_end, [WETH])

def get_pairs():
    with RemoteServer(remote=REMOTE) as server:
        with DBMySQL(port=server.local_bind_port) as db:
            return db.get_pairs()

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

KEY_FILE = '../keys/aws_db.sec'
with open(KEY_FILE, 'r') as f:
    users = json.load(f)

commentFilter = pyparsing.cppStyleComment.suppress()

db_host = users["bot_db1"]["ip"]
db_user = users["bot_db1"]["user"]
db_passwd = users["bot_db1"]["password"]
db_name = "sniper_v1"

db_connection = MySQLdb.connect(host=db_host, user=db_user, passwd=db_passwd, db=db_name)
cursor = db_connection.cursor()

# sql = "show tables"
# cursor.execute(sql)
# q = cursor.fetchall()
# print(q)

df_buys_sniper = fetch_to_df(cursor, "buys", ["id"])
df_pairs_sniper = fetch_to_df(cursor, "pairs", ["id"])
df_tokens_sniper = fetch_to_df(cursor, "tokens", ["id"])

WETH_id = df_tokens_sniper.loc[df_tokens_sniper["address"] == WETH].index[0]
df_pairs_bought = df_pairs_sniper.loc[df_buys_sniper["pairId"].unique()]

token_list = list(df_pairs_bought.loc[df_pairs_bought["token0Id"] == WETH_id, "token1Id"].values)
token_list.extend(list(df_pairs_bought.loc[df_pairs_bought["token1Id"] == WETH_id, "token0Id"].values))
token_list = list(df_tokens_sniper.loc[token_list, "address"].values)

# pairs = get_pairs_with_contracts(308000, 400000)
pairs = get_pairs()
df_pairs = pd.DataFrame.from_records(pairs, index=["pair_id"])
price_history = get_pair_range_history(308000, 400000)
df_price_history = pd.DataFrame.from_records(price_history, index=["event_id"])

tokens = {}
contracts = {}
with RemoteServer(remote=REMOTE) as server:
    with DBMySQL(port=server.local_bind_port) as db:
        for token in token_list:
            # token = token_list[-2]
            q = db.get_token(token)
            qq = db.get_contract_code(token)
            tokens[token] = {"name": q[0]["token_name"], "symbol": q[0]["token_symbol"], "decimals": q[0]["decimals"]}
            contracts[token] = qq[1]

for token in token_list:
    pair_id = df_pairs.loc[df_pairs["token"]==token].index[0]
    is_token0 = df_pairs.loc[pair_id, "token0"] == token
    df_price_history_pair = df_price_history.loc[df_price_history["pair_id"] == pair_id]
    start_block = df_price_history_pair["block_number"].min()
    block_burn = df_price_history_pair.loc[df_price_history_pair["operation"]=="burn", "block_number"].min()
    if np.isnan(block_burn):
        block_burn = None
    else:
        block_burn = block_burn - start_block
    if is_token0:
        sells_count = df_price_history_pair.loc[(df_price_history_pair["operation"]=="swap V2") &
                                                (df_price_history_pair["amount1Out"]!= 0), "pair_id"].count()
        if sells_count:
            df_buy_token_first = df_price_history_pair.loc[(df_price_history_pair["operation"]=="swap V2") &
                                                (df_price_history_pair["amount0Out"]!= 0)].groupby(["sender"])["block_number"].min().to_frame()
            df_buy_sell_last = df_price_history_pair.loc[(df_price_history_pair["operation"]=="swap V2") &
                                                (df_price_history_pair["amount1Out"]!= 0)].groupby(["sender"])["block_number"].max().to_frame()
            df_buy_token_first.rename(columns={"block_number": "buy"}, inplace=True)
            df_buy_sell_last.rename(columns={"block_number": "sell"}, inplace=True)
            df_buy_token_first["sell"] = np.nan
            df_buy_token_first.update(df_buy_sell_last)
            max_term = (df_buy_token_first["sell"] - df_buy_token_first["buy"]).max()
        else:
            max_term = None
    else:
        sells_count = df_price_history_pair.loc[(df_price_history_pair["operation"]=="swap V2") &
                                                (df_price_history_pair["amount0Out"]!= 0), "pair_id"].count()
        if sells_count:
            df_buy_token_first = df_price_history_pair.loc[(df_price_history_pair["operation"]=="swap V2") &
                                                (df_price_history_pair["amount1Out"]!= 0)].groupby(["sender"])["block_number"].min().to_frame()
            df_buy_sell_last = df_price_history_pair.loc[(df_price_history_pair["operation"]=="swap V2") &
                                                (df_price_history_pair["amount0Out"]!= 0)].groupby(["sender"])["block_number"].max().to_frame()
            df_buy_token_first.rename(columns={"block_number": "buy"}, inplace=True)
            df_buy_sell_last.rename(columns={"block_number": "sell"}, inplace=True)
            df_buy_token_first["sell"] = np.nan
            df_buy_token_first.update(df_buy_sell_last)
            max_term = (df_buy_token_first["sell"] - df_buy_token_first["buy"]).max()
        else:
            max_term = None

    tokens[token]["block_burn"] = block_burn
    tokens[token]["sells_count"] = sells_count
    tokens[token]["max_term"] = max_term


for token in token_list:
    if not tokens[token]["block_burn"] is None:
        tokens[token]["block_burn"] = int(tokens[token]["block_burn"])
    tokens[token]["sells_count"] = int(tokens[token]["sells_count"])
    if not tokens[token]["max_term"] and not pd.isnull(tokens[token]["max_term"]) :
        tokens[token]["max_term"] = int(tokens[token]["max_term"])


# with RemoteServer(remote=REMOTE) as server:
#     with DBMySQL(port=server.local_bind_port) as db:
#         for token in token_list:
#             qq = db.get_contract_code(token)
#             contracts[token] = qq[1]


for token in token_list:
    if contracts[token] is None:
        tokens[token]["contract"] = False
        continue
    source_code_filtered = commentFilter.transformString(contracts[token])
    source_code_list0 = source_code_filtered.split("\n")
    source_code_list1 = sum([s.split("\\n") for s in source_code_list0 if len(s) > 0], [])
    source_code_filtered = "".join([s.strip() + "\n" for s in source_code_list1 if len(s.strip()) > 0])

    messages0 = [
        {"role": "user", "content": "make analysis of the token reading contract code " + source_code_filtered},
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

    all_content = ""
    for m in messages0:
        all_content = all_content + "\n" + m["content"]
    messages = [{"role": "user", "content": all_content}]
    
    client = OpenAI(api_key=openai_key)
    completion = client.chat.completions.create(
      model="gpt-3.5-turbo-1106",
      temperature=0,
        response_format={ "type": "json_object" },
      # max_tokens=parameters["MAX_TOKENS"],
      messages=messages)

    tokens[token]["contract"] = True
    tokens[token].update(json.loads(completion.choices[0].message.content))
    tokens[token].update(dict(completion.usage))

tokens_to_export = []
for token in token_list:
    if tokens[token]["contract"]:
        tokens_to_export.append({"token": token,
                             "symbol": tokens[token]["symbol"],
                             "block_burn": tokens[token]["block_burn"],
                             "sells_count": tokens[token]["sells_count"],
                             "max_term": tokens[token]["max_term"],
                             "contract": tokens[token]["contract"],
                             "canOwnerChangeBalance": tokens[token]["can owner change balance"],
                             "isTradingRestrictedByBlockNumber": tokens[token]["is trading restricted by block Number"],
                             "isMaliciousToken": tokens[token]["is this token fraudlent or high risk of fraud"],
                             })
    else:
        tokens_to_export.append({"token": token,
                             "symbol": tokens[token]["symbol"],
                             "block_burn": tokens[token]["block_burn"],
                             "sells_count": tokens[token]["sells_count"],
                             "max_term": tokens[token]["max_term"],
                             "contract": tokens[token]["contract"],
                             })

        

df_tokens_to_export = pd.DataFrame.from_records(tokens_to_export, index=["token"])
df_tokens_to_export.to_csv(CSV_FILE + str(len(tokens)) + "tokens_gpt_analysis.csv")

with open(CSV_FILE + str(len(tokens)) + "tokens_gpt_analysis.json", "w") as f:
    json.dump(tokens, f, indent=4)

