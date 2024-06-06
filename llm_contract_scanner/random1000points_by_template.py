#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
os.sys.path.append(os.path.dirname(os.path.abspath('.')))
import json
import pandas as pd
import numpy as np
from openai import OpenAI
import pyparsing

from db.bot_db import DBMySQL
from db.remote import open_remote_port, close_remote_port, RemoteServer

PARAMETERS_FILE = "~/git/scanner_research/llm_contract_scanner/parameters.json"
CSV_FILE = '/media/Data/csv/sniping_test_'

with open(os.path.expanduser(PARAMETERS_FILE), "r") as f:
    parameters = json.load(f)
with open(os.path.expanduser(parameters["OPENAI_KEY_FILE"]), 'r') as f:
    openai_key = f.read().strip()

good_templates = {}
for good_template_name in parameters["GOOD_TEMPLATE_FILES"]:
    with open(good_template_name, 'r') as f:
        good_templates[good_template_name.split(".")[0]] = f.read()

commentFilter = pyparsing.cppStyleComment.suppress()

if "DB_SERVER" in parameters and parameters["DB_SERVER"] != "":
    REMOTE = parameters["DB_SERVER"]
else:
    REMOTE = None

    
file_name = CSV_FILE + "270000_280000_0.05.csv"
df_results = pd.read_csv(file_name, index_col=0)
df_results["distance_from_template1"] = np.nan

to_check = list(df_results.index)
np.random.shuffle(to_check)

ii = 0
for t in to_check[:1000]:
    print(ii); ii += 1
    try:
        with RemoteServer(remote=REMOTE) as server:
            with DBMySQL(port=server.local_bind_port) as db:
                token, source_code, _ = db.get_contract_code(t)
        
        template_token = list(good_templates.keys())[0]   #!!! only one
        source_code_template = good_templates[template_token]
        
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
            {"role": "user", "content": "compare two following token contracts"},
            {"role": "user", "content": "GOODTOKEN" + " token is " + source_code_template},
            {"role": "user", "content": "NEWTOKEN" + " token is " + source_code},
            {"role": "user", "content": "please give the measure of likeness from 0 to 100 based on the code similarity, ignore names and constant values"},
            {"role": "user", "content": "give report on differences as detailed as possible, put all to JSON fields"},
            {"role": "user", "content": "add values of Solidity variables related to the buying tax and the selling tax from the contract code of " + "NEWTOKEN"},
            {"role": "user", "content": "add to the JSON the name only of a function changing any of these variables after the contract creation if it exists in the contract code of " + "NEWTOKEN"},
            {"role": "user", "content": "a malicious token typically tries to transfer my tokens to the contract owner wallet using malicious transfer, transferFrom or approve function"},
            {"role": "user", "content": "also malicious token can set fee to be a lion's share of approved tokens in the approve function. Is " + "NEWTOKEN" + " token malicious?"},
            {"role": "user", "content": 'fields for JSON shoud be: "tokenLikeness", "descriptionOfToken" - for ' + "NEWTOKEN" + ', "majorDifferences", "nonStandardFeatures", "initialBuyTax", "initialSellTax", "finalBuyTax", "finalSellTax", "reduceBuyTaxAt", "reduceSellTaxAt", "preventSwapBefore", "taxChangingFunction", "maliciousToken"'},
          ]
        )
        
        df_results.loc[t, "distance_from_template1"] = json.loads(completion.choices[0].message.content)["tokenLikeness"]
    except:
        pass

# df_results.to_csv(file_name)

# df_results["sum_squares"] = np.nan
# df_results.loc[df_results["distance_from_template1"] == 60, "distance_from_template1"]