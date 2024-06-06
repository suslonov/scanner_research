#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from web3 import Web3
from datetime import datetime, timedelta, timezone
from tzlocal import get_localzone_name
import pytz
import pandas as pd

EASTERN = pytz.timezone('US/Eastern'); JERUSALEM = pytz.timezone('Asia/Jerusalem'); UTC = pytz.UTC
LOCAL_TIMEZONE = pytz.timezone(get_localzone_name())
KEY_FILE = '../keys/alchemy.sec'

KNOWN_BUILDERS = {"0xDAFEA492D9c6733ae3d56b7Ed1ADB60692c98Bc5".lower(): "flashbots-builder",
                  "0x95222290DD7278Aa3Ddd389Cc1E1d165CC4BAfe5".lower(): "beaverbuild",
                  "0xbd3Afb0bB76683eCb4225F9DBc91f998713C3b01".lower(): "MEV Builder: 0xbd...b01",
                  "0x388C818CA8B9251b393131C08a736A67ccB19297".lower(): "Lido: Execution Layer Rewards Vault",
                  "0x1f9090aaE28b8a3dCeaDf281B0F12828e676c326".lower(): "rsync-builder",
                  "0x690B9A9E9aa1C9dB991C7721a92d351Db4FaC990".lower(): "builder0x69",
                  "0x4838B106FCe9647Bdf1E7877BF73cE8B0BAD5f97".lower(): "Titan Builder",
                  }
MEV_SEARCHERS = {"0xae2Fc483527B8EF99EB5D9B44875F005ba1FaE13".lower(): "Jared",
                 "0xE58371eCA4ae49Eb9F7De0C41847Ed7177870001".lower(): "31330000",
                 "0xbff30006c162C399dE4C685Ea912995c77Dd0006".lower(): "31330000",
                 "0x7354a5A2663a88fC28FDa71211967Eb001f30005".lower(): "31330000",
                 "0xAc11105D05b8d1117173a232BC8F33d239F50000".lower(): "31330000",
                 "0xe280359aD700115a919AA4f32Fd50Ae018ea0004".lower(): "31330000",
                 "0x295ba1EbccbCF66dA9799Aec81e5e15155770009".lower(): "31330000",
                 "0x590d5E3dC5Dae1d5e6B8eB6cbe9B232c45240003".lower(): "31330000",
                 "0xB49e09760F31e7aF00c69861A10afB414E1C0008".lower(): "31330000",
                 "0xEB82E00788DeE76053495694243C2cb956490002".lower(): "31330000",
                 "0x14f605407E8EF6389Fa222D2337B53479DB10007".lower(): "31330000",
                 } # https://eigenphi.io/mev/ethereum/sandwich

with open(KEY_FILE, 'r') as f:
    k1 = f.readline()
    net_url = k1.strip('\n')
    k2 = f.readline()
    net_wss = k2.strip('\n')

w3 = Web3(Web3.HTTPProvider(net_url))

latest_block = w3.eth.get_block('latest')
latest_block_number = latest_block["number"]
latest_block_timestamp = latest_block["timestamp"]
date_now = datetime.fromtimestamp(latest_block_timestamp, LOCAL_TIMEZONE).astimezone(UTC)

date_end = date_now
n_days = 10  #!!!
date_start = date_now - timedelta(days=n_days) # -  timedelta(hours=4)
block_start_number = latest_block_number - int((date_now - date_start).total_seconds() / 12)

# block_number = 18114160

blocks_casche = {}
blocks = []
t0 = datetime.now()
mev_searchers_set = set()
for block_number in range(block_start_number, latest_block_number + 1):
    if block_number in blocks_casche:
        block = blocks_casche[block_number]
    else:
        block = w3.eth.get_block(block_number, full_transactions=True)
        blocks_casche[block_number] = block
    block_transactions = block["transactions"]
    builder_address = block["miner"].lower()
    mev_searchers = {}
    for transaction in block_transactions:
        transaction_from = transaction["from"].lower()
        if transaction_from in MEV_SEARCHERS:
            if not MEV_SEARCHERS[transaction_from] in mev_searchers:
                mev_searchers[MEV_SEARCHERS[transaction_from]] = 1
                mev_searchers_set.add(MEV_SEARCHERS[transaction_from])
            else:
                mev_searchers[MEV_SEARCHERS[transaction_from]] += 1

    block_analytics = {"Builder": (KNOWN_BUILDERS[builder_address] if builder_address in KNOWN_BUILDERS else "other"),
                       "Hash": block["hash"].hex(),
                       "Builder_address": builder_address,
                       "Block_datetime": datetime.fromtimestamp(block["timestamp"], LOCAL_TIMEZONE).astimezone(UTC),
                       } | mev_searchers

    blocks.append(block_analytics)
    print(block_number, latest_block_number - block_number)
print(datetime.now() - t0, (datetime.now() - t0).total_seconds() / (len(blocks)))

mev_searchers_list = list(mev_searchers_set)

df_blocks = pd.DataFrame.from_records(blocks)
df_table = df_blocks.groupby(["Builder"])["Builder"].count().to_frame()
df_table.rename(columns={"Builder": "Count"}, inplace=True)
df_table["Share"] = df_table["Count"] / df_blocks["Hash"].count()
df_table[mev_searchers_list] = df_blocks.groupby(["Builder"])[mev_searchers_list].count()
df_table["Jared_share"] = df_table["Jared"] / df_table["Count"]

print(df_table)

df_table1 = df_blocks.groupby(["Builder_address"])["Builder_address"].count().to_frame()
df_table1.rename(columns={"Builder_address": "Count"}, inplace=True)
df_table1["Share"] = df_table1["Count"] / df_blocks["Hash"].count()
df_table1.sort_values(["Share"], inplace=True, ascending=False)

df_table1[mev_searchers_list] = df_blocks.groupby(["Builder_address"])[mev_searchers_list].count()
df_table1["Jared_share"] = df_table1["Jared"] / df_table1["Count"]

print(df_table)
