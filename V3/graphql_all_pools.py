#!/usr/bin/env python3
# -*- coding: utf-8 -*-


import requests
import pandas as pd
import numpy as np
from math import sqrt, isqrt

def list_to_dict(l, field):
    return {v[field]: v for v in l if field in v}

HEADERS = {'Content-Type': "application/json", 'accept': '*/*'}
URL = "https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v3"

all_pools = []

POOLS_QUERY_STR = '{pools(first:1000, orderBy: id, orderDirection: asc){id createdAtTimestamp createdAtBlockNumber token0 {id} token1 {id} feeTier'
POOLS_QUERY_STR += " liquidity sqrtPrice feeGrowthGlobal0X128 feeGrowthGlobal1X128 token0Price token1Price"
POOLS_QUERY_STR += " tick observationIndex volumeToken0 volumeToken1 volumeUSD untrackedVolumeUSD feesUSD"
POOLS_QUERY_STR += " txCount collectedFeesToken0 collectedFeesToken1 collectedFeesUSD totalValueLockedToken0"
POOLS_QUERY_STR += ' totalValueLockedToken1 totalValueLockedETH totalValueLockedUSD totalValueLockedUSDUntracked}}'
POOLS_QUERY = {"query": POOLS_QUERY_STR}
res = requests.post(URL, json=POOLS_QUERY, headers=HEADERS)
d = res.json()
pools = d["data"]["pools"]
pool_id = pools[-1]["id"]
all_pools.extend(pools)

while True:
    POOLS_QUERY_STR = '{pools(first:1000, orderBy: id, orderDirection: asc, where:{id_gt: "' + pool_id + '"})'
    POOLS_QUERY_STR += "{id createdAtTimestamp createdAtBlockNumber token0 {id} token1 {id} feeTier"
    POOLS_QUERY_STR += " liquidity sqrtPrice feeGrowthGlobal0X128 feeGrowthGlobal1X128 token0Price token1Price"
    POOLS_QUERY_STR += " tick observationIndex volumeToken0 volumeToken1 volumeUSD untrackedVolumeUSD feesUSD"
    POOLS_QUERY_STR += " txCount collectedFeesToken0 collectedFeesToken1 collectedFeesUSD totalValueLockedToken0"
    POOLS_QUERY_STR += ' totalValueLockedToken1 totalValueLockedETH totalValueLockedUSD totalValueLockedUSDUntracked}}'
    POOLS_QUERY = {"query": POOLS_QUERY_STR}
    res = requests.post(URL, json=POOLS_QUERY, headers=HEADERS)
    d = res.json()
    pools = d["data"]["pools"]
    if len(pools) == 0:
        break
    pool_id = pools[-1]["id"]
    all_pools.extend(pools)
    print(len(pools), len(all_pools))

df = pd.DataFrame.from_records(all_pools)
df.to_csv('/media/Data/csv/V3_pools.csv')

