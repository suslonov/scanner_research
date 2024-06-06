#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# https://thegraph.com/hosted-service/subgraph/uniswap/uniswap-v3
# https://github.com/Uniswap/v3-subgraph/blob/main/schema.graphql

import requests
import pandas as pd
import numpy as np
from math import sqrt, isqrt

def list_to_dict(l, field):
    return {v[field]: v for v in l if field in v}

HEADERS = {'Content-Type': "application/json", 'accept': '*/*'}
URL = "https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v3"

POOL = "0xAD9eF19e289dcbC9AB27b83D2dF53CDEFF60f02D".lower()


POOLS_QUERY_STR = '{pools(where: {id: "' + POOL + '"}){id createdAtTimestamp createdAtBlockNumber token0 {id} token1 {id} feeTier'
POOLS_QUERY_STR += " liquidity sqrtPrice feeGrowthGlobal0X128 feeGrowthGlobal1X128 token0Price token1Price"
POOLS_QUERY_STR += " tick observationIndex volumeToken0 volumeToken1 volumeUSD untrackedVolumeUSD feesUSD"
POOLS_QUERY_STR += " txCount collectedFeesToken0 collectedFeesToken1 collectedFeesUSD totalValueLockedToken0"
POOLS_QUERY_STR += " totalValueLockedToken1 totalValueLockedETH totalValueLockedUSD totalValueLockedUSDUntracked}}"
POOLS_QUERY = {"query": POOLS_QUERY_STR}

TICKS_QUERY_STR = '{ticks(where: {poolAddress: "' + POOL + '"}){id tickIdx liquidityGross liquidityNet price0'
TICKS_QUERY_STR += " price1 volumeToken0 volumeToken1 volumeUSD untrackedVolumeUSD feesUSD collectedFeesToken0"
TICKS_QUERY_STR += " collectedFeesToken1 collectedFeesUSD createdAtTimestamp createdAtBlockNumber"
TICKS_QUERY_STR += " feeGrowthOutside0X128 feeGrowthOutside1X128}}"
TICKS_QUERY = {"query": TICKS_QUERY_STR}


res = requests.post(URL, json=POOLS_QUERY, headers=HEADERS)
d = res.json()
pool = d["data"]["pools"][0]
price = int(pool["sqrtPrice"])**2 >> 192


res = requests.post(URL, json=TICKS_QUERY, headers=HEADERS)
d = res.json()
ticks_dict = list_to_dict(d["data"]["ticks"], "tickIdx")

for t in ticks_dict:
    print(t, ticks_dict[t]["liquidityGross"])




L = int(pool["liquidity"])
sqrtP = int(pool["sqrtPrice"]) >> 96
fee = int(pool["feeTier"])

dx = int(0.001 * 1e18)
dx_fee = int(dx * (1e6 - fee) / 1e6)

sqrtP2 = sqrtP + dx_fee / L

# dy = L * (1/sqrtP2 - 1/sqrtP)

dy = L * sqrtP * L//(dx_fee*sqrtP + L) - L * sqrtP

print(dy/1e18)
