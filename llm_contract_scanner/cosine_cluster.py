#!/usr/bin/env python3
# -*- coding: utf-8 -*-

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
os.sys.path.append(os.path.dirname(os.path.abspath('.')))
import json
import pandas as pd
import numpy as np

import pyparsing
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.cluster import AgglomerativeClustering, KMeans

from db.bot_db import DBMySQL
from db.remote import open_remote_port, close_remote_port, RemoteServer
from _utils.uniswap import amount_out_v2

PARAMETERS_FILE = "~/git/scanner_research/parameters.json"
CSV_FILE = '/media/Data/csv/sniping_test_'
# CSV_FILE = '/home/anton/tmp/sniping_test_'
SHORT_TERM_BLOCKS_LIMIT = 100
MAX_POSITION_ETH = 0.1
TAKE_PROFITS = [0.1, 0.5, 1, 2, 3]
MAX_POSITION_TERM = 100

with open(os.path.expanduser(PARAMETERS_FILE), "r") as f:
    parameters = json.load(f)

if "DB_SERVER" in parameters and parameters["DB_SERVER"] != "":
    REMOTE = parameters["DB_SERVER"]
else:
    REMOTE = None


def get_pairs_with_contracts(pair_id_start=None, pair_id_end=None):
    with RemoteServer(remote=REMOTE) as server:
        with DBMySQL(port=server.local_bind_port) as db:
            return db.get_pairs_with_contracts(pair_id_start, pair_id_end)

def get_pair_range_history(pair_id_start, pair_id_end):
    if pair_id_end == pair_id_start:
        pair_id_end = pair_id_start + 1
    with RemoteServer(remote=REMOTE) as server:
        with DBMySQL(port=server.local_bind_port) as db:
            return db.get_event_history_many(pair_id_start, pair_id_end)

def save_results_to_db(analytics):
    with RemoteServer(remote=REMOTE) as server:
        with DBMySQL(port=server.local_bind_port) as db:
            for i in analytics:
                # try:
                    db.update_json("t_pairs", i, "price_analytics", 
                                   {a: None if pd.isnull(analytics[i][1][a]) else analytics[i][1][a] for a in analytics[i][1]})
                # except:
                #     print(i, analytics[i][1])

def main():
    commentFilter = pyparsing.cppStyleComment.suppress()
    tfidf_vectorizer = TfidfVectorizer()

    contracts = {}
    source_codes_filtered = {}

    start_pair = 270000
    end_pair = 280000
    pair_id = start_pair

    pairs = get_pairs_with_contracts(start_pair, end_pair)
    df_pairs = pd.DataFrame.from_records(pairs, index=["pair_id"])

    with RemoteServer(remote=REMOTE) as server:
        with DBMySQL(port=server.local_bind_port) as db:
            for pair_id in df_pairs.index:
                token = df_pairs.loc[pair_id, "token"]
                contracts[token] = db.get_contract_code(token)[1]

    for t in contracts:
        source_code_filtered = commentFilter.transformString(contracts[t])
        source_code_list = source_code_filtered.split("\n")
        source_code_list = sum([s.split("\\n") for s in source_code_list if len(s) > 0], [])
        source_code_filtered = "".join([s.strip() + "\n" for s in source_code_list if len(s.strip()) > 0])
        source_codes_filtered[t] = source_code_filtered

    token_list = []
    source_codes_filtered_list = []
    for t in source_codes_filtered:
        token_list.append(t)
        source_codes_filtered_list.append(source_codes_filtered[t])

    tfidf_matrix = tfidf_vectorizer.fit_transform(source_codes_filtered_list)
   
    clustering = AgglomerativeClustering(n_clusters=10, metric="cosine", linkage="average", compute_distances=True)
    clustering.fit(tfidf_matrix.toarray())
    distances = clustering.distances_
    labels = clustering.labels_
    children = clustering.children_

    counts = np.zeros(clustering.children_.shape[0])
    n_samples = len(clustering.labels_)
    for i, merge in enumerate(clustering.children_):
        current_count = 0
        for child_idx in merge:
            if child_idx < n_samples:
                current_count += 1  # leaf node
            else:
                current_count += counts[child_idx - n_samples]
        counts[i] = current_count

    linkage_matrix = np.column_stack([clustering.children_, clustering.distances_, counts])

    children[-10:, :]
    linkage_matrix[-10:, :]
    np.column_stack([clustering.children_, counts]).astype(int)[-30:]

    # df_pairs.iloc[1605]
    # token_list[5734]

    # from sklearn.neighbors import NearestCentroid
    # clf = NearestCentroid()
    # clf.fit(tfidf_matrix, clustering.labels_)
    # centroids = clf.centroids_


    from matplotlib import pyplot as plt
    from scipy.cluster.hierarchy import dendrogram
    _ = dendrogram(linkage_matrix.astype(float))
    plt.xlabel("index data")
    plt.show()


    clustering1 = KMeans(n_clusters=10)
    clustering1.fit(tfidf_matrix)
    centers = clustering1.cluster_centers_
    labels = clustering1.labels_
    print(np.unique(labels, return_counts=True))

    from scipy.cluster.vq import vq

    closest, distances = vq(centers, tfidf_matrix.toarray())

