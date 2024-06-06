#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from datetime import datetime
import numpy as np
import decimal
import pickle
import zlib
import json
import MySQLdb

class DBMySQL(object):
    db_host="127.0.0.1"
    db_user="sniper"
    db_passwd="sniper"
    db_name="sniper"

    def fetch_with_description(self, cursor):
        return [{n[0]: v for n, v in zip(cursor.description, row)} for row in cursor.fetchall()]

    def fetch_to_dict(self, cursor, id_field):
        temp_list = [{n[0]: v for n, v in zip(cursor.description, row)} for row in cursor.fetchall()]
        return {t[id_field]: t for t in temp_list}

    def descriptions(self, cursor):
        return [n[0] for n in cursor.description]

    def __init__(self, port=None):
        self.port = port

    def start(self):
        if self.port:
            self.db_connection = MySQLdb.connect(host=self.db_host, user=self.db_user, passwd=self.db_passwd, db=self.db_name, port=self.port)
        else:
            self.db_connection = MySQLdb.connect(host=self.db_host, user=self.db_user, passwd=self.db_passwd, db=self.db_name)
        self.cursor = self.db_connection.cursor()

    def commit(self):
        self.db_connection.commit()

    def stop(self):
        self.db_connection.commit()
        self.db_connection.close()

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.stop()
        
    def exec_sql(self, s):
        return self.cursor.execute(s)

    def exec_sql_plain_list(self, s):
        self.cursor.execute(s)
        return self.descriptions(self.cursor), self.cursor.fetchall()

    def exec_sql_dict_list(self, s):
        self.cursor.execute(s)
        return self.fetch_with_description(self.cursor)

    def _create_table(self, sql_drop, *sql):
            try:
                self.cursor.execute(sql_drop)
            except:
                pass
            for s in sql:
                self.cursor.execute(s)
        
    def create_tables(self, tables):
        if "t_pairs2" in tables:
            s1 = "DROP TABLE t_pairs2"
            s2 = "CREATE TABLE t_pairs2 (pair_id INT NOT NULL PRIMARY KEY, "
            s2 += "pair VARCHAR(256), token0 VARCHAR(256), token1 VARCHAR(256), token VARCHAR(256), token_name VARCHAR(256), token_symbol VARCHAR(256), decimals int, "
            s2 += "swaps int, first_block_number int, price_analytics JSON, chat_gpt JSON, competitors JSON)"
            s2 += " DATA DIRECTORY = '/media/data/mysql'"
            self._create_table(s1, s2)

        if "t_tokens2" in tables:
            s1 = "DROP TABLE t_tokens2"
            s2 = "CREATE TABLE t_tokens2 (token VARCHAR(256) NOT NULL PRIMARY KEY, "
            s2 += "token_name VARCHAR(256), token_symbol VARCHAR(256), decimals int, "
            s2 += "properties JSON, chat_gpt JSON)"
            s2 += " DATA DIRECTORY = '/media/data/mysql'"
            self._create_table(s1, s2)

        if "t_event_history2" in tables:
            s1 = "DROP TABLE t_event_history2"
            s2 = "CREATE TABLE t_event_history2 (event_id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, pair_id INT NOT NULL, "
            s2 += "transactionHash VARCHAR(256), block_number int, timeStamp datetime NOT NULL, operation VARCHAR(10), sender VARCHAR(256), "
            s2 += "amount0 DECIMAL(60), amount1 DECIMAL(60), amount0In DECIMAL(60), amount1In DECIMAL(60), amount0Out DECIMAL(60), amount1Out DECIMAL(60))"
            s2 += " DATA DIRECTORY = '/media/data/mysql'"
            s3 = "ALTER TABLE t_event_history2 ADD INDEX (pair_id)"
            self._create_table(s1, s2, s3)

        if "t_contract_code2" in tables:
            s1 = "DROP TABLE t_contract_code2"
            s2 = "CREATE TABLE t_contract_code2 (token_id VARCHAR(256) NOT NULL PRIMARY KEY, contract_text LONGBLOB, contract_abi LONGBLOB, contract_analytics JSON)"
            s2 += " DATA DIRECTORY = '/media/data/mysql'"
            self._create_table(s1, s2)

        if "t_attackers" in tables:
            s1 = "DROP TABLE t_attackers"
            s2 = "CREATE TABLE t_attackers (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, tx_from VARCHAR(256), tx_to VARCHAR(256), status INT, note VARCHAR(1024))"
            self._create_table(s1, s2)

        if "t_sandwich_tx" in tables:
            s1 = "DROP TABLE t_sandwich_tx"
            s2 = "CREATE TABLE t_sandwich_tx (tx_id VARCHAR(256) NOT NULL PRIMARY KEY, block_number INT, role INT, attacker_id INT)" # role 0 victim, 1 attack
            self._create_table(s1, s2)
            
        if "t_sandwich_logs" in tables:
            s1 = "DROP TABLE t_sandwich_logs"
            s2 = "CREATE TABLE t_sandwich_logs (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, tx_id VARCHAR(256), address VARCHAR(256), data JSON)"
            self._create_table(s1, s2)

        if "t_filter_events" in tables:
            s1 = "DROP TABLE t_filter_events"
            s2 = "CREATE TABLE t_filter_events (topic VARCHAR(256) NOT NULL PRIMARY KEY, note VARCHAR(1024), signature VARCHAR(1024))"
            self._create_table(s1, s2)

        if "t_processed_blocks" in tables:
            s1 = "DROP TABLE t_processed_blocks"
            s2 = "CREATE TABLE t_processed_blocks (block_number INT NOT NULL PRIMARY KEY)"
            self._create_table(s1, s2)

    def check_lock_pair(self, pair_id):
        s0 = "select pair_id from t_pairs2 where pair_id=%s"
        l = self.cursor.execute(s0, (pair_id, ))
        if l:
            return 1
        else:
            s1 = "insert into t_pairs2(pair_id) values(%s)"
            self.cursor.execute(s1, (pair_id, ))
            return 0

    def add_pair(self, pair_id, pair, pair_data):
        s0 = "select pair_id from t_pairs2 where pair_id=%s"
        l = self.cursor.execute(s0, (pair_id, ))
        if l:
            if not "token" in pair_data or pair_data["token"] is None:
                s1 = "update t_pairs2 set pair=%s, token0=%s, token1=%s  where pair_id = %s"
                self.cursor.execute(s1, (pair, pair_data["token0"], pair_data["token1"], pair_id))
            else:
                s1 = "update t_pairs2 set pair=%s, token0=%s, token1=%s, token=%s, swaps=%s, first_block_number=%s where pair_id = %s"
                self.cursor.execute(s1, (pair, pair_data["token0"], pair_data["token1"], pair_data["token"],
                                         pair_data["swaps"], pair_data["first_block_number"], pair_id))
                self.add_token(pair_data["token"], pair_data["token_name"], pair_data["token_symbol"], pair_data["decimals"])
        else:
            if not "token" in pair_data or pair_data["token"] is None:
                s1 = "insert into t_pairs2(pair_id, pair, token0, token1) values(%s, %s, %s, %s)"
                self.cursor.execute(s1, (pair_id, pair, pair_data["token0"], pair_data["token1"]))
            else:
                s1 = "insert into t_pairs2(pair_id, pair, token0, token1, token, swaps, first_block_number) values(%s, %s, %s, %s, %s, %s, %s)"
                self.cursor.execute(s1, (pair_id, pair, pair_data["token0"], pair_data["token1"], pair_data["token"],
                                         pair_data["swaps"], pair_data["first_block_number"]))
                self.add_token(pair_data["token"], pair_data["token_name"], pair_data["token_symbol"], pair_data["decimals"])

    def add_event_history(self, pair_id, data_list):
        if not data_list:
            return
        s0 = "delete from t_event_history2 where pair_id = " + str(pair_id)
        self.cursor.execute(s0)
        
        s1 = "insert into t_event_history2(pair_id, transactionHash, block_number, timeStamp, operation, sender, amount0, amount1, amount0In, amount1In, amount0Out, amount1Out) values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        ii = 0
        for d in data_list:
            self.cursor.execute(s1, (pair_id, d["transactionHash"], d["block_number"], d["timeStamp"],
                                     d["operation"], d["sender"],
                                     d.get("amount0"),
                                     d.get("amount1"),
                                     d.get("amount0In"),
                                     d.get("amount1In"),
                                     d.get("amount0Out"),
                                     d.get("amount1Out")))
            ii += 1
            if ii % 1000 == 0:
                self.db_connection.commit()

    def remove_event_history(self, pair_id):
        s1 = "delete t_event_history2 where pair_id=" + str(pair_id)
        self.cursor.execute(s1)
                                     
    def update_json(self, table, row_id, field, data, condition):
        for f in data:
            if type(data[f]) == float and np.isnan(data[f]):
                data[f] = ""
            if type(data[f]) == np.int64:
                data[f] = int(data[f])
            elif type(data[f]) == np.float64:
                data[f] = float(data[f])
        if type(row_id) == str:
            s1 = "UPDATE " + table + " SET " + field + "='" + json.dumps(data) + "' WHERE " + condition + "='" + str(row_id) + "'"
        else:
            s1 = "UPDATE " + table + " SET " + field + "='" + json.dumps(data) + "' WHERE " + condition + "=" + str(row_id)
        self.cursor.execute(s1)
        
       
    def get_pairs(self):
        s1 = "select pair_id, pair, token0, token1, token, swaps, first_block_number from t_pairs2"
        self.cursor.execute(s1)
        return self.fetch_with_description(self.cursor)

    def get_pairs_max_block(self, pair_id_start=None, pair_id_end=None):
        s1 = "select t_pairs2.pair_id pair_id, pair, token0, token1, token, swaps, "
        s1 += "first_block_number, max(t_event_history2.block_number) max_block_number from t_pairs2 "
        s1 += "left join t_event_history2 on t_pairs2.pair_id = t_event_history2.pair_id "
        if not pair_id_start is None:
            s1 += " where t_pairs2.pair_id >= " + str(pair_id_start)
            if not pair_id_end is None:
                s1 += " and t_pairs2.pair_id < " + str(pair_id_end)
        elif not pair_id_end is None:
            s1 += " where t_pairs2.pair_id < " + str(pair_id_end)
        s1 += " group by t_pairs2.pair_id, pair, token0, token1, token, swaps, first_block_number"
        # print(s1)
        self.cursor.execute(s1)
        return self.fetch_with_description(self.cursor)
    
    def get_max_block_times(self, min_block=None):
        s1 = "select pair_id, max(timeStamp) max_block_timeStamp, max(block_number) max_block_number from t_event_history2 "
        if not min_block is None:
            s1 += " where block_number >= " + str(min_block)
        s1 += " group by pair_id"
        self.cursor.execute(s1)
        return self.fetch_with_description(self.cursor)


    def get_pairs_no_text(self):
        s1 = "select pair_id, pair, token0, token1, token, swaps, first_block_number from t_pairs2 "
        s1 += "left join t_contract_code2 on t_pairs2.token = t_contract_code2.token_id "
        s1 += "where t_contract_code2.token_id is null and not t_pairs2.token is null"
        self.cursor.execute(s1)
        return self.fetch_with_description(self.cursor)

    def get_pairs_with_contracts(self, pair_id_start=None, pair_id_end=None, liquid_tokens=None):
        s1 = "select pair_id, pair, token0, token1, t_pairs2.token token, t_tokens2.decimals, swaps, first_block_number from t_pairs2 "
        s1 += "inner join t_contract_code2 on t_pairs2.token = t_contract_code2.token_id "
        s1 += "inner join t_tokens2 on t_pairs2.token = t_tokens2.token "
        s1 += "where length(t_contract_code2.contract_text) > 20 "
        if not liquid_tokens is None:
            s1 += "and (token0 in (" + "".join([(", '" if i>0 else "'") + str(t) + "'" for i, t in enumerate(liquid_tokens)]) + ") "
            s1 += "or token1 in (" + "".join([(", '" if i>0 else "'") + str(t) + "'" for i, t in enumerate(liquid_tokens)]) + ")) "
            s1 += "and not t_pairs2.token in (" + "".join([(", '" if i>0 else "'") + str(t) + "'" for i, t in enumerate(liquid_tokens)]) + ")"
        if not pair_id_start is None:
            s1 += " and pair_id >= " + str(pair_id_start)
        if not pair_id_end is None:
            s1 += " and pair_id < " + str(pair_id_end)
        self.cursor.execute(s1)
        return self.fetch_with_description(self.cursor)

    def get_pair(self, pair_id):
        s1 = "select pair, token0, token1, token, swaps, first_block_number from t_pairs2 where pair_id = " + str(pair_id)
        self.cursor.execute(s1)
        return self.fetch_with_description(self.cursor)

    def get_event_history(self, pair_id):
        s1 = "select * from t_event_history2 where pair_id = " + str(pair_id)
        self.cursor.execute(s1)
        history = self.fetch_with_description(self.cursor)
        for h in history:
            for f in h:
                if f[:6] == "amount":
                    if h[f] is None:
                        h[f] = 0
                    else:
                        h[f] = int(h[f])
        return history

    def get_event_history_many(self, pair_id_start, pair_id_end):
        s1 = "select * from t_event_history2 where pair_id >= " + str(pair_id_start) + " and pair_id < " + str(pair_id_end)
        self.cursor.execute(s1)
        history = self.fetch_with_description(self.cursor)
        for h in history:
            for f in h:
                if f[:6] == "amount":
                    if h[f] is None:
                        h[f] = 0
                    else:
                        h[f] = int(h[f])
        return history

    def get_json(self, table, row_id, field, condition):
        if type(row_id) == str:
            s1 = "select " + field + " from " + table + " WHERE " + condition + "='" + str(row_id) + "'"
        else:
            s1 = "select " + field + " from " + table + " WHERE " + condition + "=" + str(row_id)

        self.cursor.execute(s1)
        return json.loads(self.cursor.fetchall()[0][0])

    def add_contract_code(self, token, code, abi):
        s1 = "DELETE FROM t_contract_code2 WHERE token_id = %s"
        self.cursor.execute(s1, (token, ))
        s2 = """INSERT INTO t_contract_code2 (token_id, contract_text, contract_abi) VALUES (%s, _binary "%s", _binary "%s")"""
        self.cursor.execute(s2, (token, zlib.compress(pickle.dumps(code)), zlib.compress(pickle.dumps(abi))))

    def add_contract_abi(self, token, abi):
        s1 = """UPDATE t_contract_code2 set contract_abi = _binary "%s" WHERE token_id = %s"""
        self.cursor.execute(s1, (zlib.compress(pickle.dumps(abi)), token))

    def check_contract_code(self, token):
        s1 = "SELECT length(contract_text), length(contract_abi) from t_contract_code2 where token_id = %s"
        n = self.cursor.execute(s1, (token, ))
        if n == 0:
            return 0, 0
        else:
            (len_contract_text, len_contract_abi) = self.cursor.fetchone()
            res1 = 0; res2 = 0
            if len_contract_text == 0:
                return 0, 0
            elif len_contract_text == 20:
                res1 = 20
            else:
                res1 = 1
            if len_contract_abi and len_contract_abi > 0:
                res2 = 1
            return res1, res2

    def get_contract_code(self, token):
        s1 = "SELECT contract_text, contract_analytics from t_contract_code2 where token_id = %s"
        n = self.cursor.execute(s1, (token, ))
        if n:
            (contract_text, contract_analytics) = self.cursor.fetchone()
            return token, pickle.loads(zlib.decompress(contract_text[1:-1])), contract_analytics
        else:
            return token, None, None

    def get_contract_code_and_abi(self, token):
        s1 = "SELECT contract_text, contract_abi, contract_analytics from t_contract_code2 where token_id = %s"
        n = self.cursor.execute(s1, (token, ))
        if n:
            (contract_text, contract_abi, contract_analytics) = self.cursor.fetchone()
            return token, pickle.loads(zlib.decompress(contract_text[1:-1])), (pickle.loads(zlib.decompress(contract_abi[1:-1])) if not contract_abi is None else None), contract_analytics
        else:
            return token, None, None, None

    def clean_for_reload(self, start_pair, end_pair):
        s1 = "delete from t_pairs2 where token is null and pair_id >= %s and pair_id < %s"
        s2 = "delete from t_pairs2 where not token is null and first_block_number is null and pair_id >= %s and pair_id < %s"
        s3 = "delete from t_pairs2 where (swaps is null or swaps = 0) and pair_id >= %s and pair_id < %s"
        
        i1 = self.cursor.execute(s1, (start_pair, end_pair))
        i2 = self.cursor.execute(s2, (start_pair, end_pair))
        i3 = self.cursor.execute(s3, (start_pair, end_pair))
        return i1, i2, i3

    def add_token(self, token, token_name, token_symbol, decimals):
        s0 = "select * from t_tokens2 where token=%s"
        l = self.cursor.execute(s0, (token, ))
        if l:
            s1 = "update t_tokens2 set token_name=%s, token_symbol=%s, decimals=%s where token = %s"
            self.cursor.execute(s1, (token_name[:255], token_symbol[:255], decimals, token))
        else:
            s1 = "insert into t_tokens2(token, token_name, token_symbol, decimals) values(%s, %s, %s, %s)"
            self.cursor.execute(s1, (token, token_name[:255], token_symbol[:255], decimals))

    def get_token(self, token):
        s1 = "select token, token_name, token_symbol, decimals from t_tokens2 where token = '" + token + "'"
        self.cursor.execute(s1)
        return self.fetch_with_description(self.cursor)

    def get_tokens(self):
        s1 = "select token, token_name, token_symbol, decimals from t_tokens2"
        self.cursor.execute(s1)
        return self.fetch_with_description(self.cursor)

    def get_tokens_with_property(self, property_name):
        s1 = "select token, token_name, token_symbol, decimals, JSON_EXTRACT(properties, '$.\"" + property_name + "\"') " + property_name + " from t_tokens2 where not JSON_EXTRACT(properties, '$.\"" + property_name + "\"') is null"
        self.cursor.execute(s1)
        return self.fetch_with_description(self.cursor)

    def get_tokens_without_property(self, property_name):
        s1 = "select token, token_name, token_symbol, decimals from t_tokens2 where JSON_EXTRACT(properties, '$.\"" + property_name + "\"') is null"
        self.cursor.execute(s1)
        return self.fetch_with_description(self.cursor)

    def add_topic_filter(self, topic, note=None, signature=None):
        s1 = "insert into t_filter_events(topic, note, signature) values(%s, %s, %s)"
        self.cursor.execute(s1, (topic, note, signature))
        
    def update_topic_filter(self, topic, note=None, signature=None):
        s1 = "delete from t_filter_events where topic = %s "
        s2 = "insert into t_filter_events(topic, note) values(%s, %s)"
        self.cursor.execute(s1, (topic, ))
        self.cursor.execute(s2, (topic, note))

    def add_attacker(self, tx_from, tx_to, status, note=None):
        s1 = "insert into t_attackers(tx_from, tx_to, status, note) values(%s, %s, %s, %s)"
        self.cursor.execute(s1, (tx_from, tx_to, status, note))
        return self.cursor.lastrowid

    def get_topic_filters(self):
        s1 = "select * from t_filter_events"
        self.cursor.execute(s1)
        return self.fetch_to_dict(self.cursor, "topic")

    def get_attackers(self):
        s1 = "select * from t_attackers"
        self.cursor.execute(s1)
        return self.fetch_with_description(self.cursor)
