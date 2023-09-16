import time
import re
import sqlite3
import pandas as pd
import sys
import boto3
import paramiko
from botocore.exceptions import NoCredentialsError
import os
import psycopg2
import pymongo
import logging
from . import __base__


class Helper(object):
    """Helper functions for the processing"""

    def __init__(self):
        self.alpha_ranks = [
            "Ac", "Ad", "Ah", "As",
            "Kc", "Kd", "Kh", "Ks",
            "Qc", "Qd", "Qh", "Qs",
            "Jc", "Jd", "Jh", "Js",
            "Tc", "Td", "Th", "Ts",
            "9c", "9d", "9h", "9s",
            "8c", "8d", "8h", "8s",
            "7c", "7d", "7h", "7s",
            "6c", "6d", "6h", "6s",
            "5c", "5d", "5h", "5s",
            "4c", "4d", "4h", "4s",
            "3c", "3d", "3h", "3s",
            "2c", "2d", "2h", "2s",
        ]
        self.anti_alpha_ranks = [
            "As", "Ah", "Ad", "Ac",
            "Ks", "Kh", "Kd", "Kc",
            "Qs", "Qh", "Qd", "Qc",
            "Js", "Jh", "Jd", "Jc",
            "Ts", "Th", "Td", "Tc",
            "9s", "9h", "9d", "9c",
            "8s", "8h", "8d", "8c",
            "7s", "7h", "7d", "7c",
            "6s", "6h", "6d", "6c",
            "5s", "5h", "5d", "5c",
            "4s", "4h", "4d", "4c",
            "3s", "3h", "3d", "3c",
            "2s", "2h", "2d", "2c",
        ]

        self.configdict = __base__.configdict
        self.dbs = [
            "PLO50_50BB_2P", "PLO50_100BB_2P",
            "PLO500_50BB_2P", "PLO500_100BB_2P", "PLO500_200BB_2P",
            "PLO5000_50BB_2P", "PLO5000_100BB_2P",
            "PLO50_30BB_6P", "PLO50_50BB_6P", "PLO50_100BB_6P",
            "PLO500_30BB_6P", "PLO500_50BB_6P", "PLO500_100BB_6P", "PLO500_150BB_6P"
        ]
        self.conn = dict()
        self.cursordict = dict()
        self.movesdb = dict()
        self.mongoconn = pymongo.MongoClient("mongodb://{}:{}@{}:{}".format(
            self.configdict.get("MONGO_USER"),
            self.configdict.get("MONGO_PWD"),
            self.configdict.get("MONGO_HOST"),
            self.configdict.get("MONGO_PORT")
        ))
        self.category_db = self.mongoconn["ranges"]["card_ranges"]
        for db in self.dbs:
            try:
                self.conn[db] = psycopg2.connect("postgresql://{}:{}@{}:{}/{}".format(
                    self.configdict.get("PG_USER"),
                    self.configdict.get("PG_PWD"),
                    self.configdict.get("PG_HOST"),
                    self.configdict.get("PG_PORT"),
                    db))
                self.cursordict[db] = self.conn[db].cursor()
                self.movesdb[db] = self.mongoconn[self.configdict.get(
                    "MOVES_DB_NAME")][db]
            except Exception as e:
                print(e)
                logging.error("Could not connect to database {}".format(db))

        self.table_name_regex = re.compile(r'\w+')
        self.mapping = {"raise": "r", "fold": "f", "call": "c", "bet": "b",
                        "all_in": "a", "check": "x", "villain": "v", "hero": "h"}
        self.reverse_mapping = {"r": "raise", "f": "fold", "c": "call",
                                "b": "bet", "a": "all_in", "x": "check", "v": "villain", "h": "hero"}

        self.order_graph = {
            0: ["s", "d", "c", "h", "s"],  # s -> d -> c -> h -> s
            1: ["s", "d", "h", "c", "s"],  # s -> d -> h -> c -> s
            2: ["s", "c", "d", "h", "s"],  # s -> c -> d -> h -> s
            3: ["s", "c", "h", "d", "s"],  # s -> c -> h -> d -> s
            4: ["s", "h", "c", "d", "s"],  # s -> h -> c -> d -> s
            5: ["s", "h", "d", "c", "s"],  # s -> h -> d -> c -> s
        }
        self.order_hashmap = {
            0: {'c': 'c', 'd': 'd', 'h': 'h', 's': 's'},
            1: {'c': 'c', 'd': 'd', 'h': 's', 's': 'h'},
            2: {'c': 'c', 'd': 'h', 'h': 'd', 's': 's'},
            3: {'c': 'c', 'd': 'h', 'h': 's', 's': 'd'},
            4: {'c': 'c', 'd': 's', 'h': 'd', 's': 'h'},
            5: {'c': 'c', 'd': 's', 'h': 'h', 's': 'd'},
            6: {'c': 'd', 'd': 'c', 'h': 'h', 's': 's'},
            7: {'c': 'd', 'd': 'c', 'h': 's', 's': 'h'},
            8: {'c': 'd', 'd': 'h', 'h': 'c', 's': 's'},
            9: {'c': 'd', 'd': 'h', 'h': 's', 's': 'c'},
            10: {'c': 'd', 'd': 's', 'h': 'c', 's': 'h'},
            11: {'c': 'd', 'd': 's', 'h': 'h', 's': 'c'},
            12: {'c': 'h', 'd': 'c', 'h': 'd', 's': 's'},
            13: {'c': 'h', 'd': 'c', 'h': 's', 's': 'd'},
            14: {'c': 'h', 'd': 'd', 'h': 'c', 's': 's'},
            15: {'c': 'h', 'd': 'd', 'h': 's', 's': 'c'},
            16: {'c': 'h', 'd': 's', 'h': 'c', 's': 'd'},
            17: {'c': 'h', 'd': 's', 'h': 'd', 's': 'c'},
            18: {'c': 's', 'd': 'c', 'h': 'd', 's': 'h'},
            19: {'c': 's', 'd': 'c', 'h': 'h', 's': 'd'},
            20: {'c': 's', 'd': 'd', 'h': 'c', 's': 'h'},
            21: {'c': 's', 'd': 'd', 'h': 'h', 's': 'c'},
            22: {'c': 's', 'd': 'h', 'h': 'c', 's': 'd'},
            23: {'c': 's', 'd': 'h', 'h': 'd', 's': 'c'}
        }
        self.reverse_order_hashmap = {
            'cc': [0, 1, 2, 3, 4, 5],
            'dd': [0, 1, 14, 15, 20, 21],
            'hh': [0, 5, 6, 11, 19, 21],
            'ss': [0, 2, 6, 8, 12, 14],
            'hs': [1, 3, 7, 9, 13, 15],
            'sh': [1, 4, 7, 10, 18, 20],
            'dh': [2, 3, 8, 9, 22, 23],
            'hd': [2, 4, 12, 17, 18, 23],
            'sd': [3, 5, 13, 16, 19, 22],
            'ds': [4, 5, 10, 11, 16, 17],
            'cd': [6, 7, 8, 9, 10, 11],
            'dc': [6, 7, 12, 13, 18, 19],
            'hc': [8, 10, 14, 16, 20, 22],
            'sc': [9, 11, 15, 17, 21, 23],
            'ch': [12, 13, 14, 15, 16, 17],
            'cs': [18, 19, 20, 21, 22, 23]
        }
        self.card_regex = re.compile(r'[2-9AKQJT][sdch]')

    def rearrange_cards_alphabetically(self, cards: str, _reversed=False) -> str:
        """
        Inputs:
            cards: a 2n length string of cards
            _reversed: a boolean. returns anti-alphabetical if true
        This function takes in two arguments, cards and _reversed.
        If _reversed is True, it will rearrange the cards in
        anti-alphabetical order of suites
        else, it will rearrange the cards in alphabetical order of suites
        """
        if isinstance(cards, str):
            assert len(cards) % 2 == 0, "Length of cards should be even"
            cards = [cards[i:i+2] for i in range(0, len(cards), 2)]
        return "".join(sorted(cards,
                              key=self.alpha_ranks.index,
                              reverse=_reversed)
                       )

    def rearrange_cards_anti_alphabetically(self, cards: str, _reversed=False) -> str:
        """
        Inputs:
            cards: a 2n length string of cards
            _reversed: a boolean. returns anti-alphabetical if true
        This function takes in two arguments, cards and _reversed.
        If _reversed is True, it will rearrange the cards in
        anti-alphabetical order of suites
        else, it will rearrange the cards in alphabetical order of suites
        """
        if isinstance(cards, str):
            assert len(cards) % 2 == 0, "Length of cards should be even"
            cards = [cards[i:i+2] for i in range(0, len(cards), 2)]
        return "".join(sorted(cards,
                              key=self.anti_alpha_ranks.index,
                              reverse=_reversed)
                       )

    def mapping_func(self, inp):
        if inp in self.mapping:
            return self.mapping[inp]
        return inp

    def reverse_mapping_func(self, inp):
        if inp in self.reverse_mapping:
            return self.reverse_mapping[inp]
        return inp

    def get_short_table_name(self, long_table_name):
        long_table_name = self.table_name_regex.findall(long_table_name)
        long_table_name = map(self.mapping_func, long_table_name)
        return "_".join(long_table_name)

    def get_long_table_name(self, short_table_name):
        short_table_name = short_table_name.split("_")
        long_table_name = map(self.reverse_mapping_func, short_table_name)
        return " ".join(long_table_name)

    def search_dict(self, keys, dictionary):
        if keys[0] in dictionary:
            if isinstance(dictionary[keys[0]], list) and keys[0] == 'h':
                if "v" in dictionary[keys[0]]:
                    dictionary[keys[0]].pop(dictionary[keys[0]].index("v"))
                return dictionary[keys[0]]
            elif len(keys) == 1:
                return list(dictionary[keys[0]].keys())
            key = keys.pop(0)
            return self.search_dict(keys, dictionary[key])

    def search_moves(self, initial_moves, dbname):
        """Searching next moves"""
        collection = self.movesdb[dbname]
        data = collection.find_one()
        list_of_next_moves = self.search_dict(initial_moves.copy(), data)
        return list_of_next_moves

    def _dissect_cards(self, cards: str):
        assert len(cards) % 2 == 0, "Cards should be represented by two letters"
        ranks = ["_"]*(len(cards)//2)
        suites = ["_"]*(len(cards)//2)
        for i in range(0, len(cards)):
            if i % 2 == 0:
                ranks[i//2] = cards[i]
            else:
                suites[(i-1)//2] = cards[i]
        return ranks, suites

    def _find_suite_from_id(self, id_set: set):
        if id_set is not None and len(id_set):
            return self.order_hashmap[list(id_set)[0]]

    def _change_suites(self, suites: list, _id: int):
        """
        Changes suite of given suites list based on given _id
        _id is defined separately from hashmap
        """
        assert _id >= 0 and _id < 6, "ID should be between 0 and 5"
        hashmap = self.order_hashmap[_id]
        for i, suite in enumerate(suites):
            suites[i] = hashmap[suite]
        return suites

    def _change_multiple_suites(self, order_of_suites):
        if len(order_of_suites) % 2 != 0:
            return None
        if not isinstance(order_of_suites, list):
            order_of_suites = list(order_of_suites)
        _list = []
        for i in range(0, len(order_of_suites), 2):
            a, b = order_of_suites[i], order_of_suites[i+1]
            _list.append(self._find_change_id(a, b))

        if len(_list) > 0:
            a = _list[0]
            for b in _list:
                a = a & b
            if len(a) > 0:
                return a
            else:
                return None
        else:
            return None

    def _find_change_id(self, src: str, dst: str):
        """
        Takes in a src suite and a dst suite and returns the ID which has that transformation
        """
        return set(self.reverse_order_hashmap["{}{}".format(src, dst)])

    def reverse_dictionary(self, inp_dict):
        ret_dict = {}
        for key, value in inp_dict.items():
            ret_dict[value] = key
        return ret_dict

    def find_flop_changes(self, src: str, dst: str):
        src = self.rearrange_cards_anti_alphabetically(src)
        # dst = self.rearrange_cards_anti_alphabetically(dst)
        src_ranks, src_suites = self._dissect_cards(src)
        dst_ranks, dst_suites = self._dissect_cards(dst)
        if src_ranks == dst_ranks:
            changes = [None] * 6
            for i in range(6):
                if i % 2 == 0:
                    changes[i] = src_suites[i // 2]
                else:
                    changes[i] = dst_suites[(i - 1) // 2]
            return self._find_suite_from_id(self._change_multiple_suites("".join(changes)))
        else:
            return None

    def change_cards(self, src_cards, src_flop, dst_flop):
        flop_changes = self.find_flop_changes(src_flop, dst_flop)
        flop_changes = self.reverse_dictionary(flop_changes)
        src_cards = list(src_cards)
        for i in range(0, len(src_cards), 2):
            src_cards[i+1] = flop_changes[src_cards[i+1]]
        return "".join(src_cards)

    def get_table_names_from_db(self, initial_moves, list_of_next_moves):
        """Getting table names from initial moves"""
        if list_of_next_moves is not None:
            return {key: "_".join([*initial_moves, key]) for key in list_of_next_moves}

    def search_tables_with_cards(self, cards, long_moves_string, dbname, short=False):
        """Search table for cards and get weight, and ev"""
        if long_moves_string[-2:] == '_h':
            long_moves_string = long_moves_string[-2:]
        if not short:
            initial_moves = self.get_short_table_name(
                long_moves_string).split("_")
        else:
            initial_moves = long_moves_string.split("_")
        list_of_next_moves = self.search_moves(initial_moves, dbname)
        if list_of_next_moves is None:
            return None
        next_tables = self.get_table_names_from_db(
            initial_moves, list_of_next_moves)
        ret_dict = {}
        for move in next_tables:
            full_move = self.reverse_mapping_func(move)
            self.cursordict[dbname].execute(
                "SELECT * FROM \"{}\" WHERE combo = '{}';".format(next_tables[move], cards))
            res = self.cursordict[dbname].fetchall()
            if len(res):
                ret_dict[full_move] = {
                    "weight": res[0][1],
                    "ev": res[0][2]
                }
        if len(ret_dict) == 0:
            return "Could not find cards"
        sum_ = 0
        for move in ret_dict:
            sum_ += ret_dict[move]['weight']
        for move in ret_dict:
            ret_dict[move]['weight'] /= sum_
        return ret_dict

    def search_tables(self, cards, long_moves_string, dbname):
        flop = self.card_regex.findall(long_moves_string)
        if len(flop):
            flop = "".join(flop)
            preflop, postflop = long_moves_string.split(flop)
            short_preflop = self.get_short_table_name(preflop)
            flops = self.search_moves(short_preflop.split("_"), dbname)
            rearranged_flop = self.rearrange_cards_anti_alphabetically(flop)
            if rearranged_flop in flops:
                rearranged_short_table_name = "_".join(
                    [short_preflop, rearranged_flop, self.get_short_table_name(postflop)])
                if cards is None:
                    return "Cards are none"
                return self.search_tables_with_cards(cards, rearranged_short_table_name, dbname, short=True)

            for flop in flops:
                if rearranged_flop[0] == flop[0] and rearranged_flop[2] == flop[2] and rearranged_flop[4] == flop[4]:
                    changes = self.find_flop_changes(rearranged_flop, flop)
                    if changes is not None:
                        changed_cards = self.change_cards(
                            cards, rearranged_flop, flop)
                        rearranged_short_table_name = "_".join(
                            [short_preflop, flop, self.get_short_table_name(postflop)])
                        if cards is None:
                            return "Cards are none"
                        return self.search_tables_with_cards(changed_cards, rearranged_short_table_name, dbname, short=True)
        if cards is None:
            return "cards are none"
        return self.search_tables_with_cards(cards, long_moves_string, dbname)

    def get_category(self, cards: str):
        return self.category_db.find_one({"cards": self.rearrange_cards_alphabetically(cards)})

    def run_everything(self, cards: str, action_sequence: str, stacksize: int, rake: int, number_of_players: int):
        if rake == 5000:
            if number_of_players == 2:
                if stacksize < 70:
                    dbname = "PLO5000_50BB_2P"
                else:
                    dbname = "PLO5000_100BB_2P"
            else:
                if stacksize < 40:
                    dbname = "PLO500_30BB_6P"
                elif stacksize < 70:
                    dbname = "PLO500_50BB_6P"
                elif stacksize < 120:
                    dbname = "PLO500_100BB_6P"
                elif stacksize < 160:
                    dbname = "PLO500_150BB_6P"
                else:
                    dbname = "PLO500_200BB_6P"
        elif rake == 50:
            if number_of_players == 2:
                if stacksize < 70:
                    dbname = "PLO50_50BB_2P"
                else:
                    dbname = "PLO50_100BB_2P"
            else:
                if stacksize < 40:
                    dbname = "PLO50_30BB_6P"
                elif stacksize < 70:
                    dbname = "PLO50_50BB_6P"
                else:
                    dbname = "PLO50_100BB_6P"
        else:
            if number_of_players == 2:
                if stacksize < 70:
                    dbname = "PLO500_50BB_2P"
                elif stacksize < 140:
                    dbname = "PLO500_100BB_2P"
                else:
                    dbname = "PLO500_200BB_2P"
            else:
                if stacksize < 40:
                    dbname = "PLO500_30BB_6P"
                elif stacksize < 70:
                    dbname = "PLO500_50BB_6P"
                elif stacksize < 120:
                    dbname = "PLO500_100BB_6P"
                elif stacksize < 160:
                    dbname = "PLO500_150BB_6P"
                else:
                    dbname = "PLO500_150BB_6P"
        if cards is not None:
            cards = self.rearrange_cards_alphabetically(cards)
        data = self.search_tables(cards, action_sequence, dbname)
        return data


if __name__ == '__main__':
    helper = Helper()
    print(helper.run_everything("AcKh9s9c", "raise raise call hero", 140, 50, 6))
