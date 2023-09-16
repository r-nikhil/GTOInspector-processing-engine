
import datetime
import enum
import os
import re
import pandas as pd
import time
from . import __base__
from .helper_functions import Helper
from . import pdf_to_text
from pprint import pprint


class Adda52Parser(object):

    def __init__(self, **kwargs) -> None:
        self.helper = kwargs.get("helper")
        self.configdict = __base__.configdict
        self.rake = kwargs.get("rake", 500)
        self.logger = kwargs.get("logger")
        self.logging_enabled = kwargs.get("logging_enabled")

        self.cards_regex = re.compile(
            r'[cdsh][(]\w+[)]', flags=re.IGNORECASE)
        self.moves_regex = re.compile(
            r'\bfold\b|\bcheck\b|\braise\b|\ball-in\b|\bcall\b', flags=re.IGNORECASE)
        self.move_regex = re.compile(
            r'\bfolded\b|\bchecked\b|\braised\b|\ball-in\b|\bcalled\b', flags=re.IGNORECASE)

        # Constants
        self.correct_terms = ["Correct", "EV Loss"]
        self.heroname = kwargs.get("heroname", None)
        self.positions = ["EP", "MP", "CO", "BU", "SB", "BB"]

    def convert_pdf_to_txt(self, pdf_filename, text_filename):
        with open(text_filename, "w+") as f:
            f.write(pdf_to_text.convert_to_text(pdf_filename))

    def get_bigblind(self, line):
        try:
            return int(re.findall(r'\w+', line)[1])
        except IndexError:
            return 200

    def get_named_position(self, line):
        if "Button" in line:
            return "BU"
        elif "S Blind" in line:
            return "SB"
        elif "B Blind" in line:
            return "BB"
        return None

    def get_gameid(self, line):
        return line.split(":")[1].strip()

    def convert_cards(self, card):
        suite = card[0]
        rank = card[2:-1]
        if rank == '10':
            return "T{}".format(suite)
        elif rank == "1":
            return "A{}".format(suite)
        return "{}{}".format(rank.capitalize(), suite)

    def get_holecards(self, line):
        cards = self.cards_regex.findall(line)
        for i, card in enumerate(cards):
            cards[i] = self.convert_cards(card)
        return cards

    def get_file_contents(self, filepath):
        with open(filepath) as f:
            return f.readlines()

    def get_file_segments(self, filepath):
        file_contents = self.get_file_contents(filepath)
        segments = []
        info_lines = []
        preflop_lines = []
        winner_lines = []
        flag = 1
        for i, line in enumerate(file_contents):
            if line == '\n':
                continue
            if "*****" in line:
                if len(info_lines) and len(preflop_lines):
                    segments.append([info_lines, preflop_lines, winner_lines])
                    flag = 1
                    info_lines = []
                    preflop_lines = []
                    winner_lines = []
            if flag:
                if ":" in line:
                    info_lines.append(line)
                else:
                    preflop_lines.append(line)
            if "Flop Cards" in line:
                flag = 0
            if "Winner" in line:
                winner_lines.append(line)
        return segments

    def parse_text(self, filepath):
        if os.path.isfile(filepath):
            self.heroname = self.get_heroname(filepath)
            segments = self.get_file_segments(filepath)
            len_segments = len(segments)
            return_values = []
            for k, segment in enumerate(segments):
                try:
                    info_lines, action_lines, winner_lines = segment
                    position = None
                    i = 0
                    action_sequence = []
                    position_found = False
                    position_found_using_index = False
                    user_lines = []
                    amount_won = 0.
                    for line in info_lines:
                        if self.heroname in line:
                            position = self.get_named_position(line)
                            position_found = True
                        elif "Blinds" in line:
                            bigblind = self.get_bigblind(line)
                        elif "Hand ID" in line:
                            gameid = self.get_gameid(line)
                    if position == "SB":
                        amount_won -= bigblind/2
                    elif position == "BB":
                        amount_won -= bigblind

                    for line in action_lines:
                        if "My Cards" in line:
                            holecards = self.get_holecards(line)
                        else:
                            move = self.move_regex.findall(line)
                            if len(move):
                                user_lines.append(line)
                                if self.heroname in line:
                                    amount = re.findall(r'\d+', line)
                                    if len(amount):
                                        if "raise" in move[0].lower() or "call" in move[0].lower():
                                            amount_won = -float(amount[0])

                                        elif "return" in move[0].lower():
                                            amount_won += float(amount[0])

                                    action_sequence.append(
                                        " ".join(["hero", move[0]]))
                                    if not position_found:
                                        position_found = True
                                        position_found_using_index = True
                                        position_index = i
                                else:
                                    action_sequence.append(move[0])
                                i += 1
                    unique_players = self.get_unique_users(user_lines)
                    if position_found_using_index:
                        position = self.positions[position_index -
                                                  len(unique_players) + 6]
                    action_sequences = self.process_action_sequence(
                        action_sequence)
                    for line in winner_lines:
                        if self.heroname in line:
                            amount = re.findall(r'\d+', line)
                            if len(amount):
                                amount_won = float(amount[0])
                    category = self.helper.get_category(holecards)
                    if category is None:
                        category = {}
                    for action_seq in action_sequences:
                        opportunity = self.get_strategy_from_moves(action_seq)
                        current_action = self.moves_regex.findall(
                            action_seq[-1])
                        action_seq[-1] = "hero"
                        res_dict = self.helper.run_everything(holecards, " ".join(
                            action_seq), 100, self.rake, len(unique_players))
                        if res_dict is None:
                            continue
                        best_action = None
                        highest_ev = max([res_dict[key]["ev"]
                                         for key in res_dict])
                        for key, val in res_dict.items():
                            if highest_ev - val["ev"] < 0.0001:
                                best_action = key
                        if current_action[0] == best_action:
                            correct = self.correct_terms[0]
                        else:
                            correct = self.correct_terms[1]
                        move_ev = res_dict.get(
                            current_action[0], {}).get("ev", None)
                        if move_ev is not None:
                            move_ev = move_ev/2000
                        return_values.append({
                            "ID": gameid,
                            "Hand": holecards,
                            "Pairedness": category.get("pairing", ""),
                            "Suitedness": category.get("suiting", ""),
                            "Hand Category": category.get("category", ""),
                            "Position": position,
                            "Result": correct,
                            "Opportunity": opportunity,
                            "Big Blind": "{}/{}".format(int(bigblind/2), int(bigblind)),
                            "Player's Move": current_action[0],
                            "GTO Move": best_action,
                            "Amount Won in Terms of BB": amount_won/bigblind,
                            "Move EV": move_ev,
                            "GTO EV": highest_ev
                        })
                except Exception as e:
                    print("Error in section: {}: {}".format(k, e))
            return return_values, len_segments

    def get_strategy_from_moves(self, action_sequence):
        fold = 0
        call = 0
        raises = 0
        all_in = 0
        check = 0
        for move in action_sequence:
            if "hero" in move:
                break
            elif "fold" in move:
                fold += 1
            elif "call" in move:
                call += 1
            elif "raise" in move:
                raises += 1
            elif "all-in" in move:
                all_in += 1
            elif "check" in move:
                check += 1
        if (raises == 0) and (call == 0) and (check == 0):
            return "RFI"
        elif (raises == 1) and (call == 0):
            return "3 Bet / Call 2 Bet"
        elif (raises == 1) and (call >= 1):
            return "Squeeze / Overcall"
        else:
            return "Other"

    def get_heroname(self, filepath):
        file_contents = self.get_file_contents(filepath)
        for line in file_contents:
            if "My Cards" in line:
                current_cards = set(self.get_holecards(line))
            if "Winner" in line:
                winner_cards = set(self.get_holecards(line))
                if len(winner_cards):
                    if len(current_cards.intersection(set(winner_cards))):
                        return re.findall(r'\w+', line)[2]

    def process_action_sequence(self, action_sequence: list) -> list:
        int_action_sequence = []
        all_action_sequences = []
        for action in action_sequence:
            if "fold" in action.lower():
                if "hero" in action:
                    all_action_sequences.append(
                        int_action_sequence + ["hero fold"])
                int_action_sequence.append("fold")
            elif "raise" in action.lower():
                if "hero" in action:
                    all_action_sequences.append(
                        int_action_sequence + ["hero raise"])
                int_action_sequence.append("raise")
            elif "call" in action.lower():
                if "hero" in action:
                    all_action_sequences.append(
                        int_action_sequence + ["hero call"])
                int_action_sequence.append("call")
            elif "check" in action.lower():
                if "hero" in action:
                    all_action_sequences.append(
                        int_action_sequence + ["hero check"])
                int_action_sequence.append("check")
            elif "all-in" in action.lower():
                if "hero" in action:
                    all_action_sequences.append(
                        int_action_sequence + ["hero all-in"])
                int_action_sequence.append("all-in")
        return all_action_sequences

    def get_unique_users(self, lines):
        users = set()
        for line in lines:
            users.add(line.split(" ")[0])
        return list(users)

    def run_everything(self, filepath):
        start_time = time.time()
        return_values, len_segments = self.parse_text(filepath)
        end_time = time.time()
        print("Time taken: {:.2f}min".format((end_time - start_time)/60))
        if return_values is not None:
            df = pd.DataFrame(return_values)
            return df, len_segments
        else:
            return None, 0


if __name__ == '__main__':
    adda52parser = Adda52Parser(helper=None)
    # adda52parser.convert_pdf_to_txt("sample_files/adda52_test.pdf")
