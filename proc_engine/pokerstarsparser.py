from .helper_functions import Helper
import os
import re
import pandas as pd
import time
from . import __base__


class PokerStarsParser():

    def __init__(self, **kwargs):
        # Classes
        self.helper = kwargs.get("helper")
        self.config = __base__.configdict
        self.rake = kwargs.get("rake", 500)
        # Regexes
        self.cards_regex = re.compile(r'[2-9TKQJA][cdhs]', flags=re.IGNORECASE)
        self.bb_regex = re.compile(r'\/\$\d+')
        self.ss_regex = re.compile(r'\$\d+.\d+')
        self.moves_regex = re.compile(
            r'\bfolds\b|\bchecks\b|\braises\b|\ball-in\b|\bcalls\b', flags=re.IGNORECASE)
        self.move_regex = re.compile(
            r'\bfold\b|\bcheck\b|\braise\b|\ball-in\b|\bcall\b', flags=re.IGNORECASE)
        self.seat_regex = re.compile(r'\bseat\b', flags=re.IGNORECASE)
        self.dealt_to_regex = re.compile(r'\bdealt to\b', flags=re.IGNORECASE)
        self.summary_regex = re.compile(r'\bsummary\b', flags=re.IGNORECASE)
        self.uncalled_regex = re.compile(r'\buncalled\b', flags=re.IGNORECASE)
        self.collected_regex = re.compile(
            r'\bcollected\b', flags=re.IGNORECASE)
        self.id_regex = re.compile(r'\#\d+')

        # Constants
        self.correct_terms = ["Correct", "EV Loss"]
        self.heroname = kwargs.get("heroname", None)
        self.positions = ["EP", "MP", "CO", "BU", "SB", "BB"]

    def get_cards(self, line):
        cards = self.cards_regex.findall(line)
        if len(cards) == 4:
            cards = self.helper.rearrange_cards_alphabetically(cards)
            return "".join(cards)

    def find_bb(self, line):
        try:
            return float(self.bb_regex.findall(line)[0][2:])
        except IndexError:
            print("Could not find big blind", line)
            return 0

    def find_stacksize(self, line):
        try:
            return float(self.ss_regex.findall(line)[0][1:])
        except IndexError:
            print("Could not find stack size", line)
            return 0

    def hero_action(self, line, heroname):
        move = self.moves_regex.findall(line)
        if len(move):
            if heroname in line:
                return "hero {}".format(move[0][:-1])
            else:
                return move[0][:-1]
        else:
            move = self.move_regex.findall(line)
            if len(move):
                return move[0]

    def set_heroname(self, list_of_lines: list):
        cards, heroname = None, None
        for line in list_of_lines:
            res = self.dealt_to_regex.findall(line)
            if len(res):
                cards = self.get_cards(line)
                heroname = line.split(" ")[2]
                break
        return heroname, cards

    def get_seating_info(self, list_of_lines: list):
        num_players = 0
        player_position_index = None
        stacksize = None
        for i, line in enumerate(list_of_lines):
            if "HOLE CARDS" in line:
                break
            if "Table" in line:
                continue
            if num_players == 6:
                break
            res = self.seat_regex.findall(line)
            if len(res):
                num_players += 1
                if self.heroname in line:
                    player_position_index = num_players
                    stacksize = self.find_stacksize(line)
                    if stacksize == 0:
                        print(i)
                        print(list_of_lines)
        return player_position_index, stacksize, num_players

    def get_amount_won(self, list_of_lines: list):
        amount_won = 0
        for line in list_of_lines:
            summary_res = self.summary_regex.findall(line)
            if len(summary_res):
                break
            seat_res = self.seat_regex.findall(line)
            if not len(seat_res):
                if self.heroname in line:
                    money = self.ss_regex.findall(line)
                    uncalled = self.uncalled_regex.findall(line)
                    collected = self.collected_regex.findall(line)
                    if len(money) == 1:
                        if len(uncalled) or len(collected):
                            amount_won += float(money[0][1:])
                        else:
                            amount_won -= float(money[0][1:])
                    elif len(money) == 2:
                        amount_won -= float(money[1][1:])
        return amount_won

    def get_position(self, list_of_lines):
        for i, line in enumerate(list_of_lines):
            if self.heroname is not None and self.heroname in line:
                return self.positions[i]

    def get_action_chain(self, list_of_lines, num_players):
        action_sequences = []
        action_sequence = []
        if num_players > 2:
            for i in range(6 - num_players):
                action_sequence.append("fold")
        for line in list_of_lines:
            if "FLOP" in line:
                break
            else:
                action = self.hero_action(line, self.heroname)
                if action is not None:
                    action_sequence.append(action)
                    if "hero" in action:
                        action_sequences.append(action_sequence.copy())
                    action_sequence.pop()
                    action_sequence.append(
                        self.hero_action(action, self.heroname))
        return action_sequences

    def get_gameid(self, line):
        res = self.id_regex.findall(line)
        if len(res):
            return res[0]

    def dissect_section(self, list_of_lines):
        title = list_of_lines[0]
        seat_lines = []
        action_lines = []
        game_flag = 0
        seat_flag = 1
        for line in list_of_lines:
            if "SUMMARY" in line:
                seat_flag = 0
            if seat_flag and "Seat" in line and "Table" not in line:
                seat_lines.append(line)
            elif "HOLE CARDS" in line:
                game_flag = 1
            elif "FLOP" in line:
                game_flag = 0

            if game_flag:
                action_lines.append(line)
        return title, seat_lines, action_lines

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

    def process_section(self, list_of_lines):
        title, seat_lines, action_lines = self.dissect_section(list_of_lines)
        bigblind = self.find_bb(title)
        if bigblind == 0:
            return []
        gameid = self.get_gameid(title)

        self.heroname, cards = self.set_heroname(action_lines)
        player_index, stacksize, num_players = self.get_seating_info(
            seat_lines)
        if stacksize == 0:
            return []
        player_position = self.get_position(action_lines)
        action_sequences = self.get_action_chain(action_lines, num_players)
        category = self.helper.get_category(cards)
        if category is None:
            category = {}
        amount_won = self.get_amount_won(action_lines)
        return_values = []
        for action_sequence in action_sequences:
            opportunity = self.get_strategy_from_moves(action_sequence)
            current_action = self.move_regex.findall(action_sequence[-1])
            action_sequence[-1] = "hero"
            res_dict = self.helper.run_everything(cards, " ".join(
                action_sequence), stacksize, self.rake, num_players)
            if res_dict is None:
                continue
            best_action = None
            highest_ev = max([res_dict[key]["ev"] for key in res_dict])
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
                "Hero Name": self.heroname,
                "Hand": cards,
                "Pairedness": category.get("pairing", ""),
                "Suitedness": category.get("suiting", ""),
                "Hand Category": category.get("category", ""),
                "Position": player_position,
                "Result": correct,
                "Opportunity": opportunity,
                "Stack Size": stacksize/bigblind,
                "Big Blind": "{}/{}".format(int(bigblind/2), int(bigblind)),
                "Player's Move": current_action[0],
                "GTO Move": best_action,
                "Amount Won in Terms of BB": amount_won/bigblind,
                "Move EV": move_ev,
                "GTO EV": highest_ev
            })
        return return_values

    def process_file(self, filename):
        if os.path.isfile(filename):
            with open(filename, 'r') as f:
                file_lines = f.readlines()

            current_segment = []
            segments = []
            file_len = len(file_lines)
            for i, line in enumerate(file_lines):
                if i < file_len - 1 and (file_lines[i] == '\n'):
                    if len(current_segment):
                        segments.append(current_segment)
                    current_segment = []
                    continue
                current_segment.append(line)
            return segments
        else:
            raise FileNotFoundError("file not found: {}".format(filename))

    def run_everything(self, filename):
        print("Starting")
        segments = self.process_file(filename)
        overall_vals = []
        start_time = time.time()
        len_segments = len(segments)
        for i, segment in enumerate(segments):
            try:
                # print("\rDone: {} out of {}".format(i, len_segments), end=" ")
                ret_vals = self.process_section(segment)
                for ret_val in ret_vals:
                    overall_vals.append(ret_val)
            except Exception as e:
                print("Error in iteration {}: {}".format(i, e))
        end_time = time.time()
        df = pd.DataFrame(overall_vals)
        print("Time taken: {:.2f}min for {} hands".format(
            (end_time-start_time)/60, len(segments)))
        return df, len_segments


if __name__ == '__main__':
    list_of_lines = """PokerStars Hand #163848438: Omaha Pot Limit ($200/$400 USD) - 2020/10/24 06:12:42 ET
Table 'ADA874189191' 6-max Seat #5 is the button
Seat 1: fernz666 ($57179 in chips)
Seat 2: bothras ($37360 in chips)
Seat 3: vimalchotai001 ($29212 in chips)
Seat 4: TurnOnTuneInDropOut ($32226 in chips)
Seat 5: hashstack ($24937 in chips)
Seat 6: nimit2908 ($14695 in chips)
nimit2908: posts small blind $200
fernz666: posts big blind $400
*** HOLE CARDS ***
Dealt to TurnOnTuneInDropOut [Ks Tc 7d 9d]
bothras: folds
vimalchotai001: folds
TurnOnTuneInDropOut: raises $1000 to $1400
hashstack: calls $1400
nimit2908: calls $1200
fernz666: folds
*** FLOP *** [7s 6d 2d]
nimit2908: checks
TurnOnTuneInDropOut: checks
hashstack: checks
*** TURN *** [7s 6d 2d] [As]
nimit2908: checks
TurnOnTuneInDropOut: checks
hashstack: checks
*** RIVER *** [7s 6d 2d As] [Ad]
nimit2908: checks
TurnOnTuneInDropOut: checks
hashstack: bets $2250
nimit2908: folds
TurnOnTuneInDropOut: calls $2250
*** SHOW DOWN ***
TurnOnTuneInDropOut: shows [Ks Tc 7d 9d]
hashstack: shows [Qh Td Js 2s]
TurnOnTuneInDropOut collected $8735 from pot
*** SUMMARY ***
Total pot $9100 | Rake $365
Board [7s 6d 2d As Ad]
Seat 1: fernz666 (big blind) folded before Flop
Seat 2: bothras folded before Flop (didn't bet)
Seat 3: vimalchotai001 folded before Flop (didn't bet)
Seat 4: TurnOnTuneInDropOut showed [Ks Tc 7d 9d] and won ($8735)
Seat 5: hashstack (button) showed [Qh Td Js 2s] and lost
Seat 6: nimit2908 (small blind) folded on the River"""

    #  list_of_lines = list_of_lines.split("\n")
    parser = PokerStarsParser()
    import time
    from pprint import pprint
    segments = parser.process_file("test_files")
    start = time.time()
    parser.process_segments(segments)
    end = time.time()
    print("Time taken: ", (end-start)/60)
