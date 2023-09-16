import datetime
import random

from pandas.core.indexing import is_nested_tuple
from proc_engine import adda52parser
from proc_engine import pdf_to_text
from proc_engine.adda52parser import Adda52Parser
from proc_engine.helper_functions import Helper
from proc_engine.pokerstarsparser import PokerStarsParser
import os
import logging
import proc_engine.__base__ as base
import time
import pandas as pd
from proc_engine.pdf_to_text import convert_to_text
from proc_engine import exceptions


class Main(object):

    def __init__(self):
        self.base = base
        self.basedb = base.BaseDB()
        self.helper = Helper()
        self.root_logger = self.get_logger("root.log", logger_name="root")
        self.pokerstars_logger = self.get_logger(
            "pokerstars.log", logger_name="9stacks")
        self.adda52_logger = self.get_logger(
            "adda52.log", logger_name="adda52")
        self.pokerstarsparser = PokerStarsParser(
            helper=self.helper, logging_enabled=True, logger=self.pokerstars_logger)
        self.adda52parser = Adda52Parser(
            helper=self.helper, logging_enabled=True, logger=self.adda52_logger)

    def get_logger(self, filename='debug.log', level=logging.INFO, logger_name: str = "root") -> logging.Logger:
        if not os.path.isdir(os.path.join(base.configdict.get("HOME_DIR"), "logs")):
            os.mkdir(os.path.join(base.configdict.get("HOME_DIR"), "logs"))

        logging.basicConfig(filename=os.path.join(base.configdict.get("HOME_DIR"), "logs", filename),
                            filemode='a', format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        return logging.getLogger(logger_name)

    def process_file(self, filename, filetype):
        # Checking if the filepath is absolute or not
        filepath = os.path.join(self.base.configdict.get(
            "UNPROCESSED_FILE_DIR"), filename)

        if filename.endswith(".pdf"):
            pdf_filename = filename
            txt_filename = filename.replace(".pdf", ".txt")
        elif filename.endswith(".txt"):
            pdf_filename = filename.replace(".txt", ".pdf")
            txt_filename = filename

        pdf_filepath = os.path.join(self.base.configdict.get(
            "UNPROCESSED_FILE_DIR"), pdf_filename)
        txt_filepath = os.path.join(self.base.configdict.get(
            "UNPROCESSED_FILE_DIR"), txt_filename)

        csv_filepath = os.path.join(self.base.configdict.get(
            "PROCESSED_FILE_DIR"), txt_filename.replace(".txt", ".csv"))
        # Checking if the file exists or not
        if not os.path.isfile(filepath):
            raise exceptions.UnprocessedFileNotFoundError(
                "Could not find file: {}".format(filepath))
        # Checking the format
        if filetype == 'adda52':
            # Checking if the upload extension is pdf
            if filename.endswith("pdf"):
                # PDF format
                # TODO: Add file size check as well
                if not os.path.isfile(txt_filepath):
                    file_contents = convert_to_text(pdf_filepath)

                    with open(txt_filepath, "w+") as f:
                        f.write(file_contents)

            if not os.path.isfile(txt_filepath):
                raise exceptions.UnprocessedFileNotFoundError(
                    "Converted PDF file not found as txt at: {}".format(txt_filepath))

            start_time = time.time()
            df, len_segments = self.adda52parser.run_everything(txt_filepath)
            end_time = time.time()
            processing_time = end_time - start_time
            if isinstance(df, pd.DataFrame):
                # TODO: Continue
                df.to_csv(csv_filepath)
                print("Saved to filepath: {}".format(csv_filepath))
                res = self.basedb.update_file_metadata(
                    filename, is_processed=True, num_hands=len_segments, num_hands_processed=df.shape[0], processing_time=processing_time)
                print("Commit to database: ", res)

            else:
                raise exceptions.ProcessingError("Could not process file")

        elif filetype == 'pokerstars':
            start_time = time.time()
            df, len_segments = self.pokerstarsparser.run_everything(
                txt_filepath)
            end_time = time.time()
            processing_time = end_time - start_time
            if isinstance(df, pd.DataFrame):
                df.to_csv(csv_filepath)
                print("Saved to filepath: {}".format(csv_filepath))
                res = self.basedb.update_file_metadata(
                    filename, is_processed=True, num_hands=len_segments, num_hands_processed=df.shape[0], processing_time=processing_time)
                print("Commit to database: ", res)
            else:
                raise exceptions.ProcessingError("Could not process file")

        else:
            raise exceptions.InvalidEnumerationError("Invalid file type input")


def insert_seed_data():
    basedb = base.BaseDB()
    emails = ["anniesri3110@gmail.com",
              "animesh.srivastava.1999@gmail.com", "gtoinspector.poker@gmail.com"]
    formats = ["adda52", "pokerstars"]
    print("Starting")
    start_time = time.time()
    for i in range(10000):
        randint = random.randint(10000, 99999)
        month_int = random.randint(1, 12)
        date_int = random.randint(1, 28)
        hour_int = random.randint(0, 23)
        minute_int = random.randint(0, 59)
        filename = "file_{}".format(i)
        basedb.add_file(filename, datetime.datetime(2021, month_int, date_int,
                        hour_int, minute_int), random.choice(emails), random.choice(formats))
    end_time = time.time()
    print("Done. Number of iterations:", i)
    print("time taken: ", end_time - start_time)


if __name__ == '__main__':
    main = Main()
    main.basedb.add_file(
        "adda52_test.pdf", datetime.datetime.now(), "anniesri3110@gmail.com", "adda52")
    main.basedb.add_file(
        "pokerstars_test.txt", datetime.datetime.now(), "anniesri3110@gmail.com", "pokerstars")
    print("Running script")
    main.process_file("adda52_test.pdf", "adda52")
    print("Adda52 complete")
    main.process_file("pokerstars_test.txt", "pokerstars")
    print("Pokerstarts complete")
